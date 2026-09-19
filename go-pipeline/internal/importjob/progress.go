package importjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// EventSource supplies consumed records and offset commits for the
// importer's own event consumption. Declaring the port here keeps the
// importer dependent on the shape it needs, not on any consumer
// implementation.
type EventSource interface {
	Poll(ctx context.Context) []*kgo.Record
	CommitRecords(ctx context.Context, recs ...*kgo.Record) error
	Close()
}

// uuidPattern matches the canonical textual UUID form. Import job ids are
// database-generated UUIDs; progress events referencing anything else come
// from foreign producers and are consumed without effect.
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ProgressTracker consumes the catalog worker's progress events and folds
// them into the importer's own job records. This is the backward half of
// the event-driven contract: the worker reports through the progress
// topic and the importer updates its own database, with no shared schema
// between the two services.
type ProgressTracker struct {
	store  JobStore
	source EventSource
}

// NewProgressTracker wires the tracker.
func NewProgressTracker(store JobStore, source EventSource) *ProgressTracker {
	return &ProgressTracker{store: store, source: source}
}

// Run consumes progress events until the context is cancelled. Offsets
// commit only after their counts are durably folded into a job record,
// mirroring the catalog worker's commit-after-write rule: a crash between
// apply and commit re-delivers the event, and the fold is idempotent
// because completed jobs ignore further progress.
func (t *ProgressTracker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		recs := t.source.Poll(ctx)
		if len(recs) == 0 {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}

		applied := make([]*kgo.Record, 0, len(recs))
		for _, rec := range recs {
			if t.apply(ctx, rec) {
				applied = append(applied, rec)
			}
		}
		if len(applied) > 0 {
			if err := t.source.CommitRecords(ctx, applied...); err != nil {
				return fmt.Errorf("committing progress offsets: %w", err)
			}
		}
	}
}

// apply folds one progress event into its job record and reports whether
// the event is settled: settled events may commit their offset, while
// store failures leave the offset uncommitted for redelivery.
func (t *ProgressTracker) apply(ctx context.Context, rec *kgo.Record) bool {
	var evt events.ImportProgress
	if err := json.Unmarshal(rec.Value, &evt); err != nil {
		// The progress stream is advisory: an undecodable event is
		// consumed and logged rather than wedging the tracker.
		log.Printf("progress tracker: skipping undecodable event on %s/%d@%d: %v",
			rec.Topic, rec.Partition, rec.Offset, err)
		return true
	}
	if !uuidPattern.MatchString(evt.JobID) {
		log.Printf("progress tracker: skipping event for foreign job id %q", evt.JobID)
		return true
	}

	if err := t.store.ApplyProgress(ctx, evt.JobID, evt.ProcessedRows, evt.RetriedRows, evt.DeadRows); err != nil {
		if errors.Is(err, ErrNotFound) {
			log.Printf("progress tracker: no import job %s (event consumed)", evt.JobID)
			return true
		}
		log.Printf("progress tracker: applying progress for job %s failed: %v", evt.JobID, err)
		return false
	}
	return true
}
