package importjob

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
)

// projectionWriteAttempts bounds in-place projection write retries. The
// consumer client does not redeliver polled records within a session, so
// a failed write must be retried before its offsets may commit.
const projectionWriteAttempts = 3

// ProductProjector consumes product events and maintains the importer's
// product projection. It is a second, independent consumer group on the
// product topic: the catalog-worker materializes the authoritative
// catalog while the projector feeds the importer's query-side view. The
// projection exists to serve the UI; eventual consistency between the
// two views is inherent and acceptable.
type ProductProjector struct {
	store  ProjectionStore
	source EventSource
}

// NewProductProjector wires the projector.
func NewProductProjector(store ProjectionStore, source EventSource) *ProductProjector {
	return &ProductProjector{store: store, source: source}
}

// Run consumes until the context is cancelled. Each cycle polls records,
// upserts the projection, and commits offsets only after the write
// succeeded: the commit-after-write rule that keeps the projection
// consistent with its offsets.
func (p *ProductProjector) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		recs := p.source.Poll(ctx)
		if len(recs) == 0 {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}

		// Undecodable or invalid records are skipped: the catalog worker
		// has already quarantined them to the dead letter topic, and a
		// read model must never wedge on poison input.
		evts := make([]events.ProductImported, 0, len(recs))
		for _, rec := range recs {
			var evt events.ProductImported
			if err := json.Unmarshal(rec.Value, &evt); err != nil {
				log.Printf("projection: skipping undecodable record %s/%d@%d: %v",
					rec.Topic, rec.Partition, rec.Offset, err)
				continue
			}
			if !uuidPattern.MatchString(evt.JobID) ||
				evt.Product.MerchantID == "" || evt.Product.ProductID == "" {
				continue
			}
			evts = append(evts, evt)
		}

		if len(evts) > 0 {
			var writeErr error
			for attempt := 0; attempt < projectionWriteAttempts; attempt++ {
				writeErr = p.store.UpsertProducts(ctx, evts)
				if writeErr == nil {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
				}
			}
			if writeErr != nil {
				return fmt.Errorf("projecting products after %d attempts: %w", projectionWriteAttempts, writeErr)
			}
		}

		if err := p.source.CommitRecords(ctx, recs...); err != nil {
			return fmt.Errorf("committing projection offsets: %w", err)
		}
	}
}
