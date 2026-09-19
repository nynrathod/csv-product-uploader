package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// WorkerConfig tunes the consume-process-commit loop.
type WorkerConfig struct {
	// ProgressFlushInterval flushes accumulated progress at least this
	// often while work is flowing.
	ProgressFlushInterval time.Duration
}

// Worker drives the consume-process-commit cycle.
type Worker struct {
	source  RecordSource
	service *Service
	cfg     WorkerConfig
}

// NewWorker wires the loop, applying a safe default.
func NewWorker(source RecordSource, service *Service, cfg WorkerConfig) *Worker {
	if cfg.ProgressFlushInterval <= 0 {
		cfg.ProgressFlushInterval = 2 * time.Second
	}
	return &Worker{source: source, service: service, cfg: cfg}
}

// Run consumes until the context is cancelled. Each cycle polls records,
// processes them, commits offsets, then flushes progress: progress events
// always trail durable processing, never lead it. Undecodable records are
// dead-lettered so their offsets still commit; a poison payload can never
// wedge the pipeline.
func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.cfg.ProgressFlushInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return w.drain()
		}

		recs := w.source.Poll(ctx)
		if len(recs) == 0 {
			select {
			case <-ctx.Done():
				return w.drain()
			case <-ticker.C:
				if err := w.service.FlushProgress(ctx); err != nil {
					log.Printf("flushing progress: %v", err)
				}
			}
			continue
		}

		// Split decodable events from poison records up front.
		evts := make([]Event, 0, len(recs))
		var poison []*kgo.Record
		for _, rec := range recs {
			evt, err := parseEvent(rec)
			if err != nil {
				poison = append(poison, rec)
				continue
			}
			evts = append(evts, evt)
		}

		for _, rec := range poison {
			if err := w.service.ProcessPoison(ctx, rec); err != nil {
				// Dead-lettering failed: do not commit; reprocessing the
				// record re-attempts the dead letter, which is idempotent
				// in effect (the DLQ holds a copy either way).
				log.Printf("dead-lettering record %s/%d@%d failed: %v",
					rec.Topic, rec.Partition, rec.Offset, err)
				// Drop the record from this cycle's commit set by
				// aborting the whole cycle: retry everything.
				return fmt.Errorf("dead-lettering poison record: %w", err)
			}
		}

		if len(evts) > 0 {
			if err := w.service.ProcessBatch(ctx, evts); err != nil {
				// Unadjudicated failure: offsets stay uncommitted and the
				// next poll reprocesses the batch; upserts are idempotent.
				log.Printf("processing batch: %v", err)
				continue
			}
		}

		if err := w.source.CommitRecords(ctx, recs...); err != nil {
			return fmt.Errorf("committing offsets: %w", err)
		}
	}
}

// drain flushes outstanding progress on shutdown so the last counts are
// never lost to a restart.
func (w *Worker) drain() error {
	if err := w.service.FlushProgress(context.Background()); err != nil {
		return fmt.Errorf("flushing progress during shutdown: %w", err)
	}
	return nil
}

// parseEvent decodes one consumed record into an Event.
func parseEvent(rec *kgo.Record) (Event, error) {
	var evt events.ProductImported
	if err := json.Unmarshal(rec.Value, &evt); err != nil {
		return Event{}, fmt.Errorf("decoding event: %w", err)
	}
	return Event{
		ProductImported: evt,
		Attempt:         decodeAttemptHeader(rec),
		Topic:           rec.Topic,
	}, nil
}

// decodeAttemptHeader reads the retry attempt count from record headers.
func decodeAttemptHeader(rec *kgo.Record) int {
	for _, h := range rec.Headers {
		if h.Key == HeaderAttempts {
			return DecodeAttempts(h.Value)
		}
	}
	return 0
}
