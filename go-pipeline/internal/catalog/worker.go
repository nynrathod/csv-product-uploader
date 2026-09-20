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
// processes them, publishes progress, then commits offsets: progress is
// always published before the offsets it covers are committed, so a
// worker death can never strand counts between applied and reported. If
// the worker dies between publishing progress and committing, the
// successor re-consumes and re-counts at most one batch; the catalog's
// own row count remains the ground truth.
func (w *Worker) Run(ctx context.Context) error {
	// Every exit path flushes outstanding progress so the last counts of
	// a drain are never lost to a shutdown or an error return.
	defer func() {
		if err := w.drain(); err != nil {
			log.Printf("flushing progress on exit: %v", err)
		}
	}()

	// The flusher owns idle-period cadence on its own goroutine: it never
	// depends on the main loop cycling, so a drain's final totals are
	// always published. FlushProgress synchronizes on the service's
	// mutex, making concurrent flushes with the main loop safe.
	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		ticker := time.NewTicker(w.cfg.ProgressFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := w.service.FlushProgress(ctx); err != nil {
					log.Printf("flushing progress: %v", err)
				}
			}
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			<-flushDone
			return nil
		}

		recs := w.source.Poll(ctx)
		if len(recs) == 0 {
			if ctx.Err() != nil {
				<-flushDone
				return nil
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
				log.Printf("dead-lettering record %s/%d@%d failed: %v",
					rec.Topic, rec.Partition, rec.Offset, err)
				return fmt.Errorf("dead-lettering poison record: %w", err)
			}
		}

		if len(evts) > 0 {
			if err := w.service.ProcessBatch(ctx, evts); err != nil {
				// The batch could not be adjudicated after in-place
				// retries. Offsets stay uncommitted and the worker stops:
				// a restart re-fetches from the last committed offset,
				// which preserves at-least-once delivery.
				return fmt.Errorf("processing batch: %w", err)
			}
		}

		// Progress before commit: the reported totals must cover
		// everything this commit covers. One small event per batch keeps
		// this cheap even at large batch sizes.
		if err := w.service.FlushProgress(ctx); err != nil {
			return fmt.Errorf("flushing progress before commit: %w", err)
		}

		if err := w.source.CommitRecords(ctx, recs...); err != nil {
			return fmt.Errorf("committing offsets: %w", err)
		}
	}
}

// drain flushes outstanding progress so the last counts of a drain are
// never lost to a shutdown.
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
