package importjob

import (
	"context"
	"errors"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// ErrNotFound reports an unknown import job id.
var ErrNotFound = errors.New("import job not found")

// JobStore persists import jobs in the importer-owned database.
type JobStore interface {
	// Create inserts a new job and fills its identity and timestamps.
	Create(ctx context.Context, job *ImportJob) error
	// Get returns the current state of one job.
	Get(ctx context.Context, id string) (ImportJob, error)
	// List returns the most recent jobs, newest first.
	List(ctx context.Context, limit int) ([]ImportJob, error)
	// UpdateProgress overwrites the row counters of a running job.
	UpdateProgress(ctx context.Context, id string, p Progress) error
	// UpdateStatus applies a lifecycle transition and optionally records
	// the failure cause.
	UpdateStatus(ctx context.Context, id string, to Status, lastErr *string) error
	// FailStale marks every non-terminal job as failed; it runs at service
	// startup so jobs orphaned by a restart or crash never hang forever.
	FailStale(ctx context.Context, reason string) (int64, error)
}

// EventPublisher emits product events onto the durable event stream that
// decouples the importer from every downstream consumer.
type EventPublisher interface {
	// PublishProductImported emits one normalized product event. It must
	// be safe for concurrent use.
	PublishProductImported(ctx context.Context, evt events.ProductImported) error
	// Close flushes and releases the underlying producer.
	Close()
}
