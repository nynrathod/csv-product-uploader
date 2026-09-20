package importjob

import (
	"context"
	"errors"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
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
	// FailStale marks every job whose work died with the process as
	// failed; it runs at service startup so orphaned jobs never hang.
	FailStale(ctx context.Context, reason string) (int64, error)
	// ApplyProgress folds one catalog worker's cumulative snapshot into
	// the job's ledger. Each worker's snapshot folds monotonically and
	// the job totals derive from the sum across workers; duplicate or
	// replayed reports converge instead of double-counting.
	ApplyProgress(ctx context.Context, id, workerID string, processed, retried, dead int64) error
}

// EventPublisher emits product events onto the durable event stream that
// decouples the importer from every downstream consumer.
type EventPublisher interface {
	// PublishProductImported emits one normalized product event. It must
	// be safe for concurrent use.
	PublishProductImported(ctx context.Context, evt events.ProductImported) error
	// Flush waits until everything published so far is durably delivered
	// and reports the first delivery failure, if any.
	Flush(ctx context.Context) error
	// Close flushes and releases the underlying producer.
	Close()
}

// LatencySummary carries measured latency percentiles in milliseconds.
type LatencySummary struct {
	Samples int
	P50MS   float64
	P95MS   float64
	MaxMS   float64
}

// ProductQuery selects one page of the product projection.
type ProductQuery struct {
	FilterName string
	SortBy     string
	SortOrder  string
	Limit      int
	Offset     int
}

// ProjectionStore maintains the importer's own read model of products.
// The projection is derived exclusively from the product event stream, so
// the importer serves catalog queries without ever touching the catalog
// database: the same events feed the catalog-worker's write model and
// this read model.
type ProjectionStore interface {
	UpsertProducts(ctx context.Context, evts []events.ProductImported) error
	ListProducts(ctx context.Context, jobID string, q ProductQuery) ([]events.ProductData, int64, error)
}
