package catalog

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
)

// ProductWriter applies batches of products to the catalog with idempotent
// upserts: re-applying the same batch must not create duplicates.
type ProductWriter interface {
	UpsertBatch(ctx context.Context, products []Product) error
}

// RetryAction is the verdict of a retry policy.
type RetryAction int

const (
	// RetryActionRetry defers the event to the retry topic.
	RetryActionRetry RetryAction = iota
	// RetryActionDeadLetter permanently rejects the event to the DLQ.
	RetryActionDeadLetter
)

// RetryPolicy decides what happens to an event whose processing failed.
type RetryPolicy interface {
	OnFailure(attempt int, err error) RetryAction
}

// Event is a consumed product event paired with the metadata the worker
// needs to make delivery decisions about it.
type Event struct {
	events.ProductImported
	// Attempt is the number of processing attempts already made, read
	// from the retry header; zero on the first attempt.
	Attempt int
	// Topic is the topic the event was consumed from.
	Topic string
}

// EventRepublisher republishes failed events onto the retry and dead
// letter topics, preserving the original payload and carrying retry
// metadata in headers.
type EventRepublisher interface {
	// RepublishRetry emits an event onto the retry topic.
	RepublishRetry(ctx context.Context, evt events.ProductImported, attempt int, cause error) error
	// RepublishDead emits an event onto the dead letter topic.
	RepublishDead(ctx context.Context, evt events.ProductImported, cause error) error
	// RepublishDeadRaw dead-letters a record's payload verbatim, used for
	// events whose bytes could not be decoded at all.
	RepublishDeadRaw(ctx context.Context, rec *kgo.Record, cause error) error
	Flush(ctx context.Context)
	Close()
}

// ProgressReporter announces import progress back onto the event stream so
// the importer advances its job without any database or RPC coupling.
type ProgressReporter interface {
	ReportProgress(ctx context.Context, jobID string, processed, retried, dead int64) error
	Flush(ctx context.Context) error
	Close()
}

// RecordSource supplies consumed records and offset commits; it is the
// consumer port so the processing loop is testable without a broker.
type RecordSource interface {
	// Poll returns records for one poll window; empty when idle.
	Poll(ctx context.Context) []*kgo.Record
	// CommitRecords commits the given records' offsets.
	CommitRecords(ctx context.Context, recs ...*kgo.Record) error
	Close()
}
