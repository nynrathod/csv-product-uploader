package importjob

import (
	"context"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
)

// NoopPublisher discards published events. Tests use it to exercise the
// import flow without a broker; the Kafka-backed implementation serves
// production.
type NoopPublisher struct{}

// PublishProductImported accepts and discards the event.
func (NoopPublisher) PublishProductImported(context.Context, events.ProductImported) error {
	return nil
}

// Flush is a no-op: nothing is buffered, so nothing can fail.
func (NoopPublisher) Flush(context.Context) error { return nil }

// Close is a no-op.
func (NoopPublisher) Close() {}
