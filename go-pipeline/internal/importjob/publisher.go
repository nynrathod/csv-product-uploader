package importjob

import (
	"context"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// NoopPublisher discards published events. It exists so the importer can
// run, and be tested, without an event broker configured; the Kafka-backed
// implementation replaces it whenever one is.
type NoopPublisher struct{}

// PublishProductImported accepts and discards the event.
func (NoopPublisher) PublishProductImported(context.Context, events.ProductImported) error {
	return nil
}

// Close is a no-op.
func (NoopPublisher) Close() {}
