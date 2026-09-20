// Package kafka adapts the platform's event contracts to franz-go backed
// Kafka clients: topic declaration, durable production, and consumption.
package kafka

// Topic names are the single source of truth for the event backbone.
// Producers and consumers reference these constants; string literals never
// appear elsewhere. The .v1 suffix leaves room for incompatible contract
// changes via new topics rather than in-place mutation.
const (
	TopicProductImported = "product.imported.v1"
	TopicProductRetry    = "product.retry.v1"
	TopicProductDLQ      = "product.dlq.v1"
	TopicImportProgress  = "import.progress.v1"
)

// Partition counts per topic. Six partitions on the product topics let a
// consumer group scale to six parallel workers while preserving per-product
// ordering; the progress topics carry far less volume.
const (
	PartitionsProductImported = 6
	PartitionsProductRetry    = 6
	PartitionsProductDLQ      = 3
	PartitionsImportProgress  = 3
)

// Consumer group identifiers. Every group owns independent offsets on the
// same topics: the catalog writer materializes products while the import
// tracker advances import progress, without any coordination.
const (
	GroupCatalogWriter = "catalog-writer"
	GroupImportTracker = "import-tracker"
	GroupImportReader  = "import-reader"
)
