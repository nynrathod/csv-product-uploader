// Package events defines the contracts exchanged between the importer and
// the catalog-worker over Kafka. These types are the single source of truth
// shared by both services; changes must remain backward compatible.
package events

import "time"

// ProductData is the normalized product payload carried by ProductImported
// events. It is produced by the importer from CSV input and materialized by
// the catalog-worker into the product catalog.
type ProductData struct {
	MerchantID string `json:"merchant_id"`
	ProductID  string `json:"product_id"`
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Currency   string `json:"currency"`
	// ExpirationDate is the canonical YYYY-MM-DD date, empty when the
	// product has no expiration.
	ExpirationDate string `json:"expiration_date,omitempty"`
}

// ProductImported wraps a product payload with import metadata so consumers
// can trace every catalog record back to the originating import job and row.
type ProductImported struct {
	JobID      string      `json:"job_id"`
	RowNum     int64       `json:"row_num"`
	ProducedAt time.Time   `json:"produced_at"`
	Product    ProductData `json:"product"`
}

// PartitionKey returns the Kafka routing key for the product: merchant and
// product identity together. Producers use it so every event for one
// product lands on the same partition, which is the guarantee that
// per-product updates are consumed in the order they were published.
func (p ProductData) PartitionKey() string {
	return p.MerchantID + ":" + p.ProductID
}
