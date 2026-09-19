// Package catalog implements the catalog-worker service: consuming product
// events, materializing them into the product catalog, and reporting import
// progress back onto the event stream.
package catalog

import (
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// Product is a catalog record materialized from one ProductImported event.
// Merchant and product identity together form the natural key; the same
// identity arriving twice is the same product, not a duplicate.
type Product struct {
	MerchantID     string
	ProductID      string
	Name           string
	PriceCents     int64
	Currency       string
	ExpirationDate string
	SourceJobID    string
}

// FromEvent materializes the product carried by an event.
func FromEvent(evt events.ProductImported) Product {
	return Product{
		MerchantID:     evt.Product.MerchantID,
		ProductID:      evt.Product.ProductID,
		Name:           evt.Product.Name,
		PriceCents:     evt.Product.PriceCents,
		Currency:       evt.Product.Currency,
		ExpirationDate: evt.Product.ExpirationDate,
		SourceJobID:    evt.JobID,
	}
}
