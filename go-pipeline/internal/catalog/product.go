// Package catalog implements the catalog-worker service: consuming product
// events, materializing them into the product catalog, and reporting import
// progress back onto the event stream.
package catalog

import (
	"fmt"

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

// FromEvent materializes the product carried by an event. Rows the event
// stream should never have accepted, such as malformed job ids, are
// rejected here: the catalog validates its inputs independently of the
// producer, so one malformed event cannot poison a batch write.
func FromEvent(evt events.ProductImported) (Product, error) {
	p := Product{
		MerchantID:     evt.Product.MerchantID,
		ProductID:      evt.Product.ProductID,
		Name:           evt.Product.Name,
		PriceCents:     evt.Product.PriceCents,
		Currency:       evt.Product.Currency,
		ExpirationDate: evt.Product.ExpirationDate,
		SourceJobID:    evt.JobID,
	}
	if err := p.validate(); err != nil {
		return Product{}, fmt.Errorf("event row %d: %w", evt.RowNum, err)
	}
	return p, nil
}

// validate enforces the catalog's own write invariants.
func (p Product) validate() error {
	if p.MerchantID == "" {
		return fmt.Errorf("merchant_id is required")
	}
	if p.ProductID == "" {
		return fmt.Errorf("product_id is required")
	}
	if p.Name == "" {
		return fmt.Errorf("name is required")
	}
	if p.PriceCents < 0 {
		return fmt.Errorf("price_cents cannot be negative")
	}
	if p.Currency == "" {
		return fmt.Errorf("currency is required")
	}
	// source_job_id feeds a uuid column; non-uuid values fail the batch's
	// cast and poison otherwise-valid rows sharing the statement.
	if !isUUID(p.SourceJobID) {
		return fmt.Errorf("source_job_id %q is not a uuid", p.SourceJobID)
	}
	return nil
}
