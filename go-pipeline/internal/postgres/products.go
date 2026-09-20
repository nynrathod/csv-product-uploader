package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/catalog"
)

// ProductWriter applies product upserts to the catalog database.
type ProductWriter struct {
	pool *pgxpool.Pool
}

// NewProductWriter returns a writer backed by the given pool.
func NewProductWriter(pool *pgxpool.Pool) *ProductWriter {
	return &ProductWriter{pool: pool}
}

// UpsertBatch applies every product as one unnest-driven upsert: the whole
// batch is a single statement and a single network round trip, which keeps
// catalog drain rate limited by the database, not by statement chatter.
// The upsert is idempotent on the (merchant_id, product_id) natural key:
// re-applying the same product updates the same row instead of duplicating
// it, which is what makes at-least-once delivery safe.
func (w *ProductWriter) UpsertBatch(ctx context.Context, products []catalog.Product) error {
	if len(products) == 0 {
		return nil
	}

	merchants := make([]string, len(products))
	productIDs := make([]string, len(products))
	names := make([]string, len(products))
	prices := make([]int64, len(products))
	currencies := make([]string, len(products))
	expirations := make([]string, len(products))
	jobIDs := make([]string, len(products))
	for i, p := range products {
		merchants[i] = p.MerchantID
		productIDs[i] = p.ProductID
		names[i] = p.Name
		prices[i] = p.PriceCents
		currencies[i] = p.Currency
		expirations[i] = p.ExpirationDate
		jobIDs[i] = p.SourceJobID
	}

	// A single statement is atomic on its own; no explicit transaction is
	// needed. NULLIF maps the empty string to a missing expiration.
	_, err := w.pool.Exec(ctx, `
        INSERT INTO products (merchant_id, product_id, name, price_cents, currency, expiration_date, source_job_id)
        SELECT m, p, n, pr, c, NULLIF(e, '')::date, j::uuid
        FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::text[], $7::text[])
            AS t(m, p, n, pr, c, e, j)
        ON CONFLICT (merchant_id, product_id) DO UPDATE
        SET name = EXCLUDED.name,
            price_cents = EXCLUDED.price_cents,
            currency = EXCLUDED.currency,
            expiration_date = EXCLUDED.expiration_date,
            source_job_id = EXCLUDED.source_job_id,
            updated_at = now()`,
		merchants, productIDs, names, prices, currencies, expirations, jobIDs)
	if err != nil {
		return fmt.Errorf("applying catalog batch: %w", err)
	}
	return nil
}
