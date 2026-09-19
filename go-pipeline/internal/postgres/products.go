package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/catalog"
)

// ProductWriter applies product upserts to the catalog database.
type ProductWriter struct {
	pool *pgxpool.Pool
}

// NewProductWriter returns a writer backed by the given pool.
func NewProductWriter(pool *pgxpool.Pool) *ProductWriter {
	return &ProductWriter{pool: pool}
}

// UpsertBatch applies every product in one transaction. The upsert is
// idempotent on the (merchant_id, product_id) natural key: re-applying the
// same product updates the same row instead of duplicating it, which is
// what makes at-least-once delivery safe.
func (w *ProductWriter) UpsertBatch(ctx context.Context, products []catalog.Product) error {
	if len(products) == 0 {
		return nil
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning catalog transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, p := range products {
		batch.Queue(`
            INSERT INTO products (merchant_id, product_id, name, price_cents, currency, expiration_date, source_job_id)
            VALUES ($1, $2, $3, $4, $5, NULLIF($6, '')::date, $7)
            ON CONFLICT (merchant_id, product_id) DO UPDATE
            SET name = EXCLUDED.name,
                price_cents = EXCLUDED.price_cents,
                currency = EXCLUDED.currency,
                expiration_date = EXCLUDED.expiration_date,
                source_job_id = EXCLUDED.source_job_id,
                updated_at = now()`,
			p.MerchantID, p.ProductID, p.Name, p.PriceCents, p.Currency, p.ExpirationDate, p.SourceJobID)
	}

	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("applying catalog batch: %w", err)
	}
	return tx.Commit(ctx)
}
