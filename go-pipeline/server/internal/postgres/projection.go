package postgres

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/importjob"
)

// ProjectionStore maintains the importer's product projection in the
// import database: a derived, query-oriented view of the product event
// stream.
type ProjectionStore struct {
	pool *pgxpool.Pool
}

// NewProjectionStore returns a store backed by the given pool.
func NewProjectionStore(pool *pgxpool.Pool) *ProjectionStore {
	return &ProjectionStore{pool: pool}
}

// projectionSortColumns allowlists sortable columns; user input never
// reaches the order-by clause verbatim.
var projectionSortColumns = map[string]string{
	"name":       "name",
	"price":      "price_cents",
	"expiration": "expiration_date",
}

// UpsertProducts applies a batch of product events as one unnest-driven
// upsert, deduplicated on the natural key inside the statement: event
// streams legitimately carry repeated identities, and PostgreSQL rejects
// ON CONFLICT statements that would touch the same row twice. The last
// occurrence per identity wins, so the projection converges to the
// latest state of every product. The event's production timestamp is
// carried through, enabling exact event-to-database latency measurement.
func (s *ProjectionStore) UpsertProducts(ctx context.Context, evts []events.ProductImported) error {
	if len(evts) == 0 {
		return nil
	}

	merchants := make([]string, len(evts))
	productIDs := make([]string, len(evts))
	names := make([]string, len(evts))
	prices := make([]int64, len(evts))
	currencies := make([]string, len(evts))
	expirations := make([]string, len(evts))
	jobIDs := make([]string, len(evts))
	produced := make([]time.Time, len(evts))
	for i, e := range evts {
		merchants[i] = e.Product.MerchantID
		productIDs[i] = e.Product.ProductID
		names[i] = e.Product.Name
		prices[i] = e.Product.PriceCents
		currencies[i] = e.Product.Currency
		expirations[i] = e.Product.ExpirationDate
		jobIDs[i] = e.JobID
		produced[i] = e.ProducedAt
	}

	_, err := s.pool.Exec(ctx, `
        INSERT INTO product_projection (merchant_id, product_id, name, price_cents, currency, expiration_date, source_job_id, produced_at)
        SELECT m, p, n, pr, c, NULLIF(e, '')::date, j::uuid, pa
        FROM (
            SELECT DISTINCT ON (m, p) m, p, n, pr, c, e, j, pa
            FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::text[], $6::text[], $7::text[], $8::timestamptz[])
                WITH ORDINALITY AS t(m, p, n, pr, c, e, j, pa, ord)
            ORDER BY m, p, ord DESC
        ) AS batch
        ON CONFLICT (merchant_id, product_id) DO UPDATE
        SET name = EXCLUDED.name,
            price_cents = EXCLUDED.price_cents,
            currency = EXCLUDED.currency,
            expiration_date = EXCLUDED.expiration_date,
            source_job_id = EXCLUDED.source_job_id,
            produced_at = EXCLUDED.produced_at,
            updated_at = now()`,
		merchants, productIDs, names, prices, currencies, expirations, jobIDs, produced)
	if err != nil {
		return fmt.Errorf("upserting product projection: %w", err)
	}
	return nil
}

// ListProducts returns one page of the products most recently projected
// from the given import, plus the total matching count.
func (s *ProjectionStore) ListProducts(ctx context.Context, jobID string, q importjob.ProductQuery) ([]events.ProductData, int64, error) {
	where := "source_job_id = $1::uuid"
	args := []any{jobID}
	if q.FilterName != "" {
		args = append(args, q.FilterName)
		where += fmt.Sprintf(" AND name ILIKE '%%' || $%d || '%%'", len(args))
	}

	var total int64
	if err := s.pool.QueryRow(ctx,
		"SELECT count(*) FROM product_projection WHERE "+where, args...,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("counting products: %w", err)
	}

	column := projectionSortColumns[q.SortBy]
	if column == "" {
		column = "name"
	}
	order := "ASC"
	if q.SortOrder == "DESC" {
		order = "DESC"
	}

	args = append(args, q.Limit, q.Offset)
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
        SELECT merchant_id, product_id, name, price_cents, currency,
               COALESCE(to_char(expiration_date, 'YYYY-MM-DD'), '')
        FROM product_projection
        WHERE %s
        ORDER BY %s %s NULLS LAST, merchant_id, product_id
        LIMIT $%d OFFSET $%d`,
		where, column, order, len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, fmt.Errorf("listing products: %w", err)
	}
	defer rows.Close()

	products := make([]events.ProductData, 0, q.Limit)
	for rows.Next() {
		var p events.ProductData
		if err := rows.Scan(&p.MerchantID, &p.ProductID, &p.Name, &p.PriceCents, &p.Currency, &p.ExpirationDate); err != nil {
			return nil, 0, fmt.Errorf("scanning product: %w", err)
		}
		products = append(products, p)
	}
	return products, total, rows.Err()
}

// ProductLatency measures event-to-database latency over the most
// recently projected products. Each projected row records the event's
// production timestamp, so the delta between projection write time and
// event production time is an exact end-to-end sample.
func (s *ProjectionStore) ProductLatency(ctx context.Context, limit int) (importjob.LatencySummary, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT EXTRACT(EPOCH FROM (updated_at - produced_at)) * 1000
        FROM product_projection
        WHERE produced_at IS NOT NULL
        ORDER BY updated_at DESC
        LIMIT $1`, limit)
	if err != nil {
		return importjob.LatencySummary{}, fmt.Errorf("measuring product latency: %w", err)
	}
	defer rows.Close()

	lats := make([]float64, 0, limit)
	for rows.Next() {
		var ms float64
		if err := rows.Scan(&ms); err != nil {
			return importjob.LatencySummary{}, fmt.Errorf("scanning latency: %w", err)
		}
		lats = append(lats, ms)
	}
	if err := rows.Err(); err != nil {
		return importjob.LatencySummary{}, err
	}

	sort.Float64s(lats)
	summary := importjob.LatencySummary{Samples: len(lats)}
	if len(lats) > 0 {
		summary.P50MS = percentile(lats, 0.50)
		summary.P95MS = percentile(lats, 0.95)
		summary.MaxMS = lats[len(lats)-1]
	}
	return summary, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
