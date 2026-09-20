-- The importer's product projection: a derived, query-side view of the
-- product event stream. The authoritative catalog lives in the
-- catalog-worker's database; this table lets the importer serve product
-- queries for its upload UI without any cross-database access.
-- Current-state semantics: the latest event per product identity wins.
CREATE TABLE IF NOT EXISTS product_projection (
    merchant_id     TEXT        NOT NULL,
    product_id      TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    price_cents     BIGINT      NOT NULL,
    currency        TEXT        NOT NULL,
    expiration_date DATE,
    source_job_id   UUID,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_product_projection_job ON product_projection (source_job_id);