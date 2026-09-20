-- catalog_db - owned by the catalog-worker service
CREATE TABLE IF NOT EXISTS products (
    merchant_id     TEXT        NOT NULL,
    product_id      TEXT        NOT NULL,
    name            TEXT        NOT NULL,
    price_cents     BIGINT      NOT NULL CHECK (price_cents >= 0),
    currency        CHAR(3)     NOT NULL,
    expiration_date DATE,
    source_job_id   UUID,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (merchant_id, product_id)   -- idempotent upsert target
);