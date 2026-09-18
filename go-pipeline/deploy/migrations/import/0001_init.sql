-- import_db - owned by the importer service
CREATE TABLE IF NOT EXISTS import_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    file_name       TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN
                      ('uploading','parsing','publishing','processing','completed','failed')),
    total_rows      BIGINT      NOT NULL DEFAULT 0,
    valid_rows      BIGINT      NOT NULL DEFAULT 0,
    invalid_rows    BIGINT      NOT NULL DEFAULT 0,
    published_rows  BIGINT      NOT NULL DEFAULT 0,
    processed_rows  BIGINT      NOT NULL DEFAULT 0,   -- updated ONLY via import.progress.v1 events
    retried_rows    BIGINT      NOT NULL DEFAULT 0,
    dead_rows       BIGINT      NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_import_jobs_created_at ON import_jobs (created_at DESC);