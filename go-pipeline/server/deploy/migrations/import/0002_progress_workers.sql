-- Per-worker progress snapshots for import jobs. Workers count
-- independently because Kafka assigns partitions per worker; a job's
-- totals derive from the sum across its workers' latest snapshots.
CREATE TABLE IF NOT EXISTS import_progress_workers (
    job_id         UUID        NOT NULL,
    worker_id      TEXT        NOT NULL,
    processed_rows BIGINT      NOT NULL DEFAULT 0,
    retried_rows   BIGINT      NOT NULL DEFAULT 0,
    dead_rows      BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, worker_id)
);