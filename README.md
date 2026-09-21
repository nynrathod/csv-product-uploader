# CatalogStream - go-pipeline

Distributed, event-driven product-import platform. CSV uploads are streamed with bounded memory, validated, and published to Kafka as durable events; an independent worker materializes them into PostgreSQL with idempotent upserts. Two services, two databases, one event log.

```text
                    ┌─────────────────────────┐
CSV upload ─HTTP──▶ │ importer (Golang)     │ owns import_db
                    │ stream · validate       │
                    │ publish · track         │
                    └───────────┬─────────────┘
                                │ product.imported.v1
                                │ key: merchantId:productId
                                ▼
                    ┌─────────────────────────┐
                    │ Kafka                   │ durable event log
                    └───────────┬─────────────┘
                                │ consumer group
                                │ N workers · 6 partitions
                                ▼
                    ┌─────────────────────────┐
products table ◀──── │ catalog-worker          │ owns catalog_db
                    │ idempotent upserts      │
                    │ retry · DLQ             │
                    │ commit-after-write      │
                    └───────────┬─────────────┘
                                │ import.progress.v1
                                │ per-worker snapshots
                                ▼
                    importer folds progress into its own jobs;
                    a job completes when processed + dead ≥ published
```

## Design

- **Two independently deployable services** (`cmd/importer`, `cmd/catalog-worker`), each owning its own database. The boundary is enforced by PostgreSQL roles - the importer's role cannot connect to `catalog_db` at all.
- **No synchronous coupling.** Cross-service knowledge flows only through Kafka: product events forward, progress events backward.
- **Effectively-once materialization** from at-least-once delivery: idempotent `ON CONFLICT` upserts plus offsets committed only after every event in a batch is applied or republished.
- **Failure isolation.** Invalid events are dead-lettered without a database round trip; write failures retry in place; undecodable records are quarantined to the DLQ with original bytes preserved.
- **Recovery.** Killed workers resume from committed offsets; jobs awaiting confirmation survive importer restarts; the catalog rebuilds entirely from retained events via `cmd/replay` - no re-upload.
- **Bounded memory.** Streaming CSV parse with record recycling, batched progress writes, and a capped producer buffer. RSS stays flat regardless of file size.

## Topics

| Topic | Partitions | Purpose |
|---|---|---|
| `product.imported.v1` | 6 | product events, keyed `merchantId:productId` |
| `product.retry.v1` | 6 | retries with attempt headers |
| `product.dlq.v1` | 3 | permanently rejected events |
| `import.progress.v1` | 3 | per-worker progress snapshots |

## Quick start

```bash
docker compose -f deploy/docker-compose.yml up -d   # kafka, kafka-ui, postgres
go run ./cmd/importer                                # :8080
go run ./cmd/catalog-worker                          # consumer group
```

Upload and watch progress:

```bash
curl -F "file=@products.csv" http://localhost:8080/api/v1/imports
curl http://localhost:8080/api/v1/imports/<id>        # live counters
curl http://localhost:8080/api/v1/imports/<id>/report # reconciliation
```

Kafka UI: http://localhost:8081

## API

| Method | Path | Description |
|---|---|---|
| POST | `/api/v1/imports` | multipart CSV upload; returns 202 with job id |
| GET | `/api/v1/imports` | list recent jobs |
| GET | `/api/v1/imports/:id` | job status and counters |
| GET | `/api/v1/imports/:id/events` | SSE live progress until terminal state |
| GET | `/api/v1/imports/:id/report` | reconciliation: published vs confirmed |

## Benchmarks

Measured end-to-end through the public HTTP  (Docker Desktop: Kafka + PostgreSQL). Zero-loss is verified independently: published events = processed events = catalog row count. Run-to-run variance is roughly ±10%; reproduce with `go run ./benchmark/run`.

| Rows | Workers | End-to-end | Sustained | Peak drain | p50 | p95 | p99 | Peak RSS |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1M (68 MB) | 1 | 24.1 s | 40.8K rows/s | 103K rows/s | 8 ms | 16 ms | 19 ms | 251 MB |
| 1M (68 MB) | 3 | 11.3 s | 87.3K rows/s | 199K rows/s | 5 ms | 16 ms | 33 ms | 292 MB |
| 3M (204 MB) | 5 | 29.3 s | 100.9K rows/s | 380K rows/s | 17 ms | 48 ms | 56 ms | 625 MB |

Ingest throughput (CSV → durable Kafka events): **~290–355K rows/s**.

**Crash recovery** (`-kill-test`): a worker is hard-killed at 30% of a 1M-row import. The respawned worker resumes from the last committed offset; the import completes with zero data loss and full reconciliation.

**Replay** (`cmd/replay`): truncate the catalog, reset the consumer group's offsets, restart the worker - the catalog rebuilds entirely from retained Kafka events. The source CSV is never re-uploaded.

```bash
go run ./benchmark/run                      # 1M rows, 1 worker
go run ./benchmark/run -workers 3           # scaling
go run ./benchmark/run -kill-test           # crash recovery
go run ./benchmark/run -rows 3000000 -workers 5
```

## Repository layout

```text
cmd/importer          HTTP API: upload, stream, validate, publish
cmd/catalog-worker    consumer group: upserts, retry, DLQ, progress
cmd/replay            offset reset + catalog rebuild tool
internal/importjob    import domain: CSV streamer, jobs, SSE, progress tracker
internal/catalog      worker domain: batch processing, retry policy
internal/kafka        franz-go clients: producer, consumer, admin
internal/postgres     pgx adapters + migrations
deploy/               docker-compose, init SQL, Dockerfiles
benchmark/            fixture generator + measurement harness
```

## Testing

```bash
go test ./... -count=1
TEST_KAFKA_BROKERS=localhost:29092 go test ./internal/kafka -count=1 -v   # broker integration
```



## Related projects

- [csv-product-uploader](https://github.com/nynrathod/csv-product-uploader) - earlier monolithic version (NestJS + React)
