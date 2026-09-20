package events

import "time"

// ImportProgress reports one catalog worker's cumulative snapshot for an
// import job: the totals that worker has processed, retried and
// dead-lettered so far. Workers count independently because Kafka
// assigns partitions per worker; the importer folds each worker's
// snapshot monotonically and derives the job totals as the sum across
// workers. Snapshot semantics make duplicate or replayed reports converge
// instead of double-counting.
type ImportProgress struct {
	JobID         string    `json:"job_id"`
	WorkerID      string    `json:"worker_id"`
	ProcessedRows int64     `json:"processed_rows"`
	RetriedRows   int64     `json:"retried_rows"`
	DeadRows      int64     `json:"dead_rows"`
	ReportedAt    time.Time `json:"reported_at"`

	LatencySamples int     `json:"latency_samples,omitempty"`
	LatencyP50Ms   float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms   float64 `json:"latency_p95_ms,omitempty"`
	LatencyMaxMs   float64 `json:"latency_max_ms,omitempty"`
}
