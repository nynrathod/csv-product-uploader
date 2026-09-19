package events

import "time"

// ImportProgress reports how much of an import the catalog has processed.
// The worker publishes it; the importer's own consumer folds these counts
// into its import jobs. Each event reports the counts observed since the
// worker's last report for that job.
type ImportProgress struct {
	JobID         string    `json:"job_id"`
	ProcessedRows int64     `json:"processed_rows"`
	RetriedRows   int64     `json:"retried_rows"`
	DeadRows      int64     `json:"dead_rows"`
	ReportedAt    time.Time `json:"reported_at"`
}
