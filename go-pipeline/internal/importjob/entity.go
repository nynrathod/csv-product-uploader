package importjob

import (
	"fmt"
	"time"
)

// Status is the import job lifecycle state. The pipeline advances a job
// from parsing through publishing to processing; processing ends once the
// catalog worker confirms consumption of the published events. When no
// event broker is configured the job completes directly after parsing.
type Status string

const (
	StatusUploading  Status = "uploading"
	StatusParsing    Status = "parsing"
	StatusPublishing Status = "publishing"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

// Terminal reports whether the status admits no further transitions.
func (s Status) Terminal() bool { return s == StatusCompleted || s == StatusFailed }

// allowedTransitions defines the lifecycle graph.
var allowedTransitions = map[Status][]Status{
	StatusUploading:  {StatusParsing},
	StatusParsing:    {StatusPublishing, StatusCompleted, StatusFailed},
	StatusPublishing: {StatusProcessing, StatusFailed},
	StatusProcessing: {StatusCompleted, StatusFailed},
	StatusCompleted:  {},
	StatusFailed:     {},
}

// ImportJob is the importer's own record of one CSV upload: what was
// received, how far it streamed, and how many rows reached the event
// stream. The importer owns this data; the catalog worker learns about an
// import only through the events it consumes.
type ImportJob struct {
	ID            string
	FileName      string
	Status        Status
	TotalRows     int64
	ValidRows     int64
	InvalidRows   int64
	PublishedRows int64
	ProcessedRows int64
	RetriedRows   int64
	DeadRows      int64
	LastError     *string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Progress is a point-in-time counter snapshot of a running import.
type Progress struct {
	TotalRows     int64
	ValidRows     int64
	InvalidRows   int64
	PublishedRows int64
}

// Transition validates and applies a lifecycle change in place.
func (j *ImportJob) Transition(to Status, lastErr *string) error {
	for _, next := range allowedTransitions[j.Status] {
		if next == to {
			j.Status = to
			j.LastError = lastErr
			return nil
		}
	}
	return fmt.Errorf("invalid status transition %s -> %s", j.Status, to)
}
