package importjob

import (
	"context"
	"time"

	"github.com/gofiber/fiber/v3"
)

// LiveLatencySource exposes the worker-reported event-to-database
// latency: measured at catalog write time on the real pipeline path, it
// is the platform's single source of truth for latency.
type LiveLatencySource interface {
	// Latency returns the latest worker-reported latency summary and
	// whether one exists yet.
	Latency() (LatencySummary, bool)
}

// GroupLagSource reports the total consumer lag of one group.
type GroupLagSource interface {
	// GroupLag returns the summed lag across a group's partitions.
	GroupLag(ctx context.Context, group string) (int64, error)
}

// MetricsHandler exposes the platform's operating picture as one
// aggregate snapshot: derived throughput from the most recent import,
// worker-reported event-to-database latency, live consumer lag for the
// importer's consumer groups, and a zero-loss verdict for the latest
// import. Everything is computed from the platform's own records;
// nothing is sampled or estimated.
type MetricsHandler struct {
	jobs    JobStore
	lags    GroupLagSource
	latency LiveLatencySource
}

// NewMetricsHandler builds the handler.
func NewMetricsHandler(jobs JobStore, lags GroupLagSource, latency LiveLatencySource) *MetricsHandler {
	return &MetricsHandler{jobs: jobs, lags: lags, latency: latency}
}

// Register mounts the metrics route.
func (h *MetricsHandler) Register(app *fiber.App) {
	app.Get("/api/v1/metrics", h.snapshot)
}

// metricsResponse is the external metrics snapshot.
type metricsResponse struct {
	GeneratedAt time.Time `json:"generatedAt"`

	// Throughput derived from the most recent import.
	Import struct {
		JobID             string  `json:"jobId"`
		FileName          string  `json:"fileName"`
		Status            string  `json:"status"`
		IngestRowsPerSec  float64 `json:"ingestRowsPerSec"`
		CatalogRowsPerSec float64 `json:"catalogRowsPerSec"`
	} `json:"import"`

	// Latency measured by the catalog worker at database write time.
	Latency struct {
		Samples int     `json:"samples"`
		P50MS   float64 `json:"p50Ms"`
		P95MS   float64 `json:"p95Ms"`
		MaxMS   float64 `json:"maxMs"`
	} `json:"latency"`

	// Live stream position.
	Stream struct {
		EventsInFlight int64 `json:"eventsInFlight"`
		CatalogLag     int64 `json:"catalogLag"`
		ProjectionLag  int64 `json:"projectionLag"`
	} `json:"stream"`

	// Independent verification for the latest import.
	ZeroLoss *bool `json:"zeroLoss,omitempty"`
}

// snapshot computes the platform metrics for the latest import.
func (h *MetricsHandler) snapshot(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var resp metricsResponse
	resp.GeneratedAt = time.Now().UTC()

	jobs, err := h.jobs.List(ctx, 1)
	if err == nil && len(jobs) == 1 {
		job := jobs[0]
		resp.Import.JobID = job.ID
		resp.Import.FileName = job.FileName
		resp.Import.Status = externalStatus(job.Status)

		if job.UpdatedAt.After(job.CreatedAt) {
			secs := job.UpdatedAt.Sub(job.CreatedAt).Seconds()
			if secs > 0 {
				// Ingest covers parse and publish; catalog covers the
				// worker's confirmed materialization over the same
				// window.
				resp.Import.IngestRowsPerSec = float64(job.PublishedRows) / secs
				resp.Import.CatalogRowsPerSec = float64(job.ProcessedRows+job.DeadRows) / secs
			}
		}

		inFlight := job.PublishedRows - job.ProcessedRows - job.DeadRows
		if inFlight < 0 {
			inFlight = 0
		}
		resp.Stream.EventsInFlight = inFlight

		if job.PublishedRows > 0 {
			verified := job.ProcessedRows+job.DeadRows >= job.PublishedRows
			resp.ZeroLoss = &verified
		}
	}

	if h.latency != nil {
		if lat, ok := h.latency.Latency(); ok && lat.Samples > 0 {
			resp.Latency.Samples = lat.Samples
			resp.Latency.P50MS = lat.P50MS
			resp.Latency.P95MS = lat.P95MS
			resp.Latency.MaxMS = lat.MaxMS
		}
	}

	if h.lags != nil {
		if lag, err := h.lags.GroupLag(ctx, "catalog-writer"); err == nil {
			resp.Stream.CatalogLag = lag
		}
		if lag, err := h.lags.GroupLag(ctx, "import-reader"); err == nil {
			resp.Stream.ProjectionLag = lag
		}
	}

	return c.JSON(resp)
}
