package importjob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
)

// errUploadTooLarge reports an upload past the configured size limit.
var errUploadTooLarge = errors.New("upload exceeds the configured size limit")

// HandlerConfig carries the HTTP layer's operational limits.
type HandlerConfig struct {
	// UploadDir is where uploads are spooled; empty means the OS temp dir.
	UploadDir string
	// MaxUploadBytes bounds one uploaded file.
	MaxUploadBytes int64
	// SSEPollInterval is how often job progress is re-read while streaming.
	SSEPollInterval time.Duration
	// SSEMaxDuration bounds one progress stream.
	SSEMaxDuration time.Duration
}

// HTTPHandler exposes the importer's REST surface: upload, list, status and
// live progress.
type HTTPHandler struct {
	svc   *ImportService
	store JobStore
	cfg   HandlerConfig
}

// NewHTTPHandler builds the handler, applying safe defaults for unset
// limits.
func NewHTTPHandler(svc *ImportService, store JobStore, cfg HandlerConfig) *HTTPHandler {
	if cfg.SSEPollInterval <= 0 {
		cfg.SSEPollInterval = 500 * time.Millisecond
	}
	if cfg.SSEMaxDuration <= 0 {
		cfg.SSEMaxDuration = 15 * time.Minute
	}
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = 512 << 20
	}
	return &HTTPHandler{svc: svc, store: store, cfg: cfg}
}

// Register mounts all import routes.
func (h *HTTPHandler) Register(app *fiber.App) {
	app.Post("/api/v1/imports", h.createImport)
	app.Get("/api/v1/imports", h.listImports)
	app.Get("/api/v1/imports/:id", h.getImport)
	app.Get("/api/v1/imports/:id/events", h.streamEvents)
	app.Get("/api/v1/imports/:id/report", h.getReport)
}

// jobResponse is the external job representation. Field names and status
// values follow the upload UI contract (pending/processing/completed/
// failed), which stays stable while the internal lifecycle evolves.
type jobResponse struct {
	ID            string    `json:"id"`
	FileName      string    `json:"fileName"`
	Status        string    `json:"status"`
	TotalRows     int64     `json:"totalRows"`
	ValidRows     int64     `json:"validRows"`
	InvalidRows   int64     `json:"invalidRows"`
	PublishedRows int64     `json:"publishedRows"`
	ProcessedRows int64     `json:"processedRows"`
	RetriedRows   int64     `json:"retriedRows"`
	DeadRows      int64     `json:"deadRows"`
	LastError     *string   `json:"lastError,omitempty"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// reportResponse is the external reconciliation report for one import.
type reportResponse struct {
	JobID          string         `json:"jobId"`
	FileName       string         `json:"fileName"`
	Status         string         `json:"status"`
	Rows           rowLedger      `json:"rows"`
	Events         eventLedger    `json:"events"`
	Reconciliation reconciliation `json:"reconciliation"`
	GeneratedAt    time.Time      `json:"generatedAt"`
}

// rowLedger is the parsing outcome of the uploaded file.
type rowLedger struct {
	Total   int64 `json:"total"`
	Valid   int64 `json:"valid"`
	Invalid int64 `json:"invalid"`
}

// eventLedger is the event-stream outcome of the import.
type eventLedger struct {
	Published int64 `json:"published"`
	Processed int64 `json:"processed"`
	Retried   int64 `json:"retried"`
	Dead      int64 `json:"dead"`
}

// reconciliation compares what was published with what the catalog
// confirmed. Confirmed counts processed and dead-lettered events; anything
// published but unconfirmed is still in flight.
type reconciliation struct {
	Published   int64  `json:"published"`
	Confirmed   int64  `json:"confirmed"`
	Unconfirmed int64  `json:"unconfirmed"`
	State       string `json:"state"`
}

// externalStatus maps internal lifecycle states onto the four states the
// upload UI contract defines.
func externalStatus(s Status) string {
	switch s {
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusUploading:
		return "pending"
	default:
		return "processing"
	}
}

func toResponse(j ImportJob) jobResponse {
	return jobResponse{
		ID:            j.ID,
		FileName:      j.FileName,
		Status:        externalStatus(j.Status),
		TotalRows:     j.TotalRows,
		ValidRows:     j.ValidRows,
		InvalidRows:   j.InvalidRows,
		PublishedRows: j.PublishedRows,
		ProcessedRows: j.ProcessedRows,
		RetriedRows:   j.RetriedRows,
		DeadRows:      j.DeadRows,
		LastError:     j.LastError,
		CreatedAt:     j.CreatedAt,
		UpdatedAt:     j.UpdatedAt,
	}
}

// createImport receives a multipart upload, spools the file part to disk
// with bounded memory, registers the import job and answers 202 with the
// job id. The file is streamed from the socket part by part; it is never
// held in RAM.
func (h *HTTPHandler) createImport(c fiber.Ctx) error {
	mediaType, params, err := mime.ParseMediaType(c.Get(fiber.HeaderContentType))
	if err != nil || mediaType != "multipart/form-data" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "content type must be multipart/form-data",
		})
	}
	body := requestBody(c)
	if body == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "request body is required",
		})
	}

	var (
		tmpPath  string
		fileName string
		merchant string
	)
	reader := multipart.NewReader(body, params["boundary"])
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			h.cleanup(tmpPath)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "malformed multipart body",
			})
		}

		switch name := part.FormName(); {
		case name == "file" && part.FileName() != "":
			if tmpPath != "" {
				part.Close()
				h.cleanup(tmpPath)
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "only one file part is allowed",
				})
			}
			path, err := h.savePart(part)
			if err != nil {
				return h.uploadError(c, err)
			}
			tmpPath, fileName = path, filepath.Base(part.FileName())
			part.Close()
		case name == "merchant_id":
			merchant = strings.TrimSpace(readField(part, 256))
		default:
			// Unknown parts are drained so the multipart stream keeps
			// advancing.
			_, _ = io.Copy(io.Discard, part)
			part.Close()
		}
	}

	if tmpPath == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "file part is required",
		})
	}

	opts := StreamOptions{
		DefaultMerchantID: defaultMerchant(merchant),
		DefaultCurrency:   DefaultCurrency,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	job, err := h.svc.StartImport(ctx, tmpPath, fileName, opts)
	if err != nil {
		h.cleanup(tmpPath)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "starting the import failed",
		})
	}

	return c.Status(fiber.StatusAccepted).JSON(toResponse(job))
}

// savePart streams one file part to a temp file and enforces the size
// limit.
func (h *HTTPHandler) savePart(part io.Reader) (string, error) {
	f, err := os.CreateTemp(h.cfg.UploadDir, "import-*.csv")
	if err != nil {
		return "", fmt.Errorf("creating temp file: %w", err)
	}
	path := f.Name()

	n, copyErr := io.Copy(f, part)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		os.Remove(path)
		return "", fmt.Errorf("storing upload: %w", errors.Join(copyErr, closeErr))
	}
	if n > h.cfg.MaxUploadBytes {
		os.Remove(path)
		return "", errUploadTooLarge
	}
	return path, nil
}

func (h *HTTPHandler) uploadError(c fiber.Ctx, err error) error {
	if errors.Is(err, errUploadTooLarge) {
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
		"error": "storing the uploaded file failed",
	})
}

func (h *HTTPHandler) cleanup(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}

// readField reads a small form field value, bounded so a runaway field
// cannot exhaust memory.
func readField(r io.Reader, limit int64) string {
	b, _ := io.ReadAll(io.LimitReader(r, limit))
	return string(b)
}

// requestBody exposes the upload body for multipart parsing. Production
// serves with body streaming enabled and delivers a live request stream;
// the test harness and deployments without body streaming deliver buffered
// bytes. Both shapes are supported so uploads behave identically either way.
func requestBody(c fiber.Ctx) io.Reader {
	if bs := c.Request().BodyStream(); bs != nil {
		return bs
	}
	if raw := c.Body(); len(raw) > 0 {
		return bytes.NewReader(raw)
	}
	return nil
}

func defaultMerchant(merchant string) string {
	if merchant == "" {
		return DefaultMerchantID
	}
	return merchant
}

// listImports returns the most recent import jobs, newest first.
func (h *HTTPHandler) listImports(c fiber.Ctx) error {
	limit := 20
	if v := c.Query("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobs, err := h.store.List(ctx, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "listing imports failed",
		})
	}
	out := make([]jobResponse, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toResponse(j))
	}
	return c.JSON(out)
}

// getImport returns the current state of one import job.
func (h *HTTPHandler) getImport(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := h.store.Get(ctx, c.Params("id"))
	if err != nil {
		return importNotFound(c, err)
	}
	return c.JSON(toResponse(job))
}

// streamEvents streams live job progress as server-sent events until the
// job reaches a terminal status. Progress events carry the full job
// snapshot; the final state is additionally announced as a done event.
func (h *HTTPHandler) streamEvents(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := h.store.Get(ctx, c.Params("id")); err != nil {
		return importNotFound(c, err)
	}

	c.Set(fiber.HeaderContentType, "text/event-stream")
	c.Set(fiber.HeaderCacheControl, "no-cache")
	// Reverse proxies are told not to buffer the stream.
	c.Set("X-Accel-Buffering", "no")

	pr, pw := io.Pipe()
	go h.pumpEvents(pw, c.Params("id"))
	return c.SendStream(pr)
}

// pumpEvents polls the job store and writes server-sent events until the
// job reaches a terminal status, the client disconnects (pipe writes fail)
// or the maximum stream duration elapses.
func (h *HTTPHandler) pumpEvents(pw *io.PipeWriter, jobID string) {
	defer pw.Close()

	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.SSEMaxDuration)
	defer cancel()

	ticker := time.NewTicker(h.cfg.SSEPollInterval)
	defer ticker.Stop()

	for {
		job, err := h.store.Get(ctx, jobID)
		if err != nil {
			_ = writeSSE(pw, "error", fiber.Map{"error": "import job is no longer available"})
			return
		}
		if err := writeSSE(pw, "progress", toResponse(job)); err != nil {
			return
		}
		if job.Status.Terminal() {
			_ = writeSSE(pw, "done", toResponse(job))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func writeSSE(w io.Writer, event string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encoding sse payload: %w", err)
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data); err != nil {
		return fmt.Errorf("writing sse event: %w", err)
	}
	return nil
}

func importNotFound(c fiber.Ctx, err error) error {
	if errors.Is(err, ErrNotFound) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "import job not found"})
	}
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "loading the import job failed"})
}

// getReport returns the reconciliation report for one import.
func (h *HTTPHandler) getReport(c fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := h.store.Get(ctx, c.Params("id"))
	if err != nil {
		return importNotFound(c, err)
	}
	return c.JSON(buildReport(job))
}

// buildReport renders the reconciliation report for a job.
func buildReport(j ImportJob) reportResponse {
	confirmed := j.ProcessedRows + j.DeadRows
	unconfirmed := j.PublishedRows - confirmed

	state := "in-flight"
	switch {
	case j.Status == StatusFailed:
		state = "failed"
	case unconfirmed < 0:
		state = "discrepancy"
	case j.Status == StatusCompleted:
		if unconfirmed == 0 {
			state = "complete"
		} else {
			state = "discrepancy"
		}
	}

	return reportResponse{
		JobID:    j.ID,
		FileName: j.FileName,
		Status:   externalStatus(j.Status),
		Rows:     rowLedger{Total: j.TotalRows, Valid: j.ValidRows, Invalid: j.InvalidRows},
		Events: eventLedger{
			Published: j.PublishedRows, Processed: j.ProcessedRows,
			Retried: j.RetriedRows, Dead: j.DeadRows,
		},
		Reconciliation: reconciliation{
			Published: j.PublishedRows, Confirmed: confirmed,
			Unconfirmed: unconfirmed, State: state,
		},
		GeneratedAt: time.Now().UTC(),
	}
}
