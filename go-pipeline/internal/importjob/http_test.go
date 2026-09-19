package importjob

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func newTestApp(store *memStore, flushEvery int64) *fiber.App {
	svc := NewImportService(store, NoopPublisher{}, flushEvery)
	app := fiber.New(fiber.Config{
		BodyLimit: 1 << 20,
	})
	NewHTTPHandler(svc, store, HandlerConfig{
		SSEPollInterval: time.Millisecond,
		SSEMaxDuration:  time.Second,
		MaxUploadBytes:  1 << 20,
	}).Register(app)
	return app
}

func TestCreateImportFlow(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	app := newTestApp(store, 1)

	csvData := "name;price;expiration\n" +
		"Calypso - Lemonade #(4026987913289674);$115.55;1/11/2023\n" +
		"Cheese - Grana Padano #(3566971102136738);$163.88;1/14/2023\n" +
		"Broken Item;not-a-price;\n"

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "products.csv")
	if err != nil {
		t.Fatalf("form file: %v", err)
	}
	if _, err := fw.Write([]byte(csvData)); err != nil {
		t.Fatalf("write body: %v", err)
	}
	if err := mw.WriteField("merchant_id", "m-7"); err != nil {
		t.Fatalf("write field: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/imports", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 202; response body: %s", resp.StatusCode, body)
	}

	var created jobResponse
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if created.ID == "" || created.Status != "processing" {
		t.Fatalf("created = %+v", created)
	}

	waitForJob(t, store, created.ID, func(j ImportJob) bool {
		return j.Status == StatusProcessing && j.PublishedRows == 2
	}, 5*time.Second)

	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/imports/"+created.ID, nil)
	getResp, err := app.Test(getReq)
	if err != nil {
		t.Fatalf("app.Test get: %v", err)
	}
	var got jobResponse
	if err := json.NewDecoder(getResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode job: %v", err)
	}
	// The importer's work is done; the catalog's confirmation is pending.
	if got.Status != "processing" {
		t.Fatalf("status = %q, want processing", got.Status)
	}
	if got.TotalRows != 3 || got.ValidRows != 2 || got.InvalidRows != 1 || got.PublishedRows != 2 {
		t.Fatalf("job = %+v, want 3 total / 2 valid / 1 invalid / 2 published", got)
	}
}

func TestCreateImportRejectsMissingFile(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	app := newTestApp(store, 1)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/imports", strings.NewReader("junk"))
	req.Header.Set("Content-Type", "text/plain")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestGetImportNotFound(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	app := newTestApp(store, 1)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/imports/00000000-0000-0000-0000-000000000000", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestSSEStreamCompletes(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	store.put(&ImportJob{
		ID: "j1", FileName: "x.csv", Status: StatusCompleted,
		TotalRows: 2, ValidRows: 2, PublishedRows: 2,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})

	h := NewHTTPHandler(nil, store, HandlerConfig{
		SSEPollInterval: time.Millisecond,
		SSEMaxDuration:  time.Second,
	})

	pr, pw := io.Pipe()
	go h.pumpEvents(pw, "j1")

	data, err := io.ReadAll(pr)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	out := string(data)
	if !strings.Contains(out, "event: progress") {
		t.Fatalf("stream missing progress events:\n%s", out)
	}
	if !strings.Contains(out, "event: done") {
		t.Fatalf("stream missing done event:\n%s", out)
	}
	if !strings.Contains(out, `"status":"completed"`) {
		t.Fatalf("stream missing completed status:\n%s", out)
	}
	if !strings.Contains(out, `"totalRows":2`) {
		t.Fatalf("stream missing counts:\n%s", out)
	}
}

func TestReportEndpoint(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	store.put(&ImportJob{
		ID: "r-1", FileName: "x.csv", Status: StatusCompleted,
		TotalRows: 10, ValidRows: 8, InvalidRows: 2,
		PublishedRows: 8, ProcessedRows: 8,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	})

	app := newTestApp(store, 1)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/imports/r-1/report", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}

	var rep struct {
		Status string `json:"status"`
		Rows   struct {
			Total   int64 `json:"total"`
			Valid   int64 `json:"valid"`
			Invalid int64 `json:"invalid"`
		} `json:"rows"`
		Reconciliation struct {
			Published   int64  `json:"published"`
			Confirmed   int64  `json:"confirmed"`
			Unconfirmed int64  `json:"unconfirmed"`
			State       string `json:"state"`
		} `json:"reconciliation"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rep); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	if rep.Status != "completed" || rep.Rows.Total != 10 || rep.Rows.Invalid != 2 {
		t.Fatalf("report = %+v", rep)
	}
	if rep.Reconciliation.State != "complete" || rep.Reconciliation.Unconfirmed != 0 ||
		rep.Reconciliation.Confirmed != 8 || rep.Reconciliation.Published != 8 {
		t.Fatalf("reconciliation = %+v, want complete at 8/8", rep.Reconciliation)
	}
}
