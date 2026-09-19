package importjob

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// memStore is an in-memory JobStore for tests.
type memStore struct {
	mu   sync.Mutex
	jobs map[string]*ImportJob
}

func newMemStore() *memStore { return &memStore{jobs: map[string]*ImportJob{}} }

func (m *memStore) put(j *ImportJob) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
}

func (m *memStore) Create(_ context.Context, job *ImportJob) error {
	if job.ID == "" {
		job.ID = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	now := time.Now().UTC()
	job.CreatedAt, job.UpdatedAt = now, now
	m.put(job)
	return nil
}

func (m *memStore) Get(_ context.Context, id string) (ImportJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return ImportJob{}, ErrNotFound
	}
	return *j, nil
}

func (m *memStore) List(_ context.Context, limit int) ([]ImportJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ImportJob, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, *j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CreatedAt.After(out[k].CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *memStore) UpdateProgress(_ context.Context, id string, p Progress) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return ErrNotFound
	}
	j.TotalRows, j.ValidRows, j.InvalidRows, j.PublishedRows = p.TotalRows, p.ValidRows, p.InvalidRows, p.PublishedRows
	j.UpdatedAt = time.Now().UTC()
	return nil
}

func (m *memStore) UpdateStatus(_ context.Context, id string, to Status, lastErr *string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return ErrNotFound
	}
	j.Status, j.LastError, j.UpdatedAt = to, lastErr, time.Now().UTC()
	return nil
}

func (m *memStore) FailStale(_ context.Context, reason string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, j := range m.jobs {
		if !j.Status.Terminal() {
			j.Status = StatusFailed
			r := reason
			j.LastError = &r
			n++
		}
	}
	return n, nil
}

func waitTerminal(t *testing.T, store *memStore, id string, timeout time.Duration) ImportJob {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := store.Get(context.Background(), id)
		if err == nil && j.Status.Terminal() {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach a terminal status within %v", id, timeout)
	return ImportJob{}
}

func TestImportServiceCompletes(t *testing.T) {
	t.Parallel()

	csvData := "name;price;expiration\n" +
		"Calypso - Lemonade #(4026987913289674);$115.55;1/11/2023\n" +
		"Cheese - Grana Padano #(3566971102136738);$163.88;1/14/2023\n" +
		"Veal - Loin #(5552033378109898);$72.60;12/16/2022\n" +
		"Broken Item;not-a-price;\n"

	dir := t.TempDir()
	path := filepath.Join(dir, "products.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	store := newMemStore()
	svc := NewImportService(store, NoopPublisher{}, 1) // flush every row

	job, err := svc.StartImport(context.Background(), path, "products.csv", StreamOptions{
		DefaultMerchantID: "default",
		DefaultCurrency:   "USD",
	})
	if err != nil {
		t.Fatalf("StartImport: %v", err)
	}
	if job.ID == "" || job.Status != StatusParsing {
		t.Fatalf("job = %+v, want an id and parsing status", job)
	}

	final := waitTerminal(t, store, job.ID, 5*time.Second)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s, lastError = %v", final.Status, final.LastError)
	}
	if final.TotalRows != 4 || final.ValidRows != 3 || final.InvalidRows != 1 || final.PublishedRows != 3 {
		t.Fatalf("final = %+v, want 4 total / 3 valid / 1 invalid / 3 published", final)
	}

	// The service owns the file and removes it when processing ends.
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temp file still present: %v", err)
	}
}

func TestImportServiceFailsOnMissingFile(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	svc := NewImportService(store, NoopPublisher{}, 1)

	job, err := svc.StartImport(context.Background(), filepath.Join(t.TempDir(), "missing.csv"), "missing.csv", StreamOptions{})
	if err != nil {
		t.Fatalf("StartImport: %v", err)
	}

	final := waitTerminal(t, store, job.ID, 5*time.Second)
	if final.Status != StatusFailed || final.LastError == nil || *final.LastError == "" {
		t.Fatalf("final = %+v, want failed with a cause", final)
	}
}

func TestFailStale(t *testing.T) {
	t.Parallel()

	store := newMemStore()
	store.put(&ImportJob{ID: "running", Status: StatusParsing})
	store.put(&ImportJob{ID: "done", Status: StatusCompleted})

	n, err := store.FailStale(context.Background(), "importer restarted")
	if err != nil {
		t.Fatalf("FailStale: %v", err)
	}
	if n != 1 {
		t.Fatalf("marked %d jobs, want 1", n)
	}
	running, _ := store.Get(context.Background(), "running")
	if running.Status != StatusFailed || running.LastError == nil {
		t.Fatalf("running = %+v, want failed with cause", running)
	}
}

func TestStatusTransitionRules(t *testing.T) {
	t.Parallel()

	valid := []struct{ from, to Status }{
		{StatusParsing, StatusCompleted},
		{StatusParsing, StatusPublishing},
		{StatusPublishing, StatusProcessing},
		{StatusProcessing, StatusCompleted},
		{StatusParsing, StatusFailed},
	}
	for _, tc := range valid {
		j := ImportJob{Status: tc.from}
		if err := j.Transition(tc.to, nil); err != nil {
			t.Errorf("transition %s -> %s rejected: %v", tc.from, tc.to, err)
		}
	}

	invalid := []struct{ from, to Status }{
		{StatusCompleted, StatusParsing},
		{StatusFailed, StatusParsing},
		{StatusParsing, StatusProcessing},
	}
	for _, tc := range invalid {
		j := ImportJob{Status: tc.from}
		if err := j.Transition(tc.to, nil); err == nil {
			t.Errorf("transition %s -> %s must be rejected", tc.from, tc.to)
		}
	}
}

func TestExternalStatusMapping(t *testing.T) {
	t.Parallel()

	cases := map[Status]string{
		StatusUploading:  "pending",
		StatusParsing:    "processing",
		StatusPublishing: "processing",
		StatusProcessing: "processing",
		StatusCompleted:  "completed",
		StatusFailed:     "failed",
	}
	for in, want := range cases {
		if got := externalStatus(in); got != want {
			t.Errorf("externalStatus(%s) = %q, want %q", in, got, want)
		}
	}
}

// fakePublisher records published events for assertions and counts flushes.
type fakePublisher struct {
	mu      sync.Mutex
	events  []events.ProductImported
	flushes int
}

func (f *fakePublisher) PublishProductImported(_ context.Context, evt events.ProductImported) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, evt)
	return nil
}

func (f *fakePublisher) Flush(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flushes++
	return nil
}

func (f *fakePublisher) Close() {}

func (f *fakePublisher) snapshot() ([]events.ProductImported, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]events.ProductImported(nil), f.events...), f.flushes
}

func TestImportServicePublishesValidRows(t *testing.T) {
	t.Parallel()

	csvData := "name;price;expiration\n" +
		"Calypso - Lemonade #(4026987913289674);$115.55;1/11/2023\n" +
		"Broken Item;not-a-price;\n" +
		"Veal - Loin #(5552033378109898);$72.60;12/16/2022\n"

	dir := t.TempDir()
	path := filepath.Join(dir, "products.csv")
	if err := os.WriteFile(path, []byte(csvData), 0o600); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}

	store := newMemStore()
	pub := &fakePublisher{}
	svc := NewImportService(store, pub, 1)

	job, err := svc.StartImport(context.Background(), path, "products.csv", StreamOptions{
		DefaultMerchantID: "default",
		DefaultCurrency:   "USD",
	})
	if err != nil {
		t.Fatalf("StartImport: %v", err)
	}

	final := waitTerminal(t, store, job.ID, 5*time.Second)
	if final.Status != StatusCompleted {
		t.Fatalf("status = %s, lastError = %v", final.Status, final.LastError)
	}

	published, flushes := pub.snapshot()
	if len(published) != 2 {
		t.Fatalf("published %d events, want the 2 valid rows", len(published))
	}
	if flushes == 0 {
		t.Fatal("completion must follow a publisher flush")
	}

	first, second := published[0], published[1]
	if first.JobID != job.ID || second.JobID != job.ID {
		t.Fatalf("event job ids = %q, %q; want %q", first.JobID, second.JobID, job.ID)
	}
	if first.RowNum != 2 || second.RowNum != 4 {
		t.Fatalf("event row nums = %d, %d; want 2 and 4 (file rows; the header is 1)",
			first.RowNum, second.RowNum)
	}
	if first.Product.ProductID != "4026987913289674" || second.Product.ProductID != "5552033378109898" {
		t.Fatalf("event product ids = %q, %q", first.Product.ProductID, second.Product.ProductID)
	}
	if first.Product.PriceCents != 11555 || second.Product.PriceCents != 7260 {
		t.Fatalf("event prices = %d, %d", first.Product.PriceCents, second.Product.PriceCents)
	}
}
