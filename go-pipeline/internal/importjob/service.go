package importjob

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// maxImportDuration bounds a single import's processing time so an
// orphaned file handle or a stalled stream cannot hold resources forever.
const maxImportDuration = 30 * time.Minute

// ImportService coordinates one CSV import: it streams and validates the
// file, publishes every valid row as a product event, and tracks progress
// in the job store.
type ImportService struct {
	store      JobStore
	publisher  EventPublisher
	streamer   *Streamer
	flushEvery int64
}

// NewImportService wires the service. flushEvery controls how many rows are
// processed between progress writes; batching these writes keeps row
// counters from becoming a write hotspot on large imports.
func NewImportService(store JobStore, publisher EventPublisher, flushEvery int64) *ImportService {
	if flushEvery < 1 {
		flushEvery = 1
	}
	return &ImportService{
		store:      store,
		publisher:  publisher,
		streamer:   NewStreamer(),
		flushEvery: flushEvery,
	}
}

// StartImport registers the job and processes the file at filePath in the
// background. It returns as soon as the job row exists so the HTTP request
// can answer immediately with the job id; live progress is observable via
// the job status and event endpoints. The service owns filePath and removes
// it when processing ends, successfully or not.
func (s *ImportService) StartImport(ctx context.Context, filePath, fileName string, opts StreamOptions) (ImportJob, error) {
	job := ImportJob{
		FileName: filepath.Base(fileName),
		Status:   StatusParsing,
	}
	if err := s.store.Create(ctx, &job); err != nil {
		return ImportJob{}, fmt.Errorf("creating import job: %w", err)
	}

	go s.process(job.ID, filePath, opts)
	return job, nil
}

// process streams the file, publishes valid rows and keeps the job's
// progress current, then applies the terminal status. processed_rows is
// advanced by the catalog worker's progress events once a worker is
// deployed; until then completion reflects the importer's own work.
func (s *ImportService) process(jobID, filePath string, opts StreamOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), maxImportDuration)
	defer cancel()
	defer os.Remove(filePath)

	file, err := os.Open(filePath)
	if err != nil {
		s.fail(ctx, jobID, err)
		return
	}
	defer file.Close()

	var total, valid, invalid, published int64

	_, err = s.streamer.Stream(ctx, file, opts, func(row Row) error {
		total++
		if !row.Valid() {
			invalid++
			return nil
		}

		evt := events.ProductImported{
			JobID:      jobID,
			RowNum:     row.Num,
			ProducedAt: time.Now().UTC(),
			Product:    row.Product,
		}
		if err := s.publisher.PublishProductImported(ctx, evt); err != nil {
			return fmt.Errorf("publishing row %d: %w", row.Num, err)
		}
		valid++
		published++

		// A failed progress write means the job store is unavailable; the
		// import cannot be observed anymore and is failed rather than left
		// silently running.
		if published%s.flushEvery == 0 {
			return s.flushProgress(ctx, jobID, total, valid, invalid, published)
		}
		return nil
	})

	if err != nil {
		s.fail(ctx, jobID, err)
		return
	}
	if err := s.flushProgress(ctx, jobID, total, valid, invalid, published); err != nil {
		s.fail(ctx, jobID, err)
		return
	}
	if err := s.store.UpdateStatus(ctx, jobID, StatusCompleted, nil); err != nil {
		log.Printf("import %s: marking completed failed: %v", jobID, err)
	}
}

// flushProgress persists the running counters.
func (s *ImportService) flushProgress(ctx context.Context, jobID string, total, valid, invalid, published int64) error {
	return s.store.UpdateProgress(ctx, jobID, Progress{
		TotalRows:     total,
		ValidRows:     valid,
		InvalidRows:   invalid,
		PublishedRows: published,
	})
}

// fail records the terminal failure cause on the job.
func (s *ImportService) fail(ctx context.Context, jobID string, cause error) {
	msg := cause.Error()
	if err := s.store.UpdateStatus(ctx, jobID, StatusFailed, &msg); err != nil {
		log.Printf("import %s: marking failed failed: %v (cause: %v)", jobID, err, cause)
	}
}
