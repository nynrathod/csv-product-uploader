package importjob

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
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

// process streams the file, publishes every valid row as a product event,
// and keeps the job's progress current, then applies the terminal status.
// Completion is a durability guarantee: the event stream is flushed and
// broker-confirmed before the job is marked completed. processed_rows is
// advanced by the catalog worker's progress events as consumption proceeds.
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

	// Flush before completing: a nil flush means every published event is
	// confirmed by the broker's in-sync replicas, not merely buffered.
	if err := s.publisher.Flush(ctx); err != nil {
		s.fail(ctx, jobID, fmt.Errorf("delivering published events: %w", err))
		return
	}
	if err := s.flushProgress(ctx, jobID, total, valid, invalid, published); err != nil {
		s.fail(ctx, jobID, err)
		return
	}

	// An import with nothing to materialize is complete the moment
	// publishing ends. Otherwise the importer awaits the catalog worker's
	// confirmation: the progress tracker folds the worker's reports into
	// this job, and the await loop below derives completion from the
	// folded counters. Evaluating completion here, against the final
	// published count, cannot race intermediate progress writes.
	if published == 0 {
		if err := s.store.UpdateStatus(ctx, jobID, StatusCompleted, nil); err != nil {
			log.Printf("import %s: marking completed failed: %v", jobID, err)
		}
		return
	}
	if err := s.store.UpdateStatus(ctx, jobID, StatusProcessing, nil); err != nil {
		s.fail(ctx, jobID, err)
		return
	}
	s.awaitConfirmation(ctx, jobID)

}

// confirmationPollInterval is how often the importer re-evaluates catalog
// confirmation while a job awaits it.
const confirmationPollInterval = 250 * time.Millisecond

// confirmationTimeout bounds how long a job may sit without any progress
// activity before the import is failed as unconfirmed.
const confirmationTimeout = 10 * time.Minute

// awaitConfirmation polls the job's folded counters until the catalog has
// confirmed every published event as processed or dead-lettered, the
// confirmation budget expires, or a terminal decision lands elsewhere.
// Completion is derived by the importer, the owner of the job record,
// from the catalog worker's reported progress.
func (s *ImportService) awaitConfirmation(ctx context.Context, jobID string) {
	ticker := time.NewTicker(confirmationPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.fail(ctx, jobID, fmt.Errorf("waiting for catalog confirmation: %w", ctx.Err()))
			return
		case <-ticker.C:
		}

		job, err := s.store.Get(ctx, jobID)
		if err != nil {
			log.Printf("import %s: reading confirmation state failed: %v", jobID, err)
			continue
		}
		if job.Status != StatusProcessing {
			return
		}
		if job.ProcessedRows+job.DeadRows >= job.PublishedRows {
			if err := s.store.UpdateStatus(ctx, jobID, StatusCompleted, nil); err != nil {
				log.Printf("import %s: marking completed failed: %v", jobID, err)
			}
			return
		}
		if time.Since(job.UpdatedAt) > confirmationTimeout {
			s.fail(ctx, jobID, errors.New("timed out waiting for catalog confirmation"))
			return
		}
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
