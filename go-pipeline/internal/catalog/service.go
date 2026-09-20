package catalog

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProcessingLimits bound one processing cycle.
type ProcessingLimits struct {
	// BatchSize is how many events one ProcessBatch call adjudicates.
	BatchSize int
}

// jobEntry tracks one import job's cumulative outcome counters and the
// totals most recently published to the progress topic.
type jobEntry struct {
	applied, retried, dead          int64
	pubApplied, pubRetried, pubDead int64
	published                       bool
}

// jobIDPattern recovers the import job identity from a record's raw bytes
// even when the payload as a whole cannot be decoded, so undecodable
// records still count toward their import's ledger.
var jobIDPattern = regexp.MustCompile(`"job_id"\s*:\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"`)

// writeRetryAttempts bounds in-place batch write retries. Retrying in
// place is required for correctness: the consumer client does not
// redeliver polled records within a session, so a failed batch must be
// retried before its offsets may commit.
const writeRetryAttempts = 3

// Service processes consumed product events into the catalog.
type Service struct {
	writer   ProductWriter
	policy   RetryPolicy
	retrier  EventRepublisher
	reporter ProgressReporter
	limits   ProcessingLimits

	mu   sync.Mutex
	jobs map[string]*jobEntry
}

// NewService wires the processing service, applying safe defaults.
func NewService(writer ProductWriter, policy RetryPolicy, retrier EventRepublisher, reporter ProgressReporter, limits ProcessingLimits) *Service {
	if limits.BatchSize <= 0 {
		limits.BatchSize = 500
	}
	return &Service{
		writer:   writer,
		policy:   policy,
		retrier:  retrier,
		reporter: reporter,
		limits:   limits,
		jobs:     make(map[string]*jobEntry),
	}
}

// ProcessBatch adjudicates one consumed batch. Every event either commits
// to the catalog or is dead-lettered; a batch returns nil only when fully
// adjudicated, which is what permits committing its source offsets: the
// commit-after-write rule that, combined with idempotent upserts, yields
// effectively-once materialization despite at-least-once delivery.
func (s *Service) ProcessBatch(ctx context.Context, evts []Event) error {
	if len(evts) == 0 {
		return nil
	}

	// Validation happens before any write: a malformed event is
	// dead-lettered without a database round trip and can never fail the
	// shared batch statement.
	products := make([]Product, 0, len(evts))
	validEvts := make([]Event, 0, len(evts))
	var invalid []Event
	for _, evt := range evts {
		p, err := FromEvent(evt.ProductImported)
		if err != nil {
			invalid = append(invalid, evt)
			continue
		}
		products = append(products, p)
		validEvts = append(validEvts, evt)
	}
	for _, evt := range invalid {
		if err := s.retrier.RepublishDead(ctx, evt.ProductImported, fmt.Errorf("catalog validation rejected the event")); err != nil {
			return fmt.Errorf("dead-lettering invalid event row %d: %w", evt.RowNum, err)
		}
	}
	if len(products) == 0 {
		s.mu.Lock()
		for _, evt := range invalid {
			s.entryLocked(evt.JobID).dead++
		}
		s.mu.Unlock()
		return nil
	}

	// The whole batch is one statement; failures retry in place with
	// backoff because uncommitted polled records are never redelivered
	// within a session.
	var writeErr error
	for attempt := 0; attempt < writeRetryAttempts; attempt++ {
		writeErr = s.writer.UpsertBatch(ctx, products)
		if writeErr == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
		}
	}
	if writeErr != nil {
		return fmt.Errorf("catalog write failed after %d attempts: %w", writeRetryAttempts, writeErr)
	}

	s.mu.Lock()
	for _, evt := range validEvts {
		s.entryLocked(evt.JobID).applied++
	}
	for _, evt := range invalid {
		s.entryLocked(evt.JobID).dead++
	}
	s.mu.Unlock()
	return nil
}

// ProcessPoison dead-letters a record whose payload could not be decoded:
// retrying an undecodable event can never succeed, so it is rejected
// permanently while its offset still commits. The job identity is
// recovered from the raw bytes where possible so the import's ledger
// stays complete; the record is logged loudly because a poison payload is
// a producer contract violation worth investigating.
func (s *Service) ProcessPoison(ctx context.Context, rec *kgo.Record) error {
	jobID := ""
	if m := jobIDPattern.FindSubmatch(rec.Value); m != nil {
		jobID = string(m[1])
	}
	if jobID != "" {
		s.mu.Lock()
		s.entryLocked(jobID).dead++
		s.mu.Unlock()
	}
	log.Printf("dead-lettering undecodable record %s/%d@%d (job %s), first bytes: %.80q",
		rec.Topic, rec.Partition, rec.Offset, jobID, rec.Value)
	return s.retrier.RepublishDeadRaw(ctx, rec, fmt.Errorf("undecodable payload"))
}

// FlushProgress publishes cumulative per-job totals onto the progress
// topic. Totals are snapshots, not deltas: the importer folds them with a
// monotonic set, so duplicate or replayed reports converge instead of
// double-counting, and a failed publish loses nothing — the next flush
// republishes the same totals. Only entries whose totals changed since
// the last successful publish are resent.
func (s *Service) FlushProgress(ctx context.Context) error {
	type report struct {
		jobID                  string
		applied, retried, dead int64
	}
	var pending []report

	s.mu.Lock()
	for id, e := range s.jobs {
		if e.published && e.applied == e.pubApplied && e.retried == e.pubRetried && e.dead == e.pubDead {
			continue
		}
		pending = append(pending, report{id, e.applied, e.retried, e.dead})
	}
	s.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}

	for _, r := range pending {
		if err := s.reporter.ReportProgress(ctx, r.jobID, r.applied, r.retried, r.dead); err != nil {
			return fmt.Errorf("reporting progress for job %s: %w", r.jobID, err)
		}
	}
	if err := s.reporter.Flush(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	for _, r := range pending {
		if e, ok := s.jobs[r.jobID]; ok {
			e.pubApplied, e.pubRetried, e.pubDead = r.applied, r.retried, r.dead
			e.published = true
		}
	}
	s.mu.Unlock()
	return nil
}

// entryLocked returns the job's counter entry; the caller holds the lock.
func (s *Service) entryLocked(jobID string) *jobEntry {
	e, ok := s.jobs[jobID]
	if !ok {
		e = &jobEntry{}
		s.jobs[jobID] = e
	}
	return e
}
