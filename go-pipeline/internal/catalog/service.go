package catalog

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProcessingLimits bound one processing cycle.
type ProcessingLimits struct {
	// BatchSize is how many events one ProcessBatch call adjudicates.
	BatchSize int
}

// Service processes consumed product events into the catalog.
type Service struct {
	writer   ProductWriter
	policy   RetryPolicy
	retrier  EventRepublisher
	reporter ProgressReporter
	limits   ProcessingLimits

	// Per-job counters accumulate between progress flushes; the maps key
	// by import job id and reset on every flush.
	mu           sync.Mutex
	appliedByJob map[string]int64
	retriedByJob map[string]int64
	deadByJob    map[string]int64
}

// NewService wires the processing service, applying safe defaults.
func NewService(writer ProductWriter, policy RetryPolicy, retrier EventRepublisher, reporter ProgressReporter, limits ProcessingLimits) *Service {
	if limits.BatchSize <= 0 {
		limits.BatchSize = 500
	}
	return &Service{
		writer:       writer,
		policy:       policy,
		retrier:      retrier,
		reporter:     reporter,
		limits:       limits,
		appliedByJob: make(map[string]int64),
		retriedByJob: make(map[string]int64),
		deadByJob:    make(map[string]int64),
	}
}

// ProcessBatch adjudicates one consumed batch. Every event either commits
// to the catalog, is republished to the retry topic, or is dead-lettered.
// It returns nil only when the batch is fully adjudicated, which is what
// permits the caller to commit the source offsets: the commit-after-write
// rule that, combined with idempotent upserts, yields effectively-once
// materialization despite at-least-once delivery.
func (s *Service) ProcessBatch(ctx context.Context, evts []Event) error {
	if len(evts) == 0 {
		return nil
	}

	products := make([]Product, 0, len(evts))
	for _, evt := range evts {
		products = append(products, FromEvent(evt.ProductImported))
	}

	// Fast path: one transactional batch upsert for the whole poll.
	if err := s.writer.UpsertBatch(ctx, products); err == nil {
		s.mu.Lock()
		for _, evt := range evts {
			s.appliedByJob[evt.JobID]++
		}
		s.mu.Unlock()
		return nil
	}

	// The batch failed; adjudicate events individually so one poison event
	// cannot block its batch forever. Events that succeed individually
	// still commit, isolating the failure to the events that caused it.
	for i, evt := range evts {
		err := s.writer.UpsertBatch(ctx, products[i:i+1])
		if err == nil {
			s.mu.Lock()
			s.appliedByJob[evt.JobID]++
			s.mu.Unlock()
			continue
		}
		action := s.policy.OnFailure(evt.Attempt, err)
		if aerr := s.applyAction(ctx, evt, action, err); aerr != nil {
			return fmt.Errorf("republishing event for row %d: %w", evt.RowNum, aerr)
		}
	}
	return nil
}

// ProcessPoison dead-letters a record whose payload could not be decoded:
// retrying an undecodable event can never succeed, so it is rejected
// permanently while its offset still commits. The original bytes are
// preserved in the dead letter topic for inspection.
func (s *Service) ProcessPoison(ctx context.Context, rec *kgo.Record) error {
	s.mu.Lock()
	s.deadByJob[string(rec.Key)]++
	s.mu.Unlock()
	return s.retrier.RepublishDeadRaw(ctx, rec, fmt.Errorf("undecodable payload"))
}

// applyAction routes one failed event to retry or the DLQ, preserving the
// true failure cause for inspection.
func (s *Service) applyAction(ctx context.Context, evt Event, action RetryAction, cause error) error {
	switch action {
	case RetryActionDeadLetter:
		s.mu.Lock()
		s.deadByJob[evt.JobID]++
		s.mu.Unlock()
		return s.retrier.RepublishDead(ctx, evt.ProductImported, cause)
	default:
		s.mu.Lock()
		s.retriedByJob[evt.JobID]++
		s.mu.Unlock()
		return s.retrier.RepublishRetry(ctx, evt.ProductImported, evt.Attempt+1, cause)
	}
}

// FlushProgress publishes accumulated per-job counts onto the progress
// topic and resets the accumulators. The worker calls it after offset
// commits, so progress always trails durability.
func (s *Service) FlushProgress(ctx context.Context) error {
	s.mu.Lock()
	type counts struct{ applied, retried, dead int64 }
	summary := make(map[string]counts, len(s.appliedByJob))
	for k, v := range s.appliedByJob {
		c := summary[k]
		c.applied = v
		summary[k] = c
	}
	for k, v := range s.retriedByJob {
		c := summary[k]
		c.retried = v
		summary[k] = c
	}
	for k, v := range s.deadByJob {
		c := summary[k]
		c.dead = v
		summary[k] = c
	}
	s.appliedByJob = make(map[string]int64)
	s.retriedByJob = make(map[string]int64)
	s.deadByJob = make(map[string]int64)
	s.mu.Unlock()

	if len(summary) == 0 {
		return nil
	}

	for jobID, c := range summary {
		if err := s.reporter.ReportProgress(ctx, jobID, c.applied, c.retried, c.dead); err != nil {
			return fmt.Errorf("reporting progress for job %s: %w", jobID, err)
		}
	}
	return s.reporter.Flush(ctx)
}

// logProcessingGap is reserved for reporting batches that were reprocessed
// after an unadjudicated failure; kept minimal by design.
func logProcessingGap(msg string) { log.Printf("catalog: %s", msg) }

var _ = logProcessingGap
