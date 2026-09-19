package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// fakeWriter applies products or fails on demand.
type fakeWriter struct {
	failAll   bool
	failFirst int // number of single-event writes to fail before succeeding
	written   []Product
}

func (f *fakeWriter) UpsertBatch(_ context.Context, products []Product) error {
	if f.failAll {
		return errors.New("catalog unavailable")
	}
	if len(products) == 1 && f.failFirst > 0 {
		f.failFirst--
		return errors.New("transient write failure")
	}
	f.written = append(f.written, products...)
	return nil
}

// fakeRetrier records republished events.
type fakeRetrier struct {
	retries map[string]int // key: partition key, value: attempts
	deads   []string
	raws    int
}

func newFakeRetrier() *fakeRetrier { return &fakeRetrier{retries: map[string]int{}} }

func (f *fakeRetrier) RepublishRetry(_ context.Context, evt events.ProductImported, attempt int, _ error) error {
	f.retries[evt.Product.PartitionKey()] = attempt
	return nil
}

func (f *fakeRetrier) RepublishDead(_ context.Context, evt events.ProductImported, _ error) error {
	f.deads = append(f.deads, evt.Product.PartitionKey())
	return nil
}

func (f *fakeRetrier) RepublishDeadRaw(_ context.Context, _ *kgo.Record, _ error) error {
	f.raws++
	return nil
}

func (f *fakeRetrier) Flush(context.Context) {}
func (f *fakeRetrier) Close()                {}

// fakeReporter records progress reports.
type fakeReporter struct {
	reports []events.ImportProgress
}

func (f *fakeReporter) ReportProgress(_ context.Context, jobID string, processed, retried, dead int64) error {
	f.reports = append(f.reports, events.ImportProgress{
		JobID: jobID, ProcessedRows: processed, RetriedRows: retried, DeadRows: dead,
	})
	return nil
}

func (f *fakeReporter) Flush(context.Context) error { return nil }
func (f *fakeReporter) Close()                      {}

func mkEvent(jobID, merchant, product string, attempt int) Event {
	return Event{
		ProductImported: events.ProductImported{
			JobID:  jobID,
			RowNum: 2,
			Product: events.ProductData{
				MerchantID: merchant, ProductID: product, Name: "Widget",
				PriceCents: 1999, Currency: "USD",
			},
		},
		Attempt: attempt,
	}
}

func TestProcessBatchAppliesAll(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	err := svc.ProcessBatch(context.Background(), []Event{
		mkEvent("job-1", "m-1", "p-1", 0),
		mkEvent("job-1", "m-1", "p-2", 0),
	})
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	if len(writer.written) != 2 {
		t.Fatalf("written = %d, want 2", len(writer.written))
	}
	if len(retrier.retries) != 0 || len(retrier.deads) != 0 {
		t.Fatalf("unexpected republishes: %+v %+v", retrier.retries, retrier.deads)
	}

	if err := svc.FlushProgress(context.Background()); err != nil {
		t.Fatalf("FlushProgress: %v", err)
	}
	if len(reporter.reports) != 1 || reporter.reports[0].ProcessedRows != 2 {
		t.Fatalf("reports = %+v, want one report of 2 processed", reporter.reports)
	}
}

func TestProcessBatchIsolatesPoisonEvent(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{failFirst: 1} // one single-event write fails
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	// The batch write succeeds as a whole (failAll false), so this tests
	// the fast path; poison isolation is exercised via retry policy below.
	err := svc.ProcessBatch(context.Background(), []Event{mkEvent("job-1", "m-1", "p-1", 0)})
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}

	// Direct adjudication path: a batch that fails wholesale falls back to
	// per-event writes; the failing event is retried, not lost.
	writer2 := &fakeWriter{failAll: true}
	svc2 := NewService(writer2, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})
	err = svc2.ProcessBatch(context.Background(), []Event{mkEvent("job-2", "m-2", "p-9", 0)})
	if err != nil {
		t.Fatalf("ProcessBatch adjudicated: %v", err)
	}
	if got := retrier.retries["m-2:p-9"]; got != 1 {
		t.Fatalf("retry attempt = %d, want 1", got)
	}
}

func TestRetryBudgetDeadLetters(t *testing.T) {
	t.Parallel()

	policy := LinearRetryPolicy{}
	if policy.OnFailure(0, errors.New("x")) != RetryActionRetry {
		t.Fatal("first failure must retry")
	}
	if policy.OnFailure(1, errors.New("x")) != RetryActionRetry {
		t.Fatal("second failure must retry")
	}
	if policy.OnFailure(2, errors.New("x")) != RetryActionDeadLetter {
		t.Fatal("third failure must dead-letter")
	}
	if policy.OnFailure(0, ErrNonRetryable) != RetryActionDeadLetter {
		t.Fatal("non-retryable failures must dead-letter immediately")
	}
}

func TestEncodeDecodeAttempts(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 2, 42} {
		if got := DecodeAttempts(EncodeAttempts(n)); got != n {
			t.Errorf("round trip of %d = %d", n, got)
		}
	}
	if DecodeAttempts(nil) != 0 {
		t.Error("nil header must decode to zero")
	}
	if DecodeAttempts([]byte("garbage")) != 0 {
		t.Error("malformed header must decode to zero")
	}
	if DecodeAttempts([]byte("-3")) != 0 {
		t.Error("negative header must decode to zero")
	}
}

func TestBackoffDelays(t *testing.T) {
	t.Parallel()

	b := BackoffPolicy{Base: 100, Max: 800}
	if b.DelayFor(0) != 100 {
		t.Errorf("delay(0) = %v", b.DelayFor(0))
	}
	if b.DelayFor(1) != 200 {
		t.Errorf("delay(1) = %v", b.DelayFor(1))
	}
	if b.DelayFor(2) != 400 {
		t.Errorf("delay(2) = %v", b.DelayFor(2))
	}
	if b.DelayFor(10) != 800 {
		t.Errorf("delay(10) = %v, want max cap", b.DelayFor(10))
	}
}

func TestFlushProgressResetsAccumulators(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	_ = svc.ProcessBatch(context.Background(), []Event{mkEvent("job-1", "m-1", "p-1", 0)})
	if err := svc.FlushProgress(context.Background()); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	// Second flush with no work must report nothing.
	if err := svc.FlushProgress(context.Background()); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if len(reporter.reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(reporter.reports))
	}
}
