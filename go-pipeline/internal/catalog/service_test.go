package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
)

// fakeWriter applies products or fails on demand.
type fakeWriter struct {
	failAll bool
	written []Product
}

func (f *fakeWriter) UpsertBatch(_ context.Context, products []Product) error {
	if f.failAll {
		return errors.New("catalog unavailable")
	}
	f.written = append(f.written, products...)
	return nil
}

// fakeRetrier records republished events.
type fakeRetrier struct {
	retries map[string]int
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

func mkEvent(jobID, merchant, product string) Event {
	return Event{
		ProductImported: events.ProductImported{
			JobID:  jobID,
			RowNum: 2,
			Product: events.ProductData{
				MerchantID: merchant, ProductID: product, Name: "Widget",
				PriceCents: 1999, Currency: "USD",
			},
		},
		Attempt: 0,
	}
}

func TestProcessBatchAppliesAll(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	err := svc.ProcessBatch(context.Background(), []Event{
		mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-1"),
		mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-2"),
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

func TestProcessBatchDeadLettersInvalidEvents(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	// A non-uuid job id would fail the shared batch statement's cast and
	// poison otherwise-valid rows; validation routes it to the DLQ before
	// any database round trip.
	err := svc.ProcessBatch(context.Background(), []Event{
		mkEvent("not-a-uuid", "m-1", "p-bad"),
		mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-good"),
	})
	if err != nil {
		t.Fatalf("ProcessBatch: %v", err)
	}
	if len(writer.written) != 1 || writer.written[0].ProductID != "p-good" {
		t.Fatalf("written = %+v, want only the valid product", writer.written)
	}
	if len(retrier.deads) != 1 {
		t.Fatalf("deads = %v, want the invalid event dead-lettered", retrier.deads)
	}
}

func TestProcessBatchDefersWriteFailures(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{failAll: true}
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	// Environmental failures defer the whole batch: offsets stay
	// uncommitted and the next poll retries; nothing is republished.
	err := svc.ProcessBatch(context.Background(), []Event{
		mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-1"),
	})
	if err == nil {
		t.Fatal("write failure must defer the batch")
	}
	if len(retrier.deads) != 0 || len(retrier.retries) != 0 {
		t.Fatalf("environmental failure must not republish: %+v %+v", retrier.deads, retrier.retries)
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

func TestFlushProgressIsIdempotentSnapshot(t *testing.T) {
	t.Parallel()

	writer := &fakeWriter{}
	retrier := newFakeRetrier()
	reporter := &fakeReporter{}
	svc := NewService(writer, LinearRetryPolicy{}, retrier, reporter, ProcessingLimits{})

	_ = svc.ProcessBatch(context.Background(), []Event{mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-1")})
	if err := svc.FlushProgress(context.Background()); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	// Unchanged totals are not resent: the snapshot is idempotent.
	if err := svc.FlushProgress(context.Background()); err != nil {
		t.Fatalf("second flush: %v", err)
	}
	if len(reporter.reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(reporter.reports))
	}

	// New work raises the cumulative total and is reported.
	_ = svc.ProcessBatch(context.Background(), []Event{mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-2")})
	if err := svc.FlushProgress(context.Background()); err != nil {
		t.Fatalf("third flush: %v", err)
	}
	if len(reporter.reports) != 2 || reporter.reports[1].ProcessedRows != 2 {
		t.Fatalf("reports = %+v, want cumulative 2", reporter.reports)
	}
}

func TestFromEventValidation(t *testing.T) {
	t.Parallel()

	valid := mkEvent("11111111-1111-1111-1111-111111111111", "m-1", "p-1").ProductImported
	if _, err := FromEvent(valid); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	noMerchant := valid
	noMerchant.Product.MerchantID = ""
	if _, err := FromEvent(noMerchant); err == nil {
		t.Fatal("missing merchant must be rejected")
	}

	badJob := valid
	badJob.JobID = "not-a-uuid"
	if _, err := FromEvent(badJob); err == nil {
		t.Fatal("non-uuid job id must be rejected before the batch write")
	}
}
