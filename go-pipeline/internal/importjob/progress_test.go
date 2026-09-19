package importjob

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// fakeSource scripts one record batch, then reports idle.
type fakeSource struct {
	mu      sync.Mutex
	records []*kgo.Record
	commits [][]*kgo.Record
}

func (f *fakeSource) Poll(context.Context) []*kgo.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.records) == 0 {
		return nil
	}
	batch := f.records
	f.records = nil
	return batch
}

func (f *fakeSource) CommitRecords(_ context.Context, recs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, recs)
	return nil
}

func (f *fakeSource) Close() {}

func TestProgressTrackerAppliesAndCommits(t *testing.T) {
	t.Parallel()

	const jobID = "11111111-1111-1111-1111-111111111111"
	store := newMemStore()
	store.put(&ImportJob{ID: jobID, Status: StatusProcessing, PublishedRows: 5})

	valid, err := json.Marshal(events.ImportProgress{JobID: jobID, ProcessedRows: 5})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	foreign, err := json.Marshal(events.ImportProgress{JobID: "publisher-test", ProcessedRows: 99})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	src := &fakeSource{records: []*kgo.Record{
		{Topic: "import.progress.v1", Key: []byte(jobID), Value: valid},
		{Topic: "import.progress.v1", Value: foreign},
		{Topic: "import.progress.v1", Value: []byte("not-json")},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	tracker := NewProgressTracker(store, src)

	done := make(chan struct{})
	go func() {
		tracker.Run(ctx)
		close(done)
	}()

	waitForJob(t, store, jobID, func(j ImportJob) bool { return j.Status == StatusCompleted }, 5*time.Second)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("tracker did not stop after cancellation")
	}

	j, _ := store.Get(context.Background(), jobID)
	if j.ProcessedRows != 5 || j.Status != StatusCompleted {
		t.Fatalf("job = %+v, want completed with 5 processed", j)
	}

	// Every settled record, including the foreign and undecodable ones,
	// commits exactly once.
	if len(src.commits) != 1 || len(src.commits[0]) != 3 {
		t.Fatalf("commits = %+v, want one commit of all three records", src.commits)
	}
}
