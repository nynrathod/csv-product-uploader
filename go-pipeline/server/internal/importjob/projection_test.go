package importjob

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
)

// fakeProjectionStore records upserted events.
type fakeProjectionStore struct {
	mu      sync.Mutex
	upserts []events.ProductImported
}

func (f *fakeProjectionStore) UpsertProducts(_ context.Context, evts []events.ProductImported) error {
	f.mu.Lock()
	f.upserts = append(f.upserts, evts...)
	f.mu.Unlock()
	return nil
}

func (f *fakeProjectionStore) ListProducts(context.Context, string, ProductQuery) ([]events.ProductData, int64, error) {
	return nil, 0, nil
}

func TestProductProjectorProjectsAndCommits(t *testing.T) {
	t.Parallel()

	valid, err := json.Marshal(events.ProductImported{
		JobID:  "11111111-1111-1111-1111-111111111111",
		RowNum: 2,
		Product: events.ProductData{
			MerchantID: "m-1", ProductID: "p-1", Name: "Widget",
			PriceCents: 1999, Currency: "USD",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	src := &fakeSource{records: []*kgo.Record{
		{Topic: "product.imported.v1", Key: []byte("m-1:p-1"), Value: valid},
		{Topic: "product.imported.v1", Value: []byte("not-json")},         // undecodable: skipped
		{Topic: "product.imported.v1", Value: []byte(`{"job_id":"bad"}`)}, // invalid job id: skipped
	}}

	store := &fakeProjectionStore{}
	projector := NewProductProjector(store, src)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		projector.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.Lock()
		n := len(store.upserts)
		store.mu.Unlock()
		src.mu.Lock()
		c := len(src.commits)
		src.mu.Unlock()
		if n == 1 && c == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("projector did not stop after cancellation")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.upserts) != 1 || store.upserts[0].Product.ProductID != "p-1" {
		t.Fatalf("upserts = %+v, want only the valid event", store.upserts)
	}
}

func (f *fakeProjectionStore) ProductLatency(context.Context, int) (LatencySummary, error) {
	return LatencySummary{}, nil
}
