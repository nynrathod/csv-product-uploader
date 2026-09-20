package events

import (
	"encoding/json"
	"testing"
	"time"
)

func TestProductDataPartitionKey(t *testing.T) {
	t.Parallel()

	p := ProductData{MerchantID: "m-1", ProductID: "p-9"}
	if got := p.PartitionKey(); got != "m-1:p-9" {
		t.Fatalf("PartitionKey() = %q, want %q", got, "m-1:p-9")
	}
}

func TestProductImportedJSONContract(t *testing.T) {
	t.Parallel()

	// The JSON shape is the wire contract shared by the importer and every
	// consumer; renaming a field silently breaks the pipeline. This test
	// pins the exact field names.
	evt := ProductImported{
		JobID:      "job-1",
		RowNum:     42,
		ProducedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		Product: ProductData{
			MerchantID:     "m-1",
			ProductID:      "p-1",
			Name:           "Widget",
			PriceCents:     1999,
			Currency:       "USD",
			ExpirationDate: "2027-01-31",
		},
	}

	want := `{"job_id":"job-1","row_num":42,"produced_at":"2026-01-02T03:04:05Z",` +
		`"product":{"merchant_id":"m-1","product_id":"p-1","name":"Widget",` +
		`"price_cents":1999,"currency":"USD","expiration_date":"2027-01-31"}}`

	got, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(got) != want {
		t.Fatalf("json contract changed:\n got: %s\nwant: %s", got, want)
	}

	var back ProductImported
	if err := json.Unmarshal(got, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back != evt {
		t.Fatalf("round trip = %+v, want %+v", back, evt)
	}
}
