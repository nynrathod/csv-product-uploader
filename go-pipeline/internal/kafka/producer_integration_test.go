package kafka

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// brokerEnv opts into integration tests against a live broker; plain unit
// runs skip them.
const brokerEnv = "TEST_KAFKA_BROKERS"

func testBrokers(t *testing.T) []string {
	t.Helper()
	addr := os.Getenv(brokerEnv)
	if addr == "" {
		t.Skipf("set %s to run kafka integration tests", brokerEnv)
	}
	return []string{addr}
}

func TestPublisherPublishesAndFlushes(t *testing.T) {
	brokers := testBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := EnsureTopics(ctx, brokers); err != nil {
		t.Fatalf("EnsureTopics: %v", err)
	}

	p, err := NewPublisher(PublisherConfig{
		Brokers:         brokers,
		ClientID:        "publisher-test",
		Linger:          5 * time.Millisecond,
		DeliveryTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	defer p.Close()

	evt := events.ProductImported{
		JobID:      "11111111-1111-1111-1111-111111111111",
		RowNum:     2,
		ProducedAt: time.Now().UTC(),
		Product: events.ProductData{
			MerchantID: "m-test",
			ProductID:  "4026987913289674",
			Name:       "Calypso - Lemonade #(4026987913289674)",
			PriceCents: 11555,
			Currency:   "USD",
		},
	}

	for i := 0; i < 100; i++ {
		if err := p.PublishProductImported(ctx, evt); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	// A nil flush is the durability confirmation: all 100 records are
	// broker-acknowledged.
	if err := p.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
