package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/events"
)

// PublisherConfig tunes the durable producer.
type PublisherConfig struct {
	Brokers            []string
	ClientID           string
	Linger             time.Duration
	MaxBufferedRecords int
	DeliveryTimeout    time.Duration
}

// Publisher implements the EventPublisher port with a Kafka producer.
// Events are produced asynchronously: parsing continues while batches are
// delivered in the background, and durability is confirmed by Flush.
type Publisher struct {
	cl *kgo.Client

	mu       sync.Mutex
	firstErr error
}

// NewPublisher builds the producer, applying safe defaults for unset
// tuning values.
func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	if cfg.ClientID == "" {
		cfg.ClientID = "importer"
	}
	if cfg.Linger <= 0 {
		cfg.Linger = 5 * time.Millisecond
	}
	if cfg.MaxBufferedRecords <= 0 {
		cfg.MaxBufferedRecords = 50_000
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),

		// acks=all: a publish counts as durable only once every in-sync
		// replica holds the record. Combined with the idempotent producer
		// (on by default), broker-side retries can neither lose nor
		// duplicate an event.
		kgo.RequiredAcks(kgo.AllISRAcks()),

		// A record that cannot be delivered within this window fails its
		// promise; the importer surfaces the failure instead of stalling
		// an import forever.
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),

		// Throughput economics: a few milliseconds of linger lets the
		// client pack far more records into each request, and zstd
		// shrinks every batch on the wire.
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(cfg.Linger),

		// The buffer cap bounds producer memory: whatever the file size,
		// no more than this many undelivered records are ever held. Once
		// full, production applies backpressure to the parse loop.
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Publisher{cl: cl}, nil
}

// PublishProductImported emits one event, keyed by the product identity so
// all events for a product stay ordered on one partition.
func (p *Publisher) PublishProductImported(ctx context.Context, evt events.ProductImported) error {
	if err := p.failure(); err != nil {
		// Fail fast: once a delivery has failed, continuing to buffer more
		// rows only delays the inevitable job failure.
		return fmt.Errorf("event delivery previously failed: %w", err)
	}

	value, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("encoding product event: %w", err)
	}

	rec := &kgo.Record{
		Topic:     TopicProductImported,
		Key:       []byte(evt.Product.PartitionKey()),
		Value:     value,
		Timestamp: evt.ProducedAt,
	}

	// The promise records the first delivery failure. Promises run before
	// the buffered-record count drops, so Flush always observes them.
	p.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if err == nil {
			return
		}
		p.mu.Lock()
		if p.firstErr == nil {
			p.firstErr = err
		}
		p.mu.Unlock()
	})
	return nil
}

// Flush waits until every buffered record has been delivered or failed,
// then reports the first delivery failure. A nil Flush is the durability
// confirmation callers act on.
func (p *Publisher) Flush(ctx context.Context) error {
	p.cl.Flush(ctx)

	p.mu.Lock()
	err := p.firstErr
	p.firstErr = nil
	p.mu.Unlock()
	return err
}

// Close flushes any still-buffered records and releases the client.
func (p *Publisher) Close() {
	p.cl.Close()
}

// failure returns the first undelivered-event error, if any.
func (p *Publisher) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.firstErr
}
