package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/catalog"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/events"
)

// ConsumerConfig tunes the consuming worker.
type ConsumerConfig struct {
	Brokers       []string
	Group         string
	ClientID      string
	FetchMaxBytes int32
	MaxWait       time.Duration
}

// Consumer wraps a franz-go client in group consume mode and satisfies the
// catalog worker's RecordSource port.
type Consumer struct {
	cl *kgo.Client
}

// NewConsumer builds a group member subscribing to the given topics.
func NewConsumer(cfg ConsumerConfig, topics ...string) (*Consumer, error) {
	if cfg.ClientID == "" {
		cfg.ClientID = cfg.Group
	}
	if cfg.FetchMaxBytes <= 0 {
		cfg.FetchMaxBytes = 32 << 20
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 250 * time.Millisecond
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(topics...),

		// Offsets are committed explicitly after events are durably
		// applied or republished; automatic commits are disabled so a
		// crash can never skip unprocessed events.
		kgo.DisableAutoCommit(),

		// Cooperative sticky balancing avoids stop-the-world pauses when
		// workers join or leave the group.
		kgo.Balancers(kgo.CooperativeStickyBalancer()),

		kgo.FetchMaxBytes(cfg.FetchMaxBytes),
		kgo.FetchMaxWait(cfg.MaxWait),

		// When no committed offset exists for a partition, consumption
		// begins at the earliest record: a reset group rebuilds the full
		// event history rather than silently skipping it.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating consumer client: %w", err)
	}
	return &Consumer{cl: cl}, nil
}

// Poll returns records for one poll window; empty when idle. Partition
// fetch errors are logged and those partitions are simply re-polled. The
// records are owned by the client's fetch buffers and are valid only
// until the next poll: the worker fully adjudicates every record, and
// copies payloads out while decoding, before polling again.
func (c *Consumer) Poll(ctx context.Context) []*kgo.Record {
	fetches := c.cl.PollRecords(ctx, 500)

	fetches.EachError(func(topic string, partition int32, err error) {
		log.Printf("fetch error on %s/%d: %v", topic, partition, err)
	})

	records := make([]*kgo.Record, 0, 128)
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})
	return records
}

// CommitRecords commits the offsets of the given records as processed.
func (c *Consumer) CommitRecords(ctx context.Context, recs ...*kgo.Record) error {
	return c.cl.CommitRecords(ctx, recs...)
}

// Close releases the consumer.
func (c *Consumer) Close() {
	c.cl.Close()
}

// EventRepublisher republishes failed product events onto the retry and
// dead letter topics, carrying retry metadata in headers.
type EventRepublisher struct {
	cl *kgo.Client
}

// NewEventRepublisher builds a republisher from a dedicated producer
// client; retry and dead letter publications must be durable before their
// source offsets commit, so they produce with full acknowledgements.
func NewEventRepublisher(cfg PublisherConfig) (*EventRepublisher, error) {
	if cfg.ClientID == "" {
		cfg.ClientID = "republisher"
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("creating republisher client: %w", err)
	}
	return &EventRepublisher{cl: cl}, nil
}

// RepublishRetry emits an event onto the retry topic with its attempt
// count stamped in headers.
func (r *EventRepublisher) RepublishRetry(ctx context.Context, evt events.ProductImported, attempt int, cause error) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("encoding retry event: %w", err)
	}
	rec := &kgo.Record{
		Topic: TopicProductRetry,
		Key:   []byte(evt.Product.PartitionKey()),
		Value: value,
		Headers: append(retryHeaders(attempt, cause),
			kgo.RecordHeader{Key: catalog.HeaderOrigTopic, Value: []byte(TopicProductImported)},
		),
	}
	return produce(ctx, r.cl, rec)
}

// RepublishDead emits an event onto the dead letter topic with its retry
// history in headers.
func (r *EventRepublisher) RepublishDead(ctx context.Context, evt events.ProductImported, cause error) error {
	value, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("encoding dead event: %w", err)
	}
	rec := &kgo.Record{
		Topic: TopicProductDLQ,
		Key:   []byte(evt.Product.PartitionKey()),
		Value: value,
		Headers: append(retryHeaders(0, cause),
			kgo.RecordHeader{Key: catalog.HeaderOrigTopic, Value: []byte(TopicProductImported)},
		),
	}
	return produce(ctx, r.cl, rec)
}

// RepublishDeadRaw dead-letters a record's payload verbatim, preserving
// the original bytes for inspection.
func (r *EventRepublisher) RepublishDeadRaw(ctx context.Context, rec *kgo.Record, cause error) error {
	out := &kgo.Record{
		Topic: TopicProductDLQ,
		Key:   rec.Key,
		Value: rec.Value,
		Headers: append(rec.Headers,
			kgo.RecordHeader{Key: catalog.HeaderOrigTopic, Value: []byte(rec.Topic)},
			kgo.RecordHeader{Key: catalog.HeaderLastError, Value: []byte(cause.Error())},
		),
	}
	return produce(ctx, r.cl, out)
}

// Flush waits for buffered republished records to deliver.
func (r *EventRepublisher) Flush(ctx context.Context) {
	r.cl.Flush(ctx)
}

// Close releases the republisher.
func (r *EventRepublisher) Close() {
	r.cl.Close()
}

// ProgressReporter publishes import progress events.
type ProgressReporter struct {
	cl *kgo.Client
}

// NewProgressReporter builds a reporter from a dedicated producer client.
func NewProgressReporter(cfg PublisherConfig) (*ProgressReporter, error) {
	if cfg.ClientID == "" {
		cfg.ClientID = "progress-reporter"
	}
	if cfg.DeliveryTimeout <= 0 {
		cfg.DeliveryTimeout = 30 * time.Second
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("creating progress producer: %w", err)
	}
	return &ProgressReporter{cl: cl}, nil
}

// ReportProgress publishes processed, retried and dead counts for a job.
func (r *ProgressReporter) ReportProgress(ctx context.Context, jobID string, processed, retried, dead int64) error {
	evt := events.ImportProgress{
		JobID:         jobID,
		ProcessedRows: processed,
		RetriedRows:   retried,
		DeadRows:      dead,
		ReportedAt:    time.Now().UTC(),
	}
	value, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("encoding progress event: %w", err)
	}
	return produce(ctx, r.cl, &kgo.Record{
		Topic: TopicImportProgress,
		Key:   []byte(jobID),
		Value: value,
	})
}

// Flush waits for buffered progress records to deliver.
func (r *ProgressReporter) Flush(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		r.cl.Flush(ctx)
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close releases the reporter.
func (r *ProgressReporter) Close() {
	r.cl.Close()
}

// produce synchronously delivers one record; retry and DLQ publications
// must be durable before their source offsets commit.
func produce(ctx context.Context, cl *kgo.Client, rec *kgo.Record) error {
	done := make(chan error, 1)
	cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		done <- err
	})
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// retryHeaders renders the shared retry metadata headers.
func retryHeaders(attempt int, cause error) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: catalog.HeaderAttempts, Value: catalog.EncodeAttempts(attempt)},
		{Key: catalog.HeaderLastError, Value: []byte(cause.Error())},
	}
}
