package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicSpec pairs a topic with its planned partition count.
type topicSpec struct {
	Name       string
	Partitions int32
}

// requiredTopics declares the platform's full event backbone. The broker
// has auto-creation disabled, so contracts are declared, not implied by
// the first accidental publish.
var requiredTopics = []topicSpec{
	{TopicProductImported, PartitionsProductImported},
	{TopicProductRetry, PartitionsProductRetry},
	{TopicProductDLQ, PartitionsProductDLQ},
	{TopicImportProgress, PartitionsImportProgress},
}

// EnsureTopics creates any missing required topic, retrying while the
// broker is still starting. The operation is idempotent: existing topics
// are left untouched, so every service can safely run it at boot.
func EnsureTopics(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("topic-admin"),
	)
	if err != nil {
		return fmt.Errorf("creating admin client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = ensureTopicsOnce(ctx, adm)
		if lastErr == nil {
			return nil
		}
		time.Sleep(time.Second)
	}
	return lastErr
}

// ensureTopicsOnce performs a single create-if-missing pass.
func ensureTopicsOnce(ctx context.Context, adm *kadm.Client) error {
	existing, err := adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("listing topics: %w", err)
	}

	for _, want := range requiredTopics {
		if detail, ok := existing[want.Name]; ok {
			// The topic's partitions are keyed by partition id, so their
			// number is the map's length.
			if int32(len(detail.Partitions)) < want.Partitions {
				log.Printf("topic %s exists with %d partitions, %d planned",
					want.Name, len(detail.Partitions), want.Partitions)
			}
			continue
		}
		// Single-replica development broker; production raises the factor.
		if _, err := adm.CreateTopics(ctx, want.Partitions, 1, nil, want.Name); err != nil {
			return fmt.Errorf("creating topic %s: %w", want.Name, err)
		}
		log.Printf("created topic %s with %d partitions", want.Name, want.Partitions)
	}
	return nil
}

// GroupLagSource for the importer's metrics: summed consumer lag.
type AdminLagSource struct {
	cl *kgo.Client
}

// NewAdminLagSource builds a lag source from a dedicated client.
func NewAdminLagSource(brokers []string) (*AdminLagSource, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ClientID("metrics-lag"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating lag client: %w", err)
	}
	return &AdminLagSource{cl: cl}, nil
}

// Close releases the client.
func (a *AdminLagSource) Close() { a.cl.Close() }

// GroupLag returns the summed lag of a consumer group: the difference
// between each partition's log end offset and the group's committed
// offset. Responses are keyed by topic and partition, so both are read
// directly from the maps.
func (a *AdminLagSource) GroupLag(ctx context.Context, group string) (int64, error) {
	adm := kadm.NewClient(a.cl)

	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return 0, fmt.Errorf("fetching committed offsets: %w", err)
	}

	// The group's assigned topics are the committed response's keys.
	topics := make([]string, 0, len(committed))
	for t := range committed {
		topics = append(topics, t)
	}
	if len(topics) == 0 {
		return 0, nil
	}

	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return 0, fmt.Errorf("listing end offsets: %w", err)
	}

	var total int64
	for topic, partitions := range ends {
		for partition, end := range partitions {
			at, ok := committed[topic][partition]
			if !ok || at.At < 0 {
				// No committed offset: the whole partition is lag.
				total += end.Offset
				continue
			}
			if lag := end.Offset - at.At; lag > 0 {
				total += lag
			}
		}
	}
	return total, nil
}
