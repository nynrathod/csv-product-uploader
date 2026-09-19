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
