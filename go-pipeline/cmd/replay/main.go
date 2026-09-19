// Command replay rebuilds catalog state from the retained event stream.
//
// It resets the catalog worker's consumer group offsets to the beginning
// of the product topics and, with -truncate, empties the products table
// first so the rebuild provably starts from nothing. The worker is
// restarted afterwards and re-consumes the full event history without any
// re-upload: the event log is the system of record, the catalog is a
// projection of it.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	brokers := flag.String("brokers", "localhost:29092", "comma separated kafka bootstrap addresses")
	group := flag.String("group", "catalog-writer", "consumer group whose offsets are reset")
	topics := flag.String("topics", "product.imported.v1,product.retry.v1", "comma separated topics to replay")
	truncate := flag.Bool("truncate", false, "empty the products table before replaying")
	dbURL := flag.String("catalog-db", "postgres://catalog_svc:catalog_dev@localhost:5433/catalog_db?sslmode=disable", "catalog database connection string")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *truncate {
		if err := truncateCatalog(ctx, *dbURL); err != nil {
			log.Fatalf("truncating the catalog: %v", err)
		}
	}

	topicList := splitAndTrim(*topics)
	if err := resetOffsets(ctx, *brokers, *group, topicList); err != nil {
		log.Fatalf("resetting offsets: %v", err)
	}

	fmt.Println("offsets reset: restart the catalog worker to replay the full event history")
}

// truncateCatalog empties the products projection. The event log is
// untouched: the catalog rebuilds entirely from retained events.
func truncateCatalog(ctx context.Context, dbURL string) error {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("connecting to the catalog database: %w", err)
	}
	defer pool.Close()

	var before int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM products`).Scan(&before); err != nil {
		return fmt.Errorf("counting products: %w", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE products`); err != nil {
		return fmt.Errorf("truncating products: %w", err)
	}
	log.Printf("catalog truncated: %d product rows removed", before)
	return nil
}

// resetOffsets deletes the group's committed offsets on the given topics.
// With no committed offset, the worker's reset policy starts consumption
// at the earliest retained record. Partition ids are enumerated from the
// broker's topic metadata: Kafka assigns contiguous ids, so the partition
// count yields every id.
func resetOffsets(ctx context.Context, brokers, group string, topics []string) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitAndTrim(brokers)...),
		kgo.ClientID("replay"),
	)
	if err != nil {
		return fmt.Errorf("creating admin client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	existing, err := adm.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("listing topics: %w", err)
	}

	// TopicsSet is a topic -> partition set map; every partition of every
	// requested topic is reset explicitly.
	set := kadm.TopicsSet{}
	for _, topic := range topics {
		detail, ok := existing[topic]
		if !ok {
			return fmt.Errorf("topic %s does not exist", topic)
		}
		partitions := make(map[int32]struct{}, len(detail.Partitions))
		for p := int32(0); p < int32(len(detail.Partitions)); p++ {
			partitions[p] = struct{}{}
		}
		set[topic] = partitions
	}

	deleted, err := adm.DeleteOffsets(ctx, group, set)
	if err != nil {
		return fmt.Errorf("deleting committed offsets: %w", err)
	}
	// Deletion results arrive as topic -> partition -> outcome.
	for topic, partitions := range deleted {
		for partition, perr := range partitions {
			if perr != nil {
				log.Printf("reset %s/%d failed: %v", topic, partition, perr)
				continue
			}
			log.Printf("reset %s/%d to earliest", topic, partition)
		}
	}
	return nil
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
