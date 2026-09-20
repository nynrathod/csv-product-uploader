package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/catalog"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/config"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/kafka"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/postgres"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.CatalogDBURL)
	if err != nil {
		log.Fatalf("connecting to the catalog database: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("pinging the catalog database: %v", err)
	}

	if err := postgres.Migrate(ctx, pool, cfg.CatalogMigrationsDir); err != nil {
		log.Fatalf("applying catalog migrations: %v", err)
	}

	// The event backbone is declared, not implied; the call is idempotent.
	if err := kafka.EnsureTopics(ctx, cfg.KafkaBrokers); err != nil {
		log.Fatalf("ensuring kafka topics: %v", err)
	}

	// The worker identity scopes its progress snapshots: the importer
	// folds each worker's counters independently and derives job totals
	// as the sum, which is correct under partitioned consumption.
	host, _ := os.Hostname()
	workerID := fmt.Sprintf("%s-%d", host, os.Getpid())

	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:        cfg.KafkaBrokers,
		Group:          kafka.GroupCatalogWriter,
		ClientID:       "catalog-worker",
		MaxPollRecords: cfg.WorkerMaxPollRecords,
		MaxWait:        time.Duration(cfg.WorkerFetchMaxWaitMillis) * time.Millisecond,
	}, kafka.TopicProductImported, kafka.TopicProductRetry)
	if err != nil {
		log.Fatalf("creating the event consumer: %v", err)
	}
	defer consumer.Close()

	retrier, err := kafka.NewEventRepublisher(kafka.PublisherConfig{
		Brokers:         cfg.KafkaBrokers,
		ClientID:        "catalog-worker-republisher",
		DeliveryTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("creating the event republisher: %v", err)
	}
	defer retrier.Close()

	reporter, err := kafka.NewProgressReporter(kafka.PublisherConfig{
		Brokers:         cfg.KafkaBrokers,
		ClientID:        "catalog-worker-progress",
		DeliveryTimeout: 30 * time.Second,
	}, workerID)
	if err != nil {
		log.Fatalf("creating the progress reporter: %v", err)
	}
	defer reporter.Close()

	writer := postgres.NewProductWriter(pool)
	service := catalog.NewService(
		writer,
		catalog.LinearRetryPolicy{},
		retrier,
		reporter,
		catalog.ProcessingLimits{BatchSize: cfg.WorkerBatchSize},
	)

	worker := catalog.NewWorker(consumer, service, catalog.WorkerConfig{
		ProgressFlushInterval: 2 * time.Second,
	})

	log.Printf("catalog-worker %s consuming %s and %s (group %s)",
		workerID, kafka.TopicProductImported, kafka.TopicProductRetry, kafka.GroupCatalogWriter)

	if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("catalog-worker exited: %v", err)
	}
	log.Println("catalog-worker: graceful shutdown complete")
}
