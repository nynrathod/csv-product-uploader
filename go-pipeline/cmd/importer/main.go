package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/logger"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/config"
	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/importjob"
	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/kafka"
	"github.com/nynrathod/csv-product-uploader/catalog-stream/internal/postgres"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.ImportDBURL)
	if err != nil {
		log.Fatalf("connecting to the import database: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("pinging the import database: %v", err)
	}

	if err := postgres.Migrate(ctx, pool, cfg.ImportMigrationsDir); err != nil {
		log.Fatalf("applying import migrations: %v", err)
	}

	store := postgres.NewJobStore(pool)

	// Jobs orphaned by a previous restart or crash are failed explicitly so
	// they never hang in a non-terminal state.
	if n, err := store.FailStale(ctx, "importer restarted before the import finished"); err != nil {
		log.Fatalf("failing stale imports: %v", err)
	} else if n > 0 {
		log.Printf("marked %d interrupted import(s) as failed", n)
	}

	// The event backbone is declared, not implied: the broker disables
	// auto-creation, so the platform's topics exist before the first
	// publish. The call is idempotent.
	if err := kafka.EnsureTopics(ctx, cfg.KafkaBrokers); err != nil {
		log.Fatalf("ensuring kafka topics: %v", err)
	}

	publisher, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:            cfg.KafkaBrokers,
		ClientID:           "importer",
		Linger:             time.Duration(cfg.ProducerLingerMillis) * time.Millisecond,
		MaxBufferedRecords: cfg.MaxBufferedRecords,
		DeliveryTimeout:    30 * time.Second,
	})
	if err != nil {
		log.Fatalf("creating the event publisher: %v", err)
	}
	defer publisher.Close()

	service := importjob.NewImportService(store, publisher, cfg.ProgressFlushRows)

	// The progress tracker is the importer's own consumer: it folds the
	// catalog worker's progress events into the import jobs this service
	// owns, completing them once the catalog confirms every event.
	progressConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:  cfg.KafkaBrokers,
		Group:    kafka.GroupImportTracker,
		ClientID: "import-tracker",
	}, kafka.TopicImportProgress)
	if err != nil {
		log.Fatalf("creating the progress consumer: %v", err)
	}

	tracker := importjob.NewProgressTracker(store, progressConsumer)
	trackerDone := make(chan struct{})
	go func() {
		defer close(trackerDone)
		if err := tracker.Run(ctx); err != nil {
			// Without the tracker, imports can never observe catalog
			// confirmation; failing fast surfaces the fault instead of
			// silently freezing jobs.
			log.Fatalf("progress tracker exited: %v", err)
		}
	}()

	app := fiber.New(fiber.Config{
		// Streaming request bodies keep multipart uploads out of RAM; the
		// file is spooled to disk part by part.
		StreamRequestBody: true,
		BodyLimit:         cfg.MaxUploadMB << 20,
	})

	app.Use(logger.New())

	handler := importjob.NewHTTPHandler(service, store, importjob.HandlerConfig{
		UploadDir:       cfg.UploadDir,
		MaxUploadBytes:  int64(cfg.MaxUploadMB) << 20,
		SSEPollInterval: time.Duration(cfg.SSEPollMillis) * time.Millisecond,
		SSEMaxDuration:  time.Duration(cfg.SSEMaxDurationSec) * time.Second,
	})
	handler.Register(app)

	app.Get("/health", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"service": "importer",
			"status":  "ok",
			"time":    time.Now().UTC(),
		})
	})

	log.Printf("importer listening on :%s (kafka=%v)", cfg.HTTPPort, cfg.KafkaBrokers)

	err = app.Listen(":"+cfg.HTTPPort, fiber.ListenConfig{
		GracefulContext: ctx,
		ShutdownTimeout: time.Duration(cfg.ShutdownTimeoutSec) * time.Second,
	})

	// The HTTP server is down; the tracker drains and stops with the
	// cancelled context before its consumer is released.
	<-trackerDone
	progressConsumer.Close()

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("importer exited: %v", err)
	}
	log.Println("importer: graceful shutdown complete")
}
