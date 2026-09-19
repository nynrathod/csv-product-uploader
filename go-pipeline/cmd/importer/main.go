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

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/config"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/importjob"
	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/postgres"
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

	service := importjob.NewImportService(store, importjob.NoopPublisher{}, cfg.ProgressFlushRows)

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

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("importer exited: %v", err)
	}
	log.Println("importer: graceful shutdown complete")
}
