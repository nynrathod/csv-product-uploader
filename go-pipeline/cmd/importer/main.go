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

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/config"
)

func main() {
	cfg := config.Load()

	app := fiber.New()

	app.Get("/health", func(c fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"service": "importer",
			"status":  "ok",
			"time":    time.Now().UTC(),
		})
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("importer listening on :%s (kafka=%v)", cfg.HTTPPort, cfg.KafkaBrokers)

	err := app.Listen(":"+cfg.HTTPPort, fiber.ListenConfig{
		GracefulContext: ctx,
		ShutdownTimeout: time.Duration(cfg.ShutdownTimeoutSec) * time.Second,
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("importer exited: %v", err)
	}
	log.Println("importer: graceful shutdown complete")
}
