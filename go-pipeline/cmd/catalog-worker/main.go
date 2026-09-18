package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/config"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("catalog-worker started (kafka=%v)", cfg.KafkaBrokers)

	<-ctx.Done()

	log.Println("catalog-worker: graceful shutdown complete")
}
