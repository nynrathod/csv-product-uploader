// Package config is the single source of truth for both services' runtime
// settings. Every value is environment-overridable with a development
// default.
package config

import (
	"os"
	"strconv"
	"strings"
)

// Config carries the shared and service-specific settings.
type Config struct {
	// Shared.
	KafkaBrokers       []string
	ShutdownTimeoutSec int

	// Event production.
	ProducerLingerMillis int
	MaxBufferedRecords   int

	// Importer service.
	HTTPPort            string
	ImportDBURL         string
	UploadDir           string
	MaxUploadMB         int
	ProgressFlushRows   int64
	SSEPollMillis       int
	SSEMaxDurationSec   int
	ImportMigrationsDir string

	// Catalog-worker service.
	CatalogDBURL         string
	CatalogMigrationsDir string
}

// Load reads the environment and applies defaults.
func Load() Config {
	return Config{
		KafkaBrokers:       splitCSV(getenv("KAFKA_BROKERS", "localhost:29092")),
		ShutdownTimeoutSec: getint("SHUTDOWN_TIMEOUT_SEC", 10),

		ProducerLingerMillis: getint("PRODUCER_LINGER_MILLIS", 5),
		MaxBufferedRecords:   getint("PRODUCER_MAX_BUFFERED_RECORDS", 50000),

		HTTPPort:            getenv("IMPORTER_HTTP_PORT", "8080"),
		ImportDBURL:         getenv("IMPORT_DB_URL", "postgres://importer_svc:importer_dev@localhost:5433/import_db?sslmode=disable"),
		UploadDir:           getenv("UPLOAD_DIR", ""),
		MaxUploadMB:         getint("MAX_UPLOAD_MB", 512),
		ProgressFlushRows:   int64(getint("PROGRESS_FLUSH_ROWS", 5000)),
		SSEPollMillis:       getint("SSE_POLL_MILLIS", 500),
		SSEMaxDurationSec:   getint("SSE_MAX_DURATION_SEC", 900),
		ImportMigrationsDir: getenv("IMPORT_MIGRATIONS_DIR", "deploy/migrations/import"),

		CatalogDBURL:         getenv("CATALOG_DB_URL", "postgres://catalog_svc:catalog_dev@localhost:5433/catalog_db?sslmode=disable"),
		CatalogMigrationsDir: getenv("CATALOG_MIGRATIONS_DIR", "deploy/migrations/catalog"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
