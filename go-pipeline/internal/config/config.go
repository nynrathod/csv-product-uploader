package config

import (
	"os"
	"strconv"
	"strings"
)

// Config is the single source of truth for both services' runtime settings.
type Config struct {
	// importer
	HTTPPort    string
	ImportDBURL string

	// catalog-worker
	CatalogDBURL string

	// shared
	KafkaBrokers       []string
	ShutdownTimeoutSec int
}

func Load() Config {
	return Config{
		HTTPPort:           getenv("IMPORTER_HTTP_PORT", "8080"),
		ImportDBURL:        getenv("IMPORT_DB_URL", "postgres://importer_svc:importer_dev@localhost:5432/import_db?sslmode=disable"),
		CatalogDBURL:       getenv("CATALOG_DB_URL", "postgres://catalog_svc:catalog_dev@localhost:5432/catalog_db?sslmode=disable"),
		KafkaBrokers:       splitCSV(getenv("KAFKA_BROKERS", "localhost:29092")),
		ShutdownTimeoutSec: getint("SHUTDOWN_TIMEOUT_SEC", 10),
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
