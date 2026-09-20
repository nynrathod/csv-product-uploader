// Package postgres implements the database adapters of both services.
package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrateLockID identifies this project's migration advisory lock. An
// advisory lock serializes concurrent migrators, e.g. two service replicas
// starting at once, so a migration is applied exactly once.
const migrateLockID = 721001

// Migrate applies every pending .sql file in dir, in filename order,
// exactly once. Applied versions are tracked in schema_migrations inside
// the service's own database.
func Migrate(ctx context.Context, pool *pgxpool.Pool, dir string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockID); err != nil {
		return fmt.Errorf("acquiring migration lock: %w", err)
	}
	defer conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, migrateLockID)

	if _, err := conn.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    text        PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )`); err != nil {
		return fmt.Errorf("ensuring schema_migrations: %w", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading migrations dir %s: %w", dir, err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var applied bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name,
		).Scan(&applied); err != nil {
			return fmt.Errorf("checking migration %s: %w", name, err)
		}
		if applied {
			continue
		}

		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(content)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("applying migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("recording migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing migration %s: %w", name, err)
		}
	}
	return nil
}
