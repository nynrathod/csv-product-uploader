package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nynrathod/csv-product-uploader/go-pipeline/internal/importjob"
)

// JobStore persists import jobs in the importer-owned import_db.
type JobStore struct {
	pool *pgxpool.Pool
}

// NewJobStore returns a JobStore backed by the given pool.
func NewJobStore(pool *pgxpool.Pool) *JobStore {
	return &JobStore{pool: pool}
}

const jobColumns = `id::text, file_name, status, total_rows, valid_rows, invalid_rows,
    published_rows, processed_rows, retried_rows, dead_rows, last_error, created_at, updated_at`

// rowScanner is satisfied by both single rows and row iterators.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(r rowScanner) (importjob.ImportJob, error) {
	var (
		job    importjob.ImportJob
		status string
	)
	err := r.Scan(
		&job.ID, &job.FileName, &status,
		&job.TotalRows, &job.ValidRows, &job.InvalidRows,
		&job.PublishedRows, &job.ProcessedRows, &job.RetriedRows, &job.DeadRows,
		&job.LastError, &job.CreatedAt, &job.UpdatedAt,
	)
	job.Status = importjob.Status(status)
	return job, err
}

// Create inserts a new import job, letting the database assign identity and
// timestamps.
func (s *JobStore) Create(ctx context.Context, job *importjob.ImportJob) error {
	return s.pool.QueryRow(ctx, `
        INSERT INTO import_jobs (file_name, status)
        VALUES ($1, $2)
        RETURNING id::text, created_at, updated_at`,
		job.FileName, string(job.Status),
	).Scan(&job.ID, &job.CreatedAt, &job.UpdatedAt)
}

// Get loads one import job by id.
func (s *JobStore) Get(ctx context.Context, id string) (importjob.ImportJob, error) {
	job, err := scanJob(s.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM import_jobs WHERE id = $1::uuid`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job, importjob.ErrNotFound
	}
	return job, err
}

// List returns the most recent import jobs, newest first.
func (s *JobStore) List(ctx context.Context, limit int) ([]importjob.ImportJob, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+jobColumns+` FROM import_jobs ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make([]importjob.ImportJob, 0, limit)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scanning import job: %w", err)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// UpdateProgress overwrites the row counters of a running job.
func (s *JobStore) UpdateProgress(ctx context.Context, id string, p importjob.Progress) error {
	tag, err := s.pool.Exec(ctx, `
        UPDATE import_jobs
        SET total_rows = $2, valid_rows = $3, invalid_rows = $4, published_rows = $5,
            updated_at = now()
        WHERE id = $1::uuid`,
		id, p.TotalRows, p.ValidRows, p.InvalidRows, p.PublishedRows)
	if err != nil {
		return err
	}
	return rowsAffected(tag, id)
}

// UpdateStatus applies a lifecycle transition and optionally records the
// failure cause.
func (s *JobStore) UpdateStatus(ctx context.Context, id string, to importjob.Status, lastErr *string) error {
	tag, err := s.pool.Exec(ctx, `
        UPDATE import_jobs
        SET status = $2::text, last_error = $3, updated_at = now()
        WHERE id = $1::uuid`,
		id, string(to), lastErr)
	if err != nil {
		return err
	}
	return rowsAffected(tag, id)
}

// FailStale marks every non-terminal job as failed with the given reason.
func (s *JobStore) FailStale(ctx context.Context, reason string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
        UPDATE import_jobs
        SET status = 'failed', last_error = $1, updated_at = now()
        WHERE status NOT IN ('completed', 'failed')`, reason)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func rowsAffected(tag pgconn.CommandTag, id string) error {
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", importjob.ErrNotFound, id)
	}
	return nil
}
