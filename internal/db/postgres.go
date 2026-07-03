package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"vod/internal/models"
)

type DB struct {
	Pool *pgxpool.Pool
}

func Connect(ctx context.Context, url string) (*DB, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &DB{Pool: pool}, nil
}

func (d *DB) Close() {
	d.Pool.Close()
}

// InsertVideo creates the videos row and one pending encoding_jobs row per
// job type, all in one transaction. Called by the upload service after
// CompleteMultipartUpload succeeds.
func (d *DB) InsertVideo(ctx context.Context, id, title, storageKey string) error {
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO videos (id, title, storage_key) VALUES ($1, $2, $3)`,
		id, title, storageKey,
	)
	if err != nil {
		return fmt.Errorf("insert video: %w", err)
	}

	for _, jobType := range []models.JobType{models.JobTypeHLS, models.JobTypeThumbnail, models.JobTypeCaption} {
		_, err = tx.Exec(ctx,
			`INSERT INTO encoding_jobs (video_id, job_type) VALUES ($1, $2)
			 ON CONFLICT (video_id, job_type) DO NOTHING`,
			id, jobType,
		)
		if err != nil {
			return fmt.Errorf("insert encoding_job %s: %w", jobType, err)
		}
	}

	return tx.Commit(ctx)
}

func (d *DB) GetVideo(ctx context.Context, id string) (models.Video, error) {
	var v models.Video
	row := d.Pool.QueryRow(ctx,
		`SELECT id, title, storage_key, encoding_status, thumbnail_status, caption_status, created_at
		 FROM videos WHERE id = $1`, id)
	err := row.Scan(&v.ID, &v.Title, &v.StorageKey, &v.EncodingStatus, &v.ThumbnailStatus, &v.CaptionStatus, &v.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Video{}, ErrNotFound
	}
	if err != nil {
		return models.Video{}, fmt.Errorf("get video: %w", err)
	}
	return v, nil
}

var ErrNotFound = errors.New("not found")

// ListVideos returns the most recently created videos first, limit/offset
// paginated.
func (d *DB) ListVideos(ctx context.Context, limit, offset int) ([]models.Video, error) {
	rows, err := d.Pool.Query(ctx,
		`SELECT id, title, storage_key, encoding_status, thumbnail_status, caption_status, created_at
		 FROM videos ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("list videos: %w", err)
	}
	defer rows.Close()

	var videos []models.Video
	for rows.Next() {
		var v models.Video
		if err := rows.Scan(&v.ID, &v.Title, &v.StorageKey, &v.EncodingStatus, &v.ThumbnailStatus, &v.CaptionStatus, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan video: %w", err)
		}
		videos = append(videos, v)
	}
	return videos, rows.Err()
}

// DeleteVideo removes the videos row (cascading to encoding_jobs via the FK)
// and returns the storage_key so the caller can clean up object storage —
// deleting the DB row first means a half-finished storage cleanup never
// leaves an orphaned video record behind for the API to keep returning.
func (d *DB) DeleteVideo(ctx context.Context, id string) (storageKey string, err error) {
	row := d.Pool.QueryRow(ctx, `DELETE FROM videos WHERE id = $1 RETURNING storage_key`, id)
	if err := row.Scan(&storageKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("delete video: %w", err)
	}
	return storageKey, nil
}

// ResetJobForRetry puts a job (and its denormalized video status column)
// back to pending with a fresh attempt count, for manual reprocessing after
// a fix — e.g. the kind of DLQ'd job this project hit for real during
// development (odd rendition width, missing pix_fmt).
func (d *DB) ResetJobForRetry(ctx context.Context, videoID string, jobType models.JobType) error {
	col, err := jobStatusColumn(jobType)
	if err != nil {
		return err
	}
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx,
		`UPDATE encoding_jobs SET status = 'pending', attempt_count = 0, last_error = NULL,
		     started_at = NULL, completed_at = NULL, worker_id = NULL
		 WHERE video_id = $1 AND job_type = $2`,
		videoID, jobType,
	)
	if err != nil {
		return fmt.Errorf("reset job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	_, err = tx.Exec(ctx,
		fmt.Sprintf(`UPDATE videos SET %s = 'pending' WHERE id = $1`, col),
		videoID,
	)
	if err != nil {
		return fmt.Errorf("update video status: %w", err)
	}

	return tx.Commit(ctx)
}

// jobStatusColumn maps a job type to the denormalized status column on
// videos, which is what playability/UI reads check.
func jobStatusColumn(jobType models.JobType) (string, error) {
	switch jobType {
	case models.JobTypeHLS:
		return "encoding_status", nil
	case models.JobTypeThumbnail:
		return "thumbnail_status", nil
	case models.JobTypeCaption:
		return "caption_status", nil
	default:
		return "", fmt.Errorf("unknown job type %q", jobType)
	}
}

// ClaimJob marks a job as processing and bumps attempt_count, but only if it
// is not already completed. This makes duplicate delivery a safe no-op:
// a second delivery of an already-completed job claims nothing and the
// caller should treat zero rows affected as "already done, ACK and skip".
// The returned attempt count lets the caller decide retry vs DLQ policy.
func (d *DB) ClaimJob(ctx context.Context, videoID string, jobType models.JobType, workerID string) (claimed bool, attempt int, err error) {
	row := d.Pool.QueryRow(ctx,
		`UPDATE encoding_jobs
		 SET status = 'processing', attempt_count = attempt_count + 1,
		     started_at = now(), worker_id = $3
		 WHERE video_id = $1 AND job_type = $2 AND status != 'completed'
		 RETURNING attempt_count`,
		videoID, jobType, workerID,
	)
	if err := row.Scan(&attempt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("claim job: %w", err)
	}
	return true, attempt, nil
}

// CompleteJob marks the job and the corresponding video status column as
// completed in one transaction, called right before the worker ACKs.
func (d *DB) CompleteJob(ctx context.Context, videoID string, jobType models.JobType) error {
	col, err := jobStatusColumn(jobType)
	if err != nil {
		return err
	}
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`UPDATE encoding_jobs SET status = 'completed', completed_at = now()
		 WHERE video_id = $1 AND job_type = $2`,
		videoID, jobType,
	)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}

	_, err = tx.Exec(ctx,
		fmt.Sprintf(`UPDATE videos SET %s = 'completed' WHERE id = $1`, col),
		videoID,
	)
	if err != nil {
		return fmt.Errorf("update video status: %w", err)
	}

	return tx.Commit(ctx)
}

// FailJob records a failed attempt with the error message. The caller
// decides retry vs DLQ policy at the queue layer; this just records state.
func (d *DB) FailJob(ctx context.Context, videoID string, jobType models.JobType, errMsg string) error {
	col, err := jobStatusColumn(jobType)
	if err != nil {
		return err
	}
	tx, err := d.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`UPDATE encoding_jobs SET status = 'failed', last_error = $3
		 WHERE video_id = $1 AND job_type = $2`,
		videoID, jobType, errMsg,
	)
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}

	_, err = tx.Exec(ctx,
		fmt.Sprintf(`UPDATE videos SET %s = 'failed' WHERE id = $1`, col),
		videoID,
	)
	if err != nil {
		return fmt.Errorf("update video status: %w", err)
	}

	return tx.Commit(ctx)
}
