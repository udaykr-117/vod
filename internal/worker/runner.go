package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"vod/internal/db"
	"vod/internal/metrics"
	"vod/internal/models"
	"vod/internal/queue"
)

// MaxAttempts bounds retries before a job is dead-lettered. encoding_jobs
// tracks attempt_count durably, so this policy survives worker restarts —
// a crashed worker's redelivered message still sees the real attempt count.
const MaxAttempts = 3

// ProcessFunc does the actual work (download, transcode/thumbnail/transcribe,
// upload) for one video. Returning an error marks the attempt failed; the
// runner decides retry vs DLQ.
type ProcessFunc func(ctx context.Context, event models.VideoUploadedEvent) error

// Run consumes queueName forever, claiming each job in Postgres before doing
// any work and committing completion/failure before ACKing — so a crash
// between delivery and DB commit always results in safe redelivery, never
// a silently lost or double-counted job.
func Run(ctx context.Context, q *queue.Conn, database *db.DB, queueName string, jobType models.JobType, workerID string, process ProcessFunc) error {
	deliveries, err := q.Consume(queueName, workerID)
	if err != nil {
		return err
	}

	slog.Info("consuming queue", "worker_id", workerID, "queue", queueName, "job_type", jobType)

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			handleDelivery(ctx, database, jobType, workerID, process, d)
		}
	}
}

func handleDelivery(ctx context.Context, database *db.DB, jobType models.JobType, workerID string, process ProcessFunc, d queue.Delivery) {
	var event models.VideoUploadedEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("malformed message, dead-lettering", "worker_id", workerID, "error", err)
		_ = d.Nack(false, false)
		return
	}
	log := slog.With("worker_id", workerID, "video_id", event.VideoID, "job_type", jobType)

	claimed, attempt, err := database.ClaimJob(ctx, event.VideoID, jobType, workerID)
	if err != nil {
		log.Error("claim job failed, requeueing", "error", err)
		_ = d.Nack(false, true)
		return
	}
	if !claimed {
		// Already completed — duplicate delivery is a no-op, not an error.
		log.Info("job already completed, skipping")
		metrics.JobsProcessedTotal.WithLabelValues(string(jobType), "skipped_duplicate").Inc()
		_ = d.Ack(false)
		return
	}

	start := time.Now()
	if err := process(ctx, event); err != nil {
		log.Warn("job attempt failed", "attempt", attempt, "error", err)
		if dbErr := database.FailJob(ctx, event.VideoID, jobType, err.Error()); dbErr != nil {
			log.Error("failed to record failure", "error", dbErr)
		}
		if attempt < MaxAttempts {
			_ = d.Nack(false, true) // transient: requeue for another attempt
		} else {
			metrics.JobsProcessedTotal.WithLabelValues(string(jobType), "failed").Inc()
			metrics.JobDuration.WithLabelValues(string(jobType)).Observe(time.Since(start).Seconds())
			_ = d.Nack(false, false) // exhausted retries: dead-letter
		}
		return
	}

	if err := database.CompleteJob(ctx, event.VideoID, jobType); err != nil {
		log.Error("failed to record completion, requeueing", "error", err)
		_ = d.Nack(false, true)
		return
	}

	metrics.JobsProcessedTotal.WithLabelValues(string(jobType), "completed").Inc()
	metrics.JobDuration.WithLabelValues(string(jobType)).Observe(time.Since(start).Seconds())
	log.Info("job completed", "attempt", attempt)

	// ACK only after the DB commit above succeeds — a crash before this
	// point means RabbitMQ redelivers, and ClaimJob/CompleteJob make that
	// redelivery idempotent rather than a duplicate side effect.
	_ = d.Ack(false)
}
