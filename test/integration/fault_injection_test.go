//go:build integration

package integration

import (
	"testing"
	"time"

	"vod/internal/models"

	"github.com/google/uuid"
)

// TestDuplicateJobDeliveryIsIdempotent simulates RabbitMQ redelivering a
// message for a job that has already completed (e.g. an ACK was lost on the
// network after the worker's DB commit succeeded). The second claim must be
// a no-op, not a second transcode/upload.
func TestDuplicateJobDeliveryIsIdempotent(t *testing.T) {
	ctx, _ := withTimeout(t)
	d := testDB(t, ctx)
	videoID, _ := insertTestVideo(t, ctx, d)

	claimed, attempt, err := d.ClaimJob(ctx, videoID, models.JobTypeHLS, "worker-a")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed || attempt != 1 {
		t.Fatalf("expected first claim to succeed with attempt=1, got claimed=%v attempt=%d", claimed, attempt)
	}

	if err := d.CompleteJob(ctx, videoID, models.JobTypeHLS); err != nil {
		t.Fatalf("complete job: %v", err)
	}

	// Redelivery of the same message after completion.
	claimed, _, err = d.ClaimJob(ctx, videoID, models.JobTypeHLS, "worker-b")
	if err != nil {
		t.Fatalf("duplicate claim: %v", err)
	}
	if claimed {
		t.Fatalf("expected duplicate delivery of a completed job to NOT be claimable, but it was")
	}

	video, err := d.GetVideo(ctx, videoID)
	if err != nil {
		t.Fatalf("get video: %v", err)
	}
	if video.EncodingStatus != models.StatusCompleted {
		t.Fatalf("expected encoding_status=completed, got %s", video.EncodingStatus)
	}
}

// TestWorkerCrashBeforeAckIsSafelyRedelivered simulates a worker that
// crashes after claiming a job but before completing it (i.e. before the
// commit that would make the ACK valid). RabbitMQ would redeliver in this
// scenario; the redelivered claim must still succeed (status stays
// "processing", not "completed"), and the attempt count must reflect both
// tries — no job is silently lost or double-completed.
func TestWorkerCrashBeforeAckIsSafelyRedelivered(t *testing.T) {
	ctx, _ := withTimeout(t)
	d := testDB(t, ctx)
	videoID, _ := insertTestVideo(t, ctx, d)

	claimed, attempt, err := d.ClaimJob(ctx, videoID, models.JobTypeThumbnail, "worker-crash")
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !claimed || attempt != 1 {
		t.Fatalf("expected claimed=true attempt=1, got claimed=%v attempt=%d", claimed, attempt)
	}
	// Simulate crash here: no CompleteJob, no ACK. RabbitMQ would requeue
	// and redeliver to another consumer.

	claimed, attempt, err = d.ClaimJob(ctx, videoID, models.JobTypeThumbnail, "worker-recovery")
	if err != nil {
		t.Fatalf("redelivery claim: %v", err)
	}
	if !claimed {
		t.Fatalf("expected redelivered claim to succeed since job never completed")
	}
	if attempt != 2 {
		t.Fatalf("expected attempt_count=2 after redelivery, got %d", attempt)
	}

	if err := d.CompleteJob(ctx, videoID, models.JobTypeThumbnail); err != nil {
		t.Fatalf("complete job after recovery: %v", err)
	}

	video, err := d.GetVideo(ctx, videoID)
	if err != nil {
		t.Fatalf("get video: %v", err)
	}
	if video.ThumbnailStatus != models.StatusCompleted {
		t.Fatalf("expected thumbnail_status=completed, got %s", video.ThumbnailStatus)
	}
}

// TestSessionTTLExpiry confirms an upload session disappears once its TTL
// elapses, per the architecture's "Redis for upload sessions ... TTL-based
// ephemeral session expiry" decision (see arch.md Decisions #3).
func TestSessionTTLExpiry(t *testing.T) {
	ctx, _ := withTimeout(t)
	store := testSessionStore(t)

	uploadID := uuid.NewString()
	sess := newTestSession(uploadID)

	if err := store.Create(ctx, sess, 1*time.Second); err != nil {
		t.Fatalf("create session: %v", err)
	}

	if _, err := store.Get(ctx, uploadID); err != nil {
		t.Fatalf("expected session to be readable immediately after creation: %v", err)
	}

	time.Sleep(2 * time.Second)

	if _, err := store.Get(ctx, uploadID); err == nil {
		t.Fatalf("expected session to be expired and gone after TTL elapsed")
	}
}
