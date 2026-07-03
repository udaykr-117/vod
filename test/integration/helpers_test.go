//go:build integration

// Package integration exercises the system against the live docker-compose
// stack (Postgres, Redis, MinIO, RabbitMQ) — no mocks. Run with:
//
//	go test -tags=integration ./test/integration/...
//
// Stop any manually-running upload-service/worker processes first: they
// compete for the same RabbitMQ queues as these tests' own consumers.
package integration

import (
	"context"
	"testing"
	"time"

	"vod/internal/config"
	"vod/internal/db"
	"vod/internal/queue"
	"vod/internal/session"
	"vod/internal/storage"

	"github.com/google/uuid"
)

func testStorage(t *testing.T, ctx context.Context) storage.Storage {
	cfg := config.Load()
	s, err := storage.NewS3Storage(ctx, storage.S3Config{
		Endpoint:       cfg.S3Endpoint,
		PublicEndpoint: cfg.S3PublicEndpoint,
		Region:         cfg.S3Region,
		AccessKey:      cfg.S3AccessKey,
		SecretKey:      cfg.S3SecretKey,
		Bucket:         cfg.S3Bucket,
	})
	if err != nil {
		t.Fatalf("init storage: %v", err)
	}
	return s
}

func testDB(t *testing.T, ctx context.Context) *db.DB {
	cfg := config.Load()
	d, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect db: %v", err)
	}
	t.Cleanup(d.Close)
	return d
}

func testQueue(t *testing.T) *queue.Conn {
	cfg := config.Load()
	q, err := queue.Dial(cfg.RabbitMQURL)
	if err != nil {
		t.Fatalf("connect queue: %v", err)
	}
	t.Cleanup(q.Close)
	return q
}

func testSessionStore(t *testing.T) session.Store {
	cfg := config.Load()
	return session.NewRedisStore(cfg.RedisAddr)
}

// insertTestVideo creates a video + its 3 pending encoding_jobs rows and
// registers cleanup, mirroring what the upload service does on completion.
func insertTestVideo(t *testing.T, ctx context.Context, d *db.DB) (videoID, storageKey string) {
	videoID = uuid.NewString()
	storageKey = "raw/" + videoID + ".mp4"
	if err := d.InsertVideo(ctx, videoID, "Integration Test Video", storageKey); err != nil {
		t.Fatalf("insert video: %v", err)
	}
	t.Cleanup(func() {
		_, _ = d.Pool.Exec(context.Background(), `DELETE FROM videos WHERE id = $1`, videoID)
	})
	return videoID, storageKey
}

func newTestSession(uploadID string) session.UploadSession {
	return session.UploadSession{
		UploadID:   uploadID,
		StorageKey: "raw/" + uploadID + ".mp4",
		S3UploadID: "fake-s3-upload-id",
		PartSize:   8 * 1024 * 1024,
		PartCount:  1,
		Title:      "TTL Test Video",
	}
}

func withTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx, cancel
}
