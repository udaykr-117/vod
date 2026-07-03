package session

import (
	"context"
	"time"
)

// UploadSession holds ephemeral metadata for an in-progress multipart
// upload. This is recreatable from the storage backend's multipart upload
// state plus the videos table — it is never the only copy of anything
// durable, which is why it is allowed to expire via TTL.
type UploadSession struct {
	UploadID   string `json:"upload_id"`
	StorageKey string `json:"storage_key"`
	S3UploadID string `json:"s3_upload_id"`
	PartSize   int64  `json:"part_size"`
	PartCount  int32  `json:"part_count"`
	Title      string `json:"title"`
}

// Store is a small interface over the upload-session backing store so the
// implementation (Redis today) can change without touching HTTP handlers.
type Store interface {
	Create(ctx context.Context, sess UploadSession, ttl time.Duration) error
	Get(ctx context.Context, uploadID string) (UploadSession, error)
	Delete(ctx context.Context, uploadID string) error
}

var ErrNotFound = sessionNotFoundError{}

type sessionNotFoundError struct{}

func (sessionNotFoundError) Error() string { return "upload session not found or expired" }
