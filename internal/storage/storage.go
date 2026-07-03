package storage

import (
	"context"
	"io"
	"time"
)

// Part describes one received part of a multipart upload, as reported by
// the storage backend's ListParts call. This is the source of truth for
// upload resume — not Redis, not Postgres.
type Part struct {
	PartNumber int32
	ETag       string
	Size       int64
}

// Storage is the single interface the rest of the system depends on. Exactly
// one backend is active at a time (MinIO locally, Cloudflare R2 in prod).
// Chunk bytes never pass through the Go server — only presigned URLs do.
type Storage interface {
	// CreateMultipartUpload starts a multipart upload for key and returns the
	// backend's upload ID.
	CreateMultipartUpload(ctx context.Context, key string) (uploadID string, err error)

	// PresignUploadPart returns a presigned PUT URL the browser uses to
	// upload one part's bytes directly to storage.
	PresignUploadPart(ctx context.Context, key, uploadID string, partNumber int32, expires time.Duration) (url string, err error)

	// ListParts returns the parts received so far for an in-progress
	// multipart upload. Used to compute resume state.
	ListParts(ctx context.Context, key, uploadID string) ([]Part, error)

	// CompleteMultipartUpload finalizes the upload given the parts received.
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []Part) error

	// AbortMultipartUpload cancels an in-progress multipart upload.
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error

	// GetObject downloads an object's bytes. Used by workers to fetch the
	// raw upload independently.
	GetObject(ctx context.Context, key string) (io.ReadCloser, error)

	// PutObject uploads bytes to key. Used by workers to write derived
	// artifacts (renditions, thumbnails, captions).
	PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) error

	// DeleteObjectsByPrefix removes every object under prefix. Object storage
	// has no native "delete a folder" operation, so this lists then batch
	// deletes — used to clean up a video's raw upload, renditions,
	// thumbnails, and captions in one call when the video is deleted.
	DeleteObjectsByPrefix(ctx context.Context, prefix string) error
}
