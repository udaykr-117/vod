package worker

import (
	"context"
	"fmt"
	"io"
	"os"

	"vod/internal/storage"
)

// DownloadToTempFile fetches key from storage into a local temp file.
// ffmpeg needs a seekable file path (it can't transcode from an arbitrary
// io.Reader for most container formats), so every worker downloads the raw
// upload to local disk before invoking ffmpeg — independently, with no
// shared state between workers, per the architecture's worker-coupling
// trade-off.
func DownloadToTempFile(ctx context.Context, store storage.Storage, key, pattern string) (path string, cleanup func(), err error) {
	body, err := store.GetObject(ctx, key)
	if err != nil {
		return "", nil, fmt.Errorf("get object %s: %w", key, err)
	}
	defer body.Close()

	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", nil, fmt.Errorf("create temp file: %w", err)
	}
	cleanup = func() { os.Remove(f.Name()) }

	if _, err := io.Copy(f, body); err != nil {
		f.Close()
		cleanup()
		return "", nil, fmt.Errorf("copy to temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("close temp file: %w", err)
	}
	return f.Name(), cleanup, nil
}
