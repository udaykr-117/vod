package worker

import (
	"context"
	"fmt"
	"mime"
	"os"
	"path/filepath"

	"vod/internal/storage"
)

// hlsContentTypes overrides Go's mime package, which doesn't know about HLS
// extensions on a minimal Linux image and would otherwise serve playlists
// and segments as application/octet-stream — many players refuse to play
// HLS served with the wrong content type.
var hlsContentTypes = map[string]string{
	".m3u8": "application/vnd.apple.mpegurl",
	".ts":   "video/mp2t",
}

// UploadDir walks localDir and PUTs every file to storage under
// keyPrefix/<relative path>, preserving the directory structure HLS output
// needs (master playlist + per-rendition playlists + .ts segments).
func UploadDir(ctx context.Context, store storage.Storage, localDir, keyPrefix string) error {
	return filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(localDir, path)
		if err != nil {
			return fmt.Errorf("relative path for %s: %w", path, err)
		}
		key := keyPrefix + "/" + filepath.ToSlash(rel)

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		defer f.Close()

		ext := filepath.Ext(path)
		contentType := hlsContentTypes[ext]
		if contentType == "" {
			contentType = mime.TypeByExtension(ext)
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		if err := store.PutObject(ctx, key, f, info.Size(), contentType); err != nil {
			return fmt.Errorf("put object %s: %w", key, err)
		}
		return nil
	})
}
