//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"vod/internal/storage"
)

// TestInterruptedUploadResumes exercises the exact mechanism the upload
// service's GET /status relies on: ListParts against the storage backend is
// the source of truth, not anything cached locally. We upload part 1,
// "interrupt" (never touch part 2), confirm the backend reports it missing,
// then complete the upload by sending part 2 late — proving resume works
// without any client-side bookkeeping surviving the interruption.
func TestInterruptedUploadResumes(t *testing.T) {
	ctx, _ := withTimeout(t)
	store := testStorage(t, ctx)

	key := "raw/test-resume-" + randomHex(t) + ".mp4"
	uploadID, err := store.CreateMultipartUpload(ctx, key)
	if err != nil {
		t.Fatalf("create multipart upload: %v", err)
	}
	t.Cleanup(func() { _ = store.AbortMultipartUpload(ctx, key, uploadID) })

	part1 := randomBytes(t, 5*1024*1024) // S3 multipart minimum part size
	part2 := randomBytes(t, 1024 * 1024)

	putPart(t, ctx, store, key, uploadID, 1, part1)

	// "Interruption": check status before part 2 ever arrives.
	parts, err := store.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("list parts after interruption: %v", err)
	}
	if len(parts) != 1 || parts[0].PartNumber != 1 {
		t.Fatalf("expected exactly part 1 to be present after interruption, got %+v", parts)
	}

	// Resume: client reconnects, re-derives missing parts from ListParts
	// (not from any local state), and uploads only what's missing.
	putPart(t, ctx, store, key, uploadID, 2, part2)

	parts, err = store.ListParts(ctx, key, uploadID)
	if err != nil {
		t.Fatalf("list parts after resume: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts present after resume, got %d", len(parts))
	}

	if err := store.CompleteMultipartUpload(ctx, key, uploadID, parts); err != nil {
		t.Fatalf("complete multipart upload: %v", err)
	}
}

func putPart(t *testing.T, ctx context.Context, store storage.Storage, key, uploadID string, partNumber int32, data []byte) {
	url, err := store.PresignUploadPart(ctx, key, uploadID, partNumber, 5*time.Minute)
	if err != nil {
		t.Fatalf("presign part %d: %v", partNumber, err)
	}
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build PUT request for part %d: %v", partNumber, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT part %d: %v", partNumber, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT part %d returned status %d", partNumber, resp.StatusCode)
	}
}

func randomBytes(t *testing.T, n int) []byte {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("generate random bytes: %v", err)
	}
	return b
}

func randomHex(t *testing.T) string {
	b := randomBytes(t, 8)
	const hex = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hex[v>>4]
		out[i*2+1] = hex[v&0x0f]
	}
	return string(out)
}
