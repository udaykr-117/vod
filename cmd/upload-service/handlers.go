package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"vod/internal/db"
	"vod/internal/metrics"
	"vod/internal/models"
	"vod/internal/queue"
	"vod/internal/ratelimit"
	"vod/internal/session"
	"vod/internal/storage"
)

// minPartSize matches S3's multipart minimum (5MB) for every part except
// the last one, which may be smaller.
const minPartSize int64 = 5 * 1024 * 1024
const defaultPartSize int64 = 8 * 1024 * 1024
const sessionTTL = 24 * time.Hour
const presignExpiry = 15 * time.Minute

type Server struct {
	Storage  storage.Storage
	Sessions session.Store
	DB       *db.DB
	Queue    *queue.Conn
	// APIKey, when non-empty, is required as "Authorization: Bearer <key>"
	// on every route except /healthz. Left empty, the API stays open — the
	// same as before this was added — so existing local-dev setups aren't
	// broken by default; set API_KEY to opt in.
	APIKey string

	// UploadRateLimiter caps how often one client IP can call POST /uploads
	// — the one route that does real work against the storage backend
	// before any other validation. Nil disables rate limiting entirely.
	UploadRateLimiter *ratelimit.Limiter
}

func (s *Server) Routes() chi.Router {
	r := chi.NewRouter()
	r.Use(corsMiddleware)
	r.Use(metricsMiddleware)
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	r.Get("/metrics", promhttp.Handler().ServeHTTP)

	r.Group(func(r chi.Router) {
		r.Use(s.authMiddleware)
		r.With(s.rateLimitMiddleware).Post("/uploads", s.handleCreateUpload)
		r.Get("/uploads/{id}/parts/{n}/presigned-url", s.handlePresignPart)
		r.Get("/uploads/{id}/status", s.handleStatus)
		r.Post("/uploads/{id}/complete", s.handleComplete)
		r.Get("/videos", s.handleListVideos)
		r.Get("/videos/{id}", s.handleGetVideo)
		r.Delete("/videos/{id}", s.handleDeleteVideo)
		r.Post("/videos/{id}/retry", s.handleRetryJob)
	})

	return r
}

// authMiddleware enforces "Authorization: Bearer <API_KEY>" only when
// s.APIKey is configured. CORS preflight (OPTIONS) requests never reach
// here — corsMiddleware answers those directly — so this only ever sees
// real requests that already carry whatever headers the browser was
// allowed to send.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.APIKey == "" {
			next.ServeHTTP(w, r)
			return
		}
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got == "" || got != s.APIKey {
			writeError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware caps requests per client IP using the fixed-window
// counter in s.UploadRateLimiter. A nil limiter (rate limiting disabled, or
// Redis unreachable at startup) means this is a no-op — the same
// fail-open posture as APIKey being unset, so a Redis hiccup degrades to
// "no rate limiting" rather than taking the upload endpoint down entirely.
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.UploadRateLimiter == nil {
			next.ServeHTTP(w, r)
			return
		}
		ip := clientIP(r)
		allowed, retryAfter, err := s.UploadRateLimiter.Allow(r.Context(), ip)
		if err != nil {
			// Limiter itself failing (e.g. Redis down) should not block real
			// uploads — log and let the request through.
			slog.Error("rate limiter check failed, allowing request", "ip", ip, "error", err)
			next.ServeHTTP(w, r)
			return
		}
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded, try again later")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP prefers X-Forwarded-For (set by the TLS-terminating reverse
// proxy in front of this service) over RemoteAddr, which would otherwise
// always be the proxy's own address once one is introduced.
func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return strings.TrimSpace(strings.Split(fwd, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// statusRecorder captures the status code a handler wrote, since
// http.ResponseWriter doesn't expose it directly.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// metricsMiddleware records request counts/latency by method and matched
// route pattern (e.g. "/videos/{id}", not the literal path with a real ID
// in it — that would create unbounded label cardinality in Prometheus).
// Reading the pattern after next.ServeHTTP returns works because chi
// mutates its RouteContext in place during routing/dispatch, and that
// context is attached to the request before the handler chain runs.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPRequestsTotal.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}

// corsMiddleware allows the plain HTML test client (served from a different
// origin/port, or opened as a file://) to call this API directly. Local dev
// only — a real deployment would scope this to the actual frontend origin.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type createUploadRequest struct {
	Title         string `json:"title"`
	FileSizeBytes int64  `json:"file_size_bytes"`
	ContentType   string `json:"content_type"`
}

type createUploadResponse struct {
	UploadID  string `json:"upload_id"`
	PartSize  int64  `json:"part_size"`
	PartCount int32  `json:"part_count"`
}

func (s *Server) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	var req createUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Title == "" || req.FileSizeBytes <= 0 {
		writeError(w, http.StatusBadRequest, "title and file_size_bytes are required")
		return
	}

	uploadID := uuid.NewString()
	storageKey := fmt.Sprintf("raw/%s.mp4", uploadID)

	s3UploadID, err := s.Storage.CreateMultipartUpload(r.Context(), storageKey)
	if err != nil {
		slog.Error("create multipart upload", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to initiate upload")
		return
	}

	partSize := defaultPartSize
	partCount := int32((req.FileSizeBytes + partSize - 1) / partSize)
	if partCount == 0 {
		partCount = 1
	}

	sess := session.UploadSession{
		UploadID:   uploadID,
		StorageKey: storageKey,
		S3UploadID: s3UploadID,
		PartSize:   partSize,
		PartCount:  partCount,
		Title:      req.Title,
	}
	if err := s.Sessions.Create(r.Context(), sess, sessionTTL); err != nil {
		slog.Error("create session", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create upload session")
		return
	}

	writeJSON(w, http.StatusCreated, createUploadResponse{
		UploadID:  uploadID,
		PartSize:  partSize,
		PartCount: partCount,
	})
}

func (s *Server) loadSession(ctx context.Context, w http.ResponseWriter, uploadID string) (session.UploadSession, bool) {
	sess, err := s.Sessions.Get(ctx, uploadID)
	if errors.Is(err, session.ErrNotFound) {
		writeError(w, http.StatusNotFound, "upload session not found or expired")
		return session.UploadSession{}, false
	}
	if err != nil {
		slog.Error("get session", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load upload session")
		return session.UploadSession{}, false
	}
	return sess, true
}

type presignResponse struct {
	URL string `json:"url"`
}

func (s *Server) handlePresignPart(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "id")
	var partNumber int32
	if _, err := fmt.Sscanf(chi.URLParam(r, "n"), "%d", &partNumber); err != nil || partNumber < 1 {
		writeError(w, http.StatusBadRequest, "invalid part number")
		return
	}

	sess, ok := s.loadSession(r.Context(), w, uploadID)
	if !ok {
		return
	}
	if partNumber > sess.PartCount {
		writeError(w, http.StatusBadRequest, "part number exceeds part count")
		return
	}

	url, err := s.Storage.PresignUploadPart(r.Context(), sess.StorageKey, sess.S3UploadID, partNumber, presignExpiry)
	if err != nil {
		slog.Error("presign part", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to presign part")
		return
	}
	writeJSON(w, http.StatusOK, presignResponse{URL: url})
}

type statusResponse struct {
	UploadID       string  `json:"upload_id"`
	PartCount      int32   `json:"part_count"`
	ReceivedParts  []int32 `json:"received_parts"`
	MissingParts   []int32 `json:"missing_parts"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "id")
	sess, ok := s.loadSession(r.Context(), w, uploadID)
	if !ok {
		return
	}

	// ListParts against the storage backend is the source of truth for
	// which parts arrived — not anything cached locally. This is what
	// makes resuming an interrupted upload correct.
	parts, err := s.Storage.ListParts(r.Context(), sess.StorageKey, sess.S3UploadID)
	if err != nil {
		slog.Error("list parts", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list parts")
		return
	}

	received := make(map[int32]bool, len(parts))
	receivedList := make([]int32, 0, len(parts))
	for _, p := range parts {
		received[p.PartNumber] = true
		receivedList = append(receivedList, p.PartNumber)
	}
	var missing []int32
	for n := int32(1); n <= sess.PartCount; n++ {
		if !received[n] {
			missing = append(missing, n)
		}
	}

	writeJSON(w, http.StatusOK, statusResponse{
		UploadID:      uploadID,
		PartCount:     sess.PartCount,
		ReceivedParts: receivedList,
		MissingParts:  missing,
	})
}

func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request) {
	uploadID := chi.URLParam(r, "id")
	sess, ok := s.loadSession(r.Context(), w, uploadID)
	if !ok {
		return
	}

	parts, err := s.Storage.ListParts(r.Context(), sess.StorageKey, sess.S3UploadID)
	if err != nil {
		slog.Error("list parts", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list parts")
		return
	}
	if int32(len(parts)) != sess.PartCount {
		writeError(w, http.StatusConflict, fmt.Sprintf("upload incomplete: %d/%d parts received", len(parts), sess.PartCount))
		return
	}

	if err := s.Storage.CompleteMultipartUpload(r.Context(), sess.StorageKey, sess.S3UploadID, parts); err != nil {
		slog.Error("complete multipart upload", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to complete upload")
		return
	}

	if err := s.DB.InsertVideo(r.Context(), uploadID, sess.Title, sess.StorageKey); err != nil {
		slog.Error("insert video", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to record video")
		return
	}

	event := models.VideoUploadedEvent{VideoID: uploadID, StorageKey: sess.StorageKey}
	if err := s.Queue.Publish(r.Context(), event); err != nil {
		// The video row exists and the bytes are durably stored; a missed
		// publish just means processing doesn't start automatically. Log
		// loudly rather than fail the request, since the upload itself
		// fully succeeded.
		slog.Error("publish video.uploaded", "video_id", uploadID, "error", err)
	}

	_ = s.Sessions.Delete(r.Context(), uploadID)

	video, err := s.DB.GetVideo(r.Context(), uploadID)
	if err != nil {
		slog.Error("get video after complete", "error", err)
		writeError(w, http.StatusInternalServerError, "video completed but failed to load")
		return
	}
	writeJSON(w, http.StatusOK, video)
}

func (s *Server) handleGetVideo(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	video, err := s.DB.GetVideo(r.Context(), id)
	if errors.Is(err, db.ErrNotFound) {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}
	if err != nil {
		slog.Error("get video", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load video")
		return
	}
	writeJSON(w, http.StatusOK, video)
}

const defaultListLimit = 50
const maxListLimit = 200

func (s *Server) handleListVideos(w http.ResponseWriter, r *http.Request) {
	limit := defaultListLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= maxListLimit {
			limit = n
		}
	}
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	videos, err := s.DB.ListVideos(r.Context(), limit, offset)
	if err != nil {
		slog.Error("list videos", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to list videos")
		return
	}
	writeJSON(w, http.StatusOK, videos)
}

// storagePrefixesForVideo enumerates every object-storage location this
// video could have written to. Object storage has no "delete a folder"
// primitive, so the raw upload and each artifact directory are deleted as
// separate prefix sweeps.
func storagePrefixesForVideo(videoID string) []string {
	return []string{
		fmt.Sprintf("raw/%s", videoID),
		fmt.Sprintf("renditions/%s/", videoID),
		fmt.Sprintf("thumbnails/%s/", videoID),
		fmt.Sprintf("captions/%s/", videoID),
	}
}

func (s *Server) handleDeleteVideo(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Delete the DB row first: if storage cleanup below fails partway, the
	// video is already gone from the API's perspective rather than left as
	// a zombie record that still appears to exist. Any orphaned storage
	// objects from a failed cleanup are harmless — nothing references them.
	if _, err := s.DB.DeleteVideo(r.Context(), id); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			writeError(w, http.StatusNotFound, "video not found")
			return
		}
		slog.Error("delete video", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to delete video")
		return
	}

	for _, prefix := range storagePrefixesForVideo(id) {
		if err := s.Storage.DeleteObjectsByPrefix(r.Context(), prefix); err != nil {
			// DB row is already gone; log loudly but don't fail the request
			// over leftover bytes the API will never reference again.
			slog.Error("cleanup storage prefix for deleted video", "prefix", prefix, "video_id", id, "error", err)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	jobTypeParam := r.URL.Query().Get("job_type")

	var jobType models.JobType
	switch jobTypeParam {
	case string(models.JobTypeHLS), string(models.JobTypeThumbnail), string(models.JobTypeCaption):
		jobType = models.JobType(jobTypeParam)
	default:
		writeError(w, http.StatusBadRequest, "job_type must be one of: hls, thumbnail, caption")
		return
	}

	video, err := s.DB.GetVideo(r.Context(), id)
	if errors.Is(err, db.ErrNotFound) {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}
	if err != nil {
		slog.Error("get video for retry", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load video")
		return
	}

	if err := s.DB.ResetJobForRetry(r.Context(), id, jobType); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			writeError(w, http.StatusNotFound, "job not found for this video")
			return
		}
		slog.Error("reset job for retry", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to reset job")
		return
	}

	// Re-publish to the fanout exchange. Every worker type receives this,
	// but only the one matching jobType will find a non-completed job to
	// claim — the others will see their own job already completed and
	// skip it, per the same idempotency that makes duplicate delivery safe
	// everywhere else in this system.
	event := models.VideoUploadedEvent{VideoID: id, StorageKey: video.StorageKey}
	if err := s.Queue.Publish(r.Context(), event); err != nil {
		slog.Error("publish retry event", "video_id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "job reset but failed to publish retry event")
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
