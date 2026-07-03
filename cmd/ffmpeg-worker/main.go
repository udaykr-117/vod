package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"vod/internal/config"
	"vod/internal/db"
	"vod/internal/logging"
	"vod/internal/metrics"
	"vod/internal/models"
	"vod/internal/queue"
	"vod/internal/storage"
	"vod/internal/worker"
)

func main() {
	logging.Setup("ffmpeg-worker")
	cfg := config.Load()
	metrics.ServeMetrics(":9090")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.NewS3Storage(ctx, storage.S3Config{
		Endpoint:       cfg.S3Endpoint,
		PublicEndpoint: cfg.S3PublicEndpoint,
		Region:         cfg.S3Region,
		AccessKey:      cfg.S3AccessKey,
		SecretKey:      cfg.S3SecretKey,
		Bucket:         cfg.S3Bucket,
	})
	if err != nil {
		slog.Error("init storage", "error", err)
		os.Exit(1)
	}

	database, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("connect db", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	q, err := queue.Dial(cfg.RabbitMQURL)
	if err != nil {
		slog.Error("connect queue", "error", err)
		os.Exit(1)
	}
	defer q.Close()

	process := func(ctx context.Context, event models.VideoUploadedEvent) error {
		inputPath, cleanupInput, err := worker.DownloadToTempFile(ctx, store, event.StorageKey, "ffmpeg-input-*.mp4")
		if err != nil {
			return fmt.Errorf("download raw upload: %w", err)
		}
		defer cleanupInput()

		outDir, err := os.MkdirTemp("", "ffmpeg-hls-*")
		if err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
		defer os.RemoveAll(outDir)

		if err := GenerateHLS(ctx, inputPath, outDir); err != nil {
			return fmt.Errorf("generate HLS: %w", err)
		}

		keyPrefix := fmt.Sprintf("renditions/%s", event.VideoID)
		if err := worker.UploadDir(ctx, store, outDir, keyPrefix); err != nil {
			return fmt.Errorf("upload renditions: %w", err)
		}

		return nil
	}

	workerID := cfg.WorkerID + "-ffmpeg"
	if err := worker.Run(ctx, q, database, queue.QueueHLS, models.JobTypeHLS, workerID, process); err != nil {
		slog.Error("worker run", "error", err)
		os.Exit(1)
	}
}
