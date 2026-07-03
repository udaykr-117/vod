package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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

// selectTranscriber picks the best available backend at startup: OpenAI's
// hosted API if a key is configured (highest fidelity, costs money per
// request), otherwise the local whisper.cpp binary if it and its model are
// present in the image (real transcription, zero cost, zero network),
// falling back to the deterministic stub only if neither is available — so
// the pipeline never simply fails to start over a missing model.
func selectTranscriber(cfg config.Config) Transcriber {
	if cfg.WhisperAPIKey != "" {
		slog.Info("whisper backend selected", "backend", "openai")
		return OpenAIWhisper{APIKey: cfg.WhisperAPIKey}
	}
	if fileExists(cfg.WhisperCppBin) && fileExists(cfg.WhisperCppModel) {
		slog.Info("whisper backend selected", "backend", "whisper.cpp", "bin", cfg.WhisperCppBin, "model", cfg.WhisperCppModel)
		return LocalWhisperCpp{BinPath: cfg.WhisperCppBin, ModelPath: cfg.WhisperCppModel}
	}
	slog.Warn("whisper backend selected", "backend", "stub", "reason", "no API key and no local binary/model found")
	return StubTranscriber{}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func main() {
	logging.Setup("whisper-worker")
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

	transcriber := selectTranscriber(cfg)

	process := func(ctx context.Context, event models.VideoUploadedEvent) error {
		inputPath, cleanupInput, err := worker.DownloadToTempFile(ctx, store, event.StorageKey, "whisper-input-*.mp4")
		if err != nil {
			return fmt.Errorf("download raw upload: %w", err)
		}
		defer cleanupInput()

		hasAudio, err := HasAudioStream(ctx, inputPath)
		if err != nil {
			return fmt.Errorf("probe audio stream: %w", err)
		}

		var vtt string
		if !hasAudio {
			// Silent video is a legitimate upload, not a failure — caption
			// status still completes, just with an empty track.
			duration, err := probeDuration(ctx, inputPath)
			if err != nil {
				return fmt.Errorf("probe duration: %w", err)
			}
			vtt = fmt.Sprintf("WEBVTT\n\n00:00:00.000 --> %s\n[no audio track]\n", formatVTTTimestamp(duration))
		} else {
			audioPath := strings.TrimSuffix(inputPath, filepath.Ext(inputPath)) + ".wav"
			if err := ExtractAudio(ctx, inputPath, audioPath); err != nil {
				return fmt.Errorf("extract audio: %w", err)
			}
			defer os.Remove(audioPath)

			vtt, err = transcriber.Transcribe(ctx, audioPath)
			if err != nil {
				return fmt.Errorf("transcribe: %w", err)
			}
		}

		key := fmt.Sprintf("captions/%s/captions.vtt", event.VideoID)
		if err := store.PutObject(ctx, key, strings.NewReader(vtt), int64(len(vtt)), "text/vtt"); err != nil {
			return fmt.Errorf("upload captions: %w", err)
		}

		return nil
	}

	workerID := cfg.WorkerID + "-whisper"
	if err := worker.Run(ctx, q, database, queue.QueueCaption, models.JobTypeCaption, workerID, process); err != nil {
		slog.Error("worker run", "error", err)
		os.Exit(1)
	}
}
