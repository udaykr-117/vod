package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"vod/internal/config"
	"vod/internal/db"
	"vod/internal/logging"
	"vod/internal/queue"
	"vod/internal/ratelimit"
	"vod/internal/session"
	"vod/internal/storage"
)

func main() {
	logging.Setup("upload-service")
	cfg := config.Load()

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

	srv := &Server{
		Storage:           store,
		Sessions:          session.NewRedisStore(cfg.RedisAddr),
		DB:                database,
		Queue:             q,
		APIKey:            cfg.APIKey,
		UploadRateLimiter: ratelimit.New(cfg.RedisAddr, cfg.UploadRateLimitPerMinute, time.Minute),
	}
	slog.Info("auth configuration", "api_key_enabled", srv.APIKey != "")
	slog.Info("upload rate limit configured", "per_minute", cfg.UploadRateLimitPerMinute)

	httpServer := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: srv.Routes(),
	}

	go func() {
		slog.Info("upload-service listening", "addr", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}
