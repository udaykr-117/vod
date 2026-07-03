package config

import (
	"os"
	"strconv"
)

type Config struct {
	HTTPAddr string

	DatabaseURL string

	RedisAddr string

	S3Endpoint  string
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string
	// S3PublicEndpoint is used to build presigned URLs returned to browsers.
	// Inside Docker, services reach MinIO at "minio:9000"; the browser must use
	// the host-published address instead, so this is configured separately.
	S3PublicEndpoint string

	RabbitMQURL string

	WhisperAPIKey   string
	WhisperCppBin   string
	WhisperCppModel string
	WorkerID        string

	// APIKey, when set, is required on upload-service routes. Left unset,
	// the API stays open (existing local-dev behavior unchanged).
	APIKey string

	// UploadRateLimitPerMinute caps POST /uploads per client IP.
	UploadRateLimitPerMinute int
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func Load() Config {
	return Config{
		HTTPAddr:                 envOr("HTTP_ADDR", ":8080"),
		DatabaseURL:              envOr("DATABASE_URL", "postgres://vod:vod@localhost:5433/vod?sslmode=disable"),
		RedisAddr:                envOr("REDIS_ADDR", "localhost:6379"),
		S3Endpoint:               envOr("S3_ENDPOINT", "http://localhost:9000"),
		S3Region:                 envOr("S3_REGION", "us-east-1"),
		S3AccessKey:              envOr("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:              envOr("S3_SECRET_KEY", "minioadmin"),
		S3Bucket:                 envOr("S3_BUCKET", "vod-videos"),
		S3PublicEndpoint:         envOr("S3_PUBLIC_ENDPOINT", "http://localhost:9000"),
		RabbitMQURL:              envOr("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		WhisperAPIKey:            os.Getenv("WHISPER_API_KEY"),
		WhisperCppBin:            envOr("WHISPER_CPP_BIN", "/usr/local/bin/whisper-cli"),
		WhisperCppModel:          envOr("WHISPER_CPP_MODEL", "/models/ggml-base.en.bin"),
		WorkerID:                 envOr("WORKER_ID", workerIDDefault()),
		APIKey:                   os.Getenv("API_KEY"),
		UploadRateLimitPerMinute: envOrInt("UPLOAD_RATE_LIMIT_PER_MINUTE", 10),
	}
}

func workerIDDefault() string {
	host, err := os.Hostname()
	if err != nil {
		return "worker-" + strconv.Itoa(os.Getpid())
	}
	return host
}
