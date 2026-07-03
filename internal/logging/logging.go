package logging

import (
	"log/slog"
	"os"
)

// Setup installs a JSON structured logger as the slog default for the
// whole process. service is attached to every log line so aggregating logs
// from all 4 binaries in one place (e.g. `docker compose logs`, or a real
// log aggregator later) still lets you filter by which one emitted a line.
// Level is controlled by LOG_LEVEL (debug|info|warn|error), defaulting to
// info — no new dependency, this is all stdlib (Go 1.21+).
func Setup(service string) *slog.Logger {
	level := slog.LevelInfo
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	logger := slog.New(handler).With("service", service)
	slog.SetDefault(logger)
	return logger
}
