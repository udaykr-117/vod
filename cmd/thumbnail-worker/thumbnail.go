package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
)

// probeDuration shells out to ffprobe to get the input's duration in
// seconds, used to space thumbnails evenly regardless of clip length.
func probeDuration(ctx context.Context, inputPath string) (float64, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "json",
		inputPath,
	)
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("ffprobe: %w", err)
	}
	var probe struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &probe); err != nil {
		return 0, fmt.Errorf("parse ffprobe output: %w", err)
	}
	duration, err := strconv.ParseFloat(probe.Format.Duration, 64)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", probe.Format.Duration, err)
	}
	return duration, nil
}

// thumbnailFractions spaces three thumbnails across the clip so the user
// gets a representative choice rather than always the first frame, which is
// often a black/blank frame for real-world video.
var thumbnailFractions = []float64{0.1, 0.5, 0.9}

// GenerateThumbnails writes one JPEG per entry in thumbnailFractions into
// outDir, named thumb_0.jpg, thumb_1.jpg, ...
func GenerateThumbnails(ctx context.Context, inputPath, outDir string) error {
	duration, err := probeDuration(ctx, inputPath)
	if err != nil {
		return fmt.Errorf("probe duration: %w", err)
	}

	for i, frac := range thumbnailFractions {
		timestamp := fmt.Sprintf("%.3f", duration*frac)
		outPath := filepath.Join(outDir, fmt.Sprintf("thumb_%d.jpg", i))

		cmd := exec.CommandContext(ctx, "ffmpeg",
			"-y", "-ss", timestamp, "-i", inputPath,
			"-frames:v", "1", "-q:v", "2",
			outPath,
		)
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("ffmpeg thumbnail %d failed: %w\n%s", i, err, truncate(output, 2000))
		}
	}
	return nil
}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[len(b)-n:]
}
