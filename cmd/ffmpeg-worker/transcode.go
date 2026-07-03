package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
)

// threadsPerRendition divides available cores by the number of renditions
// run concurrently, with a floor of 1 so this is still correct on small
// hosts (e.g. a 2-core CI runner).
func threadsPerRendition() string {
	n := runtime.NumCPU() / len(renditions)
	if n < 1 {
		n = 1
	}
	return strconv.Itoa(n)
}

type rendition struct {
	name      string
	width     int
	height    int
	bitrateKb int
}

// renditions matches the architecture's "1080p / 720p / 480p" set.
var renditions = []rendition{
	{"1080p", 1920, 1080, 5000},
	{"720p", 1280, 720, 2800},
	{"480p", 854, 480, 1400},
}

// GenerateHLS runs one ffmpeg invocation per rendition, in parallel — each
// is a separate process with its own decode pass, so there's no shared
// state to race on, and running all three concurrently turns the wall-clock
// cost from sum(rendition durations) into roughly max(rendition durations)
// on a multi-core host. It then writes a master playlist referencing all
// rendition playlists. outDir is a fresh local directory; its contents are
// uploaded verbatim afterward.
func GenerateHLS(ctx context.Context, inputPath, outDir string) error {
	masterEntries := make([]string, len(renditions))
	errs := make([]error, len(renditions))

	var wg sync.WaitGroup
	for i, r := range renditions {
		wg.Add(1)
		go func(i int, r rendition) {
			defer wg.Done()
			entry, err := encodeRendition(ctx, inputPath, outDir, r)
			masterEntries[i] = entry
			errs[i] = err
		}(i, r)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("rendition %s: %w", renditions[i].name, err)
		}
	}

	master := "#EXTM3U\n#EXT-X-VERSION:3\n" + joinLines(masterEntries) + "\n"
	if err := os.WriteFile(filepath.Join(outDir, "master.m3u8"), []byte(master), 0o644); err != nil {
		return fmt.Errorf("write master playlist: %w", err)
	}
	return nil
}

func encodeRendition(ctx context.Context, inputPath, outDir string, r rendition) (masterEntry string, err error) {
	renditionDir := filepath.Join(outDir, r.name)
	if err := os.MkdirAll(renditionDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir %s: %w", renditionDir, err)
	}

	playlist := filepath.Join(renditionDir, "index.m3u8")
	segmentPattern := filepath.Join(renditionDir, "seg_%03d.ts")

	args := []string{
		"-y", "-i", inputPath,
		// The second scale stage forces both dimensions even: libx264
		// requires this for yuv420p chroma subsampling, but
		// force_original_aspect_ratio can land on an odd value (e.g.
		// 853 when fitting 480p height into a 16:9 source).
		"-vf", fmt.Sprintf("scale=w=%d:h=%d:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2", r.width, r.height),
		"-pix_fmt", "yuv420p",
		// veryfast trades a few % of compression efficiency for several
		// times the encode speed versus the libx264 default ("medium") —
		// the right trade for a job queue where wall-clock latency matters
		// more than shaving bitrate.
		"-preset", "veryfast",
		// Without this, each of the 3 concurrent renditions tries to claim
		// every available core, oversubscribing the host (seen as >800% CPU
		// on an 8-core box) and fighting for cache/scheduler time instead of
		// actually finishing faster. Capping each to a fair share keeps the
		// parallelism benefit without the contention cost.
		"-threads", threadsPerRendition(),
		"-c:a", "aac", "-ar", "48000", "-b:a", "128k",
		"-c:v", "h264", "-profile:v", "main", "-crf", "20",
		"-b:v", fmt.Sprintf("%dk", r.bitrateKb), "-maxrate", fmt.Sprintf("%dk", r.bitrateKb*107/100), "-bufsize", fmt.Sprintf("%dk", r.bitrateKb*150/100),
		"-hls_time", "6", "-hls_playlist_type", "vod",
		"-hls_segment_filename", segmentPattern,
		playlist,
	}

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("ffmpeg failed: %w\n%s", err, truncate(output, 4000))
	}

	return fmt.Sprintf(
		"#EXT-X-STREAM-INF:BANDWIDTH=%d,RESOLUTION=%dx%d\n%s/index.m3u8",
		r.bitrateKb*1000, r.width, r.height, r.name,
	), nil
}

func joinLines(lines []string) string {
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[len(b)-n:]
}
