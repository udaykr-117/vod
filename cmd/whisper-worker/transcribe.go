package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// HasAudioStream reports whether inputPath contains at least one audio
// stream. A silent video (e.g. screen recording with no narration) is a
// legitimate upload, not an error, so the caller uses this to skip
// extraction/transcription instead of failing the job.
func HasAudioStream(ctx context.Context, inputPath string) (bool, error) {
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-select_streams", "a",
		"-show_entries", "stream=index",
		"-of", "csv=p=0",
		inputPath,
	)
	out, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("ffprobe: %w", err)
	}
	return len(out) > 0, nil
}

// ExtractAudio pulls the audio track out as 16kHz mono 16-bit PCM WAV —
// whisper.cpp requires raw PCM input, not a compressed container, and 16kHz
// mono is the exact format its models are trained on.
func ExtractAudio(ctx context.Context, inputPath, outPath string) error {
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-y", "-i", inputPath,
		"-vn", "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le",
		outPath,
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg extract audio failed: %w\n%s", err, truncate(output, 2000))
	}
	return nil
}

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return b[len(b)-n:]
}

// Transcriber produces a WebVTT caption file from an audio file. Two
// implementations: OpenAI's hosted Whisper API (when WHISPER_API_KEY is
// configured) and a CPU-only local stub (the default, so the pipeline is
// fully runnable and testable without any API key or GPU infra).
type Transcriber interface {
	Transcribe(ctx context.Context, audioPath string) (vtt string, err error)
}

// OpenAIWhisper calls OpenAI's /v1/audio/transcriptions endpoint, which
// can return WebVTT directly via response_format, so no local segment
// stitching is needed.
type OpenAIWhisper struct {
	APIKey string
}

func (o OpenAIWhisper) Transcribe(ctx context.Context, audioPath string) (string, error) {
	f, err := os.Open(audioPath)
	if err != nil {
		return "", fmt.Errorf("open audio file: %w", err)
	}
	defer f.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(audioPath))
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(part, f); err != nil {
		return "", fmt.Errorf("copy audio into form: %w", err)
	}
	_ = writer.WriteField("model", "whisper-1")
	_ = writer.WriteField("response_format", "vtt")
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/audio/transcriptions", &body)
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+o.APIKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("call whisper API: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("whisper API returned %d: %s", resp.StatusCode, truncate(respBody, 1000))
	}
	return string(respBody), nil
}

// LocalWhisperCpp runs whisper.cpp's CLI directly against the extracted WAV
// — a real CPU-only speech-to-text model with no network call and no
// per-request cost, per the architecture's "calls Whisper API or runs
// CPU-only — not GPU infra" framing. whisper.cpp's -ovtt flag writes WebVTT
// directly, so no segment stitching is needed here either.
type LocalWhisperCpp struct {
	BinPath   string
	ModelPath string
}

func (l LocalWhisperCpp) Transcribe(ctx context.Context, audioPath string) (string, error) {
	outPrefix := strings.TrimSuffix(audioPath, filepath.Ext(audioPath))

	cmd := exec.CommandContext(ctx, l.BinPath,
		"-m", l.ModelPath,
		"-f", audioPath,
		"-ovtt",
		"-of", outPrefix,
		"-nt", // no per-line timestamps in stdout; we only care about the .vtt file
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("whisper.cpp failed: %w\n%s", err, truncate(output, 4000))
	}

	vttPath := outPrefix + ".vtt"
	vtt, err := os.ReadFile(vttPath)
	if err != nil {
		return "", fmt.Errorf("read whisper.cpp output %s: %w", vttPath, err)
	}
	_ = os.Remove(vttPath)
	return string(vtt), nil
}

// StubTranscriber is the default backend: a deterministic CPU-only
// placeholder so the full pipeline (extract -> transcribe -> upload ->
// caption_status=completed) is runnable and testable with zero external
// dependencies. Swap in OpenAIWhisper by setting WHISPER_API_KEY.
type StubTranscriber struct{}

func (StubTranscriber) Transcribe(ctx context.Context, audioPath string) (string, error) {
	duration, err := probeDuration(ctx, audioPath)
	if err != nil {
		return "", fmt.Errorf("probe audio duration: %w", err)
	}
	end := formatVTTTimestamp(duration)
	return fmt.Sprintf("WEBVTT\n\n00:00:00.000 --> %s\n[captions unavailable: no Whisper backend configured]\n", end), nil
}

func formatVTTTimestamp(seconds float64) string {
	if seconds < 0 {
		seconds = 0
	}
	h := int(seconds) / 3600
	m := (int(seconds) % 3600) / 60
	s := int(seconds) % 60
	ms := int((seconds - float64(int(seconds))) * 1000)
	return fmt.Sprintf("%02d:%02d:%02d.%03d", h, m, s, ms)
}
