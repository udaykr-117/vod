package models

import "time"

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type JobType string

const (
	JobTypeHLS       JobType = "hls"
	JobTypeThumbnail JobType = "thumbnail"
	JobTypeCaption   JobType = "caption"
)

type Video struct {
	ID               string    `json:"id"`
	Title            string    `json:"title"`
	StorageKey       string    `json:"storage_key"`
	EncodingStatus   Status    `json:"encoding_status"`
	ThumbnailStatus  Status    `json:"thumbnail_status"`
	CaptionStatus    Status    `json:"caption_status"`
	CreatedAt        time.Time `json:"created_at"`
}

func (v Video) Playable() bool {
	return v.EncodingStatus == StatusCompleted
}

type EncodingJob struct {
	JobID        string     `json:"job_id"`
	VideoID      string     `json:"video_id"`
	JobType      JobType    `json:"job_type"`
	Status       Status     `json:"status"`
	AttemptCount int        `json:"attempt_count"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	WorkerID     string     `json:"worker_id,omitempty"`
	LastError    string     `json:"last_error,omitempty"`
}

// VideoUploadedEvent is published to the video.uploaded exchange after a
// multipart upload completes. Each worker downloads the raw object
// independently from storage using StorageKey.
type VideoUploadedEvent struct {
	VideoID    string `json:"video_id"`
	StorageKey string `json:"storage_key"`
}
