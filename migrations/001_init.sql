CREATE TABLE IF NOT EXISTS videos (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title           TEXT NOT NULL,
    storage_key     TEXT NOT NULL UNIQUE,
    encoding_status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (encoding_status IN ('pending', 'processing', 'completed', 'failed')),
    thumbnail_status TEXT NOT NULL DEFAULT 'pending'
                        CHECK (thumbnail_status IN ('pending', 'processing', 'completed', 'failed')),
    caption_status  TEXT NOT NULL DEFAULT 'pending'
                        CHECK (caption_status IN ('pending', 'processing', 'completed', 'failed')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS encoding_jobs (
    job_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    video_id        UUID NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    job_type        TEXT NOT NULL CHECK (job_type IN ('hls', 'thumbnail', 'caption')),
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    attempt_count   INT NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    worker_id       TEXT,
    last_error      TEXT,
    UNIQUE (video_id, job_type)
);

CREATE INDEX IF NOT EXISTS idx_encoding_jobs_video_id ON encoding_jobs(video_id);
CREATE INDEX IF NOT EXISTS idx_videos_encoding_status ON videos(encoding_status);
