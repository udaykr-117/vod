# VOD Platform

A Video-on-Demand media processing platform: resumable multipart uploads, HLS
transcoding (1080p/720p/480p), thumbnails, captions, all backed by durable,
idempotent background workers. See [arch.md](arch.md) for the full design
rationale and the decisions that were tried and reversed along the way.

## Stack

- **upload-service** (Go) — plain HTTP/JSON API for resumable multipart uploads.
- **ffmpeg-worker**, **thumbnail-worker**, **whisper-worker** (Go) — independent
  RabbitMQ consumers, each downloads the raw upload on its own and writes its
  own artifact + status column.
- **Postgres** — durable video/job metadata. **Redis** — ephemeral upload
  sessions (TTL). **MinIO** — S3-compatible object storage. **RabbitMQ** —
  durable job queue with per-queue DLQ.
- **web/** — a plain HTML/JS test client for exercising uploads by hand.

## Running it

Everything runs in Docker (built and tested inside WSL2 Ubuntu with Docker
Desktop's WSL integration enabled):

```bash
cp .env.example .env   # first time only — see Secrets section below
docker compose build
docker compose up -d
```

This builds the four Go services, starts Postgres/Redis/MinIO/RabbitMQ,
applies the schema (`migrate` service, idempotent — safe to re-run), creates
the `vod-videos` bucket, and starts all three workers consuming their queues.

Check everything is up:

```bash
docker compose ps
curl http://localhost:8080/healthz
```

Open the test client at **http://localhost:8081** (served from `web/`),
point it at `http://localhost:8080`, pick a video file, and upload. Or drive
it by hand:

```bash
SIZE=$(stat -c%s myvideo.mp4)
curl -X POST http://localhost:8080/uploads -H 'Content-Type: application/json' \
  -d "{\"title\":\"My Video\",\"file_size_bytes\":$SIZE,\"content_type\":\"video/mp4\"}"
# -> {"upload_id":"...", "part_size":8388608, "part_count":1}

curl http://localhost:8080/uploads/<upload_id>/parts/1/presigned-url
# -> {"url":"http://localhost:9000/..."}  -- PUT your file bytes here directly

curl -X POST http://localhost:8080/uploads/<upload_id>/complete
# -> the video row, with encoding_status/thumbnail_status/caption_status all "pending"

curl http://localhost:8080/videos/<upload_id>
# -> watch the three statuses flip to "completed" as workers finish

curl "http://localhost:8080/videos?limit=20&offset=0"
# -> most recently created videos first

curl -X DELETE http://localhost:8080/videos/<id>
# -> 204; deletes the DB row and every object under raw/, renditions/,
#    thumbnails/, captions/ for that video

curl -X POST "http://localhost:8080/videos/<id>/retry?job_type=hls"
# -> 202; resets that job to pending (fresh attempt_count) and republishes
#    video.uploaded — job_type is one of hls, thumbnail, caption
```

### Auth

All routes except `/healthz` accept an optional `Authorization: Bearer <key>`.
It's **unenforced by default** — set `API_KEY` on `upload-service` in
`docker-compose.yml` to require it; leaving it unset keeps the API open,
matching every example above. The web test client has an "API key" field
that's sent automatically once filled in.

Renditions, thumbnails, and captions land in MinIO under
`renditions/<id>/master.m3u8`, `thumbnails/<id>/thumb_0.jpg`, and
`captions/<id>/captions.vtt`, all browsable directly at
`http://localhost:9000/vod-videos/...`.

### Ports

| Service              | Host port             |
|-----------------------|------------------------|
| upload-service (API) | 8080 (HTTP), 8443 via Caddy (HTTPS) |
| web test client       | 8081                   |
| Postgres              | **5433** (not 5432 — see note below) |
| Redis                 | 6379                   |
| MinIO API / Console   | 9000 / 9001            |
| RabbitMQ AMQP / mgmt  | 5672 / 15672 (guest/guest) |
| Prometheus            | 9091                   |
| Grafana               | 3000 (admin / `GRAFANA_ADMIN_PASSWORD`, default `admin`) |

### Metrics & dashboards

`upload-service` exposes `/metrics` on its existing port (8080); each
worker runs a second, metrics-only HTTP listener on `:9090` internally
(workers have no other HTTP server). Prometheus scrapes all four every 5s
(`prometheus.yml`).

Metrics: `vod_http_requests_total`/`vod_http_request_duration_seconds`
(labeled by method + **route pattern**, e.g. `/videos/{id}` — never the
literal path with a real ID, which would blow up cardinality), and
`vod_jobs_processed_total`/`vod_job_duration_seconds` (labeled by
`job_type` and `result`: `completed` | `failed` | `skipped_duplicate` — that
last one is duplicate-delivery hits, the same idempotency this whole
project relies on, now visible as a metric instead of just a log line).

Open Grafana at **http://localhost:3000** — the Prometheus datasource and a
"VOD Platform Overview" dashboard (request rates/latency, job
throughput/latency by type, completed-vs-failed) are both auto-provisioned
on first boot from `grafana/provisioning/`; nothing to click through
manually.

### TLS

`caddy` terminates TLS on **:8443** and reverse-proxies to `upload-service`
over plain HTTP inside the Docker network — the standard shape (TLS at the
edge, plaintext between trusted containers). It uses `tls internal`
(Caddyfile), which means Caddy mints its own local CA and signs a cert for
`localhost` — not a publicly trusted certificate, so curl/browsers need `-k`
or to trust Caddy's root CA (stored in the `caddy_data` volume) to avoid a
self-signed warning. Plain `:8080` stays open for local testing; point
anything real at `:8443` instead.

Postgres is mapped to host port **5433**, not the standard 5432, because a
native PostgreSQL install commonly already occupies 5432 inside a WSL
distro. Containers talk to each other over the Docker network using the
standard 5432 internally — only host-side tools (psql, local Go binaries run
outside Docker) need to use 5433.

### Whisper backend

`whisper-worker`'s image (`Dockerfile.whisper-worker`) builds `whisper.cpp`
from source and bakes in a `base.en` model (~148MB) — real speech-to-text,
CPU-only, no GPU, no network call, no per-request cost. This is the default
and what runs out of the box.

To use OpenAI's hosted Whisper API instead (higher fidelity, costs money per
request), set `WHISPER_API_KEY` for the `whisper-worker` service in
`docker-compose.yml` and redeploy — `selectTranscriber` in
[cmd/whisper-worker/main.go](cmd/whisper-worker/main.go) prefers it over the
local binary whenever a key is present. If neither the API key nor the local
binary/model are available for some reason, it falls back to a deterministic
placeholder-text stub rather than failing the job outright.

### Rate limiting

`POST /uploads` is capped per client IP at `UPLOAD_RATE_LIMIT_PER_MINUTE`
(default 10), using a Redis fixed-window counter — the existing Redis
instance, not a new dependency. Exceeding it returns `429` with a
`Retry-After` header giving seconds until the window resets. Every other
route is unaffected; if Redis itself is unreachable, the limiter fails
open (logs and lets the request through) rather than taking uploads down.

### Secrets

Postgres/MinIO/RabbitMQ credentials and `API_KEY`/`WHISPER_API_KEY` are read
from a git-ignored `.env` file (`docker-compose.yml` interpolates
`${VAR}`). Copy `.env.example` to `.env` before first run — the example file
ships with the same defaults that used to be hardcoded directly in
`docker-compose.yml`, fine for localhost, not for anything real.

## Testing

Integration tests run against the live stack — no mocks — and cover the
fault-injection scenarios from the architecture's build order: duplicate job
delivery, a worker crashing before its ACK, upload-session TTL expiry,
fanout to all three worker queues, and resuming an interrupted upload.

```bash
docker compose up -d                                            # backing infra must be running
docker compose stop ffmpeg-worker thumbnail-worker whisper-worker # see note below
go test -tags=integration ./test/integration/...
docker compose start ffmpeg-worker thumbnail-worker whisper-worker
```

The worker containers must be stopped while the suite runs: `TestFanoutDeliversToAllWorkerQueues`
publishes directly to the same queues the real workers consume from, and a
running worker will race the test's own consumer for that message (confirmed
by hand — the test fails intermittently with the workers up, and passes
consistently with them stopped). Likewise, stop any manually-run
(`go run ./cmd/...`) service/worker processes first.

## Local (non-Docker) development

Each service reads its config from env vars with sensible localhost
defaults (see [internal/config/config.go](internal/config/config.go)), so
you can also just run e.g. `go run ./cmd/upload-service` directly against
the same `docker compose up -d postgres redis minio rabbitmq` infra — useful
for fast iteration without rebuilding images. Use `DATABASE_URL` with port
5433 in that case (see note above).
