// Package metrics defines the Prometheus metrics shared across all four
// binaries. Kept in one place so the upload-service and the three workers
// expose consistent metric names/labels for the same kind of event
// (a job finishing, an HTTP request completing) rather than each inventing
// its own.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vod_http_requests_total",
		Help: "Total HTTP requests handled by upload-service.",
	}, []string{"method", "route", "status"})

	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vod_http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	JobsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vod_jobs_processed_total",
		Help: "Total jobs processed by workers, by job_type and result.",
	}, []string{"job_type", "result"}) // result: completed | failed | skipped_duplicate

	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vod_job_duration_seconds",
		Help:    "Job processing time in seconds, for completed/failed jobs only (not skipped duplicates).",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200},
	}, []string{"job_type"})
)

// ServeMetrics starts a dedicated HTTP server exposing /metrics on addr.
// Workers have no other HTTP server, so this is theirs exclusively;
// upload-service mounts the same handler on its existing router instead
// (see Server.Routes) so it doesn't need a second listener.
func ServeMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(addr, mux) //nolint:errcheck // best-effort; metrics aren't load-bearing
}
