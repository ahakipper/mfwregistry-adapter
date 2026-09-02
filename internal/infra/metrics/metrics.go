// Package metrics wires the Prometheus collectors registered by
// pkg/metrics to the ports.MetricsRecorder interface and serves them over
// HTTP.
//
// The Recorder mirrors internal/server.go globalMetricsRecorder: it does
// not create its own collectors, it observes the package-level registered
// ones. The HTTP server mirrors pkg/metrics/proserver.go (promhttp plus
// pprof) but returns errors instead of calling Fatal, and shuts down
// gracefully and idempotently.
package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // registers pprof handlers on http.DefaultServeMux
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"spotter/internal/ports"
	"spotter/pkg/metrics"
	"spotter/pkg/providers"
)

// defaultMetricsAddr is used by StartHTTP when addr is empty; it matches
// the default metrics address of the legacy flag set.
const defaultMetricsAddr = ":8090"

// shutdownTimeout bounds the graceful shutdown of the HTTP server.
const shutdownTimeout = 5 * time.Second

// Recorder records synchronization metrics on the package-level collectors
// registered by pkg/metrics (see its init function). It holds no state, so
// it is safe for concurrent use by multiple goroutines.
type Recorder struct{}

// Compile-time assertion that Recorder satisfies the port.
var _ ports.MetricsRecorder = (*Recorder)(nil)

// New returns a Recorder. It never fails; the collectors it observes are
// already registered by the pkg/metrics init function.
func New() *Recorder {
	return &Recorder{}
}

// ObserveSyncOnceDuration records a single-provider sync duration in
// milliseconds on the sync_once_durations_histogram collector.
func (r *Recorder) ObserveSyncOnceDuration(duration time.Duration) {
	metrics.SyncOnceDurationsHistogram.Observe(float64(duration.Milliseconds()))
}

// ObserveSyncAllDuration records a full-push sync duration in milliseconds
// on the sync_all_durations_histogram collector, selecting the k8s, ecs or
// all variant by provider, exactly like internal/server.go
// globalMetricsRecorder.
func (r *Recorder) ObserveSyncAllDuration(provider string, duration time.Duration) {
	switch provider {
	case providers.ProviderK8s:
		metrics.SyncAllK8sDurationsHistogram.Observe(float64(duration.Milliseconds()))
	case providers.ProviderEcs:
		metrics.SyncAllEcsDurationsHistogram.Observe(float64(duration.Milliseconds()))
	default:
		metrics.SyncAllDurationsHistogram.Observe(float64(duration.Milliseconds()))
	}
}

// SetSyncErrorQueueDepth records the current depth of the sync error queue
// on the sync_error_gauge collector.
func (r *Recorder) SetSyncErrorQueueDepth(depth int) {
	metrics.SyncErrorGauge.WithLabelValues("sync_error_gauge").Set(float64(depth))
}

// MarkSyncOnce marks that a single-provider sync just happened by setting
// the sync_once_gauge to 1.
func (r *Recorder) MarkSyncOnce() {
	metrics.SyncOnceGauge.WithLabelValues("sync_once_gauge").Set(1)
}

// httpServer serves promhttp and pprof endpoints and stops idempotently.
type httpServer struct {
	srv  *http.Server
	once sync.Once
	stop func() error
}

// metricsMuxOnce registers /metrics on http.DefaultServeMux a single
// time. DefaultServeMux is a process-wide singleton (the net/http/pprof
// import registers its handlers there), so repeated calls must not
// re-register /metrics; pkg/metrics/proserver.go has the same behavior but
// relies on being started only once per process.
var metricsMuxOnce sync.Once

// newMetricsMux returns the default serve mux with the promhttp /metrics
// handler registered, mirroring pkg/metrics/proserver.go, which registers
// the promhttp handler on the default mux (where net/http/pprof already
// registered its handlers). A dedicated mux would hide the pprof
// endpoints, so the default mux is kept.
func newMetricsMux() *http.ServeMux {
	metricsMuxOnce.Do(func() {
		http.Handle("/metrics", promhttp.Handler())
	})
	return http.DefaultServeMux
}

// OnListener starts serving metrics on the given listener and returns a
// stop function. The server serves in a background goroutine; stop
// performs a graceful shutdown with a bounded timeout and is idempotent.
// The returned stop function returns the serve error when the server was
// stopped by the caller first (nil on a self-termination, e.g. a listener
// error), and the shutdown error otherwise.
func OnListener(l net.Listener) (stop func() error, err error) {
	mux := newMetricsMux()
	srv := &http.Server{Handler: mux}
	h := &httpServer{srv: srv}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(l)
	}()

	h.stop = func() error {
		var shutdownErr error
		h.once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()
			shutdownErr = srv.Shutdown(ctx)
			// Drain the serve goroutine so it never leaks; Serve returns
			// http.ErrServerClosed after a successful Shutdown.
			<-serveErr
		})
		return shutdownErr
	}
	return h.stop, nil
}

// StartHTTP starts a metrics HTTP server on addr, serving promhttp and
// pprof (see OnListener). An empty addr defaults to ":8090". Unlike
// pkg/metrics.PrometheusService.Start, it returns the listen error rather
// than logging it fatally, and the returned stop function shuts the server
// down gracefully and idempotently.
func StartHTTP(addr string) (stop func() error, err error) {
	if addr == "" {
		addr = defaultMetricsAddr
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server listen %s: %w", addr, err)
	}
	return OnListener(l)
}
