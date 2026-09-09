//go:build soak
// +build soak

package soak

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The retry-queue observation side of AUDIT-D-5: the soak scrapes the
// child's metrics endpoint for sync_error_gauge — the per-sink retry-queue
// depth series the worker publishes on every 5s retry tick — and enforces a
// standing drain bound. F7's exact signature was a NON-DRAINING queue: the
// live incident held 130 pending entries and fired 9624 futile retries of a
// permanently-rejected DELETE, and the 1h soak run was blind to it (the
// metrics port was scraped for liveness only; a post-hoc log parse found the
// condition). The series names (pkg/metrics/stat.go + the worker's
// SetSyncErrorQueueDepth call sites):
//
//	sync_error_gauge{syncgauge="atlas"} / {syncgauge="nacos"} — per-sink
//	sync_error_gauge{syncgauge="__total__"}                — all sinks incl.
//	                                                         ghost-sink keys
//
// The parsing is deliberately plain text over the Prometheus exposition
// format (no prometheus dependency): the gauge lines are flat
// `name{label="value"} number` samples, and only this one series is needed.

// metricsObserver scrapes the spotter child's /metrics endpoint and parses
// the sync_error_gauge series (D-5).
type metricsObserver struct {
	addr string
	http *http.Client
}

// newMetricsObserver builds the observer for the child's metrics port.
func newMetricsObserver() *metricsObserver {
	return &metricsObserver{
		addr: fmt.Sprintf("http://127.0.0.1:%d", MetricsPort),
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

// queueDepths scrapes /metrics once and returns the sink -> depth map of
// every observed sync_error_gauge series (including __total__). A scrape
// failure or a non-200 answer is an error the caller treats as
// un-observable, never as depth 0 (a metrics outage must not fake a drained
// queue).
func (o *metricsObserver) queueDepths() (map[string]int, error) {
	response, err := o.http.Get(o.addr + "/metrics") //nolint:gosec // fixed loopback URL
	if err != nil {
		return nil, fmt.Errorf("metrics scrape: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics scrape answered %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("metrics scrape read: %w", err)
	}
	return parseSyncErrorGauge(string(body)), nil
}

// parseSyncErrorGauge extracts the sync_error_gauge samples from a
// Prometheus text exposition body. It handles the one label the series
// carries (syncgauge="...") plus the label-less degenerate form; comments,
// help/type lines and other series are skipped. Unparsable sample values are
// skipped (never guessed). The label order of a one-label series is fixed by
// the writer, so a plain prefix match is sufficient and faithful.
func parseSyncErrorGauge(body string) map[string]int {
	depths := map[string]int{}
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "sync_error_gauge") {
			continue
		}
		sink, value, ok := splitGaugeSample(line)
		if !ok {
			continue
		}
		depths[sink] = value
	}
	return depths
}

// splitGaugeSample splits one `sync_error_gauge{syncgauge="X"} N` (or
// `sync_error_gauge N`) sample into its sink label and value.
func splitGaugeSample(line string) (string, int, bool) {
	const seriesPrefix = "sync_error_gauge"
	rest := strings.TrimPrefix(line, seriesPrefix)
	// Label-less form: rest starts with whitespace then the value.
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "{") {
		fields := strings.Fields(rest)
		if len(fields) != 1 {
			return "", 0, false
		}
		value, err := strconv.Atoi(fields[0])
		if err != nil {
			return "", 0, false
		}
		return "", value, true
	}
	// One-label form: {syncgauge="X"} N. Split the label block from the
	// trailing value at the closing brace.
	end := strings.Index(rest, "}")
	if end < 0 {
		return "", 0, false
	}
	labelPart := rest[1:end] // inside the braces
	valuePart := strings.TrimSpace(rest[end+1:])
	value, err := strconv.Atoi(valuePart)
	if err != nil {
		return "", 0, false
	}
	const key = `syncgauge="`
	keyAt := strings.Index(labelPart, key)
	if keyAt < 0 {
		return "", 0, false
	}
	remainder := labelPart[keyAt+len(key):]
	quoteAt := strings.Index(remainder, `"`)
	if quoteAt < 0 {
		return "", 0, false
	}
	return remainder[:quoteAt], value, true
}

// formatDepths renders a depth map for check lines and failure messages
// (deterministic order).
func formatDepths(depths map[string]int) string {
	if len(depths) == 0 {
		return "queue=unobserved"
	}
	sinks := make([]string, 0, len(depths))
	for sink := range depths {
		sinks = append(sinks, sink)
	}
	sort.Strings(sinks)
	parts := make([]string, 0, len(sinks))
	for _, sink := range sinks {
		parts = append(parts, fmt.Sprintf("%s=%d", sink, depths[sink]))
	}
	return "queue[" + strings.Join(parts, ",") + "]"
}

// drainViolation describes a standing drain-bound violation: the sink whose
// queue stayed non-zero past the bound and the depths observed at the trip.
type drainViolation struct {
	sink    string
	depths  map[string]int
	since   time.Time
	heldFor time.Duration
}

func (v drainViolation) String() string {
	return fmt.Sprintf("sink %s held %d entries for %s past churn quiescence (%s) — a non-draining retry queue is F7's exact signature",
		v.sink, drainDepth(v.depths, v.sink), v.heldFor.Round(time.Second), formatDepths(v.depths))
}

// drainDepth returns the depth of one sink (0 when absent).
func drainDepth(depths map[string]int, sink string) int {
	return depths[sink]
}
