//go:build observe
// +build observe

package observe

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// The child's metrics endpoint observation (Fix-A's series + the D-5
// queue-depth discipline, plain-text Prometheus parsing — no prometheus
// dependency, the soak metrics.go pattern):
//
//   - sync_error_gauge{syncgauge="nacos"} — the retry-queue depth (the
//     drain criterion);
//   - events_dropped_total{cluster="..."} — the silent-drop counter (the
//     completeness criterion: 0 over the window);
//   - k8s_queue_depth — the robot's coalescing queue depth (the queue
//     trajectory);
//   - event_to_store_e2e_duration_seconds — the Fix-A end-to-end latency
//     histogram (per sink/outcome): the bucket counts yield p50/p95/p99
//     for the steady vs under-churn SLO verdicts.
type metricsView struct {
	addr string
	http *http.Client

	// droppedBase is the counter value per cluster at the FIRST scrape
	// (counters only increase; the run measures the delta).
	mu          sync.Mutex
	droppedBase map[string]float64
	scrapes     int
}

func newMetricsView(port int) *metricsView {
	return &metricsView{
		addr: fmt.Sprintf("http://127.0.0.1:%d", port),
		http: sharedHTTP,
	}
}

// childMetrics is one scrape's derived observation.
type childMetrics struct {
	// RetryDepths is the per-sink retry-queue depth map (sync_error_gauge).
	RetryDepths map[string]int
	// DroppedTotal is the run-to-date total dropped events (the counter
	// delta from the first scrape, summed across cluster labels).
	DroppedTotal float64
	// QueueDepth is the robot's coalescing queue depth.
	QueueDepth int
	// Latency is the e2e histogram observation (nacos sink, outcome ok).
	Latency *histogramSnapshot
}

// observe scrapes once and derives the child metrics. A scrape failure is
// an error (un-observable, never depth-0 — a metrics outage must not fake
// a drained queue).
func (m *metricsView) observe() (*childMetrics, error) {
	body, err := m.scrape()
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.scrapes++
	first := m.scrapes == 1
	if first {
		m.droppedBase = map[string]float64{}
	}
	out := &childMetrics{RetryDepths: map[string]int{}}
	var droppedDelta float64
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		switch {
		case strings.HasPrefix(line, "sync_error_gauge"):
			if sink, value, ok := parseOneLabelSample(line, "syncgauge"); ok {
				out.RetryDepths[sink] = value
			}
		case strings.HasPrefix(line, "events_dropped_total"):
			cluster, value, ok := parseOneLabelFloat(line, "cluster")
			if !ok {
				continue
			}
			if first {
				m.droppedBase[cluster] = value
				continue
			}
			delta := value - m.droppedBase[cluster]
			if delta < 0 {
				delta = value // a child restart reset the counter: report absolute
			}
			droppedDelta += delta
		case strings.HasPrefix(line, "k8s_queue_depth"):
			if value, ok := parseLabelLessFloat(line, "k8s_queue_depth"); ok {
				out.QueueDepth = int(value)
			}
		case strings.HasPrefix(line, "event_to_store_e2e_duration_seconds_bucket"):
			le, sink, outcome, count, ok := parseBucketSample(line)
			if !ok || sink != "nacos" || outcome != "ok" {
				continue
			}
			if out.Latency == nil {
				out.Latency = &histogramSnapshot{Buckets: map[float64]uint64{}}
			}
			if le < 0 {
				// The +Inf bucket: its count IS the observation count.
				out.Latency.Count = count
				continue
			}
			out.Latency.Buckets[le] = count
		}
	}
	if !first {
		out.DroppedTotal = droppedDelta
	}
	return out, nil
}

// scrape pulls the whole exposition body once.
func (m *metricsView) scrape() (string, error) {
	response, err := m.http.Get(m.addr + "/metrics") //nolint:gosec // fixed loopback URL
	if err != nil {
		return "", fmt.Errorf("metrics scrape: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics scrape answered %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return "", fmt.Errorf("metrics scrape read: %w", err)
	}
	return string(body), nil
}

// histogramSnapshot is one scrape's cumulative bucket counts of the e2e
// histogram (nacos/ok). Percentile derives the bucket boundary at a
// quantile — the standard histogram_quantile reading; the cumulative
// counts come from the scraped _bucket lines (already cumulative).
type histogramSnapshot struct {
	Buckets map[float64]uint64
	Count   uint64
}

// Percentile returns the bucket boundary at quantile q (0..1); 0 when
// there are no observations.
func (h *histogramSnapshot) Percentile(q float64) float64 {
	if h == nil || h.Count == 0 {
		return 0
	}
	target := uint64(float64(h.Count) * q)
	if target == 0 {
		target = 1
	}
	var bounds []float64
	for le := range h.Buckets {
		bounds = append(bounds, le)
	}
	sort.Float64s(bounds)
	for _, le := range bounds {
		if h.Buckets[le] >= target {
			return le
		}
	}
	if len(bounds) == 0 {
		return 0
	}
	return bounds[len(bounds)-1]
}

// parseBucketSample parses one
// event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="0.05"} 123
// sample. le="+Inf" returns le = -1 (the +Inf marker).
func parseBucketSample(line string) (le float64, sink, outcome string, count uint64, ok bool) {
	rest := strings.TrimPrefix(line, "event_to_store_e2e_duration_seconds_bucket")
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "{") {
		return 0, "", "", 0, false
	}
	end := strings.Index(rest, "}")
	if end < 0 {
		return 0, "", "", 0, false
	}
	labelPart := rest[1:end]
	valuePart := strings.TrimSpace(rest[end+1:])
	parsed, err := strconv.ParseUint(valuePart, 10, 64)
	if err != nil {
		return 0, "", "", 0, false
	}
	leRaw := labelValue(labelPart, "le")
	if leRaw == "" {
		return 0, "", "", 0, false
	}
	if leRaw == "+Inf" {
		return -1, labelValue(labelPart, "sink"), labelValue(labelPart, "outcome"), parsed, true
	}
	bound, err := strconv.ParseFloat(leRaw, 64)
	if err != nil {
		return 0, "", "", 0, false
	}
	return bound, labelValue(labelPart, "sink"), labelValue(labelPart, "outcome"), parsed, true
}

// parseOneLabelSample parses `name{label="X"} N` into (label, int value).
func parseOneLabelSample(line, labelKey string) (string, int, bool) {
	cluster, value, ok := parseOneLabelFloat(line, labelKey)
	if !ok {
		return "", 0, false
	}
	return cluster, int(value), true
}

// parseOneLabelFloat parses `name{label="X"} N` (or the label-less
// `name N`) into (label, float value).
func parseOneLabelFloat(line, labelKey string) (string, float64, bool) {
	idx := strings.Index(line, "{")
	if idx < 0 {
		// Label-less form: `name N`.
		value, ok := parseLabelLessFloat(line, seriesNameOf(line))
		return "", value, ok
	}
	end := strings.Index(line, "}")
	if end < 0 {
		return "", 0, false
	}
	labelPart := line[idx+1 : end]
	valuePart := strings.TrimSpace(line[end+1:])
	value, err := strconv.ParseFloat(valuePart, 64)
	if err != nil {
		return "", 0, false
	}
	return labelValue(labelPart, labelKey), value, true
}

// parseLabelLessFloat parses `name N` into the float value.
func parseLabelLessFloat(line, series string) (float64, bool) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return 0, false
	}
	value, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// seriesNameOf returns the first token of a sample line (the series name).
func seriesNameOf(line string) string {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// labelValue extracts `key="value"` from a label block.
func labelValue(labelPart, key string) string {
	pattern := key + `="`
	at := strings.Index(labelPart, pattern)
	if at < 0 {
		return ""
	}
	rest := labelPart[at+len(pattern):]
	quote := strings.Index(rest, `"`)
	if quote < 0 {
		return ""
	}
	return rest[:quote]
}
