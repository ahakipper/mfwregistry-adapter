//go:build observe
// +build observe

package observe

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// tickRecord is the §4.3 step-6 JSONL record: one line per tick, both
// sides' observations (the bidirectionality proof), the in-flight count,
// the divergences with details, the environment snapshot, the queue
// depths, and the tick's own duration.
type tickRecord struct {
	Tick       int          `json:"tick"`
	TS         string       `json:"ts"`
	ElapsedMS  int64        `json:"elapsedMs"`
	Verdict    string       `json:"verdict"`
	Source     sideCount    `json:"source"`
	Remote     sideCount    `json:"remote"`
	InFlight   int          `json:"inFlight"`
	Divergence []divergence `json:"divergences"`
	Env        envState     `json:"env"`
	Queue      queueState   `json:"queue"`
	TickMS     int64        `json:"tickMs"`
}

// sideCount is one side's observation (bidirectionality: both sides'
// counts equal after tolerance on every consistent tick).
type sideCount struct {
	Count     int `json:"count"`     // total instances observed this side
	Online    int `json:"online"`    // source: Running+ready; remote: enabled
	Unhealthy int `json:"unhealthy"` // source: Running-not-ready; remote: disabled
	Pending   int `json:"pending"`   // source only: filtered Pending pods
	Services  int `json:"services"`  // services with >= 1 instance
}

// envState is the per-tick environment classification (§4.5).
type envState struct {
	Class      string `json:"class"` // healthy | leaderless | unreachable | read-error
	Detail     string `json:"detail,omitempty"`
	EnvOverlap bool   `json:"envOverlap"` // the tick's window overlapped a classified outage
}

// queueState is the per-tick queue observation.
type queueState struct {
	RetryNacos int     `json:"retryNacos"` // sync_error_gauge{syncgauge="nacos"}
	RobotDepth int     `json:"robotDepth"` // k8s_queue_depth
	Dropped    float64 `json:"dropped"`    // events_dropped_total delta (cumulative)
}

// forensicRecord is the §4.4 per-divergence forensic record: the pod's
// identity, the nacos composite id, the ledger link, the true ages, and
// the child's log slice around firstSeen.
type forensicRecord struct {
	Key        string   `json:"key"`
	AppCode    string   `json:"appCode"`
	Cluster    string   `json:"cluster"`
	PodUID     string   `json:"podUid"`
	PodName    string   `json:"podName"`
	InstanceID string   `json:"instanceId"` // the composite nacos id
	Kind       string   `json:"kind"`
	FirstSeen  string   `json:"firstSeen"`
	Resolved   string   `json:"resolved,omitempty"`
	AgeMS      int64    `json:"ageMs"`
	LedgerOp   string   `json:"ledgerOp,omitempty"`
	LedgerAt   string   `json:"ledgerAt,omitempty"`
	LogSlice   []string `json:"logSlice"`
}

// recordWriter appends JSONL lines to the per-tick record file.
type recordWriter struct {
	path string
	file *os.File
}

func newRecordWriter(path string) (*recordWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := openAppend(path)
	if err != nil {
		return nil, err
	}
	return &recordWriter{path: path, file: file}, nil
}

func (w *recordWriter) writeTick(record tickRecord) error {
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := w.file.Write(append(line, '\n')); err != nil {
		return err
	}
	return nil
}

func (w *recordWriter) close() error {
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

// runSummary is the aggregated verdict artifact (§4.3's verdict
// aggregation): the counters, the acceptance evaluation, the latency
// percentiles, the queue trajectory, and the divergence ledger.
type runSummary struct {
	// Identity
	Stamp            string  `json:"stamp"`
	Duration         string  `json:"duration"`
	Scale            int     `json:"scale"`
	Services         int     `json:"services"`
	ChurnRate        float64 `json:"churnRatePctPerMin"`
	ObsBound         string  `json:"obsBound"`
	PushIntervalSecs int     `json:"pushIntervalSecs"`
	ReconcileSource  string  `json:"reconcileSource"`

	// Ticks
	Ticks            int     `json:"ticks"`
	Consistent       int     `json:"consistent"`
	Divergent        int     `json:"divergent"`
	ObsErr           int     `json:"obserr"`
	ConsistencyRatio float64 `json:"consistencyRatio"`
	ObsErrRatio      float64 `json:"obserrRatio"`
	MaxObsErrStreak  int     `json:"maxObserrStreak"`

	// Scale criterion
	TicksSourceGEBase      int     `json:"ticksSourceGeBase"`
	TicksSourceGEBaseRatio float64 `json:"ticksSourceGeBaseRatio"`
	MinSourceCount         int     `json:"minSourceCount"`

	// Divergences
	ProductDivergentTicks int              `json:"productDivergentTicks"`
	MaxHeal               string           `json:"maxHeal"`
	Forensics             []forensicRecord `json:"forensics,omitempty"`
	MaxInFlight           int              `json:"maxInFlight"`

	// Queue / drops
	MaxRetryDepth int     `json:"maxRetryDepth"`
	MaxRobotDepth int     `json:"maxRobotDepth"`
	DroppedTotal  float64 `json:"droppedTotal"`
	DrainedAtEnd  bool    `json:"drainedAtEnd"`

	// Latency (nacos/ok e2e percentiles, seconds)
	Latency struct {
		P50   float64 `json:"p50"`
		P95   float64 `json:"p95"`
		P99   float64 `json:"p99"`
		Count uint64  `json:"count"`
	} `json:"latency"`

	// Environment
	EnvWindows []envWindow `json:"envWindows,omitempty"`

	// Churn accounting
	ChurnCreates int `json:"churnCreates"`
	ChurnDeletes int `json:"churnDeletes"`
	ChurnErrors  int `json:"churnErrors"`

	// Burst events (§4.2 OBS_BURSTS): per burst, both convergence legs.
	Bursts []burstSummary `json:"bursts,omitempty"`

	// The verdict
	Pass  bool     `json:"pass"`
	Fails []string `json:"fails,omitempty"`
}

// burstSummary is one burst event's measured record: the issue times and
// the tick-stamped convergence of BOTH legs (up: pods visible in nacos;
// down: pods gone from nacos), each must land within OBS_BOUND.
type burstSummary struct {
	Name            string `json:"name"`
	Size            int    `json:"size"`
	CreateIssued    string `json:"createIssued"`
	ConvergedUp     string `json:"convergedUp"`
	UpConvergence   string `json:"upConvergence"`
	DeleteIssued    string `json:"deleteIssued"`
	ConvergedDown   string `json:"convergedDown"`
	DownConvergence string `json:"downConvergence"`
}

// UpConvergenceOrNever renders the up leg (never-converged marker when 0).
func (b burstSummary) UpConvergenceOrNever() string {
	if b.UpConvergence == "" {
		return "NEVER"
	}
	return b.UpConvergence
}

// DownConvergenceOrNever renders the down leg (never-converged marker).
func (b burstSummary) DownConvergenceOrNever() string {
	if b.DownConvergence == "" {
		if b.DeleteIssued == "" {
			return "not issued (window ended in the settle wait)"
		}
		return "NEVER"
	}
	return b.DownConvergence
}

// DeleteIssuedOrNone renders the delete time (marker when unissued).
func (b burstSummary) DeleteIssuedOrNone() string {
	if b.DeleteIssued == "" {
		return "not issued"
	}
	return b.DeleteIssued
}

// envWindow is one classified environment outage window.
type envWindow struct {
	Class  string `json:"class"`
	From   string `json:"from"`
	To     string `json:"to"`
	Ticks  int    `json:"ticks"`
	Detail string `json:"detail,omitempty"`
}

// renderSummaryMarkdown writes the human-readable summary (the committed
// evidence artifact, tests/observe/results/<stamp>.md).
func renderSummaryMarkdown(s runSummary) string {
	var b strings.Builder
	b.WriteString("# DSCA-4 observation run\n\n")
	b.WriteString(fmt.Sprintf("- Window: %s, scale %d across %d services, churn %.1f%%/min\n", s.Duration, s.Scale, s.Services, s.ChurnRate))
	b.WriteString(fmt.Sprintf("- OBS_BOUND = max(SLO %ds, push-interval %ds) = %s; reconcile-source %s\n", SloBoundSecs, PushIntervalSecs, s.ObsBound, s.ReconcileSource))
	b.WriteString("\n## Ticks\n\n")
	b.WriteString(fmt.Sprintf("Total %d: CONSISTENT %d, DIVERGENT %d, OBSERR %d (ratio %.4f, max streak %d)\n",
		s.Ticks, s.Consistent, s.Divergent, s.ObsErr, s.ObsErrRatio, s.MaxObsErrStreak))
	b.WriteString(fmt.Sprintf("Consistency ratio: %.4f\n", s.ConsistencyRatio))
	b.WriteString(fmt.Sprintf("Scale: source >= %d on %d/%d ticks (%.4f); min source count %d\n",
		s.Scale, s.TicksSourceGEBase, s.Ticks, s.TicksSourceGEBaseRatio, s.MinSourceCount))
	b.WriteString("\n## Queue / drops\n\n")
	b.WriteString(fmt.Sprintf("Max retry-queue depth %d; max robot queue depth %d; dropped events %.0f; drained at end: %v\n",
		s.MaxRetryDepth, s.MaxRobotDepth, s.DroppedTotal, s.DrainedAtEnd))
	b.WriteString("\n## Latency (event_to_store_e2e, nacos/ok)\n\n")
	b.WriteString(fmt.Sprintf("p50 %.3fs, p95 %.3fs, p99 %.3fs over %d observations\n", s.Latency.P50, s.Latency.P95, s.Latency.P99, s.Latency.Count))
	b.WriteString("\n## Divergences\n\n")
	b.WriteString(fmt.Sprintf("Product-divergent ticks: %d; max heal %s; max in-flight %d\n", s.ProductDivergentTicks, s.MaxHeal, s.MaxInFlight))
	if len(s.Bursts) > 0 {
		b.WriteString("\n## Burst events (OBS_BURSTS)\n\n")
		for _, burst := range s.Bursts {
			b.WriteString(fmt.Sprintf("- %s (n=%d): create@%s -> up-converged %s; delete@%s -> down-converged %s\n",
				burst.Name, burst.Size, burst.CreateIssued, burst.UpConvergenceOrNever(), burst.DeleteIssuedOrNone(), burst.DownConvergenceOrNever()))
		}
	}
	if len(s.Forensics) > 0 {
		b.WriteString("\n### Forensics\n\n")
		for _, f := range s.Forensics {
			b.WriteString(fmt.Sprintf("- %s %s/%s %s instanceId=%s pod=%s kind=%s firstSeen=%s resolved=%s age=%dms ledger=%s@%s\n",
				f.Key, f.AppCode, f.Cluster, f.PodUID, f.InstanceID, f.PodName, f.Kind, f.FirstSeen, f.Resolved, f.AgeMS, f.LedgerOp, f.LedgerAt))
			for _, line := range f.LogSlice {
				b.WriteString(fmt.Sprintf("    log: %s\n", line))
			}
		}
	}
	if len(s.EnvWindows) > 0 {
		b.WriteString("\n## Environment windows\n\n")
		for _, w := range s.EnvWindows {
			b.WriteString(fmt.Sprintf("- %s %s -> %s (%d ticks) %s\n", w.Class, w.From, w.To, w.Ticks, w.Detail))
		}
	}
	b.WriteString("\n## Verdict\n\n")
	if s.Pass {
		b.WriteString("PASS — every acceptance criterion of §5.2 holds.\n")
	} else {
		b.WriteString("FAIL — \n")
		for _, fail := range s.Fails {
			b.WriteString(fmt.Sprintf("- %s\n", fail))
		}
	}
	return b.String()
}

// writeSummaryFile writes the JSON and markdown artifacts.
func writeSummaryFile(dir, stamp string, s runSummary) (jsonPath, mdPath string, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	jsonPath = filepath.Join(dir, fmt.Sprintf("%s-summary.json", stamp))
	mdPath = filepath.Join(dir, fmt.Sprintf("%s-summary.md", stamp))
	jsonBytes, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return "", "", err
	}
	if err := os.WriteFile(jsonPath, jsonBytes, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(mdPath, []byte(renderSummaryMarkdown(s)), 0o644); err != nil {
		return "", "", err
	}
	return jsonPath, mdPath, nil
}

// sortedKeys returns the sorted keys of a string-keyed map (deterministic
// iteration for summaries).
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// formatTime renders a time for records (RFC3339).
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
