//go:build observe
// +build observe

package observe

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Scratch ports of the observe stack (dsca-4 §4.1's port discipline: the
// demo stack owns 18848/18500/6443/12379/19999/19848/19849/18090 and the
// soak compose's range — every observe port is outside that set). The
// throwaway nacos container maps host 28848 -> container 8848; the Atlas
// stand-in and the child's metrics endpoint take the OBS_ATLAS_PORT /
// OBS_METRICS_PORT knobs (defaults 19997 / 19998).
const (
	defaultNacosHostPort = 28848
	defaultAtlasPort     = 19997
	defaultMetricsPort   = 19998
)

// PushIntervalSecs is the --push-interval the harness passes the child:
// 60s (the demo's cadence), the P of the OBS_BOUND formula. A full-push
// (SyncAll + prune) cycle runs every P, so a divergence whose only heal
// route is the prune legitimately waits up to P — that is exactly why P
// floors the bound.
const PushIntervalSecs = 60

// SloBoundSecs is the event-driven SLO bound leg of the OBS_BOUND formula
// (dsca-4 §3.4: OBS_BOUND = max(track-2's SLO bound, push-interval P)).
// Track 2's SLO table (dsca-2-latency.md §7) states burst convergence
// <= 10 s measured by exactly this kind of external harness — the p50<1s /
// p99<2s percentiles are per-instance, not last-instance-visible, so the
// external-harness bound is the SLO leg. max(10s, 60s) = 60s.
const SloBoundSecs = 10

// TickSecs is the per-tick observation cadence (dsca-4 §4.3: default 10s,
// knob OBS_TICK).
const TickSecs = 10

// ChurnEverySecs is the churn-driver cadence (dsca-4 §4.2: default 20s,
// knob OBS_CHURN_EVERY).
const ChurnEverySecs = 20

// DefaultChurnRatePercentPerMin is the default churn rate as a fraction of
// the base population per minute (dsca-4 §4.2: 5%/min, create-before-delete
// net-neutral, knob OBS_CHURN_RATE).
const DefaultChurnRatePercentPerMin = 5

// observeConfig carries the harness knobs resolved from the environment.
type observeConfig struct {
	// Duration is the wall-clock observation window.
	Duration time.Duration
	// SpotterBin is the built spotter binary the harness execs.
	SpotterBin string
	// Kubeconfig is the kwok cluster's kubeconfig.
	Kubeconfig string
	// WorkDir is the harness scratch dir (logs, records, summary).
	WorkDir string
	// ResultsDir is where the summary + JSONL artifacts land.
	ResultsDir string
	// BaseInstances is the steady-state pod population (OBS_BASE_INSTANCES).
	BaseInstances int
	// Services is the distinct app-code count (OBS_SERVICES).
	Services int
	// Tick is the observation cadence (OBS_TICK).
	Tick time.Duration
	// ChurnEvery is the churn cadence (OBS_CHURN_EVERY).
	ChurnEvery time.Duration
	// ChurnRatePctPerMin is the churn rate in % of base per minute
	// (OBS_CHURN_RATE).
	ChurnRatePctPerMin float64
	// NacosAddr is the throwaway nacos address (host:port).
	NacosAddr string
	// AtlasPort is the Atlas stand-in's scratch port.
	AtlasPort int
	// MetricsPort is the child's metrics endpoint port.
	MetricsPort int
	// StackMode selects the stack shape ("kwok" — the only shipped one; the
	// k3s rehearsal shape of §4.1 is not implemented, see the report).
	StackMode string
	// Bursts enables the §4.2 OBS_BURSTS schedule: a 100-in-1s storm early
	// (5%) + 200-instance batches at 25%/50%/75%, each create-then-rollback
	// with both convergence directions observed (OBS_BURSTS knob; "false"
	// keeps the sustained-churn-only shape of the 2h definitive window).
	Bursts bool
}

// loadObserveConfig resolves the harness knobs with the contract's
// defaults: 2h / 1000 instances / 20 services / 10s tick / 20s churn / 5%/min.
func loadObserveConfig() (observeConfig, error) {
	cfg := observeConfig{
		Duration:           2 * time.Hour,
		SpotterBin:         "build/observe/spotter",
		Kubeconfig:         "",
		WorkDir:            "build/observe",
		ResultsDir:         "tests/observe/results",
		BaseInstances:      1000,
		Services:           20,
		Tick:               TickSecs * time.Second,
		ChurnEvery:         ChurnEverySecs * time.Second,
		ChurnRatePctPerMin: DefaultChurnRatePercentPerMin,
		NacosAddr:          fmt.Sprintf("127.0.0.1:%d", defaultNacosHostPort),
		AtlasPort:          defaultAtlasPort,
		MetricsPort:        defaultMetricsPort,
		StackMode:          "kwok",
	}
	if root, err := repoRoot(); err == nil {
		cfg.SpotterBin = filepath.Join(root, "build/observe/spotter")
		cfg.WorkDir = filepath.Join(root, "build/observe")
		cfg.ResultsDir = filepath.Join(root, "tests/observe/results")
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_KUBECONFIG")); raw != "" {
		cfg.Kubeconfig = resolveRel(raw)
	} else {
		// The default vehicle: track 1's dsca1 kwok cluster kubeconfig
		// (the cluster this harness owns for its runs — dsca-1's scale
		// design leaves it running for reuse).
		defaultKC := filepath.Join(homeDir(), ".kwok/clusters/dsca1/kubeconfig.yaml")
		if fileExists(defaultKC) {
			cfg.Kubeconfig = defaultKC
		} else if root, err := repoRoot(); err == nil {
			cfg.Kubeconfig = filepath.Join(root, "build/observe/kubeconfig")
		}
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_DURATION")); raw != "" {
		d, err := parseDuration(raw)
		if err != nil {
			return observeConfig{}, fmt.Errorf("OBS_DURATION %q: %w", raw, err)
		}
		cfg.Duration = d
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_SCALE")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return observeConfig{}, fmt.Errorf("OBS_SCALE %q: not a positive integer", raw)
		}
		cfg.BaseInstances = n
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_SERVICES")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return observeConfig{}, fmt.Errorf("OBS_SERVICES %q: not a positive integer", raw)
		}
		cfg.Services = n
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_TICK")); raw != "" {
		d, err := parseDuration(raw)
		if err != nil || d <= 0 {
			return observeConfig{}, fmt.Errorf("OBS_TICK %q: not a duration", raw)
		}
		cfg.Tick = d
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_CHURN_EVERY")); raw != "" {
		d, err := parseDuration(raw)
		if err != nil || d <= 0 {
			return observeConfig{}, fmt.Errorf("OBS_CHURN_EVERY %q: not a duration", raw)
		}
		cfg.ChurnEvery = d
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_CHURN_RATE")); raw != "" {
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil || f < 0 {
			return observeConfig{}, fmt.Errorf("OBS_CHURN_RATE %q: not a non-negative number", raw)
		}
		cfg.ChurnRatePctPerMin = f
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_NACOS_ADDR")); raw != "" {
		cfg.NacosAddr = raw
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_ATLAS_PORT")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 65535 {
			return observeConfig{}, fmt.Errorf("OBS_ATLAS_PORT %q: not a port", raw)
		}
		cfg.AtlasPort = n
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_METRICS_PORT")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > 65535 {
			return observeConfig{}, fmt.Errorf("OBS_METRICS_PORT %q: not a port", raw)
		}
		cfg.MetricsPort = n
	}
	if raw := strings.TrimSpace(os.Getenv("OBS_STACK")); raw != "" {
		cfg.StackMode = raw
	}
	// OBS_BURSTS: "true"/"1" enables the §4.2 burst schedule (default on
	// for windows <= 30m — the burst-augmented window shape; the 2h
	// sustained default stays off so the definitive window is one shape).
	if raw := strings.TrimSpace(os.Getenv("OBS_BURSTS")); raw != "" {
		cfg.Bursts = raw == "true" || raw == "1"
	} else if cfg.Duration <= 30*time.Minute {
		cfg.Bursts = true
	}
	if cfg.Duration < time.Minute {
		return observeConfig{}, fmt.Errorf("OBS_DURATION %s is below the 1m harness floor", cfg.Duration)
	}
	if cfg.BaseInstances < cfg.Services {
		return observeConfig{}, fmt.Errorf("OBS_SCALE %d is below OBS_SERVICES %d (each service needs at least one pod)", cfg.BaseInstances, cfg.Services)
	}
	return cfg, nil
}

// obsBound is the single §3.4 formula, stated once: OBS_BOUND =
// max(the SLO bound, the push-interval bound P).
func (c observeConfig) obsBound() time.Duration {
	bound := time.Duration(SloBoundSecs) * time.Second
	if p := time.Duration(PushIntervalSecs) * time.Second; p > bound {
		bound = p
	}
	return bound
}

// parseDuration accepts a Go duration string or a bare number of seconds.
func parseDuration(raw string) (time.Duration, error) {
	if seconds, err := strconv.Atoi(raw); err == nil {
		return time.Duration(seconds) * time.Second, nil
	}
	return time.ParseDuration(raw)
}

// resolveRel anchors a relative path at the repo root (go test runs with
// the package directory as the working directory).
func resolveRel(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	if root, err := repoRoot(); err == nil {
		return filepath.Join(root, path)
	}
	return path
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// homeDir returns the user's home directory ("" when unresolvable).
func homeDir() string {
	if home, err := os.UserHomeDir(); err == nil {
		return home
	}
	return os.Getenv("HOME")
}

// repoRoot walks up from the working directory until go.mod is found.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}
