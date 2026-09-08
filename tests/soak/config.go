//go:build soak
// +build soak

package soak

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Stack addresses: the fixed host ports of the compose stack (plan §8.3's
// port table; the plan's literal values are kept — 18848 exists precisely
// because host 8848 is taken by the colima SSH tunnel).
const (
	nacosAddr  = "127.0.0.1:18848"
	consulAddr = "127.0.0.1:18500"
)

// PushIntervalSecs is the --push-interval the harness passes the binary:
// 120s per plan §8.4, making the full-push reconciliation bound (2 min)
// observable inside the soak instead of the shipped 21600s default.
const PushIntervalSecs = 120

// MetricsPort is the spotter child's metrics endpoint port (ephemeral range
// per plan §8.3's 18090).
const MetricsPort = 18090

// soakConfig carries the harness knobs resolved from the environment.
type soakConfig struct {
	// Duration is the churn + scenario window length.
	Duration time.Duration
	// SpotterBin is the built binary the test execs.
	SpotterBin string
	// Kubeconfig is the k3s kubeconfig extracted by soak-up.sh.
	Kubeconfig string
	// WorkDir is the harness scratch dir (logs, kubeconfig, report).
	WorkDir string
}

// loadSoakConfig resolves the harness knobs from the environment with the
// plan's defaults. SOAK_DURATION accepts Go duration strings ("90s", "5m",
// "1h") and a bare number is seconds.
//
// The default paths are repo-relative, but `go test` runs the test with the
// package directory (tests/soak) as the working directory, so they are
// resolved against the repository root (located by walking up until go.mod).
func loadSoakConfig() (soakConfig, error) {
	cfg := soakConfig{
		Duration:   time.Hour,
		SpotterBin: "build/spotter",
		Kubeconfig: "build/soak/kubeconfig",
		WorkDir:    "build/soak",
	}
	if root, err := repoRoot(); err == nil {
		cfg.SpotterBin = filepath.Join(root, "build/spotter")
		cfg.Kubeconfig = filepath.Join(root, "build/soak/kubeconfig")
		cfg.WorkDir = filepath.Join(root, "build/soak")
	}
	if raw, ok := os.LookupEnv("SOAK_DURATION"); ok && strings.TrimSpace(raw) != "" {
		raw = strings.TrimSpace(raw)
		seconds, err := strconv.Atoi(raw)
		if err == nil {
			cfg.Duration = time.Duration(seconds) * time.Second
		} else {
			parsed, perr := time.ParseDuration(raw)
			if perr != nil {
				return soakConfig{}, fmt.Errorf("SOAK_DURATION %q: not a duration", raw)
			}
			cfg.Duration = parsed
		}
	}
	if raw := os.Getenv("SPOTTER_BIN"); strings.TrimSpace(raw) != "" {
		cfg.SpotterBin = strings.TrimSpace(raw)
	}
	if raw := os.Getenv("KUBECONFIG"); strings.TrimSpace(raw) != "" {
		cfg.Kubeconfig = strings.TrimSpace(raw)
	}
	if raw := os.Getenv("SOAK_WORKDIR"); strings.TrimSpace(raw) != "" {
		cfg.WorkDir = strings.TrimSpace(raw)
	}
	if cfg.Duration < 30*time.Second {
		return soakConfig{}, fmt.Errorf("SOAK_DURATION %s is below the 30s harness floor", cfg.Duration)
	}
	return cfg, nil
}

// smoke scales the harness's own timings for a short window: the full hour
// runs churn every ~20s, assertions every 10s and the scenarios at their
// scheduled points (plan §8.5); a minutes-long shakedown compresses every
// cadence proportionally so the same code paths execute. The window
// checkpoint times are fractions of the whole run.
type windowSchedule struct {
	// ChurnEvery is the churn-driver cadence.
	ChurnEvery time.Duration
	// AssertEvery is the standing assertion cadence.
	AssertEvery time.Duration
	// IncrementalBound is the heal bound for event-driven divergence.
	IncrementalBound time.Duration
	// FullPushBound is the heal bound for divergence whose only heal path
	// is the full push (the --push-interval tick).
	FullPushBound time.Duration
	// BatchBound is the generous batch bound for the 100-replica scale.
	BatchBound time.Duration
	// ConsulOutage is scenario (e)'s outage length.
	ConsulOutage time.Duration
}

// scheduleFor derives the cadences for a window of length d. The churn and
// assertion cadences compress with the window (a smoke shakedown drives the
// same code paths faster); the heal bounds do NOT compress below the real
// binary cadences — the --push-interval tick is a fixed 120s (plan §8.4),
// so bound compression would only mislabel every tick-dependent heal as
// "never healed".
func scheduleFor(d time.Duration) windowSchedule {
	scale := d
	if scale > time.Hour {
		scale = time.Hour
	}
	churn := 20 * time.Second
	assertEvery := 10 * time.Second
	if scale < time.Minute {
		churn = 8 * time.Second
		assertEvery = 5 * time.Second
	}
	fullPush := PushIntervalSecs * time.Second
	// The full-push bound never compresses below the real --push-interval
	// (120s, fixed by plan §8.4): the binary's tick cadence is NOT scaled
	// with the window, so a compressed bound would judge every
	// tick-dependent heal as "late". Only churn/assert cadences compress.
	// A window shorter than the interval (a sub-120s smoke) keeps the real
	// bound anyway — the scenario waits outlive the churn window, which is
	// exactly how the smoke exercises those waits.
	incremental := 30 * time.Second
	if incremental > scale {
		incremental = scale
	}
	batch := 300 * time.Second
	if batch > 10*scale {
		batch = 10 * scale
	}
	outage := 60 * time.Second
	if outage > scale/3 {
		outage = scale / 3
	}
	return windowSchedule{
		ChurnEvery:       churn,
		AssertEvery:      assertEvery,
		IncrementalBound: incremental,
		FullPushBound:    fullPush,
		BatchBound:       batch,
		ConsulOutage:     outage,
	}
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
