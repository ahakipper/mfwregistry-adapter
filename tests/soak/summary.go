//go:build soak
// +build soak

package soak

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// newContext is a plain background context (indirection keeps the child
// restart call sites readable).
func newContext() context.Context { return context.Background() }

// writeSummary writes the committed evidence artifact of plan §8.6: a soak
// summary (run window, stack versions, flags, per-scenario pass/fail,
// checks count, divergence count, max divergence age, max heal time per
// scenario, spotter restarts, final convergence table) — key metrics only,
// no raw log lines. The raw 1h log stays local under build/.
func (h *soakHarness) writeSummary(stamp string) string {
	// The summary is committed evidence (plan §8.6): tests/soak/results at
	// the REPOSITORY root. The test's working directory is the package
	// dir, so the path is anchored at the repo root go.mod identifies.
	dir := "tests/soak/results"
	if root, err := repoRoot(); err == nil {
		dir = filepath.Join(root, "tests", "soak", "results")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Logf("summary dir: %v", err)
		dir = h.cfg.WorkDir
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-local.md", stamp))

	passed := 0
	for _, s := range h.scenarios {
		if s.Pass {
			passed++
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# Soak summary — local stack run %s\n\n", stamp)
	fmt.Fprintf(&b, "- Run window: %s (started %s)\n", h.cfg.Duration, stamp)
	fmt.Fprintf(&b, "- Harness mode: %s\n", modeLabel(h.cfg.Duration))
	fmt.Fprintf(&b, "- Flags: --env test --providers k8s,ecs --push-interval %d --nacos-addr %s --consul-addr %s --etcd-endpoints <embedded> --kubeconfig %s\n",
		PushIntervalSecs, nacosAddr, consulAddr, h.cfg.Kubeconfig)
	fmt.Fprintf(&b, "- Stack: nacos/nacos-server:v2.1.0 (18848), hashicorp/consul:1.15 (18500), rancher/k3s:v1.28.8-k3s1 (6443), embedded etcd (etcdmock, loopback), atlas stand-in 127.0.0.1:15051\n")
	fmt.Fprintf(&b, "- Cadences: churn %s, assertions %s, incremental bound %s, full-push bound %s\n\n",
		h.schedule.ChurnEvery, h.schedule.AssertEvery, h.schedule.IncrementalBound, h.schedule.FullPushBound)

	fmt.Fprintf(&b, "## Scenarios\n\n")
	fmt.Fprintf(&b, "| # | Scenario | Result | Notes (max heal) |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- |\n")
	for _, s := range h.scenarios {
		fmt.Fprintf(&b, "%s\n", summaryScenarioLine(s))
	}
	fmt.Fprintf(&b, "\nScenarios passed: %d/%d\n\n", passed, len(h.scenarios))

	fmt.Fprintf(&b, "## Checks and convergence\n\n")
	fmt.Fprintf(&b, "- Standing checks: %d run, %d passed clean (divergence-free), %d divergence observations total\n",
		h.checksRun, h.checksPassed, h.divergences)
	fmt.Fprintf(&b, "- Max observed heal time: %s\n", healStr(h.healMax))
	fmt.Fprintf(&b, "- Spotter starts: %d (restarts: %d)\n", h.child.startsCount(), h.restarts)
	fmt.Fprintf(&b, "- Atlas stand-in calls: %d (SynInstance/SynAllInstance/GetAllInstance)\n", h.atlasPushCount())
	fmt.Fprintf(&b, "- Final convergence: %s\n", convLabel(h.converged))
	fmt.Fprintf(&b, "\nRaw log: %s (local only, not committed).\n", filepath.Join(h.cfg.WorkDir, "soak-"+stamp+".log"))

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		h.t.Fatalf("write summary %s: %v", path, err)
	}
	return path
}

// modeLabel distinguishes the full run from a smoke shakedown.
func modeLabel(d time.Duration) string {
	if d >= time.Hour {
		return "full 1h soak"
	}
	return fmt.Sprintf("smoke shakedown (%s window, cadences compressed; bounds are not the shipped ones)", d)
}

// convLabel renders the final convergence verdict.
func convLabel(ok bool) string {
	if ok {
		return "PASS (zero divergence on the final full comparison)"
	}
	return "FAIL (divergence persisted past the bound)"
}

// timeNow is a tiny seam for the stamp; kept for determinism in future reuse.
var timeNow = time.Now
