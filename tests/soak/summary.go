//go:build soak
// +build soak

package soak

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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

	// D-5: the retry-queue observation block. The per-cycle depths land in
	// the check lines of the raw log; the summary carries the last observed
	// line, the max depth, and the drain verdict — F7's non-draining queue
	// signature is a first-class summary row now.
	fmt.Fprintf(&b, "- Retry queue (sync_error_gauge, last observed): %s\n", h.lastQueueDepthLine())
	if maxSink, maxDepth := h.maxQueueDepth(); maxDepth > 0 {
		fmt.Fprintf(&b, "- Retry queue max depth: %d (sink %s)\n", maxDepth, maxSink)
	} else {
		fmt.Fprintf(&b, "- Retry queue max depth: 0 (never held an entry)\n")
	}
	fmt.Fprintf(&b, "- Retry queue drain bound (D-5, quiescence + %s): %s\n",
		h.schedule.FullPushBound, h.drainLabel())
	fmt.Fprintf(&b, "- Atlas payload parity (D-11): %s\n", h.atlasParityLabel())
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

// lastQueueDepthLine returns the most recently observed queue-depth line
// (D-5), or the unobserved marker.
func (h *soakHarness) lastQueueDepthLine() string {
	if len(h.queueDepthsLog) == 0 {
		return "unobserved (metrics endpoint never answered)"
	}
	return h.queueDepthsLog[len(h.queueDepthsLog)-1]
}

// maxQueueDepth returns the sink and depth of the deepest single-sink
// observation ever recorded in the raw log lines (the per-cycle depth lines
// carry "queue[a=0,n=1,...]" shapes; the parse tolerates them).
func (h *soakHarness) maxQueueDepth() (string, int) {
	maxDepth := 0
	maxSink := ""
	for _, line := range h.queueDepthsLog {
		for sink, depth := range parseDepthsLine(line) {
			if depth > maxDepth {
				maxDepth = depth
				maxSink = sink
			}
		}
	}
	return maxSink, maxDepth
}

// parseDepthsLine parses one "queue[a=0,n=1,__total__=1]" line back into a
// map (best-effort; malformed lines yield an empty map).
func parseDepthsLine(line string) map[string]int {
	depths := map[string]int{}
	open := strings.Index(line, "[")
	closeAt := strings.LastIndex(line, "]")
	if open < 0 || closeAt <= open {
		return depths
	}
	for _, pair := range strings.Split(line[open+1:closeAt], ",") {
		eq := strings.Index(pair, "=")
		if eq < 0 {
			continue
		}
		value, err := strconv.Atoi(strings.TrimSpace(pair[eq+1:]))
		if err != nil {
			continue
		}
		depths[strings.TrimSpace(pair[:eq])] = value
	}
	return depths
}

// drainLabel renders the D-5 drain-bound verdict for the summary.
func (h *soakHarness) drainLabel() string {
	if h.drainBreach == nil {
		return "PASS (queue drained to 0 after churn quiescence)"
	}
	return "FAIL — " + h.drainBreach.String()
}

// atlasParityLabel renders the D-11 Atlas payload parity verdict for the
// summary.
func (h *soakHarness) atlasParityLabel() string {
	if len(h.atlasDivergence) == 0 {
		return "PASS (every SynInstance payload matched the expected model)"
	}
	parts := make([]string, 0, len(h.atlasDivergence))
	for _, d := range h.atlasDivergence {
		parts = append(parts, d.String())
	}
	return "FAIL — " + strings.Join(parts, "; ")
}

// timeNow is a tiny seam for the stamp; kept for determinism in future reuse.
var timeNow = time.Now
