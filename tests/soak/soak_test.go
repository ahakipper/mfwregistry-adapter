//go:build soak
// +build soak

package soak

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/etcdmock"
)

// TestSoakLocalStack is the local full-stack e2e + soak harness of
// docs/nacos-sink-plan.md §8 (the plan's §8.4 names this exact test). It
// owns the dynamic lifecycle in the plan's order:
//
//  1. boot the embedded etcd (etcdmock, §8.2's primary election decision)
//  2. start the Atlas stand-in (discoverymock on a TCP listener, §8.3)
//  3. exec the spotter binary as a child with the §8.4 flag set
//  4. run the churn + scenario window (§8.5), including the kill/restart
//     scenarios
//  5. tear everything down via defers: binary first, then stand-in, then
//     the embedded etcd
//
// The containerized stack (nacos 18848, consul 18500, k3s 6443) must
// already be up and healthy — scripts/soak-up.sh (driven by make
// test-soak) does that before the test runs.
//
// SOAK_DURATION sets the window (default 1h). A minutes-long value is the
// harness's own smoke mode: the same code paths compressed, proving stack
// up, binary start, churn, assertions, scenarios and teardown without the
// full hour.
func TestSoakLocalStack(t *testing.T) {
	cfg, err := loadSoakConfig()
	if err != nil {
		t.Fatalf("soak config: %v", err)
	}
	schedule := scheduleFor(cfg.Duration)
	t.Logf("soak window: %s (churn every %s, assert every %s, incremental bound %s, full-push bound %s)",
		cfg.Duration, schedule.ChurnEvery, schedule.AssertEvery, schedule.IncrementalBound, schedule.FullPushBound)

	// --- static prerequisites -------------------------------------------
	if _, err := os.Stat(cfg.SpotterBin); err != nil {
		t.Fatalf("spotter binary %s not found (go build -o %s .): %v", cfg.SpotterBin, cfg.SpotterBin, err)
	}
	if _, err := os.Stat(cfg.Kubeconfig); err != nil {
		t.Fatalf("kubeconfig %s not found (scripts/soak-up.sh extracts it): %v", cfg.Kubeconfig, err)
	}
	nacos := newNacosObserver(nacosAddr)
	if err := nacos.readiness(); err != nil {
		t.Fatalf("nacos not ready on %s: %v", nacosAddr, err)
	}
	consul := newConsulDriver(consulAddr)
	if err := consul.healthy(); err != nil {
		t.Fatalf("consul not healthy on %s: %v", consulAddr, err)
	}
	k8s := &k8sDriver{kubeconfig: cfg.Kubeconfig}
	if _, err := k8s.kubectl("get", "nodes"); err != nil {
		t.Fatalf("kubectl cannot reach k3s with %s: %v", cfg.Kubeconfig, err)
	}

	// --- harness workdir + report --------------------------------------
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		t.Fatalf("workdir: %v", err)
	}
	if err := os.MkdirAll(cfg.WorkDir+"/log", 0o755); err != nil {
		t.Fatalf("workdir log dir: %v", err)
	}
	stamp := time.Now().Format("20060102-1504")
	soakLogPath := filepath.Join(cfg.WorkDir, fmt.Sprintf("soak-%s.log", stamp))
	log, err := newSoakLog(soakLogPath)
	if err != nil {
		t.Fatalf("soak log: %v", err)
	}
	defer func() { _ = log.close() }()

	harness := &soakHarness{
		t:         t,
		cfg:       cfg,
		schedule:  schedule,
		log:       log,
		nacos:     nacos,
		consul:    consul,
		k8s:       k8s,
		expected:  newExpectedState(),
		scenarios: []scenarioResult{},
	}

	// --- teardown discipline (plan §8.4 step 5: binary first, then -----
	// --- stand-in, then embedded etcd, via defers + signal handling) ---
	var child *spotterChild
	var standin *discoverymock.Server
	var etcd *etcdmock.Server
	defer func() {
		if child != nil {
			child.kill()
		}
		if standin != nil {
			standin.Close()
		}
		if etcd != nil {
			etcd.Close()
		}
	}()

	// Signal handling: an interrupted harness still cleans up the child.
	canceled := make(chan os.Signal, 1)
	notifySignals(canceled)
	defer stopSignals(canceled)
	go func() {
		select {
		case <-canceled:
			harness.log.event("harness interrupted: tearing down")
			if child != nil {
				child.kill()
			}
			os.Exit(130)
		case <-time.After(cfg.Duration + 10*time.Minute):
		}
	}()

	// --- step 1: boot the embedded etcd (B5 order) ---------------------
	log.event("booting embedded etcd")
	etcd, err = etcdmock.Start()
	if err != nil {
		t.Fatalf("embedded etcd: %v", err)
	}
	log.event("embedded etcd ready: %v", etcd.ClientEndpoints())

	// --- step 2: start the Atlas stand-in ------------------------------
	log.event("starting atlas stand-in")
	standin, err = discoverymock.StartTCP("127.0.0.1:15051")
	if err != nil {
		t.Fatalf("atlas stand-in: %v", err)
	}
	atlasAddr := standin.Addr()
	log.event("atlas stand-in listening on %s", atlasAddr)

	// --- step 3: exec the spotter binary as a child --------------------
	child = newSpotterChild(cfg.SpotterBin, cfg.WorkDir, cfg.Kubeconfig, etcd.ClientEndpoints(), atlasAddr)
	if err := child.start(context.Background()); err != nil {
		t.Fatalf("start spotter: %v", err)
	}
	if err := child.waitForHealthy(60 * time.Second); err != nil {
		t.Fatalf("spotter startup: %v", err)
	}
	if err := child.waitForLog("leader", 90*time.Second); err != nil {
		t.Fatalf("spotter election: %v", err)
	}
	harness.atlas = standin
	harness.child = child
	log.event("spotter child started (pid %d), metrics on :%d", child.pid(), MetricsPort)

	// --- step 4: the churn + scenario window ---------------------------
	harness.runWindow()

	// --- final convergence check (§8.5 hard criterion) -----------------
	harness.finalConvergence()

	// --- evidence: the summary file (§8.6) -----------------------------
	summaryPath := harness.writeSummary(stamp)
	t.Logf("soak summary written to %s", summaryPath)

	// --- verdict -------------------------------------------------------
	harness.report()
}

// soakHarness bundles everything the window needs.
type soakHarness struct {
	t        *testing.T
	cfg      soakConfig
	schedule windowSchedule
	log      *soakLog

	nacos    *nacosObserver
	consul   *consulDriver
	k8s      *k8sDriver
	expected *expectedState

	atlas     *discoverymock.Server
	child     *spotterChild
	scenarios []scenarioResult

	checksRun    int
	checksPassed int
	divergences  int
	healMax      time.Duration
	restarts     int
	converged    bool
}

// scenarioResult records one edge scenario's outcome (§8.6's summary table).
type scenarioResult struct {
	Letter string
	Name   string
	Pass   bool
	Note   string
}

// runWindow drives the whole churn + scenario window of §8.5: continuous
// churn on its cadence, the standing assertion loop on its cadence, and
// the edge scenarios at their scheduled points (fractions of the window in
// smoke mode).
func (h *soakHarness) runWindow() {
	d := h.cfg.Duration
	// Scenario schedule: (a) storm at 20%, (b) zero-healthy at 30%, (c)
	// both-providers at 5% (setup — it must exist before assertions judge
	// coexistence), (d) nacos restart at 40%, (e) consul outage at 55%,
	// (f) spotter restart at 70%, (g) rapid flap at 80%, (h) 100-replica
	// scale at 85%. Each phase is asserted in place.
	at := func(fraction float64) time.Duration {
		return time.Duration(float64(d) * fraction)
	}
	scenarioClock := []struct {
		when time.Duration
		run  func()
	}{
		{at(0.05), func() { h.scenarioBothProviders() }},  // (c)
		{at(0.20), func() { h.scenarioPodStorm() }},       // (a)
		{at(0.30), func() { h.scenarioZeroHealthy() }},    // (b)
		{at(0.40), func() { h.scenarioNacosRestart() }},   // (d)
		{at(0.55), func() { h.scenarioConsulOutage() }},   // (e)
		{at(0.70), func() { h.scenarioSpotterRestart() }}, // (f)
		{at(0.80), func() { h.scenarioRapidFlap() }},      // (g)
		{at(0.85), func() { h.scenarioScale100() }},       // (h)
	}

	// Seed the k8s leg so the binary has something to push from t=0.
	if err := h.k8s.apply(spikeApp, 2); err != nil {
		h.t.Errorf("seed k8s %s: %v", spikeApp, err)
	}
	h.syncExpectedK8s(spikeApp)

	// Seed the consul leg with the two stable app-codes (§8.5).
	for _, code := range stableConsulApps {
		id := consulID(code, 1)
		if err := h.consul.register(code, id, consulPort(code, id)); err != nil {
			h.t.Errorf("seed consul %s: %v", code, err)
		}
		h.expected.setConsul(code, []string{id})
	}

	started := time.Now()
	deadline := started.Add(d)
	scenarioIdx := 0
	churnTicker := time.NewTicker(h.schedule.ChurnEvery)
	defer churnTicker.Stop()
	assertTicker := time.NewTicker(h.schedule.AssertEvery)
	defer assertTicker.Stop()
	churnTick := 0

	// Scenarios run SEQUENTIALLY — the review decision after the
	// concurrent-scenario experiment regressed the smoke run: the scenario
	// goroutines and the churn loop both mutate the consul catalog and the
	// expected model (the churn's instance rotations and the scenarios'
	// deregister/restore paths can register the same instanceId under
	// different ports, producing two composite ids for one domain
	// instance), and a blocked (d) starves the (e)/(f)/(g) heal windows on
	// the compressed smoke schedule. Honest deviation from §8.5
	// "concurrently with scenarios", documented: the churn+assertion loop
	// keeps running across the whole window and between scenarios, but a
	// blocking scenario pauses the churn ticks for its duration, so the
	// retry-queue behavior during (d)'s JVM rebuild is observed from the
	// standing assertions (whose nacos reads error and recover) rather
	// than from continued source churn. The full 1h run's schedule leaves
	// several minutes between the outage scenarios, which the churn fills.
	for time.Now().Before(deadline) {
		select {
		case <-churnTicker.C:
			churnTick++
			h.churn(churnTick)
		case <-assertTicker.C:
			h.assertOnce(time.Until(deadline))
		case <-time.After(500 * time.Millisecond):
		}
		// Scenarios fire at their scheduled wall-clock points, in order.
		for scenarioIdx < len(scenarioClock) && time.Since(started) >= scenarioClock[scenarioIdx].when {
			h.log.event("scenario window point reached (idx %d)", scenarioIdx)
			scenarioClock[scenarioIdx].run()
			scenarioIdx++
		}
	}
	// Run any scenarios the short window did not reach (smoke mode: every
	// scenario executes, compressed).
	for ; scenarioIdx < len(scenarioClock); scenarioIdx++ {
		h.log.event("scenario window compressed (idx %d)", scenarioIdx)
		scenarioClock[scenarioIdx].run()
	}
}

// churn rotates the sources: k8s pod set sizes oscillate 1..10 and consul
// entries rotate under the two stable app-codes plus a rotating one
// (§8.5 continuous churn).
func (h *soakHarness) churn(tick int) {
	// k8s: scale spike-app in a 1..10 oscillation, rotating a second
	// app-code's presence.
	replicas := tick%(10-1+1) + 1
	if err := h.k8s.scale(spikeApp, replicas); err != nil {
		h.log.event("churn k8s scale %s x%d failed: %v", spikeApp, replicas, err)
	} else {
		h.syncExpectedK8s(spikeApp)
	}
	if tick%3 == 0 {
		rotating := rotatingApp(tick)
		if err := h.k8s.apply(rotating, 1); err != nil {
			h.log.event("churn k8s rotating %s failed: %v", rotating, err)
		} else {
			h.syncExpectedK8s(rotating)
		}
	} else if tick%3 == 1 {
		rotating := rotatingApp(tick - 1)
		if err := h.k8s.delete(rotating); err != nil {
			h.log.event("churn k8s delete %s failed: %v", rotating, err)
		} else {
			h.expected.setK8s(rotating, []string{})
		}
	}

	// consul: rotate the instance under the two stable app-codes (the new
	// registration replaces the old one — consul catalog register is
	// per-service-id, so a different id means BOTH exist; the churn
	// deregisters the old id explicitly to model a real deploy rotation)
	// and rotate a third app-code's presence.
	for _, code := range stableConsulApps {
		next := tick%4 + 1
		id := consulID(code, next)
		old := consulID(code, prevRotation(tick, 4))
		if old != id {
			if err := h.consul.deregisterService(code, old); err != nil {
				h.log.event("churn consul %s deregister old %s failed: %v", code, old, err)
			}
		}
		if err := h.consul.register(code, id, consulPort(code, id)); err != nil {
			h.log.event("churn consul %s register failed: %v", code, err)
			continue
		}
		ids, _ := h.consul.liveServiceIDs(code)
		h.expected.setConsul(code, ids)
	}
	if tick%2 == 0 {
		code := rotatingConsulApp(tick)
		if err := h.consul.register(code, consulID(code, tick), consulPort(code, consulID(code, tick))); err != nil {
			h.log.event("churn consul rotating %s register failed: %v", code, err)
		}
	} else {
		code := rotatingConsulApp(tick - 1)
		if err := h.consul.deregister(code); err != nil {
			h.log.event("churn consul rotating %s deregister failed: %v", code, err)
		}
		h.expected.setConsul(code, nil)
	}
}

// prevRotation returns the previous value of a tick%n+1 rotation, so the
// churn can deregister exactly the instance it is replacing.
func prevRotation(tick, n int) int {
	prev := (tick-1)%n + 1
	if prev < 1 {
		prev = n
	}
	return prev
}

// syncExpectedK8s reads back the live pods and records them as the
// expected k8s instance ids of appCode.
func (h *soakHarness) syncExpectedK8s(appCode string) {
	pods, err := h.k8s.livePods(appCode, 30*time.Second)
	if err != nil {
		h.log.event("k8s live pods %s failed: %v", appCode, err)
		return
	}
	h.expected.setK8s(appCode, pods)
}

// refreshExpected re-derives the expected sets of the given app-codes from
// the live sources (both sides a model entry exists for). Scenarios call
// it before waiting on a heal when an earlier phase may have legitimately
// changed the sources: the model is the source of truth, and a stale
// snapshot mislabels a healed state as divergent.
func (h *soakHarness) refreshExpected(appCodes []string) {
	for _, code := range appCodes {
		if h.expected.k8sFor(code) != nil {
			h.syncExpectedK8s(code)
		}
		if h.expected.consulFor(code) != nil {
			ids, err := h.consul.liveServiceIDs(code)
			if err == nil {
				h.expected.setConsul(code, ids)
			}
		}
	}
}

// assertOnce runs one standing assertion cycle and records it. Divergences
// inside the current bounds are observations (the soak reports a complete
// picture); the final convergence check is the hard criterion (§8.5).
func (h *soakHarness) assertOnce(remaining time.Duration) {
	services, divergences, err := h.nacos.checkState(h.expected)
	if err != nil {
		h.log.record(checkResult{Time: time.Now(), Detail: fmt.Sprintf("check error: %v", err)}, 0)
		return
	}
	h.checksRun++
	worst := time.Duration(0)
	detail := ""
	if len(divergences) > 0 {
		h.divergences += len(divergences)
		parts := make([]string, 0, len(divergences))
		for _, d := range divergences {
			parts = append(parts, d.String())
		}
		detail = strings.Join(parts, "; ")
		if len(detail) > 300 {
			detail = detail[:300] + "..."
		}
		// The divergence age is unknown per-item; bound the worst by what
		// the remaining window still allows to heal.
		worst = h.schedule.IncrementalBound
	} else {
		h.checksPassed++
	}
	h.log.record(checkResult{
		Time: time.Now(), Services: services, Divergences: len(divergences), Detail: detail,
	}, worst)
}

// finalConvergence is §8.5's hard criterion: after the window, one full
// comparison pass (with the sources settled) must return zero divergence.
func (h *soakHarness) finalConvergence() {
	h.t.Logf("final convergence check (bound: incremental %s, full push %s)",
		h.schedule.IncrementalBound, h.schedule.FullPushBound)
	bound := h.schedule.FullPushBound + h.schedule.IncrementalBound
	if bound < 60*time.Second {
		bound = 60 * time.Second
	}
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		// Refresh the expected sets from the sources first: the model is
		// the source of truth, not a stale snapshot. Every app-code the
		// model tracks on a side refreshes on that side (a consul outage
		// legitimately empties the catalog, so entries the model still
		// holds must be re-derived from the live source or the final check
		// would assert against a world that no longer exists).
		for _, code := range h.expected.appCodes() {
			if h.expected.k8sFor(code) != nil {
				h.syncExpectedK8s(code)
			}
			if h.expected.consulFor(code) != nil {
				ids, err := h.consul.liveServiceIDs(code)
				if err == nil {
					h.expected.setConsul(code, ids)
				}
			}
		}
		_, divergences, err := h.nacos.checkState(h.expected)
		if err == nil && len(divergences) == 0 {
			h.converged = true
			h.log.event("final convergence: PASS (zero divergence)")
			return
		}
		if err != nil {
			h.log.event("final convergence check error: %v", err)
		} else {
			parts := make([]string, 0, len(divergences))
			for _, d := range divergences {
				parts = append(parts, d.String())
			}
			h.log.event("final convergence pending: %s", strings.Join(parts, "; "))
		}
		time.Sleep(5 * time.Second)
	}
	h.log.event("final convergence: FAIL (divergence persisted past bound)")
}

// report prints the verdict summary into the test log.
func (h *soakHarness) report() {
	passedScenarios := 0
	for _, s := range h.scenarios {
		if s.Pass {
			passedScenarios++
		}
		h.t.Logf("scenario (%s) %-28s %s", s.Letter, s.Name, verdict(s.Pass))
	}
	h.t.Logf("checks: %d run / %d passed, divergence observations: %d, spotter starts: %d",
		h.checksRun, h.checksPassed, h.divergences, h.child.startsCount())

	problems := []string{}
	if !h.converged {
		problems = append(problems, "final state did not converge to zero divergence")
	}
	for _, s := range h.scenarios {
		if !s.Pass {
			problems = append(problems, fmt.Sprintf("scenario (%s) %s failed: %s", s.Letter, s.Name, s.Note))
		}
	}
	// The Atlas stand-in must have received pushes throughout (§8.5).
	if h.atlas != nil && len(h.atlas.Calls()) == 0 {
		problems = append(problems, "the Atlas stand-in received no pushes (fanout starved)")
	}
	if len(problems) > 0 {
		h.t.Errorf("soak verdict: FAIL — %s", strings.Join(problems, "; "))
		return
	}
	h.t.Logf("soak verdict: PASS (converged, %d/%d scenarios, atlas push count %d)",
		passedScenarios, len(h.scenarios), len(h.atlas.Calls()))
}

// verdict renders a pass/fail label.
func verdict(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}

// record appends a scenario result.
func (h *soakHarness) record(letter, name string, pass bool, note string) {
	h.scenarios = append(h.scenarios, scenarioResult{Letter: letter, Name: name, Pass: pass, Note: note})
	h.log.event("scenario (%s) %s: %s — %s", letter, name, verdict(pass), note)
}

// waitForConverged polls the assertion until the model and Nacos agree or
// the bound expires; returns the observed heal time (or -1 on miss).
//
// scope lists the app-codes the wait judges. A scenario asserts ITS OWN
// divergence — an unrelated service still healing from an earlier phase
// must not fail the wait (that mislabeling was a real smoke-run artifact:
// every scenario after a restart reported "never" while the global state
// converged one tick later). An empty scope judges every app-code (the
// standing assertion and the final check keep the global semantics).
//
// Nacos-read resilience, two rules (the measurement fix for the overlap
// pollution found in review): (1) the wait first gates on nacos actually
// serving the ns API — measuring a heal from inside a JVM restart would
// time the rebuild, not the pipeline; (2) a nacos read error (EOF/timeout/
// 5xx) DURING the wait is RETRYABLE, not divergence, and if nacos stops
// serving mid-wait the heal clock RESETS to the moment it serves again —
// the outage windows of an infrastructure scenario must not consume a
// data scenario's bound. Scenario (d) itself — the restart owner — is the
// one wait that legitimately measures through nacos being down, and it
// calls this AFTER its own post-restart gate, so the reset never triggers
// for it.
func (h *soakHarness) waitForConverged(bound time.Duration, scope ...string) time.Duration {
	h.waitNacosServing(10 * time.Minute)
	start := time.Now()
	deadline := start.Add(bound)
	for time.Now().Before(deadline) {
		_, divergences, err := h.nacos.checkStateScoped(h.expected, scope)
		if err == nil && len(divergences) == 0 {
			healed := time.Since(start)
			if healed > h.healMax {
				h.healMax = healed
			}
			return healed
		}
		if err != nil {
			// Retryable read error: if nacos itself stopped serving (a
			// restart overlap), re-gate and reset the clock — the bound
			// measures convergence, not the outage.
			if h.nacos.nsAPIReady() != nil {
				h.waitNacosServing(10 * time.Minute)
				start = time.Now()
				deadline = start.Add(bound)
			}
		}
		time.Sleep(2 * time.Second)
	}
	return -1
}

// waitNacosServing blocks until the v1 ns API serves (bounded).
func (h *soakHarness) waitNacosServing(bound time.Duration) {
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if h.nacos.nsAPIReady() == nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// atlasPushCount returns the stand-in's observed call count.
func (h *soakHarness) atlasPushCount() int {
	if h.atlas == nil {
		return 0
	}
	return len(h.atlas.Calls())
}

// app codes the churn uses (§8.5: 2 stable consul app-codes plus a rotating
// one; spike-app is the k8s mainstay).
const (
	spikeApp         = "spike-app"
	stableConsulApp1 = "soak-stable-1"
	stableConsulApp2 = "soak-stable-2"
)

var stableConsulApps = []string{stableConsulApp1, stableConsulApp2}

// consulID derives a consul service id.
func consulID(appCode string, n int) string {
	return fmt.Sprintf("%s-inst-%d", appCode, n)
}

// consulPort derives the registration port of a (appCode, instance id)
// pair — deterministically, so every registration site (seed, churn
// rotation, scenario restores) produces the SAME composite id for the
// same instance. Registering the same instanceId under different ports
// would create two Nacos composite ids for one domain instance (the
// duplicate mechanism found in review).
func consulPort(appCode, instanceID string) int {
	sum := 0
	for _, r := range appCode + "#" + instanceID {
		sum = (sum*31 + int(r)) % 2000
	}
	return 20000 + sum
}

// rotatingApp derives the rotating k8s app-code of a tick.
func rotatingApp(tick int) string {
	return fmt.Sprintf("soak-rot-k8s-%d", tick/3)
}

// rotatingConsulApp derives the rotating consul app-code of a tick.
func rotatingConsulApp(tick int) string {
	return fmt.Sprintf("soak-rot-ecs-%d", tick/2)
}
