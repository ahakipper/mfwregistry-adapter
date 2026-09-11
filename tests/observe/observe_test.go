//go:build observe
// +build observe

package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/etcdmock"
)

// TestObserveConsistency is the dsca-4 §4 harness: the sustained
// large-scale consistency observation against the observe stack (throwaway
// nacos on a scratch port, in-process Atlas stand-in on a scratch port,
// embedded etcd, the spotter child built from the repo pointed at the kwok
// cluster with --reconcile-source nacos and --leader-elect=false).
//
// The run:
//  1. bring the throwaway nacos container up (health-gated);
//  2. boot the embedded etcd + the Atlas stand-in (scratch ports);
//  3. exec the child, health-gate it;
//  4. COLD ATTACH the base population (OBS_SCALE pods) and wait for the
//     initial convergence;
//  5. run the window: sustained churn (create-before-delete net-neutral,
//     OBS_CHURN_RATE %/min) + the per-tick observation loop (OBS_TICK);
//  6. evaluate the §5.2 acceptance criteria and write the artifacts.
//
// The demo stack is never addressed: every port is a scratch port; the
// kwok cluster is the harness's own vehicle.
func TestObserveConsistency(t *testing.T) {
	cfg, err := loadObserveConfig()
	if err != nil {
		t.Fatalf("observe config: %v", err)
	}
	bound := cfg.obsBound()
	t.Logf("observe window: %s, scale %d across %d services, tick %s, churn %.1f%%/min every %s, OBS_BOUND %s",
		cfg.Duration, cfg.BaseInstances, cfg.Services, cfg.Tick, cfg.ChurnRatePctPerMin, cfg.ChurnEvery, bound)

	// --- static prerequisites -------------------------------------------
	if _, err := os.Stat(cfg.SpotterBin); err != nil {
		t.Fatalf("spotter binary %s not found (go build -o %s .): %v", cfg.SpotterBin, cfg.SpotterBin, err)
	}
	if _, err := os.Stat(cfg.Kubeconfig); err != nil {
		t.Fatalf("kwok kubeconfig %s not found: %v", cfg.Kubeconfig, err)
	}
	appCodes := make([]string, cfg.Services)
	for i := range appCodes {
		appCodes[i] = fmt.Sprintf("%s-app-%d", observePodPrefix, i)
	}
	driver := newChurnDriver(cfg.Kubeconfig, appCodes, observePodPrefix)
	if _, err := driver.kubectlStdin("", "get", "nodes"); err != nil {
		t.Fatalf("kubectl cannot reach the kwok cluster with %s: %v", cfg.Kubeconfig, err)
	}

	// --- harness workdir + record writers -------------------------------
	if err := os.MkdirAll(cfg.WorkDir, 0o755); err != nil {
		t.Fatalf("workdir: %v", err)
	}
	if err := os.MkdirAll(cfg.WorkDir+"/log", 0o755); err != nil {
		t.Fatalf("workdir log dir: %v", err)
	}
	stamp := time.Now().Format("20060102-1504")
	harnessLog, err := newHarnessLog(filepath.Join(cfg.WorkDir, fmt.Sprintf("observe-%s.log", stamp)))
	if err != nil {
		t.Fatalf("harness log: %v", err)
	}
	defer func() { _ = harnessLog.close() }()
	records, err := newRecordWriter(filepath.Join(cfg.ResultsDir, fmt.Sprintf("%s-ticks.jsonl", stamp)))
	if err != nil {
		t.Fatalf("record writer: %v", err)
	}
	defer func() { _ = records.close() }()

	// --- the observe stack ------------------------------------------------
	// The throwaway nacos: owned container, scratch port, health-gated.
	if err := dockerRunNacos(nacosHostPortAsInt(cfg.NacosAddr)); err != nil {
		t.Fatalf("throwaway nacos: %v", err)
	}
	t.Cleanup(func() {
		if err := dockerStopNacos(); err != nil {
			t.Logf("throwaway nacos teardown: %v", err)
		}
	})
	if err := nacosHealthWait(cfg.NacosAddr, 10*time.Minute); err != nil {
		t.Fatalf("throwaway nacos health: %v", err)
	}
	harnessLog.event("throwaway nacos ready at %s", cfg.NacosAddr)

	view := newNacosView(cfg.NacosAddr)
	metrics := newMetricsView(cfg.MetricsPort)

	// The embedded etcd (the child's elector campaigns here — the demo's
	// etcd at 12379 is never shared).
	etcd, err := etcdmock.Start()
	if err != nil {
		t.Fatalf("embedded etcd: %v", err)
	}
	defer etcd.Close()
	harnessLog.event("embedded etcd ready: %v", etcd.ClientEndpoints())

	// The Atlas stand-in (the throwaway atlasrun): discoverymock on the
	// scratch TCP port — the child fails fast without a reachable Atlas.
	standin, err := discoverymock.StartTCP(fmt.Sprintf("127.0.0.1:%d", cfg.AtlasPort))
	if err != nil {
		t.Fatalf("atlas stand-in: %v", err)
	}
	defer standin.Close()
	atlasAddr := standin.Addr()
	harnessLog.event("atlas stand-in listening on %s", atlasAddr)

	// The child.
	child := newSpotterChild(cfg.SpotterBin, cfg.WorkDir, cfg.Kubeconfig,
		etcd.ClientEndpoints(), atlasAddr, cfg.NacosAddr, cfg.MetricsPort)
	defer child.kill()
	if err := child.start(context.Background()); err != nil {
		t.Fatalf("start spotter: %v", err)
	}
	if err := child.waitForHealthy(120 * time.Second); err != nil {
		t.Fatalf("spotter startup: %v", err)
	}
	harnessLog.event("spotter child started (pid %d), metrics on :%d, reconcile-source nacos, leader-elect=false", child.pid(), cfg.MetricsPort)

	// --- cold attach: the base population --------------------------------
	// Apply the harness's pods (the existing kwok pods of the same prefix
	// are torn down first: one clean population, one ledger).
	if err := driver.deleteAll(); err != nil {
		t.Fatalf("pre-run pod cleanup: %v", err)
	}
	harnessLog.event("cold attach: applying %d pods", cfg.BaseInstances)
	if _, err := driver.applyBatch(cfg.BaseInstances, 100, time.Now()); err != nil {
		t.Fatalf("cold attach apply: %v", err)
	}
	harnessLog.event("cold attach applied: %d pods across %d services", cfg.BaseInstances, cfg.Services)

	// The initial convergence wait: the first tick where the verdict is
	// CONSISTENT after the cold attach (bounded by 10 minutes — the
	// full-push path is the worst-case healer at 1000+ instances).
	started := time.Now()
	converged := false
	for time.Since(started) < 10*time.Minute {
		record := runTick(t, cfg, driver, view, metrics, child, records, harnessLog, 0, started)
		if record.Verdict == string(verdictConsistent) && record.Remote.Count == record.Source.Count {
			// CONSISTENT with the REMOTE side actually populated: the first
			// tick after the child's informer sync is CONSISTENT purely by
			// the in-flight tolerance (every expected entry young) with an
			// empty remote — a vacuous convergence for a cold-attach gate.
			converged = true
			harnessLog.event("cold attach converged after %s (source %d, remote %d)",
				time.Since(started).Round(time.Second), record.Source.Count, record.Remote.Count)
			break
		}
		if record.Verdict == string(verdictDivergent) {
			harnessLog.event("cold attach divergent tick: %d in-flight, %d divergences", record.InFlight, len(record.Divergence))
		}
		time.Sleep(cfg.Tick)
	}
	if !converged {
		t.Fatalf("cold attach did not converge within 10 minutes (see %s)", harnessLog.path)
	}

	// --- the observation window -------------------------------------------
	windowStart := time.Now()
	run := &observeRun{
		cfg:         cfg,
		driver:      driver,
		view:        view,
		metrics:     metrics,
		child:       child,
		records:     records,
		log:         harnessLog,
		tracker:     newDivergenceTracker(),
		appCodes:    appCodes,
		windowStart: windowStart,
	}
	run.runWindow(t)
	windowElapsed := time.Since(windowStart)

	// --- post-window: quiesced drain + final convergence -------------------
	// Churn stopped: after quiescence + one OBS_BOUND (the slowest
	// legitimate heal path), the queue must drain (D-5 semantics with the
	// two-consecutive-publication rule).
	run.drainCheck(t)

	// --- the acceptance evaluation -----------------------------------------
	summary := run.evaluate(t, stamp, windowElapsed)
	jsonPath, mdPath, err := writeSummaryFile(cfg.ResultsDir, stamp, summary)
	if err != nil {
		t.Fatalf("write summary: %v", err)
	}
	t.Logf("observe summary written to %s + %s", jsonPath, mdPath)
	for _, fail := range summary.Fails {
		t.Errorf("acceptance: %s", fail)
	}
	if summary.Pass {
		t.Logf("observe verdict: PASS (%d ticks, %d consistent, consistency ratio %.4f)",
			summary.Ticks, summary.Consistent, summary.ConsistencyRatio)
	} else {
		t.Errorf("observe verdict: FAIL — %s", strings.Join(summary.Fails, "; "))
	}

	// --- teardown: the harness's pods (the kwok cluster itself stays) -----
	if err := driver.deleteAll(); err != nil {
		t.Logf("pod teardown: %v", err)
	}
	_ = view.probeRemove()
}

// nacosHostPortAsInt extracts the host port of a host:port address.
func nacosHostPortAsInt(addr string) int {
	value := nacosHostPort(addr)
	var port int
	if _, err := fmt.Sscanf(value, "%d", &port); err != nil {
		return defaultNacosHostPort
	}
	return port
}

// harnessLog is the rolling timestamped observe log.
type harnessLog struct {
	path string
	file *os.File
}

func newHarnessLog(path string) (*harnessLog, error) {
	file, err := openAppend(path)
	if err != nil {
		return nil, err
	}
	return &harnessLog{path: path, file: file}, nil
}

func (l *harnessLog) event(format string, args ...interface{}) {
	_, _ = fmt.Fprintf(l.file, "%s EVENT %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

func (l *harnessLog) close() error { return l.file.Close() }

// observeRun carries the window's state.
type observeRun struct {
	cfg      observeConfig
	driver   *churnDriver
	view     *nacosView
	metrics  *metricsView
	child    *spotterChild
	records  *recordWriter
	log      *harnessLog
	tracker  *divergenceTracker
	appCodes []string

	windowStart time.Time

	tickCount       int
	consistent      int
	divergent       int
	obsErr          int
	sourceGEBase    int
	minSource       int
	maxInFlight     int
	maxRetryDepth   int
	maxRobotDepth   int
	droppedTotal    float64
	obsErrStreak    int
	maxObsErrStreak int

	// Latency percentiles (the last scrape's histogram — cumulative).
	latency *histogramSnapshot

	// Env classification state.
	envClass   string
	envFrom    time.Time
	envTicks   int
	envWindows []envWindow
	envDetail  string

	// Divergence classification (§4.5's separation): divergent ticks are
	// product (no env overlap at firstSeen, or persisted past the outage
	// end + one bound) or env-overlap (recorded honestly, separated in the
	// summary; only product divergence fails the acceptance).
	productDivergentTicks int
	envDivergentTicks     int

	// Churn accounting.
	churnCreates  int
	churnDeletes  int
	churnErrors   int
	churnQuiesced time.Time

	// Sub-tick churn fraction accumulator (small-scale runs).
	fractionAcc float64

	// Burst events (§4.2 OBS_BURSTS): the 100-in-1s storm early + the
	// 200-instance batches at 25%/50%/75%. Each burst creates its pods,
	// waits one OBS_BOUND + tick (so the up-convergence is observed by the
	// standing ticks), then deletes its own pods (the down-convergence is
	// judged by the ongoing ticks — an entry unconverged past the bound
	// flips its tick DIVERGENT, the no-amnesty semantics).
	burstMu     sync.Mutex
	burstEvents []burstEvent

	// Drain state.
	drainHeld     map[string]int // the previous nonzero depth past the gate
	drainBreach   bool
	lastDrainZero bool
}

// burstEvent is one §4.2 burst's record: the storm (n=100, one apply,
// ~1s) or a batch (n=200, two 100-pod applies). ConvergedUp/Down are
// stamped by the tick loop (a burst pod not appearing in a tick's
// divergence list is converged on that side — missing/extra entries are
// exactly how an unconverged burst pod surfaces).
type burstEvent struct {
	Name          string
	Size          int
	Pods          []string
	CreateIssued  time.Time
	DeleteIssued  time.Time
	ConvergedUp   time.Time
	ConvergedDown time.Time
}

// burstSchedule derives the window's burst marks: the storm at 5% (early),
// the batches at 25%/50%/75% (dsca-4 §4.2's OBS_BURSTS row).
func burstSchedule(d time.Duration) []struct {
	at time.Duration
	n  int
	id string
} {
	mark := func(f float64) time.Duration { return time.Duration(float64(d) * f) }
	return []struct {
		at time.Duration
		n  int
		id string
	}{
		{mark(0.05), 100, "storm-100"},
		{mark(0.25), 200, "batch-200@25%"},
		{mark(0.50), 200, "batch-200@50%"},
		{mark(0.75), 200, "batch-200@75%"},
	}
}

// runBursts executes the burst schedule on its own goroutine (churn and
// ticks keep running — a burst never blocks an observation). Each burst:
// apply n pods (the storm in ONE 100-pod apply ≈ 1s), wait OBS_BOUND + one
// tick, delete the SAME pods (the rollback leg — both convergence
// directions are observed and bounded).
func (r *observeRun) runBursts(stop <-chan struct{}) {
	for _, b := range burstSchedule(r.cfg.Duration) {
		wait := b.at - time.Since(r.windowStart)
		if wait > 0 {
			select {
			case <-stop:
				return
			case <-time.After(wait):
			}
		}
		r.log.event("burst %s: applying %d pods", b.id, b.n)
		issued := time.Now()
		pods, err := r.driver.applyBatch(b.n, 100, issued)
		if err != nil {
			r.churnErrors++
			r.log.event("burst %s: apply failed: %v", b.id, err)
			continue
		}
		r.burstMu.Lock()
		r.burstEvents = append(r.burstEvents, burstEvent{
			Name: b.id, Size: b.n, Pods: pods, CreateIssued: issued,
		})
		r.burstMu.Unlock()
		r.churnCreates += b.n
		r.log.event("burst %s: %d pods applied (ledger create@%s)", b.id, b.n, formatTime(issued))
		// The settle: one OBS_BOUND + one tick, so the up-convergence is
		// judged by real ticks (an entry unconverged past the bound makes
		// the tick DIVERGENT — no amnesty).
		select {
		case <-stop:
			return
		case <-time.After(r.cfg.obsBound() + r.cfg.Tick):
		}
		delIssued := time.Now()
		if err := r.driver.deletePods(pods, delIssued); err != nil {
			r.churnErrors++
			r.log.event("burst %s: delete failed: %v", b.id, err)
			continue
		}
		r.burstMu.Lock()
		r.burstEvents[len(r.burstEvents)-1].DeleteIssued = delIssued
		r.burstMu.Unlock()
		r.churnDeletes += b.n
		r.log.event("burst %s: %d pods deleted (ledger delete@%s)", b.id, b.n, formatTime(delIssued))
	}
}

// checkBurstConvergence folds one tick's divergence list into the burst
// accounting: a burst pod absent from the list is converged on the side
// the burst is currently proving (up: expected-online + remote-present +
// enabled — a missing/disabled entry is how an unconverged up-pod
// surfaces; down: source-gone + remote-gone — an extra entry is how an
// unconverged down-pod surfaces). OBSERR ticks carry no divergence
// evidence and are skipped.
func (r *observeRun) checkBurstConvergence(record tickRecord) {
	if record.Verdict == string(verdictObsErr) {
		return
	}
	r.burstMu.Lock()
	defer r.burstMu.Unlock()
	if len(r.burstEvents) == 0 {
		return
	}
	tickTS, err := time.Parse(time.RFC3339, record.TS)
	if err != nil {
		return
	}
	divIDs := map[string]bool{}
	for _, d := range record.Divergence {
		divIDs[d.ID] = true
	}
	for i := range r.burstEvents {
		ev := &r.burstEvents[i]
		if ev.ConvergedUp.IsZero() && tickTS.After(ev.CreateIssued) {
			converged := true
			for _, pod := range ev.Pods {
				if divIDs[pod] {
					converged = false
					break
				}
			}
			if converged {
				ev.ConvergedUp = tickTS
				r.log.event("burst %s: UP-converged after %s", ev.Name, tickTS.Sub(ev.CreateIssued).Round(time.Second))
			}
		}
		if !ev.DeleteIssued.IsZero() && ev.ConvergedDown.IsZero() &&
			tickTS.After(ev.DeleteIssued.Add(r.cfg.Tick)) {
			converged := true
			for _, pod := range ev.Pods {
				if divIDs[pod] {
					converged = false
					break
				}
			}
			if converged {
				ev.ConvergedDown = tickTS
				r.log.event("burst %s: DOWN-converged after %s", ev.Name, tickTS.Sub(ev.DeleteIssued).Round(time.Second))
			}
		}
	}
}

// runWindow runs the churn + observation window: the churn on its cadence,
// the observation tick on its cadence, both until the wall-clock deadline.
// One goroutine runs the churn, the loop's own goroutine runs the ticks —
// the churn never blocks an observation (the tick reads whatever the
// cluster holds when it reads).
func (r *observeRun) runWindow(t *testing.T) {
	deadline := r.windowStart.Add(r.cfg.Duration)
	churnEvery := r.cfg.ChurnEvery
	stopChurn := make(chan struct{})
	defer close(stopChurn)
	go func() {
		tick := time.NewTicker(churnEvery)
		defer tick.Stop()
		for {
			select {
			case <-stopChurn:
				return
			case <-tick.C:
				if time.Now().After(deadline) {
					return
				}
				r.churnOnce()
			}
		}
	}()
	// The §4.2 burst events run on their own goroutine: churn and the
	// observation ticks continue across a burst (the storm shape is a
	// superposition, not a pause).
	if r.cfg.Bursts {
		go r.runBursts(stopChurn)
	}

	tickNo := 0
	next := time.Now()
	for time.Now().Before(deadline) {
		now := time.Now()
		if now.Before(next) {
			time.Sleep(next.Sub(now))
		}
		tickNo++
		record := r.observeTick(t, tickNo)
		r.recordTick(t, record)
		next = next.Add(r.cfg.Tick)
		if drift := time.Since(next); drift > 0 {
			// A stretched tick still produces exactly one record; the
			// cadence catches up from the next slot boundary.
			next = time.Now().Add(r.cfg.Tick)
		}
	}
	r.churnQuiesced = time.Now()
}

// churnOnce executes one churn cadence: create-before-delete net-neutral
// replacement of rate/100 * base * (churnEvery/minute) pods. The ledger
// records every mutation with its issue time (the in-flight clock).
func (r *observeRun) churnOnce() {
	exact := r.churnPerTickExact()
	r.fractionAcc += exact
	if r.fractionAcc < 1 {
		return // sub-tick fraction has not accumulated to one pod
	}
	perTick := int(r.fractionAcc)
	r.fractionAcc -= float64(perTick)
	// Pick perTick victims from the live source view (one kubectl read).
	// The candidate filter is the harness's OWN pods only (observed
	// app-code + the harness's pod-name prefix): run 2 of the rehearsal
	// ladder proved the unfiltered picker deletes FOREIGN pods (track 1's
	// leftover dsca-scale-app pods — the run-2 population drift), so the
	// filter is load-bearing, not cosmetic.
	pods, err := r.driver.liveSourcePods("app-code")
	if err != nil {
		r.churnErrors++
		r.log.event("churn: source read failed: %v", err)
		return
	}
	candidates := make([]string, 0, len(pods))
	for _, pod := range pods {
		if pod.Phase == "Running" && r.isOwnPod(pod) {
			candidates = append(candidates, pod.Name)
		}
	}
	if len(candidates) <= perTick {
		// Never drop below base: create-before-delete, but the source
		// read is stale — only delete what exists beyond the replacement.
		perTick = len(candidates) - 1
		if perTick < 0 {
			perTick = 0
		}
	}
	victims := pickVictims(candidates, perTick)
	issued := time.Now()
	if len(victims) > 0 {
		// CREATE first (net-neutral: the population never drops below base).
		if _, err := r.driver.applyBatch(len(victims), 100, issued); err != nil {
			r.churnErrors++
			r.log.event("churn: create failed: %v", err)
		} else {
			r.churnCreates += len(victims)
		}
		if err := r.driver.deletePods(victims, issued); err != nil {
			r.churnErrors++
			r.log.event("churn: delete failed: %v", err)
		} else {
			r.churnDeletes += len(victims)
		}
	}
}

// isOwnPod reports whether a live pod belongs to this harness (its
// app-code is one of the run's observed services AND its name carries the
// harness's pod prefix — the foreign-pod guard the churn victim picker
// and the burst accounting key on).
func (r *observeRun) isOwnPod(pod sourcePod) bool {
	return isObservedAppCode(pod.AppCode, r.appCodes) &&
		strings.HasPrefix(pod.Name, r.driver.prefix+"-pod-")
}

// churnPerTickExact returns the fractional churn count per cadence.
func (r *observeRun) churnPerTickExact() float64 {
	if r.cfg.ChurnRatePctPerMin == 0 {
		return 0
	}
	perMinute := float64(r.cfg.BaseInstances) * r.cfg.ChurnRatePctPerMin / 100
	return perMinute * float64(r.cfg.ChurnEvery) / float64(time.Minute)
}

// pickVictims selects n pod names from the candidates (deterministic
// rotation: the harness's own pods, sorted).
func pickVictims(candidates []string, n int) []string {
	if n >= len(candidates) {
		return append([]string(nil), candidates...)
	}
	sorted := append([]string(nil), candidates...)
	sort.Strings(sorted)
	return sorted[:n]
}

// observeTick executes one §4.3 loop pass, folds the divergences into the
// continuity tracker (the §4.3 step-7 ledger: firstSeen pins the true
// age), and returns the tick record.
func (r *observeRun) observeTick(t *testing.T, tickNo int) tickRecord {
	record := runTick(t, r.cfg, r.driver, r.view, r.metrics, r.child, r.records, r.log, tickNo, r.windowStart)
	// The continuity tracker sees the tick's divergences (the §6.2-corrected
	// semantics: in-flight mismatches still count as divergences — their
	// heal time is the acceptance's convergence criterion — while the tick
	// verdict tolerates them inside the bound).
	r.tracker.observeTick(time.Now(), record.Divergence)
	return record
}

// recordTick folds one tick record into the run's aggregates.
func (r *observeRun) recordTick(t *testing.T, record tickRecord) {
	r.tickCount++
	// §4.5's env-overlap separation: a DIVERGENT tick whose tick overlaps
	// an open env window is tagged env-overlap (still recorded as
	// divergent — the honest log — but the summary separates it from
	// product divergence; only product divergence fails the acceptance).
	envOverlap := r.envClass != "" && r.envClass != "healthy"
	switch record.Verdict {
	case string(verdictConsistent):
		r.consistent++
		r.obsErrStreak = 0
	case string(verdictDivergent):
		r.divergent++
		r.obsErrStreak = 0
		if envOverlap {
			r.envDivergentTicks++
			r.log.event("tick %d: DIVERGENT with env overlap (%s)", record.Tick, r.envClass)
		} else {
			r.productDivergentTicks++
		}
	case string(verdictObsErr):
		r.obsErr++
		r.obsErrStreak++
		if r.obsErrStreak > r.maxObsErrStreak {
			r.maxObsErrStreak = r.obsErrStreak
		}
	}
	if record.Source.Count >= r.cfg.BaseInstances {
		r.sourceGEBase++
	}
	if r.tickCount == 1 || record.Source.Count < r.minSource {
		r.minSource = record.Source.Count
	}
	if record.InFlight > r.maxInFlight {
		r.maxInFlight = record.InFlight
	}
	if record.Queue.RetryNacos > r.maxRetryDepth {
		r.maxRetryDepth = record.Queue.RetryNacos
	}
	if record.Queue.RobotDepth > r.maxRobotDepth {
		r.maxRobotDepth = record.Queue.RobotDepth
	}
	if record.Queue.Dropped > r.droppedTotal {
		r.droppedTotal = record.Queue.Dropped
	}
	// Env classification bookkeeping: entering/leaving a non-healthy class
	// closes/opens a window.
	class := record.Env.Class
	if class == "" {
		class = "healthy"
	}
	if class == "healthy" {
		if r.envClass != "" && r.envClass != "healthy" {
			r.envWindows = append(r.envWindows, envWindow{
				Class: r.envClass, From: formatTime(r.envFrom), To: formatTime(time.Now()),
				Ticks: r.envTicks, Detail: r.envDetail,
			})
			r.envClass = ""
			r.envTicks = 0
		}
	} else {
		if r.envClass == "" || r.envClass == "healthy" {
			r.envClass = class
			r.envFrom = time.Now()
			r.envTicks = 1
			r.envDetail = record.Env.Detail
		} else if class == r.envClass {
			r.envTicks++
		}
	}
	// The burst accounting folds this tick's divergence evidence (§4.2's
	// burst convergence measurement).
	r.checkBurstConvergence(record)
}

// drainCheck enforces the D-5 drain bound after churn quiescence: wait
// one OBS_BOUND past the last churn, then the retry queue must be 0,
// judged across two consecutive scrapes (the worker publishes at the
// start of each 5s retry tick — one scrape can be a stale publication).
func (r *observeRun) drainCheck(t *testing.T) {
	if r.churnQuiesced.IsZero() {
		return
	}
	deadline := r.churnQuiesced.Add(r.cfg.obsBound())
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
	}
	// Two consecutive scrapes, both must be zero.
	for i := 0; i < 2; i++ {
		m, err := r.metrics.observe()
		if err != nil {
			r.log.event("drain check: metrics scrape failed: %v", err)
			r.drainBreach = true // un-observable is not drained
			return
		}
		if m.RetryDepths["nacos"] > 0 {
			r.log.event("drain check: retry queue holds %d entries (scrape %d)", m.RetryDepths["nacos"], i+1)
			r.drainBreach = true
			// One more wait: the two-consecutive rule tolerates one stale
			// publication — a held depth on the SECOND scrape is the breach.
			if i == 0 {
				time.Sleep(6 * time.Second)
				continue
			}
			return
		}
	}
	r.lastDrainZero = true
}

// evaluate folds the aggregates into the §5.2 acceptance verdict.
func (r *observeRun) evaluate(t *testing.T, stamp string, windowElapsed time.Duration) runSummary {
	summary := runSummary{
		Stamp:             stamp,
		Duration:          windowElapsed.Round(time.Second).String(),
		Scale:             r.cfg.BaseInstances,
		Services:          r.cfg.Services,
		ChurnRate:         r.cfg.ChurnRatePctPerMin,
		ObsBound:          r.cfg.obsBound().String(),
		PushIntervalSecs:  PushIntervalSecs,
		ReconcileSource:   "nacos",
		Ticks:             r.tickCount,
		Consistent:        r.consistent,
		Divergent:         r.divergent,
		ObsErr:            r.obsErr,
		TicksSourceGEBase: r.sourceGEBase,
		MinSourceCount:    r.minSource,
		MaxInFlight:       r.maxInFlight,
		MaxRetryDepth:     r.maxRetryDepth,
		MaxRobotDepth:     r.maxRobotDepth,
		DroppedTotal:      r.droppedTotal,
		DrainedAtEnd:      r.lastDrainZero && !r.drainBreach,
		MaxHeal:           r.tracker.maxHeal().Round(time.Millisecond).String(),
		EnvWindows:        r.envWindows,
		ChurnCreates:      r.churnCreates,
		ChurnDeletes:      r.churnDeletes,
		ChurnErrors:       r.churnErrors,
	}
	// The burst events' measured records (§4.2 OBS_BURSTS): both legs'
	// convergence times, judged against OBS_BOUND by the acceptance below.
	r.burstMu.Lock()
	for _, ev := range r.burstEvents {
		b := burstSummary{
			Name:         ev.Name,
			Size:         ev.Size,
			CreateIssued: formatTime(ev.CreateIssued),
			DeleteIssued: formatTime(ev.DeleteIssued),
		}
		if !ev.ConvergedUp.IsZero() {
			b.ConvergedUp = formatTime(ev.ConvergedUp)
			b.UpConvergence = ev.ConvergedUp.Sub(ev.CreateIssued).Round(time.Second).String()
		}
		if !ev.ConvergedDown.IsZero() {
			b.ConvergedDown = formatTime(ev.ConvergedDown)
			b.DownConvergence = ev.ConvergedDown.Sub(ev.DeleteIssued).Round(time.Second).String()
		}
		summary.Bursts = append(summary.Bursts, b)
	}
	r.burstMu.Unlock()
	if r.tickCount > 0 {
		summary.ConsistencyRatio = float64(r.consistent) / float64(r.tickCount)
		summary.ObsErrRatio = float64(r.obsErr) / float64(r.tickCount)
		summary.TicksSourceGEBaseRatio = float64(r.sourceGEBase) / float64(r.tickCount)
	}
	// The latency percentiles from the LAST scrape (the cumulative
	// histogram of the whole window).
	if m, err := r.metrics.observe(); err == nil && m.Latency != nil {
		summary.Latency.P50 = m.Latency.Percentile(0.50)
		summary.Latency.P95 = m.Latency.Percentile(0.95)
		summary.Latency.P99 = m.Latency.Percentile(0.99)
		summary.Latency.Count = m.Latency.Count
		r.latency = m.Latency
	} else if err != nil {
		summary.Fails = append(summary.Fails, fmt.Sprintf("final metrics scrape failed: %v (latency unobserved)", err))
	}
	// The forensics: every tracked divergence with its ledger link and the
	// child's log slice around firstSeen.
	for _, tracked := range r.tracker.entries {
		f := forensicRecord{
			Key:        tracked.Key,
			AppCode:    tracked.Divergence.AppCode,
			Cluster:    tracked.Divergence.Cluster,
			PodUID:     tracked.Divergence.ID,
			PodName:    tracked.Divergence.ID,
			InstanceID: tracked.Divergence.Composite,
			Kind:       string(tracked.Divergence.Kind),
			FirstSeen:  formatTime(tracked.FirstSeen),
			Resolved:   formatTime(tracked.Resolved),
		}
		if !tracked.Resolved.IsZero() {
			f.AgeMS = tracked.Resolved.Sub(tracked.FirstSeen).Milliseconds()
		} else {
			f.AgeMS = time.Since(tracked.FirstSeen).Milliseconds()
		}
		if entry, ok := r.driver.ledgerLookup(tracked.Divergence.ID); ok {
			f.LedgerOp = entry.Op
			f.LedgerAt = formatTime(entry.IssuedAt)
		}
		// The forensic window spans firstSeen ±30s: the register lines
		// trail the ADD by the push latency, and the retry lines trail
		// further — a 5s window misses exactly the lines the record
		// exists to capture.
		f.LogSlice = r.child.logSlice(tracked.FirstSeen, 30*time.Second,
			[]string{tracked.Divergence.ID, tracked.Divergence.Composite}, 8)
		summary.Forensics = append(summary.Forensics, f)
	}
	sort.Slice(summary.Forensics, func(i, j int) bool {
		return summary.Forensics[i].Key < summary.Forensics[j].Key
	})

	// Product-divergent ticks: the recordTick separation already counted
	// divergent ticks into product (no env overlap) vs env-overlap (§4.5's
	// honest log; only product divergence fails the acceptance).
	summary.ProductDivergentTicks = r.productDivergentTicks
	if r.envDivergentTicks > 0 {
		summary.Fails = append(summary.Fails, fmt.Sprintf("%d divergent ticks overlapped environment windows (separated per §4.5; see envWindows)", r.envDivergentTicks))
	}

	pass, fails := r.evaluateAcceptance(summary)
	summary.Pass = pass && len(summary.Fails) == 0
	summary.Fails = append(summary.Fails, fails...)
	return summary
}

// evaluateAcceptance checks every §5.2 criterion (the definitive-run
// shape, scaled by the run's own knobs: the ≥95% scale criterion is judged
// against this run's OBS_SCALE).
func (r *observeRun) evaluateAcceptance(s runSummary) (bool, []string) {
	fails := []string{}
	// Duration: the window completed (the run loop only exits at the
	// deadline; a truncated run never reaches evaluate). The floor is the
	// run's own configured duration — the definitive 2h gate stays 2h
	// (the contract's >= 2h0m), and a smoke's 1m window is judged against
	// its own 1m.
	if windowParsed, err := time.ParseDuration(s.Duration); err == nil {
		if windowParsed < r.cfg.Duration-5*time.Second {
			fails = append(fails, fmt.Sprintf("duration: window %s < configured %s (truncated)", s.Duration, r.cfg.Duration))
		}
	}
	// Scale: source count >= base on >= 95% of ticks.
	if s.Ticks > 0 && s.TicksSourceGEBaseRatio < 0.95 {
		fails = append(fails, fmt.Sprintf("scale: source >= %d on only %.4f of ticks (< 0.95; min observed %d)",
			s.Scale, s.TicksSourceGEBaseRatio, s.MinSourceCount))
	}
	// The core requirement: zero product-divergent ticks.
	if s.ProductDivergentTicks > 0 {
		fails = append(fails, fmt.Sprintf("consistency: %d product-divergent ticks (requirement: zero; see the forensics)", s.ProductDivergentTicks))
	}
	// OBSERR bounds: no streak > 6 ticks, < 5% of ticks.
	if s.MaxObsErrStreak > 6 {
		fails = append(fails, fmt.Sprintf("observability: max consecutive OBSERR streak %d ticks (> 6)", s.MaxObsErrStreak))
	}
	if s.Ticks > 0 && s.ObsErrRatio >= 0.05 {
		fails = append(fails, fmt.Sprintf("observability: OBSERR ratio %.4f (>= 0.05)", s.ObsErrRatio))
	}
	// No duplicates: the engine flags them as divergent (never tolerable) —
	// covered by the divergence criterion; a dup-only check for the
	// summary line.
	for _, f := range s.Forensics {
		if f.Kind == string(divDup) {
			fails = append(fails, fmt.Sprintf("duplicates: %s (duplicate composite instance ids — never tolerable)", f.Key))
			break
		}
	}
	// Retry queue drained at end.
	if !s.DrainedAtEnd {
		fails = append(fails, fmt.Sprintf("retry queue: not drained at window end (max depth %d)", s.MaxRetryDepth))
	}
	// Drops: zero over the window (the completeness SLO).
	if s.DroppedTotal > 0 {
		fails = append(fails, fmt.Sprintf("completeness: events_dropped_total = %.0f over the window (SLO: 0)", s.DroppedTotal))
	}
	// Convergence: every ledger entry converged within OBS_BOUND; the max
	// observed heal must be <= the bound.
	if heal := r.tracker.maxHeal(); heal > r.cfg.obsBound() {
		fails = append(fails, fmt.Sprintf("convergence: max heal %s exceeds OBS_BOUND %s", heal, r.cfg.obsBound()))
	}
	// Burst events (§4.2 OBS_BURSTS): every scheduled burst must have RUN
	// and BOTH convergence legs must be stamped and within OBS_BOUND. A
	// burst window runs only when cfg.Bursts is set (the 2h sustained
	// default is the single-shape window; a burst-augmented window is its
	// own complementary run, per the lead's ruling).
	if r.cfg.Bursts {
		want := burstSchedule(r.cfg.Duration)
		if len(s.Bursts) != len(want) {
			fails = append(fails, fmt.Sprintf("bursts: %d of %d scheduled burst events executed (a window shorter than the 75%% mark loses its tail bursts — run the full window)", len(s.Bursts), len(want)))
		}
		for _, b := range s.Bursts {
			if b.ConvergedUp == "" {
				fails = append(fails, fmt.Sprintf("burst %s: UP leg never converged (pods never all visible in nacos)", b.Name))
				continue
			}
			if d, err := time.ParseDuration(b.UpConvergence); err == nil && d > r.cfg.obsBound() {
				fails = append(fails, fmt.Sprintf("burst %s: UP convergence %s exceeds OBS_BOUND %s", b.Name, b.UpConvergence, r.cfg.obsBound()))
			}
			if b.DeleteIssued == "" {
				fails = append(fails, fmt.Sprintf("burst %s: rollback delete never issued (window ended inside the settle wait)", b.Name))
				continue
			}
			if b.ConvergedDown == "" {
				fails = append(fails, fmt.Sprintf("burst %s: DOWN leg never converged (pods still in nacos past the bound after delete)", b.Name))
				continue
			}
			if d, err := time.ParseDuration(b.DownConvergence); err == nil && d > r.cfg.obsBound() {
				fails = append(fails, fmt.Sprintf("burst %s: DOWN convergence %s exceeds OBS_BOUND %s", b.Name, b.DownConvergence, r.cfg.obsBound()))
			}
		}
	}
	return len(fails) == 0, fails
}

// runTick executes ONE full §4.3 loop pass (source read, remote read,
// ledger, diff, verdict, record) and returns the tick record. Shared by
// the cold-attach wait and the window loop.
func runTick(t *testing.T, cfg observeConfig, driver *churnDriver, view *nacosView,
	metrics *metricsView, child *spotterChild, records *recordWriter, log *harnessLog,
	tickNo int, windowStart time.Time) tickRecord {

	tickStart := time.Now()
	record := tickRecord{
		Tick: tickNo,
		TS:   formatTime(tickStart),
	}

	// 1. SOURCE: one kubectl read; failure is OBSERR (never divergence
	// evidence on an emptied side — the §6.2 engine fix).
	pods, srcErr := driver.liveSourcePods("app-code")
	if srcErr != nil {
		record.Verdict = string(verdictObsErr)
		record.Env = envState{Class: "read-error", Detail: fmt.Sprintf("source: %v", srcErr)}
		finalizeTick(&record, tickStart, nil)
		writeTickRecord(t, records, log, record)
		return record
	}
	model := buildSourceModel(pods, driver.appCodes)
	record.Source = sideCount{
		Count:     model.Count,
		Online:    lenIDs(model.Online),
		Unhealthy: lenIDs(model.Unhealthy),
		Pending:   countPending(pods, driver.appCodes),
		Services:  countServices(model),
	}

	// 2. REMOTE: per service, the list ∪ catalog union; any read failure
	// is OBSERR.
	remoteAll := map[string][]remoteEntry{}
	var remoteErrs []string
	leaderless := false
	for _, appCode := range driver.appCodes {
		serviceView, err := view.fullServiceView(serviceNameOf(appCode))
		if err != nil {
			remoteErrs = append(remoteErrs, err.Error())
			if isLeaderlessErr(err) {
				leaderless = true
			}
			continue
		}
		remoteAll[appCode] = serviceView["k8s"]
	}
	if len(remoteErrs) > 0 {
		record.Verdict = string(verdictObsErr)
		class := "read-error"
		if leaderless {
			class = "leaderless"
		}
		record.Env = envState{Class: class, Detail: strings.Join(remoteErrs, "; ")}
		// The write probe sharpens the leaderless classification (reads
		// can keep serving while writes 500 — only the probe sees it).
		if probeErr := view.writeProbe(); probeErr != nil {
			if isLeaderlessErr(probeErr) {
				record.Env.Class = "leaderless"
				record.Env.Detail += "; write probe: " + probeErr.Error()
			}
		}
		finalizeTick(&record, tickStart, nil)
		writeTickRecord(t, records, log, record)
		return record
	}
	remoteCount, remoteOnline, remoteDisabled, remoteServices := 0, 0, 0, 0
	for _, entries := range remoteAll {
		if len(entries) > 0 {
			remoteServices++
		}
		seen := map[string]bool{}
		for _, entry := range entries {
			if !seen[entry.ID] {
				seen[entry.ID] = true
				remoteCount++
				if entry.Enabled {
					remoteOnline++
				} else {
					remoteDisabled++
				}
			}
		}
	}
	record.Remote = sideCount{
		Count:     remoteCount,
		Online:    remoteOnline,
		Unhealthy: remoteDisabled,
		Services:  remoteServices,
	}

	// 3-4. LEDGER + DIFF per service: the bidirectional diff with the
	// in-flight tolerance (OBS_BOUND).
	var divergences []divergence
	inFlight := 0
	for _, appCode := range driver.appCodes {
		diff := compareService(appCode, model, remoteAll[appCode],
			driver.ledgerLookup, cfg.obsBound(), tickStart)
		divergences = append(divergences, diff.Divergences...)
		inFlight += diff.InFlightCount
	}
	record.InFlight = inFlight

	// 5. VERDICT.
	diffResult := diffResult{Divergences: divergences, InFlightCount: inFlight}
	record.Verdict = string(tickVerdict(diffResult))
	record.Divergence = divergences

	// 6. The queue state rides the record (metrics scrape failure leaves
	// the queue unobserved but never fails the tick — the drain criterion
	// handles un-observability at the end).
	if m, err := metrics.observe(); err == nil {
		record.Queue = queueState{
			RetryNacos: m.RetryDepths["nacos"],
			RobotDepth: m.QueueDepth,
			Dropped:    m.DroppedTotal,
		}
	}
	record.Env = envState{Class: "healthy"}

	finalizeTick(&record, tickStart, divergences)
	writeTickRecord(t, records, log, record)
	return record
}

// finalizeTick stamps the elapsed time and ages.
func finalizeTick(record *tickRecord, tickStart time.Time, divergences []divergence) {
	record.TickMS = time.Since(tickStart).Milliseconds()
	record.ElapsedMS = tickStart.UnixNano() / int64(time.Millisecond)
}

// writeTickRecord persists one tick record (JSONL + the one-line console
// log; the log line carries the invariant that held — verdict + counts).
func writeTickRecord(t *testing.T, records *recordWriter, log *harnessLog, record tickRecord) {
	if err := records.writeTick(record); err != nil {
		t.Errorf("write tick record: %v", err)
	}
	log.event("tick %d %s src=%d(on %d/unh %d/pend %d/svc %d) rmt=%d(on %d/dis %d/svc %d) inflight=%d div=%d queue=[retry %d robot %d dropped %.0f] %dms",
		record.Tick, record.Verdict,
		record.Source.Count, record.Source.Online, record.Source.Unhealthy, record.Source.Pending, record.Source.Services,
		record.Remote.Count, record.Remote.Online, record.Remote.Unhealthy, record.Remote.Services,
		record.InFlight, len(record.Divergence), record.Queue.RetryNacos, record.Queue.RobotDepth, record.Queue.Dropped,
		record.TickMS)
}

// lenIDs sums the id counts across a map's slices.
func lenIDs(m map[string][]string) int {
	n := 0
	for _, ids := range m {
		n += len(ids)
	}
	return n
}

// countPending counts the filtered Pending pods of the observed app-codes.
func countPending(pods []sourcePod, appCodes []string) int {
	n := 0
	for _, pod := range pods {
		if !isObservedAppCode(pod.AppCode, appCodes) {
			continue
		}
		if pod.Phase != "Running" {
			n++
		}
	}
	return n
}

// countServices counts app-codes with at least one modeled instance.
func countServices(model *sourceModel) int {
	n := 0
	for code := range model.Online {
		if len(model.Online[code]) > 0 {
			n++
			continue
		}
	}
	for code := range model.Unhealthy {
		if len(model.Unhealthy[code]) > 0 && len(model.Online[code]) == 0 {
			n++
		}
	}
	return n
}

// The env-overlap flagging of divergent ticks happens in recordTick; the
// JSON marshaling helper keeps record.go free of the testing dependency.
func marshalTick(record tickRecord) string {
	raw, _ := json.Marshal(record)
	return string(raw)
}
