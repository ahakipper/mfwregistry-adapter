//go:build soak
// +build soak

package soak

import (
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// The edge scenarios of plan §8.5, each an asserted phase. Every scenario
// records a scenarioResult into the harness and writes its marker into the
// soak log; heal-time bounds come from the window schedule (compressed in
// smoke mode).

// scenarioPodStorm (a): delete 50% of the spike-app pods at once. Every
// removed pod's Nacos instance deregisters within the incremental bound;
// survivors untouched; no duplicates.
func (h *soakHarness) scenarioPodStorm() {
	h.log.event("scenario (a) pod restart storm begin")
	before, _ := h.k8s.livePods(spikeApp, 30*time.Second)
	if len(before) < 2 {
		// Ensure a stormable pod count first.
		if err := h.k8s.scale(spikeApp, 4); err != nil {
			h.record("a", "pod restart storm", false, fmt.Sprintf("pre-scale failed: %v", err))
			return
		}
		before, _ = h.k8s.livePods(spikeApp, 30*time.Second)
	}
	// Delete half the pods by scaling down 50%.
	target := len(before) / 2
	if err := h.k8s.scale(spikeApp, target); err != nil {
		h.record("a", "pod restart storm", false, fmt.Sprintf("scale to %d failed: %v", target, err))
		return
	}
	h.syncExpectedK8s(spikeApp)
	heal := h.waitForConverged(h.incrementalWithFullPush(), spikeApp)
	note := fmt.Sprintf("%d->%d pods, healed in %s", len(before), target, healStr(heal))
	h.record("a", "pod restart storm", heal >= 0, note)
}

// scenarioZeroHealthy (b): scale the second k8s app-code to 0 and
// deregister every entry of one consul service. The service stays listed
// with 0 instances (or absent); no stale instances past the bound.
func (h *soakHarness) scenarioZeroHealthy() {
	h.log.event("scenario (b) zero healthy instances begin")
	// k8s leg: a dedicated app-code scaled to 0.
	zeroApp := "soak-zero-app"
	if err := h.k8s.apply(zeroApp, 1); err != nil {
		h.record("b", "zero healthy instances", false, fmt.Sprintf("pre-apply failed: %v", err))
		return
	}
	h.syncExpectedK8s(zeroApp)
	if err := h.k8s.scale(zeroApp, 0); err != nil {
		h.record("b", "zero healthy instances", false, fmt.Sprintf("scale to 0 failed: %v", err))
		return
	}
	h.expected.setK8s(zeroApp, []string{})

	// consul leg: deregister every entry of one stable app-code.
	if err := h.consul.deregister(stableConsulApp1); err != nil {
		h.record("b", "zero healthy instances", false, fmt.Sprintf("consul deregister failed: %v", err))
		return
	}
	h.expected.setConsul(stableConsulApp1, []string{})

	heal := h.waitForConverged(h.incrementalWithFullPush(), zeroApp, stableConsulApp1)
	// The service with 0 instances may be listed empty or absent: both are
	// acceptable (the assertion is on instances, not the listing).
	note := fmt.Sprintf("%s scaled to 0 + %s deregistered, healed in %s", zeroApp, stableConsulApp1, healStr(heal))
	if heal < 0 {
		// Product finding (recorded, not hidden): a fully-deregistered
		// consul service leaves stale Nacos instances — the consul
		// provider's empty-catalog guard blocks the incremental delete
		// flush, and the SyncAll prune is scoped to (service, cluster)
		// pairs present in the pushed data, so an empty service is never
		// pruned. The heal only comes from a re-registration.
		note += " — stale instance past bound (consul empty-catalog guard: the incremental delete path cannot fire; documented finding for the full run)"
	}
	h.record("b", "zero healthy instances", heal >= 0, note)

	// Restore the steady state: re-register the deregistered app-code so
	// the stale instance heals (the re-registration drives the delete of
	// the old id through the normal update path) and the later scenarios
	// are not judged against a service the harness deliberately emptied.
	if err := h.consul.register(stableConsulApp1, consulID(stableConsulApp1, 1), consulPort(stableConsulApp1, consulID(stableConsulApp1, 1))); err != nil {
		h.log.event("scenario (b) restore %s failed: %v", stableConsulApp1, err)
	} else {
		ids, _ := h.consul.liveServiceIDs(stableConsulApp1)
		h.expected.setConsul(stableConsulApp1, ids)
		healRestore := h.waitForConverged(h.incrementalWithFullPush(), stableConsulApp1)
		h.log.event("scenario (b) steady state restored, healed in %s", healStr(healRestore))
	}
}

// scenarioBothProviders (c): spike-app pods AND a consul service with
// appCode spike-app. Both coexist under clusters k8s and ecs in one
// service; distinct composite ids; each provider's prune never deletes the
// other cluster's instances (§7.4).
func (h *soakHarness) scenarioBothProviders() {
	h.log.event("scenario (c) same app-code from both providers begin")
	if err := h.k8s.apply(spikeApp, 2); err != nil {
		h.record("c", "both providers one app-code", false, fmt.Sprintf("k8s apply failed: %v", err))
		return
	}
	// The provider's informer must observe the pods before the coexistence
	// can be judged: wait until the live pods exist (source-side), which is
	// the earliest the k8s converter can have pushed them.
	pods, err := h.k8s.livePods(spikeApp, h.schedule.IncrementalBound+30*time.Second)
	if err != nil || len(pods) == 0 {
		h.record("c", "both providers one app-code", false, fmt.Sprintf("no live pods: %v", err))
		return
	}
	h.expected.setK8s(spikeApp, pods)
	// The consul leg under the SAME app-code.
	if err := h.consul.register(spikeApp, consulID(spikeApp, 1001), consulPort(spikeApp, consulID(spikeApp, 1001))); err != nil {
		h.record("c", "both providers one app-code", false, fmt.Sprintf("consul register failed: %v", err))
		return
	}
	ids, _ := h.consul.liveServiceIDs(spikeApp)
	h.expected.setConsul(spikeApp, ids)

	heal := h.waitForConverged(h.incrementalWithFullPush(), spikeApp)
	// Coexistence is judged only once BOTH clusters are actually present in
	// the service: the k8s provider's push of freshly-created pods can lag
	// the source-side readiness by a poll cycle, and judging a half-pushed
	// service would report a false coexistence failure.
	bound := h.incrementalWithFullPush()
	deadline := time.Now().Add(bound)
	var view map[string][]string
	var viewErr error
	for time.Now().Before(deadline) {
		view, viewErr = retryClusterView(h.nacos, spikeApp, 60*time.Second)
		if viewErr == nil && len(view["k8s"]) > 0 && len(view["ecs"]) > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	note := ""
	pass := heal >= 0
	if viewErr != nil {
		pass = false
		note = fmt.Sprintf("cluster view error: %v", err)
	} else {
		k8sCount := len(view["k8s"])
		ecsCount := len(view["ecs"])
		both := k8sCount > 0 && ecsCount > 0
		note = fmt.Sprintf("k8s cluster %d, ecs cluster %d, healed in %s", k8sCount, ecsCount, healStr(heal))
		if !both {
			pass = false
			note += " — one cluster missing (coexistence broken)"
		} else {
			// Distinct composite ids: no id may appear in both clusters.
			seen := map[string]string{}
			for cluster, ids := range view {
				for _, id := range ids {
					if prev, ok := seen[id]; ok && prev != cluster {
						pass = false
						note += fmt.Sprintf(" — composite id %s in clusters %s and %s", id, prev, cluster)
					}
					seen[id] = cluster
				}
			}
		}
	}
	h.record("c", "both providers one app-code", pass, note)
}

// scenarioNacosRestart (d): docker restart the nacos container (restart,
// not recreate — the derby store survives). Push errors land in the
// per-sink retry queue (5s cadence) and the next --push-interval tick's
// SyncAll prune restores the full desired state; Atlas pushes continue
// during the outage.
func (h *soakHarness) scenarioNacosRestart() {
	h.log.event("scenario (d) nacos restart mid-soak begin")
	atlasBefore := h.atlasPushCount()
	if out, err := exec.Command("docker", "restart", "soak-nacos").CombinedOutput(); err != nil { //nolint:gosec // fixed container name
		h.record("d", "nacos restart mid-soak", false, fmt.Sprintf("docker restart failed: %v (%s)", err, out))
		return
	}
	// Wait for nacos readiness to come back (the JVM: allow a few minutes).
	// The console readiness endpoint answers BEFORE the naming service is
	// fully rebuilt (observed on this ARM/colima stack: instance/list
	// serves an empty view for minutes after readiness turns 200), so the
	// gate also polls the v1 ns service list until it answers 200 — the
	// heal clock starts only when the ns API is actually serving.
	readyDeadline := time.Now().Add(6 * time.Minute)
	for time.Now().Before(readyDeadline) {
		if err := h.nacos.readiness(); err == nil && h.nacos.nsAPIReady() == nil {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if err := h.nacos.readiness(); err != nil {
		h.record("d", "nacos restart mid-soak", false, fmt.Sprintf("nacos never came ready: %v", err))
		return
	}
	if err := h.nacos.nsAPIReady(); err != nil {
		h.record("d", "nacos restart mid-soak", false, fmt.Sprintf("nacos ns API never came back: %v", err))
		return
	}
	// Convergence within the full-push bound (the SyncAll prune is the
	// asserted heal path, §8.5 (d)). The bound covers one full re-armed
	// tick: the interval timer restarts from the child's perspective
	// during the outage, so the prune can legitimately land one interval
	// after nacos returns — 2x the full-push interval + the incremental
	// bound covers that worst case.
	heal := h.waitForConverged(2*h.schedule.FullPushBound+h.schedule.IncrementalBound, append(stableConsulApps, spikeApp, "soak-flap-app")...)
	atlasDelta := h.atlasPushCount() - atlasBefore
	note := fmt.Sprintf("restarted, healed in %s, atlas pushes during+after: +%d",
		healStr(heal), atlasDelta)
	pass := heal >= 0
	if heal < 0 {
		// Environment finding (recorded, not hidden — the scenario (b)
		// pattern): on this ARM/rosetta colima stack the nacos 2.1.0 JVM
		// rebuilds its naming service for minutes after `docker restart`
		// — the console readiness endpoint AND the v1 service list answer
		// well before the instance view serves the persisted derby data,
		// so the heal clock measured from "ns API serving" still watches
		// an empty view for a large part of its 2x-full-push+incremental
		// (270s) bound, and the restoration lands one-or-two 120s ticks
		// after the bound expires. The plan §8.2-documented "Nacos JVM
		// flaky on ARM/colima" risk, materializing on the restart path
		// specifically. The self-heal mechanics ARE verified: pushes fail
		// into the per-sink retry queue during the outage, the next tick's
		// SyncAll re-registers everything, Atlas keeps pushing, and the
		// final convergence check passes — this scenario alone reports the
		// exceeded bound.
		note += " — heal exceeded the bound (ARM/colima JVM rebuild: readiness and service/list return before the instance view; restoration lands past the 270s bound; retry-queue + tick self-heal verified, final convergence still passes; documented environment finding for the full run)"
	}
	if atlasDelta == 0 {
		pass = false
		note += " — atlas received nothing across the restart"
	}
	h.record("d", "nacos restart mid-soak", pass, note)
}

// scenarioConsulOutage (e): docker stop consul for 60s, then start. During
// the outage k8s-sourced services keep converging; after start
// consul-sourced state converges within the bound. The scenario restores
// the consul catalog after the start (the churn's steady state — consul
// dev mode is in-memory, so the outage legitimately empties it, and the
// post-start convergence needs the entries back to converge to).
func (h *soakHarness) scenarioConsulOutage() {
	h.log.event("scenario (e) consul outage begin")
	outage := h.schedule.ConsulOutage
	if out, err := exec.Command("docker", "stop", "soak-consul").CombinedOutput(); err != nil { //nolint:gosec // fixed container name
		h.record("e", "consul outage 60s", false, fmt.Sprintf("docker stop failed: %v (%s)", err, out))
		return
	}
	// During the outage: the k8s leg must keep converging.
	h.syncExpectedK8s(spikeApp)
	midHeal := h.waitForConverged(h.schedule.IncrementalBound+outage, spikeApp)
	time.Sleep(time.Until(time.Now().Add(outage)))
	if out, err := exec.Command("docker", "start", "soak-consul").CombinedOutput(); err != nil { //nolint:gosec // fixed container name
		h.record("e", "consul outage 60s", false, fmt.Sprintf("docker start failed: %v (%s)", err, out))
		return
	}
	// consul healthy again; restore the churn's steady state and refresh
	// the expected sets from the live catalog.
	leaderDeadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(leaderDeadline) {
		if err := h.consul.healthy(); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	for _, code := range stableConsulApps {
		if err := h.consul.register(code, consulID(code, 1), consulPort(code, consulID(code, 1))); err != nil {
			h.log.event("scenario (e) restore consul %s failed: %v", code, err)
		}
	}
	for _, code := range stableConsulApps {
		ids, err := h.consul.liveServiceIDs(code)
		if err == nil {
			h.expected.setConsul(code, ids)
		}
	}
	heal := h.waitForConverged(h.schedule.IncrementalBound+h.schedule.FullPushBound, stableConsulApps...)
	note := fmt.Sprintf("outage %s; k8s leg mid-outage healed in %s; post-start healed in %s",
		outage, healStr(midHeal), healStr(heal))
	h.record("e", "consul outage 60s", heal >= 0 && midHeal >= 0, note)
}

// scenarioSpotterRestart (f): kill the binary; the etcd lease expires; the
// harness restarts it. Re-election is observed on the embedded etcd
// campaign; the startup CompareAndFlush heals the downtime gap.
func (h *soakHarness) scenarioSpotterRestart() {
	h.log.event("scenario (f) spotter restart begin")
	before := h.atlasPushCount()
	h.child.kill()
	time.Sleep(10 * time.Second) // let the etcd lease expire (TTL clamps to ~2s; 10s is safe)
	if err := h.child.start(newContext()); err != nil {
		h.record("f", "spotter restart re-election", false, fmt.Sprintf("restart failed: %v", err))
		return
	}
	if err := h.child.waitForHealthy(60 * time.Second); err != nil {
		h.record("f", "spotter restart re-election", false, fmt.Sprintf("post-restart health: %v", err))
		return
	}
	if err := h.child.waitForLog("leader", 120*time.Second); err != nil {
		h.record("f", "spotter restart re-election", false, fmt.Sprintf("re-election not observed: %v", err))
		return
	}
	h.restarts++
	// Refresh the scoped expected sets from the live sources before
	// waiting: scenario (e)'s consul outage may have legitimately changed
	// the consul catalog (entries deleted while the model still expects
	// them), and the restart's heal must be judged against the CURRENT
	// desired state, not a stale model snapshot.
	refreshScope := append([]string(nil), stableConsulApps...)
	refreshScope = append(refreshScope, spikeApp)
	h.refreshExpected(refreshScope)
	// The startup CompareAndFlush (k8s.go / consul.go) heals the downtime
	// gap: its evidence is Atlas push traffic after the re-election. Wait
	// for the stand-in's call count to grow — the informer sync and the
	// startup flush take their time — before judging.
	flushDeadline := time.Now().Add(h.schedule.IncrementalBound + h.schedule.FullPushBound)
	for time.Now().Before(flushDeadline) {
		if h.atlasPushCount() > before {
			break
		}
		time.Sleep(2 * time.Second)
	}
	// The startup CompareAndFlush also heals any downtime gap in Nacos.
	heal := h.waitForConverged(h.schedule.IncrementalBound+h.schedule.FullPushBound, append(stableConsulApps, spikeApp)...)
	atlasDelta := h.atlasPushCount() - before
	note := fmt.Sprintf("restart %d, re-elected, healed in %s, atlas pushes +%d",
		h.child.startsCount(), healStr(heal), atlasDelta)
	pass := heal >= 0
	if atlasDelta == 0 {
		pass = false
		note += " — atlas received nothing after the restart (startup CompareAndFlush never reached the wire)"
	}
	h.record("f", "spotter restart re-election", pass, note)
}

// scenarioRapidFlap (g): register/deregister the same consul service 20x
// back-to-back. Final state matches the last op; keep-highest-Reversion
// retry never pushes the stale version; no duplicates.
func (h *soakHarness) scenarioRapidFlap() {
	h.log.event("scenario (g) rapid flap begin")
	code := "soak-flap-app"
	const flapCount = 20
	finalRegistered := true
	for i := 0; i < flapCount; i++ {
		if i%2 == 0 {
			if err := h.consul.register(code, consulID(code, i), consulPort(code, consulID(code, i))); err != nil {
				h.record("g", "rapid flap 20x", false, fmt.Sprintf("register %d failed: %v", i, err))
				return
			}
		} else {
			if err := h.consul.deregister(code); err != nil {
				h.record("g", "rapid flap 20x", false, fmt.Sprintf("deregister %d failed: %v", i, err))
				return
			}
		}
	}
	// The last op (i = flapCount-1, odd) deregistered; re-register so the
	// service is deliberately present, then assert the final state.
	if finalRegistered {
		if err := h.consul.register(code, consulID(code, flapCount), consulPort(code, consulID(code, flapCount))); err != nil {
			h.record("g", "rapid flap 20x", false, fmt.Sprintf("final register failed: %v", err))
			return
		}
	}
	ids, _ := h.consul.liveServiceIDs(code)
	h.expected.setConsul(code, ids)
	heal := h.waitForConverged(h.schedule.IncrementalBound+h.schedule.FullPushBound, code)
	view, err := retryClusterView(h.nacos, code, 60*time.Second)
	note := fmt.Sprintf("%d ops, final: %s, healed in %s", flapCount, fmt.Sprint(len(ids)), healStr(heal))
	pass := heal >= 0
	if err == nil {
		ecsIDs := view["ecs"]
		dup := duplicates(ecsIDs)
		if len(dup) > 0 {
			pass = false
			note += fmt.Sprintf(" — duplicates %v", dup)
		}
		if len(ecsIDs) != len(ids) {
			pass = false
			note += fmt.Sprintf(" — final state mismatch: nacos %v vs consul %v", ecsIDs, ids)
		}
	} else {
		pass = false
		note += fmt.Sprintf(" — view error: %v", err)
	}
	h.record("g", "rapid flap 20x", pass, note)
}

// scenarioScale100 (h): kubectl scale --replicas=N. All N instances present
// within the generous batch bound (≤300s documented); no duplicates; the
// soak log records the convergence time. N is 100 in the full run (plan
// §8.5); the smoke shakedown scales it down with its compressed window —
// a 90s window cannot honestly demonstrate a 100-replica batch.
func (h *soakHarness) scenarioScale100() {
	replicas := 100
	if h.cfg.Duration < 10*time.Minute {
		replicas = 20
	}
	h.log.event("scenario (h) scale to %d replicas begin (full run: 100)", replicas)
	scaleApp := "soak-100-app"
	if err := h.k8s.apply(scaleApp, 1); err != nil {
		h.record("h", "scale to 100 replicas", false, fmt.Sprintf("pre-apply failed: %v", err))
		return
	}
	h.syncExpectedK8s(scaleApp)
	if err := h.k8s.scale(scaleApp, replicas); err != nil {
		h.record("h", "scale to 100 replicas", false, fmt.Sprintf("scale to %d failed: %v", replicas, err))
		return
	}
	// Wait for the pods themselves to run (source-side readiness — the
	// quiescence check guards against judging a half-scaled set), then for
	// nacos to mirror them.
	pods, err := h.k8s.livePodsStable(scaleApp, h.schedule.BatchBound, 10*time.Second, replicas)
	if err != nil {
		h.record("h", "scale to 100 replicas", false, fmt.Sprintf("live pods: %v", err))
		return
	}
	h.expected.setK8s(scaleApp, pods)
	heal := h.waitForConverged(h.schedule.BatchBound, scaleApp)
	// The post-wait view read verifies the no-duplicates invariant. A
	// single transient read error (the ARM JVM's request latency spikes
	// under the 20-pod burst right after a (d) rebuild) must not fail a
	// scenario whose own heal already converged — retry the read briefly.
	view, err := retryClusterView(h.nacos, scaleApp, 60*time.Second)
	note := fmt.Sprintf("%d live pods, healed in %s (bound %s)", len(pods), healStr(heal), h.schedule.BatchBound)
	pass := heal >= 0 && len(pods) == replicas
	if err == nil {
		k8sIDs := view["k8s"]
		if len(k8sIDs) != replicas {
			pass = false
			note += fmt.Sprintf(" — nacos k8s cluster holds %d, want %d", len(k8sIDs), replicas)
		}
		if dups := duplicates(k8sIDs); len(dups) > 0 {
			pass = false
			note += fmt.Sprintf(" — duplicates %v", dups)
		}
	} else {
		pass = false
		note += fmt.Sprintf(" — view error: %v", err)
	}
	h.record("h", "scale to 100 replicas", pass, note)
}

// retryClusterView reads a service's cluster view, retrying transient nacos
// read errors (EOF/timeout spikes on the emulated JVM) up to bound.
func retryClusterView(observer *nacosObserver, service string, bound time.Duration) (map[string][]string, error) {
	deadline := time.Now().Add(bound)
	var lastErr error
	for time.Now().Before(deadline) {
		view, err := observer.clusterView(service)
		if err == nil {
			return view, nil
		}
		lastErr = err
		time.Sleep(3 * time.Second)
	}
	return nil, lastErr
}

// incrementalWithFullPush returns the generous bound: the incremental heal
// plus one full-push tick (divergence whose only heal path is the full push
// is legitimate mid-soak).
func (h *soakHarness) incrementalWithFullPush() time.Duration {
	return h.schedule.IncrementalBound + h.schedule.FullPushBound
}

// healStr renders a heal time (or "never" when negative).
func healStr(d time.Duration) string {
	if d < 0 {
		return "never"
	}
	return d.Round(time.Millisecond).String()
}

// duplicates returns ids appearing more than once.
func duplicates(ids []string) []string {
	counts := map[string]int{}
	for _, id := range ids {
		counts[id]++
	}
	dups := []string{}
	for id, count := range counts {
		if count > 1 {
			dups = append(dups, id)
		}
	}
	sort.Strings(dups)
	return dups
}

// summaryScenarioLine renders one summary table row.
func summaryScenarioLine(s scenarioResult) string {
	return fmt.Sprintf("| (%s) | %s | %s | %s |", s.Letter, s.Name, verdict(s.Pass), strings.TrimSpace(s.Note))
}
