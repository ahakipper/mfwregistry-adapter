//go:build observe
// +build observe

package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/etcdmock"
)

// ladderSample is one measured source mutation-to-exact-Nacos convergence
// latency. The sample is strict: every poll uses compareService, which checks
// the complete projected Instance including labels, metadata, and Reversion.
type ladderSample struct {
	Scale                  int     `json:"scale"`
	Operation              string  `json:"operation"`
	LatencySec             float64 `json:"latencySec"`
	SourceToNacosSec       float64 `json:"sourceToNacosSec"`
	APIToK8sWatchSec       float64 `json:"apiToK8sWatchSec"`
	APIToSpotterEventSec   float64 `json:"apiToSpotterEventSec"`
	K8sWatchToSpotterSec   float64 `json:"k8sWatchToSpotterSec"`
	SpotterToNacosWatchSec float64 `json:"spotterToNacosWatchSec"`
	APIToNacosWatchSec     float64 `json:"apiToNacosWatchSec"`
	SourceWatch            bool    `json:"sourceWatchObserved"`
	SpotterObserved        bool    `json:"spotterObserved"`
	NacosWatch             bool    `json:"nacosWatchObserved"`
	Polls                  int     `json:"consistencyPolls"`
	MismatchPolls          int     `json:"mismatchPolls"`
}

type ladderAggregate struct {
	Scale                  int     `json:"scale"`
	Operation              string  `json:"operation"`
	Samples                int     `json:"samples"`
	P90                    float64 `json:"p90"`
	P95                    float64 `json:"p95"`
	P99                    float64 `json:"p99"`
	Max                    float64 `json:"max"`
	SourceToNacosP90       float64 `json:"sourceToNacosP90"`
	SourceToNacosP95       float64 `json:"sourceToNacosP95"`
	SourceToNacosP99       float64 `json:"sourceToNacosP99"`
	APIToK8sWatchP90       float64 `json:"apiToK8sWatchP90"`
	APIToK8sWatchP95       float64 `json:"apiToK8sWatchP95"`
	APIToK8sWatchP99       float64 `json:"apiToK8sWatchP99"`
	APIToSpotterEventP90   float64 `json:"apiToSpotterEventP90"`
	APIToSpotterEventP95   float64 `json:"apiToSpotterEventP95"`
	APIToSpotterEventP99   float64 `json:"apiToSpotterEventP99"`
	K8sWatchToSpotterP90   float64 `json:"k8sWatchToSpotterP90"`
	K8sWatchToSpotterP95   float64 `json:"k8sWatchToSpotterP95"`
	K8sWatchToSpotterP99   float64 `json:"k8sWatchToSpotterP99"`
	SpotterToNacosWatchP90 float64 `json:"spotterToNacosWatchP90"`
	SpotterToNacosWatchP95 float64 `json:"spotterToNacosWatchP95"`
	SpotterToNacosWatchP99 float64 `json:"spotterToNacosWatchP99"`
	APIToNacosWatchP90     float64 `json:"apiToNacosWatchP90"`
	APIToNacosWatchP95     float64 `json:"apiToNacosWatchP95"`
	APIToNacosWatchP99     float64 `json:"apiToNacosWatchP99"`
	ConsistencyPolls       int     `json:"consistencyPolls"`
	MismatchPolls          int     `json:"mismatchPolls"`
}

type crashTransition struct {
	Operation              string  `json:"operation"`
	LatencySec             float64 `json:"latencySec"`
	APIToK8sWatchSec       float64 `json:"apiToK8sWatchSec"`
	APIToSpotterEventSec   float64 `json:"apiToSpotterEventSec"`
	K8sWatchToSpotterSec   float64 `json:"k8sWatchToSpotterSec"`
	SpotterToNacosWatchSec float64 `json:"spotterToNacosWatchSec"`
	APIToNacosWatchSec     float64 `json:"apiToNacosWatchSec"`
	Polls                  int     `json:"consistencyPolls"`
	Mismatches             int     `json:"mismatchPolls"`
}

type scaleLadderReport struct {
	Stamp       string            `json:"stamp"`
	Target      string            `json:"target"`
	Scales      []int             `json:"scales"`
	Aggregates  []ladderAggregate `json:"aggregates"`
	Samples     []ladderSample    `json:"samples"`
	Crash       []crashTransition `json:"crashTransitions"`
	WatchErrors []string          `json:"watchErrors,omitempty"`
	Pass        bool              `json:"pass"`
	Failures    []string          `json:"failures,omitempty"`
}

type ladderWaitResult struct {
	Latency             time.Duration
	SourceToNacos       time.Duration
	SourceSeen          time.Time
	SourceWatchSeen     time.Time
	SpotterSeen         time.Time
	NacosWatchSeen      time.Time
	APIToK8sWatch       time.Duration
	APIToSpotterEvent   time.Duration
	K8sWatchToSpotter   time.Duration
	SpotterToNacosWatch time.Duration
	APIToNacosWatch     time.Duration
	Polls               int
	Mismatches          int
}

// TestObserveScaleLadder measures single-Pod, 10-Pod, 100-Pod, 500-Pod and
// 1000-Pod create/delete convergence against a real Nacos 3 SDK session. It
// also drives a CrashLoopBackOff -> Ready transition. Every polling instant
// performs the same strict source/Nacos comparison used by the sustained
// observer; a result is accepted only when all projected fields match.
//
// The test is deliberately separate from the two-hour window so its report
// can attribute P90/P95/P99 to a precise mutation size and operation. The
// runner starts it with scripts/observe-up.sh and tears the owned kwok stack
// down with scripts/observe-down.sh.
func TestObserveScaleLadder(t *testing.T) {
	t.Setenv("SPOTTER_OBSERVE_DEBUG", "1")
	cfg, err := loadObserveConfig()
	if err != nil {
		t.Fatalf("observe config: %v", err)
	}
	if _, err := os.Stat(cfg.SpotterBin); err != nil {
		t.Skipf("NOT VERIFIED: EnvError spotter binary %s: %v", cfg.SpotterBin, err)
	}
	if _, err := os.Stat(cfg.Kubeconfig); err != nil {
		t.Skipf("NOT VERIFIED: EnvError kwok kubeconfig %s: %v", cfg.Kubeconfig, err)
	}

	const appCode = "ladder-app-0"
	driver := newChurnDriver(cfg.Kubeconfig, []string{appCode}, "ladder")
	if _, err := driver.kubectlStdin("", "get", "nodes"); err != nil {
		t.Skipf("NOT VERIFIED: EnvError kwok preflight: %v", err)
	}
	if err := driver.deleteAll(); err != nil {
		t.Fatalf("delete stale ladder pods: %v", err)
	}

	if err := dockerRunNacos(nacosHostPortAsInt(cfg.NacosAddr)); err != nil {
		t.Fatalf("start Nacos 3: %v", err)
	}
	t.Cleanup(func() {
		if err := dockerStopNacos(); err != nil {
			t.Errorf("Nacos teardown: %v", err)
		}
	})
	if err := nacosHealthWait(cfg.NacosAddr, 10*time.Minute); err != nil {
		t.Fatalf("Nacos health: %v", err)
	}
	if err := waitForNacosSDKReadiness(cfg.NacosAddr, 10*time.Minute); err != nil {
		t.Fatalf("Nacos SDK readiness: %v", err)
	}
	view := newNacosView(cfg.NacosAddr)
	t.Cleanup(view.close)

	etcd, err := etcdmock.Start()
	if err != nil {
		t.Fatalf("embedded etcd: %v", err)
	}
	t.Cleanup(etcd.Close)
	standin, err := discoverymock.StartTCP(fmt.Sprintf("127.0.0.1:%d", cfg.AtlasPort))
	if err != nil {
		t.Fatalf("Atlas stand-in: %v", err)
	}
	t.Cleanup(standin.Close)
	child := newSpotterChild(cfg.SpotterBin, cfg.WorkDir, cfg.Kubeconfig,
		etcd.ClientEndpoints(), standin.Addr(), cfg.NacosAddr, cfg.MetricsPort)
	t.Cleanup(func() {
		if err := child.kill(); err != nil {
			t.Errorf("spotter child teardown: %v", err)
		}
	})
	if err := child.start(context.Background()); err != nil {
		t.Fatalf("start spotter child: %v", err)
	}
	if err := child.waitForHealthy(120 * time.Second); err != nil {
		t.Fatalf("spotter child health: %v", err)
	}
	watchCtx, watchCancel := context.WithCancel(context.Background())
	t.Cleanup(watchCancel)
	k8sEvents, err := startK8sPodWatch(watchCtx, cfg.Kubeconfig, "app-code="+appCode)
	if err != nil {
		t.Fatalf("start independent K8s watch: %v", err)
	}
	nacosEvents, err := startNacosServiceWatch(watchCtx, cfg.NacosAddr, appCode)
	if err != nil {
		t.Fatalf("start independent Nacos Subscribe watch: %v", err)
	}
	spotterEvents := startSpotterEventWatch(watchCtx, cfg.MetricsPort)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents.Events())
	// Prime both independent streams before collecting percentile samples. The
	// Nacos Subscribe RPC can return before the server-side push stream has
	// delivered its first changed snapshot; starting the first measured create
	// immediately makes observer startup latency indistinguishable from product
	// propagation latency. The warm-up is fully converged and removed, but is
	// deliberately excluded from the measured sample set.
	warmIssued := time.Now()
	warmNames, err := driver.applyBatch(1, 100, warmIssued)
	if err != nil {
		t.Fatalf("watch warm-up create: %v", err)
	}
	if _, err := waitLadderExact(t, driver, view, child, nil, appCode, warmNames, warmIssued, true, 2*time.Minute); err != nil {
		t.Fatalf("watch warm-up create convergence: %v", err)
	}
	if err := waitWatchCoverage(timeline, warmNames, true, warmIssued, 30*time.Second); err != nil {
		t.Fatalf("watch warm-up create coverage: %v", err)
	}
	warmDeleted := time.Now()
	if err := driver.deletePods(warmNames, warmDeleted); err != nil {
		t.Fatalf("watch warm-up delete: %v", err)
	}
	if _, err := waitLadderExact(t, driver, view, child, nil, appCode, warmNames, warmDeleted, false, 2*time.Minute); err != nil {
		t.Fatalf("watch warm-up delete convergence: %v", err)
	}
	if err := waitWatchCoverage(timeline, warmNames, false, warmDeleted, 30*time.Second); err != nil {
		t.Fatalf("watch warm-up delete coverage: %v", err)
	}

	stamp := time.Now().Format("20060102-150405")
	scales := []int{1, 10, 100, 500, 1000}
	if os.Getenv("OBS_LADDER_DIAGNOSTIC") == "1" {
		scales = []int{1}
	}
	report := scaleLadderReport{Stamp: stamp, Target: "Nacos 3 ARM64 + KWork", Scales: scales}
	for _, scale := range report.Scales {
		repetitions := ladderRepetitions(scale)
		for i := 0; i < repetitions; i++ {
			issued := time.Now()
			names, err := driver.applyBatch(scale, 100, issued)
			if err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("create scale %d sample %d: %v", scale, i+1, err))
				continue
			}
			wait, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, issued, true, 2*time.Minute)
			if err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("create scale %d sample %d: %v", scale, i+1, err))
			} else {
				report.Samples = append(report.Samples, ladderSampleFromWait(scale, "create", wait))
			}
			deletedAt := time.Now()
			if err := driver.deletePods(names, deletedAt); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("delete scale %d sample %d: %v", scale, i+1, err))
				continue
			}
			wait, err = waitLadderExact(t, driver, view, child, timeline, appCode, names, deletedAt, false, 2*time.Minute)
			if err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("delete scale %d sample %d: %v", scale, i+1, err))
			} else {
				report.Samples = append(report.Samples, ladderSampleFromWait(scale, "delete", wait))
			}
		}
	}

	// Crash consistency: create one steady instance, transition it to
	// CrashLoopBackOff, recover it, and finally delete it. The comparison loop
	// must observe status/state/enabled and the complete metadata at every poll.
	issued := time.Now()
	names, err := driver.applyBatch(1, 100, issued)
	if err != nil {
		report.Failures = append(report.Failures, fmt.Sprintf("crash setup create: %v", err))
	} else {
		if _, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, issued, true, 2*time.Minute); err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("crash setup convergence: %v", err))
		} else {
			crashAt := time.Now()
			if err := driver.patchCrash(names[0], crashAt); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("CrashLoopBackOff patch: %v", err))
			} else if wait, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, crashAt, true, 2*time.Minute); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("crash convergence: %v", err))
			} else {
				report.Crash = append(report.Crash, crashTransitionFromWait("crash", wait))
			}
			recoverAt := time.Now()
			if err := driver.patchRecovered(names[0], recoverAt); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("crash recovery patch: %v", err))
			} else if wait, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, recoverAt, true, 2*time.Minute); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("crash recovery convergence: %v", err))
			} else {
				report.Crash = append(report.Crash, crashTransitionFromWait("recover", wait))
			}
		}
		if err := driver.deletePods(names, time.Now()); err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("crash cleanup delete: %v", err))
		} else if _, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, time.Now(), false, 2*time.Minute); err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("crash cleanup convergence: %v", err))
		}
	}

	report.Aggregates = aggregateLadder(report.Samples)
	report.WatchErrors = timeline.errorsSnapshot()
	for _, sample := range report.Samples {
		if !sample.SourceWatch || !sample.SpotterObserved || !sample.NacosWatch {
			report.Failures = append(report.Failures, fmt.Sprintf("watch coverage missing for %s scale %d (source=%v spotter=%v nacos=%v)", sample.Operation, sample.Scale, sample.SourceWatch, sample.SpotterObserved, sample.NacosWatch))
		}
	}
	if len(report.WatchErrors) > 0 {
		report.Failures = append(report.Failures, report.WatchErrors...)
	}
	report.Pass = len(report.Failures) == 0 && len(report.Crash) == 2 && len(report.Aggregates) == len(report.Scales)*2
	if err := writeScaleLadderReport(cfg.ResultsDir, report); err != nil {
		t.Fatalf("write scale ladder report: %v", err)
	}
	if !report.Pass {
		t.Fatalf("scale ladder failed: %s", strings.Join(report.Failures, "; "))
	}
}

func ladderRepetitions(scale int) int {
	switch scale {
	case 1, 10:
		return 10
	case 100:
		return 5
	default:
		return 3
	}
}

func waitLadderExact(t *testing.T, driver *churnDriver, view *nacosView, child *spotterChild, timeline *watchTimeline, appCode string, names []string, issuedAt time.Time, present bool, timeout time.Duration) (ladderWaitResult, error) {
	t.Helper()
	started := time.Now()
	result := ladderWaitResult{}
	lastDetail := "no observation"
	for time.Since(started) < timeout {
		result.Polls++
		pods, err := driver.liveSourcePods("app-code=" + appCode)
		if err != nil {
			result.Mismatches++
			time.Sleep(time.Second)
			continue
		}
		model := buildSourceModel(pods, []string{appCode})
		now := time.Now()
		if result.SourceSeen.IsZero() && ladderSourceReady(model, appCode, names, present) {
			result.SourceSeen = now
		}
		spotterOK := true
		if child != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			snapshot, snapshotErr := child.debugSnapshot(ctx)
			cancel()
			if snapshotErr != nil {
				result.Mismatches++
				time.Sleep(time.Second)
				continue
			}
			spotterOK = spotterProjectionMatches(pods, []string{appCode}, snapshot.Instances, snapshot.CanonicalPayload)
			if spotterOK && result.SpotterSeen.IsZero() && !snapshot.ObservedAt.IsZero() && !snapshot.ObservedAt.Before(issuedAt) {
				result.SpotterSeen = snapshot.ObservedAt
			}
		}
		fresh, err := view.freshSDKView()
		if err != nil {
			result.Mismatches++
			time.Sleep(time.Second)
			continue
		}
		remote, err := fresh.fullServiceView(appCode)
		fresh.close()
		if err != nil {
			result.Mismatches++
			time.Sleep(time.Second)
			continue
		}
		diff := compareService(appCode, model, remote["k8s"], driver.ledgerLookup, time.Minute, now)
		boundaryOK := timeline == nil
		var sourceWatchAt, spotterEventAt, nacosWatchAt time.Time
		if timeline != nil {
			sourceWatchAt, spotterEventAt, nacosWatchAt, boundaryOK = timeline.boundaryTimes(names, present, issuedAt)
		}
		if len(diff.Divergences) == 0 && diff.InFlightCount == 0 && spotterOK && ladderIDsPresent(model, remote["k8s"], appCode, names, present) && boundaryOK {
			result.Latency = now.Sub(issuedAt)
			if !result.SourceSeen.IsZero() {
				result.SourceToNacos = now.Sub(result.SourceSeen)
			}
			if timeline != nil {
				result.SourceWatchSeen = sourceWatchAt
				result.SpotterSeen = spotterEventAt
				result.NacosWatchSeen = nacosWatchAt
				result.APIToK8sWatch = sourceWatchAt.Sub(issuedAt)
				result.APIToSpotterEvent = spotterEventAt.Sub(issuedAt)
				result.K8sWatchToSpotter = spotterEventAt.Sub(sourceWatchAt)
				result.SpotterToNacosWatch = nacosWatchAt.Sub(spotterEventAt)
				result.APIToNacosWatch = nacosWatchAt.Sub(issuedAt)
			}
			return result, nil
		}
		lastDetail = ladderMismatchDetail(model, remote["k8s"], appCode, names, spotterOK, diff)
		result.Mismatches++
		time.Sleep(time.Second)
	}
	return result, fmt.Errorf("strict equality did not converge within %s (polls=%d mismatches=%d): %s", timeout, result.Polls, result.Mismatches, lastDetail)
}

func ladderMismatchDetail(model *sourceModel, remote []remoteEntry, appCode string, names []string, spotterOK bool, diff diffResult) string {
	parts := []string{fmt.Sprintf("spotterEqual=%v", spotterOK)}
	for _, name := range names {
		expected, sourcePresent := model.Entries[appCode][name]
		var found *remoteEntry
		for i := range remote {
			if remote[i].ID == name {
				found = &remote[i]
				break
			}
		}
		if found == nil {
			parts = append(parts, fmt.Sprintf("%s source=%v remote=false", name, sourcePresent))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s source=%v remote=true expectedStatus=%s remoteStatus=%s expectedEnabled=%v remoteEnabled=%v expectedPayload=%s remotePayload=%s", name, sourcePresent, expected.Status, found.Metadata["status"], expected.Enabled, found.Enabled, expected.Metadata["spotter.instance"], found.Metadata["spotter.instance"]))
	}
	if len(diff.Divergences) > 0 {
		parts = append(parts, fmt.Sprintf("divergences=%v", diff.Divergences))
	}
	return strings.Join(parts, "; ")
}

func allWatchReady(timeline *watchTimeline, names []string, present bool, issuedAt time.Time, source bool) bool {
	for _, name := range names {
		if source {
			if !timeline.sourceReady(name, present, issuedAt) {
				return false
			}
		} else if !timeline.nacosReady(name, present, issuedAt) {
			return false
		}
	}
	return true
}

func waitWatchCoverage(timeline *watchTimeline, names []string, present bool, issuedAt time.Time, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, _, _, ok := timeline.boundaryTimes(names, present, issuedAt); ok {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	spotterReady := true
	for _, name := range names {
		if !timeline.spotterReady(name, present, issuedAt) {
			spotterReady = false
			break
		}
	}
	return fmt.Errorf("independent watch coverage missing after %s (source=%v spotter=%v nacos=%v errors=%v)", timeout,
		allWatchReady(timeline, names, present, issuedAt, true),
		spotterReady, allWatchReady(timeline, names, present, issuedAt, false), timeline.errorsSnapshot())
}

func ladderSourceReady(model *sourceModel, appCode string, names []string, present bool) bool {
	entries := model.Entries[appCode]
	for _, name := range names {
		_, exists := entries[name]
		if exists != present {
			return false
		}
	}
	return true
}

func ladderIDsPresent(model *sourceModel, remote []remoteEntry, appCode string, names []string, present bool) bool {
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
	}
	seen := map[string]bool{}
	for _, entry := range remote {
		seen[entry.ID] = true
	}
	for name := range want {
		if seen[name] != present {
			return false
		}
	}
	if present {
		return len(model.Entries[appCode]) >= len(names)
	}
	return true
}

func ladderSampleFromWait(scale int, operation string, wait ladderWaitResult) ladderSample {
	return ladderSample{
		Scale: scale, Operation: operation, LatencySec: wait.Latency.Seconds(),
		SourceToNacosSec: wait.SourceToNacos.Seconds(),
		APIToK8sWatchSec: wait.APIToK8sWatch.Seconds(), APIToSpotterEventSec: wait.APIToSpotterEvent.Seconds(),
		K8sWatchToSpotterSec:   wait.K8sWatchToSpotter.Seconds(),
		SpotterToNacosWatchSec: wait.SpotterToNacosWatch.Seconds(), APIToNacosWatchSec: wait.APIToNacosWatch.Seconds(),
		SourceWatch: !wait.SourceWatchSeen.IsZero(), SpotterObserved: !wait.SpotterSeen.IsZero(),
		NacosWatch: !wait.NacosWatchSeen.IsZero(), Polls: wait.Polls, MismatchPolls: wait.Mismatches,
	}
}

func crashTransitionFromWait(operation string, wait ladderWaitResult) crashTransition {
	return crashTransition{
		Operation: operation, LatencySec: wait.Latency.Seconds(),
		APIToK8sWatchSec: wait.APIToK8sWatch.Seconds(), APIToSpotterEventSec: wait.APIToSpotterEvent.Seconds(),
		K8sWatchToSpotterSec:   wait.K8sWatchToSpotter.Seconds(),
		SpotterToNacosWatchSec: wait.SpotterToNacosWatch.Seconds(), APIToNacosWatchSec: wait.APIToNacosWatch.Seconds(),
		Polls: wait.Polls, Mismatches: wait.Mismatches,
	}
}

func aggregateLadder(samples []ladderSample) []ladderAggregate {
	groups := map[string][]ladderSample{}
	for _, sample := range samples {
		key := fmt.Sprintf("%d/%s", sample.Scale, sample.Operation)
		groups[key] = append(groups[key], sample)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]ladderAggregate, 0, len(keys))
	for _, key := range keys {
		var scale int
		var op string
		_, _ = fmt.Sscanf(key, "%d/%s", &scale, &op)
		group := groups[key]
		values := make([]float64, len(group))
		sourceValues := make([]float64, len(group))
		apiK8sValues := make([]float64, len(group))
		apiSpotterValues := make([]float64, len(group))
		k8sSpotterValues := make([]float64, len(group))
		spotterNacosValues := make([]float64, len(group))
		apiNacosValues := make([]float64, len(group))
		for i, sample := range group {
			values[i] = sample.LatencySec
			sourceValues[i] = sample.SourceToNacosSec
			apiK8sValues[i] = sample.APIToK8sWatchSec
			apiSpotterValues[i] = sample.APIToSpotterEventSec
			k8sSpotterValues[i] = sample.K8sWatchToSpotterSec
			spotterNacosValues[i] = sample.SpotterToNacosWatchSec
			apiNacosValues[i] = sample.APIToNacosWatchSec
		}
		sort.Float64s(values)
		sort.Float64s(sourceValues)
		sort.Float64s(apiK8sValues)
		sort.Float64s(apiSpotterValues)
		sort.Float64s(k8sSpotterValues)
		sort.Float64s(spotterNacosValues)
		sort.Float64s(apiNacosValues)
		polls, mismatches := 0, 0
		for _, sample := range group {
			polls += sample.Polls
			mismatches += sample.MismatchPolls
		}
		out = append(out, ladderAggregate{
			Scale: scale, Operation: op, Samples: len(values), P90: ladderQuantile(values, 0.90), P95: ladderQuantile(values, 0.95), P99: ladderQuantile(values, 0.99), Max: values[len(values)-1],
			SourceToNacosP90: ladderQuantile(sourceValues, 0.90), SourceToNacosP95: ladderQuantile(sourceValues, 0.95), SourceToNacosP99: ladderQuantile(sourceValues, 0.99),
			APIToK8sWatchP90: ladderQuantile(apiK8sValues, 0.90), APIToK8sWatchP95: ladderQuantile(apiK8sValues, 0.95), APIToK8sWatchP99: ladderQuantile(apiK8sValues, 0.99),
			APIToSpotterEventP90: ladderQuantile(apiSpotterValues, 0.90), APIToSpotterEventP95: ladderQuantile(apiSpotterValues, 0.95), APIToSpotterEventP99: ladderQuantile(apiSpotterValues, 0.99),
			K8sWatchToSpotterP90: ladderQuantile(k8sSpotterValues, 0.90), K8sWatchToSpotterP95: ladderQuantile(k8sSpotterValues, 0.95), K8sWatchToSpotterP99: ladderQuantile(k8sSpotterValues, 0.99),
			SpotterToNacosWatchP90: ladderQuantile(spotterNacosValues, 0.90), SpotterToNacosWatchP95: ladderQuantile(spotterNacosValues, 0.95), SpotterToNacosWatchP99: ladderQuantile(spotterNacosValues, 0.99),
			APIToNacosWatchP90: ladderQuantile(apiNacosValues, 0.90), APIToNacosWatchP95: ladderQuantile(apiNacosValues, 0.95), APIToNacosWatchP99: ladderQuantile(apiNacosValues, 0.99),
			ConsistencyPolls: polls, MismatchPolls: mismatches,
		})
	}
	return out
}

func ladderQuantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(q*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func writeScaleLadderReport(dir string, report scaleLadderReport) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	jsonPath := filepath.Join(dir, report.Stamp+"-scale-ladder-summary.json")
	mdPath := filepath.Join(dir, report.Stamp+"-scale-ladder-summary.md")
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jsonPath, data, 0o644); err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# KWork scale ladder — %s\n\n", report.Target)
	b.WriteString("Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.\n\n")
	b.WriteString("| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, aggregate := range report.Aggregates {
		fmt.Fprintf(&b, "| %d | %s | %d | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %d |\n", aggregate.Scale, aggregate.Operation, aggregate.Samples, aggregate.P90, aggregate.P95, aggregate.P99, aggregate.SourceToNacosP90, aggregate.SourceToNacosP95, aggregate.SourceToNacosP99, aggregate.MismatchPolls)
	}
	b.WriteString("\n## Continuous watch stage latency\n\n| Scale | Operation | Stage | P90 (s) | P95 (s) | P99 (s) |\n|---:|---|---|---:|---:|---:|\n")
	for _, aggregate := range report.Aggregates {
		rows := []struct {
			stage         string
			p90, p95, p99 float64
		}{
			{"API→K8s Watch", aggregate.APIToK8sWatchP90, aggregate.APIToK8sWatchP95, aggregate.APIToK8sWatchP99},
			{"API→Spotter event", aggregate.APIToSpotterEventP90, aggregate.APIToSpotterEventP95, aggregate.APIToSpotterEventP99},
			{"K8s Watch→Spotter event", aggregate.K8sWatchToSpotterP90, aggregate.K8sWatchToSpotterP95, aggregate.K8sWatchToSpotterP99},
			{"Spotter event→Nacos Subscribe", aggregate.SpotterToNacosWatchP90, aggregate.SpotterToNacosWatchP95, aggregate.SpotterToNacosWatchP99},
			{"API→Nacos Subscribe", aggregate.APIToNacosWatchP90, aggregate.APIToNacosWatchP95, aggregate.APIToNacosWatchP99},
		}
		for _, row := range rows {
			fmt.Fprintf(&b, "| %d | %s | %s | %.3f | %.3f | %.3f |\n", aggregate.Scale, aggregate.Operation, row.stage, row.p90, row.p95, row.p99)
		}
	}
	b.WriteString("\n## Crash consistency\n\n| Transition | Exact latency (s) | API→K8s (s) | API→Spotter (s) | Spotter→Nacos (s) | API→Nacos (s) | Polls |\n|---|---:|---:|---:|---:|---:|---:|\n")
	for _, transition := range report.Crash {
		fmt.Fprintf(&b, "| %s | %.3f | %.3f | %.3f | %.3f | %.3f | %d |\n", transition.Operation, transition.LatencySec, transition.APIToK8sWatchSec, transition.APIToSpotterEventSec, transition.SpotterToNacosWatchSec, transition.APIToNacosWatchSec, transition.Polls)
	}
	if len(report.WatchErrors) > 0 {
		b.WriteString("\n## Watch errors\n\n")
		for _, watchErr := range report.WatchErrors {
			fmt.Fprintf(&b, "- %s\n", watchErr)
		}
	}
	b.WriteString("\n## Verdict\n\n")
	if report.Pass {
		b.WriteString("PASS — all scale/operation samples and CrashLoopBackOff recovery transitions converged with strict equality.\n")
	} else {
		b.WriteString("FAIL\n")
		for _, failure := range report.Failures {
			fmt.Fprintf(&b, "- %s\n", failure)
		}
	}
	return os.WriteFile(mdPath, []byte(b.String()), 0o644)
}
