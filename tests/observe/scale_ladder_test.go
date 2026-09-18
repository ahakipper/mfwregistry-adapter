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
	"strconv"
	"strings"
	"testing"
	"time"

	domaininstance "spotter/internal/domain/instance"
	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/etcdmock"
)

// ladderSample is one measured source mutation-to-exact-Nacos convergence
// latency. The sample is strict: every poll uses compareService, which checks
// the complete projected Instance including labels, metadata, and Reversion.
type ladderSample struct {
	Scale         int     `json:"scale"`
	Operation     string  `json:"operation"`
	LatencySec    float64 `json:"strictBatchConvergenceSec"`
	Polls         int     `json:"consistencyPolls"`
	MismatchPolls int     `json:"mismatchPolls"`
}

// ladderInstanceSample is one revision/status/canonical-correlated transition.
// Percentiles are computed from these per-instance observations, not from the
// three high-scale batches where nearest-rank P90/P95/P99 all collapse to max.
type ladderInstanceSample struct {
	Scale                  int     `json:"scale"`
	Operation              string  `json:"operation"`
	InstanceID             string  `json:"instanceId"`
	Reversion              int64   `json:"reversion"`
	SpotterOperate         string  `json:"spotterOperate"`
	SpotterOrigin          string  `json:"spotterOrigin"`
	CorrelationMode        string  `json:"correlationMode"`
	APIToK8sWatchSec       float64 `json:"apiToK8sWatchSec"`
	APIToSpotterEventSec   float64 `json:"apiToSpotterEventSec"`
	SpotterQueueSec        float64 `json:"spotterProviderTriggerToWorkerSec"`
	SpotterToNacosWatchSec float64 `json:"spotterToNacosWatchSec"`
	ExternalK8sToNacosSec  float64 `json:"externalK8sWatchToNacosSec"`
	APIToNacosWatchSec     float64 `json:"apiToNacosWatchSec"`
	SourceWatchObserved    bool    `json:"sourceWatchObserved"`
	SpotterEventObserved   bool    `json:"spotterEventObserved"`
	NacosSubscribeObserved bool    `json:"nacosSubscribeObserved"`
}

type ladderAggregate struct {
	Scale                  int     `json:"scale"`
	Operation              string  `json:"operation"`
	Samples                int     `json:"samples"`
	P90                    float64 `json:"apiToNacosP90"`
	P95                    float64 `json:"apiToNacosP95"`
	P99                    float64 `json:"apiToNacosP99"`
	Max                    float64 `json:"apiToNacosMax"`
	BatchSamples           int     `json:"batchSamples"`
	BatchExactMin          float64 `json:"batchExactMin"`
	BatchExactMedian       float64 `json:"batchExactMedian"`
	BatchExactMax          float64 `json:"batchExactMax"`
	SourceToNacosP90       float64 `json:"sourceToNacosP90"`
	SourceToNacosP95       float64 `json:"sourceToNacosP95"`
	SourceToNacosP99       float64 `json:"sourceToNacosP99"`
	APIToK8sWatchP90       float64 `json:"apiToK8sWatchP90"`
	APIToK8sWatchP95       float64 `json:"apiToK8sWatchP95"`
	APIToK8sWatchP99       float64 `json:"apiToK8sWatchP99"`
	APIToSpotterEventP90   float64 `json:"apiToSpotterEventP90"`
	APIToSpotterEventP95   float64 `json:"apiToSpotterEventP95"`
	APIToSpotterEventP99   float64 `json:"apiToSpotterEventP99"`
	SpotterQueueP90        float64 `json:"spotterProviderTriggerToWorkerP90"`
	SpotterQueueP95        float64 `json:"spotterProviderTriggerToWorkerP95"`
	SpotterQueueP99        float64 `json:"spotterProviderTriggerToWorkerP99"`
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
	SpotterOperate         string  `json:"spotterOperate"`
	SpotterOrigin          string  `json:"spotterOrigin"`
	LatencySec             float64 `json:"latencySec"`
	APIToK8sWatchSec       float64 `json:"apiToK8sWatchSec"`
	APIToSpotterEventSec   float64 `json:"apiToSpotterEventSec"`
	SpotterQueueSec        float64 `json:"spotterProviderTriggerToWorkerSec"`
	SpotterToNacosWatchSec float64 `json:"spotterToNacosWatchSec"`
	APIToNacosWatchSec     float64 `json:"apiToNacosWatchSec"`
	Polls                  int     `json:"consistencyPolls"`
	Mismatches             int     `json:"mismatchPolls"`
}

type scaleLadderReport struct {
	Stamp           string                 `json:"stamp"`
	Target          string                 `json:"target"`
	Scales          []int                  `json:"scales"`
	Aggregates      []ladderAggregate      `json:"aggregates"`
	Samples         []ladderSample         `json:"samples"`
	InstanceSamples []ladderInstanceSample `json:"instanceSamples"`
	Crash           []crashTransition      `json:"crashTransitions"`
	WatchErrors     []string               `json:"watchErrors,omitempty"`
	Pass            bool                   `json:"pass"`
	Failures        []string               `json:"failures,omitempty"`
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
	SpotterQueue        time.Duration
	SpotterToNacosWatch time.Duration
	APIToNacosWatch     time.Duration
	InstanceBoundaries  map[string]exactWatchBoundary
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
	stepTimeout := 2 * time.Minute
	if os.Getenv("OBS_LADDER_DIAGNOSTIC") == "1" {
		stepTimeout = 15 * time.Second
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
	report := scaleLadderReport{Stamp: stamp, Target: "Nacos 3 ARM64 + KWOK", Scales: scales}
	for _, scale := range report.Scales {
		repetitions := ladderRepetitions(scale)
		for i := 0; i < repetitions; i++ {
			issued := time.Now()
			names, err := driver.applyBatch(scale, 100, issued)
			if err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("create scale %d sample %d: %v", scale, i+1, err))
				continue
			}
			wait, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, issued, true, stepTimeout)
			if err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("create scale %d sample %d: %v", scale, i+1, err))
			} else {
				report.Samples = append(report.Samples, ladderSampleFromWait(scale, "create", wait))
				report.InstanceSamples = append(report.InstanceSamples, ladderInstanceSamplesFromWait(scale, "create", issued, wait)...)
			}
			deletedAt := time.Now()
			if err := driver.deletePods(names, deletedAt); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("delete scale %d sample %d: %v", scale, i+1, err))
				continue
			}
			wait, err = waitLadderExact(t, driver, view, child, timeline, appCode, names, deletedAt, false, stepTimeout)
			if err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("delete scale %d sample %d: %v", scale, i+1, err))
			} else {
				report.Samples = append(report.Samples, ladderSampleFromWait(scale, "delete", wait))
				report.InstanceSamples = append(report.InstanceSamples, ladderInstanceSamplesFromWait(scale, "delete", deletedAt, wait)...)
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
		if _, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, issued, true, stepTimeout); err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("crash setup convergence: %v", err))
		} else {
			crashAt := time.Now()
			if err := driver.patchCrash(names[0], crashAt); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("CrashLoopBackOff patch: %v", err))
			} else if wait, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, crashAt, true, stepTimeout); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("crash convergence: %v", err))
			} else {
				report.Crash = append(report.Crash, crashTransitionFromWait("crash", wait))
			}
			recoverAt := time.Now()
			if err := driver.patchRecovered(names[0], recoverAt); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("crash recovery patch: %v", err))
			} else if wait, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, recoverAt, true, stepTimeout); err != nil {
				report.Failures = append(report.Failures, fmt.Sprintf("crash recovery convergence: %v", err))
			} else {
				report.Crash = append(report.Crash, crashTransitionFromWait("recover", wait))
			}
		}
		cleanupAt := time.Now()
		if err := driver.deletePods(names, cleanupAt); err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("crash cleanup delete: %v", err))
		} else if _, err := waitLadderExact(t, driver, view, child, timeline, appCode, names, cleanupAt, false, stepTimeout); err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("crash cleanup convergence: %v", err))
		}
	}

	report.Aggregates = aggregateLadder(report.Samples, report.InstanceSamples)
	report.WatchErrors = timeline.errorsSnapshot()
	report.WatchErrors = append(report.WatchErrors, timeline.healthErrors()...)
	for _, sample := range report.InstanceSamples {
		if !sample.SourceWatchObserved || !sample.SpotterEventObserved || !sample.NacosSubscribeObserved || sample.CorrelationMode == "" {
			report.Failures = append(report.Failures, fmt.Sprintf("exact watch coverage missing for %s scale %d instance %s", sample.Operation, sample.Scale, sample.InstanceID))
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
	if os.Getenv("OBS_LADDER_DIAGNOSTIC") == "1" {
		return 1
	}
	switch scale {
	case 1:
		return 100
	case 10:
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
		sourceFingerprint := sourceMutationFingerprint(pods, []string{appCode})
		now := time.Now()
		if result.SourceSeen.IsZero() && ladderSourceReady(model, appCode, names, present) {
			result.SourceSeen = now
		}
		spotterOK := true
		var spotterBefore spotterSnapshot
		if child != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			snapshot, snapshotErr := child.debugSnapshot(ctx)
			cancel()
			if snapshotErr != nil {
				result.Mismatches++
				time.Sleep(time.Second)
				continue
			}
			spotterBefore = snapshot
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
		remoteSeenAt := time.Now()

		// Stable-cut validation: the remote read is accepted only when neither
		// the K8s projection nor Spotter's internal cache changed across it.
		// Otherwise the three sequential reads represent different logical
		// states and must be retried, never labeled exact equality.
		podsAfter, err := driver.liveSourcePods("app-code=" + appCode)
		if err != nil || sourceMutationFingerprint(podsAfter, []string{appCode}) != sourceFingerprint {
			lastDetail = "unstable K8s projection across Nacos read"
			result.Mismatches++
			time.Sleep(100 * time.Millisecond)
			continue
		}
		model = buildSourceModel(podsAfter, []string{appCode})
		if child != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			spotterAfter, snapshotErr := child.debugSnapshot(ctx)
			cancel()
			if snapshotErr != nil || spotterAfter.CacheGeneration != spotterBefore.CacheGeneration ||
				spotterAfter.LatestEventSequence != spotterBefore.LatestEventSequence ||
				spotterProjectionFingerprint(spotterAfter.Instances, []string{appCode}) != spotterProjectionFingerprint(spotterBefore.Instances, []string{appCode}) {
				lastDetail = "unstable Spotter cache across Nacos read"
				result.Mismatches++
				time.Sleep(100 * time.Millisecond)
				continue
			}
			spotterOK = spotterProjectionMatches(podsAfter, []string{appCode}, spotterAfter.Instances, spotterAfter.CanonicalPayload)
		}
		diff := compareService(appCode, model, remote["k8s"], driver.ledgerLookup, time.Minute, remoteSeenAt)
		boundaryOK := timeline == nil
		boundaries := map[string]exactWatchBoundary{}
		if timeline != nil {
			boundaries, boundaryOK = ladderExactBoundaries(timeline, model, appCode, names, present, issuedAt)
		}
		if len(diff.Divergences) == 0 && diff.InFlightCount == 0 && spotterOK && ladderIDsPresent(model, remote["k8s"], appCode, names, present) && boundaryOK {
			result.Latency = time.Since(issuedAt)
			if timeline != nil {
				result.InstanceBoundaries = boundaries
				summarizeLadderBoundaries(&result, boundaries, issuedAt)
			}
			return result, nil
		}
		lastDetail = ladderMismatchDetail(model, remote["k8s"], appCode, names, spotterOK, diff)
		result.Mismatches++
		time.Sleep(time.Second)
	}
	return result, fmt.Errorf("strict equality did not converge within %s (polls=%d mismatches=%d): %s", timeout, result.Polls, result.Mismatches, lastDetail)
}

func ladderExactBoundaries(timeline *watchTimeline, model *sourceModel, appCode string, names []string, present bool, issuedAt time.Time) (map[string]exactWatchBoundary, bool) {
	boundaries := make(map[string]exactWatchBoundary, len(names))
	for _, name := range names {
		status := int32(3)
		canonical := ""
		if present {
			expected, ok := model.Entries[appCode][name]
			if !ok {
				return nil, false
			}
			parsed, err := strconv.ParseInt(expected.Status, 10, 32)
			if err != nil {
				return nil, false
			}
			status = int32(parsed)
			decoded, err := domaininstance.DecodeCompressedCanonicalPayload(expected.Metadata["spotter.instance"])
			if err != nil || decoded == nil {
				return nil, false
			}
			canonical = domaininstance.CanonicalPayload(decoded)
		}
		boundary, ok := timeline.exactBoundary(name, present, status, canonical, issuedAt)
		if !ok {
			return nil, false
		}
		boundaries[name] = boundary
	}
	return boundaries, true
}

func summarizeLadderBoundaries(result *ladderWaitResult, boundaries map[string]exactWatchBoundary, issuedAt time.Time) {
	for _, boundary := range boundaries {
		if boundary.SourceSeen.After(result.SourceWatchSeen) {
			result.SourceWatchSeen = boundary.SourceSeen
		}
		if boundary.SpotterSeen.After(result.SpotterSeen) {
			result.SpotterSeen = boundary.SpotterSeen
		}
		if boundary.NacosSeen.After(result.NacosWatchSeen) {
			result.NacosWatchSeen = boundary.NacosSeen
		}
		result.APIToK8sWatch = maxDuration(result.APIToK8sWatch, boundary.SourceSeen.Sub(issuedAt))
		result.APIToSpotterEvent = maxDuration(result.APIToSpotterEvent, boundary.SpotterSeen.Sub(issuedAt))
		result.SpotterQueue = maxDuration(result.SpotterQueue, boundary.SpotterSeen.Sub(boundary.SpotterTrigger))
		result.SpotterToNacosWatch = maxDuration(result.SpotterToNacosWatch, boundary.NacosSeen.Sub(boundary.SpotterSeen))
		result.SourceToNacos = maxDuration(result.SourceToNacos, boundary.NacosSeen.Sub(boundary.SourceSeen))
		result.APIToNacosWatch = maxDuration(result.APIToNacosWatch, boundary.NacosSeen.Sub(issuedAt))
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if b > a {
		return b
	}
	return a
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
		Polls: wait.Polls, MismatchPolls: wait.Mismatches,
	}
}

func ladderInstanceSamplesFromWait(scale int, operation string, issuedAt time.Time, wait ladderWaitResult) []ladderInstanceSample {
	names := make([]string, 0, len(wait.InstanceBoundaries))
	for name := range wait.InstanceBoundaries {
		names = append(names, name)
	}
	sort.Strings(names)
	samples := make([]ladderInstanceSample, 0, len(names))
	for _, name := range names {
		boundary := wait.InstanceBoundaries[name]
		spotterQueue := time.Duration(0)
		if !boundary.SpotterTrigger.IsZero() {
			spotterQueue = boundary.SpotterSeen.Sub(boundary.SpotterTrigger)
		}
		samples = append(samples, ladderInstanceSample{
			Scale: scale, Operation: operation, InstanceID: name, Reversion: boundary.Reversion,
			SpotterOperate: boundary.Operation, SpotterOrigin: boundary.Origin,
			APIToK8sWatchSec: boundary.SourceSeen.Sub(issuedAt).Seconds(), APIToSpotterEventSec: boundary.SpotterSeen.Sub(issuedAt).Seconds(),
			SpotterQueueSec: spotterQueue.Seconds(), SpotterToNacosWatchSec: boundary.NacosSeen.Sub(boundary.SpotterSeen).Seconds(),
			ExternalK8sToNacosSec: boundary.NacosSeen.Sub(boundary.SourceSeen).Seconds(), APIToNacosWatchSec: boundary.NacosSeen.Sub(issuedAt).Seconds(),
			SourceWatchObserved: !boundary.SourceSeen.IsZero(), SpotterEventObserved: !boundary.SpotterSeen.IsZero(),
			NacosSubscribeObserved: !boundary.NacosSeen.IsZero(), CorrelationMode: ladderCorrelationMode(operation),
		})
	}
	return samples
}

func ladderCorrelationMode(operation string) string {
	return watchCorrelationMode(operation)
}

func crashTransitionFromWait(operation string, wait ladderWaitResult) crashTransition {
	return crashTransition{
		Operation: operation, LatencySec: wait.Latency.Seconds(),
		SpotterOperate: firstBoundaryOperation(wait.InstanceBoundaries), SpotterOrigin: firstBoundaryOrigin(wait.InstanceBoundaries),
		APIToK8sWatchSec: wait.APIToK8sWatch.Seconds(), APIToSpotterEventSec: wait.APIToSpotterEvent.Seconds(),
		SpotterQueueSec:        wait.SpotterQueue.Seconds(),
		SpotterToNacosWatchSec: wait.SpotterToNacosWatch.Seconds(), APIToNacosWatchSec: wait.APIToNacosWatch.Seconds(),
		Polls: wait.Polls, Mismatches: wait.Mismatches,
	}
}

func firstBoundaryOperation(boundaries map[string]exactWatchBoundary) string {
	for _, boundary := range boundaries {
		return boundary.Operation
	}
	return ""
}

func firstBoundaryOrigin(boundaries map[string]exactWatchBoundary) string {
	for _, boundary := range boundaries {
		return boundary.Origin
	}
	return ""
}

func aggregateLadder(samples []ladderSample, instanceSamples []ladderInstanceSample) []ladderAggregate {
	batchGroups := map[string][]ladderSample{}
	for _, sample := range samples {
		key := fmt.Sprintf("%d/%s", sample.Scale, sample.Operation)
		batchGroups[key] = append(batchGroups[key], sample)
	}
	instanceGroups := map[string][]ladderInstanceSample{}
	for _, sample := range instanceSamples {
		key := fmt.Sprintf("%d/%s", sample.Scale, sample.Operation)
		instanceGroups[key] = append(instanceGroups[key], sample)
	}
	keys := make([]string, 0, len(batchGroups))
	for key := range batchGroups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]ladderAggregate, 0, len(keys))
	for _, key := range keys {
		var scale int
		var op string
		_, _ = fmt.Sscanf(key, "%d/%s", &scale, &op)
		batchGroup := batchGroups[key]
		instanceGroup := instanceGroups[key]
		batchValues := make([]float64, len(batchGroup))
		sourceValues := make([]float64, len(instanceGroup))
		apiK8sValues := make([]float64, len(instanceGroup))
		apiSpotterValues := make([]float64, len(instanceGroup))
		spotterQueueValues := make([]float64, len(instanceGroup))
		spotterNacosValues := make([]float64, len(instanceGroup))
		apiNacosValues := make([]float64, len(instanceGroup))
		for i, sample := range batchGroup {
			batchValues[i] = sample.LatencySec
		}
		for i, sample := range instanceGroup {
			apiK8sValues[i] = sample.APIToK8sWatchSec
			apiSpotterValues[i] = sample.APIToSpotterEventSec
			spotterQueueValues[i] = sample.SpotterQueueSec
			spotterNacosValues[i] = sample.SpotterToNacosWatchSec
			sourceValues[i] = sample.ExternalK8sToNacosSec
			apiNacosValues[i] = sample.APIToNacosWatchSec
		}
		sort.Float64s(batchValues)
		sort.Float64s(sourceValues)
		sort.Float64s(apiK8sValues)
		sort.Float64s(apiSpotterValues)
		sort.Float64s(spotterQueueValues)
		sort.Float64s(spotterNacosValues)
		sort.Float64s(apiNacosValues)
		polls, mismatches := 0, 0
		for _, sample := range batchGroup {
			polls += sample.Polls
			mismatches += sample.MismatchPolls
		}
		out = append(out, ladderAggregate{
			Scale: scale, Operation: op, Samples: len(apiNacosValues), P90: ladderQuantile(apiNacosValues, 0.90), P95: ladderQuantile(apiNacosValues, 0.95), P99: ladderQuantile(apiNacosValues, 0.99), Max: ladderMax(apiNacosValues),
			BatchSamples: len(batchValues), BatchExactMin: ladderMin(batchValues), BatchExactMedian: ladderMedian(batchValues), BatchExactMax: ladderMax(batchValues),
			SourceToNacosP90: ladderQuantile(sourceValues, 0.90), SourceToNacosP95: ladderQuantile(sourceValues, 0.95), SourceToNacosP99: ladderQuantile(sourceValues, 0.99),
			APIToK8sWatchP90: ladderQuantile(apiK8sValues, 0.90), APIToK8sWatchP95: ladderQuantile(apiK8sValues, 0.95), APIToK8sWatchP99: ladderQuantile(apiK8sValues, 0.99),
			APIToSpotterEventP90: ladderQuantile(apiSpotterValues, 0.90), APIToSpotterEventP95: ladderQuantile(apiSpotterValues, 0.95), APIToSpotterEventP99: ladderQuantile(apiSpotterValues, 0.99),
			SpotterQueueP90: ladderQuantile(spotterQueueValues, 0.90), SpotterQueueP95: ladderQuantile(spotterQueueValues, 0.95), SpotterQueueP99: ladderQuantile(spotterQueueValues, 0.99),
			SpotterToNacosWatchP90: ladderQuantile(spotterNacosValues, 0.90), SpotterToNacosWatchP95: ladderQuantile(spotterNacosValues, 0.95), SpotterToNacosWatchP99: ladderQuantile(spotterNacosValues, 0.99),
			APIToNacosWatchP90: ladderQuantile(apiNacosValues, 0.90), APIToNacosWatchP95: ladderQuantile(apiNacosValues, 0.95), APIToNacosWatchP99: ladderQuantile(apiNacosValues, 0.99),
			ConsistencyPolls: polls, MismatchPolls: mismatches,
		})
	}
	return out
}

func ladderMin(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return values[0]
}

func ladderMax(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}

func ladderMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	middle := len(values) / 2
	if len(values)%2 == 1 {
		return values[middle]
	}
	return (values[middle-1] + values[middle]) / 2
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
	fmt.Fprintf(&b, "# KWOK scale ladder — %s\n\n", report.Target)
	b.WriteString("Create/Crash/Recovery samples use exact Reversion + Status + canonical-payload correlation across K8s Watch, Spotter provider-output/pre-worker, and Nacos Subscribe. Delete uses UID/SourceKey + offline + service-snapshot removal. Every batch also requires a stable-cut full-Instance snapshot equality check.\n\n")
	b.WriteString("| Scale | Operation | Instance samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | External K8s Watch→Nacos P90 (s) | P95 (s) | P99 (s) | Mismatch polls |\n|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, aggregate := range report.Aggregates {
		fmt.Fprintf(&b, "| %d | %s | %d | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %d |\n", aggregate.Scale, aggregate.Operation, aggregate.Samples, aggregate.P90, aggregate.P95, aggregate.P99, aggregate.SourceToNacosP90, aggregate.SourceToNacosP95, aggregate.SourceToNacosP99, aggregate.MismatchPolls)
	}
	b.WriteString("\n## Strict batch convergence\n\nHigh-scale batch repetitions are intentionally reported as min/median/max, not mislabeled as statistically meaningful P99.\n\n| Scale | Operation | Batches | Min (s) | Median (s) | Max (s) |\n|---:|---|---:|---:|---:|---:|\n")
	for _, aggregate := range report.Aggregates {
		fmt.Fprintf(&b, "| %d | %s | %d | %.3f | %.3f | %.3f |\n", aggregate.Scale, aggregate.Operation, aggregate.BatchSamples, aggregate.BatchExactMin, aggregate.BatchExactMedian, aggregate.BatchExactMax)
	}
	b.WriteString("\n## Continuous watch stage latency\n\n| Scale | Operation | Stage | P90 (s) | P95 (s) | P99 (s) |\n|---:|---|---|---:|---:|---:|\n")
	for _, aggregate := range report.Aggregates {
		rows := []struct {
			stage         string
			p90, p95, p99 float64
		}{
			{"API→K8s Watch", aggregate.APIToK8sWatchP90, aggregate.APIToK8sWatchP95, aggregate.APIToK8sWatchP99},
			{"API→Spotter event", aggregate.APIToSpotterEventP90, aggregate.APIToSpotterEventP95, aggregate.APIToSpotterEventP99},
			{"Spotter provider trigger→pre-worker", aggregate.SpotterQueueP90, aggregate.SpotterQueueP95, aggregate.SpotterQueueP99},
			{"Spotter event→Nacos Subscribe", aggregate.SpotterToNacosWatchP90, aggregate.SpotterToNacosWatchP95, aggregate.SpotterToNacosWatchP99},
			{"API→Nacos Subscribe", aggregate.APIToNacosWatchP90, aggregate.APIToNacosWatchP95, aggregate.APIToNacosWatchP99},
		}
		for _, row := range rows {
			fmt.Fprintf(&b, "| %d | %s | %s | %.3f | %.3f | %.3f |\n", aggregate.Scale, aggregate.Operation, row.stage, row.p90, row.p95, row.p99)
		}
	}
	b.WriteString("\n## Crash consistency\n\n| Transition | Exact latency (s) | API→K8s (s) | API→Spotter (s) | Spotter internal queue (s) | Spotter→Nacos (s) | API→Nacos (s) | Polls |\n|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, transition := range report.Crash {
		fmt.Fprintf(&b, "| %s | %.3f | %.3f | %.3f | %.3f | %.3f | %.3f | %d |\n", transition.Operation, transition.LatencySec, transition.APIToK8sWatchSec, transition.APIToSpotterEventSec, transition.SpotterQueueSec, transition.SpotterToNacosWatchSec, transition.APIToNacosWatchSec, transition.Polls)
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
