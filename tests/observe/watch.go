//go:build observe
// +build observe

package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	domaininstance "spotter/internal/domain/instance"
	spotternacos "spotter/pkg/nacos"
)

// k8sWatchEvent is an independently observed source event. It is deliberately
// separate from Spotter's informer queue so the harness can measure API/watch
// propagation without reusing the implementation under test.
type k8sWatchEvent struct {
	Type string
	Pod  *v1.Pod
	At   time.Time
	Err  error
}

// startK8sPodWatch starts a client-go watch against the K8s API. The caller
// owns ctx cancellation; the returned channel closes when the watch ends.
func startK8sPodWatch(ctx context.Context, kubeconfig, selector string) (<-chan k8sWatchEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build k8s watch config: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build k8s watch client: %w", err)
	}
	list, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list before k8s pod watch: %w", err)
	}
	out := make(chan k8sWatchEvent, 4096)
	go func() {
		defer close(out)
		rv := list.ResourceVersion
		emit := func(event k8sWatchEvent) bool {
			select {
			case out <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for i := range list.Items {
			if !emit(k8sWatchEvent{Type: string(watch.Added), Pod: list.Items[i].DeepCopy(), At: time.Now()}) {
				return
			}
		}
		for {
			if ctx.Err() != nil {
				return
			}
			watcher, watchErr := client.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{LabelSelector: selector, ResourceVersion: rv})
			if watchErr != nil {
				if !emit(k8sWatchEvent{Type: string(watch.Error), At: time.Now(), Err: watchErr}) {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(250 * time.Millisecond):
				}
				continue
			}
			relistNeeded := false
			for event := range watcher.ResultChan() {
				if event.Type == watch.Error {
					_ = emit(k8sWatchEvent{Type: string(event.Type), At: time.Now(), Err: fmt.Errorf("k8s watch error: %v", event.Object)})
					relistNeeded = true
					break
				}
				pod, ok := event.Object.(*v1.Pod)
				if !ok || pod == nil {
					continue
				}
				if pod.ResourceVersion != "" {
					rv = pod.ResourceVersion
				}
				if !emit(k8sWatchEvent{Type: string(event.Type), Pod: pod.DeepCopy(), At: time.Now()}) {
					watcher.Stop()
					return
				}
			}
			watcher.Stop()
			if relistNeeded && ctx.Err() == nil {
				relisted, relistErr := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: selector})
				if relistErr != nil {
					if !emit(k8sWatchEvent{Type: string(watch.Error), At: time.Now(), Err: fmt.Errorf("relist after k8s watch gap: %w", relistErr)}) {
						return
					}
				} else {
					rv = relisted.ResourceVersion
					for i := range relisted.Items {
						if !emit(k8sWatchEvent{Type: string(watch.Added), Pod: relisted.Items[i].DeepCopy(), At: time.Now()}) {
							return
						}
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	return out, nil
}

// nacosWatchEvent is one official SDK Subscribe callback. Hosts is a complete
// service snapshot, not a delta; the callback timestamp is the sink-watch
// visibility boundary.
type nacosWatchEvent struct {
	Service string
	Hosts   []spotternacos.Host
	At      time.Time
	Err     error
}

type spotterWatchEvent struct {
	Sequence         uint64 `json:"sequence"`
	Boundary         string `json:"boundary"`
	Operation        string `json:"operation"`
	Origin           string `json:"origin"`
	TriggerAt        string `json:"triggerAt"`
	ObservedAt       string `json:"observedAt"`
	InstanceID       string `json:"instanceId"`
	AppCode          string `json:"appCode"`
	SourceKey        string `json:"sourceKey"`
	SourceCluster    string `json:"sourceCluster"`
	Reversion        int64  `json:"reversion"`
	Status           int32  `json:"status"`
	CanonicalPayload string `json:"canonicalPayload"`
	Err              error  `json:"-"`
}

type spotterWatchBatch struct {
	Events           []spotterWatchEvent `json:"events"`
	EarliestSequence uint64              `json:"earliestSequence"`
	LatestSequence   uint64              `json:"latestSequence"`
	Gap              bool                `json:"gap"`
}

// startSpotterEventWatch continuously consumes the guarded long-poll endpoint
// backed by Spotter's K8s event boundary. A ring gap or HTTP/decode failure is
// emitted as an explicit watch error and therefore fails the ladder report.
func startSpotterEventWatch(ctx context.Context, metricsPort int) <-chan spotterWatchEvent {
	if ctx == nil {
		ctx = context.Background()
	}
	out := make(chan spotterWatchEvent, 4096)
	go func() {
		defer close(out)
		var after uint64
		emit := func(event spotterWatchEvent) bool {
			select {
			case out <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}
		for ctx.Err() == nil {
			url := fmt.Sprintf("http://127.0.0.1:%d/debug/spotter/events?after=%d&wait=5s", metricsPort, after)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				_ = emit(spotterWatchEvent{Err: err})
				return
			}
			response, err := sharedHTTP.Do(req) //nolint:gosec // fixed loopback URL
			if err != nil {
				if ctx.Err() == nil && !emit(spotterWatchEvent{Err: fmt.Errorf("spotter event watch: %w", err)}) {
					return
				}
				continue
			}
			var batch spotterWatchBatch
			decodeErr := json.NewDecoder(io.LimitReader(response.Body, 32<<20)).Decode(&batch)
			_ = response.Body.Close()
			if response.StatusCode != http.StatusOK || decodeErr != nil {
				err = fmt.Errorf("spotter event watch answered %d: %v", response.StatusCode, decodeErr)
				if !emit(spotterWatchEvent{Err: err}) {
					return
				}
				continue
			}
			if batch.Gap {
				if !emit(spotterWatchEvent{Err: fmt.Errorf("spotter event ring gap: after=%d earliest=%d latest=%d", after, batch.EarliestSequence, batch.LatestSequence)}) {
					return
				}
			}
			if batch.LatestSequence < after {
				_ = emit(spotterWatchEvent{Err: fmt.Errorf("spotter event sequence reset: after=%d latest=%d", after, batch.LatestSequence)})
				return
			}
			for _, event := range batch.Events {
				if event.Sequence <= after {
					continue
				}
				if event.Sequence != after+1 {
					if !emit(spotterWatchEvent{Err: fmt.Errorf("spotter event sequence hole: after=%d next=%d", after, event.Sequence)}) {
						return
					}
				}
				if !emit(event) {
					return
				}
				after = event.Sequence
			}
		}
	}()
	return out
}

type nacosServiceWatch struct {
	client     *spotternacos.Client
	service    string
	group      string
	clusters   []string
	callback   func([]spotternacos.Host, error)
	events     chan nacosWatchEvent
	callbackMu sync.Mutex
	callbackWG sync.WaitGroup
	closing    bool
}

type nacosWatchGroup struct {
	events        <-chan nacosWatchEvent
	subscriptions int
}

type planeWatchObservation struct {
	At               time.Time
	TriggerAt        time.Time
	Present          bool
	SourceKey        string
	Operation        string
	Origin           string
	Status           int32
	Reversion        int64
	CanonicalPayload string
}

type exactWatchBoundary struct {
	IssuedAt       time.Time
	SourceSeen     time.Time
	SpotterTrigger time.Time
	SpotterSeen    time.Time
	NacosSeen      time.Time
	Reversion      int64
	Operation      string
	Origin         string
}

// watchTimeline retains the independently observed event boundaries needed
// to correlate one mutation without trusting the implementation's timestamps.
// It is intentionally lossy only for historical events; each ladder target
// has a unique name and is matched against an issue timestamp.
type watchTimeline struct {
	mu             sync.Mutex
	sourcePresent  map[string]time.Time
	sourceDeleted  map[string]time.Time
	nacosPresent   map[string]time.Time
	nacosDeleted   map[string]time.Time
	nacosSnapshots map[string]map[string]string
	nacosStates    map[string]map[string]planeWatchObservation
	// nacosKnown retains the last complete observation for every identity ever
	// seen in a service. Subscribe callbacks can arrive as rapid successive
	// snapshots; using only the immediately previous snapshot can lose the
	// source identity before the delete callback is correlated.
	nacosKnown     map[string]map[string]planeWatchObservation
	nacosEmpty     time.Time
	spotterPresent map[string]time.Time
	spotterDeleted map[string]time.Time
	sourceHistory  map[string][]planeWatchObservation
	spotterHistory map[string][]planeWatchObservation
	nacosHistory   map[string][]planeWatchObservation
	errors         []error
	sourceEvents   int
	spotterEvents  int
	nacosEvents    int
	sourceClosed   bool
	spotterClosed  bool
	nacosClosed    bool
}

func newWatchTimeline(k8sEvents <-chan k8sWatchEvent, spotterEvents <-chan spotterWatchEvent, nacosEvents <-chan nacosWatchEvent) *watchTimeline {
	timeline := &watchTimeline{
		sourcePresent: map[string]time.Time{}, sourceDeleted: map[string]time.Time{},
		spotterPresent: map[string]time.Time{}, spotterDeleted: map[string]time.Time{},
		nacosPresent: map[string]time.Time{}, nacosDeleted: map[string]time.Time{},
		nacosSnapshots: map[string]map[string]string{},
		nacosStates:    map[string]map[string]planeWatchObservation{},
		nacosKnown:     map[string]map[string]planeWatchObservation{},
		sourceHistory:  map[string][]planeWatchObservation{}, spotterHistory: map[string][]planeWatchObservation{},
		nacosHistory: map[string][]planeWatchObservation{},
	}
	go func() {
		defer func() {
			timeline.mu.Lock()
			timeline.sourceClosed = true
			timeline.mu.Unlock()
		}()
		for event := range k8sEvents {
			timeline.mu.Lock()
			timeline.sourceEvents++
			if event.Err != nil {
				timeline.errors = append(timeline.errors, event.Err)
				timeline.mu.Unlock()
				continue
			}
			if event.Pod != nil {
				observation := k8sPlaneObservation(event)
				timeline.sourceHistory[event.Pod.Name] = append(timeline.sourceHistory[event.Pod.Name], observation)
				if event.Type == "DELETED" {
					timeline.sourceDeleted[event.Pod.Name] = event.At
				} else {
					timeline.sourcePresent[event.Pod.Name] = event.At
				}
			}
			timeline.mu.Unlock()
		}
	}()
	go func() {
		defer func() {
			timeline.mu.Lock()
			timeline.spotterClosed = true
			timeline.mu.Unlock()
		}()
		for event := range spotterEvents {
			timeline.mu.Lock()
			timeline.spotterEvents++
			if event.Err != nil {
				timeline.errors = append(timeline.errors, event.Err)
				timeline.mu.Unlock()
				continue
			}
			if event.Boundary != "provider-output/pre-worker" || event.Operation == "" || event.Origin == "" {
				timeline.errors = append(timeline.errors, fmt.Errorf("invalid Spotter event boundary=%q operation=%q origin=%q", event.Boundary, event.Operation, event.Origin))
				timeline.mu.Unlock()
				continue
			}
			observedAt, err := time.Parse(time.RFC3339Nano, event.ObservedAt)
			if err != nil {
				timeline.errors = append(timeline.errors, fmt.Errorf("spotter event timestamp %q: %w", event.ObservedAt, err))
			} else if event.Status == 3 {
				timeline.spotterDeleted[event.InstanceID] = observedAt
			} else {
				timeline.spotterPresent[event.InstanceID] = observedAt
			}
			if err == nil {
				triggerAt, triggerErr := time.Parse(time.RFC3339Nano, event.TriggerAt)
				if triggerErr != nil {
					timeline.errors = append(timeline.errors, fmt.Errorf("spotter trigger timestamp %q: %w", event.TriggerAt, triggerErr))
					timeline.mu.Unlock()
					continue
				}
				if event.Operation == "Sync" && event.Origin == "event-cache-applied" {
					timeline.spotterHistory[event.InstanceID] = append(timeline.spotterHistory[event.InstanceID], planeWatchObservation{
						At: observedAt, TriggerAt: triggerAt, Present: event.Status != 3, SourceKey: event.SourceKey,
						Operation: event.Operation, Origin: event.Origin, Status: event.Status,
						Reversion: event.Reversion, CanonicalPayload: event.CanonicalPayload,
					})
				}
			}
			timeline.mu.Unlock()
		}
	}()
	go func() {
		defer func() {
			timeline.mu.Lock()
			timeline.nacosClosed = true
			timeline.mu.Unlock()
		}()
		for event := range nacosEvents {
			timeline.mu.Lock()
			timeline.nacosEvents++
			if event.Err != nil {
				timeline.errors = append(timeline.errors, event.Err)
			}
			current := make(map[string]string, len(event.Hosts))
			currentStates := make(map[string]planeWatchObservation, len(event.Hosts))
			if len(event.Hosts) == 0 {
				timeline.nacosEmpty = event.At
			}
			for _, host := range event.Hosts {
				id := host.Metadata["instanceId"]
				if id == "" {
					id = host.InstanceID
				}
				if id != "" {
					signature := nacosHostWatchSignature(host)
					current[id] = signature
					observation := nacosPlaneObservation(host, event.At)
					currentStates[id] = observation
					if timeline.nacosKnown[event.Service] == nil {
						timeline.nacosKnown[event.Service] = map[string]planeWatchObservation{}
					}
					timeline.nacosKnown[event.Service][id] = observation
					if previous, existed := timeline.nacosSnapshots[event.Service][id]; !existed || previous != signature {
						timeline.nacosPresent[id] = event.At
						timeline.nacosHistory[id] = append(timeline.nacosHistory[id], observation)
					}
				}
			}
			for id := range timeline.nacosSnapshots[event.Service] {
				if _, exists := current[id]; !exists {
					timeline.nacosDeleted[id] = event.At
					removed := timeline.nacosKnown[event.Service][id]
					if removed.SourceKey == "" {
						removed = timeline.nacosStates[event.Service][id]
					}
					removed.At = event.At
					removed.Present = false
					removed.Status = 3
					timeline.nacosHistory[id] = append(timeline.nacosHistory[id], removed)
				}
			}
			timeline.nacosSnapshots[event.Service] = current
			timeline.nacosStates[event.Service] = currentStates
			timeline.mu.Unlock()
		}
	}()
	return timeline
}

func k8sPlaneObservation(event k8sWatchEvent) planeWatchObservation {
	observation := planeWatchObservation{At: event.At, Present: event.Type != "DELETED"}
	if event.Pod == nil {
		return observation
	}
	observation.Reversion, _ = strconv.ParseInt(event.Pod.ResourceVersion, 10, 64)
	observation.SourceKey = string(event.Pod.UID)
	if event.Type == "DELETED" {
		observation.Status = 3
		return observation
	}
	if event.Pod.Status.Phase == v1.PodRunning && event.Pod.Status.PodIP != "" {
		observation.Status = 2
		if containersReadyOf(event.Pod) {
			observation.Status = 1
		}
	}
	return observation
}

func nacosPlaneObservation(host spotternacos.Host, at time.Time) planeWatchObservation {
	observation := planeWatchObservation{At: at, Present: true}
	status, _ := strconv.ParseInt(host.Metadata["status"], 10, 32)
	observation.Status = int32(status)
	observation.Reversion, _ = strconv.ParseInt(host.Metadata["reversion"], 10, 64)
	if decoded, err := domaininstance.DecodeCompressedCanonicalPayload(host.Metadata["spotter.instance"]); err == nil && decoded != nil {
		observation.CanonicalPayload = domaininstance.CanonicalPayload(decoded)
		observation.SourceKey = decoded.SourceKey
	}
	return observation
}

func (t *watchTimeline) exactBoundary(name string, present bool, status int32, canonical string, issuedAt time.Time) (exactWatchBoundary, bool) {
	if t == nil {
		return exactWatchBoundary{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, spotter := range t.spotterHistory[name] {
		if spotter.Operation != "Sync" || spotter.Origin != "event-cache-applied" {
			continue
		}
		if spotter.At.Before(issuedAt) || spotter.Present != present || spotter.Status != status {
			continue
		}
		if canonical != "" && spotter.CanonicalPayload != canonical {
			continue
		}
		if present && spotter.CanonicalPayload == "" {
			continue
		}
		var sourceAt time.Time
		for _, source := range t.sourceHistory[name] {
			if source.At.Before(issuedAt) || source.Present != present || source.Status != status {
				continue
			}
			if present && source.Reversion != spotter.Reversion {
				continue
			}
			if !present && !deleteSourceIdentityMatches(source, spotter) {
				continue
			}
			sourceAt = source.At
			break
		}
		if sourceAt.IsZero() {
			continue
		}
		var nacosAt time.Time
		for _, nacos := range t.nacosHistory[name] {
			if nacos.At.Before(spotter.At) || nacos.Present != present || nacos.Status != status {
				continue
			}
			if present && (nacos.Reversion != spotter.Reversion || nacos.CanonicalPayload != spotter.CanonicalPayload) {
				continue
			}
			if !present && !deleteNacosIdentityMatches(nacos, spotter) {
				continue
			}
			nacosAt = nacos.At
			break
		}
		if nacosAt.IsZero() {
			continue
		}
		return exactWatchBoundary{
			IssuedAt: issuedAt, SourceSeen: sourceAt, SpotterTrigger: spotter.TriggerAt, SpotterSeen: spotter.At,
			NacosSeen: nacosAt, Reversion: spotter.Reversion, Operation: spotter.Operation, Origin: spotter.Origin,
		}, true
	}
	return exactWatchBoundary{}, false
}

func (t *watchTimeline) mutationBoundary(entry ledgerEntry) (exactWatchBoundary, bool) {
	present := true
	status := int32(1)
	switch entry.Op {
	case "delete":
		present = false
		status = 3
	case "crash":
		status = 2
	case "create", "recover":
		status = 1
	default:
		return exactWatchBoundary{}, false
	}
	return t.exactBoundary(entry.PodName, present, status, "", entry.IssuedAt)
}

// finalMutationBoundary verifies that every plane ends the horizon in the
// desired state, then returns the earliest exact three-plane boundary after
// each plane's last opposite state. It therefore ignores repeated healthy
// refreshes without under-reporting delete→resurrect→delete at the first
// delete.
func (t *watchTimeline) finalMutationBoundary(entry ledgerEntry, horizon time.Time) (exactWatchBoundary, bool) {
	if t == nil {
		return exactWatchBoundary{}, false
	}
	present := true
	status := int32(1)
	switch entry.Op {
	case "delete":
		present = false
		status = 3
	case "crash":
		status = 2
	case "create", "recover":
		status = 1
	default:
		return exactWatchBoundary{}, false
	}
	inWindow := func(at time.Time) bool {
		if at.Before(entry.IssuedAt) {
			return false
		}
		return horizon.IsZero() || !at.After(horizon)
	}
	t.mu.Lock()
	sources := append([]planeWatchObservation(nil), t.sourceHistory[entry.PodName]...)
	spotters := append([]planeWatchObservation(nil), t.spotterHistory[entry.PodName]...)
	nacoses := append([]planeWatchObservation(nil), t.nacosHistory[entry.PodName]...)
	t.mu.Unlock()
	spotters = slicesMatching(spotters, func(observation planeWatchObservation) bool {
		return observation.Operation == "Sync" && observation.Origin == "event-cache-applied"
	})
	for _, history := range [][]planeWatchObservation{sources, spotters, nacoses} {
		sort.Slice(history, func(i, j int) bool { return history[i].At.Before(history[j].At) })
	}
	stateMatches := func(observation planeWatchObservation) bool {
		return observation.Present == present && observation.Status == status
	}
	lastState := func(history []planeWatchObservation) (planeWatchObservation, bool) {
		var latest planeWatchObservation
		for _, observation := range history {
			if inWindow(observation.At) {
				latest = observation
			}
		}
		return latest, !latest.At.IsZero() && stateMatches(latest)
	}
	for _, history := range [][]planeWatchObservation{sources, spotters, nacoses} {
		if _, ok := lastState(history); !ok {
			return exactWatchBoundary{}, false
		}
	}
	cutoff := func(history []planeWatchObservation) time.Time {
		result := entry.IssuedAt
		for _, observation := range history {
			if inWindow(observation.At) && !stateMatches(observation) && observation.At.After(result) {
				result = observation.At
			}
		}
		return result
	}
	sourceCutoff, spotterCutoff, nacosCutoff := cutoff(sources), cutoff(spotters), cutoff(nacoses)
	completedAt := func(source, spotter, nacos planeWatchObservation) time.Time {
		latest := source.At
		if spotter.At.After(latest) {
			latest = spotter.At
		}
		if nacos.At.After(latest) {
			latest = nacos.At
		}
		return latest
	}
	var best exactWatchBoundary
	var bestCompleted time.Time
	for _, spotter := range spotters {
		if !inWindow(spotter.At) || spotter.At.Before(spotterCutoff) || !stateMatches(spotter) {
			continue
		}
		for _, source := range sources {
			if !inWindow(source.At) || source.At.Before(sourceCutoff) || !stateMatches(source) {
				continue
			}
			if present && source.Reversion != spotter.Reversion {
				continue
			}
			if !present && !deleteSourceIdentityMatches(source, spotter) {
				continue
			}
			for _, nacos := range nacoses {
				if !inWindow(nacos.At) || nacos.At.Before(nacosCutoff) || nacos.At.Before(spotter.At) || !stateMatches(nacos) {
					continue
				}
				if present && (spotter.CanonicalPayload == "" || nacos.Reversion != spotter.Reversion || nacos.CanonicalPayload != spotter.CanonicalPayload) {
					continue
				}
				if !present && !deleteNacosIdentityMatches(nacos, spotter) {
					continue
				}
				completed := completedAt(source, spotter, nacos)
				if bestCompleted.IsZero() || completed.Before(bestCompleted) {
					bestCompleted = completed
					best = exactWatchBoundary{
						IssuedAt: entry.IssuedAt, SourceSeen: source.At, SpotterTrigger: spotter.TriggerAt,
						SpotterSeen: spotter.At, NacosSeen: nacos.At, Reversion: spotter.Reversion,
						Operation: spotter.Operation, Origin: spotter.Origin,
					}
				}
			}
		}
	}
	if bestCompleted.IsZero() {
		return exactWatchBoundary{}, false
	}
	return best, true
}

func slicesMatching(values []planeWatchObservation, keep func(planeWatchObservation) bool) []planeWatchObservation {
	result := make([]planeWatchObservation, 0, len(values))
	for _, value := range values {
		if keep(value) {
			result = append(result, value)
		}
	}
	return result
}

func (t *watchTimeline) waitForMutationCoverage(entries []ledgerEntry, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		complete := true
		for _, entry := range entries {
			if _, ok := t.mutationBoundary(entry); !ok {
				complete = false
				break
			}
		}
		if complete || time.Now().After(deadline) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

type watchLatencyValues struct {
	apiK8s, apiSpotter, spotterQueue, spotterNacos, apiNacos []float64
}

func (t *watchTimeline) summarizeMutations(entries []ledgerEntry, subscriptionsWanted, subscriptionsStarted int) (watchRunEvidence, []watchMutationRecord) {
	evidence := watchRunEvidence{
		SubscriptionsWanted: subscriptionsWanted, SubscriptionsStarted: subscriptionsStarted,
		Mutations: len(entries), ByOperation: map[string]watchOperationSummary{},
	}
	t.mu.Lock()
	evidence.K8sEvents = t.sourceEvents
	evidence.SpotterEvents = t.spotterEvents
	evidence.NacosEvents = t.nacosEvents
	t.mu.Unlock()
	evidence.Errors = append(evidence.Errors, t.errorsSnapshot()...)
	evidence.Errors = append(evidence.Errors, t.healthErrors()...)
	values := map[string]*watchLatencyValues{}
	consumed := map[string]struct{}{}
	records := make([]watchMutationRecord, 0, len(entries))
	for _, entry := range entries {
		record := watchMutationRecord{
			Operation: entry.Op, AppCode: entry.AppCode, InstanceID: entry.PodName,
			IssuedAt: entry.IssuedAt.UTC().Format(time.RFC3339Nano),
		}
		boundary, ok := t.mutationBoundary(entry)
		if !ok {
			record.Missing = "exact incremental K8s/Spotter/Nacos boundary"
			evidence.Missing++
			records = append(records, record)
			continue
		}
		boundaryKey := fmt.Sprintf("%s\x00%d\x00%d\x00%d", entry.PodName,
			boundary.SourceSeen.UnixNano(), boundary.SpotterSeen.UnixNano(), boundary.NacosSeen.UnixNano())
		if _, reused := consumed[boundaryKey]; reused {
			record.Missing = "exact boundary reused by another mutation"
			evidence.Missing++
			records = append(records, record)
			continue
		}
		consumed[boundaryKey] = struct{}{}
		evidence.Correlated++
		record.K8sWatchAt = boundary.SourceSeen.UTC().Format(time.RFC3339Nano)
		record.SpotterAt = boundary.SpotterSeen.UTC().Format(time.RFC3339Nano)
		record.NacosAt = boundary.NacosSeen.UTC().Format(time.RFC3339Nano)
		record.Reversion = boundary.Reversion
		record.CorrelationMode = watchCorrelationMode(entry.Op)
		record.APIToK8sSec = boundary.SourceSeen.Sub(entry.IssuedAt).Seconds()
		record.APIToSpotterSec = boundary.SpotterSeen.Sub(entry.IssuedAt).Seconds()
		if !boundary.SpotterTrigger.IsZero() {
			record.SpotterQueueSec = boundary.SpotterSeen.Sub(boundary.SpotterTrigger).Seconds()
		}
		record.SpotterToNacosSec = boundary.NacosSeen.Sub(boundary.SpotterSeen).Seconds()
		record.APIToNacosSec = boundary.NacosSeen.Sub(entry.IssuedAt).Seconds()
		group := values[entry.Op]
		if group == nil {
			group = &watchLatencyValues{}
			values[entry.Op] = group
		}
		group.apiK8s = append(group.apiK8s, record.APIToK8sSec)
		group.apiSpotter = append(group.apiSpotter, record.APIToSpotterSec)
		group.spotterQueue = append(group.spotterQueue, record.SpotterQueueSec)
		group.spotterNacos = append(group.spotterNacos, record.SpotterToNacosSec)
		group.apiNacos = append(group.apiNacos, record.APIToNacosSec)
		records = append(records, record)
	}
	for operation, group := range values {
		evidence.ByOperation[operation] = watchOperationSummary{
			APIToK8s: watchPercentiles(group.apiK8s), APIToSpotter: watchPercentiles(group.apiSpotter),
			SpotterQueue: watchPercentiles(group.spotterQueue), SpotterToNacos: watchPercentiles(group.spotterNacos),
			APIToNacos: watchPercentiles(group.apiNacos),
		}
	}
	return evidence, records
}

func watchCorrelationMode(operation string) string {
	if operation == "delete" {
		return "uid-sourcekey-offline-removal"
	}
	return "revision-status-canonical"
}

func watchPercentiles(values []float64) watchLatencyPercentiles {
	if len(values) == 0 {
		return watchLatencyPercentiles{}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	quantile := func(q float64) float64 {
		index := int(math.Ceil(q*float64(len(sorted)))) - 1
		if index < 0 {
			index = 0
		}
		if index >= len(sorted) {
			index = len(sorted) - 1
		}
		return sorted[index]
	}
	return watchLatencyPercentiles{
		Samples: len(sorted), P50: quantile(0.50), P90: quantile(0.90),
		P95: quantile(0.95), P99: quantile(0.99), Max: sorted[len(sorted)-1],
	}
}

func deleteSourceIdentityMatches(source, spotter planeWatchObservation) bool {
	return source.SourceKey != "" && spotter.SourceKey != "" &&
		(spotter.SourceKey == source.SourceKey || strings.HasSuffix(spotter.SourceKey, "/"+source.SourceKey))
}

func deleteNacosIdentityMatches(nacos, spotter planeWatchObservation) bool {
	return nacos.SourceKey != "" && spotter.SourceKey != "" && nacos.SourceKey == spotter.SourceKey
}

func (t *watchTimeline) spotterReady(name string, present bool, issuedAt time.Time) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if present {
		return !t.spotterPresent[name].IsZero() && !t.spotterPresent[name].Before(issuedAt)
	}
	return !t.spotterDeleted[name].IsZero() && !t.spotterDeleted[name].Before(issuedAt)
}

func (t *watchTimeline) boundaryTimes(names []string, present bool, issuedAt time.Time) (source, spotter, nacos time.Time, ok bool) {
	if t == nil {
		return time.Time{}, time.Time{}, time.Time{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, name := range names {
		var sourceAt, spotterAt, nacosAt time.Time
		if present {
			sourceAt = t.sourcePresent[name]
			spotterAt = t.spotterPresent[name]
			nacosAt = t.nacosPresent[name]
		} else {
			sourceAt = t.sourceDeleted[name]
			spotterAt = t.spotterDeleted[name]
			nacosAt = t.nacosDeleted[name]
			if nacosAt.IsZero() {
				nacosAt = t.nacosEmpty
			}
		}
		if sourceAt.Before(issuedAt) || spotterAt.Before(issuedAt) || nacosAt.Before(issuedAt) || sourceAt.IsZero() || spotterAt.IsZero() || nacosAt.IsZero() {
			return time.Time{}, time.Time{}, time.Time{}, false
		}
		if sourceAt.After(source) {
			source = sourceAt
		}
		if spotterAt.After(spotter) {
			spotter = spotterAt
		}
		if nacosAt.After(nacos) {
			nacos = nacosAt
		}
	}
	return source, spotter, nacos, true
}

func (t *watchTimeline) sourceReady(name string, present bool, issuedAt time.Time) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if present {
		return !t.sourcePresent[name].IsZero() && !t.sourcePresent[name].Before(issuedAt)
	}
	return !t.sourceDeleted[name].IsZero() && !t.sourceDeleted[name].Before(issuedAt)
}

func (t *watchTimeline) nacosReady(name string, present bool, issuedAt time.Time) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if present {
		return !t.nacosPresent[name].IsZero() && !t.nacosPresent[name].Before(issuedAt)
	}
	// The ladder service owns no other instances, so an empty Subscribe
	// callback is the authoritative DELETE event. The catalog read remains a
	// second independent check for servers/SDKs that suppress empty updates.
	deletedAt := t.nacosDeleted[name]
	if deletedAt.IsZero() {
		deletedAt = t.nacosEmpty
	}
	return !deletedAt.IsZero() && !deletedAt.Before(issuedAt)
}

func nacosHostWatchSignature(host spotternacos.Host) string {
	return fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%t\x00%t\x00%t\x00%s\x00%s",
		host.InstanceID, host.IP, host.Port, host.ClusterName, host.Healthy, host.Enabled,
		host.Ephemeral, host.Metadata["status"], host.Metadata["spotter.instance"])
}

func (t *watchTimeline) errorsSnapshot() []string {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.errors))
	for i, err := range t.errors {
		out[i] = err.Error()
	}
	return out
}

func (t *watchTimeline) healthErrors() []string {
	if t == nil {
		return []string{"watch timeline is nil"}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	errors := make([]string, 0, 3)
	if t.sourceClosed {
		errors = append(errors, "K8s watch channel closed before observation completed")
	}
	if t.spotterClosed {
		errors = append(errors, "Spotter event channel closed before observation completed")
	}
	if t.nacosClosed {
		errors = append(errors, "Nacos Subscribe channel closed before observation completed")
	}
	if t.sourceEvents == 0 || t.spotterEvents == 0 || t.nacosEvents == 0 {
		errors = append(errors, fmt.Sprintf("watch event counts incomplete (k8s=%d spotter=%d nacos=%d)", t.sourceEvents, t.spotterEvents, t.nacosEvents))
	}
	return errors
}

func startNacosServiceWatch(ctx context.Context, addr, service string) (*nacosServiceWatch, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	client, err := spotternacos.NewClientWithConfig(spotternacos.ClientConfig{
		ServerURL: addr, TransportMode: spotternacos.TransportSDK,
		NamespaceID: nacosNamespace, GroupName: nacosGroup, Timeout: 10 * time.Second,
		UpdateCacheWhenEmpty: true,
	}, nil)
	if err != nil {
		return nil, err
	}
	watch := &nacosServiceWatch{client: client, service: service, group: nacosGroup, clusters: []string{"k8s"}, events: make(chan nacosWatchEvent, 256)}
	watch.callback = func(hosts []spotternacos.Host, callbackErr error) {
		callbackAt := time.Now()
		watch.callbackMu.Lock()
		if watch.closing {
			watch.callbackMu.Unlock()
			return
		}
		watch.callbackWG.Add(1)
		watch.callbackMu.Unlock()
		defer watch.callbackWG.Done()
		copyHosts := cloneNacosHosts(hosts)
		select {
		case watch.events <- nacosWatchEvent{Service: service, Hosts: copyHosts, At: callbackAt, Err: callbackErr}:
		case <-ctx.Done():
		}
	}
	if err := client.Subscribe(service, nacosGroup, watch.clusters, watch.callback); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("subscribe Nacos service %s: %w", service, err)
	}
	go func() {
		<-ctx.Done()
		watch.callbackMu.Lock()
		watch.closing = true
		watch.callbackMu.Unlock()
		_ = client.Unsubscribe(service, nacosGroup, watch.clusters, watch.callback)
		watch.callbackWG.Wait()
		_ = client.Close()
		close(watch.events)
	}()
	return watch, nil
}

func startNacosServiceWatches(ctx context.Context, addr string, services []string) (*nacosWatchGroup, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	merged := make(chan nacosWatchEvent, 4096)
	var wg sync.WaitGroup
	for _, service := range services {
		watch, err := startNacosServiceWatch(ctx, addr, service)
		if err != nil {
			return nil, fmt.Errorf("start Nacos Subscribe for %s: %w", service, err)
		}
		wg.Add(1)
		go func(events <-chan nacosWatchEvent) {
			defer wg.Done()
			for event := range events {
				select {
				case merged <- event:
				case <-ctx.Done():
					return
				}
			}
		}(watch.Events())
	}
	go func() {
		wg.Wait()
		close(merged)
	}()
	return &nacosWatchGroup{events: merged, subscriptions: len(services)}, nil
}

func (g *nacosWatchGroup) Events() <-chan nacosWatchEvent {
	if g == nil {
		return nil
	}
	return g.events
}

func cloneNacosHosts(hosts []spotternacos.Host) []spotternacos.Host {
	copyHosts := make([]spotternacos.Host, len(hosts))
	for i, host := range hosts {
		copyHosts[i] = host
		if host.Metadata != nil {
			copyHosts[i].Metadata = make(map[string]string, len(host.Metadata))
			for key, value := range host.Metadata {
				copyHosts[i].Metadata[key] = value
			}
		}
	}
	return copyHosts
}

func (w *nacosServiceWatch) Events() <-chan nacosWatchEvent {
	if w == nil {
		return nil
	}
	return w.events
}
