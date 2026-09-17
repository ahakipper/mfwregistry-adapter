//go:build observe
// +build observe

package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

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
			for event := range watcher.ResultChan() {
				if event.Type == watch.Error {
					_ = emit(k8sWatchEvent{Type: string(event.Type), At: time.Now(), Err: fmt.Errorf("k8s watch error: %v", event.Object)})
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
	Hosts []spotternacos.Host
	At    time.Time
	Err   error
}

type spotterWatchEvent struct {
	Sequence         uint64 `json:"sequence"`
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
			for _, event := range batch.Events {
				if event.Sequence <= after {
					continue
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

// watchTimeline retains the independently observed event boundaries needed
// to correlate one mutation without trusting the implementation's timestamps.
// It is intentionally lossy only for historical events; each ladder target
// has a unique name and is matched against an issue timestamp.
type watchTimeline struct {
	mu             sync.Mutex
	sourcePresent  map[string]time.Time
	sourceDeleted  map[string]time.Time
	nacosPresent   map[string]time.Time
	nacosEmpty     time.Time
	spotterPresent map[string]time.Time
	spotterDeleted map[string]time.Time
	errors         []error
}

func newWatchTimeline(k8sEvents <-chan k8sWatchEvent, spotterEvents <-chan spotterWatchEvent, nacosEvents <-chan nacosWatchEvent) *watchTimeline {
	timeline := &watchTimeline{
		sourcePresent: map[string]time.Time{}, sourceDeleted: map[string]time.Time{},
		spotterPresent: map[string]time.Time{}, spotterDeleted: map[string]time.Time{},
		nacosPresent: map[string]time.Time{},
	}
	go func() {
		for event := range k8sEvents {
			timeline.mu.Lock()
			if event.Err != nil {
				timeline.errors = append(timeline.errors, event.Err)
				timeline.mu.Unlock()
				continue
			}
			if event.Pod != nil {
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
		for event := range spotterEvents {
			timeline.mu.Lock()
			if event.Err != nil {
				timeline.errors = append(timeline.errors, event.Err)
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
			timeline.mu.Unlock()
		}
	}()
	go func() {
		for event := range nacosEvents {
			timeline.mu.Lock()
			if event.Err != nil {
				timeline.errors = append(timeline.errors, event.Err)
			}
			if len(event.Hosts) == 0 {
				timeline.nacosEmpty = event.At
			}
			for _, host := range event.Hosts {
				id := host.Metadata["instanceId"]
				if id == "" {
					id = host.InstanceID
				}
				if id != "" {
					timeline.nacosPresent[id] = event.At
				}
			}
			timeline.mu.Unlock()
		}
	}()
	return timeline
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
			nacosAt = t.nacosEmpty
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
	return !t.nacosEmpty.IsZero() && !t.nacosEmpty.Before(issuedAt)
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
		case watch.events <- nacosWatchEvent{Hosts: copyHosts, At: time.Now(), Err: callbackErr}:
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
