//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/worker"

	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/nacos"
)

// TestE2EPermanent4xxDropsFromRetryQueue assembles AUDIT-D-7/E2E-1's chain —
// the exact composition that failed live in the F7 incident — from exported
// API only:
//
//	discoverymock (bufconn, the Atlas leg) -> discoverycenter.Registry
//	nacosmock (loopback HTTP, the Nacos leg) -> nacos.Sink
//	                                          -> worker.FanoutSink(atlas, nacos)
//	                                          -> worker.NewResourceWorker (real retry loop)
//
// Arrange: one healthy online instance is pushed through the worker and lands
// in both sinks. Act: the nacosmock answers HTTP 400 on every endpoint
// (SetStatus — the failure is isolated to the nacos leg structurally: the
// Atlas leg is a separate discoverymock server, so SetStatus's every-endpoint
// semantics cannot touch it), then the worker handles one Sync event carrying
// an OFFLINE instance with a valid IP — the deregister the 400 rejects.
//
// Assert, on the REAL 5s retry ticker:
//
//   - the retry queue entry for the nacos sink DROPS (the 4xx is permanent:
//     an identical retry can never succeed) — observed through the metrics
//     recorder injected into the worker: the last nacos queue-depth
//     observation is 0 and the queue drains after at most one retry cycle;
//   - THE REQUEST-COUNT BOUND, the incident's anti-signature: the nacosmock
//     sees at most 2 DELETE attempts (the initial push + at most one retry
//     cycle), NOT the 9624 futile retries the live incident produced. A
//     regression that re-classifies the 4xx as retriable spins the DELETE
//     every 5s and blows this bound immediately;
//   - the Atlas leg is unharmed: its push of the same event succeeded, so it
//     is never queued and its error surface stays empty.
//
// The wait bound is 12s: the retry ticker is 5s of real time, so the initial
// push, one retry tick and its drop, plus the depth publication of the NEXT
// tick fit with margin (CI descheduling headroom; the worker blackbox suite
// uses 15s for two ticks, this needs one cycle plus margin).
func TestE2EPermanent4xxDropsFromRetryQueue(t *testing.T) {
	// --- Atlas leg: the real registry over the in-memory discovery mock.
	discovery, err := discoverymock.Start()
	if err != nil {
		t.Fatalf("discoverymock.Start() error = %v", err)
	}
	defer discovery.Close()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := discovery.DialContext(dialCtx)
	if err != nil {
		t.Fatalf("discoverymock.DialContext() error = %v", err)
	}
	defer func() { _ = conn.Close() }()

	client, err := discoverycenter.NewClient(&serviceClient{conn: conn}, nil, nil)
	if err != nil {
		t.Fatalf("discoverycenter.NewClient() error = %v", err)
	}
	registry, err := discoverycenter.NewDiscoveryCenter(client, nil, nil, false)
	if err != nil {
		t.Fatalf("discoverycenter.NewDiscoveryCenter() error = %v", err)
	}

	// --- Nacos leg: the real sink over the loopback nacosmock.
	nacosServer := nacosmock.Start()
	defer nacosServer.Close()
	nacosSink, err := nacos.NewSink(nacosServer.URL(), nil)
	if err != nil {
		t.Fatalf("nacos.NewSink() error = %v", err)
	}

	// --- The fan-out and the worker: the same declaration order the server
	// wiring uses (Atlas primary, Nacos second), with the e2e's observation
	// recorder injected — the worker constructor accepts it, and the retry
	// queue publishes its per-sink depths through it on every tick.
	metrics := &queueDepthRecorder{}
	fanout, err := worker.NewFanoutSink(nil,
		worker.NamedSink{Name: worker.AtlasSinkName, Sink: registry},
		worker.NamedSink{Name: nacos.SinkName, Sink: nacosSink},
	)
	if err != nil {
		t.Fatalf("worker.NewFanoutSink() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, fanout, nil, metrics)
	if err != nil {
		t.Fatalf("worker.NewResourceWorker() error = %v", err)
	}

	// --- Arrange: the healthy instance registers through the full fan-out
	// (both sinks succeed). The composite id the deregister must target.
	healthy := &v2.Instance{
		InstanceId: "pod-f7", AppCode: "payments", Ip: "10.0.0.7",
		Ports: []*v2.PortInfo{{Port: 8080}}, Provider: "k8s",
		Status: 1, Reversion: 1, EnvType: "test",
	}
	w.Handle(&worker.Event{Trigger: 1, Data: []*v2.Instance{healthy}, Operate: worker.OperateTypeSync})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(nacosServer.Instances("payments", "k8s")) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(nacosServer.Instances("payments", "k8s")); got != 1 {
		t.Fatalf("nacos payments/k8s instances after the healthy push = %d, want 1 (the arrange step); requests = %d",
			got, len(nacosServer.Requests()))
	}
	if got := len(discovery.Calls()); got == 0 {
		t.Fatal("the Atlas leg received no push for the healthy instance (the arrange step failed on the atlas side)")
	}

	// --- Act: only the Nacos leg fails — every nacosmock endpoint answers
	// 400 (a permanent, request-rejected class: APIError.Permanent() is true
	// for 4xx). The discoverymock is a separate server, so the Atlas leg
	// keeps succeeding.
	nacosServer.SetStatus(400)

	// The offline event: the same instance, status 3, valid IP — the exact
	// deregister shape the incident spun on.
	offline := &v2.Instance{
		InstanceId: "pod-f7", AppCode: "payments", Ip: "10.0.0.7",
		Ports: []*v2.PortInfo{{Port: 8080}}, Provider: "k8s",
		Status: 3, Reversion: 2, EnvType: "test",
	}
	w.Handle(&worker.Event{Trigger: 2, Data: []*v2.Instance{offline}, Operate: worker.OperateTypeSync})

	// --- Assert (1): the nacos queue entry drops. The retry ticker (5s
	// real) fires at most one retry cycle for the entry; the retry fails
	// with the same 400, classifies permanent through the FanoutError
	// nesting, and the entry is dropped. The queue then holds nothing for
	// the nacos sink, observable through the injected metrics recorder: the
	// LAST depth observation for the nacos sink is 0 and the entry was seen
	// queued at most once. Bound: 12s (one retry tick + one depth tick +
	// CI margin).
	dropDeadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(dropDeadline) {
		if metrics.lastDepth(nacos.SinkName) == 0 && metrics.sawNonzero(nacos.SinkName) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !metrics.sawNonzero(nacos.SinkName) {
		t.Fatalf("the failed nacos push was never queued: no nonzero depth observation for sink %q; observations = %v",
			nacos.SinkName, metrics.snapshot())
	}
	if got := metrics.lastDepth(nacos.SinkName); got != 0 {
		t.Fatalf("last queue depth for the nacos sink = %d, want 0 (the permanent 4xx must drop the entry); observations = %v",
			got, metrics.snapshot())
	}

	// --- Assert (2): the request-count bound — the incident's
	// anti-signature. The nacosmock records every request; DELETE attempts
	// against the instance endpoint must be at most 2 (the initial push of
	// the offline event + at most one retry cycle). The live F7 incident
	// fired 9624; a regression that re-classifies 4xx as retriable spins the
	// DELETE every 5s and this bound fails within seconds.
	//
	// The bound is checked AFTER the queue has drained, so late retries
	// have no cover: the drop already happened, no further attempt can
	// arrive except by re-queueing (the regression being guarded against).
	if deletes := countDeletes(nacosServer.Requests()); deletes > 2 {
		t.Fatalf("nacos DELETE attempts = %d, want <= 2 (the initial push + at most one retry; the F7 anti-signature) — the retry queue is spinning on a permanent 4xx",
			deletes)
	}

	// The Atlas leg was never queued (its push succeeded) and saw no nacos
	// failure: the fan-out error surface stayed nacos-only.
	if got := metrics.lastDepth(worker.AtlasSinkName); got > 0 {
		t.Fatalf("last queue depth for the atlas sink = %d, want 0 (its push succeeded; observations = %v)",
			got, metrics.snapshot())
	}
	if got := metrics.sawNonzero(worker.AtlasSinkName); got {
		t.Fatalf("the atlas sink was queued for retry, want never (its push of the offline event succeeded); observations = %v",
			metrics.snapshot())
	}

	// One full extra retry window of stability: the dropped entry never
	// comes back (no re-queue, no depth growth), and the DELETE count stays
	// within the bound.
	time.Sleep(6 * time.Second)
	if deletes := countDeletes(nacosServer.Requests()); deletes > 2 {
		t.Fatalf("nacos DELETE attempts after one more retry window = %d, want <= 2 (the drop is final; the F7 anti-signature)",
			deletes)
	}
	if got := metrics.lastDepth(nacos.SinkName); got != 0 {
		t.Fatalf("queue depth for the nacos sink after one more window = %d, want 0 (the drop is final); observations = %v",
			got, metrics.snapshot())
	}

	// --- Cleanup: stop the retry loop.
	cancel()
}

// countDeletes counts the DELETE requests against the nacos instance
// endpoint in the mock's recorded request log.
func countDeletes(requests []nacosmock.Request) int {
	count := 0
	for _, request := range requests {
		if request.Method == "DELETE" && request.Path == "/nacos/v1/ns/instance" {
			count++
		}
	}
	return count
}

// queueDepthRecorder is the e2e's observation recorder: it records every
// SetSyncErrorQueueDepth call the worker's retry loop publishes (the same
// seam the unit tier's fakes.FakeMetricsRecorder observes, kept test-local
// because the e2e package cannot import internal/testkit/fakes' unexported
// internals — NewFakeMetricsRecorder IS exported, but a local struct keeps
// the assertion surface (lastDepth/sawNonzero) explicit and dependency-free).
// It is written from the worker's 5s-ticker goroutine and read from the
// test goroutine's poll loop, so every field access is mutex-guarded (the
// FakeMetricsRecorder pattern).
type queueDepthRecorder struct {
	mu           sync.Mutex
	observations []queueDepthObservation
}

// queueDepthObservation is one captured depth publication: the sink name and
// the depth at that moment.
type queueDepthObservation struct {
	Sink  string
	Depth int
}

func (r *queueDepthRecorder) ObserveSyncOnceDuration(time.Duration)                     {}
func (r *queueDepthRecorder) ObserveSyncAllDuration(string, time.Duration)              {}
func (r *queueDepthRecorder) ObserveEventToStoreDuration(string, string, time.Duration) {}
func (r *queueDepthRecorder) IncEventsDropped(string)                                   {}
func (r *queueDepthRecorder) SetK8sQueueDepth(int)                                      {}
func (r *queueDepthRecorder) SetSyncErrorQueueDepth(sink string, depth int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observations = append(r.observations, queueDepthObservation{Sink: sink, Depth: depth})
}
func (r *queueDepthRecorder) MarkSyncOnce() {}

// lastDepth returns the most recent observed depth of one sink (-1 when the
// sink was never observed).
func (r *queueDepthRecorder) lastDepth(sink string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := -1
	for _, observation := range r.observations {
		if observation.Sink == sink {
			last = observation.Depth
		}
	}
	return last
}

// sawNonzero reports whether any observation of the sink carried a nonzero
// depth (the entry WAS queued — the drop assertion needs the queueing to
// have been observable, so a silently-never-queued failure cannot pass).
func (r *queueDepthRecorder) sawNonzero(sink string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, observation := range r.observations {
		if observation.Sink == sink && observation.Depth > 0 {
			return true
		}
	}
	return false
}

// snapshot returns a copy of the observations for failure messages (the
// worker's ticker goroutine keeps appending while the test renders them).
func (r *queueDepthRecorder) snapshot() []queueDepthObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queueDepthObservation(nil), r.observations...)
}
