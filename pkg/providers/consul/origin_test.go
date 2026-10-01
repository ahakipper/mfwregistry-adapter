package consul

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/providers"
	workerpkg "spotter/pkg/worker"
)

// TestConsulWatchOriginFlowsThroughIncrementalAndRecoveryFullEvents pins the
// source-to-worker timing boundary: a blocking-watch return timestamp must be
// the Trigger for add, update, delete, and the trusted full snapshot emitted
// when a source recovers from a confirmed empty catalog. The provider's
// payload-free legacy syncInstance/EventsSync wrappers are intentionally not
// used for this path; the timestamped monitor callback calls syncInstanceAt.
func TestConsulWatchOriginFlowsThroughIncrementalAndRecoveryFullEvents(t *testing.T) {
	worker := &fakeWorker{}
	entry := healthyEntry("origin-service", "10.0.0.21", 10)
	provider := newStaticConsulProvider(t, worker, map[string][]*api.ServiceEntry{
		"pay-user": {entry},
	})
	provider.emptyRetryInterval = 0
	defer provider.shutdown()

	origin := time.Unix(1_700_000_000, 123_456_789)
	if err := provider.syncInstanceAt(origin); err != nil {
		t.Fatalf("initial watch sync error = %v, want nil", err)
	}
	assertLastConsulEventTrigger(t, worker, worker.handleSnapshot(), origin, "add")

	entry.Service.ModifyIndex = 11
	entry.Service.Meta["version"] = "v2"
	if err := provider.syncInstanceAt(origin); err != nil {
		t.Fatalf("update watch sync error = %v, want nil", err)
	}
	assertLastConsulEventTrigger(t, worker, worker.handleSnapshot(), origin, "update")

	provider.monitor.(*staticMonitor).entries["pay-user"] = nil
	for attempt := 0; attempt < 3; attempt++ {
		_ = provider.syncInstanceAt(origin)
	}
	events := worker.handleSnapshot()
	assertLastConsulEventTrigger(t, worker, events, origin, "delete")
	if events[len(events)-1].Data[0].Status != providers.InstanceStatusOffline {
		t.Fatalf("delete status = %d, want offline", events[len(events)-1].Data[0].Status)
	}

	// Reintroducing the service after the confirmed empty state exercises the
	// recovered full-sync branch. Both the incremental add and the trusted
	// full snapshot must retain the same watch origin.
	provider.monitor.(*staticMonitor).entries["pay-user"] = []*api.ServiceEntry{entry}
	if err := provider.syncInstanceAt(origin); err != nil {
		t.Fatalf("recovery watch sync error = %v, want nil", err)
	}
	events = worker.handleSnapshot()
	assertLastConsulEventTrigger(t, worker, events, origin, "recovery add")
	full := worker.syncAllEvents()
	if len(full) != 1 {
		t.Fatalf("recovery SyncAll events = %d, want one", len(full))
	}
	if full[0].Trigger != origin.UnixNano() {
		t.Fatalf("recovery SyncAll trigger = %d, want watch origin %d", full[0].Trigger, origin.UnixNano())
	}
}

func assertLastConsulEventTrigger(t *testing.T, worker *fakeWorker, events []*workerpkg.Event, origin time.Time, operation string) {
	t.Helper()
	if len(events) == 0 {
		t.Fatalf("%s emitted no worker event", operation)
	}
	got := events[len(events)-1]
	if got.Trigger != origin.UnixNano() {
		t.Fatalf("%s trigger = %d, want watch origin %d", operation, got.Trigger, origin.UnixNano())
	}
}

// TestConsulWatchOriginConcurrentEventsRemainExact exercises the same
// timestamp seam under concurrent source callbacks. The worker recording
// double is synchronized, so -race validates that origin propagation does
// not introduce shared mutable timing state.
func TestConsulWatchOriginConcurrentEventsRemainExact(t *testing.T) {
	worker := &fakeWorker{}
	provider := &consul{
		ctx:           context.Background(),
		worker:        worker,
		sourceCluster: "consul-race",
	}
	const count = 64
	var done = make(chan struct{}, count)
	for i := 0; i < count; i++ {
		i := i
		go func() {
			origin := time.Unix(1_700_000_000, int64(i+1))
			provider.eventsSyncAt([]*sv.Instance{{InstanceId: string(rune('a' + i)), Status: providers.InstanceStatusOnline}}, nil, nil, origin)
			done <- struct{}{}
		}()
	}
	for i := 0; i < count; i++ {
		<-done
	}

	events := worker.handleSnapshot()
	if len(events) != count {
		t.Fatalf("concurrent events = %d, want %d", len(events), count)
	}
	for _, event := range events {
		if len(event.Data) != 1 {
			t.Fatalf("concurrent event data = %#v, want one instance", event.Data)
		}
		id := []rune(event.Data[0].InstanceId)[0]
		want := time.Unix(1_700_000_000, int64(id-'a'+1)).UnixNano()
		if event.Trigger != want {
			t.Fatalf("instance %q trigger = %d, want %d", event.Data[0].InstanceId, event.Trigger, want)
		}
	}
}
