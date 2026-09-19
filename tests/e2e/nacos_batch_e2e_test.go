//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/fakes"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/nacos"
	"spotter/pkg/worker"
)

// TestE2ENacosPersistentBatchRetryConverges proves the full-operation failure
// boundary against the real Nacos sink and mock server: a failed application
// batch is queued as one SyncAll retry, does not prune partial remote state,
// and converges after the next retry succeeds.
func TestE2ENacosPersistentBatchRetryConverges(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	logger := &fakes.FakeLogger{}
	sink, err := nacos.NewHTTPCompatSink(server.URL(), logger)
	if err != nil {
		t.Fatalf("nacos.NewHTTPCompatSink() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, sink, logger, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("worker.NewResourceWorker() error = %v", err)
	}

	// Fail only instance writes. The health-check PUT remains available, so
	// the failure is in the application batch phase rather than startup.
	server.SetEndpointStatus("/nacos/v1/ns/instance", 500)
	items := []*instance.Instance{
		{Provider: "k8s", AppCode: "pay-user", InstanceId: "pod-a", Ip: "10.0.0.1", Status: instance.InstanceStatusOnline, Enabled: true, Reversion: 1},
		{Provider: "k8s", AppCode: "pay-user", InstanceId: "pod-b", Ip: "10.0.0.2", Status: instance.InstanceStatusOnline, Enabled: true, Reversion: 2},
	}
	w.Handle(&worker.Event{
		Trigger:  1,
		Data:     items,
		Operate:  worker.OperateTypeSyncAll,
		Scope:    "k8s",
		BatchID:  worker.FullBatchID("k8s", items),
		Sequence: 1,
	})

	// The failed batch must not proceed to the prune read. This snapshot is
	// taken before enabling the retry, so it cannot include a later success.
	for _, request := range server.Requests() {
		if request.Method == "GET" && (request.Path == "/nacos/v1/ns/instance/list" || request.Path == "/nacos/v1/ns/catalog/instances") {
			t.Fatalf("prune read occurred after failed batch: %+v", request)
		}
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 0 {
		t.Fatalf("instances after failed batch = %d, want zero before retry", got)
	}

	// The worker's normal five-second retry loop replays the complete SyncAll
	// operation. Once the sink is healthy, the retry must register both items
	// and then complete prune without leaving the operation queued.
	server.SetEndpointStatus("/nacos/v1/ns/instance", 0)
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if len(server.Instances("pay-user", "k8s")) == len(items) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Nacos catalog did not converge after full-sync retry: instances=%v requests=%v", server.Instances("pay-user", "k8s"), server.Requests())
}

// TestE2ENacosSinkWideConcurrencyCapsIncrementalAndFullPublication proves the
// configured item-call limit is shared by concurrent incremental and full
// publication entry points. A per-call semaphore would let these two waves
// reach twice the configured concurrency at the server.
func TestE2ENacosSinkWideConcurrencyCapsIncrementalAndFullPublication(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	server.SetEndpointDelay("/nacos/v1/ns/instance", 50*time.Millisecond)
	sink, err := nacos.NewHTTPCompatSink(server.URL(), &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewHTTPCompatSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	nacos.SetPushConcurrency(3)
	t.Cleanup(func() { nacos.SetPushConcurrency(nacos.DefaultPushConcurrency) })
	server.SetInstances([]nacosmock.Host{{
		IP: "10.99.0.1", Port: 8999, Enabled: true,
		Metadata: map[string]string{"instanceId": "stale-full"},
	}}, "DEFAULT_GROUP", "full-app", "k8s")

	makeItems := func(app, prefix, provider string, count int) []*instance.Instance {
		items := make([]*instance.Instance, count)
		for i := range items {
			items[i] = &instance.Instance{
				InstanceId: fmt.Sprintf("%s-%02d", prefix, i), AppCode: app, Provider: provider,
				Ip:     fmt.Sprintf("10.%d.%d.%d", len(app), len(prefix), i+1),
				Ports:  []*instance.PortInfo{{Port: int32(8000 + i)}},
				Status: instance.InstanceStatusOnline, Enabled: true, Reversion: int64(i + 1),
			}
		}
		return items
	}
	// Two providers share one Sink. The short K8s full snapshot reaches its
	// stale DELETE while the longer ECS incremental wave is still active; the
	// Sink-wide cap must cover POST and DELETE together without cross-provider
	// prune.
	incremental := makeItems("incremental-app", "inc", "ecs", 50)
	full := makeItems("full-app", "full", "k8s", 5)

	fullErr := make(chan error, 1)
	go func() { fullErr <- sink.PushAll(time.Now().UnixNano(), full) }()
	// The mock records the DELETE before applying its endpoint delay. Wait for
	// that deterministic barrier, then start the ECS wave while the prune
	// permit is held.
	deleteDeadline := time.Now().Add(5 * time.Second)
	deleteStarted := false
	for time.Now().Before(deleteDeadline) {
		for _, request := range server.Requests() {
			if request.Path == "/nacos/v1/ns/instance" && request.Method == "DELETE" {
				deleteStarted = true
				break
			}
		}
		if deleteStarted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !deleteStarted {
		t.Fatal("full snapshot never reached the prune DELETE barrier")
	}
	if err := sink.Push(time.Now().UnixNano(), incremental); err != nil {
		t.Fatalf("incremental publication during prune: %v", err)
	}
	if err := <-fullErr; err != nil {
		t.Fatalf("full publication: %v", err)
	}
	if got := server.MaxConcurrentRequests("/nacos/v1/ns/instance"); got > 3 || got < 2 {
		t.Fatalf("max concurrent instance requests = %d, want 2..3 with Sink-wide cap 3", got)
	}
	if got := server.MaxConcurrentWhileDelete("/nacos/v1/ns/instance"); got < 2 || got > 3 {
		t.Fatalf("max active requests while prune DELETE was in flight = %d, want 2..3 under the same cap", got)
	}
	if got := len(server.Instances("incremental-app", "ecs")); got != len(incremental) {
		t.Fatalf("incremental entries=%d, want %d", got, len(incremental))
	}
	if got := len(server.Instances("full-app", "k8s")); got != len(full) {
		t.Fatalf("full entries=%d, want %d", got, len(full))
	}
	deleteSeen := false
	for _, request := range server.Requests() {
		if request.Path == "/nacos/v1/ns/instance" && request.Method == "DELETE" {
			deleteSeen = true
			break
		}
	}
	if !deleteSeen {
		t.Fatal("full snapshot did not exercise stale-instance prune DELETE")
	}
}
