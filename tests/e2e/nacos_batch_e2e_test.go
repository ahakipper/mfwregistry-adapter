//go:build e2e
// +build e2e

package e2e

import (
	"context"
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
