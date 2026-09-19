//go:build e2e

package k8s

import (
	"context"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/nacos"
	"spotter/pkg/worker"
)

// TestE2EK8sCompareAndFullPushPrunesBootGhost drives the real K8s provider
// CompareAndFlush + revalidated emitSyncAll path through DefaultWorker and the
// Nacos Sink. Remote-only deletion must be deferred by CompareAndFlush and
// then performed by the exclusive full-push prune, which closes the mixed-time
// delete race without leaving the boot-time ghost behind.
func TestE2EK8sCompareAndFullPushPrunesBootGhost(t *testing.T) {
	remote := nacosmock.Start()
	defer remote.Close()
	sink, err := nacos.NewHTTPCompatSink(remote.URL(), nil)
	if err != nil {
		t.Fatalf("NewHTTPCompatSink: %v", err)
	}
	defer func() { _ = sink.Close() }()
	fanout, err := worker.NewFanoutSink(nil, worker.NamedSink{Name: nacos.SinkName, Sink: sink})
	if err != nil {
		t.Fatalf("NewFanoutSink: %v", err)
	}
	if err := fanout.SetReconcileSource(nacos.SinkName); err != nil {
		t.Fatalf("SetReconcileSource: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, fanout, nil, nil)
	if err != nil {
		t.Fatalf("NewResourceWorker: %v", err)
	}

	pod := newValidPod("msp", "pod-live")
	pod.ResourceVersion = "42"
	live := instanceFromPod(t, pod)
	ghost := *live
	ghost.InstanceId = "pod-ghost"
	ghost.Ip = "172.17.0.99"
	ghost.Reversion = 41
	if err := sink.PushAll(time.Now().UnixNano(), []*instance.Instance{live, &ghost}); err != nil {
		t.Fatalf("seed remote state: %v", err)
	}

	provider := newTestProvider(newFakeRobot(nil, []interface{}{pod}, true), w)
	defer provider.pool.Release()
	provider.SetNacosReconcileSource(true)
	provider.CompareAndFlush()
	// CompareAndFlush deliberately does not issue an incremental ghost delete.
	// The immediately following full snapshot revalidates the processed cache
	// under the ordered sink's exclusive gate and owns destructive prune.
	provider.emitSyncAll()

	deadline := time.Now().Add(5 * time.Second)
	for {
		hosts := remote.Instances("pay-user", "k8s")
		if len(hosts) == 1 && hosts[0].Metadata["instanceId"] == "pod-live" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("boot ghost did not converge through revalidated full push: hosts=%+v", hosts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	view, err := sink.GetAll([]int32{instance.InstanceStatusOnline}, "k8s")
	if err != nil {
		t.Fatalf("GetAll after prune: %v", err)
	}
	if len(view.Instance) != 1 || instance.DiffNacosReconcile(live, view.Instance[0]) {
		t.Fatalf("final canonical view=%#v, want only live=%#v", view.Instance, live)
	}
}
