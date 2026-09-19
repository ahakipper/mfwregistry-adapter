//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/nacosmock"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/nacos"
	"spotter/pkg/worker"
)

// TestE2ENacosFanoutTransportDriftHealPrimitives exercises the real
// worker/fanout/Nacos-Sink transport boundary for remote ghost deletion and a
// wire Enabled heal. It deliberately scripts the provider outputs; the real
// K8s CompareAndFlush + revalidated SyncAll/prune chain is covered by
// pkg/providers/k8s/TestE2EK8sCompareAndFullPushPrunesBootGhost.
//
//	nacosmock (the only active sink)
//	  -> FanoutSink with Nacos as its primary/read source
//	  -> DefaultWorker
//	  -> scripted provider-equivalent worker events
//
// The scenario: nacos state drifted while spotter was down — one live pod's
// registration is intact, one pod was deleted while down (its registration
// is a remote-only ghost), and one healthy registration was console-disabled
// (enabled=false, metadata status "1"). The scripted outputs verify the
// transport primitives used by the real provider test:
//   - a status-3 event deregisters the ghost;
//   - a local status-1 event re-enables the console-disabled host.
//
// The nacos catalog replaces the in-memory remembered memory as the
// boot-time ownership record (the 416e62a residual, closed).
func TestE2ENacosFanoutTransportDriftHealPrimitives(t *testing.T) {
	// --- Nacos side: the real sink over the loopback nacosmock, seeded
	// with the downtime drift (the state spotter would have written before
	// it went down, plus one console edit).
	nacosServer := nacosmock.Start()
	defer nacosServer.Close()
	nacosSink, err := nacos.NewHTTPCompatSink(nacosServer.URL(), nil)
	if err != nil {
		t.Fatalf("nacos.NewHTTPCompatSink() error = %v", err)
	}

	// The live pod's steady registration (what spotter itself wrote).
	liveHost := nacosmock.Host{
		IP: "10.0.0.1", Port: 7096, Enabled: true, Ephemeral: false,
		Metadata: map[string]string{
			"instanceId": "pod-live", "envType": "test", "envGroup": "7",
			"reversion": "42", "status": "1", "state": "running",
			"idc": "", "cpu": "0", "version": "v1", "schemaVersion": "1",
		},
	}
	// The ghost: a pod deleted while spotter was down. Its registration
	// survives (persistent), the local informer list no longer has it.
	ghostHost := nacosmock.Host{
		IP: "10.0.0.2", Port: 7096, Enabled: true, Ephemeral: false,
		Metadata: map[string]string{
			"instanceId": "pod-ghost", "envType": "test", "envGroup": "7",
			"reversion": "42", "status": "1", "state": "running",
			"idc": "", "cpu": "0", "version": "v1", "schemaVersion": "1",
		},
	}
	// The console-disabled healthy instance: an operator drained it during
	// the incident; the metadata still says status "1" but the wire enabled
	// is false. The reconcile must re-assert the k8s truth (enabled=true).
	drainedHost := nacosmock.Host{
		IP: "10.0.0.3", Port: 7096, Enabled: false, Ephemeral: false,
		Metadata: map[string]string{
			"instanceId": "pod-drained", "envType": "test", "envGroup": "7",
			"reversion": "42", "status": "1", "state": "running",
			"idc": "", "cpu": "0", "version": "v1", "schemaVersion": "1",
		},
	}
	nacosServer.SetInstances([]nacosmock.Host{liveHost, ghostHost, drainedHost}, "DEFAULT_GROUP", "pay-user", "k8s")

	// --- The one-sink fan-out used by the active Nacos-only server graph.
	fanout, err := worker.NewFanoutSink(nil,
		worker.NamedSink{Name: nacos.SinkName, Sink: nacosSink},
	)
	if err != nil {
		t.Fatalf("worker.NewFanoutSink() error = %v", err)
	}
	if err := fanout.SetReconcileSource(nacos.SinkName); err != nil {
		t.Fatalf("fanout.SetReconcileSource(nacos) error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, fanout, nil, nil)
	if err != nil {
		t.Fatalf("worker.NewResourceWorker() error = %v", err)
	}

	// --- Read the designated catalog view through the real worker/fanout.
	list, err := w.GetAll([]int32{1, 2}, "k8s")
	if err != nil {
		t.Fatalf("worker.GetAll([1,2], k8s) error = %v, want nil (the designated nacos view)", err)
	}
	ids := map[string]bool{}
	enabledByID := map[string]bool{}
	for _, ins := range list.Instance {
		ids[ins.InstanceId] = true
		enabledByID[ins.InstanceId] = ins.Enabled
	}
	// The catalog view serves ALL THREE — including the console-disabled
	// one (the instance/list view would hide it; the diff would then
	// misread it as absent).
	for _, want := range []string{"pod-live", "pod-ghost", "pod-drained"} {
		if !ids[want] {
			t.Fatalf("the designated view is missing %s (the catalog must serve disabled hosts too); got %v", want, ids)
		}
	}
	if enabledByID["pod-drained"] {
		t.Fatal("the console-disabled instance reconstructed as enabled=true; the wire enabled=false must be honored")
	}
	if !enabledByID["pod-live"] {
		t.Fatal("the live instance reconstructed as enabled=false; the steady registration must reconstruct enabled")
	}

	// --- Script the two provider-equivalent heal outputs. The value of this
	// test is the real worker/fanout/Nacos transport and final-state assertion,
	// not provider comparison policy.
	local := []*v2.Instance{
		{InstanceId: "pod-live", AppCode: "pay-user", Ip: "10.0.0.1",
			Ports: []*v2.PortInfo{{Port: 7096}}, Provider: "k8s", Cluster: "",
			EnvType: "test", EnvGroup: "7", State: "running", Status: 1,
			Enabled: true, Reversion: 42, Version: "v1"},
		{InstanceId: "pod-drained", AppCode: "pay-user", Ip: "10.0.0.3",
			Ports: []*v2.PortInfo{{Port: 7096}}, Provider: "k8s", Cluster: "",
			EnvType: "test", EnvGroup: "7", State: "running", Status: 1,
			Enabled: true, Reversion: 42, Version: "v1"},
	}
	ghostOffline := &v2.Instance{
		InstanceId: "pod-ghost", AppCode: "pay-user", Ip: "10.0.0.2",
		Ports: []*v2.PortInfo{{Port: 7096}}, Provider: "k8s", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "terminated", Status: 3,
		Enabled: false, Reversion: 42, Version: "v1",
	}
	// The k8s compare's case-3 shape: remote-only → status 3 (deregister);
	// the case-1 Enabled diff → the local truth (the re-enable push).
	w.Handle(&worker.Event{Trigger: time.Now().UnixNano(), Data: []*v2.Instance{ghostOffline}, Operate: worker.OperateTypeSync})
	w.Handle(&worker.Event{Trigger: time.Now().UnixNano(), Data: []*v2.Instance{local[1]}, Operate: worker.OperateTypeSync})

	// --- Assert convergence on the nacos state.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(nacosServer.Instances("pay-user", "k8s")) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	instances := nacosServer.Instances("pay-user", "k8s")
	if len(instances) != 2 {
		t.Fatalf("pay-user/k8s instances after the boot heal = %d, want 2 (the ghost deregistered); state = %+v", len(instances), instances)
	}
	for _, stored := range instances {
		if stored.IP == "10.0.0.3" && !stored.Enabled {
			t.Fatalf("the console-disabled instance was not re-enabled by the reconcile push (the enabled-flip heal); stored = %+v", stored)
		}
		if stored.IP == "10.0.0.2" {
			t.Fatalf("the ghost survived the boot heal; stored = %+v", stored)
		}
	}
}

// TestE2ENacosCanonicalFullFieldDriftRoundTrip proves the E2E transport
// precondition of full-field reconcile: all canonical fields survive a Nacos
// write/read, an out-of-band labels/images/ports/source mutation is detected
// even at equal Reversion, and one local re-publication restores equality.
func TestE2ENacosCanonicalFullFieldDriftRoundTrip(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	sink, err := nacos.NewHTTPCompatSink(server.URL(), nil)
	if err != nil {
		t.Fatalf("nacos.NewHTTPCompatSink: %v", err)
	}
	defer func() { _ = sink.Close() }()

	want := &instance.Instance{
		SourceKey: "cluster-a/uid-a", SourceCluster: "cluster-a",
		InstanceId: "pod-a", AppCode: "pay-user", Provider: "k8s",
		Ip: "10.0.0.1", Ports: []*instance.PortInfo{{Name: "http", Protocol: "http", Port: 8080, ServicePort: 18080}},
		EnvCode: "test#blue", EnvType: "test", EnvGroup: "blue", Cluster: "edge",
		Version: "v1", Enabled: true, State: instance.InstanceStateRunning,
		HealthState: "ready", Status: instance.InstanceStatusOnline, Reversion: 42,
		Label: map[string]string{"team": "discovery", "zone": "a"}, Hostname: "pod-a",
		Cpu: 2.5, Memory: 256, Disk: 10, Os: "linux", Image: map[string]string{"app": "repo/pay:v1"}, Idc: "idc-a",
	}
	if err := sink.Push(time.Now().UnixNano(), []*instance.Instance{want}); err != nil {
		t.Fatalf("initial canonical push: %v", err)
	}
	read := func() *instance.Instance {
		list, err := sink.GetAll([]int32{instance.InstanceStatusOnline}, "k8s")
		if err != nil {
			t.Fatalf("GetAll canonical view: %v", err)
		}
		if len(list.Instance) != 1 {
			t.Fatalf("canonical view entries = %d, want 1", len(list.Instance))
		}
		return list.Instance[0]
	}
	if got := read(); !instance.EqualNacosReconcile(want, got) {
		t.Fatalf("initial round-trip lost fields: got=%s want=%s", instance.CanonicalPayload(got), instance.CanonicalPayload(want))
	}

	stored := server.Instances("pay-user", "k8s")
	if len(stored) != 1 {
		t.Fatalf("stored hosts = %d, want 1", len(stored))
	}
	drifted := *want
	drifted.Label = map[string]string{"team": "tampered", "zone": "a"}
	drifted.Image = map[string]string{"app": "repo/pay:tampered"}
	drifted.Ports = []*instance.PortInfo{{Name: "http", Protocol: "http", Port: 9090, ServicePort: 19090}}
	drifted.SourceKey = "cluster-a/forged"
	driftedMetadata := stored[0].Metadata
	driftedMetadata["spotter.instance"] = instance.CompressedCanonicalPayload(&drifted)
	server.SetInstances([]nacosmock.Host{{
		InstanceID: stored[0].InstanceID, IP: stored[0].IP, Port: stored[0].Port,
		Enabled: stored[0].Enabled, Healthy: true, Ephemeral: stored[0].Ephemeral,
		ClusterName: stored[0].ClusterName, ServiceName: stored[0].ServiceName,
		Metadata: driftedMetadata,
	}}, "DEFAULT_GROUP", "pay-user", "k8s")
	if got := read(); !instance.DiffNacosReconcile(want, got) {
		t.Fatal("equal-Reversion canonical field drift was not detected")
	}

	if err := sink.Push(time.Now().UnixNano(), []*instance.Instance{want}); err != nil {
		t.Fatalf("heal canonical drift: %v", err)
	}
	if got := read(); !instance.EqualNacosReconcile(want, got) {
		t.Fatalf("canonical drift did not heal: got=%s want=%s", instance.CanonicalPayload(got), instance.CanonicalPayload(want))
	}
}
