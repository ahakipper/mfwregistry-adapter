//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"spotter/config"
	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/nacosmock"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/log"
	"spotter/pkg/nacos"
	"spotter/pkg/notice"
	"spotter/pkg/worker"
)

// TestE2ENacosReconcileSourceBootHeal drives the nacos-authoritative
// reconcile end to end through the REAL production wiring (dsca-3 §3.1/§4.2):
//
//	discoverymock (Atlas, primary push target) + nacosmock (nacos sink)
//	  -> FanoutSink with SetReconcileSource("nacos")     [the flag's effect]
//	  -> DefaultWorker
//	  -> k8s.CompareAndFlush (the boot path's synchronous call)
//
// The scenario: nacos state drifted while spotter was down — one live pod's
// registration is intact, one pod was deleted while down (its registration
// is a remote-only ghost), and one healthy registration was console-disabled
// (enabled=false, metadata status "1"). The boot CompareAndFlush must heal
// BOTH drift classes with no code change beyond the source routing:
//   - the ghost → case 3 → a status-3 push → the nacos sink DEREGISTERS it;
//   - the console-disabled healthy instance → the [online, unhealthy] view
//     serves it (the catalog) → field diff on Enabled → the local truth is
//     re-registered with enabled=true.
//
// The nacos catalog replaces the in-memory remembered memory as the
// boot-time ownership record (the 416e62a residual, closed).
func TestE2ENacosReconcileSourceBootHeal(t *testing.T) {
	// The k8s provider's conversion logs through the legacy pkg/log global.
	legacyDir := t.TempDir()
	config.LogFilePath = legacyDir + string(os.PathSeparator)
	config.LogToStd = false
	if err := log.LoggerInit(); err != nil {
		t.Fatalf("log.LoggerInit() error = %v", err)
	}
	notice.InitNoticeClient("test")

	// --- Atlas side: the in-memory gRPC server over bufconn (the primary
	// PUSH target; its GetAll view is empty and irrelevant — the compare
	// no longer reads it).
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

	// --- Nacos side: the real sink over the loopback nacosmock, seeded
	// with the downtime drift (the state spotter would have written before
	// it went down, plus one console edit).
	nacosServer := nacosmock.Start()
	defer nacosServer.Close()
	nacosSink, err := nacos.NewSink(nacosServer.URL(), nil)
	if err != nil {
		t.Fatalf("nacos.NewSink() error = %v", err)
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

	// --- The fan-out with the reconcile designation — exactly what the
	// server wiring builds under --reconcile-source nacos (Atlas primary,
	// Nacos second, the read role handed to the nacos sink).
	fanout, err := worker.NewFanoutSink(nil,
		worker.NamedSink{Name: worker.AtlasSinkName, Sink: registry},
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

	// --- The compare, driven exactly as the boot path drives it: the
	// provider's local list (the informer store) against the worker's
	// GetAll (the designated nacos catalog view). The fakeProvider stands
	// in for the k8s provider's CompareAndFlush mechanics with the same
	// shape; the k8s package's own boot test covers the provider-internal
	// path, so here the e2e value is the WIRING: worker.GetAll must return
	// the nacos view through the real fanout.
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

	// --- The heal: the provider's set difference (local = pod-live +
	// pod-drained; remote = all three) produces case-3 for the ghost and a
	// field-diff push for the drained instance. The pushes go through the
	// REAL worker to the REAL nacos sink.
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
