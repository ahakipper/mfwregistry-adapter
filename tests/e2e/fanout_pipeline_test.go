//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/config"
	"spotter/internal/testkit/consulmock"
	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/log"
	"spotter/pkg/nacos"
	"spotter/pkg/notice"
	"spotter/pkg/providers/consul"
	"spotter/pkg/worker"
)

// TestE2EConsulFanoutPipeline drives the F5 multi-sink pipeline end to end:
//
//	consulmock -> NewConsulProvider -> DefaultWorker -> FanoutSink
//	                                                 |-> Atlas (DiscoveryCenter -> discoverymock)
//	                                                 |-> Nacos (pkg/nacos -> nacosmock)
//
// The consul mock serves one "microservice" endpoint; the provider's watch
// loop pushes the converted instance through the real DefaultWorker, whose
// fan-out delivers it to BOTH sinks: the discovery mock observes the
// SynInstance call and the nacos mock holds the registered persistent
// instance with the plan §7.3 field mapping. The instance list is then read
// back through the sink's own GetAll, asserting the metadata round-trip.
//
// It is bounded, fully offline and race-safe — the e2e-shaped proof of the
// §6.5 wiring (the same graph internal/server.go builds under --nacos-addr).
func TestE2EConsulFanoutPipeline(t *testing.T) {
	// The consul provider logs through the legacy pkg/log global; point the
	// log directory at a per-test temporary directory (the same guard the
	// other e2e suites use).
	legacyDir := t.TempDir()
	config.LogFilePath = legacyDir + string(os.PathSeparator)
	config.LogToStd = false
	if err := log.LoggerInit(); err != nil {
		t.Fatalf("log.LoggerInit() error = %v", err)
	}
	notice.InitNoticeClient("test")

	// --- Consul side: one microservice endpoint.
	consulServer := consulmock.Start()
	defer consulServer.Close()
	consulServer.SetServices(map[string][]string{
		"payments": {"microservice", "v2"},
	})
	consulServer.SetEntries("payments", []*api.ServiceEntry{newPaymentsEntry()})

	// --- Atlas side: in-memory gRPC server over bufconn.
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

	// --- Nacos side: the F5 sink over the loopback nacosmock.
	nacosServer := nacosmock.Start()
	defer nacosServer.Close()
	nacosSink, err := nacos.NewSink(nacosServer.URL(), nil)
	if err != nil {
		t.Fatalf("nacos.NewSink() error = %v", err)
	}

	// --- The fan-out: Atlas first (the primary), Nacos second — the same
	// declaration order internal/server.go wires under --nacos-addr.
	fanout, err := worker.NewFanoutSink(nil,
		worker.NamedSink{Name: worker.AtlasSinkName, Sink: registry},
		worker.NamedSink{Name: nacos.SinkName, Sink: nacosSink},
	)
	if err != nil {
		t.Fatalf("worker.NewFanoutSink() error = %v", err)
	}
	if names := fanout.Sinks(); len(names) != 2 || names[0] != worker.AtlasSinkName || names[1] != nacos.SinkName {
		t.Fatalf("fanout.Sinks() = %v, want [atlas nacos]", names)
	}

	// --- Worker: the real DefaultWorker over the fanout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, fanout, nil, nil)
	if err != nil {
		t.Fatalf("worker.NewResourceWorker() error = %v", err)
	}

	// --- Provider: the real consul provider, pointed at the consul mock.
	// Note: NewConsulProvider accepts pushInterval but never assigns it to
	// the provider's interval field (a pre-existing quirk outside F5's
	// scope; the field stays 0 and ProcessIntervalFullPush falls back to the
	// 21600s default), so the interval tick cannot drive the prune inside a
	// bounded test. The §7.4 SyncAll event is therefore emitted here exactly
	// as the provider's tick does (same event shape over the worker seam),
	// which exercises the same downstream path: worker.Handle(SyncAll) ->
	// FanoutSink.PushAll -> the Nacos prune sweep.
	provider, err := consul.NewConsulProvider(ctx, w, 0, []string{consulServer.Address()})
	if err != nil {
		t.Fatalf("consul.NewConsulProvider() error = %v", err)
	}

	providerDone := make(chan error, 1)
	go func() {
		providerDone <- provider.Run()
	}()

	// The watch loop notices the index change and pushes the instance
	// through the fan-out to both sinks.
	consulServer.AdvanceIndex()
	awaitSynInstance(t, discovery)

	// The Nacos sink received the same push: the persistent instance is
	// registered under the ecs cluster of the payments service with the
	// plan §7.3 mapping (provider -> clusterName, DEFAULT_GROUP, port from
	// Ports[0], the metadata round-trip fields).
	awaitNacosInstance(t, nacosServer)

	// Out-of-band drift on the Nacos side: a stale instance the sink never
	// pushed. The full-push tick's SyncAll event (§7.4) runs the PushAll
	// prune and deletes it, proving the reconcile path end to end.
	nacosServer.SetInstances([]nacosmock.Host{
		{IP: "127.0.0.9", Port: 9999, Enabled: true, Ephemeral: false,
			Metadata: map[string]string{"instanceId": "payments-ghost"}},
	}, "DEFAULT_GROUP", "payments", "ecs")

	// The §7.4 SyncAll trigger, emitted over the worker seam exactly as the
	// provider's tick emits it (same event shape, same Handle path).
	fullList := provider.GetAll()
	w.Handle(&worker.Event{Trigger: time.Now().Unix(), Data: fullList, Operate: worker.OperateTypeSyncAll})
	awaitNacosPrune(t, nacosServer)

	// The sink's own GetAll reconstructs the instance from Nacos state
	// (metadata round-trip, provider cluster filter).
	list, err := nacosSink.GetAll(nil, "ecs")
	if err != nil {
		t.Fatalf("nacosSink.GetAll(nil, ecs) error = %v", err)
	}
	if len(list.Instance) != 1 {
		t.Fatalf("nacosSink.GetAll(nil, ecs) = %d instances, want 1 (the payments-1 instance)", len(list.Instance))
	}
	got := list.Instance[0]
	if got.InstanceId != "payments-1" || got.AppCode != "payments" || got.Ip != "127.0.0.1" {
		t.Fatalf("reconstructed instance = %s/%s/%s, want payments-1/payments/127.0.0.1",
			got.InstanceId, got.AppCode, got.Ip)
	}
	if got.Provider != "ecs" || got.EnvType != "test" || got.EnvGroup != "0" {
		t.Fatalf("reconstructed provider/env = %s/%s/%s, want ecs/test/0", got.Provider, got.EnvType, got.EnvGroup)
	}

	cancel()
	select {
	case <-providerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("provider Run() did not return after context cancel")
	}
}

// awaitNacosInstance polls the nacosmock until the payments-1 instance is
// registered under the ecs cluster, then asserts the field mapping.
func awaitNacosInstance(t *testing.T, server *nacosmock.Server) {
	t.Helper()

	deadline := time.Now().Add(8 * time.Second)
	var instances []nacosmock.Instance
	for time.Now().Before(deadline) {
		instances = server.Instances("payments", "ecs")
		if len(instances) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(instances) != 1 {
		t.Fatalf("nacos payments/ecs instances = %d, want 1 (the fanned-out push)", len(instances))
	}
	stored := instances[0]
	if stored.InstanceID != "127.0.0.1#8080#ecs#DEFAULT_GROUP@@payments" {
		t.Errorf("composite id = %q, want the plan §7.3 mapping", stored.InstanceID)
	}
	if stored.Ephemeral {
		t.Errorf("instance is ephemeral, want persistent (ephemeral=false)")
	}
	if !stored.Enabled {
		t.Errorf("enabled = false, want true (online instance)")
	}
	if stored.Metadata["instanceId"] != "payments-1" {
		t.Errorf("metadata instanceId = %q, want payments-1", stored.Metadata["instanceId"])
	}
	if stored.Metadata["envType"] != "test" || stored.Metadata["envGroup"] != "0" {
		t.Errorf("metadata env = %q/%q, want test/0", stored.Metadata["envType"], stored.Metadata["envGroup"])
	}
	if stored.Metadata["status"] != "1" {
		t.Errorf("metadata status = %q, want 1 (online)", stored.Metadata["status"])
	}
}

// awaitNacosPrune polls the nacosmock until the out-of-band drift instance
// is pruned by the full-push SyncAll event, and the desired instance
// survives the sweep.
func awaitNacosPrune(t *testing.T, server *nacosmock.Server) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		instances := server.Instances("payments", "ecs")
		if len(instances) == 1 && instances[0].IP == "127.0.0.1" {
			return // the ghost is gone, the desired instance survived
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the drifted instance was not pruned within the full-push bound; instances = %+v", server.Instances("payments", "ecs"))
}
