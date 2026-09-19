//go:build e2e

package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	"spotter/internal/composition"
	"spotter/internal/domain/instance"
	infraconfig "spotter/internal/infra/config"
	"spotter/internal/testkit/etcdmock"
	"spotter/internal/testkit/fakes"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/nacos"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

// TestE2ENacosOnlyCompositionPublishesWithoutAtlas exercises the shipped
// Config.Load -> composition.Build -> NewServerFromDeps chain before replacing
// only the external provider with a deterministic fixture. The offline Nacos
// transport is the explicit HTTP compatibility fixture; guarded nacos_real
// tests cover the same Sink contract over the official SDK/Nacos 3 target.
// The active graph must publish/read/shut down without invoking Atlas.
func TestE2ENacosOnlyCompositionPublishesWithoutAtlas(t *testing.T) {
	remote := nacosmock.Start()
	defer remote.Close()
	etcd, err := etcdmock.Start()
	if err != nil {
		t.Fatalf("etcdmock.Start: %v", err)
	}
	defer etcd.Close()

	cfg, err := infraconfig.Load("test", infraconfig.Flags{
		Providers:         []string{"k8s"},
		NacosAddr:         remote.URL(),
		NacosTransport:    string(nacos.TransportHTTPCompat),
		EtcdEndpointsFlag: etcd.ClientEndpoints(),
		MetricsAddr:       "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	if cfg.EnableAtlasCompatibility || cfg.ReconcileSource != nacos.SinkName {
		t.Fatalf("resolved active graph = atlas_compat:%t reconcile:%q, want false/%q", cfg.EnableAtlasCompatibility, cfg.ReconcileSource, nacos.SinkName)
	}
	runtime, err := composition.Build(cfg, composition.Deps{
		Logger:   &fakes.FakeLogger{},
		Notifier: recordingNotifier{},
		Metrics:  fakes.NewFakeMetricsRecorder(),
		LocalIP:  func() (string, error) { return "127.0.0.1", nil },
	})
	if err != nil {
		t.Fatalf("composition.Build: %v", err)
	}
	s, err := NewServerFromDeps(runtime)
	if err != nil {
		t.Fatalf("NewServerFromDeps: %v", err)
	}
	defer s.stopElectorFunc()

	var captured worker.Worker
	providerStarted := make(chan struct{})
	atlasDialCalls := 0
	s.isLeader = true
	s.dialDiscovery = func(context.Context) (*discoverycenter.Client, error) {
		atlasDialCalls++
		return nil, errors.New("excluded Atlas path was dialed")
	}
	s.initializeProviders = func(ctx context.Context, w worker.Worker) ([]providers.Provider, error) {
		captured = w
		return []providers.Provider{&capturedWorkerProvider{ctx: ctx, started: providerStarted}}, nil
	}

	result := make(chan error, 1)
	go func() { result <- s.startProviders() }()
	select {
	case <-providerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Nacos-only server did not reach provider startup")
	}
	if atlasDialCalls != 0 {
		t.Fatalf("Atlas dial calls = %d, want 0", atlasDialCalls)
	}

	want := &instance.Instance{
		SourceKey: "cluster-a/uid-a", SourceCluster: "cluster-a",
		InstanceId: "pod-a", AppCode: "pay-user", Provider: "k8s",
		Ip: "10.0.0.1", Ports: []*instance.PortInfo{{Name: "http", Port: 8080}},
		EnvType: "test", Enabled: true, State: instance.InstanceStateRunning,
		Status: instance.InstanceStatusOnline, Reversion: 42,
		Label: map[string]string{"team": "discovery"}, Image: map[string]string{"app": "repo/pay:v1"},
	}
	captured.Handle(&worker.Event{
		Trigger: time.Now().UnixNano(), Operate: worker.OperateTypeSyncAll,
		Scope: "k8s", BatchID: worker.FullBatchID("k8s", []*instance.Instance{want}),
		Sequence: 42, Data: []*instance.Instance{want},
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		got := remote.Instances("pay-user", "k8s")
		if len(got) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Nacos did not receive the full snapshot: instances=%v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	view, err := captured.GetAll([]int32{instance.InstanceStatusOnline}, "k8s")
	if err != nil {
		t.Fatalf("Nacos authoritative GetAll: %v", err)
	}
	if len(view.Instance) != 1 || instance.CanonicalPayload(want) != instance.CanonicalPayload(view.Instance[0]) {
		t.Fatalf("authoritative view = %#v, want canonical instance %#v", view.Instance, want)
	}

	s.Stop()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Nacos-only server shutdown returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Nacos-only server did not stop")
	}
}
