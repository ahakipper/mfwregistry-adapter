package consul

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/consulmock"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/nacos"
	"spotter/pkg/worker"
)

// This runs the actual provider -> DefaultWorker -> Fanout -> orderedSink ->
// Nacos adapter graph. Loopback HTTP servers stand in for external services;
// no helper sends a manual worker event or bypasses provider conversion.
func TestConsulSourcesKeepReconcileAndEmptyPruneIsolated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blueCatalog, greenCatalog := consulmock.Start(), consulmock.Start()
	defer blueCatalog.Close()
	defer greenCatalog.Close()
	for _, catalog := range []*consulmock.Server{blueCatalog, greenCatalog} {
		catalog.SetServices(map[string][]string{"pay-user": {"microservice"}})
		catalog.SetEntries("pay-user", []*api.ServiceEntry{healthyEntry("same-id", "10.0.0.9", 22)})
	}
	remote := nacosmock.Start()
	defer remote.Close()
	sink, err := nacos.NewHTTPCompatSink(remote.URL(), nil)
	if err != nil {
		t.Fatal(err)
	}
	fanout, err := worker.NewFanoutSink(nil, worker.NamedSink{Name: nacos.SinkName, Sink: sink})
	if err != nil {
		t.Fatal(err)
	}
	defer fanout.Close()
	if err := fanout.SetReconcileSource(nacos.SinkName); err != nil {
		t.Fatal(err)
	}
	w, err := worker.NewResourceWorker(ctx, fanout, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := NewConsulProvidersWithSources(ctx, w, 21600, []ConsulSource{
		{ID: "consul-blue", Addresses: []string{blueCatalog.Address()}},
		{ID: "consul-green", Addresses: []string{greenCatalog.Address()}},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	blue, green := sources[0].(*consul), sources[1].(*consul)
	for _, source := range []*consul{blue, green} {
		source.SetNacosReconcileSource(true)
		source.emptyRetryInterval = 0
		defer source.shutdown()
		if err := source.syncInstance(); err != nil {
			t.Fatal(err)
		}
		source.emitSyncAll()
	}
	assertConsulRemoteCount(t, sink, 2)

	// A full snapshot for one source must not tombstone the other source.
	blue.emitSyncAll()
	greenEntries := []*api.ServiceEntry{healthyEntry("same-id", "10.0.0.9", 22)}
	greenEntries[0].Service.Meta["zone"] = "updated"
	greenCatalog.SetEntries("pay-user", greenEntries)
	if err := green.syncInstance(); err != nil {
		t.Fatal(err)
	}
	greenView, err := sink.GetAll(nil, "consul-green")
	if err != nil || len(greenView.Instance) != 1 || greenView.Instance[0].Label["zone"] != "updated" {
		t.Fatalf("same-revision green update after blue full: view=%#v error=%v", greenView, err)
	}

	// Provider-only equality is insufficient: compare reads both remote
	// catalogs and must filter by logical source before issuing deletes.
	blue.CompareAndFlush()
	blue.emitSyncAll() // full gate waits for accepted reconciles
	assertConsulRemoteCount(t, sink, 2)

	blueCatalog.SetEntries("pay-user", nil)
	for attempt := 0; attempt < 3; attempt++ {
		_ = blue.syncInstance()
	}
	blue.emitSyncAll()
	assertConsulRemoteCount(t, sink, 1)
	remaining, err := sink.GetAll(nil, "consul-green")
	if err != nil || len(remaining.Instance) != 1 || remaining.Instance[0].SourceCluster != "consul-green" {
		t.Fatalf("blue empty prune touched green: view=%#v error=%v", remaining, err)
	}

	// All-health removal followed by recovery may keep Service.ModifyIndex.
	// The real ordered sink must heal that equal-revision tombstone promptly.
	blueCatalog.SetEntries("pay-user", []*api.ServiceEntry{healthyEntry("same-id", "10.0.0.9", 22)})
	if err := blue.syncInstance(); err != nil {
		t.Fatal(err)
	}
	assertConsulRemoteCount(t, sink, 2)
}

func assertConsulRemoteCount(t *testing.T, sink *nacos.Sink, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		total := 0
		for _, scope := range []string{"consul-blue", "consul-green"} {
			view, err := sink.GetAll([]int32{instance.InstanceStatusOnline, instance.InstanceStatusUnhealthy}, scope)
			if err != nil {
				t.Fatal(err)
			}
			total += len(view.Instance)
		}
		if total == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("remote instances did not converge to %d", want)
}
