package consul

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"spotter/internal/ports"
	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/providers"
	workerpkg "spotter/pkg/worker"
)

func TestNormalizeSourcesTrimsAddressesAndDerivesStableID(t *testing.T) {
	sources, err := NormalizeSources([]ConsulSource{{Addresses: []string{"  consul-a:8500 ", "consul-a:8500", "consul-b:8500"}}})
	if err != nil {
		t.Fatalf("NormalizeSources() error = %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("normalized source count = %d, want 1", len(sources))
	}
	if got, want := sources[0].Addresses, []string{"consul-a:8500", "consul-b:8500"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized addresses = %#v, want %#v", got, want)
	}
	if sources[0].ID == "" {
		t.Fatal("normalized source ID is empty")
	}
}

func TestNormalizeSourcesRejectsDuplicateScope(t *testing.T) {
	_, err := NormalizeSources([]ConsulSource{
		{ID: "blue", Addresses: []string{"blue:8500"}},
		{ID: " blue ", Addresses: []string{"green:8500"}},
	})
	if err == nil || !strings.Contains(err.Error(), `duplicate consul source ID "blue"`) {
		t.Fatalf("NormalizeSources() error = %v, want duplicate source ID", err)
	}
}

func TestNormalizeSourcesRejectsUnsafeNacosWireScope(t *testing.T) {
	_, err := NormalizeSources([]ConsulSource{
		{ID: "catalog/a", Addresses: []string{"a:8500"}},
		{ID: "catalog-a", Addresses: []string{"b:8500"}},
	})
	if err == nil || !strings.Contains(err.Error(), "must contain only letters") {
		t.Fatalf("NormalizeSources() error = %v, want unsafe Nacos wire-scope rejection", err)
	}
}

func TestNewConsulProviderWithSourceIDRejectsUnsafeWireScope(t *testing.T) {
	_, err := NewConsulProviderWithSourceID(context.Background(), &fakeWorker{}, 60, []string{"catalog:8500"}, "catalog/a", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "must contain only") {
		t.Fatalf("unsafe source ID error = %v, want wire-safe validation error", err)
	}
}

func TestNewConsulProvidersWithSourcesKeepsProviderScopesIndependent(t *testing.T) {
	providersList, err := NewConsulProvidersWithSources(context.Background(), &fakeWorker{}, 60, []ConsulSource{
		{ID: "consul-blue", Addresses: []string{"blue:8500"}},
		{ID: "consul-green", Addresses: []string{"green:8500"}},
	}, nil, nil)
	if err != nil {
		t.Fatalf("NewConsulProvidersWithSources() error = %v", err)
	}
	if len(providersList) != 2 {
		t.Fatalf("provider count = %d, want 2", len(providersList))
	}
	blue, ok := providersList[0].(*consul)
	if !ok {
		t.Fatalf("provider[0] type = %T, want *consul", providersList[0])
	}
	green, ok := providersList[1].(*consul)
	if !ok {
		t.Fatalf("provider[1] type = %T, want *consul", providersList[1])
	}
	if blue.sourceCluster != "consul-blue" || green.sourceCluster != "consul-green" {
		t.Fatalf("source clusters = %q/%q, want consul-blue/consul-green", blue.sourceCluster, green.sourceCluster)
	}
	if blue.clientFactory == green.clientFactory || blue.monitor == green.monitor || blue.cache == green.cache {
		t.Fatal("multiple sources share a client factory, monitor, or cache")
	}

	// The same Consul Service.ID and InstanceId are valid in independent
	// catalogs. Source-qualified keys keep their cache and Nacos scopes apart.
	entry := healthyEntry("shared-service", "10.0.0.7", 11)
	blueInstance, err := convertInstanceForSource(entry, blue.sourceCluster)
	if err != nil {
		t.Fatalf("blue conversion error = %v", err)
	}
	greenInstance, err := convertInstanceForSource(entry, green.sourceCluster)
	if err != nil {
		t.Fatalf("green conversion error = %v", err)
	}
	if blueInstance.InstanceId != greenInstance.InstanceId || blueInstance.SourceKey == greenInstance.SourceKey || blueInstance.SourceCluster == greenInstance.SourceCluster {
		t.Fatalf("same-name instances were not source-qualified: blue=%#v green=%#v", blueInstance, greenInstance)
	}
	blue.cache.ReplaceOrInsert(blueInstance)
	green.cache.ReplaceOrInsert(greenInstance)
	if got := len(blue.cache.List()); got != 1 {
		t.Fatalf("blue cache entries = %d, want 1", got)
	}
	if got := len(green.cache.List()); got != 1 {
		t.Fatalf("green cache entries = %d, want 1", got)
	}
	if blue.cache.Get(providers.IdentityKey(greenInstance)) != nil || green.cache.Get(providers.IdentityKey(blueInstance)) != nil {
		t.Fatal("source cache lookup crossed provider boundaries")
	}
}

func TestNewConsulProvidersWithSourcesRejectsEmptySet(t *testing.T) {
	_, err := NewConsulProvidersWithSources(context.Background(), &fakeWorker{}, 60, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "no consul sources configured") {
		t.Fatalf("NewConsulProvidersWithSources() error = %v, want empty source error", err)
	}
}

func TestNewConsulProvidersWithSourcesCleansUpEarlierProvidersOnFailure(t *testing.T) {
	var calls int
	var first *consul
	construct := func(ctx context.Context, worker workerpkg.Worker, pushInterval int, source ConsulSource, logger ports.Logger, notifier ports.Notifier) (providers.Provider, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("injected source construction failure")
		}
		provider, err := NewConsulProviderWithSourceID(ctx, worker, pushInterval, source.Addresses, source.ID, logger, notifier)
		if err == nil {
			first = provider.(*consul)
		}
		return provider, err
	}

	got, err := newConsulProvidersWithConstructor(context.Background(), &fakeWorker{}, 60, []ConsulSource{
		{ID: "consul-blue", Addresses: []string{"blue:8500"}},
		{ID: "consul-green", Addresses: []string{"green:8500"}},
	}, nil, nil, construct)
	if err == nil || !strings.Contains(err.Error(), "injected source construction failure") {
		t.Fatalf("NewConsulProvidersWithSources() = providers %v, error %v; want injected failure", got, err)
	}
	if first == nil || !first.stopped {
		t.Fatalf("first provider cleanup state = %#v, want stopped", first)
	}
}

func TestConsulFullAndIncrementalEventsUseSourceScopes(t *testing.T) {
	blueWorker := &fakeWorker{}
	greenWorker := &fakeWorker{}
	blueProvider, err := NewConsulProviderWithSourceID(context.Background(), blueWorker, 60, []string{"blue:8500"}, "consul-blue", nil, nil)
	if err != nil {
		t.Fatalf("blue provider: %v", err)
	}
	greenProvider, err := NewConsulProviderWithSourceID(context.Background(), greenWorker, 60, []string{"green:8500"}, "consul-green", nil, nil)
	if err != nil {
		t.Fatalf("green provider: %v", err)
	}
	blue := blueProvider.(*consul)
	green := greenProvider.(*consul)
	defer blue.shutdown()
	defer green.shutdown()

	entry := healthyEntry("same-id", "10.0.0.9", 22)
	blueInstance, err := convertInstanceForSource(entry, blue.sourceCluster)
	if err != nil {
		t.Fatalf("blue conversion: %v", err)
	}
	greenInstance, err := convertInstanceForSource(entry, green.sourceCluster)
	if err != nil {
		t.Fatalf("green conversion: %v", err)
	}
	blue.monitor = nil
	green.monitor = nil
	blue.cache.ReplaceOrInsert(blueInstance)
	green.cache.ReplaceOrInsert(greenInstance)
	blue.emitSyncAll()
	green.emitSyncAll()
	blue.buildAndSendEvent(blueInstance)
	green.buildAndSendEvent(greenInstance)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(blueWorker.syncAllEvents()) == 1 && len(greenWorker.syncAllEvents()) == 1 && len(blueWorker.handleSnapshot()) >= 2 && len(greenWorker.handleSnapshot()) >= 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	blueEvents := blueWorker.handleSnapshot()
	greenEvents := greenWorker.handleSnapshot()
	if len(blueEvents) < 2 || len(greenEvents) < 2 {
		t.Fatalf("event counts = %d/%d, want full + incremental per source", len(blueEvents), len(greenEvents))
	}
	if blueEvents[0].Scope != "consul-blue" || greenEvents[0].Scope != "consul-green" {
		t.Fatalf("full event scopes = %q/%q", blueEvents[0].Scope, greenEvents[0].Scope)
	}
	if blueEvents[0].BatchID == greenEvents[0].BatchID {
		t.Fatalf("full batch IDs collide: %q", blueEvents[0].BatchID)
	}
	if blueEvents[1].Scope != "consul-blue" || greenEvents[1].Scope != "consul-green" {
		t.Fatalf("incremental event scopes = %q/%q", blueEvents[1].Scope, greenEvents[1].Scope)
	}
	if blueEvents[1].Identity == "" || greenEvents[1].Identity == "" || blueEvents[1].Identity == greenEvents[1].Identity {
		t.Fatalf("incremental event identities = %q/%q", blueEvents[1].Identity, greenEvents[1].Identity)
	}
}

func TestSourceScopedRemoteRejectsOtherCatalogAndLegacyRecords(t *testing.T) {
	blue := &consul{sourceCluster: "consul-blue", acceptLegacyRemote: false}
	list := &sv.InstanceList{Instance: []*sv.Instance{
		{SourceCluster: "consul-blue", SourceKey: "consul-blue/shared", InstanceId: "shared"},
		{SourceCluster: "consul-green", SourceKey: "consul-green/shared", InstanceId: "shared"},
		{Provider: providers.ProviderEcs, InstanceId: "legacy"},
	}}
	filtered := blue.sourceScopedRemote(list)
	if len(filtered.Instance) != 1 || filtered.Instance[0].SourceCluster != "consul-blue" {
		t.Fatalf("filtered remote = %#v, want only blue source", filtered.Instance)
	}
}
