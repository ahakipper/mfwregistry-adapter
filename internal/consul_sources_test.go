package internal

import (
	"context"
	"testing"

	"spotter/internal/domain/instance"
	infraconfig "spotter/internal/infra/config"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

type providerInitWorker struct{}

func (providerInitWorker) AddEventHandler(worker.OperateType, worker.EventResourceHandler) {}
func (providerInitWorker) Handle(*worker.Event)                                            {}
func (providerInitWorker) ProcessUnsynced()                                                {}
func (providerInitWorker) GetAll([]int32, string) (*instance.InstanceList, error) {
	return &instance.InstanceList{}, nil
}

func TestInitializeProvidersBuildsOneConsulProviderPerConfiguredSource(t *testing.T) {
	configured := infraconfig.Config{
		Endpoints: infraconfig.Endpoints{
			ConsulSources: []infraconfig.ConsulSource{
				{ID: "consul-blue", Addresses: []string{"blue:8500"}},
				{ID: "consul-green", Addresses: []string{"green:8500"}},
			},
		},
		Providers:       []string{providers.ProviderEcs},
		PushAllInterval: 60,
	}
	got, err := InitializeProvidersWithDeps(context.Background(), providerInitWorker{}, configured, nil, nil)
	if err != nil {
		t.Fatalf("InitializeProvidersWithDeps() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("provider count = %d, want 2", len(got))
	}
	for i, provider := range got {
		if provider == nil {
			t.Fatalf("provider[%d] is nil", i)
		}
	}
}
