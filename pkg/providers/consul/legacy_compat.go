package consul

import (
	"context"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

func NewConsulProvider(ctx context.Context, w worker.Worker, pushInterval int, addrs []string) (providers.Provider, error) {
	return NewConsulProviderWithDeps(ctx, w, pushInterval, addrs, legacycompat.Logger(), legacycompat.Notifier())
}
