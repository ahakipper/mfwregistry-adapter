package k8s

import (
	"context"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

func NewK8SProvider(ctx context.Context, w worker.Worker, pushInterval int, configPath []string) (providers.Provider, error) {
	return NewK8SProviderWithDeps(ctx, w, pushInterval, configPath, legacycompat.Logger(), legacycompat.Notifier(), legacycompat.PushAppCodes())
}
