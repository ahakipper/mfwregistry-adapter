package worker

import (
	"context"
	legacycompat "spotter/internal/infra/legacycompat"
)

func NewElector(ctx context.Context, ch chan bool) (Elector, error) {
	endpoints, cert, key, ca, campaign := legacycompat.EtcdConfig()
	return NewElectorWithDeps(ctx, ch, endpoints, cert, key, ca, campaign, legacycompat.Logger(), legacycompat.Notifier())
}
