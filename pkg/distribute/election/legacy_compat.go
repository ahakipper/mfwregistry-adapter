package election

import (
	"context"
	"go.etcd.io/etcd/client/v3"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/internal/ports"
)

func NewCandidate(ctx context.Context, c *clientv3.Client, key string) (Candidate, error) {
	if key == "" {
		key = legacycompat.CampaignKey()
	}
	return NewCandidateWithClock(ctx, c, key, nil)
}
func NewCandidateWithClock(ctx context.Context, c *clientv3.Client, key string, clock ports.Clock) (Candidate, error) {
	if key == "" {
		key = legacycompat.CampaignKey()
	}
	return NewCandidateWithDeps(ctx, c, key, clock, nil, nil)
}
