package consul

import (
	"context"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/internal/ports"
	"testing"
)

type depsConsulLogger struct{ ports.NopLogger }
type depsConsulNotifier struct{}

func (*depsConsulNotifier) Notify(string, string) {}

func TestConsulProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	logger := depsConsulLogger{}
	notifier := &depsConsulNotifier{}
	p, err := NewConsulProviderWithDeps(context.Background(), &fakeWorker{}, 23, []string{"http://127.0.0.1:8500"}, logger, notifier)
	if err != nil {
		t.Fatalf("constructor error: %v", err)
	}
	c := p.(*consul)
	if c.logger != logger || c.notifier != notifier || c.interval != 23 {
		t.Fatalf("explicit dependencies not retained")
	}
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}
