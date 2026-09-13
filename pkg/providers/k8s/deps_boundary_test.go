package k8s

import (
	"context"
	v1 "k8s.io/api/core/v1"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/internal/ports"
	"testing"
)

type depsTestLogger struct{ ports.NopLogger }
type depsTestNotifier struct{ called bool }

func (*depsTestNotifier) Notify(string, string) {}

func TestK8SProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	logger := depsTestLogger{}
	notifier := &depsTestNotifier{}
	provider, err := NewK8SProviderWithDeps(context.Background(), nil, 17, []string{"/definitely/missing/config"}, logger, notifier, []string{"explicit"})
	if err == nil || provider != nil {
		t.Fatalf("constructor result=(%v,%v), want nil provider and configuration error", provider, err)
	}
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}

func TestFormatInstanceWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	pod := &v1.Pod{}
	_ = formatInstanceWithDeps(nil, pod, []string{"explicit"}, depsTestLogger{})
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}
