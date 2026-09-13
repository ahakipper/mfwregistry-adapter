package k8s

import (
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/internal/ports"
	"testing"
)

func TestK8SProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	k := &k8s{depsConfigured: true, pushAppCodes: []string{"explicit"}, logger: ports.NopLogger{}}
	k.ensureDeps()
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}

func TestFormatInstanceWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	_ = formatInstanceWithDeps(nil, nil, []string{"explicit"}, ports.NopLogger{})
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}
