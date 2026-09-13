package consul

import (
	legacycompat "spotter/internal/infra/legacycompat"
	"testing"
)

func TestConsulProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	c := &consul{}
	c.ensureDeps()
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}
