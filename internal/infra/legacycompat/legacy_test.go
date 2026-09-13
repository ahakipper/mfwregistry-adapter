package legacycompat

import (
	"testing"

	"spotter/config"
)

func TestLegacyAdaptersAreNilSafeAndCopyMutableConfig(t *testing.T) {
	ResetAccessCounts()
	t.Cleanup(ResetAccessCounts)
	if Logger() == nil || Notifier() == nil {
		t.Fatal("legacy adapters must never be nil")
	}
	// No InitNoticeClient call is intentional: a compatibility notifier must
	// not panic when a legacy caller forgot global initialization.
	Notifier().Notify("title", "content")

	config.PushAppCodes = []string{"one"}
	got := PushAppCodes()
	got[0] = "mutated"
	if config.PushAppCodes[0] != "one" {
		t.Fatalf("PushAppCodes returned aliased slice: %v", config.PushAppCodes)
	}
	config.EtcdEndpoints = []string{"127.0.0.1:2379"}
	endpoints, _, _, _, _ := EtcdConfig()
	endpoints[0] = "mutated"
	if config.EtcdEndpoints[0] != "127.0.0.1:2379" {
		t.Fatalf("EtcdConfig returned aliased slice: %v", config.EtcdEndpoints)
	}
	counts := AccessCountsSnapshot()
	if counts.Reads == 0 {
		t.Fatal("compatibility adapters recorded no reads")
	}
	if counts.Writes != 0 {
		t.Fatalf("compatibility adapter reads unexpectedly recorded writes: %+v", counts)
	}
}

func TestRecordWriteTracksLegacyBridgeMutation(t *testing.T) {
	ResetAccessCounts()
	t.Cleanup(ResetAccessCounts)
	RecordWrite()
	if got := AccessCountsSnapshot().Writes; got != 1 {
		t.Fatalf("writes = %d, want 1", got)
	}
}
