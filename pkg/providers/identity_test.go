package providers

import (
	sv "spotter/pkg/beehive/service/v2"
	"testing"
)

func TestIdentityKeyUsesUIDAndCluster(t *testing.T) {
	a := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-a/uid-1"}}
	b := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-b/uid-1"}}
	if IdentityKey(a) == IdentityKey(b) {
		t.Fatal("source identities collided")
	}
}

func TestIdentityIndexQuarantinesAmbiguousLegacyIdentity(t *testing.T) {
	items := []*sv.Instance{{Provider: "k8s", InstanceId: "pod"}, {Provider: "k8s", InstanceId: "pod"}}
	_, ambiguous := IdentityIndex(items)
	if len(ambiguous) != 1 {
		t.Fatalf("ambiguous=%v", ambiguous)
	}
}

func TestStrictListToMapQuarantinesDuplicateSourceAndAllowsDistinctClusters(t *testing.T) {
	a := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-a/uid"}}
	b := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-b/uid"}}
	duplicate := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-a/uid"}}
	indexed, ambiguous := StrictListToMap([]*sv.Instance{a, b, duplicate})
	if len(ambiguous) != 1 || ambiguous[0] != "cluster-a/uid" {
		t.Fatalf("ambiguous = %v, want only duplicate source key", ambiguous)
	}
	if _, ok := indexed["cluster-a/uid"]; ok {
		t.Fatal("duplicate source identity remained indexed")
	}
	if got := indexed["cluster-b/uid"]; got != b {
		t.Fatalf("distinct cluster entry = %#v, want original cluster-b instance", got)
	}
}

func TestLookupIdentityLegacyFallbackRequiresUniqueWireID(t *testing.T) {
	target := &sv.Instance{Provider: "k8s", InstanceId: "legacy-pod"}
	legacy := &sv.Instance{InstanceId: "legacy-pod"}
	index := map[string]*sv.Instance{}
	if got := LookupIdentity(index, []*sv.Instance{legacy}, target); got != legacy {
		t.Fatalf("unique legacy fallback = %#v, want legacy instance", got)
	}
	other := &sv.Instance{InstanceId: "legacy-pod"}
	if got := LookupIdentity(index, []*sv.Instance{legacy, other}, target); got != nil {
		t.Fatalf("ambiguous legacy fallback = %#v, want nil", got)
	}
}

func TestLookupIdentityPrefersExplicitSourceKeyOverWireID(t *testing.T) {
	target := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-b/uid"}}
	wireMatch := &sv.Instance{InstanceId: "pod"}
	sourceMatch := &sv.Instance{Provider: "k8s", InstanceId: "pod", Label: map[string]string{"sourceKey": "cluster-b/uid"}}
	index := map[string]*sv.Instance{"cluster-b/uid": sourceMatch}
	if got := LookupIdentity(index, []*sv.Instance{wireMatch, sourceMatch}, target); got != sourceMatch {
		t.Fatalf("explicit source lookup = %#v, want source-matched instance", got)
	}
}
