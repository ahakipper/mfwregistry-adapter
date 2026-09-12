package nacos

import (
	"testing"

	"spotter/internal/domain/instance"
)

func TestNacosMetadataSourceIdentityRoundTrip(t *testing.T) {
	original := &instance.Instance{
		SourceKey: "cluster-a/uid-42", SourceCluster: "cluster-a",
		InstanceId: "pod-a", AppCode: "pay-user", Ip: "10.0.0.1",
		EnvType: "test", EnvGroup: "blue", State: instance.InstanceStateRunning,
		Status: instance.InstanceStatusOnline, Reversion: 42, Cpu: 2.5,
		Version: "v7", Idc: "kraken",
	}
	metadata := metadataOf(original)
	got := reconstruct(original.AppCode, Host{
		IP: original.Ip, Port: 8080, ClusterName: "k8s", Enabled: true,
		Metadata: metadata,
	})
	if got.SourceKey != original.SourceKey || got.SourceCluster != original.SourceCluster {
		t.Fatalf("source identity round-trip = %q/%q, want %q/%q", got.SourceKey, got.SourceCluster, original.SourceKey, original.SourceCluster)
	}
	if got.InstanceId != original.InstanceId || got.Provider != "k8s" || got.Reversion != original.Reversion {
		t.Fatalf("reconstructed identity fields = %#v, want id/provider/reversion preserved", got)
	}
}

func TestNacosReconstructLegacyMetadataFallsBackWithoutSourceKey(t *testing.T) {
	got := reconstruct("pay-user", Host{
		IP: "10.0.0.2", Port: 8080, ClusterName: "k8s", Enabled: true,
		Metadata: map[string]string{
			"instanceId": "legacy-pod", "envType": "test", "schemaVersion": metadataSchemaVersion,
		},
	})
	if got.SourceKey != "" || got.SourceCluster != "" {
		t.Fatalf("legacy reconstruction source fields = %q/%q, want empty fallback", got.SourceKey, got.SourceCluster)
	}
	if got.InstanceId != "legacy-pod" || got.Provider != "k8s" {
		t.Fatalf("legacy reconstruction identity = %#v, want legacy id and cluster provider", got)
	}
}
