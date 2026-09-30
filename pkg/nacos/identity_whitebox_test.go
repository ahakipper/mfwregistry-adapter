package nacos

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf16"

	"spotter/internal/domain/instance"
)

func TestNacosMetadataRoundTripsLabelsAndReversion(t *testing.T) {
	original := &instance.Instance{
		SourceKey: "cluster-a/uid-42", SourceCluster: "cluster-a", InstanceId: "pod-a",
		Level: "gold", Ports: []*instance.PortInfo{{Name: "http", Protocol: "http", Port: 8080, ServicePort: 18080}},
		Ip: "10.0.0.1", EnvCode: "test#blue", EnvType: "test", EnvGroup: "blue", Cluster: "edge",
		Version: "v7", Enabled: true, State: "running", HealthState: "ready", AppCode: "pay-user",
		Provider: "k8s", Label: map[string]string{"app": "pay-user", "custom": "kept", "deploy-id": "d-1"},
		Hostname: "pod-a", Cpu: 2.5, Memory: 256, Disk: 10, Os: "linux",
		Image: map[string]string{"application": "repo/app:v7"}, Idc: "idc-a", Reversion: 42, Status: 1,
	}
	metadata := metadataOf(original)
	if metadata["spotter.instance"] == "" {
		t.Fatal("metadata missing complete spotter.instance payload")
	}
	encodedMetadata, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("json.Marshal(metadata) error = %v", err)
	}
	if len(encodedMetadata) >= 1024 {
		t.Fatalf("serialized metadata length=%d, want less than Nacos 1024-byte limit", len(encodedMetadata))
	}
	got := reconstruct(original.AppCode, Host{IP: original.Ip, Port: 8080, ClusterName: "k8s", Enabled: true, Metadata: metadata})
	if got.Reversion != original.Reversion || got.Label["custom"] != "kept" || got.Ports[0].ServicePort != 18080 || got.Image["application"] != "repo/app:v7" {
		t.Fatalf("full metadata round-trip lost fields: got=%#v want=%#v", got, original)
	}
	if got.SourceKey != original.SourceKey || got.SourceCluster != original.SourceCluster || got.EnvCode != original.EnvCode || got.Cluster != original.Cluster || got.HealthState != original.HealthState || got.Disk != original.Disk || got.Os != original.Os {
		t.Fatalf("full metadata round-trip lost identity/resource fields: got=%#v want=%#v", got, original)
	}
}

func TestNacosMetadataRejectsHighEntropyPayloadBeforeWrite(t *testing.T) {
	original := &instance.Instance{
		SourceKey: "cluster-a/uid-oversized", SourceCluster: "cluster-a", InstanceId: "pod-large",
		AppCode: "pay-user", Ip: "10.0.0.1", EnvType: "test", State: "running",
		Provider: "k8s", Reversion: 42, Status: instance.InstanceStatusOnline,
		Label: map[string]string{}, Image: map[string]string{"application": "registry.example.com/" + fmt.Sprintf("%0100d", 7)},
	}
	for i := 0; i < 80; i++ {
		original.Label[fmt.Sprintf("entropy-%03d", i)] = fmt.Sprintf("value-%03d-abcdefghijklmnopqrstuvwxyz-0123456789", i)
	}
	metadata := metadataOf(original)
	var tooLarge *MetadataTooLargeError
	if err := validateMetadataSize(metadata); !errors.As(err, &tooLarge) {
		t.Fatalf("validateMetadataSize() error = %v, want MetadataTooLargeError", err)
	}
	if tooLarge.Length <= MaxNacosMetadataLength || tooLarge.Limit != MaxNacosMetadataLength {
		t.Fatalf("metadata size error = %+v, want length > %d", tooLarge, MaxNacosMetadataLength)
	}
}

func TestNacosMetadataLimitUsesExactUTF16Boundary(t *testing.T) {
	accepted := map[string]string{"k": strings.Repeat("a", MaxNacosMetadataLength-1)}
	if err := validateMetadataSize(accepted); err != nil {
		t.Fatalf("ASCII metadata at exact limit rejected: %v", err)
	}
	rejected := map[string]string{"k": strings.Repeat("a", MaxNacosMetadataLength)}
	var tooLarge *MetadataTooLargeError
	if err := validateMetadataSize(rejected); !errors.As(err, &tooLarge) {
		t.Fatalf("ASCII metadata over exact limit error = %v, want MetadataTooLargeError", err)
	}
	if tooLarge.Length != MaxNacosMetadataLength+1 {
		t.Fatalf("ASCII over-limit length = %d, want %d", tooLarge.Length, MaxNacosMetadataLength+1)
	}

	// Nacos validates Java String.length, so one supplementary Unicode code
	// point consumes two UTF-16 code units rather than one Go rune.
	unicodeValue := strings.Repeat("🙂", (MaxNacosMetadataLength-2)/2) + "a"
	if got := len(utf16.Encode([]rune(unicodeValue))); got != MaxNacosMetadataLength-1 {
		t.Fatalf("test Unicode UTF-16 length = %d, want %d", got, MaxNacosMetadataLength-1)
	}
	if err := validateMetadataSize(map[string]string{"k": unicodeValue}); err != nil {
		t.Fatalf("Unicode metadata at exact UTF-16 limit rejected: %v", err)
	}
	if err := validateMetadataSize(map[string]string{"k": unicodeValue + "🙂"}); !errors.As(err, &tooLarge) {
		t.Fatalf("Unicode metadata over UTF-16 limit error = %v, want MetadataTooLargeError", err)
	}
}

func FuzzNacosMetadataSizeValidationNeverPanics(f *testing.F) {
	f.Add("sourceKey", "cluster-a/uid-1")
	f.Add("🙂", strings.Repeat("x", 1024))
	f.Fuzz(func(t *testing.T, key, value string) {
		if key == "" {
			key = "k"
		}
		_ = validateMetadataSize(map[string]string{key: value})
	})
}

func TestReconstructForSDKSurfacesDisabledUnhealthyWireDriftWithoutSteadyLoop(t *testing.T) {
	for _, domainEnabled := range []bool{false, true} {
		local := &instance.Instance{
			InstanceId: "unhealthy", AppCode: "pay-user", Provider: "k8s",
			Ip: "10.0.0.2", Ports: []*instance.PortInfo{{Port: 8080}},
			Enabled: domainEnabled, Status: instance.InstanceStatusUnhealthy,
			State: instance.InstanceStateProbing, Reversion: 7,
		}
		metadata := metadataOf(local)
		steady := reconstructForTransport("pay-user", Host{
			IP: "10.0.0.2", Port: 8080, ClusterName: "k8s", Enabled: true, Metadata: metadata,
		}, true)
		if instance.DiffNacosReconcile(local, steady) {
			t.Fatalf("domain Enabled=%t: expected SDK unhealthy wire shape caused a loop", domainEnabled)
		}
		drifted := reconstructForTransport("pay-user", Host{
			IP: "10.0.0.2", Port: 8080, ClusterName: "k8s", Enabled: false, Metadata: metadata,
		}, true)
		if !instance.DiffNacosReconcile(local, drifted) {
			t.Fatalf("domain Enabled=%t: disabled SDK unhealthy host was not surfaced as drift", domainEnabled)
		}
		if drifted.Reversion != 0 {
			t.Fatalf("domain Enabled=%t: transport drift sentinel Reversion=%d, want 0", domainEnabled, drifted.Reversion)
		}
		combinedCanonicalDrift := *local
		combinedCanonicalDrift.Enabled = !local.Enabled
		combined := reconstructForTransport("pay-user", Host{
			IP: "10.0.0.2", Port: 8080, ClusterName: "k8s", Enabled: false,
			Metadata: metadataOf(&combinedCanonicalDrift),
		}, true)
		if !instance.DiffNacosReconcile(local, combined) || combined.Reversion != 0 {
			t.Fatalf("domain Enabled=%t: combined canonical/wire drift canceled: reconstructed=%+v", domainEnabled, combined)
		}
		compat := reconstructForTransport("pay-user", Host{
			IP: "10.0.0.2", Port: 8080, ClusterName: "k8s", Enabled: false, Metadata: metadata,
		}, false)
		if instance.DiffNacosReconcile(local, compat) {
			t.Fatalf("domain Enabled=%t: HTTP compatibility status-2 shape should retain its historical projection", domainEnabled)
		}
	}
}

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
