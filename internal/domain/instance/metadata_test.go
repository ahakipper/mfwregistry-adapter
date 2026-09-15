package instance

import "testing"

func TestCanonicalPayloadRoundTripsAllInstanceProperties(t *testing.T) {
	original := &Instance{
		SourceKey: "cluster-a/uid-1", SourceCluster: "cluster-a", InstanceId: "pod-a",
		Level: "gold", Ports: []*PortInfo{{Name: "dubbo-7096", Protocol: "dubbo", Port: 7096, ServicePort: 17096}, {Name: "http", Protocol: "http", Port: 8080}},
		Ip: "10.0.0.7", EnvCode: "test#blue", EnvType: "test", EnvGroup: "blue", Cluster: "edge",
		Version: "v7", Enabled: true, State: "running", HealthState: "ready", AppCode: "pay-user",
		Provider: "k8s", Label: map[string]string{"app": "pay-user", "deploy-id": "d-1", "custom": "kept"},
		Hostname: "pod-a", Cpu: 1.5, Memory: 256, Disk: 10, Os: "linux",
		Image: map[string]string{"application": "repo/app:v7"}, Idc: "idc-a", Reversion: 42, Status: 1,
	}
	payload := CanonicalPayload(original)
	got, err := DecodeCanonicalPayload(payload)
	if err != nil {
		t.Fatalf("DecodeCanonicalPayload() error = %v", err)
	}
	if got.InstanceId != original.InstanceId || got.Reversion != original.Reversion || got.Status != original.Status {
		t.Fatalf("identity/version/status = %#v/%d/%d, want %#v/%d/%d", got.InstanceId, got.Reversion, got.Status, original.InstanceId, original.Reversion, original.Status)
	}
	if got.Label["custom"] != "kept" || got.Image["application"] != "repo/app:v7" {
		t.Fatalf("labels/images were not preserved: labels=%v images=%v", got.Label, got.Image)
	}
	if len(got.Ports) != 2 || got.Ports[0].ServicePort != 17096 || got.Ports[1].Port != 8080 {
		t.Fatalf("ports were not preserved: %#v", got.Ports)
	}
	if got.SourceKey != original.SourceKey || got.SourceCluster != original.SourceCluster || got.EnvCode != original.EnvCode || got.Cluster != original.Cluster || got.HealthState != original.HealthState || got.Disk != original.Disk || got.Os != original.Os {
		t.Fatalf("non-obvious fields were not preserved: got=%#v", got)
	}
	if CanonicalPayload(got) != payload {
		t.Fatalf("canonical payload was not stable after round-trip:\nfirst=%s\nsecond=%s", payload, CanonicalPayload(got))
	}
	encoded := CompressedCanonicalPayload(original)
	if len(encoded) >= len(payload) {
		t.Fatalf("compressed payload length=%d, want less than JSON length=%d", len(encoded), len(payload))
	}
	compressed, err := DecodeCompressedCanonicalPayload(encoded)
	if err != nil {
		t.Fatalf("DecodeCompressedCanonicalPayload() error = %v", err)
	}
	if CanonicalPayload(compressed) != payload {
		t.Fatalf("compressed payload changed the canonical instance")
	}
}
