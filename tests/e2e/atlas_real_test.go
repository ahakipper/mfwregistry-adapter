//go:build atlas_real
// +build atlas_real

// The Atlas real-wire gate is deliberately opt-in.  It is the only test in
// this repository that is allowed to send writes to an Atlas endpoint, and it
// refuses to run unless the caller explicitly identifies a scratch target.
package e2e

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/pkg/discoverycenter"
)

// TestAtlasReal exercises the current Atlas wire contract end to end:
// Dial -> SynInstance -> SynAllInstance -> GetAllInstance.  The current
// adapter uses a JSON gRPC codec over the legacy mirror structs.  A passing
// run is real protocol evidence only when ATLAS_REAL_ADDR points at a
// disposable/scratch Atlas and ATLAS_REAL_ALLOW_WRITE=1 is set.  In all
// other environments this test skips and therefore remains NOT VERIFIED.
//
// The test intentionally does not claim protobuf compatibility.  If the
// target rejects JSON (for example, because it requires generated protobuf
// messages), the call fails and the B4 gate remains open until a generated
// client or an explicitly versioned compatibility adapter is supplied.
func TestAtlasReal(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv("ATLAS_REAL_ADDR"))
	if addr == "" {
		t.Skip("NOT VERIFIED: ATLAS_REAL_ADDR is not configured")
	}
	if os.Getenv("ATLAS_REAL_ALLOW_WRITE") != "1" || os.Getenv("ATLAS_REAL_SCRATCH") != "1" {
		t.Skip("NOT VERIFIED: set ATLAS_REAL_ALLOW_WRITE=1 and ATLAS_REAL_SCRATCH=1 for an explicit scratch target")
	}

	timeout := 15 * time.Second
	if raw := strings.TrimSpace(os.Getenv("ATLAS_REAL_TIMEOUT")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("invalid ATLAS_REAL_TIMEOUT %q: %v", raw, err)
		}
		timeout = parsed
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := discoverycenter.Dial(ctx, addr, nil, nil)
	if err != nil {
		t.Fatalf("Atlas JSON-codec dial failed (wire compatibility remains unverified): %v", err)
	}
	defer func() { _ = client.Close() }()

	stamp := time.Now().UTC().Format("20060102T150405.000000000")
	instanceID := "__spotter_atlas_real_" + stamp
	if suffix := strings.TrimSpace(os.Getenv("ATLAS_REAL_INSTANCE_SUFFIX")); suffix != "" {
		instanceID += "_" + suffix
	}
	appCode := strings.TrimSpace(os.Getenv("ATLAS_REAL_APP_CODE"))
	if appCode == "" {
		appCode = "__spotter_atlas_real"
	}
	provider := strings.TrimSpace(os.Getenv("ATLAS_REAL_PROVIDER"))
	if provider == "" {
		provider = instance.ProviderK8s
	}
	status := int32(instance.InstanceStatusOnline)
	if raw := strings.TrimSpace(os.Getenv("ATLAS_REAL_STATUS")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			t.Fatalf("invalid ATLAS_REAL_STATUS %q: %v", raw, err)
		}
		status = int32(parsed)
	}
	payload := []*instance.Instance{{
		InstanceId: instanceID,
		AppCode:    appCode,
		Provider:   provider,
		Ip:         "127.0.0.1",
		EnvType:    "test",
		Status:     status,
		Enabled:    true,
		Reversion:  time.Now().UnixNano(),
		Ports:      []*instance.PortInfo{{Name: "atlas-real", Protocol: instance.ProtoGRPC, Port: 1}},
	}}

	response, err := client.Sync(payload)
	if err != nil {
		t.Fatalf("SynInstance failed using JSON codec: %v", err)
	}
	if response == nil || response.GetCode() != 0 {
		t.Fatalf("SynInstance returned non-success response: %#v", response)
	}

	fullResponse, err := client.SyncAll(payload)
	if err != nil {
		t.Fatalf("SynAllInstance failed using JSON codec: %v", err)
	}
	if fullResponse == nil || fullResponse.GetCode() != 0 {
		t.Fatalf("SynAllInstance returned non-success response: %#v", fullResponse)
	}

	list, err := client.GetAll([]int32{status}, provider)
	if err != nil {
		t.Fatalf("GetAllInstance failed using JSON codec: %v", err)
	}
	if list == nil {
		t.Fatal("GetAllInstance returned a nil list")
	}
	found := false
	for _, got := range list.Instance {
		if got != nil && got.InstanceId == instanceID && got.AppCode == appCode {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("GetAllInstance did not return canary %q (returned %d records); inspect Atlas visibility/namespace contract", instanceID, len(list.Instance))
	}
	// Atlas exposes synchronization rather than a dedicated delete endpoint;
	// close the scratch canary through the same operation using offline status
	// so a successful gate does not leave test data behind.
	cleanupPayload := *payload[0]
	cleanupPayload.Status = instance.InstanceStatusOffline
	cleanupPayload.Enabled = false
	cleanupPayload.Reversion++
	if _, err := client.Sync([]*instance.Instance{&cleanupPayload}); err != nil {
		t.Fatalf("Atlas canary cleanup failed: %v", err)
	}
	t.Logf("ATLAS_REAL PASS: addr=%s codec=json methods=SynInstance, SynAllInstance, GetAllInstance appCode=%s instance=%s records=%d", addr, appCode, instanceID, len(list.Instance))
}
