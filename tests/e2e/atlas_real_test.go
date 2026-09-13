//go:build atlas_real
// +build atlas_real

// The Atlas real-wire gate is deliberately opt-in.  It is the only test in
// this repository that is allowed to send writes to an Atlas endpoint, and it
// refuses to run unless the caller explicitly identifies a scratch target.
package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/pkg/discoverycenter"
)

type atlasRealConfig struct {
	addr, auth, ca, serverName string
	insecure                   bool
}

func parseAtlasRealConfig() (atlasRealConfig, error) {
	c := atlasRealConfig{addr: strings.TrimSpace(os.Getenv("ATLAS_REAL_ADDR")), auth: strings.TrimSpace(os.Getenv("ATLAS_REAL_AUTH_TOKEN")), ca: strings.TrimSpace(os.Getenv("ATLAS_REAL_CA_FILE")), serverName: strings.TrimSpace(os.Getenv("ATLAS_REAL_SERVER_NAME"))}
	c.insecure = os.Getenv("ATLAS_REAL_INSECURE_SKIP_VERIFY") == "1"
	if c.addr == "" {
		return c, errors.New("ATLAS_REAL_ADDR is required")
	}
	if c.insecure && os.Getenv("ATLAS_REAL_SCRATCH") != "1" {
		return c, errors.New("insecure TLS requires scratch guard")
	}
	return c, nil
}

func atlasDialOptions(c atlasRealConfig) ([]grpc.DialOption, error) {
	opts := []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})), grpc.WithBlock()}
	if c.ca != "" || c.serverName != "" || c.insecure {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if c.ca != "" {
			pem, err := os.ReadFile(c.ca)
			if err != nil || !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("invalid Atlas CA file")
			}
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: c.serverName, InsecureSkipVerify: c.insecure}))) // #nosec G402 guarded scratch option
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	if c.auth != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(bearerCredentials{token: c.auth}))
	}
	return opts, nil
}

type bearerCredentials struct{ token string }

func (b bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}
func (b bearerCredentials) RequireTransportSecurity() bool { return false }

type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v interface{}) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                               { return "json" }

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
	cfg, cfgErr := parseAtlasRealConfig()
	addr := cfg.addr
	if addr == "" {
		t.Skip("NOT VERIFIED: ATLAS_REAL_ADDR is not configured")
	}
	if cfgErr != nil {
		t.Fatalf("invalid Atlas config: %v", cfgErr)
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
	opts, err := atlasDialOptions(cfg)
	if err != nil {
		t.Fatalf("invalid Atlas transport config: %v", err)
	}
	client, err := discoverycenter.Dial(ctx, addr, nil, nil, opts...)
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
