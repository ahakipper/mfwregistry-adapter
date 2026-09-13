//go:build nacos_sdk_eval
// +build nacos_sdk_eval

package e2e

import (
	"os"
	"strings"
	"testing"
	"time"

	"spotter/pkg/nacos"
)

// TestNacosSDKPersistentLifecycle is an opt-in scratch/pre-production gate.
// Set NACOS_SERVER to one or more comma-separated Nacos 2.x addresses and
// NACOS_NAMESPACE/NACOS_GROUP/NACOS_USERNAME/NACOS_PASSWORD as needed. The
// default CI suite intentionally skips this test when no target is supplied;
// a skip is NOT production evidence.
func TestNacosSDKPersistentLifecycle(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: NACOS_SERVER is not configured")
	}
	cfg, err := parseNacosRealConfig()
	if err != nil {
		t.Skipf("NOT VERIFIED: %v", err)
	}
	if !cfg.canWrite {
		t.Skip("NOT VERIFIED: set NACOS_REAL_SCRATCH=1 and NACOS_REAL_ALLOW_WRITE=1 for an explicit scratch target")
	}
	started := time.Now()
	client, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create SDK client: %v", err)
	}
	service := "__spotter_sdk_eval_" + time.Now().UTC().Format("20060102T150405.000000000")
	params := nacos.InstanceParams{ServiceName: service, IP: "127.0.0.1", Port: 1, ClusterName: "spotter-sdk-eval", GroupName: cfg.client.GroupName, NamespaceID: cfg.client.NamespaceID, Enabled: true, Ephemeral: false}
	registered := true
	defer func() {
		status := "not_needed"
		var cleanupErr error
		cleanupStart := time.Now()
		if registered {
			status = "passed"
			cleanupErr = client.DeregisterInstance(params)
			if cleanupErr != nil {
				status = "failed"
				t.Errorf("Nacos SDK canary cleanup failed: %v", cleanupErr)
			}
		}
		t.Logf("NACOS_SDK_EVAL cleanup_attempted=%t status=%s latency_ms=%d error=%v residual_unknown=%t endpoint=%s", registered, status, time.Since(cleanupStart).Milliseconds(), cleanupErr, cleanupErr != nil, cfg.endpoint)
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("Nacos SDK client close failed: %v", closeErr)
		}
	}()
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("SDK persistent register: %v", err)
	}
	if _, err := client.ListInstances(service); err != nil {
		t.Fatalf("SDK SelectAll query: %v", err)
	}
	if _, err := client.ListServices(100); err != nil {
		t.Fatalf("SDK service list: %v", err)
	}
	callback := func([]nacos.Host, error) {}
	if err := client.Subscribe(service, cfg.client.GroupName, nil, callback); err != nil {
		t.Fatalf("SDK subscribe: %v", err)
	}
	if err := client.Unsubscribe(service, cfg.client.GroupName, nil, callback); err != nil {
		t.Fatalf("SDK unsubscribe: %v", err)
	}
	if err := client.DeregisterInstance(params); err != nil {
		t.Fatalf("SDK persistent deregister: %v", err)
	}
	registered = false
	t.Logf("NACOS_SDK_EVAL PASS: endpoint=%s transport=sdk lifecycle=register,list,services,subscribe,unsubscribe,deregister latency_ms=%d service=%s", cfg.endpoint, time.Since(started).Milliseconds(), service)
}
