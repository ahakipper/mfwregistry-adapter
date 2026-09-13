//go:build nacos_real
// +build nacos_real

package e2e

import (
	"os"
	"strings"
	"testing"
	"time"

	"spotter/pkg/nacos"
)

// TestNacosReal is the opt-in real-Nacos gate named by the remediation plan.
// It intentionally skips without NACOS_SERVER; a skip is not production
// evidence. The target must be Nacos 2.x with the gRPC port (server port +
// 1000) reachable from the test host.
func TestNacosReal(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: NACOS_SERVER is not configured")
	}
	cfg, err := parseNacosRealConfig()
	if err != nil {
		t.Fatalf("invalid Nacos real config: %v", err)
	}
	if !cfg.canWrite {
		t.Skip("NOT VERIFIED: set NACOS_REAL_SCRATCH=1 and NACOS_REAL_ALLOW_WRITE=1 for an explicit scratch target")
	}
	started := time.Now()
	if err := nacos.CheckReadinessWithConfig(cfg.client, nil); err != nil {
		t.Fatalf("readiness read+write gate: %v", err)
	}
	client, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create official SDK client: %v", err)
	}
	service := "__spotter_real_" + time.Now().UTC().Format("20060102T150405.000000000")
	params := nacos.InstanceParams{ServiceName: service, IP: "127.0.0.1", Port: 1, ClusterName: "spotter-real", GroupName: cfg.client.GroupName, NamespaceID: cfg.client.NamespaceID, Enabled: true, Ephemeral: false}
	writeAttempted := false // SDK transport errors may be ambiguous after server-side apply.
	cleanupAttempted := false
	cleanupStatus := "not_needed"
	var cleanupElapsed time.Duration
	defer func() {
		var cleanupErr error
		if writeAttempted {
			cleanupAttempted = true
			cleanupStatus = "passed"
			cleanupStart := time.Now()
			cleanupErr = client.DeregisterInstance(params)
			cleanupElapsed = time.Since(cleanupStart)
			if cleanupErr != nil {
				cleanupStatus = "failed"
				t.Errorf("Nacos canary cleanup failed: %v", cleanupErr)
			}
		}
		t.Logf("NACOS_REAL cleanup_attempted=%t status=%s latency_ms=%d error=%v residual_unknown=%t endpoint=%s", cleanupAttempted, cleanupStatus, cleanupElapsed.Milliseconds(), cleanupErr, cleanupErr != nil, cfg.endpoint)
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("Nacos client close failed: %v", closeErr)
		}
	}()
	writeAttempted = true
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("official SDK persistent register: %v", err)
	}
	if _, err := client.ListCatalogInstances(service, "spotter-real"); err != nil {
		t.Fatalf("catalog compatibility query: %v", err)
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
	cleanupAttempted = true
	cleanupStart := time.Now()
	if err := client.DeregisterInstance(params); err != nil {
		t.Fatalf("official SDK persistent deregister: %v", err)
	}
	cleanupElapsed = time.Since(cleanupStart)
	cleanupStatus = "passed"
	t.Logf("NACOS_REAL PASS: endpoint=%s transport=sdk lifecycle=register,catalog,list,services,subscribe,unsubscribe,deregister latency_ms=%d service=%s", cfg.endpoint, time.Since(started).Milliseconds(), service)
}
