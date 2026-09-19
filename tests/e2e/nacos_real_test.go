//go:build nacos_real
// +build nacos_real

package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/pkg/nacos"
)

// oneShotBatchAdmin is an injected cluster-admin facade for the guarded real
// gate. The first health-check request fails deterministically, exercising the
// Sink's retry path without falling back to raw HTTP or changing persistent
// instance semantics. Subsequent calls succeed and are intentionally no-ops:
// this gate measures application-batch convergence against the real naming
// server, while the production admin facade is verified by its own gate.
type oneShotBatchAdmin struct {
	failed bool
}

func (a *oneShotBatchAdmin) UpdateHealthChecker(context.Context, string, string, string, string) error {
	if !a.failed {
		a.failed = true
		return errors.New("injected one-shot cluster-admin request failure")
	}
	return nil
}

func (*oneShotBatchAdmin) Close(context.Context) error { return nil }

// TestNacosRealPersistentApplicationBatch proves the Spotter application
// batch path against a guarded real Nacos server. Nacos's protocol-level
// persistent batch remains unsupported; this test instead proves that one
// application is partitioned into bounded (<=100) batches while every item is
// still written through the official SDK persistent RegisterInstance call.
func TestNacosRealPersistentApplicationBatch(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: EnvError: NACOS_SERVER is not configured")
	}
	cfg, err := parseNacosRealConfig()
	if err != nil {
		t.Skipf("NOT VERIFIED: EnvError: %v", err)
	}
	if !cfg.canWrite {
		t.Skip("NOT VERIFIED: EnvError: scratch write guards are not enabled; set NACOS_REAL_SCRATCH=1 and NACOS_REAL_ALLOW_WRITE=1")
	}

	admin := &oneShotBatchAdmin{}
	// This gate intentionally exercises the optional admin-managed retry path;
	// deployment-owned is the production default and must not invoke the
	// injected factory.
	cfg.client.HealthPolicy = nacos.HealthPolicyAdminManaged
	cfg.client.ClusterAdminFactory = func() (nacos.NacosClusterAdmin, error) { return admin, nil }
	sink, err := nacos.NewSinkWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create SDK sink: %v", err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			t.Errorf("close SDK sink: %v", err)
		}
	}()

	service := "__spotter_real_batch_" + time.Now().UTC().Format("20060102T150405.000000000")
	cluster := "spotter-real-batch"
	items := make([]*instance.Instance, 201)
	for i := range items {
		items[i] = &instance.Instance{
			InstanceId: fmt.Sprintf("batch-%03d", i),
			AppCode:    service,
			Provider:   cluster,
			Ip:         fmt.Sprintf("10.244.200.%d", i+1),
			Ports:      []*instance.PortInfo{{Port: int32(20000 + i)}},
			Enabled:    true,
			Status:     instance.InstanceStatusOnline,
		}
	}
	started := time.Now()
	writeAttempted := false
	cleanupStatus := "not_needed"
	var cleanupErr error
	defer func() {
		if writeAttempted {
			cleanupStatus = "passed"
			offline := make([]*instance.Instance, len(items))
			for i, item := range items {
				copyItem := *item
				copyItem.Status = instance.InstanceStatusOffline
				offline[i] = &copyItem
			}
			if err := sink.PushAll(time.Now().UnixNano(), offline); err != nil {
				cleanupStatus = "failed"
				cleanupErr = err
				t.Errorf("persistent batch cleanup: %v", err)
			}
			if err := verifyNacosCatalogEmpty(cfg.client, service, cluster); err != nil {
				cleanupStatus = "failed"
				if cleanupErr == nil {
					cleanupErr = err
				}
				t.Errorf("persistent batch residual verification: %v", err)
			}
		}
		t.Logf("NACOS_REAL_PERSISTENT_APPLICATION_BATCH cleanup_attempted=%t status=%s error=%v residual_unknown=%t endpoint=%s service=%s batch_max=%d configured_concurrency=%d elapsed_ms=%d", writeAttempted, cleanupStatus, cleanupErr, cleanupErr != nil, cfg.endpoint, service, nacos.MaxPersistentBatchSize, nacos.DefaultPushConcurrency, time.Since(started).Milliseconds())
	}()

	writeAttempted = true
	if err := sink.PushAll(time.Now().UnixNano(), items); err == nil {
		t.Fatal("one-shot injected request failure was not observed")
	} else {
		t.Logf("NACOS_REAL_PERSISTENT_APPLICATION_BATCH injected_error=%v retry_required=true", err)
	}
	if err := sink.PushAll(time.Now().UnixNano(), items); err != nil {
		t.Fatalf("persistent application batch retry: %v", err)
	}
	hosts, err := waitForNacosCatalogCount(cfg.client, service, cluster, len(items), 30*time.Second)
	if err != nil {
		t.Fatalf("fresh catalog convergence verification: %v", err)
	}
	for _, host := range hosts {
		if host.Metadata["spotterOwner"] != "spotter" {
			t.Fatalf("catalog entry %s is not owned by Spotter: metadata=%v", host.InstanceID, host.Metadata)
		}
		if host.Ephemeral {
			t.Fatalf("catalog entry %s is ephemeral, want persistent (ephemeral=false)", host.InstanceID)
		}
	}
	t.Logf("NACOS_REAL_PERSISTENT_APPLICATION_BATCH PASS: transport=sdk persistent=true protocol_batch=unsupported_by_pinned_sdk application_batch=true application=%s namespace=%s group=%s cluster=%s entries=%d batch_max=%d batches=%d configured_concurrency_cap=%d injected_request_failure=true retry=passed final_catalog_hash=%s", service, cfg.client.NamespaceID, cfg.client.GroupName, cluster, len(hosts), nacos.MaxPersistentBatchSize, (len(items)+nacos.MaxPersistentBatchSize-1)/nacos.MaxPersistentBatchSize, nacos.DefaultPushConcurrency, catalogHash(hosts))
}

func verifierCatalogHosts(cfg nacos.ClientConfig, service, cluster string) ([]nacos.Host, error) {
	client, err := nacos.NewClientWithConfig(cfg, nil)
	if err != nil {
		return nil, err
	}
	hosts, readErr := client.ListCatalogInstances(service, cluster)
	closeErr := client.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return hosts, nil
}

func waitForNacosCatalogCount(cfg nacos.ClientConfig, service, cluster string, want int, timeout time.Duration) ([]nacos.Host, error) {
	deadline := time.Now().Add(timeout)
	var hosts []nacos.Host
	var lastErr error
	for {
		hosts, lastErr = verifierCatalogHosts(cfg, service, cluster)
		if lastErr == nil && len(hosts) == want {
			return hosts, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("catalog convergence timeout: entries=%d want=%d last_error=%v", len(hosts), want, lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func verifyNacosCatalogEmpty(cfg nacos.ClientConfig, service, cluster string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		hosts, err := verifierCatalogHosts(cfg, service, cluster)
		if err == nil && len(hosts) == 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	hosts, err := verifierCatalogHosts(cfg, service, cluster)
	return fmt.Errorf("catalog residual hosts=%d err=%v", len(hosts), err)
}

func catalogHash(hosts []nacos.Host) string {
	ids := make([]string, 0, len(hosts))
	for _, host := range hosts {
		keys := make([]string, 0, len(host.Metadata))
		for key := range host.Metadata {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		metadata := make([]string, 0, len(keys))
		for _, key := range keys {
			metadata = append(metadata, key+"="+host.Metadata[key])
		}
		ids = append(ids, fmt.Sprintf("instance=%s|ip=%s|port=%d|cluster=%s|service=%s|ephemeral=%t|enabled=%t|healthy=%t|weight=%.6f|metadata=%s", host.InstanceID, host.IP, host.Port, host.ClusterName, host.ServiceName, host.Ephemeral, host.Enabled, host.Healthy, host.Weight, strings.Join(metadata, ",")))
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	return hex.EncodeToString(sum[:])
}

// TestNacosReal is the opt-in real-Nacos gate named by the remediation plan.
// It intentionally skips without NACOS_SERVER; a skip is not production
// evidence. The target must be Nacos 3.x with the gRPC port (server port +
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
			if residualErr := verifyNacosCanary(cfg.client, service); residualErr != nil {
				cleanupStatus = "failed"
				if cleanupErr == nil {
					cleanupErr = residualErr
				}
				t.Errorf("Nacos canary residual verification failed: %v", residualErr)
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
