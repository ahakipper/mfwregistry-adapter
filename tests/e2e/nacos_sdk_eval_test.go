//go:build nacos_sdk_eval
// +build nacos_sdk_eval

package e2e

import (
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
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
	writeAttempted := false
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
				t.Errorf("Nacos SDK canary cleanup failed: %v", cleanupErr)
			}
		}
		t.Logf("NACOS_SDK_EVAL cleanup_attempted=%t status=%s latency_ms=%d error=%v residual_unknown=%t endpoint=%s", cleanupAttempted, cleanupStatus, cleanupElapsed.Milliseconds(), cleanupErr, cleanupErr != nil, cfg.endpoint)
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("Nacos SDK client close failed: %v", closeErr)
		}
	}()
	writeAttempted = true
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("SDK persistent register: %v", err)
	}
	batchService := service + "_batch"
	batchParam := vo.BatchRegisterInstanceParam{ServiceName: batchService, GroupName: cfg.client.GroupName, Instances: []vo.RegisterInstanceParam{{Ip: "127.0.0.2", Port: 2, Enable: true, Ephemeral: true}}}
	batchAttempted := true
	defer func() {
		var cleanupErr error
		if batchAttempted {
			cleanupErr = client.DeregisterInstance(nacos.InstanceParams{ServiceName: batchService, IP: "127.0.0.2", Port: 2, ClusterName: "DEFAULT", GroupName: cfg.client.GroupName, NamespaceID: cfg.client.NamespaceID, Ephemeral: true})
			if cleanupErr == nil {
				cleanupErr = verifyNacosCanary(cfg.client, batchService)
			}
		}
		status := "passed"
		if cleanupErr != nil {
			status = "failed"
		}
		t.Logf("NACOS_SDK_EVAL_BATCH cleanup_attempted=%t status=%s error=%v residual_unknown=%t", batchAttempted, status, cleanupErr, cleanupErr != nil)
		if cleanupErr != nil {
			t.Errorf("Nacos SDK batch cleanup failed (residual_unknown=true): %v", cleanupErr)
		}
	}()
	if err := client.BatchRegisterEphemeral(batchParam); err != nil {
		t.Fatalf("SDK ephemeral batch register: %v", err)
	}
	if hosts, err := verifierHosts(cfg.client, batchService); err != nil || len(hosts) != 1 || hosts[0].IP != "127.0.0.2" || hosts[0].Port != 2 || !hosts[0].Ephemeral || !hosts[0].Enabled {
		t.Fatalf("fresh batch state: hosts=%+v err=%v", hosts, err)
	}
	if _, err := client.ListInstances(service); err != nil {
		t.Fatalf("SDK SelectAll query: %v", err)
	}
	// Verify through a separate SDK client so this gate cannot pass from an
	// in-process naming cache populated by the writer client.
	verifier, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create fresh SDK verifier: %v", err)
	}
	defer func() {
		if err := verifier.Close(); err != nil {
			t.Errorf("fresh verifier close failed: %v", err)
		}
	}()
	if hosts, err := verifier.ListInstances(service); err != nil || len(hosts) == 0 {
		t.Fatalf("fresh SDK verifier state: hosts=%d err=%v", len(hosts), err)
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
		t.Fatalf("SDK persistent deregister: %v", err)
	}
	if err := verifyNacosCanary(cfg.client, service); err != nil {
		t.Fatalf("fresh SDK residual verification: %v", err)
	}
	cleanupElapsed = time.Since(cleanupStart)
	cleanupStatus = "passed"
	t.Logf("NACOS_SDK_EVAL PASS: endpoint=%s transport=sdk lifecycle=register,list,services,subscribe,unsubscribe,deregister latency_ms=%d service=%s", cfg.endpoint, time.Since(started).Milliseconds(), service)
}

func verifierHosts(cfg nacos.ClientConfig, service string) ([]nacos.Host, error) {
	c, err := nacos.NewClientWithConfig(cfg, nil)
	if err != nil {
		return nil, err
	}
	hosts, readErr := c.ListInstances(service)
	closeErr := c.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return hosts, nil
}
