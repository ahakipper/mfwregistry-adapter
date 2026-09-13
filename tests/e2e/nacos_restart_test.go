//go:build nacos_restart
// +build nacos_restart

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"spotter/pkg/nacos"
)

func validateScratchContainerName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("NACOS_REAL_CONTAINER is required")
	}
	if strings.ContainsAny(name, "/\\ \t\r\n") || !(strings.HasPrefix(name, "dsca-") || strings.HasPrefix(name, "test-")) {
		return fmt.Errorf("container %q is not an approved scratch name (must start with dsca- or test-)", name)
	}
	return nil
}

func runNacosRestartCommand(ctx context.Context, container string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := execCommandContext(ctx, "docker", "restart", container)
	cmd.Env = os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("docker restart timed out: %w", ctx.Err())
		}
		return fmt.Errorf("docker restart %s: %w: %s", container, err, strings.TrimSpace(string(output)))
	}
	return nil
}

// execCommandContext is a small variable seam so guard tests never need to
// invoke Docker. The real test uses os/exec through this default.
var execCommandContext = defaultExecCommandContext

func defaultExecCommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}

func TestNacosRestartContainerGuard(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"dsca-observe-nacos", true},
		{"test-nacos-1", true},
		{"nacos-prod", false},
		{"dsca/escape", false},
		{"", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := validateScratchContainerName(tc.name); (got == nil) != tc.ok {
				t.Fatalf("validateScratchContainerName(%q)=%v wantOK=%t", tc.name, got, tc.ok)
			}
		})
	}
}

func TestNacosRestartGuardRequiresExplicitFlags(t *testing.T) {
	for _, key := range []string{"NACOS_REAL_CONTAINER", "NACOS_REAL_ALLOW_RESTART", "NACOS_REAL_SCRATCH", "NACOS_REAL_ALLOW_WRITE"} {
		t.Setenv(key, "")
	}
	if err := restartGuard(); err == nil {
		t.Fatal("restartGuard() accepted missing guards")
	}
	t.Setenv("NACOS_REAL_CONTAINER", "dsca-observe-nacos")
	t.Setenv("NACOS_REAL_ALLOW_RESTART", "1")
	t.Setenv("NACOS_REAL_SCRATCH", "1")
	if err := restartGuard(); err == nil {
		t.Fatal("restartGuard() accepted missing write guard")
	}
	t.Setenv("NACOS_REAL_ALLOW_WRITE", "1")
	if err := restartGuard(); err != nil {
		t.Fatalf("restartGuard() error = %v", err)
	}
}

func restartGuard() error {
	if os.Getenv("NACOS_REAL_ALLOW_RESTART") != "1" || os.Getenv("NACOS_REAL_SCRATCH") != "1" || os.Getenv("NACOS_REAL_ALLOW_WRITE") != "1" {
		return fmt.Errorf("restart requires NACOS_REAL_ALLOW_RESTART=1, NACOS_REAL_SCRATCH=1, and NACOS_REAL_ALLOW_WRITE=1")
	}
	return validateScratchContainerName(os.Getenv("NACOS_REAL_CONTAINER"))
}

func TestNacosRealRestartPersistence(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: NACOS_SERVER is not configured")
	}
	if err := restartGuard(); err != nil {
		t.Skipf("NOT VERIFIED: %v", err)
	}
	cfg, err := parseNacosRealConfig()
	if err != nil {
		t.Fatalf("invalid Nacos real config: %v", err)
	}
	if !cfg.canWrite {
		t.Skip("NOT VERIFIED: write guard is not enabled")
	}
	container := os.Getenv("NACOS_REAL_CONTAINER")
	timeout := cfg.timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create SDK client: %v", err)
	}
	service := "__spotter_restart_" + time.Now().UTC().Format("20060102T150405.000000000")
	params := nacos.InstanceParams{ServiceName: service, IP: "127.0.0.1", Port: 1, ClusterName: "spotter-restart", GroupName: cfg.client.GroupName, NamespaceID: cfg.client.NamespaceID, Enabled: true, Ephemeral: false}
	registered := true
	cleanupAttempted := false
	cleanupStatus := "not_needed"
	defer func() {
		if registered {
			cleanupAttempted = true
			cleanupStatus = "passed"
			if cleanupErr := client.DeregisterInstance(params); cleanupErr != nil {
				cleanupStatus = "failed"
				t.Errorf("Nacos restart canary cleanup failed: %v", cleanupErr)
			}
		}
		t.Logf("NACOS_RESTART cleanup_attempted=%t status=%s residual_unknown=%t endpoint=%s", cleanupAttempted, cleanupStatus, cleanupStatus != "passed" && cleanupAttempted, cfg.endpoint)
		if closeErr := client.Close(); closeErr != nil {
			t.Errorf("Nacos restart SDK close failed: %v", closeErr)
		}
	}()
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("restart canary register: %v", err)
	}
	if err := runNacosRestartCommand(ctx, container); err != nil {
		t.Fatalf("restart scratch container: %v", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		if _, err := client.ListServices(100); err == nil {
			if hosts, queryErr := client.ListInstances(service); queryErr == nil && len(hosts) > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Nacos canary did not reappear after restart within %s", timeout)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Nacos restart query timed out: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	cleanupAttempted = true
	cleanupStatus = "passed"
	if err := client.DeregisterInstance(params); err != nil {
		cleanupStatus = "failed"
		t.Fatalf("Nacos restart canary deregister: %v", err)
	}
	registered = false
	t.Logf("NACOS_RESTART PASS: endpoint=%s container=%s transport=sdk restart=persistence latency_ms=%d", cfg.endpoint, container, time.Since(deadline.Add(-timeout)).Milliseconds())
}
