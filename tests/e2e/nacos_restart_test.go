//go:build nacos_restart
// +build nacos_restart

package e2e

import (
	"context"
	"fmt"
	"net"
	"net/url"
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

func runNacosOutageRestart(ctx context.Context, container, addr string) error {
	if strings.Contains(addr, ",") {
		return fmt.Errorf("restart outage requires a single server address")
	}
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil || u.Host == "" {
			return fmt.Errorf("invalid restart endpoint")
		}
		addr = u.Host
	}
	for _, action := range []string{"stop", "start"} {
		cmd := execCommandContext(ctx, "docker", action, container)
		cmd.Env = os.Environ()
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("docker %s: %w: %s", action, err, strings.TrimSpace(string(out)))
		}
		if action == "stop" {
			deadline, ok := ctx.Deadline()
			if !ok {
				return fmt.Errorf("restart requires context deadline")
			}
			observedDown := false
			for time.Now().Before(deadline) {
				conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
				if err != nil {
					observedDown = true
					break
				}
				conn.Close()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
			if !observedDown {
				return fmt.Errorf("Nacos outage not observed: endpoint remained reachable")
			}
		} else {
			deadline, ok := ctx.Deadline()
			if !ok {
				return fmt.Errorf("restart requires context deadline")
			}
			observedUp := false
			for time.Now().Before(deadline) {
				conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
				if err == nil {
					conn.Close()
					observedUp = true
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
			}
			if !observedUp {
				return fmt.Errorf("Nacos endpoint did not recover before deadline")
			}
		}
	}
	return nil
}

func warmNacosSDK(ctx context.Context, client *nacos.Client) error {
	if client == nil {
		return fmt.Errorf("nil Nacos SDK client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if _, err := client.ListServices(1); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Nacos SDK session warmup: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
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

func TestWarmNacosSDKHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := warmNacosSDK(ctx, &nacos.Client{}); err == nil {
		t.Fatal("warmNacosSDK(canceled) returned nil")
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
			if client == nil {
				cleanupStatus = "failed"
				t.Errorf("Nacos restart canary cleanup unavailable: no active SDK client")
			} else if cleanupErr := client.DeregisterInstance(params); cleanupErr != nil {
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
	if err := warmNacosSDK(ctx, client); err != nil {
		t.Fatalf("warm pre-restart SDK session: %v", err)
	}
	// The official SDK v2.3.5 has a race in its automatic reconnect path when
	// the server restarts. Close the old client before Docker restart and
	// create a fresh SDK client afterwards; this gate proves persistence and
	// avoids suppressing the vendor race detector.
	if err := client.Close(); err != nil {
		t.Fatalf("close pre-restart SDK client: %v", err)
	}
	if err := runNacosRestartCommand(ctx, container); err != nil {
		t.Fatalf("restart scratch container: %v", err)
	}
	deadline := time.Now().Add(timeout)
	postRestartClient, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create post-restart SDK client: %v", err)
	}
	client = postRestartClient
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
	t.Logf("NACOS_RESTART PASS: endpoint=%s container=%s transport=sdk restart=persistence new_client=true sdk_auto_reconnect=NOT_VERIFIED/RACE_BLOCKED latency_ms=%d", cfg.endpoint, container, time.Since(deadline.Add(-timeout)).Milliseconds())
}

// TestNacosRealAutoReconnect keeps one SDK client across a scratch restart.
// It is an opt-in vendor compatibility gate; a race or reconnect failure is
// a hard failure when enabled and never promoted to production evidence.
func TestNacosRealAutoReconnect(t *testing.T) {
	if strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: NACOS_SERVER is not configured")
	}
	if err := restartGuard(); err != nil {
		t.Skipf("NOT VERIFIED: %v", err)
	}
	cfg, err := parseNacosRealConfig()
	if err != nil {
		t.Fatalf("invalid config: %v", err)
	}
	client, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create SDK client: %v", err)
	}
	writeAttempted := true
	service := "__spotter_reconnect_" + time.Now().UTC().Format("20060102T150405.000000000")
	params := nacos.InstanceParams{ServiceName: service, IP: "127.0.0.1", Port: 1, ClusterName: "spotter-reconnect", GroupName: cfg.client.GroupName, NamespaceID: cfg.client.NamespaceID, Enabled: true, Ephemeral: false}
	defer func() {
		attempted := writeAttempted
		status := "not_needed"
		var cleanupErr error
		if attempted {
			status = "passed"
			if err := client.DeregisterInstance(params); err != nil {
				status = "failed"
				cleanupErr = err
				t.Errorf("cleanup: %v", err)
			} else if verifier, err := nacos.NewClientWithConfig(cfg.client, nil); err != nil {
				status = "failed"
				cleanupErr = err
				t.Errorf("cleanup verifier: %v", err)
			} else {
				cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), cfg.timeout)
				for {
					hosts, queryErr := verifier.ListInstances(params.ServiceName)
					if queryErr == nil && len(hosts) == 0 {
						break
					}
					if cleanupCtx.Err() != nil {
						status = "failed"
						cleanupErr = fmt.Errorf("residual instances or query error: %v", queryErr)
						t.Errorf("cleanup residual: %v", cleanupErr)
						break
					}
					select {
					case <-cleanupCtx.Done():
						status = "failed"
						cleanupErr = cleanupCtx.Err()
						t.Errorf("cleanup deadline: %v", cleanupErr)
					case <-time.After(100 * time.Millisecond):
					}
				}
				cancelCleanup()
				if err := verifier.Close(); err != nil {
					status = "failed"
					cleanupErr = err
					t.Errorf("cleanup verifier close: %v", err)
				}
			}
		}
		t.Logf("NACOS_RECONNECT cleanup_attempted=%t status=%s residual_unknown=%t error=%v", attempted, status, cleanupErr != nil, cleanupErr)
		if err := client.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	if err := runNacosOutageRestart(ctx, os.Getenv("NACOS_REAL_CONTAINER"), cfg.endpoint); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := warmNacosSDK(ctx, client); err != nil {
		t.Fatalf("automatic reconnect visibility failed: %v", err)
	}
	params.Enabled = false
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("post-restart write: %v", err)
	}
	verifier, err := nacos.NewClientWithConfig(cfg.client, nil)
	if err != nil {
		t.Fatalf("create fresh verifier: %v", err)
	}
	defer func() {
		if err := verifier.Close(); err != nil {
			t.Errorf("fresh verifier close: %v", err)
		}
	}()
	hosts, err := verifier.ListInstances(service)
	if err != nil || len(hosts) == 0 || hosts[0].Enabled {
		t.Fatalf("reconnected canary missing: hosts=%d err=%v", len(hosts), err)
	}
	t.Logf("NACOS_RECONNECT PASS: endpoint=%s transport=sdk restart=single-client sdk_auto_reconnect=VERIFIED cleanup_deferred=true", cfg.endpoint)
}
