//go:build soak
// +build soak

package soak

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// spotterChild manages the spotter binary as a child process (plan §8.1:
// the binary is the system under test; flags, config resolution, election,
// dial-retry and restart are exercised exactly as shipped).
type spotterChild struct {
	bin        string
	workDir    string
	kubeconfig string
	etcd       []string
	atlasAddr  string

	cmd     *exec.Cmd
	mu      sync.Mutex
	starts  int
	logFile *os.File
}

// newSpotterChild builds the manager. etcd is the embedded etcd's client
// endpoints; atlasAddr is the Atlas stand-in's TCP address.
func newSpotterChild(bin, workDir, kubeconfig string, etcd []string, atlasAddr string) *spotterChild {
	return &spotterChild{
		bin:        bin,
		workDir:    workDir,
		kubeconfig: kubeconfig,
		etcd:       etcd,
		atlasAddr:  atlasAddr,
	}
}

// start execs the binary with the full flag set of plan §8.4: providers
// k8s,ecs; kubeconfig / consul-addr / etcd-endpoints / nacos-addr pointing
// at the local stack; grpc-addr at the Atlas stand-in; push-interval 120;
// an ephemeral metrics address; logs to a harness-owned file with
// log-to-std disabled. The child's stdout/stderr are captured into the log
// file too so a startup failure is visible in the artifact.
//
// The child is started in its own process group so a kill cannot take the
// harness (the test process) down with it.
func (c *spotterChild) start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd != nil && c.cmd.Process != nil {
		return fmt.Errorf("soak: spotter child already running (pid %d)", c.cmd.Process.Pid)
	}

	logPath := fmt.Sprintf("%s/spotter-child.log", c.workDir)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("soak: open child log %s: %w", logPath, err)
	}

	args := []string{
		"adapter",
		"--env", "test",
		"--providers", "k8s,ecs",
		"--kubeconfig", c.kubeconfig,
		"--consul-addr", consulAddr,
		"--etcd-endpoints", strings.Join(c.etcd, ","),
		"--nacos-addr", nacosAddr,
		"--grpc-addr", c.atlasAddr,
		"--push-interval", fmt.Sprintf("%d", PushIntervalSecs),
		"--metrics-addr", fmt.Sprintf(":%d", MetricsPort),
		"--leader-elect",
		"--log-to-std=false",
		"--log-file-path", c.workDir + "/log/",
	}
	cmd := exec.CommandContext(ctx, c.bin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Dir = c.workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("soak: start spotter binary: %w", err)
	}
	c.cmd = cmd
	c.logFile = logFile
	c.starts++
	return nil
}

// kill stops the child hard (kill -TERM then -KILL; graceful shutdown is a
// plan §10 non-goal — the soak kills the binary hard by design). Safe when
// not running.
func (c *spotterChild) kill() {
	c.mu.Lock()
	cmd := c.cmd
	c.cmd = nil
	c.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	pgid := -cmd.Process.Pid
	_ = syscall.Kill(pgid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _, _ = cmd.Process.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		<-done
	}
	if c.logFile != nil {
		_ = c.logFile.Close()
		c.logFile = nil
	}
}

// running reports whether a child process is alive.
func (c *spotterChild) running() bool {
	c.mu.Lock()
	cmd := c.cmd
	c.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return false
	}
	return cmd.ProcessState == nil
}

// starts reports how many times the child was started (scenario (f)
// restarts bump this).
func (c *spotterChild) startsCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.starts
}

// pid returns the current child's pid (0 when not running).
func (c *spotterChild) pid() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd == nil || c.cmd.Process == nil {
		return 0
	}
	return c.cmd.Process.Pid
}

// waitForHealthy blocks until the child shows a health signal: the metrics
// endpoint answering on the ephemeral port and the log file containing the
// election/provider startup lines. It polls; bound controls the wait.
func (c *spotterChild) waitForHealthy(bound time.Duration) error {
	deadline := time.Now().Add(bound)
	metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", MetricsPort)
	for time.Now().Before(deadline) {
		if !c.running() {
			return fmt.Errorf("soak: spotter child died during startup (see %s/spotter-child.log)", c.workDir)
		}
		response, err := http.Get(metricsURL) //nolint:gosec // fixed loopback URL
		if err == nil {
			_ = response.Body.Close()
			return nil // metrics server up: Run() is executing
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("soak: spotter child not healthy within %s (metrics endpoint silent; see %s/spotter-child.log)", bound, c.workDir)
}

// waitForLog blocks until a log file contains the marker, proving the
// binary reached that lifecycle point (used for leader-election evidence).
// With --log-to-std=false the application log lands in
// <workdir>/log/app.log (the legacy lumberjack sink); spotter-child.log
// holds the process stdout/stderr. Both are scanned.
func (c *spotterChild) waitForLog(marker string, bound time.Duration) error {
	paths := []string{
		fmt.Sprintf("%s/spotter-child.log", c.workDir),
		fmt.Sprintf("%s/log/app.log", c.workDir),
	}
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		for _, path := range paths {
			data, err := os.ReadFile(path) //nolint:gosec // harness-owned path
			if err == nil && strings.Contains(string(data), marker) {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("soak: log marker %q not seen within %s (checked %v)", marker, bound, paths)
}
