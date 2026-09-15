//go:build observe
// +build observe

package observe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	v2 "spotter/pkg/beehive/service/v2"
)

// The observe stack's throwaway pieces (dsca-4 §4.1): a throwaway nacos
// docker container on a scratch port (brought up OUT of band by the
// harness driver — see stack.go's stackUp/stackDown), an in-process Atlas
// stand-in on a scratch port (the throwaway "atlasrun" — discoverymock's
// StartTCP, the same stand-in the soak uses but on the observe port), the
// embedded etcd (etcdmock, ephemeral ports — never the demo's 12379), and
// the spotter child built from the repo.
//
// The child's flags (the batch-F/D contract): --providers k8s (the kwok
// source; the ecs/consul leg is not part of this stack — the scale axis is
// the k8s path per dsca-4 §8), --kubeconfig at the kwok cluster,
// --nacos-addr at the throwaway nacos, --reconcile-source nacos (Fix-C,
// first end-to-end exercise), --leader-elect=false (the harness's own
// child does not campaign — two spotter processes must never share an etcd
// campaign; the demo's embedded etcd at 12379 is untouched and the observe
// child's elector sees only the harness's own throwaway etcdmock),
// --push-interval 60 (the P of OBS_BOUND), --grpc-addr at the stand-in,
// --metrics-addr on a scratch port.

// spotterChild manages the observe spotter binary as a child process (the
// tests/soak/child.go pattern: own process group, restart-capable, killed
// by the harness teardown).
type spotterChild struct {
	bin        string
	workDir    string
	kubeconfig string
	etcd       []string
	atlasAddr  string
	nacosAddr  string
	metrics    int

	cmd      *exec.Cmd
	mu       sync.Mutex
	starts   int
	logFile  *os.File
	waitDone chan struct{}
	exitErr  error
}

func newSpotterChild(bin, workDir, kubeconfig string, etcd []string, atlasAddr, nacosAddr string, metricsPort int) *spotterChild {
	return &spotterChild{
		bin:        bin,
		workDir:    workDir,
		kubeconfig: kubeconfig,
		etcd:       etcd,
		atlasAddr:  atlasAddr,
		nacosAddr:  nacosAddr,
		metrics:    metricsPort,
	}
}

// start execs the binary with the observe flag set. The child's
// stdout/stderr are captured into the harness log so a startup failure is
// visible in the artifact; --log-to-std=false lands the application log in
// <workdir>/log/app.log (the forensic tail's source).
func (c *spotterChild) start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cmd != nil && c.cmd.Process != nil {
		if c.waitDone == nil {
			return fmt.Errorf("observe: spotter child already running (pid %d)", c.cmd.Process.Pid)
		}
		select {
		case <-c.waitDone:
			c.cmd = nil
			c.waitDone = nil
		default:
			return fmt.Errorf("observe: spotter child already running (pid %d)", c.cmd.Process.Pid)
		}
	}
	logPath := fmt.Sprintf("%s/spotter-child.log", c.workDir)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("observe: open child log %s: %w", logPath, err)
	}
	args := []string{
		"adapter",
		"--env", "test",
		"--providers", "k8s",
		"--kubeconfig", c.kubeconfig,
		"--etcd-endpoints", strings.Join(c.etcd, ","),
		"--nacos-addr", c.nacosAddr,
		"--reconcile-source", "nacos",
		"--grpc-addr", c.atlasAddr,
		"--push-interval", fmt.Sprintf("%d", PushIntervalSecs),
		"--metrics-addr", fmt.Sprintf(":%d", c.metrics),
		"--leader-elect=false",
		"--log-to-std=false",
		"--log-file-path", c.workDir + "/log/",
	}
	cmd := exec.CommandContext(ctx, c.bin, args...) //nolint:gosec // the harness's own built binary
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Dir = c.workDir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("observe: start spotter binary: %w", err)
	}
	c.cmd = cmd
	c.logFile = logFile
	c.waitDone = make(chan struct{})
	c.exitErr = nil
	c.starts++
	_ = os.WriteFile(filepath.Join(c.workDir, "spotter-child.pid"), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600)
	waitDone := c.waitDone
	go func() {
		err := cmd.Wait()
		c.mu.Lock()
		if c.cmd == cmd {
			c.exitErr = err
		}
		if c.logFile == logFile {
			_ = c.logFile.Close()
			c.logFile = nil
		}
		_ = os.Remove(filepath.Join(c.workDir, "spotter-child.pid"))
		close(waitDone)
		c.mu.Unlock()
	}()
	return nil
}

// kill stops the child hard (TERM then KILL, the soak's discipline).
func (c *spotterChild) kill() error {
	c.mu.Lock()
	cmd := c.cmd
	waitDone := c.waitDone
	logFile := c.logFile
	c.cmd = nil
	c.waitDone = nil
	c.logFile = nil
	c.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		if logFile != nil {
			_ = logFile.Close()
		}
		return nil
	}
	_ = os.Remove(filepath.Join(c.workDir, "spotter-child.pid"))
	pgid := -cmd.Process.Pid
	termErr := syscall.Kill(pgid, syscall.SIGTERM)
	if waitDone == nil {
		if logFile != nil {
			_ = logFile.Close()
		}
		return termErr
	}
	select {
	case <-waitDone:
		if logFile != nil {
			_ = logFile.Close()
		}
		return nil
	case <-time.After(10 * time.Second):
		killErr := syscall.Kill(pgid, syscall.SIGKILL)
		select {
		case <-waitDone:
			if logFile != nil {
				_ = logFile.Close()
			}
			return nil
		case <-time.After(5 * time.Second):
			// The process group did not reap within the bounded cleanup
			// window. Return the failure so OBS-full can classify teardown as
			// incomplete instead of silently reporting a clean run.
			if logFile != nil {
				_ = logFile.Close()
			}
			if killErr != nil {
				return fmt.Errorf("observe: child kill failed after bounded reap: term=%v kill=%v", termErr, killErr)
			}
			return fmt.Errorf("observe: child did not exit within bounded reap window")
		}
	}
}

// running reports whether the child process is alive.
func (c *spotterChild) running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := c.cmd
	if cmd == nil || cmd.Process == nil {
		return false
	}
	if c.waitDone == nil {
		return false
	}
	select {
	case <-c.waitDone:
		return false
	default:
		return true
	}
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

// waitForHealthy blocks until the child's metrics endpoint answers (the
// Run() is executing signal), bounded.
func (c *spotterChild) waitForHealthy(bound time.Duration) error {
	return c.waitForHealthyContext(context.Background(), bound)
}

func (c *spotterChild) waitForHealthyContext(parent context.Context, bound time.Duration) error {
	if parent == nil {
		parent = context.Background()
	}
	if bound <= 0 {
		return fmt.Errorf("observe: spotter child health bound must be positive")
	}
	ctx, cancel := context.WithTimeout(parent, bound)
	defer cancel()
	metricsURL := fmt.Sprintf("http://127.0.0.1:%d/metrics", c.metrics)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !c.running() {
			return fmt.Errorf("observe: spotter child died during startup (see %s/spotter-child.log)", c.workDir)
		}
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
		if reqErr != nil {
			return fmt.Errorf("observe: build health request: %w", reqErr)
		}
		response, err := sharedHTTP.Do(req) //nolint:gosec // fixed loopback URL
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("observe: spotter child not healthy within %s: %w (see %s/spotter-child.log)", bound, ctx.Err(), c.workDir)
		case <-ticker.C:
		}
	}
}

type spotterSnapshot struct {
	Instances        []*v2.Instance
	CanonicalPayload []string
	ObservedAt       time.Time
}

// debugSnapshot reads the test-only Spotter projection endpoint. It is
// enabled only when the child inherits SPOTTER_OBSERVE_DEBUG=1 and exposes
// the provider's informer-derived Instance list, giving the triad observer a
// third side between K8s and Nacos.
func (c *spotterChild) debugSnapshot(ctx context.Context) (spotterSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/debug/spotter/k8s", c.metrics)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return spotterSnapshot{}, err
	}
	response, err := sharedHTTP.Do(req) //nolint:gosec // fixed loopback URL
	if err != nil {
		return spotterSnapshot{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return spotterSnapshot{}, fmt.Errorf("spotter debug snapshot answered %d", response.StatusCode)
	}
	var payload struct {
		ObservedAt       string         `json:"observedAt"`
		Instances        []*v2.Instance `json:"instances"`
		CanonicalPayload []string       `json:"canonicalPayload"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&payload); err != nil {
		return spotterSnapshot{}, fmt.Errorf("decode spotter debug snapshot: %w", err)
	}
	observedAt, _ := time.Parse(time.RFC3339Nano, payload.ObservedAt)
	return spotterSnapshot{Instances: payload.Instances, CanonicalPayload: payload.CanonicalPayload, ObservedAt: observedAt}, nil
}

// logSlice extracts the lines naming any of the needles, issued within
// ±window of the reference time (the §4.4 forensic log slice — the exact
// lines the manual 20260909-0000 root-cause analysis used, captured
// automatically at divergence time). Untimestamped lines (process stdout)
// are skipped: the first divergences of a run otherwise sweep in the
// whole startup banner. Reads both the process log and the application
// log.
func (c *spotterChild) logSlice(ref time.Time, window time.Duration, needles []string, maxLines int) []string {
	if len(needles) == 0 {
		return nil
	}
	paths := []string{
		fmt.Sprintf("%s/spotter-child.log", c.workDir),
		fmt.Sprintf("%s/log/app.log", c.workDir),
	}
	var out []string
	for _, path := range paths {
		data, err := os.ReadFile(path) //nolint:gosec // harness-owned path
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			if len(out) >= maxLines {
				return out
			}
			matched := false
			for _, needle := range needles {
				if needle != "" && strings.Contains(line, needle) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			ts, ok := logLineTime(line)
			if !ok {
				continue // untimestamped lines cannot be windowed — skip
			}
			if ts.Before(ref.Add(-window)) || ts.After(ref.Add(window)) {
				continue
			}
			out = append(out, strings.TrimSpace(line))
		}
	}
	return out
}

// logLineTime extracts the leading timestamp of a spotter log line. Two
// shapes appear: the JSON zap lines ("2026-09-11T16:13:33.126+0800") and
// the lumberjack app.log's plain "2026-09-11 16:13:33.126" local lines.
func logLineTime(line string) (time.Time, bool) {
	trimmed := strings.TrimSpace(line)
	// zap's JSON encoder places the timestamp under a `ts` field rather than
	// at the beginning of the line. Decode that field first; older harness
	// versions only inspected a leading timestamp and silently lost every
	// forensic log slice from JSON application logs.
	if strings.HasPrefix(trimmed, "{") {
		var envelope struct {
			TS json.RawMessage `json:"ts"`
		}
		if err := json.Unmarshal([]byte(trimmed), &envelope); err == nil && len(envelope.TS) > 0 {
			var raw string
			if json.Unmarshal(envelope.TS, &raw) == nil {
				if ts, ok := parseLogTimestamp(raw); ok {
					return ts, true
				}
			}
			// Parse numeric zap timestamps from their decimal text rather than
			// float64; binary rounding at the nanosecond boundary otherwise
			// shifts values such as 1789136018.281 by one nanosecond.
			rawNumber := strings.TrimSpace(string(envelope.TS))
			parts := strings.SplitN(rawNumber, ".", 2)
			if whole, parseErr := strconv.ParseInt(parts[0], 10, 64); parseErr == nil {
				nanos := int64(0)
				if len(parts) == 2 {
					fraction := parts[1]
					if len(fraction) > 9 {
						fraction = fraction[:9]
					}
					fraction += strings.Repeat("0", 9-len(fraction))
					nanos, parseErr = strconv.ParseInt(fraction, 10, 64)
				}
				if parseErr == nil {
					return time.Unix(whole, nanos), true
				}
			}
		}
	}
	// Plain lumberjack lines carry the timestamp at the beginning. Scan a
	// bounded prefix instead of assuming the line starts with the timestamp;
	// this also handles a log-level prefix emitted by a wrapper process.
	for start := 0; start < len(trimmed) && start < 64; start++ {
		if ts, ok := parseLogTimestampAt(trimmed[start:]); ok {
			return ts, true
		}
	}
	return time.Time{}, false
}

func parseLogTimestampAt(value string) (time.Time, bool) {
	fields := strings.Fields(value)
	candidates := []string{value}
	if len(fields) > 0 {
		candidates = append(candidates, fields[0])
	}
	if len(fields) > 1 {
		candidates = append(candidates, fields[0]+" "+fields[1])
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999-0700",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		for _, candidate := range candidates {
			if ts, err := time.ParseInLocation(layout, candidate, time.Local); err == nil {
				return ts, true
			}
		}
	}
	return time.Time{}, false
}

func parseLogTimestamp(value string) (time.Time, bool) {
	return parseLogTimestampAt(strings.TrimSpace(value))
}
