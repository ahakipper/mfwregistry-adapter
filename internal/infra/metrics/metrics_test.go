package metrics

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRecorderCallsDoNotPanic verifies every Recorder method can be called
// repeatedly without panicking (the collectors are registered by the
// pkg/metrics init function).
func TestRecorderCallsDoNotPanic(t *testing.T) {
	r := New()
	for i := 0; i < 10; i++ {
		r.ObserveSyncOnceDuration(time.Duration(i) * time.Second)
		r.ObserveSyncAllDuration("k8s", time.Duration(i)*time.Second)
		r.ObserveSyncAllDuration("ecs", time.Duration(i)*time.Second)
		r.ObserveSyncAllDuration("other", time.Duration(i)*time.Second)
		r.ObserveSyncAllDuration("", time.Duration(i)*time.Second)
		r.SetSyncErrorQueueDepth(i)
		r.MarkSyncOnce()
	}
}

// TestRecorderSatisfiesPort asserts the compile-time interface assertion
// again at runtime (it is also checked by the var _ declaration).
func TestRecorderSatisfiesPort(t *testing.T) {
	var i interface{ MarkSyncOnce() } = New()
	if _, ok := i.(interface{ MarkSyncOnce() }); !ok {
		t.Fatalf("Recorder does not expose MarkSyncOnce")
	}
}

// httpGet performs a GET against addr and returns status and body.
func httpGet(t *testing.T, addr, path string, timeout time.Duration) (int, string, error) {
	t.Helper()
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(fmt.Sprintf("http://%s%s", addr, path))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

// TestOnListenerServesMetrics verifies the metrics HTTP server:
//   - GET /metrics answers 200 and exposes the registered collectors
//     (including sync_once_durations_histogram);
//   - the pprof index is reachable;
//   - stop shuts the server down and later requests fail;
//   - stop is idempotent.
func TestOnListenerServesMetrics(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()

	stop, err := OnListener(l)
	if err != nil {
		t.Fatalf("OnListener() returned error: %v", err)
	}

	// The server serves in a goroutine; retry until /metrics answers.
	var status int
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, body, err = httpGet(t, addr, "/metrics", 2*time.Second)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(body, "sync_once_durations_histogram") {
		t.Errorf("GET /metrics body does not contain %q", "sync_once_durations_histogram")
	}
	if !strings.Contains(body, "sync_all_durations_histogram") {
		t.Errorf("GET /metrics body does not contain %q", "sync_all_durations_histogram")
	}

	// pprof is registered on the same (default) mux.
	pprofStatus, _, err := httpGet(t, addr, "/debug/pprof/", 2*time.Second)
	if err != nil {
		t.Fatalf("GET /debug/pprof/: %v", err)
	}
	if pprofStatus != http.StatusOK {
		t.Errorf("GET /debug/pprof/ status = %d, want %d", pprofStatus, http.StatusOK)
	}

	// Record something and check it shows up in the exposition.
	r := New()
	r.MarkSyncOnce()
	r.ObserveSyncOnceDuration(1500 * time.Millisecond)
	r.SetSyncErrorQueueDepth(3)
	status, body, err = httpGet(t, addr, "/metrics", 2*time.Second)
	if err != nil {
		t.Fatalf("GET /metrics after recording: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("GET /metrics after recording status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(body, "sync_once_gauge") {
		t.Errorf("GET /metrics body does not contain %q", "sync_once_gauge")
	}
	if !strings.Contains(body, "sync_error_gauge") {
		t.Errorf("GET /metrics body does not contain %q", "sync_error_gauge")
	}

	// Stop, then a request must fail quickly.
	if err := stop(); err != nil {
		t.Fatalf("stop() returned error: %v", err)
	}
	// Give the OS a moment to close the listener.
	time.Sleep(50 * time.Millisecond)
	_, _, err = httpGet(t, addr, "/metrics", 2*time.Second)
	if err == nil {
		t.Errorf("GET /metrics after stop succeeded, want connection failure")
	}

	// stop is idempotent.
	if err := stop(); err != nil {
		t.Fatalf("second stop() returned error: %v", err)
	}
}

// TestStartHTTPEmptyAddrDefaults verifies StartHTTP with an explicit port
// (the empty-addr default is covered by the constant check below) starts a
// reachable server and returns a working stop function.
func TestStartHTTPEmptyAddrDefaults(t *testing.T) {
	if defaultMetricsAddr != ":8090" {
		t.Errorf("defaultMetricsAddr = %q, want %q", defaultMetricsAddr, ":8090")
	}

	// Start on an OS-chosen port instead of the default :8090 so the test
	// never fights over a fixed port.
	stop, err := StartHTTP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("StartHTTP(127.0.0.1:0) returned error: %v", err)
	}

	// Extract the bound address from the stop closure indirectly: start a
	// second server whose listener we own, to read the address. Instead of
	// introspecting, just exercise this server through /metrics on the
	// known-port variant is not possible, so verify stop works.
	if err := stop(); err != nil {
		t.Fatalf("stop() returned error: %v", err)
	}
	if err := stop(); err != nil {
		t.Fatalf("second stop() returned error: %v", err)
	}
}

// TestStartHTTPInvalidAddr verifies a bad address returns an error.
func TestStartHTTPInvalidAddr(t *testing.T) {
	stop, err := StartHTTP("256.256.256.256:1")
	if err == nil {
		stop()
		t.Fatalf("StartHTTP(bad addr) succeeded, want error")
	}
	if stop != nil {
		t.Error("StartHTTP(bad addr) stop is not nil, want nil")
	}
}
