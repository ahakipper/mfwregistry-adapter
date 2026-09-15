package notice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewHTTPRequiresDeploymentContract(t *testing.T) {
	logger := &fakeLogger{}
	base := HTTPConfig{Timeout: time.Second, BuildRequest: func(ctx context.Context, endpoint string, message Message) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	}}
	for name, cfg := range map[string]HTTPConfig{
		"endpoint": base,
		"auth":     func() HTTPConfig { c := base; c.Endpoint = "http://127.0.0.1"; return c }(),
		"timeout": func() HTTPConfig {
			c := base
			c.Endpoint = "http://127.0.0.1"
			c.AuthToken = "token"
			c.Timeout = 0
			return c
		}(),
		"builder": func() HTTPConfig {
			c := base
			c.Endpoint = "http://127.0.0.1"
			c.AuthToken = "token"
			c.BuildRequest = nil
			return c
		}(),
	} {
		if _, err := NewHTTP(cfg, logger); err == nil {
			t.Errorf("NewHTTP(%s) returned nil error", name)
		}
	}
}

func TestHTTPNotifierRetriesAndCountsFailures(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Errorf("Authorization = %q, want Bearer token", got)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	logger := &fakeLogger{}
	n, err := NewHTTP(HTTPConfig{
		Endpoint: server.URL, AppCode: "spotter", Env: "test", AuthToken: "token",
		Timeout: time.Second, MaxRetries: 1, BuildRequest: func(ctx context.Context, endpoint string, message Message) (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodPost, endpoint, http.NoBody)
		},
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	n.Notify("title", "content")
	deadline := time.Now().Add(2 * time.Second)
	for n.SuccessCount() == 0 && n.FailureCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.SuccessCount() != 1 || n.FailureCount() != 0 || calls.Load() != 2 {
		t.Fatalf("counts success=%d failure=%d calls=%d", n.SuccessCount(), n.FailureCount(), calls.Load())
	}
}

func TestHTTPNotifierDoesNotRetryPermanentClientError(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(http.StatusForbidden) }))
	defer server.Close()
	n, err := NewHTTP(HTTPConfig{Endpoint: server.URL, AuthKey: "key", Timeout: time.Second, MaxRetries: 4, BuildRequest: func(ctx context.Context, endpoint string, message Message) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, endpoint, io.NopCloser(strings.NewReader("x")))
	}}, &fakeLogger{})
	if err != nil {
		t.Fatal(err)
	}
	n.Notify("title", "content")
	deadline := time.Now().Add(2 * time.Second)
	for n.FailureCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.FailureCount() != 1 || calls.Load() != 1 {
		t.Fatalf("failure=%d calls=%d", n.FailureCount(), calls.Load())
	}
}

func TestFailClosedNotifierCountsWithoutClaimingDelivery(t *testing.T) {
	logger := &fakeLogger{}
	n := NewFailClosed("missing appcenter contract", logger)
	n.Notify("title", "content")
	if n.FailureCount() != 1 {
		t.Fatalf("FailureCount=%d, want 1", n.FailureCount())
	}
	logger.mu.Lock()
	joined := strings.Join(logger.errors, " ")
	logger.mu.Unlock()
	if strings.Contains(joined, "content") || strings.Contains(joined, "title") {
		t.Fatalf("fail-closed log leaked notification payload: %q", joined)
	}
}

func TestHTTPNotifierCloseWaitsForAcceptedDelivery(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(finished)
	}))
	defer server.Close()
	n, err := NewHTTP(HTTPConfig{
		Endpoint: server.URL, AuthKey: "key", Timeout: time.Second,
		BuildRequest: func(ctx context.Context, endpoint string, _ Message) (*http.Request, error) {
			return http.NewRequestWithContext(ctx, http.MethodPost, endpoint, http.NoBody)
		},
	}, &fakeLogger{})
	if err != nil {
		t.Fatal(err)
	}
	n.Notify("title", "content")
	<-started
	done := make(chan struct{})
	go func() {
		_ = n.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel delivery promptly")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("server handler did not observe request cancellation")
	}
	if n.FailureCount() != 1 {
		t.Fatalf("FailureCount=%d, want 1 after cancellation", n.FailureCount())
	}
}

// fakeLogger records Errorf calls for inspection.
type fakeLogger struct {
	mu      sync.Mutex
	errors  []string
	blocker chan struct{} // when non-nil, Errorf signals on it
}

func (f *fakeLogger) Info(args ...interface{})                 {}
func (f *fakeLogger) Infof(format string, args ...interface{}) {}
func (f *fakeLogger) Warn(args ...interface{})                 {}
func (f *fakeLogger) Warnf(format string, args ...interface{}) {}

func (f *fakeLogger) Error(args ...interface{}) {
	f.record("error", args...)
}

func (f *fakeLogger) Errorf(format string, args ...interface{}) {
	f.record(format, args...)
}

func (f *fakeLogger) record(prefix string, args ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rendered := prefix
	for _, arg := range args {
		if err, ok := arg.(error); ok {
			rendered += " " + err.Error()
			continue
		}
		if s, ok := arg.(string); ok {
			rendered += " " + s
		}
	}
	f.errors = append(f.errors, rendered)
	if f.blocker != nil {
		close(f.blocker)
		f.blocker = nil
	}
}

// errorCount returns the number of recorded error calls.
func (f *fakeLogger) errorCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.errors)
}

// TestLocalIPOk verifies LocalIP returns a non-empty, error-free address on
// this machine (at minimum loopback interfaces exist, so InterfaceAddrs
// must succeed).
func TestLocalIPOk(t *testing.T) {
	ip, err := LocalIP()
	if err != nil {
		t.Fatalf("LocalIP() returned error: %v", err)
	}
	if ip == "" {
		t.Fatalf("LocalIP() returned empty ip and no error")
	}
	if !strings.Contains(ip, ".") && !strings.Contains(ip, ":") {
		t.Errorf("LocalIP() = %q, want a dotted IPv4 or colon IPv6 address", ip)
	}
}
