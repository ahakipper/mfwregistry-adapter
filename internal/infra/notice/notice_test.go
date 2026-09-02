package notice

import (
	"strings"
	"sync"
	"testing"
	"time"

	"spotter/internal/ports"
)

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

// TestNewWithLoggerNotifyDoesNotPanic verifies that Notify with a fake
// logger neither panics nor blocks the caller.
//
// Limitation (documented in notice.go): the underlying
// appcenternotice.Noticer always succeeds offline and logs the delivered
// notice through the pkg/log global logger only when that global is
// initialized; in tests it is nil, so no global output is produced and the
// fake logger stays offline. The test therefore only asserts async
// completion (no panic inside the goroutine) and that no error is logged.
func TestNewWithLoggerNotifyDoesNotPanic(t *testing.T) {
	logger := &fakeLogger{}
	notifier := NewWithLogger("app-code", "key", "test", logger)
	if notifier == nil {
		t.Fatalf("NewWithLogger() returned nil")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		notifier.Notify("title", "content")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Notify blocked the caller")
	}

	// The async send runs in its own goroutine; wait for it to be scheduled
	// so a panic inside it would fail the test.
	time.Sleep(100 * time.Millisecond)

	if got := logger.errorCount(); got != 0 {
		t.Errorf("fake logger recorded %d errors, want 0: %v", got, logger.errors)
	}
}

// TestNewDefaultsToNopLogger verifies New (nil logger) and repeated Notify
// calls do not panic.
func TestNewDefaultsToNopLogger(t *testing.T) {
	notifier := New("app-code", "key", "dev")
	if notifier == nil {
		t.Fatalf("New() returned nil")
	}
	for i := 0; i < 5; i++ {
		notifier.Notify("title", "content")
	}
	// Give the goroutines time to run so a panic surfaces.
	time.Sleep(100 * time.Millisecond)
}

// TestNotifierImplementsPort verifies the compile-time interface assertion
// holds for the concrete type as well.
func TestNotifierImplementsPort(t *testing.T) {
	var n ports.Notifier = New("app-code", "key", "product")
	n.Notify("title", "content")
	time.Sleep(50 * time.Millisecond)
}
