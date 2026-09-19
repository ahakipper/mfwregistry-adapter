package notice

import (
	"strings"
	"sync"
	"testing"
)

func TestFailClosedNotifierCountsWithoutClaimingDelivery(t *testing.T) {
	logger := &fakeLogger{}
	n := NewFailClosed("external delivery excluded", logger)
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
	if strings.Contains(strings.ToLower(joined), "appcenter") {
		t.Fatalf("generic fail-closed notifier retained AppCenter behavior: %q", joined)
	}
}

type fakeLogger struct {
	mu     sync.Mutex
	errors []string
}

func (f *fakeLogger) Info(...interface{})                       {}
func (f *fakeLogger) Infof(string, ...interface{})              {}
func (f *fakeLogger) Warn(...interface{})                       {}
func (f *fakeLogger) Warnf(string, ...interface{})              {}
func (f *fakeLogger) Error(args ...interface{})                 { f.record("error", args...) }
func (f *fakeLogger) Errorf(format string, args ...interface{}) { f.record(format, args...) }

func (f *fakeLogger) record(prefix string, args ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rendered := prefix
	for _, arg := range args {
		if err, ok := arg.(error); ok {
			rendered += " " + err.Error()
			continue
		}
		if value, ok := arg.(string); ok {
			rendered += " " + value
		}
	}
	f.errors = append(f.errors, rendered)
}

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
