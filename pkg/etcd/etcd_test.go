package etcd

import (
	"runtime"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// TestNewClientWithEndpointsUnreachableFailsFast mirrors the elector
// constructor guard at the client layer: a refused endpoint must surface
// an error through the 5s MemberList probe instead of returning a dead
// client (clientv3.New dials lazily).
func TestNewClientWithEndpointsUnreachableFailsFast(t *testing.T) {
	type result struct {
		client *clientv3.Client
		err    error
	}
	results := make(chan result, 1)
	go func() {
		client, err := NewClientWithEndpoints([]string{"127.0.0.1:1"}, "", "", "")
		results <- result{client, err}
	}()

	select {
	case res := <-results:
		if res.err == nil {
			if res.client != nil {
				res.client.Close()
			}
			t.Fatal("NewClientWithEndpoints() error = nil, want error for refused endpoint")
		}
		if res.client != nil {
			t.Fatalf("NewClientWithEndpoints() client = %v, want nil on error", res.client)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("NewClientWithEndpoints() did not fail within 8s for a refused endpoint")
	}
}

// TestNewClientWithEndpointsClosesClientOnProbeFailure verifies the F4
// regression: a client whose MemberList probe fails must be closed, not
// leaked. clientv3.Client is a concrete type without a close hook, so the
// leak is observed indirectly through goroutine counts: every unclosed
// client pins its gRPC connection goroutines. The assertion is a
// documented approximation (bounded sampling), complemented by go vet and
// code review of the close path.
func TestNewClientWithEndpointsClosesClientOnProbeFailure(t *testing.T) {
	baseline := goroutineMin(200 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if _, err := NewClientWithEndpoints([]string{"127.0.0.1:1"}, "", "", ""); err == nil {
			t.Fatal("NewClientWithEndpoints() error = nil, want error for refused endpoint")
		}
	}
	assertGoroutinesAtMost(t, baseline+2, 3*time.Second)
}

// goroutineMin samples runtime.NumGoroutine for dur and returns the minimum
// seen, smoothing out transient runtime and test-framework goroutines.
func goroutineMin(dur time.Duration) int {
	minimum := runtime.NumGoroutine()
	deadline := time.Now().Add(dur)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		if n := runtime.NumGoroutine(); n < minimum {
			minimum = n
		}
	}
	return minimum
}

// assertGoroutinesAtMost polls until the sampled goroutine count settles at
// or below limit, failing the test when the deadline passes. Leaked
// goroutines keep the count permanently high, so a single low sample only
// occurs after genuine cleanup.
func assertGoroutinesAtMost(t *testing.T, limit int, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if runtime.NumGoroutine() <= limit {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("goroutine count did not settle at or below %d within %v (now %d)",
		limit, deadline, runtime.NumGoroutine())
}
