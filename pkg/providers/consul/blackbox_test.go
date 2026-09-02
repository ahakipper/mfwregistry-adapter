package consul

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/internal/testkit/consulmock"
	"spotter/internal/testkit/fakes"
)

// The black-box tier for package consul exercises the public contracts of
// ClientFactorySimple (NewClientFactory, ConsulClientFactory) and Monitor
// (NewConsulMonitor, AppendInstanceHandler, Start) against the in-process
// consulmock HTTP server, with fakes for the logger and the notifier. All
// listeners are loopback-only.

// TestBlackboxClientFactoryFailoverSelectsSecondServer: two consulmock
// servers; the first is degraded (empty leader) while the second stays
// healthy. The factory must probe both addresses exactly once, fail over and
// return a client whose leader is the second server's.
func TestBlackboxClientFactoryFailoverSelectsSecondServer(t *testing.T) {
	degraded := consulmock.Start()
	defer degraded.Close()
	degraded.SetLeader("")

	healthy := consulmock.Start()
	defer healthy.Close()
	healthy.SetLeader("127.0.0.1:8302")

	factory, err := NewClientFactory([]string{degraded.Address(), healthy.Address()}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	client, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("ConsulClientFactory() error = %v, want failover to the healthy server", err)
	}
	leader, err := client.Status().Leader()
	if err != nil {
		t.Fatalf("selected client leader probe: %v", err)
	}
	if leader != "127.0.0.1:8302" {
		t.Fatalf("selected client leader = %q, want the healthy server leader 127.0.0.1:8302", leader)
	}

	// Exact probe request counts: the factory probes the degraded address
	// once (fails over), the healthy address once (selected). The leader
	// read above adds one more probe to the healthy server, so the healthy
	// server observed 2 leader requests in total.
	assertBlackboxLeaderRequests(t, degraded, 1, "degraded server")
	assertBlackboxLeaderRequests(t, healthy, 2, "healthy server")
}

// TestBlackboxClientFactoryFailoverAfterCacheDegradation: the first server is
// healthy and gets cached; it then degrades (SetLeader("")). The next factory
// call must evict the cached client and fail over to the second server,
// asserting the exact probe counts on both servers.
func TestBlackboxClientFactoryFailoverAfterCacheDegradation(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()
	first.SetLeader("127.0.0.1:8301")

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("127.0.0.1:8302")

	factory, err := NewClientFactory([]string{first.Address(), second.Address()}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	// Warm the cache with the first server; the second is never probed.
	cached, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("first ConsulClientFactory() error = %v", err)
	}
	assertBlackboxLeaderRequests(t, first, 1, "first server before degradation")
	assertBlackboxLeaderRequests(t, second, 0, "second server before degradation")

	// Degrade the cached server: the next call must evict and fail over.
	first.SetLeader("")
	fallback, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("second ConsulClientFactory() error = %v, want failover after cache degradation", err)
	}
	if fallback == cached {
		t.Fatal("ConsulClientFactory() returned the degraded cached client, want a failover client")
	}
	leader, err := fallback.Status().Leader()
	if err != nil {
		t.Fatalf("failover client leader probe: %v", err)
	}
	if leader != "127.0.0.1:8302" {
		t.Fatalf("failover client leader = %q, want the second server leader 127.0.0.1:8302", leader)
	}

	// Probe counts: the first server was probed at warm-up (1), re-probed
	// during eviction (2); the second server was probed once for failover
	// plus once for the leader read above (2).
	assertBlackboxLeaderRequests(t, first, 2, "first server after degradation")
	assertBlackboxLeaderRequests(t, second, 2, "second server after failover")
}

// TestBlackboxClientFactoryAllServersDegradedReturnsError: with both servers
// degraded, the factory returns an error naming both addresses and probes
// each address exactly once.
func TestBlackboxClientFactoryAllServersDegradedReturnsError(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()
	first.SetLeader("")

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("")

	factory, err := NewClientFactory([]string{first.Address(), second.Address()}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	client, err := factory.ConsulClientFactory()
	if err == nil {
		t.Fatal("ConsulClientFactory() error = nil, want an error when every server is degraded")
	}
	if client != nil {
		t.Fatalf("ConsulClientFactory() client = %#v, want nil", client)
	}
	if got := err.Error(); !containsAll(got, first.Address(), second.Address(), "no valid Consul client") {
		t.Fatalf("ConsulClientFactory() error = %q, want both addresses and the aggregate message", got)
	}
	assertBlackboxLeaderRequests(t, first, 1, "first degraded server")
	assertBlackboxLeaderRequests(t, second, 1, "second degraded server")
}

// TestBlackboxMonitorDebounce: the real Monitor (NewConsulMonitor over a
// consulmock-backed ClientFactorySimple) debounces health-state index
// changes: the initial watch transition and a burst of AdvanceIndex calls
// each settle into exactly one instance handler invocation, not one per
// change.
//
// The monitor's watch loop polls /v1/health/state/any every 50ms
// (periodicCheckTime) and only signals a change when X-Consul-Index moves;
// the update loop runs the handlers once the index stays stable for 50ms
// (refreshIdleTime). The test uses the real clock (the debounce constants
// are 50ms), so every wait is bounded at 3s.
func TestBlackboxMonitorDebounce(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{server.Address()}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	monitor, err := NewConsulMonitor(factory, &fakes.FakeLogger{}, &fakes.FakeNotifier{}, nil)
	if err != nil {
		t.Fatalf("NewConsulMonitor() error = %v", err)
	}

	var handlerCalls int32
	handlerDone := make(chan struct{})
	var once sync.Once
	monitor.AppendInstanceHandler(func(*api.CatalogService) error {
		atomic.AddInt32(&handlerCalls, 1)
		once.Do(func() { close(handlerDone) })
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	monitorDone := make(chan error, 1)
	go func() { monitorDone <- monitor.Start(ctx) }()

	// Wait until the watch loop is polling the health state endpoint (the
	// loop starts polling immediately; the requests prove it is live).
	awaitBlackboxRequestCount(t, server, "/v1/health/state/any", 1)

	// The initial index transition (0 -> 1) settles into the first handler
	// invocation: one call for one settled change.
	awaitBlackboxHandlerDone(t, handlerDone, "initial index change")

	baseline := atomic.LoadInt32(&handlerCalls)

	// A burst of index changes inside the debounce window must collapse
	// into exactly one further handler invocation.
	server.AdvanceIndex()
	server.AdvanceIndex()
	server.AdvanceIndex()

	// Bounded wait for the burst to settle (50ms debounce, 50ms poll; the
	// 3s bound leaves ample margin), then assert only one new invocation.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&handlerCalls) > baseline {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // let any spurious extra debounce settle
	if got := atomic.LoadInt32(&handlerCalls); got != baseline+1 {
		t.Fatalf("instance handler invocations after burst = %d (baseline %d), want exactly %d (one per settled change)", got, baseline, baseline+1)
	}

	cancel()
	select {
	case <-monitorDone:
	case <-time.After(3 * time.Second):
		t.Fatal("monitor Start() did not return after context cancel")
	}
}

// TestBlackboxMonitorStartReturnsOnCancel: the composed Start (watchConsul +
// updateRecord errgroup) returns nil promptly once the context is canceled.
func TestBlackboxMonitorStartReturnsOnCancel(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{server.Address()}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}
	monitor, err := NewConsulMonitor(factory, &fakes.FakeLogger{}, &fakes.FakeNotifier{}, nil)
	if err != nil {
		t.Fatalf("NewConsulMonitor() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	monitorDone := make(chan error, 1)
	go func() { monitorDone <- monitor.Start(ctx) }()

	awaitBlackboxRequestCount(t, server, "/v1/health/state/any", 1)
	cancel()

	select {
	case err := <-monitorDone:
		if err != nil {
			t.Fatalf("monitor Start() error = %v, want nil on cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("monitor Start() did not return after context cancel")
	}
}

// assertBlackboxLeaderRequests asserts the exact number of leader probes a
// consulmock server observed, and that every request was a GET to
// /v1/status/leader with an empty query.
func assertBlackboxLeaderRequests(t *testing.T, server *consulmock.Server, want int, name string) {
	t.Helper()

	requests := server.Requests()
	if len(requests) != want {
		t.Fatalf("%s requests = %d, want %d leader probes; requests = %#v", name, len(requests), want, requests)
	}
	for i, request := range requests {
		if request.Method != "GET" {
			t.Fatalf("%s request %d method = %q, want GET", name, i, request.Method)
		}
		if request.Path != "/v1/status/leader" {
			t.Fatalf("%s request %d path = %q, want /v1/status/leader", name, i, request.Path)
		}
		if len(request.Query) != 0 {
			t.Fatalf("%s request %d query = %v, want empty query", name, i, request.Query)
		}
	}
}

// awaitBlackboxHandlerDone waits for the handler-done channel to close.
func awaitBlackboxHandlerDone(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for the instance handler after %s", name)
	}
}

// awaitBlackboxRequestCount polls until the server has observed at least want
// requests for path, or fails after a 2s bound.
func awaitBlackboxRequestCount(t *testing.T, server *consulmock.Server, path string, want int) {
	t.Helper()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		count := 0
		for _, request := range server.Requests() {
			if request.Path == path {
				count++
			}
		}
		if count >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d requests to %s", want, path)
		case <-ticker.C:
		}
	}
}

func containsAll(haystack string, needles ...string) bool {
	for _, needle := range needles {
		if !contains(haystack, needle) {
			return false
		}
	}
	return true
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
