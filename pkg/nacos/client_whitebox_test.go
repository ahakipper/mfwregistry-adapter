// Package nacos holds the whitebox tier of the Nacos adapter's client: the
// transport construction pins that must read the unexported construction
// values directly (dsca-2 DS-2-4). The black-box suite stays in
// nacos_test; only the assertions that need the Client's internals live
// here.
package nacos

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"spotter/internal/testkit/fakes"
)

// TestClientTransportConstructionPinned pins the explicit Transport's
// construction values (dsca-2 DS-2-4): a zero-Transport client inherits
// http.DefaultTransport with an effective MaxIdleConnsPerHost of 2 — the
// connection churn the audit measured (425-624 TCP connections for 1000
// concurrent registers, then only 2 kept idle). The tuned values are tied
// to the sink's push concurrency: the idle pool keeps one warm connection
// per worker, and MaxConnsPerHost caps the total at the same number (the
// breaker that bounds a wedged nacos to 8 held workers, not the ants
// pool's 100). The outer 10s RequestTimeout is preserved.
func TestClientTransportConstructionPinned(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1", &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	transport, ok := client.http.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client transport = %T, want *http.Transport (explicitly constructed, not the default)", client.http.Transport)
	}
	if got := transport.MaxIdleConnsPerHost; got != transportMaxIdleConnsPerHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want %d (tied to the sink's push concurrency, not the effective default 2)", got, transportMaxIdleConnsPerHost)
	}
	if got := transport.MaxConnsPerHost; got != transportMaxConnsPerHost {
		t.Fatalf("MaxConnsPerHost = %d, want %d (the per-host breaker)", got, transportMaxConnsPerHost)
	}
	if got := transport.MaxIdleConns; got != transportMaxIdleConns {
		t.Fatalf("MaxIdleConns = %d, want %d", got, transportMaxIdleConns)
	}
	if got := transport.IdleConnTimeout; got != transportIdleConnTimeout {
		t.Fatalf("IdleConnTimeout = %v, want %v (the default transport's 90s, kept)", got, transportIdleConnTimeout)
	}
	if got := client.http.Timeout; got != RequestTimeout {
		t.Fatalf("client Timeout = %v, want the preserved %v (it covers the body read)", got, RequestTimeout)
	}
}

// TestClientTransportTiedToPushConcurrency pins the LOAD-BEARING
// relationship, not just the numbers: the transport's per-host allowance
// must track the sink's push concurrency (a transport below it starves the
// worker group; one far above it wastes connections). If either side is
// retuned without the other, this fails — the invariant the contract
// calls "<concurrency>/<cap> tied to the DS-2-1 semaphore".
func TestClientTransportTiedToPushConcurrency(t *testing.T) {
	if transportMaxConnsPerHost < DefaultPushConcurrency {
		t.Fatalf("MaxConnsPerHost (%d) < DefaultPushConcurrency (%d): the transport starves the worker group", transportMaxConnsPerHost, DefaultPushConcurrency)
	}
	if transportMaxIdleConnsPerHost < DefaultPushConcurrency {
		t.Fatalf("MaxIdleConnsPerHost (%d) < DefaultPushConcurrency (%d): past the second worker every push dials fresh (the measured churn)", transportMaxIdleConnsPerHost, DefaultPushConcurrency)
	}
}

// TestClientConcurrentRequestsReuseConnections pins the reuse behavior the
// tuning exists for: two consecutive 16-request concurrent waves against a
// loopback server are served by at most transportMaxConnsPerHost
// connections TOTAL — the cap shapes the first wave and the idle pool
// keeps those connections warm for the second (the default transport's
// effective MaxIdleConnsPerHost=2 would retire the wave's connections and
// re-dial, growing the distinct-connection count past the first wave's).
// The count is taken server-side (distinct RemoteAddr values), the cheap
// and robust observation.
func TestClientConcurrentRequestsReuseConnections(t *testing.T) {
	var mu sync.Mutex
	remoteAddrs := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		remoteAddrs[request.RemoteAddr] = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	runWave := func() {
		var wave sync.WaitGroup
		for i := 0; i < 16; i++ {
			wave.Add(1)
			go func(i int) {
				defer wave.Done()
				params := InstanceParams{
					ServiceName: fmt.Sprintf("svc-%d", i%4),
					IP:          "10.0.0.1",
					Port:        8080,
					ClusterName: "k8s",
					Ephemeral:   false,
				}
				if err := client.RegisterInstance(params); err != nil {
					t.Errorf("RegisterInstance(%d) error = %v", i, err)
				}
			}(i)
		}
		wave.Wait()
	}

	// Wave 1: 16 concurrent requests are shaped by the transport's
	// per-host cap (the requests beyond the cap queue on the dial limiter,
	// which is exactly the intended bounding).
	runWave()
	mu.Lock()
	first := len(remoteAddrs)
	mu.Unlock()
	if first == 0 {
		t.Fatal("no connections observed")
	}
	if first > transportMaxConnsPerHost {
		t.Fatalf("connections for 16 concurrent requests = %d, want <= %d (the transport's per-host cap)", first, transportMaxConnsPerHost)
	}

	// Wave 2 (same concurrency): the idle pool must serve it WITHOUT
	// dialing fresh — the distinct-connection count must not grow. The
	// default transport would collapse to 2 idle and re-dial a full set.
	runWave()
	mu.Lock()
	second := len(remoteAddrs)
	mu.Unlock()
	if second > transportMaxConnsPerHost {
		t.Fatalf("connections after two waves = %d, want <= %d (the cap holds across waves)", second, transportMaxConnsPerHost)
	}
	if second > first {
		t.Fatalf("the second wave dialed %d NEW connections (wave 1 held %d) — the idle pool should keep the wave's connections warm", second-first, first)
	}
}

// TestClientTransportDialTimeoutBoundsConnectionSetup pins the dialer's
// bound: an unroutable address fails within ~2s (the contract's
// transportDialTimeout), not the 10s RequestTimeout — fail-fast on a dead
// server instead of holding a worker five times longer.
func TestClientTransportDialTimeoutBoundsConnectionSetup(t *testing.T) {
	// 203.0.113.0/24 is TEST-NET-3: unroutable by construction, so the
	// dialer's timeout (not a refusal) is what fires.
	client, err := NewClient("http://203.0.113.1:1", &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	start := time.Now()
	err = client.RegisterInstance(InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 8080, ClusterName: "k8s", Ephemeral: false})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("RegisterInstance(unroutable) error = nil, want a dial failure")
	}
	if elapsed >= RequestTimeout {
		t.Fatalf("dial to unroutable address took %v, want < the 10s RequestTimeout (the 2s dialer bound fails fast)", elapsed)
	}
}
