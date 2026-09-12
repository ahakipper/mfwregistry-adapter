package consul

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/config"
	"spotter/internal/testkit/consulmock"
	"spotter/internal/testkit/fakes"
	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/log"
	"spotter/pkg/notice"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

// TestMain isolates the legacy package globals the consul provider depends
// on (docs/testing.md section 7, "legacy globals"): pkg/log writes app.log
// into config.LogFilePath and pkg/notice delivers through log.Logger. Point
// both at a per-test-run temporary directory so no artifacts land in the
// repository (the same guard the k8s whitebox suite uses).
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "consul-blackbox-")
	if err != nil {
		panic(err)
	}
	config.LogFilePath = dir + string(os.PathSeparator)
	config.LogToStd = false
	if err := log.LoggerInit(); err != nil {
		panic(err)
	}
	notice.InitNoticeClient("test")
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

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

// -----------------------------------------------------------------------------
// ProcessIntervalFullPush SyncAll trigger (plan §7.4)
// -----------------------------------------------------------------------------

// fakeWorker records every Handle call and serves a scripted GetAll
// response; the consul provider's SyncAll-trigger tests drive
// ProcessIntervalFullPush through it.
type fakeWorker struct {
	handles []*worker.Event
	mu      sync.Mutex
}

func (w *fakeWorker) AddEventHandler(opt worker.OperateType, handler worker.EventResourceHandler) {}

func (w *fakeWorker) Handle(d *worker.Event) {
	if d == nil {
		return
	}
	w.mu.Lock()
	w.handles = append(w.handles, d)
	w.mu.Unlock()
}

func (w *fakeWorker) ProcessUnsynced() {}

func (w *fakeWorker) GetAll(enable []int32, provider string) (*sv.InstanceList, error) {
	return &sv.InstanceList{}, nil
}

func (w *fakeWorker) handleSnapshot() []*worker.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]*worker.Event(nil), w.handles...)
}

func (w *fakeWorker) syncAllEvents() []*worker.Event {
	var found []*worker.Event
	for _, e := range w.handleSnapshot() {
		if e.Operate == worker.OperateTypeSyncAll {
			found = append(found, e)
		}
	}
	return found
}

// newBlackboxConsulProvider builds a consul provider over the consulmock
// server, the fake worker and the given push interval (seconds).
func newBlackboxConsulProvider(t *testing.T, server *consulmock.Server, w worker.Worker, interval int, ctx context.Context) *consul {
	t.Helper()
	provider, err := NewConsulProvider(ctx, w, interval, []string{server.Address()})
	if err != nil {
		t.Fatalf("NewConsulProvider() error = %v", err)
	}
	return provider.(*consul)
}

// TestBlackboxConsulIntervalFullPushEmitsSyncAll: after each interval tick's
// CompareAndFlush, the consul provider also emits exactly one
// OperateTypeSyncAll event carrying its full instance list — the dormant
// worker path plan §7.4 revives so every sink's full-push reconcile (the
// Nacos PushAll prune included) runs each push interval.
func TestBlackboxConsulIntervalFullPushEmitsSyncAll(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	// One convertible consul service instance (the full meta schema the
	// converter requires, docs/nacos-sink-plan.md §8.2).
	server.SetServices(map[string][]string{"pay-user": {"microservice"}})
	server.SetEntries("pay-user", []*api.ServiceEntry{
		{
			Node: &api.Node{Node: "node-1", Address: "10.0.0.1"},
			Service: &api.AgentService{ID: "srv-a", Service: "pay-user", Port: 8081,
				Tags: []string{"microservice"},
				Meta: map[string]string{
					"appCode":    "pay-user",
					"envType":    "test",
					"envGroup":   "7",
					"instanceId": "srv-a",
					"version":    "v1",
					"namespace":  "default",
					"ports":      `[{"Name":"http","Protocol":"http","Port":8081}]`,
				}},
		},
	})
	server.AdvanceIndex()

	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// interval 1 comes from the CONSTRUCTOR argument (NewConsulProvider
	// assigns it — the b3578b6 behavior): no post-construction overwrite, so
	// the tick cadence under test is exactly the production wiring.
	c := newBlackboxConsulProvider(t, server, w, 1, ctx)

	done := make(chan struct{})
	go func() {
		c.ProcessIntervalFullPush()
		close(done)
	}()

	// One 1s tick must produce exactly one SyncAll event carrying the full
	// consul instance list (bounded wait: one tick plus margin).
	deadline := time.Now().Add(3 * time.Second)
	var syncAlls []*worker.Event
	for time.Now().Before(deadline) {
		if syncAlls = w.syncAllEvents(); len(syncAlls) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(syncAlls) != 1 {
		t.Fatalf("SyncAll events = %d, want exactly 1 per tick; recorded = %#v", len(syncAlls), w.handleSnapshot())
	}
	event := syncAlls[0]
	if len(event.Data) != 1 || event.Data[0].InstanceId != "srv-a" {
		t.Fatalf("SyncAll data = %#v, want the full consul list (srv-a)", event.Data)
	}
	if event.Trigger <= 0 {
		t.Fatalf("SyncAll trigger = %d, want the tick timestamp", event.Trigger)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}
}

// TestBlackboxConsulIntervalFullPushEmitsEmptySyncAllWithoutInstances: a
// tick whose consul source is empty (GetAll returns nothing) still emits
// exactly one SyncAll event carrying an EMPTY data list (AUDIT-B-4,
// mirroring the k8s provider): the emission keeps the full-push reconcile
// cadence uniform — every sink's PushAll runs each interval. The empty
// list itself is a conservative no-op at the Nacos sink (a bare empty
// push carries no provider identity, so nothing remembered is swept — see
// the sink's remembered field); the vanished-service heal rides the
// provider's non-empty pushes. No incremental (Sync) events may be
// emitted.
func TestBlackboxConsulIntervalFullPushEmitsEmptySyncAllWithoutInstances(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()
	// No services registered: GetAll yields nothing.

	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// interval 1 comes from the CONSTRUCTOR argument, like the sibling
	// SyncAll test: the tick cadence under test is the production wiring.
	c := newBlackboxConsulProvider(t, server, w, 1, ctx)

	done := make(chan struct{})
	go func() {
		c.ProcessIntervalFullPush()
		close(done)
	}()

	// Wait past one tick for the empty SyncAll event.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(w.syncAllEvents()) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	syncAlls := w.syncAllEvents()
	if len(syncAlls) != 1 {
		t.Fatalf("SyncAll events = %d, want exactly 1 per tick (an empty list still reconciles); recorded = %#v", len(syncAlls), w.handleSnapshot())
	}
	if syncAlls[0].Data != nil && len(syncAlls[0].Data) != 0 {
		t.Fatalf("SyncAll data = %#v, want an empty list (nil or zero-length)", syncAlls[0].Data)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}
	// No incremental pushes: an empty consul list produces no Sync events —
	// the single SyncAll is the only recorded Handle call.
	if events := w.handleSnapshot(); len(events) != 1 {
		t.Fatalf("events = %d, want exactly 1 (the empty SyncAll only; no incremental pushes)", len(events))
	}
}

// TestBlackboxConsulProviderConstructorAssignsInterval (AUDIT-C-10): the
// constructor-level pin that the push-interval argument reaches the
// provider's interval field — the b3578b6 behavior the blackbox suite used
// to bypass by overwriting c.interval after construction, so deleting the
// constructor assignment stayed green. With the overwrite sites removed
// this is the only guard that fails when the assignment regresses to the
// "declared but never assigned" bug (the 21600s default silently ignoring
// the flag).
func TestBlackboxConsulProviderConstructorAssignsInterval(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := newBlackboxConsulProvider(t, server, w, 7, ctx)
	if c.interval != 7 {
		t.Fatalf("NewConsulProvider(7) interval = %d, want 7 (the constructor must assign the push-interval argument)", c.interval)
	}

	// Zero is passed through verbatim too: ProcessIntervalFullPush falls back
	// to the 21600s default only when the field is 0, so 0 must stay 0.
	zero := newBlackboxConsulProvider(t, server, w, 0, ctx)
	if zero.interval != 0 {
		t.Fatalf("NewConsulProvider(0) interval = %d, want 0 (zero keeps the default-interval fallback)", zero.interval)
	}
}

// TestBlackboxConsulIntervalFullPushSkipsSyncAllWhenSourceErrors (agent-2
// review of the AUDIT-B-4 fix): a tick whose consul source READ FAILS must
// NOT emit a SyncAll event. consul's GetAll returns nil on monitor errors
// as well as on legit-empty, and an error-time empty SyncAll would assert a
// false "everything vanished" full state to every sink — the Atlas
// full-sync carries the empty list server-side, whose semantics spotter
// does not own (the Nacos side is conservative for empty pushes, but the
// guard protects the whole fan-out). The error state is observable via the
// provider's sourceErr; the healthy side (legit empty still emits) is
// pinned by
// TestBlackboxConsulIntervalFullPushEmitsEmptySyncAllWithoutInstances.
//
// The failure is injected the same way monitor_test.go injects API errors:
// the provider's factory points at a consulmock server that has been
// closed, so GetServices fails with a connection error on every call.
func TestBlackboxConsulIntervalFullPushSkipsSyncAllWhenSourceErrors(t *testing.T) {
	server := consulmock.Start()
	address := server.Address()
	server.Close() // every subsequent API call fails: connection refused

	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Build the provider against a LIVE mock first (the constructor needs a
	// working address), then rebind its monitor's client factory at the
	// CLOSED address: every subsequent GetServices call dials a dead
	// endpoint and errors instead of answering an empty catalog.
	liveServer := consulmock.Start()
	defer liveServer.Close()
	c := newBlackboxConsulProvider(t, liveServer, w, 1, ctx)
	factory, err := NewClientFactory([]string{address}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}
	c.monitor.(*consulMonitor).clientFactory = factory

	// Sanity: the source read really errors (the distinction the fix
	// introduces), and it is not a legit empty.
	if _, err := c.monitor.GetServices(); err == nil {
		t.Fatal("monitor.GetServices() error = nil, want the closed-server error")
	}
	if err := c.sourceError(); err != nil { // before any GetAll: not yet read
		t.Fatalf("sourceError before the first GetAll = %v, want nil (not read yet)", err)
	}

	done := make(chan struct{})
	go func() {
		c.ProcessIntervalFullPush()
		close(done)
	}()

	// Wait past two ticks: the error skip must hold on EVERY tick, not just
	// the first (the tick retries the read; only a successful read emits).
	time.Sleep(2200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}

	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("events = %d, want 0 (a failed source read must not emit the destructive empty SyncAll); events = %#v", len(events), events)
	}
	if err := c.sourceError(); err == nil {
		t.Fatal("sourceError after the tick = nil, want the recorded read error")
	}
}

func TestBlackboxConsulSyncInstanceSourceErrorRetainsOldCache(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()
	server.SetServices(map[string][]string{"pay-user": {"microservice"}})
	server.SetEntries("pay-user", []*api.ServiceEntry{{
		Node: &api.Node{Node: "node-1", Address: "10.0.0.1"},
		Service: &api.AgentService{ID: "srv-a", Service: "pay-user", Port: 8081, Tags: []string{"microservice"}, Meta: map[string]string{
			"app-code": "pay-user", "env-type": providers.EnvTest, "version": "v1",
		}},
	}})
	c := newBlackboxConsulProvider(t, server, &fakeWorker{}, 0, context.Background())
	old := &sv.Instance{InstanceId: "old", Provider: "ecs", Reversion: 7, Status: providers.InstanceStatusOnline}
	c.cache.ReplaceOrInsert(old)
	failing := &flippingMonitor{fails: true}
	c.monitor = failing
	if err := c.syncInstance(); err == nil {
		t.Fatal("syncInstance() error = nil, want source failure")
	}
	if got := c.cache.Get("old"); got == nil || got.Reversion != old.Reversion {
		t.Fatalf("cache after source error = %#v, want old snapshot retained", got)
	}
}

func TestBlackboxConsulEmitSyncAllSourceErrorSafeFail(t *testing.T) {
	c := &consul{cache: providers.NewCache(8), worker: &fakeWorker{}, ctx: context.Background()}
	c.cache.ReplaceOrInsert(&sv.Instance{InstanceId: "old", Provider: "ecs", Reversion: 1})
	c.Lock()
	c.sourceErr = errors.New("source unavailable")
	c.Unlock()
	c.emitSyncAll()
	if got := len(c.worker.(*fakeWorker).syncAllEvents()); got != 0 {
		t.Fatalf("SyncAll events = %d, want 0 when source read failed", got)
	}
}

func TestBlackboxConsulEmptyFullRequiresThreeSuccessfulConfirmations(t *testing.T) {
	c := &consul{cache: providers.NewCache(8), worker: &fakeWorker{}, ctx: context.Background()}
	for i := 0; i < 2; i++ {
		_, _, valid, confirmed := c.snapshotForFullPush()
		if !valid || confirmed {
			t.Fatalf("empty confirmation %d = valid:%v confirmed:%v, want valid and unconfirmed", i+1, valid, confirmed)
		}
	}
	_, _, valid, confirmed := c.snapshotForFullPush()
	if !valid || !confirmed {
		t.Fatalf("third empty confirmation = valid:%v confirmed:%v, want confirmed", valid, confirmed)
	}
}

// flippingMonitor is a Monitor double whose GetServices error state can be
// flipped concurrently: it alternates between answering an empty catalog
// (legit empty) and a connection-shaped error (source read failure). It
// backs the race-pinning stress test below; everything but GetServices is
// inert (the test never Starts it and registers no handlers).
type flippingMonitor struct {
	mu    sync.Mutex
	fails bool
}

func (m *flippingMonitor) Start(ctx context.Context) error { return nil }

func (m *flippingMonitor) GetServices() (map[string][]string, error) {
	m.mu.Lock()
	fails := m.fails
	m.mu.Unlock()
	if fails {
		return nil, errors.New("flipping monitor: injected source read failure")
	}
	return map[string][]string{}, nil
}

func (m *flippingMonitor) GetServiceEntries(name string, q *api.QueryOptions) ([]*api.ServiceEntry, error) {
	return nil, nil
}

func (m *flippingMonitor) AppendServiceHandler(ServiceHandler)   {}
func (m *flippingMonitor) AppendInstanceHandler(InstanceHandler) {}

func (m *flippingMonitor) setFails(fails bool) {
	m.mu.Lock()
	m.fails = fails
	m.mu.Unlock()
}

// TestBlackboxConsulSourceErrTickAndHandlerConcurrent (round-2 review):
// the sourceErr flag is written by GetAll on BOTH the full-push tick path
// (emitSyncAll) and the monitor-dispatched handler path (syncInstance),
// which run on different goroutines — exactly the overlap the existing
// suites never produce. This test interleaves them for 50 iterations with
// the error state flipping between iterations, under the race detector:
// with the lock discipline in place (every sourceErr access under the
// provider lock) it stays clean, and the skipped-emit decision matches the
// error state the same tick observed. The pre-fix shape (emitSyncAll
// reading/writing sourceErr unlocked) trips -race on this test: the tick
// goroutine's GetAll write races the handler goroutine's GetAll write.
func TestBlackboxConsulSourceErrTickAndHandlerConcurrent(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newBlackboxConsulProvider(t, server, w, 1, ctx)
	monitor := &flippingMonitor{}
	c.monitor = monitor

	// 50 iterations of tick + handler interleaving with a flipping error
	// state: iteration i runs emitSyncAll (the tick path, goroutine A) and
	// syncInstance (the handler path, goroutine B) concurrently, then waits
	// for both and flips the error state for the next iteration.
	const iterations = 50
	for i := 0; i < iterations; i++ {
		monitor.setFails(i%2 == 0) // alternate: error tick, healthy tick, ...

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { // the ProcessIntervalFullPush tick path
			defer wg.Done()
			c.emitSyncAll()
		}()
		go func() { // the monitor-dispatched handler path
			defer wg.Done()
			_ = c.syncInstance() // locks, calls GetAll, writes sourceErr
		}()
		wg.Wait()

		// After both settle, the flag is consistent with the LAST GetAll
		// whichever path ran it (both see the same flip state): on odd
		// (healthy) iterations it must be nil, on even (failing) ones
		// non-nil — a stale-nil would be exactly the lost-update failure
		// mode the destructive empty SyncAll guards against.
		err := c.sourceError()
		if monitor.failsNow() && err == nil {
			t.Fatalf("iteration %d: sourceErr = nil after a failing GetAll, want the recorded error (lost update)", i)
		}
		if !monitor.failsNow() && err != nil {
			t.Fatalf("iteration %d: sourceErr = %v after a healthy GetAll, want nil", i, err)
		}
	}
}

func (m *flippingMonitor) failsNow() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fails
}

// -----------------------------------------------------------------------------
// Trigger unit widening: ns-since-epoch producers (dsca-2 §3 Option b)
// -----------------------------------------------------------------------------

// TestConsulEventsSyncEmitsNsEpochTrigger pins the unit widening at the
// consul event path (EventsSync's `time.Now().UnixNano()`): the consul
// sites are REQUIRED, not optional — the fan-out decorator cannot tell
// which provider emitted an Event, and a seconds-valued Trigger would
// make the e2e observation read ~57 years (the mixed-unit hazard dsca-2
// §6 row 2). The magnitude check is the mutation pin: a regression back
// to Unix() (seconds) yields ~1.7e9, five orders of magnitude below the
// ns epoch (~1.7e18).
func TestConsulEventsSyncEmitsNsEpochTrigger(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newBlackboxConsulProvider(t, server, w, 0, ctx)

	// One convertible instance per slice exercises all three EventsSync
	// arms (add, update, del) of the widened producer.
	ins := &sv.Instance{InstanceId: "srv-ns-epoch", Status: 1, Reversion: 1}
	c.EventsSync([]*sv.Instance{ins}, []*sv.Instance{ins}, []*sv.Instance{ins})

	events := w.handleSnapshot()
	if len(events) != 3 {
		t.Fatalf("worker.Handle calls = %d, want 3 (add+update+del); events = %#v", len(events), events)
	}
	now := time.Now().UnixNano()
	for i, e := range events {
		if e.Trigger <= 1e15 {
			t.Fatalf("event[%d] trigger = %d, want a ns-since-epoch magnitude (> 1e15; a seconds-valued Unix() trigger is ~1.7e9) — the consul producer regressed to whole seconds", i, e.Trigger)
		}
		if e.Trigger < now-int64(time.Hour) || e.Trigger > now+int64(time.Hour) {
			t.Fatalf("event[%d] trigger = %d, want within one hour of now-in-ns %d", i, e.Trigger, now)
		}
	}
}

// -----------------------------------------------------------------------------
// The nacos-authoritative reconcile compare (dsca-3 §3.3 / §3.5)
// -----------------------------------------------------------------------------

// scriptableWorker records every Handle call and serves a scripted GetAll
// response — the compare tests' seam. It differs from the SyncAll fakeWorker
// above only in serving the response.
type scriptableWorker struct {
	handles []*worker.Event
	mu      sync.Mutex

	getAllResponse *sv.InstanceList
}

func (w *scriptableWorker) AddEventHandler(opt worker.OperateType, handler worker.EventResourceHandler) {
}

func (w *scriptableWorker) Handle(d *worker.Event) {
	if d == nil {
		return
	}
	w.mu.Lock()
	w.handles = append(w.handles, d)
	w.mu.Unlock()
}

func (w *scriptableWorker) ProcessUnsynced() {}

func (w *scriptableWorker) GetAll(enable []int32, provider string) (*sv.InstanceList, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.getAllResponse, nil
}

func (w *scriptableWorker) handleSnapshot() []*worker.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]*worker.Event(nil), w.handles...)
}

// healthyEntry builds a passing "microservice" service entry: the converter
// yields Status=1, State=running, Enabled=true — the steady healthy shape.
func healthyEntry(id, ip string, modifyIndex uint64) *api.ServiceEntry {
	return &api.ServiceEntry{
		Node: &api.Node{Node: "node-" + id, Address: ip},
		Service: &api.AgentService{
			ID: id, Service: "pay-user", Port: 8081,
			Tags: []string{"microservice"},
			Meta: map[string]string{
				"appCode":    "pay-user",
				"envType":    "test",
				"envGroup":   "7",
				"instanceId": id,
				"version":    "v1",
				"namespace":  "default",
				"ports":      `[{"Name":"http","Protocol":"http","Port":8081}]`,
			},
			ModifyIndex: modifyIndex,
		},
		Checks: api.HealthChecks{
			{CheckID: "service:pay-user", Status: api.HealthPassing},
		},
	}
}

// unhealthyEntry builds a FAILING-check entry: the converter yields
// Status=2, State=probing, Enabled=TRUE (the convertion.go hardcode — the
// exact asymmetry the wire projection neutralizes).
func unhealthyEntry(id, ip string, modifyIndex uint64) *api.ServiceEntry {
	entry := healthyEntry(id, ip, modifyIndex)
	entry.Checks = api.HealthChecks{
		{CheckID: "service:pay-user", Status: api.HealthCritical},
	}
	return entry
}

// seedConsul seeds the consulmock with the given entries of the pay-user
// service and returns the converted local list (c.GetAll's shape).
func seedConsul(t *testing.T, server *consulmock.Server, entries ...*api.ServiceEntry) {
	t.Helper()
	server.SetServices(map[string][]string{"pay-user": {"microservice"}})
	server.SetEntries("pay-user", entries)
}

// staticMonitor is a Monitor double serving a fixed services/entries view
// — the injection seam for local shapes the HTTP-backed consulmock cannot
// serve through its health endpoint (a critical service check is filtered
// out of Health().Service(passingOnly=true); the provider's local list
// then holds the failing entry only when the monitor serves it directly,
// which is exactly what a real consul's health view does for the
// service-check-critical shape the converter maps to status 2).
type staticMonitor struct {
	entries map[string][]*api.ServiceEntry
}

func (m *staticMonitor) Start(ctx context.Context) error { return nil }

func (m *staticMonitor) GetServices() (map[string][]string, error) {
	services := map[string][]string{}
	for name := range m.entries {
		services[name] = []string{"microservice"}
	}
	return services, nil
}

func (m *staticMonitor) GetServiceEntries(name string, q *api.QueryOptions) ([]*api.ServiceEntry, error) {
	return m.entries[name], nil
}

func (m *staticMonitor) AppendServiceHandler(ServiceHandler)   {}
func (m *staticMonitor) AppendInstanceHandler(InstanceHandler) {}

// newStaticConsulProvider builds a consul provider whose monitor serves the
// given entries verbatim (the local-shape injection seam).
func newStaticConsulProvider(t *testing.T, w worker.Worker, entries map[string][]*api.ServiceEntry) *consul {
	t.Helper()
	provider, err := NewConsulProvider(context.Background(), w, 0, []string{"127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewConsulProvider() error = %v", err)
	}
	c := provider.(*consul)
	c.monitor = &staticMonitor{entries: entries}
	return c
}

// TestConsulNacosReconcileStatus2PairNoDiffNoPush: THE DS-5-2 neutralization
// contract (dsca-3 §3.5): a local status-2 ecs instance that exists in the
// nacos catalog as enabled=false / metadata status "2" produces NO diff and
// NO push at steady state, tick after tick. The pair is exercised where it
// is most dangerous — the field compare reached with the status-2 instance
// PRESENT in the remote map (the widened-request shape the [online] guard
// currently masks): the wire projection makes the compare pass regardless
// (wireEnabled(local)=false vs remote.Enabled=false), so the pin holds
// through BOTH the projection and the accidental guards, and stays green
// whichever mechanism carries it — while asserting the observable (no
// push), so it goes red the moment either is broken. Mutation (d):
// reverting to the raw Enabled compare re-arms the every-cycle push and
// fails this test.
func TestConsulNacosReconcileStatus2PairNoDiffNoPush(t *testing.T) {
	// The local truth: a failing-check instance (Status=2, Enabled=true by
	// the converter hardcode, Reversion=308) — served through the monitor
	// double because the HTTP health view filters critical entries.
	local := unhealthyEntry("srv-uh", "127.0.0.1", 308)

	// The nacos-shaped remote view: the [online]-request filter would drop
	// a status-2 reconstruction, but the compare under test is fed the
	// WIDENED view (the pin must hold through the projection, not only
	// through the filter): the reconstruction of the same instance —
	// Status=2 (metadata), Enabled=FALSE (the wire the register wrote),
	// Cluster "" (the dsca-3 §3.2 fidelity fix), Provider "ecs", reversion
	// equal.
	remote := &sv.Instance{
		InstanceId: "srv-uh", AppCode: "pay-user", Ip: "127.0.0.1",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "probing",
		Status: providers.InstanceStatusUnhealthy, Enabled: false,
		Idc: "mix", Cpu: 0, Reversion: 308, Version: "v1",
	}

	w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	c := newStaticConsulProvider(t, w, map[string][]*api.ServiceEntry{"pay-user": {local}})
	c.SetNacosReconcileSource(true)

	// Sanity: the local list really holds the status-2 shape (the
	// precondition the whole pin stands on).
	all := c.GetAll()
	if len(all) != 1 || all[0].Status != providers.InstanceStatusUnhealthy || !all[0].Enabled {
		t.Fatalf("fixture: local status/enabled = %d/%v, want 2/true (the converter hardcode)", all[0].Status, all[0].Enabled)
	}

	// Tick after tick: the steady state must stay silent.
	for tick := 0; tick < 3; tick++ {
		c.CompareAndFlush()
	}
	time.Sleep(300 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (the status-2 ecs pair is steady: no diff, no push, tick after tick); events = %#v", len(events), events)
	}
}

// TestConsulNacosReconcileHealthyMirrorSteadyNoPush: the healthy mirror-image
// steady pin — a local healthy instance against its own nacos-shaped
// reconstruction (Status=1, Enabled=true, Provider "ecs" vs local "ecs",
// Cluster "" vs local "", reversion equal) produces no diff and no push.
// Mutation guards: the raw Cluster compare (remote "ecs" vs local "") and
// the raw Enabled compare both re-arm an every-cycle push here.
func TestConsulNacosReconcileHealthyMirrorSteadyNoPush(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	seedConsul(t, server, healthyEntry("srv-ok", "127.0.0.1", 42))

	// The nacos-shaped reconstruction of the healthy instance: Enabled=true
	// (no override at status 1), Cluster "" (never carried), Provider "ecs".
	remote := &sv.Instance{
		InstanceId: "srv-ok", AppCode: "pay-user", Ip: "127.0.0.1",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "running",
		Status: providers.InstanceStatusOnline, Enabled: true,
		Idc: "mix", Cpu: 0, Reversion: 42, Version: "v1",
	}

	w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newBlackboxConsulProvider(t, server, w, 0, ctx)
	c.SetNacosReconcileSource(true)

	c.CompareAndFlush()
	time.Sleep(300 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (the healthy ecs pair against its own reconstruction is steady); events = %#v", len(events), events)
	}
}

// TestConsulNacosReconcileReversionMismatchEitherDirectionPushes: rule R2
// on the consul leg — ANY reversion mismatch is drift and the local value
// wins. The remote-HIGHER shape is the one the old gates suppressed
// completely on this leg (the field set was reachable only at equal
// revision): a forged remote reversion must heal by overwriting, not stay
// forever.
func TestConsulNacosReconcileReversionMismatchEitherDirectionPushes(t *testing.T) {
	t.Run("remote higher pushes local (R2, the forged-revision heal)", func(t *testing.T) {
		server := consulmock.Start()
		defer server.Close()
		seedConsul(t, server, healthyEntry("srv-a", "127.0.0.1", 42))

		remote := &sv.Instance{
			InstanceId: "srv-a", AppCode: "pay-user", Ip: "127.0.0.1",
			Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
			EnvType: "test", EnvGroup: "7", State: "running",
			Status: providers.InstanceStatusOnline, Enabled: true,
			Idc: "mix", Cpu: 0, Reversion: 999999, Version: "v1", // forged above the local 42
		}

		w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := newBlackboxConsulProvider(t, server, w, 0, ctx)
		c.SetNacosReconcileSource(true)

		c.CompareAndFlush()

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(w.handleSnapshot()) >= 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		events := w.handleSnapshot()
		if len(events) == 0 {
			t.Fatal("no push for the forged-revision instance (R2: any mismatch either direction is drift; the old equal-revision gate suppressed the heal forever)")
		}
		if events[0].Data[0].Reversion != 42 {
			t.Fatalf("pushed reversion = %d, want 42 (the local value overwrites the forged remote one)", events[0].Data[0].Reversion)
		}
	})

	t.Run("remote lower pushes local", func(t *testing.T) {
		server := consulmock.Start()
		defer server.Close()
		seedConsul(t, server, healthyEntry("srv-a", "127.0.0.1", 42))

		remote := &sv.Instance{
			InstanceId: "srv-a", AppCode: "pay-user", Ip: "127.0.0.1",
			Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
			EnvType: "test", EnvGroup: "7", State: "running",
			Status: providers.InstanceStatusOnline, Enabled: true,
			Idc: "mix", Cpu: 0, Reversion: 7, Version: "v1", // ordinary local-newer drift
		}

		w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c := newBlackboxConsulProvider(t, server, w, 0, ctx)
		c.SetNacosReconcileSource(true)

		c.CompareAndFlush()

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if len(w.handleSnapshot()) >= 1 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		events := w.handleSnapshot()
		if len(events) == 0 {
			t.Fatal("no push for the remote-lower reversion drift")
		}
		if events[0].Data[0].Reversion != 42 {
			t.Fatalf("pushed reversion = %d, want 42 (local wins)", events[0].Data[0].Reversion)
		}
	})
}

// TestConsulNacosReconcileCase3Deregisters: DS-3-5 — in nacos-reconcile
// mode, a remote-only ecs instance (removed from consul while spotter was
// down, or its delete event lost) heals to a DEREGISTER (Status 3), never
// to the Atlas-world Status-2 zombie (an upsert with enabled=false, hidden
// from consumers, present in the catalog forever).
func TestConsulNacosReconcileCase3Deregisters(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	// The local truth: one healthy instance (srv-live). The remote view
	// holds srv-live's registration PLUS the ghost of an instance consul
	// no longer has.
	seedConsul(t, server, healthyEntry("srv-live", "127.0.0.1", 42))
	liveRemote := &sv.Instance{
		InstanceId: "srv-live", AppCode: "pay-user", Ip: "127.0.0.1",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "running",
		Status: providers.InstanceStatusOnline, Enabled: true,
		Idc: "mix", Cpu: 0, Reversion: 42, Version: "v1",
	}
	ghost := &sv.Instance{
		InstanceId: "srv-ghost", AppCode: "pay-user", Ip: "127.0.0.9",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "running",
		Status: providers.InstanceStatusOnline, Enabled: true,
		Idc: "mix", Cpu: 0, Reversion: 9, Version: "v1",
	}

	w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{liveRemote, ghost}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newBlackboxConsulProvider(t, server, w, 0, ctx)
	c.SetNacosReconcileSource(true)

	c.CompareAndFlush()

	deadline := time.Now().Add(3 * time.Second)
	var events []*worker.Event
	for time.Now().Before(deadline) {
		events = w.handleSnapshot()
		if len(events) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	var ghostEvent *worker.Event
	for _, e := range events {
		if len(e.Data) == 1 && e.Data[0].InstanceId == "srv-ghost" {
			ghostEvent = e
		}
	}
	if ghostEvent == nil {
		t.Fatalf("no push for the remote-only ghost; events = %#v", events)
	}
	if got := ghostEvent.Data[0].Status; got != providers.InstanceStatusOffline {
		t.Fatalf("ghost push status = %d, want %d (deregister — the Status-2 zombie heal of DS-3-5)", got, providers.InstanceStatusOffline)
	}
	if ghostEvent.Data[0].Enabled {
		t.Fatal("ghost push enabled = true, want false")
	}
}

// TestConsulAtlasModeCase3KeepsStatus2: the Atlas-primary world's case-3
// semantics stay untouched — a remote-only instance is pushed Status=2 (the
// "old instance, mark not-ready" Atlas semantics), the mode gate carrying
// the switch.
func TestConsulAtlasModeCase3KeepsStatus2(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	seedConsul(t, server, healthyEntry("srv-live", "127.0.0.1", 42))
	liveRemote := &sv.Instance{
		InstanceId: "srv-live", AppCode: "pay-user", Ip: "127.0.0.1",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "running",
		Status: providers.InstanceStatusOnline, Enabled: true,
		Idc: "mix", Cpu: 0, Reversion: 42, Version: "v1",
	}
	ghost := &sv.Instance{
		InstanceId: "srv-ghost", AppCode: "pay-user", Ip: "127.0.0.9",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "test", EnvGroup: "7", State: "running",
		Status: providers.InstanceStatusOnline, Enabled: true,
		Idc: "mix", Cpu: 0, Reversion: 9, Version: "v1",
	}

	w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{liveRemote, ghost}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newBlackboxConsulProvider(t, server, w, 0, ctx) // Atlas-primary mode (the default)

	c.CompareAndFlush()

	deadline := time.Now().Add(3 * time.Second)
	var events []*worker.Event
	for time.Now().Before(deadline) {
		events = w.handleSnapshot()
		if len(events) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, e := range events {
		if len(e.Data) == 1 && e.Data[0].InstanceId == "srv-ghost" {
			if got := e.Data[0].Status; got != providers.InstanceStatusUnhealthy {
				t.Fatalf("Atlas-mode ghost push status = %d, want %d (the Atlas semantics are unchanged)", got, providers.InstanceStatusUnhealthy)
			}
			return
		}
	}
	t.Fatalf("no push for the remote-only ghost in Atlas mode; events = %#v", events)
}

// TestConsulNacosReconcileFieldDriftUpdatePushesLocal: the UPDATE heal —
// same-id field-level drift on the remote side (a manual metadata edit)
// pushes the LOCAL instance (R1).
func TestConsulNacosReconcileFieldDriftUpdatePushesLocal(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	seedConsul(t, server, healthyEntry("srv-a", "127.0.0.1", 42))

	remote := &sv.Instance{
		InstanceId: "srv-a", AppCode: "pay-user", Ip: "127.0.0.1",
		Ports: []*sv.PortInfo{{Port: 8081}}, Provider: "ecs", Cluster: "",
		EnvType: "beta", EnvGroup: "7", State: "running", // the console edit
		Status: providers.InstanceStatusOnline, Enabled: true,
		Idc: "mix", Cpu: 0, Reversion: 42, Version: "v1",
	}

	w := &scriptableWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newBlackboxConsulProvider(t, server, w, 0, ctx)
	c.SetNacosReconcileSource(true)

	c.CompareAndFlush()

	deadline := time.Now().Add(3 * time.Second)
	var events []*worker.Event
	for time.Now().Before(deadline) {
		events = w.handleSnapshot()
		if len(events) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(events) == 0 {
		t.Fatal("no push for the field-drifted instance (the UPDATE heal)")
	}
	if events[0].Data[0].EnvType != "test" {
		t.Fatalf("pushed envType = %q, want test (local wins the field diff)", events[0].Data[0].EnvType)
	}
}
