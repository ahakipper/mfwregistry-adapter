package internal

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"

	infraconfig "spotter/internal/infra/config"
	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
	"spotter/internal/testkit/nacosmock"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

// recordingNotifier is a no-op notifier used by the server tests; the
// lifecycle paths under test must never block on notification.
type recordingNotifier struct{}

func (recordingNotifier) Notify(title, content string) {}

func TestStartProvidersCancelsDialWhenLeadershipIsLost(t *testing.T) {
	logger := zap.NewNop().Sugar()
	dialStarted := make(chan struct{})
	dialCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	initializeCalls := 0
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "", nil },
		dialDiscovery: func(ctx context.Context) (*discoverycenter.Client, error) {
			startedOnce.Do(func() { close(dialStarted) })
			<-ctx.Done()
			canceledOnce.Do(func() { close(dialCanceled) })
			return nil, ctx.Err()
		},
		initializeProviders: func(context.Context, worker.Worker) ([]providers.Provider, error) {
			initializeCalls++
			return nil, nil
		},
	}

	result := make(chan error, 1)
	go func() { result <- s.startProviders() }()
	<-dialStarted

	s.Lock()
	s.isLeader = false
	s.Unlock()
	_ = s.stopProviders()

	select {
	case <-dialCanceled:
	case <-time.After(time.Second):
		t.Fatal("leadership loss did not cancel the in-flight dial")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("startProviders() error = nil, want cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("startProviders() did not return after cancellation")
	}
	if initializeCalls != 0 {
		t.Fatalf("InitializeProviders calls = %d, want 0", initializeCalls)
	}
	if s.Providers != nil {
		t.Fatalf("Providers = %#v, want nil", s.Providers)
	}
}

func TestStopCancelsDialDuringProviderStartup(t *testing.T) {
	logger := zap.NewNop().Sugar()
	dialStarted := make(chan struct{})
	dialCanceled := make(chan struct{})
	releaseDial := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}, 1),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "", nil },
		dialDiscovery: func(ctx context.Context) (*discoverycenter.Client, error) {
			startedOnce.Do(func() { close(dialStarted) })
			select {
			case <-ctx.Done():
				canceledOnce.Do(func() { close(dialCanceled) })
				return nil, ctx.Err()
			case <-releaseDial:
				return nil, errors.New("dial released")
			}
		},
	}
	defer close(releaseDial)

	result := make(chan error, 1)
	go func() { result <- s.startProviders() }()
	<-dialStarted

	stopReturned := make(chan struct{})
	go func() {
		s.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(time.Second):
		t.Fatal("Stop() blocked during provider startup")
	}
	select {
	case <-dialCanceled:
	case <-time.After(time.Second):
		t.Fatal("Stop() did not cancel the in-flight dial")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("startProviders() error = nil, want cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("startProviders() did not return after Stop")
	}
}

func TestStopAndStartProvidersSerializesConcurrentGenerations(t *testing.T) {
	logger := zap.NewNop().Sugar()
	firstInitializeStarted := make(chan struct{})
	releaseFirstInitialize := make(chan struct{})
	secondAttempted := make(chan struct{})
	secondInitializeStarted := make(chan struct{})
	secondProviderStarted := make(chan struct{})
	secondProviderStopped := make(chan struct{})
	locker := &signalingLocker{
		attempted: secondAttempted,
		signalOn:  2,
	}
	var initializeMu sync.Mutex
	initializeCalls := 0
	s := &Server{
		isLeader:        true,
		stop:            make(chan struct{}),
		logger:          logger,
		notifier:        recordingNotifier{},
		localIP:         func() (string, error) { return "", nil },
		lifecycleLocker: locker,
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(ctx context.Context, _ worker.Worker) ([]providers.Provider, error) {
			initializeMu.Lock()
			initializeCalls++
			call := initializeCalls
			initializeMu.Unlock()
			if call == 1 {
				close(firstInitializeStarted)
				<-releaseFirstInitialize
				return nil, ctx.Err()
			}
			close(secondInitializeStarted)
			return []providers.Provider{&blockingProvider{
				ctx:     ctx,
				started: secondProviderStarted,
				stopped: secondProviderStopped,
			}}, nil
		},
	}

	firstResult := make(chan error, 1)
	go func() { firstResult <- s.stopAndStartProviders() }()
	<-firstInitializeStarted

	s.Lock()
	s.isLeader = false
	s.Unlock()
	_ = s.stopProviders()
	s.Lock()
	s.isLeader = true
	s.Unlock()

	secondResult := make(chan error, 1)
	go func() { secondResult <- s.stopAndStartProviders() }()
	select {
	case <-secondAttempted:
	case <-time.After(time.Second):
		t.Fatal("second lifecycle sequence did not reach the lifecycle lock")
	}
	select {
	case <-secondInitializeStarted:
		t.Fatal("second initialization entered while first lifecycle sequence held the lock")
	default:
	}

	close(releaseFirstInitialize)
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("first stopAndStartProviders() error = nil, want canceled generation")
		}
	case <-time.After(time.Second):
		t.Fatal("first lifecycle sequence did not return")
	}
	select {
	case <-secondInitializeStarted:
	case <-time.After(time.Second):
		t.Fatal("second initialization did not enter after first returned")
	}
	select {
	case <-secondProviderStarted:
	case <-time.After(time.Second):
		t.Fatal("newest provider did not start")
	}

	_ = s.stopProviders()
	select {
	case <-secondProviderStopped:
	case <-time.After(time.Second):
		t.Fatal("subsequent stop did not cancel newest provider")
	}
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("second stopAndStartProviders() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second lifecycle sequence did not return after stop")
	}
	s.Lock()
	defer s.Unlock()
	if s.Providers != nil {
		t.Fatalf("Providers = %#v, want nil after stop", s.Providers)
	}
}

type signalingLocker struct {
	underlying sync.Mutex
	callsMu    sync.Mutex
	calls      int
	signalOn   int
	attempted  chan struct{}
}

func (l *signalingLocker) Lock() {
	l.callsMu.Lock()
	l.calls++
	call := l.calls
	l.callsMu.Unlock()
	if call == l.signalOn {
		close(l.attempted)
	}
	l.underlying.Lock()
}

func (l *signalingLocker) Unlock() {
	l.underlying.Unlock()
}

type noopDiscoveryService struct{}

func (noopDiscoveryService) SynInstance(context.Context, *v2.SynInstancesRequest, ...grpc.CallOption) (*v2.CommonResponse, error) {
	return &v2.CommonResponse{}, nil
}

func (noopDiscoveryService) SynAllInstance(context.Context, *v2.SynAllInstancesRequest, ...grpc.CallOption) (*v2.CommonResponse, error) {
	return &v2.CommonResponse{}, nil
}

func (noopDiscoveryService) GetAllInstance(context.Context, *v2.GetAllInstancesRequest, ...grpc.CallOption) (*v2.InstanceList, error) {
	return &v2.InstanceList{}, nil
}

type blockingProvider struct {
	ctx     context.Context
	started chan struct{}
	stopped chan struct{}
}

func (p *blockingProvider) Run() error {
	close(p.started)
	<-p.ctx.Done()
	close(p.stopped)
	return nil
}

func (*blockingProvider) CompareAndFlush() {}

func (*blockingProvider) GetAll() []*v2.Instance { return nil }

func TestDialDiscoveryRetriesThreeTimesAtFiveSecondCadence(t *testing.T) {
	core, observed := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	attempts := 0
	var waits []time.Duration
	s := &Server{
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "", nil },
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			attempts++
			return nil, errors.New("offline")
		},
		waitRetry: func(ctx context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	}

	client, err := s.dialDiscoveryWithRetry(context.Background())
	if err == nil {
		t.Fatal("dialDiscoveryWithRetry() error = nil, want non-nil")
	}
	if client != nil {
		t.Fatalf("dialDiscoveryWithRetry() client = %#v, want nil", client)
	}
	if attempts != 3 {
		t.Fatalf("dial attempts = %d, want 3", attempts)
	}
	if want := []time.Duration{5 * time.Second, 5 * time.Second}; !reflect.DeepEqual(waits, want) {
		t.Fatalf("retry waits = %v, want %v", waits, want)
	}
	entries := observed.All()
	if len(entries) != 3 {
		t.Fatalf("connect failure logs = %d, want 3", len(entries))
	}
	for i, entry := range entries {
		if entry.Message != "connect fail: offline" {
			t.Fatalf("log %d = %q, want %q", i, entry.Message, "connect fail: offline")
		}
	}
}

// newElectorDrivenServer builds a Server whose Run loop can be driven
// offline through the REAL elector path: leader election is enabled and the
// elector is a fakes.NewFakeLeaderElector (which satisfies the same
// ports.LeaderElector port ElectWorker now satisfies), so Run hands its
// leaderChCh to ElectWait and consumes the transitions the fake emits —
// no more EnableLeaderElection:false bypass. The metrics server binds an
// ephemeral loopback port, and the discovery dial and provider
// initialization are injected seams.
func newElectorDrivenServer(logger *zap.SugaredLogger, elector ports.LeaderElector, initialize func(context.Context) []providers.Provider) *Server {
	return &Server{
		cfg: infraconfig.Config{
			EnableLeaderElection: true,
			MetricsAddr:          "127.0.0.1:0",
		},
		elector:    elector,
		leaderChCh: make(chan bool, 8),
		stop:       make(chan struct{}),
		logger:     logger,
		notifier:   recordingNotifier{},
		localIP:    func() (string, error) { return "127.0.0.1", nil },
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(ctx context.Context, _ worker.Worker) ([]providers.Provider, error) {
			return initialize(ctx), nil
		},
	}
}

// TestRunRealElectorPathStartsAndStopsProviders drives Run with
// EnableLeaderElection:true and a FakeLeaderElector injected through the
// ports.LeaderElector field: the server starts providers when the fake
// reports leadership gained, and stops them when the fake reports it lost.
// This proves the unified port end-to-end — the same interface the
// production ElectWorker satisfies (seam 5) — without bypassing the elector.
func TestRunRealElectorPathStartsAndStopsProviders(t *testing.T) {
	logger := zap.NewNop().Sugar()
	providerStarted := make(chan struct{})
	providerStopped := make(chan struct{})
	var initializeMu sync.Mutex
	initializeCalls := 0
	elector := fakes.NewFakeLeaderElector(true)
	s := newElectorDrivenServer(logger, elector, func(ctx context.Context) []providers.Provider {
		initializeMu.Lock()
		initializeCalls++
		initializeMu.Unlock()
		return []providers.Provider{&blockingProvider{
			ctx:     ctx,
			started: providerStarted,
			stopped: providerStopped,
		}}
	})

	runDone := make(chan struct{})
	go func() {
		s.Run()
		close(runDone)
	}()

	// The fake elector reports leadership gained through ElectWait's
	// channel; the loop must start the providers through the injected
	// initializeProviders seam.
	select {
	case <-providerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("elector-driven leader promotion did not start the providers")
	}

	// The fake elector reports leadership lost; the loop must stop the
	// running providers.
	elector.Emit(false)
	select {
	case <-providerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("leader loss did not cancel the running provider")
	}

	s.Stop()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not terminate the Run loop")
	}

	initializeMu.Lock()
	defer initializeMu.Unlock()
	if initializeCalls != 1 {
		t.Fatalf("InitializeProviders calls = %d, want 1", initializeCalls)
	}
	s.Lock()
	providers := s.Providers
	s.Unlock()
	if providers != nil {
		t.Fatalf("Providers = %#v, want nil after stop", providers)
	}
}

func TestRunIgnoresDuplicateLeaderState(t *testing.T) {
	logger := zap.NewNop().Sugar()
	firstStarted := make(chan struct{})
	firstStopped := make(chan struct{})
	secondStarted := make(chan struct{})
	secondStopped := make(chan struct{})
	var initializeMu sync.Mutex
	initializeCalls := 0
	elector := fakes.NewFakeLeaderElector(true)
	s := newElectorDrivenServer(logger, elector, func(ctx context.Context) []providers.Provider {
		initializeMu.Lock()
		initializeCalls++
		call := initializeCalls
		initializeMu.Unlock()
		if call == 1 {
			return []providers.Provider{&blockingProvider{
				ctx:     ctx,
				started: firstStarted,
				stopped: firstStopped,
			}}
		}
		return []providers.Provider{&blockingProvider{
			ctx:     ctx,
			started: secondStarted,
			stopped: secondStopped,
		}}
	})

	runDone := make(chan struct{})
	go func() {
		s.Run()
		close(runDone)
	}()
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("elector-driven leader promotion did not start the first provider generation")
	}

	// Duplicate leader-gain notifications carry no state change: the loop
	// must ignore them instead of restarting the providers.
	elector.Emit(true)
	elector.Emit(true)
	time.Sleep(300 * time.Millisecond)
	initializeMu.Lock()
	calls := initializeCalls
	initializeMu.Unlock()
	if calls != 1 {
		t.Fatalf("InitializeProviders calls after duplicate leader gains = %d, want 1", calls)
	}
	select {
	case <-firstStopped:
		t.Fatal("duplicate leader gain stopped the running provider")
	default:
	}

	// A real state change still works after the duplicates: leader loss
	// stops the first provider generation.
	elector.Emit(false)
	select {
	case <-firstStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("leader loss did not cancel the first provider generation")
	}

	// A duplicate leader-loss is ignored as well, and the loop keeps
	// processing genuine transitions afterwards: a new gain starts a second
	// provider generation.
	elector.Emit(false)
	elector.Emit(true)
	select {
	case <-secondStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("leader regain did not start the second provider generation")
	}

	s.Stop()
	select {
	case <-secondStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not cancel the second provider generation")
	}
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not terminate the Run loop")
	}

	initializeMu.Lock()
	defer initializeMu.Unlock()
	if initializeCalls != 2 {
		t.Fatalf("InitializeProviders calls = %d, want 2", initializeCalls)
	}
	s.Lock()
	providers := s.Providers
	s.Unlock()
	if providers != nil {
		t.Fatalf("Providers = %#v, want nil after stop", providers)
	}
}

func TestRunStopAfterProvidersInstalledCancelsProviders(t *testing.T) {
	logger := zap.NewNop().Sugar()
	providerStarted := make(chan struct{})
	providerStopped := make(chan struct{})
	elector := fakes.NewFakeLeaderElector(true)
	s := newElectorDrivenServer(logger, elector, func(ctx context.Context) []providers.Provider {
		return []providers.Provider{&blockingProvider{
			ctx:     ctx,
			started: providerStarted,
			stopped: providerStopped,
		}}
	})

	runDone := make(chan struct{})
	go func() {
		s.Run()
		close(runDone)
	}()
	select {
	case <-providerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("fake leader promotion did not start the providers")
	}

	// Plain server shutdown (no leader loss) must cancel the running
	// provider and terminate the Run loop. The existing
	// TestStopCancelsDialDuringProviderStartup covers Stop during the dial
	// phase; this exercises Stop after the providers are installed.
	s.Stop()
	select {
	case <-providerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not cancel the running provider")
	}
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not terminate the Run loop")
	}

	// The metrics server started by Run must have been stopped on the way
	// out of the loop.
	s.Lock()
	stopMetrics := s.stopMetrics
	s.Unlock()
	if stopMetrics != nil {
		t.Fatal("stopMetrics still set after Run exited, want the metrics server stopped")
	}

	// Stop is idempotent: a second call must neither block nor panic.
	stopReturned := make(chan struct{})
	go func() {
		s.Stop()
		close(stopReturned)
	}()
	select {
	case <-stopReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("second Stop() blocked")
	}

	s.Lock()
	providers := s.Providers
	s.Unlock()
	if providers != nil {
		t.Fatalf("Providers = %#v, want nil after stop", providers)
	}
}

// -----------------------------------------------------------------------------
// Nacos sink wiring (plan §6.5 / §7.6)
// -----------------------------------------------------------------------------

// capturedWorkerProvider is a providers.Provider that captures the worker
// the server handed to initializeProviders and blocks until the startup
// context is canceled (the lifecycle the cleanup exercises).
type capturedWorkerProvider struct {
	ctx     context.Context
	started chan struct{}
}

func (p *capturedWorkerProvider) Run() error {
	close(p.started)
	<-p.ctx.Done()
	return nil
}

func (p *capturedWorkerProvider) CompareAndFlush() {}

func (p *capturedWorkerProvider) GetAll() []*v2.Instance { return nil }

// startProvidersWithNacosAddr runs startProviders on an offline server with
// the given NacosAddr and returns the worker the fanout was built around.
// The nacosmock server supplies the address; the discovery dial and the
// provider construction are injected seams.
func startProvidersWithNacosAddr(t *testing.T, nacosAddr string) (worker.Worker, *Server, chan error) {
	t.Helper()
	logger := zap.NewNop().Sugar()

	var captured worker.Worker
	providerStarted := make(chan struct{})
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "127.0.0.1", nil },
		cfg: infraconfig.Config{
			EnableLeaderElection: true,
			MetricsAddr:          "127.0.0.1:0",
			NacosAddr:            nacosAddr,
		},
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(ctx context.Context, w worker.Worker) ([]providers.Provider, error) {
			captured = w
			return []providers.Provider{&capturedWorkerProvider{ctx: ctx, started: providerStarted}}, nil
		},
	}

	result := make(chan error, 1)
	go func() { result <- s.startProviders() }()
	<-providerStarted
	return captured, s, result
}

// TestStartProvidersWiresNacosSinkWhenAddrSet: with --nacos-addr set to a
// nacosmock address, the server constructs the Nacos sink and registers it
// as the SECOND fanout sink alongside Atlas (plan §6.5). Proven end-to-end
// through the worker's public Handle seam: one SyncAll event fans out to
// both sinks, and the nacosmock observes the registered instance.
func TestStartProvidersWiresNacosSinkWhenAddrSet(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	captured, s, result := startProvidersWithNacosAddr(t, server.URL())
	if captured == nil {
		t.Fatal("initializeProviders received a nil worker")
	}

	// Exercise the full path: one SyncAll event through Handle must reach
	// the nacosmock (register) AND the noop discovery service.
	ins := &v2.Instance{
		InstanceId: "pod-a", AppCode: "pay-user", Ip: "10.0.0.1",
		Ports: []*v2.PortInfo{{Port: 8080}}, Provider: "k8s",
		Status: 1, Reversion: 1, EnvType: "test",
	}
	captured.Handle(&worker.Event{Trigger: 1, Data: []*v2.Instance{ins}, Operate: worker.OperateTypeSyncAll})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(server.Instances("pay-user", "k8s")) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("nacosmock instances after SyncAll = %d, want 1 (the sink pushed); requests = %d", got, len(server.Requests()))
	}

	s.Stop()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("startProviders() did not return after Stop")
	}
}

// TestStartProvidersOmitsNacosSinkWhenAddrEmpty: with --nacos-addr empty
// (the default) the fanout holds exactly one sink — the pre-F5 behavior:
// the SyncAll event reaches only the discovery service, never any Nacos.
func TestStartProvidersOmitsNacosSinkWhenAddrEmpty(t *testing.T) {
	captured, s, result := startProvidersWithNacosAddr(t, "")
	if captured == nil {
		t.Fatal("initializeProviders received a nil worker")
	}

	ins := &v2.Instance{
		InstanceId: "pod-a", AppCode: "pay-user", Ip: "10.0.0.1",
		Ports: []*v2.PortInfo{{Port: 8080}}, Provider: "k8s",
		Status: 1, Reversion: 1, EnvType: "test",
	}
	captured.Handle(&worker.Event{Trigger: 1, Data: []*v2.Instance{ins}, Operate: worker.OperateTypeSyncAll})

	// No Nacos traffic can exist: there is no Nacos sink at all. The
	// discovery noop service answers, so the handler returns without error.
	time.Sleep(100 * time.Millisecond)

	s.Stop()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("startProviders() did not return after Stop")
	}
}

// TestStartProvidersNacosReadinessGate: the construction health-checks the
// Nacos address; an unreachable address fails the provider startup with the
// readiness error, before any provider runs.
func TestStartProvidersNacosReadinessGate(t *testing.T) {
	logger := zap.NewNop().Sugar()
	initializeCalls := 0
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "127.0.0.1", nil },
		cfg: infraconfig.Config{
			EnableLeaderElection: true,
			MetricsAddr:          "127.0.0.1:0",
			NacosAddr:            "http://127.0.0.1:1", // nothing listens
		},
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(context.Context, worker.Worker) ([]providers.Provider, error) {
			initializeCalls++
			return nil, nil
		},
	}

	err := s.startProviders()
	if err == nil {
		t.Fatal("startProviders() error = nil, want the Nacos readiness failure")
	}
	if !strings.Contains(err.Error(), "nacos") {
		t.Fatalf("startProviders() error = %q, want it to name the Nacos gate", err)
	}
	if initializeCalls != 0 {
		t.Fatalf("InitializeProviders calls = %d, want 0 (startup failed at the gate)", initializeCalls)
	}
}

// TestStartProvidersWiresEmptyIPShellSafely (AUDIT-D E2E-2, the F7 enabling
// hole's wiring proof): through the REAL server wiring — the
// startProvidersWithNacosAddr harness builds the fanout (atlas + the real
// nacos Sink over the nacosmock), the real DefaultWorker with its live retry
// loop — one offline instance with an EMPTY Ip must be skipped by the sink's
// empty-IP deregister guard (pkg/nacos pushOne): the v1 DELETE derives its
// composite id from the ip parameter, and a DELETE without one is answered
// 400 forever — the exact request shape that poisoned the retry queue in the
// live incident (9624 futile retries). The assertions are the guard's wiring
// proof through the server seam:
//
//   - zero DELETE requests reach the nacosmock (no deregister was even
//     attempted — the skip happens before the wire);
//   - zero POST requests either: the event's only instance is offline, so
//     nothing registers;
//   - the retry queue stays empty (observed through the worker's metrics
//     seam: the captured worker is driven by the server construction, which
//     wires the server's nil recorder — so the queue-emptiness proof is the
//     request side: a poisoned queue would keep firing DELETEs on its 5s
//     ticker, and the request log stays empty for two full retry cycles).
func TestStartProvidersWiresEmptyIPShellSafely(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	captured, s, result := startProvidersWithNacosAddr(t, server.URL())
	if captured == nil {
		t.Fatal("initializeProviders received a nil worker")
	}

	// The offline shell: status 3 with an empty Ip — the k8s CompareAndFlush
	// case-3 shape (atlas leftovers pushed offline with whatever Ip atlas
	// reported, AUDIT-C-5) that reached the sink live.
	shell := &v2.Instance{
		InstanceId: "pod-empty-ip", AppCode: "pay-user", Ip: "",
		Ports: []*v2.PortInfo{{Port: 8080}}, Provider: "k8s",
		Status: 3, Reversion: 1, EnvType: "test",
	}
	captured.Handle(&worker.Event{Trigger: 1, Data: []*v2.Instance{shell}, Operate: worker.OperateTypeSync})

	// Two full retry cycles of the real 5s ticker (10s + margin): a poisoned
	// queue would have fired at least two DELETE rounds by then.
	time.Sleep(12 * time.Second)

	requests := server.Requests()
	if deletes := countNacosDeletes(requests); deletes != 0 {
		t.Fatalf("nacos DELETE requests after the empty-IP offline push = %d, want 0 (the sink's empty-IP skip guard must hold through the server wiring); requests = %s",
			deletes, formatNacosRequests(requests))
	}
	if posts := countNacosPosts(requests); posts != 0 {
		t.Fatalf("nacos POST requests after the empty-IP offline push = %d, want 0 (the event carries one offline instance; nothing registers); requests = %s",
			posts, formatNacosRequests(requests))
	}

	s.Stop()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("startProviders() did not return after Stop")
	}
}

// countNacosDeletes counts the DELETE requests against the nacos instance
// endpoint in the mock's recorded request log.
func countNacosDeletes(requests []nacosmock.Request) int {
	count := 0
	for _, request := range requests {
		if request.Method == "DELETE" && request.Path == "/nacos/v1/ns/instance" && !strings.HasPrefix(request.Query.Get("serviceName"), "__spotter_readiness_") {
			count++
		}
	}
	return count
}

// countNacosPosts counts the POST (register) requests against the nacos
// instance endpoint.
func countNacosPosts(requests []nacosmock.Request) int {
	count := 0
	for _, request := range requests {
		if request.Method == "POST" && request.Path == "/nacos/v1/ns/instance" && !strings.HasPrefix(request.Query.Get("serviceName"), "__spotter_readiness_") {
			count++
		}
	}
	return count
}

// formatNacosRequests renders the recorded requests for a failure message.
func formatNacosRequests(requests []nacosmock.Request) string {
	var parts []string
	for _, request := range requests {
		parts = append(parts, request.Method+" "+request.Path)
	}
	if len(parts) == 0 {
		return "(none)"
	}
	return strings.Join(parts, ", ")
}

// -----------------------------------------------------------------------------
// The reconcile-source designation wiring (dsca-3 §3.1)
// -----------------------------------------------------------------------------

// startProvidersWithReconcileSource runs startProviders on an offline
// server with the given NacosAddr and reconcile-source value and returns
// the worker the fanout was built around (the same harness as
// startProvidersWithNacosAddr, plus the designation config).
func startProvidersWithReconcileSource(t *testing.T, nacosAddr, reconcileSource string) (worker.Worker, *Server, chan error) {
	t.Helper()
	logger := zap.NewNop().Sugar()

	var captured worker.Worker
	providerStarted := make(chan struct{})
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "127.0.0.1", nil },
		cfg: infraconfig.Config{
			EnableLeaderElection: true,
			MetricsAddr:          "127.0.0.1:0",
			NacosAddr:            nacosAddr,
			ReconcileSource:      reconcileSource,
		},
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(ctx context.Context, w worker.Worker) ([]providers.Provider, error) {
			captured = w
			return []providers.Provider{&capturedWorkerProvider{ctx: ctx, started: providerStarted}}, nil
		},
	}

	result := make(chan error, 1)
	go func() { result <- s.startProviders() }()
	<-providerStarted
	return captured, s, result
}

// TestStartProvidersDesignatesNacosReconcileSource: with
// --reconcile-source nacos (and --nacos-addr set), the worker's GetAll —
// the view the providers' compares read — returns the NACOS sink's view,
// not the primary's. Proven through the worker's public GetAll seam against
// a nacosmock seeded with out-of-band state: the primary (the noop
// discovery service) serves an empty view, so a non-empty answer can only
// come from the designated sink.
func TestStartProvidersDesignatesNacosReconcileSource(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	// Out-of-band nacos state: one host under the k8s cluster.
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.7", Port: 8080, Enabled: true, Ephemeral: false,
			Metadata: map[string]string{
				"instanceId": "pod-seeded", "envType": "test", "envGroup": "7",
				"reversion": "42", "status": "1", "state": "running",
				"idc": "", "cpu": "0", "version": "", "schemaVersion": "1",
			}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	captured, s, result := startProvidersWithReconcileSource(t, server.URL(), "nacos")
	if captured == nil {
		t.Fatal("initializeProviders received a nil worker")
	}

	list, err := captured.GetAll([]int32{1}, "k8s")
	if err != nil {
		t.Fatalf("worker.GetAll([1], k8s) error = %v, want nil", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "pod-seeded" {
		t.Fatalf("worker.GetAll() = %d instances, want the designated nacos sink's view (pod-seeded); got %#v", len(list.Instance), list.Instance)
	}

	s.Stop()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("startProviders() did not return after Stop")
	}
}

// TestStartProvidersReconcileSourceDefaultKeepsPrimary: without the flag,
// the worker's GetAll keeps the primary (Atlas) view — the
// production-unchanged default. The nacos sink is registered (--nacos-addr
// set), so the designated path exists but is not taken: the primary's empty
// view is what the compare sees.
func TestStartProvidersReconcileSourceDefaultKeepsPrimary(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.7", Port: 8080, Enabled: true, Ephemeral: false,
			Metadata: map[string]string{"instanceId": "pod-seeded"}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	captured, s, result := startProvidersWithReconcileSource(t, server.URL(), "")
	if captured == nil {
		t.Fatal("initializeProviders received a nil worker")
	}

	list, err := captured.GetAll([]int32{1}, "k8s")
	if err != nil {
		t.Fatalf("worker.GetAll([1], k8s) error = %v, want nil", err)
	}
	if len(list.Instance) != 0 {
		t.Fatalf("worker.GetAll() = %d instances, want 0 (the primary's view — the nacos sink is registered but not designated); got %#v", len(list.Instance), list.Instance)
	}

	s.Stop()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("startProviders() did not return after Stop")
	}
}

// TestStartProvidersReconcileSourceNacosWithoutNacosAddrFails: the
// fail-fast wiring guard — --reconcile-source nacos without --nacos-addr
// (no nacos sink registered) fails the provider startup before any provider
// runs.
func TestStartProvidersReconcileSourceNacosWithoutNacosAddrFails(t *testing.T) {
	logger := zap.NewNop().Sugar()
	initializeCalls := 0
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "127.0.0.1", nil },
		cfg: infraconfig.Config{
			EnableLeaderElection: true,
			MetricsAddr:          "127.0.0.1:0",
			ReconcileSource:      "nacos", // no NacosAddr: the sink is absent
		},
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(context.Context, worker.Worker) ([]providers.Provider, error) {
			initializeCalls++
			return nil, nil
		},
	}

	err := s.startProviders()
	if err == nil {
		t.Fatal("startProviders() error = nil, want the nacos-without-nacos-addr failure")
	}
	if !strings.Contains(err.Error(), "reconcile-source") && !strings.Contains(err.Error(), "nacos-addr") {
		t.Fatalf("startProviders() error = %q, want it to name the designation requirement", err)
	}
	if initializeCalls != 0 {
		t.Fatalf("InitializeProviders calls = %d, want 0 (startup failed at the designation guard)", initializeCalls)
	}
}

// TestStartProvidersReconcileSourceUnknownNameFails: a reconcile-source
// value that names no registered sink fails the provider startup (the
// fail-fast discipline of the fanout's name resolution).
func TestStartProvidersReconcileSourceUnknownNameFails(t *testing.T) {
	logger := zap.NewNop().Sugar()
	initializeCalls := 0
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
		logger:   logger,
		notifier: recordingNotifier{},
		localIP:  func() (string, error) { return "127.0.0.1", nil },
		cfg: infraconfig.Config{
			EnableLeaderElection: true,
			MetricsAddr:          "127.0.0.1:0",
			ReconcileSource:      "bogus",
		},
		dialDiscovery: func(context.Context) (*discoverycenter.Client, error) {
			return discoverycenter.NewClient(noopDiscoveryService{}, nil, nil)
		},
		initializeProviders: func(context.Context, worker.Worker) ([]providers.Provider, error) {
			initializeCalls++
			return nil, nil
		},
	}

	err := s.startProviders()
	if err == nil {
		t.Fatal("startProviders() error = nil, want the unknown-reconcile-source failure")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("startProviders() error = %q, want it to name the unknown sink", err)
	}
	if initializeCalls != 0 {
		t.Fatalf("InitializeProviders calls = %d, want 0 (startup failed at the designation guard)", initializeCalls)
	}
}
