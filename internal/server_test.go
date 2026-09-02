package internal

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"

	infraconfig "spotter/internal/infra/config"
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

// newFakeLeaderServer builds a Server whose Run loop can be driven offline:
// leader election is disabled (Run promotes the process itself to a fake
// leader by sending true on leaderChCh, so the nil elector is never
// consulted), the metrics server binds an ephemeral loopback port, and the
// discovery dial and provider initialization are injected seams.
func newFakeLeaderServer(logger *zap.SugaredLogger, initialize func(context.Context) []providers.Provider) *Server {
	return &Server{
		cfg: infraconfig.Config{
			EnableLeaderElection: false,
			MetricsAddr:          "127.0.0.1:0",
		},
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

func TestRunFakeLeaderStartsProviders(t *testing.T) {
	logger := zap.NewNop().Sugar()
	providerStarted := make(chan struct{})
	providerStopped := make(chan struct{})
	var initializeMu sync.Mutex
	initializeCalls := 0
	s := newFakeLeaderServer(logger, func(ctx context.Context) []providers.Provider {
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

	// The fake-leader branch of Run sends true itself; the loop must start
	// the providers through the injected initializeProviders seam.
	select {
	case <-providerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("fake leader promotion did not start the providers")
	}

	// Losing the leadership must stop the running providers.
	s.leaderChCh <- false
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
	s := newFakeLeaderServer(logger, func(ctx context.Context) []providers.Provider {
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
		t.Fatal("fake leader promotion did not start the first provider generation")
	}

	// Duplicate leader-gain notifications carry no state change: the loop
	// must ignore them instead of restarting the providers.
	s.leaderChCh <- true
	s.leaderChCh <- true
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
	s.leaderChCh <- false
	select {
	case <-firstStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("leader loss did not cancel the first provider generation")
	}

	// A duplicate leader-loss is ignored as well, and the loop keeps
	// processing genuine transitions afterwards: a new gain starts a second
	// provider generation.
	s.leaderChCh <- false
	s.leaderChCh <- true
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
	s := newFakeLeaderServer(logger, func(ctx context.Context) []providers.Provider {
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
