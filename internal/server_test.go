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

	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/log"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

func TestStartProvidersCancelsDialWhenLeadershipIsLost(t *testing.T) {
	log.Logger = zap.NewNop().Sugar()
	dialStarted := make(chan struct{})
	dialCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	initializeCalls := 0
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}),
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
	log.Logger = zap.NewNop().Sugar()
	dialStarted := make(chan struct{})
	dialCanceled := make(chan struct{})
	releaseDial := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	s := &Server{
		isLeader: true,
		stop:     make(chan struct{}, 1),
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
	log.Logger = zap.NewNop().Sugar()
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
	log.Logger = zap.New(core).Sugar()
	attempts := 0
	var waits []time.Duration
	s := &Server{
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
