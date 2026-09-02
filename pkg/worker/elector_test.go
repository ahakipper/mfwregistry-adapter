package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"

	"spotter/pkg/distribute/election"
)

// fakeCandidate implements election.Candidate without touching the network,
// so the elector seams (setLeaderChangeNotifyCall, ElectWait's campaign
// loop, Stop) can be driven deterministically.
//
// Campaign returns immediately and records its timeout argument. Wait
// signals waitStarted, then blocks on a per-call release channel: release
// channels supplied through newFakeCandidate are closed by the test to let
// exactly that Wait call return (driving the next ElectWait loop
// iteration); calls without a supplied channel park forever on a channel
// that is never closed. AddObserveCallFunc records the callbacks so tests
// can synthesize leader transitions on demand via notify.
type fakeCandidate struct {
	mu            sync.Mutex
	callbacks     []election.LeaderChangeFunc
	campaignCalls int
	campaignArg   time.Duration
	waitCalls     int
	waitStarted   chan struct{}
	waitReleases  []chan struct{}
}

// compile-time assertion: the fake must satisfy the exact Candidate seam.
var _ election.Candidate = (*fakeCandidate)(nil)

func newFakeCandidate(waitReleases ...chan struct{}) *fakeCandidate {
	return &fakeCandidate{
		waitStarted:  make(chan struct{}, 8),
		waitReleases: waitReleases,
	}
}

func (f *fakeCandidate) Campaign(timeout time.Duration) error {
	f.mu.Lock()
	f.campaignCalls++
	f.campaignArg = timeout
	f.mu.Unlock()
	return nil
}

func (f *fakeCandidate) IsLeader() (bool, error) { return false, nil }

func (f *fakeCandidate) Resign() error { return nil }

func (f *fakeCandidate) AddObserveCallFunc(fn election.LeaderChangeFunc) {
	if fn == nil {
		return
	}
	f.mu.Lock()
	f.callbacks = append(f.callbacks, fn)
	f.mu.Unlock()
}

func (f *fakeCandidate) Tag() string { return "fake-candidate-tag" }

func (f *fakeCandidate) LeaseID() clientv3.LeaseID { return 0 }

func (f *fakeCandidate) Wait() {
	f.mu.Lock()
	f.waitCalls++
	call := f.waitCalls
	var release chan struct{}
	if call-1 < len(f.waitReleases) {
		release = f.waitReleases[call-1]
	} else {
		// never closed: parks the caller for the rest of the test run
		release = make(chan struct{})
	}
	f.mu.Unlock()
	f.waitStarted <- struct{}{}
	<-release
}

// notify synchronously invokes every registered leader-change callback,
// mimicking what the real candidate does from its Wait loop (the real one
// invokes them in goroutines; calling them synchronously keeps assertions
// deterministic and race-free).
func (f *fakeCandidate) notify(isLeader bool) {
	f.mu.Lock()
	callbacks := make([]election.LeaderChangeFunc, len(f.callbacks))
	copy(callbacks, f.callbacks)
	f.mu.Unlock()
	for _, callback := range callbacks {
		callback(isLeader)
	}
}

func (f *fakeCandidate) campaignCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.campaignCalls
}

func (f *fakeCandidate) campaignTimeout() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.campaignArg
}

func (f *fakeCandidate) waitCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.waitCalls
}

// nopElectorLogger satisfies the worker logger seam (Info only).
type nopElectorLogger struct{}

func (nopElectorLogger) Info(...interface{}) {}

// TestNewElectorWithDepsUnreachableEtcdFailsFast verifies the constructor
// surfaces a connectivity failure against an unreachable endpoint instead
// of hanging: the etcd client probes the member list with a 5s deadline, so
// the call must return an error well inside the 8s guard. TLS stays
// disabled (empty cert/key/ca paths).
func TestNewElectorWithDepsUnreachableEtcdFailsFast(t *testing.T) {
	type electorResult struct {
		elector Elector
		err     error
	}
	results := make(chan electorResult, 1)
	go func() {
		elector, err := NewElectorWithDeps(
			context.Background(),
			make(chan bool, 1),
			[]string{"127.0.0.1:1"},
			"", "", "",
			"/spotter-test/unreachable",
			nopElectorLogger{},
		)
		results <- electorResult{elector, err}
	}()

	select {
	case res := <-results:
		if res.err == nil {
			t.Fatal("NewElectorWithDeps() error = nil, want error for unreachable etcd endpoint")
		}
		if res.elector != nil {
			t.Fatalf("NewElectorWithDeps() elector = %#v, want nil on error", res.elector)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("NewElectorWithDeps() did not fail within 8s for an unreachable etcd endpoint")
	}
}

// TestElectWaitForwardsLeadershipTransitions drives ElectWait with a fake
// candidate and verifies the seam wired by setLeaderChangeNotifyCall: leader
// transitions observed by the candidate are forwarded on the leader channel
// with their boolean value preserved, Stop() suppresses further forwarding,
// and a returning Wait drives a new campaign loop iteration.
func TestElectWaitForwardsLeadershipTransitions(t *testing.T) {
	firstWaitRelease := make(chan struct{})
	fake := newFakeCandidate(firstWaitRelease)
	leaderCh := make(chan bool, 8)
	w := &ElectWorker{
		ctx:       context.Background(),
		candidate: fake,
		logger:    nopElectorLogger{},
	}
	w.setLeaderChangeNotifyCall(leaderCh)

	go w.ElectWait()

	select {
	case <-fake.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not reach candidate.Wait() after campaigning")
	}
	if got := fake.campaignCount(); got != 1 {
		t.Fatalf("Campaign calls before first Wait = %d, want 1", got)
	}
	if want := election.CampainTimeout * time.Second; fake.campaignTimeout() != want {
		t.Fatalf("Campaign timeout = %v, want %v", fake.campaignTimeout(), want)
	}

	// Leader gain observed by the candidate is forwarded as true.
	fake.notify(true)
	select {
	case got := <-leaderCh:
		if !got {
			t.Fatal("forwarded leader transition = false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader gain transition was not forwarded to the leader channel")
	}

	// Leader loss observed by the candidate is forwarded as false.
	fake.notify(false)
	select {
	case got := <-leaderCh:
		if got {
			t.Fatal("forwarded leader transition = true, want false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader loss transition was not forwarded to the leader channel")
	}

	// After Stop, transitions observed by the candidate are suppressed.
	w.Stop()
	fake.notify(true)
	select {
	case got := <-leaderCh:
		t.Fatalf("leader channel received %v after Stop, want no send", got)
	case <-time.After(200 * time.Millisecond):
	}

	// Releasing the first Wait drives the next ElectWait loop iteration:
	// the candidate campaigns again and waits again.
	close(firstWaitRelease)
	select {
	case <-fake.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not re-enter candidate.Wait() after the first Wait returned")
	}
	if got := fake.campaignCount(); got != 2 {
		t.Fatalf("Campaign calls after first Wait returned = %d, want 2", got)
	}
	if got := fake.waitCount(); got != 2 {
		t.Fatalf("Wait calls after first Wait returned = %d, want 2", got)
	}
	// The second Wait parks on a never-closed channel, so ElectWait stays
	// blocked there for the remainder of the test process.
}

// TestElectorStopMarksStopped verifies Stop() flips the elector into the
// stopped state: the leader-change callback registered through
// setLeaderChangeNotifyCall forwards before Stop and stops forwarding
// afterwards.
func TestElectorStopMarksStopped(t *testing.T) {
	fake := newFakeCandidate()
	leaderCh := make(chan bool, 8)
	w := &ElectWorker{
		ctx:       context.Background(),
		candidate: fake,
		logger:    nopElectorLogger{},
	}
	w.setLeaderChangeNotifyCall(leaderCh)

	// Sanity: the callback forwards before Stop.
	fake.notify(true)
	select {
	case got := <-leaderCh:
		if !got {
			t.Fatal("forwarded leader transition = false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("leader transition was not forwarded before Stop")
	}

	w.Stop()
	fake.notify(false)
	select {
	case got := <-leaderCh:
		t.Fatalf("leader channel received %v after Stop, want no send", got)
	case <-time.After(200 * time.Millisecond):
	}
}
