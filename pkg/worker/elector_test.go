package worker

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
	"spotter/internal/ports"
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
	// ctxDone optionally mirrors the elector context's Done channel: when
	// it fires, Wait returns, mimicking the real candidate whose Wait loop
	// exits when the context is canceled. A nil channel parks forever,
	// which keeps the legacy fake behavior unchanged.
	ctxDone <-chan struct{}
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

func (f *fakeCandidate) AddObserveCallFunc(fn election.LeaderChangeFunc) {
	if fn == nil {
		return
	}
	f.mu.Lock()
	f.callbacks = append(f.callbacks, fn)
	f.mu.Unlock()
}

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
	select {
	case <-release:
	case <-f.ctxDone:
	}
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

// newTestElector builds an elector through the exported candidate-injection
// seam (NewElectorWithCandidate) instead of assigning unexported fields.
// It returns the concrete worker so tests exercising unexported helpers
// (syncStoppedState) or the owned-client field can still reach them.
func newTestElector(t *testing.T, ctx context.Context, candidate election.Candidate, leaderCh chan bool) *ElectWorker {
	t.Helper()
	elector, err := NewElectorWithCandidate(ctx, candidate, leaderCh, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewElectorWithCandidate() error = %v, want nil", err)
	}
	w, ok := elector.(*ElectWorker)
	if !ok {
		t.Fatalf("NewElectorWithCandidate() returned %T, want *ElectWorker", elector)
	}
	return w
}

// TestNewElectorWithCandidateRejectsNilCandidate locks the input guard of
// the injection seam: a nil candidate must surface an error instead of
// returning a worker that would nil-deref inside ElectWait.
func TestNewElectorWithCandidateRejectsNilCandidate(t *testing.T) {
	elector, err := NewElectorWithCandidate(context.Background(), nil, make(chan bool, 1), ports.NopLogger{})
	if err == nil {
		t.Fatal("NewElectorWithCandidate() error = nil, want error for nil candidate")
	}
	if elector != nil {
		t.Fatalf("NewElectorWithCandidate() elector = %#v, want nil on error", elector)
	}
}

// TestNewElectorWithCandidateNilLoggerIsSafe verifies the nil-safe logger
// default: constructing with a nil logger and stopping immediately must not
// panic (the pkg/log global is not initialized in this package's tests, so
// a global fallback would nil-deref here).
func TestNewElectorWithCandidateNilLoggerIsSafe(t *testing.T) {
	elector, err := NewElectorWithCandidate(context.Background(), newFakeCandidate(), make(chan bool, 1), nil)
	if err != nil {
		t.Fatalf("NewElectorWithCandidate() error = %v, want nil", err)
	}
	elector.Stop()
	elector.Stop()
}

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
			nil,
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

// TestElectWaitBindsChannelAtCallTime verifies seam 5's channel-binding
// behavior: ElectWait(changes) registers the notify callback targeting
// exactly the channel passed at call time, overriding the
// constructor-registered one. Transitions observed by the candidate arrive
// on the ElectWait channel, not on the constructor channel.
func TestElectWaitBindsChannelAtCallTime(t *testing.T) {
	fake := newFakeCandidate()
	constructorCh := make(chan bool, 8)
	electorCh := make(chan bool, 8)
	w := newTestElector(t, context.Background(), fake, constructorCh)

	// Sanity: before ElectWait, the constructor-registered callback
	// forwards to the constructor channel.
	fake.notify(true)
	select {
	case got := <-constructorCh:
		if !got {
			t.Fatal("constructor channel received false before ElectWait, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transition was not forwarded to the constructor channel before ElectWait")
	}

	go w.ElectWait(electorCh)
	select {
	case <-fake.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not reach candidate.Wait()")
	}

	// After ElectWait(electorCh), forwarding targets the ElectWait channel.
	fake.notify(true)
	select {
	case got := <-electorCh:
		if !got {
			t.Fatal("ElectWait channel received false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transition was not forwarded to the ElectWait-provided channel")
	}
	select {
	case got := <-constructorCh:
		t.Fatalf("constructor channel received %v after ElectWait bound a new channel, want no send", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestElectWaitChannelSendIsNonFatalOnStalledConsumer verifies the F5c
// regression: when nobody ever reads the leader channel, a leader-change
// dispatch must not park forever — canceling the elector context must free
// the dispatch goroutine (the legacy bare `ch <- isLeader` blocked
// indefinitely, wedging every subsequent dispatch).
func TestElectWaitChannelSendIsNonFatalOnStalledConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeCandidate()
	fake.ctxDone = ctx.Done()
	// Unbuffered channel with no reader: every send parks.
	stalled := make(chan bool)
	w := newTestElector(t, ctx, fake, stalled)

	go w.ElectWait(stalled)
	select {
	case <-fake.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not reach candidate.Wait()")
	}

	// Fire a transition with nobody reading: the dispatch goroutine parks
	// on the channel send. The elector context must bound the park.
	dispatchReturned := make(chan struct{})
	go func() {
		defer close(dispatchReturned)
		fake.notify(true)
	}()

	cancel()
	select {
	case <-dispatchReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch goroutine did not return after the context was canceled (F5c: stalled consumer parks it forever)")
	}
	// The transition is dropped at shutdown: the send must not have
	// delivered (nobody read it).
	select {
	case got := <-stalled:
		t.Fatalf("stalled channel received %v, want the transition dropped at shutdown", got)
	default:
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
	w := newTestElector(t, context.Background(), fake, leaderCh)

	// ElectWait(nil) keeps the constructor-registered callback target
	// (leaderCh): the back-compat path for callers that already bound the
	// channel at construction.
	go w.ElectWait(nil)

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
	w := newTestElector(t, context.Background(), fake, leaderCh)

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

// --- F1 regression: stopped-flag data race ---------------------------------

// TestStopConcurrentWithCallbackDispatch verifies the F1 regression: the
// leader-change callback reads the stopped flag from dispatch goroutines
// (the real candidate fires callbacks via `go call(isLeader)`) while Stop
// writes it. Under the race detector this must stay clean, which requires
// every access to the stopped flag to be synchronized.
func TestStopConcurrentWithCallbackDispatch(t *testing.T) {
	fake := newFakeCandidate()
	leaderCh := make(chan bool, 128)
	w := newTestElector(t, context.Background(), fake, leaderCh)

	// Keep draining so callback sends never block during the hammer.
	drainDone := make(chan struct{})
	go func() {
		for range leaderCh {
		}
		close(drainDone)
	}()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Callback dispatchers: hammer the notify path the way the real
	// candidate's Wait loop does (each call lands in a goroutine that
	// reads the stopped flag).
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					fake.notify(true)
				}
			}
		}()
	}
	// Stop writer: repeatedly writes the stopped flag.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				w.Stop()
			}
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(leaderCh)
	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader channel drainer did not finish")
	}
}

// --- F2 regression: hot-spin watcher + per-iteration goroutine leak -------

// TestSyncStoppedStateReturnsAfterContextCancel verifies the F2 regression:
// syncStoppedState is a one-shot watcher that must return after the owner
// cancels the context (the legacy single-case select span forever at 100%
// CPU once the context was canceled) and must flip the elector into the
// stopped state so leader-change forwarding is suppressed.
func TestSyncStoppedStateReturnsAfterContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeCandidate()
	leaderCh := make(chan bool, 1)
	w := newTestElector(t, ctx, fake, leaderCh)

	done := make(chan struct{})
	go func() {
		w.syncStoppedState()
		close(done)
	}()
	time.Sleep(100 * time.Millisecond) // let the watcher park on ctx.Done

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("syncStoppedState did not return after the context was canceled")
	}

	// After the watcher fired, forwarding must be suppressed.
	fake.notify(true)
	select {
	case got := <-leaderCh:
		t.Fatalf("leader channel received %v after context cancel, want no send", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestElectWaitExitsAfterContextCancel verifies the F2 regression: once the
// owner cancels the context, the real candidate's Wait returns immediately,
// so ElectWait must exit instead of re-campaigning in a tight loop forever.
func TestElectWaitExitsAfterContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newFakeCandidate()
	fake.ctxDone = ctx.Done()
	w := newTestElector(t, ctx, fake, make(chan bool, 1))

	baseline := goroutineMin(100 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		w.ElectWait(nil)
		close(done)
	}()
	select {
	case <-fake.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not reach candidate.Wait()")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not exit after the context was canceled")
	}

	// Post-cancel iterations must not accumulate goroutines: both the
	// elector goroutine and its stopped-state watcher are gone.
	assertGoroutinesAtMost(t, baseline+1, 3*time.Second)
}

// TestElectWaitDoesNotAccumulateGoroutines verifies the F2 regression:
// ElectWait must not spawn a permanent goroutine per loop iteration (the
// legacy code spawned syncStoppedState, which never exits, on every
// iteration). Goroutine counting is a documented approximation: the
// baseline is sampled after the first iteration started (the elector
// goroutine and its single watcher already alive), then several more
// iterations run and the count must settle back to baseline.
func TestElectWaitDoesNotAccumulateGoroutines(t *testing.T) {
	const iterations = 6
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releases := make([]chan struct{}, iterations)
	for i := range releases {
		releases[i] = make(chan struct{})
	}
	fake := newFakeCandidate(releases...)
	w := newTestElector(t, ctx, fake, make(chan bool, 1))

	go w.ElectWait(nil)

	select {
	case <-fake.waitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ElectWait did not reach candidate.Wait()")
	}
	// Baseline with the elector goroutine and its single watcher alive.
	baseline := goroutineMin(200 * time.Millisecond)

	for i := 1; i < iterations; i++ {
		close(releases[i-1])
		select {
		case <-fake.waitStarted:
		case <-time.After(2 * time.Second):
			t.Fatalf("ElectWait iteration %d did not start", i+1)
		}
	}

	// iterations-1 more loop iterations ran; none may leave a permanent
	// goroutine behind. A tolerance of 1 covers runtime noise.
	assertGoroutinesAtMost(t, baseline+1, 3*time.Second)

	// Let the elector wind down so the test leaves no residue.
	cancel()
	close(releases[iterations-1])
}

// --- F6 regression: Stop must close the owned etcd client exactly once ----

// TestStopClosesEtcdClientOnce verifies the F6 regression: Stop must close
// the etcd client this worker owns (observable through the client's context
// being canceled) exactly once, and repeated Stop calls must stay safe.
func TestStopClosesEtcdClientOnce(t *testing.T) {
	cl, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("clientv3.New() error = %v, want lazy client for refused endpoint", err)
	}
	w := newTestElector(t, context.Background(), newFakeCandidate(), make(chan bool, 1))
	// No exported constructor pairs an injected candidate with an owned etcd
	// client (that combination only arises from NewElectorWithDeps with a
	// live cluster), so mirror the ownership handoff that constructor
	// performs. The candidate is still injected through the exported seam.
	w.etcdclient = cl

	w.Stop()
	if cl.Ctx().Err() == nil {
		t.Fatal("Stop() did not close the etcd client")
	}

	// A second Stop must not panic or re-close.
	w.Stop()
	if cl.Ctx().Err() == nil {
		t.Fatal("etcd client context became live again after the second Stop")
	}
}

// TestStopWithNilEtcdClient verifies the F6 nil guard: test-constructed
// workers without an etcd client must be able to Stop safely.
func TestStopWithNilEtcdClient(t *testing.T) {
	w := &ElectWorker{
		ctx:       context.Background(),
		candidate: newFakeCandidate(),
		logger:    nopElectorLogger{},
	}
	w.Stop()
	w.Stop()
}

// --- goroutine-leak helpers (documented approximations) --------------------

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
