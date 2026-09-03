package election

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"spotter/pkg/log"
)

// observedLogs captures everything the candidate logs through the pkg/log
// global, so failure paths (the F3 rebuild log, the F5 panic-recovery log)
// are assertable in tests without a logging seam (full logger injection is
// the next phase).
var observedLogs *observer.ObservedLogs

func TestMain(m *testing.M) {
	core, logs := observer.New(zapcore.InfoLevel)
	observedLogs = logs
	log.Logger = zap.New(core).Sugar()
	os.Exit(m.Run())
}

// newRefusedEndpointClient returns a lazily-dialed client pointed at a
// refused endpoint: construction succeeds and the first RPC (Grant) fails.
func newRefusedEndpointClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cl, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("clientv3.New() error = %v, want lazy client for refused endpoint", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// hasErrorEntry reports whether any captured entry is an error-level log
// whose message contains substring.
func hasErrorEntry(entries []observer.LoggedEntry, substring string) bool {
	for _, entry := range entries {
		if entry.Level == zapcore.ErrorLevel && strings.Contains(entry.Message, substring) {
			return true
		}
	}
	return false
}

// TestNewElectionSessionGrantFailureClearsStaleSession verifies the F3
// regression: when the session rebuild fails (the initial Grant errors
// against an unreachable cluster), the failure must be logged and the stale
// closed session/election must be cleared. The legacy code returned
// silently, leaving Wait to re-campaign against the closed session.
func TestNewElectionSessionGrantFailureClearsStaleSession(t *testing.T) {
	c := &candidate{
		ctx:           context.Background(),
		client:        newRefusedEndpointClient(t),
		campaignKey:   "/spotter-test/f3-grant-failure",
		callBackFuncs: []LeaderChangeFunc{},
	}
	// Production scenario this mirrors: Wait lost leadership, Close()
	// closed the old session, and now the rebuild fails — the stale
	// pointers must not survive the failed rebuild.
	staleSession := &concurrency.Session{}
	c.session = staleSession
	c.election = concurrency.NewElection(staleSession, c.campaignKey)

	observedLogs.TakeAll()
	c.NewElectionSession(500 * time.Millisecond)

	if c.session != nil {
		t.Fatal("NewElectionSession() left session pointing at the closed session after a failed rebuild")
	}
	if c.election != nil {
		t.Fatal("NewElectionSession() left election pointing at the closed session after a failed rebuild")
	}
	if entries := observedLogs.TakeAll(); !hasErrorEntry(entries, "election session rebuild failed") {
		t.Fatalf("NewElectionSession() Grant failure was not logged (entries: %v)", entries)
	}
}

// TestNewCandidateGrantFailureSurfacesError locks the NewCandidate contract
// the elector relies on: a failing initial Grant must surface an error to
// the caller instead of returning a half-built candidate.
func TestNewCandidateGrantFailureSurfacesError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewCandidate(ctx, newRefusedEndpointClient(t), "/spotter-test/newcandidate-failure"); err == nil {
		t.Fatal("NewCandidate() error = nil, want error when the initial Grant fails")
	}
}

// TestNewCandidateNilClientSurfacesError locks the input guard.
func TestNewCandidateNilClientSurfacesError(t *testing.T) {
	if _, err := NewCandidate(context.Background(), nil, "/spotter-test/nil-client"); err == nil {
		t.Fatal("NewCandidate() error = nil, want error for nil etcd client")
	}
}

// --- F5 regression: callback dispatch hazards ------------------------------

// TestWaitTicksDispatchOnlyOnChange verifies the F5 fix: repeated Wait
// ticks observing the same leadership value dispatch exactly one callback
// (plus the first observation), and a changed value dispatches again. This
// mirrors consecutive Wait loop ticks through the extracted tick decision
// (leaderState.shouldDispatch) and dispatcher (candidate.notify). The
// legacy code fired every registered callback on every 2s tick, ~30
// events/minute per channel forever, even with no leadership change.
func TestWaitTicksDispatchOnlyOnChange(t *testing.T) {
	c := &candidate{
		ctx:           context.Background(),
		client:        newRefusedEndpointClient(t),
		callBackFuncs: []LeaderChangeFunc{},
	}
	events := make(chan bool, 8)
	c.AddObserveCallFunc(func(isLeader bool) { events <- isLeader })

	// Ticks 1-3 observe the same value: only the first may dispatch.
	var state leaderState
	for i := 0; i < 3; i++ {
		if state.shouldDispatch(true) {
			c.notify(true)
		}
	}
	// Tick 4 observes a change: dispatches again.
	if state.shouldDispatch(false) {
		c.notify(false)
	}

	for i, want := range []bool{true, false} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event %d = %v, want %v", i, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("event %d (want %v) was not dispatched", i, want)
		}
	}
	select {
	case got := <-events:
		t.Fatalf("unexpected extra event %v: dispatch must fire only on change", got)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestNotifyRecoversFromCallbackPanic verifies the F5 fix: a panicking
// leader-change callback is recovered and logged instead of crashing the
// process, and callbacks registered after it still run on the same tick.
// The legacy bare `go call(isLeader)` had no panic isolation.
func TestNotifyRecoversFromCallbackPanic(t *testing.T) {
	c := &candidate{
		ctx:           context.Background(),
		client:        newRefusedEndpointClient(t),
		callBackFuncs: []LeaderChangeFunc{},
	}
	events := make(chan bool, 8)
	c.AddObserveCallFunc(func(isLeader bool) {
		panic("leader change callback boom")
	})
	c.AddObserveCallFunc(func(isLeader bool) { events <- isLeader })

	observedLogs.TakeAll()
	c.notify(true)

	select {
	case got := <-events:
		if !got {
			t.Fatal("healthy callback received false, want true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("healthy callback was not dispatched after a sibling callback panicked")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !hasErrorEntry(observedLogs.TakeAll(), "leader change callback panicked") {
		if time.Now().After(deadline) {
			t.Fatal("the callback panic was not logged by the recovering dispatcher")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWaitExitsWhenContextCanceled covers the Wait exit path: once the
// owner cancels the candidate context, Wait returns instead of polling
// forever.
func TestWaitExitsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &candidate{
		ctx:           ctx,
		client:        newRefusedEndpointClient(t),
		callBackFuncs: []LeaderChangeFunc{},
	}
	done := make(chan struct{})
	go func() {
		c.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not exit after the context was canceled")
	}
}
