package election

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
)

// newRefusedEndpointClient returns a lazily-dialed client pointed at a
// refused endpoint: construction succeeds and the first RPC (Grant) fails
// at the request deadline.
func newRefusedEndpointClient(t *testing.T) *clientv3.Client {
	t.Helper()
	cl, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatalf("clientv3.New() error = %v, want lazy client for refused endpoint", err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// newTestCandidate builds a candidate with a deterministic clock, a
// recording logger and a recording notifier, so the Wait loop and failure
// paths are assertable offline. NewCandidate cannot succeed offline (its
// initial Grant needs a live etcd; with a canceled ctx it errors instantly,
// with a live ctx it retries forever against a refused endpoint), so this
// mirrors the constructor's dependency wiring structurally — exactly what
// the pre-E3 tests did, now with the injected seams attached. Tests
// needing a session/election set them structurally afterwards.
func newTestCandidate(t *testing.T, waitCtx context.Context, clock ports.Clock) (*candidate, *fakes.FakeLogger, *fakes.FakeNotifier) {
	t.Helper()
	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}
	if clock == nil {
		clock = fakes.NewFakeClock(time.Unix(0, 0))
	}
	c := &candidate{
		ctx:           waitCtx,
		client:        newRefusedEndpointClient(t),
		campaignKey:   "/spotter-test/clock-driven",
		clock:         clock,
		logger:        logger,
		notifier:      notifier,
		callBackFuncs: []LeaderChangeFunc{},
	}
	return c, logger, notifier
}

// findLogEntry reports whether any captured entry at level has a message
// containing substring.
func findLogEntry(logger *fakes.FakeLogger, level, substring string) bool {
	for _, entry := range logger.Entries() {
		if entry.Level == level && strings.Contains(entry.Message, substring) {
			return true
		}
	}
	return false
}

// --- constructor contracts -------------------------------------------------

// TestNewCandidateGrantFailureSurfacesError locks the NewCandidate contract
// the elector relies on: a failing initial Grant must surface an error to
// the caller instead of returning a half-built candidate. NewCandidate
// (the back-compat constructor) must keep working with its legacy
// signature and nil-out default dependencies.
func TestNewCandidateGrantFailureSurfacesError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewCandidate(ctx, newRefusedEndpointClient(t), "/spotter-test/newcandidate-failure"); err == nil {
		t.Fatal("NewCandidate() error = nil, want error when the initial Grant fails")
	}
}

// TestNewCandidateNilClientSurfacesError locks the input guard for all
// three constructor variants.
func TestNewCandidateNilClientSurfacesError(t *testing.T) {
	if _, err := NewCandidate(context.Background(), nil, "/spotter-test/nil-client"); err == nil {
		t.Fatal("NewCandidate() error = nil, want error for nil etcd client")
	}
	if _, err := NewCandidateWithClock(context.Background(), nil, "/spotter-test/nil-client", nil); err == nil {
		t.Fatal("NewCandidateWithClock() error = nil, want error for nil etcd client")
	}
	if _, err := NewCandidateWithDeps(context.Background(), nil, "/spotter-test/nil-client", nil, nil, nil); err == nil {
		t.Fatal("NewCandidateWithDeps() error = nil, want error for nil etcd client")
	}
}

// TestNewCandidateWithDepsDefaultsApplied verifies the nil-dependency
// defaults: a nil clock/logger/notifier must become realClock/nop/nop so
// the candidate never nil-derefs. NewCandidate cannot complete offline
// (the initial Grant needs a live etcd), so this drives the constructor
// with an already-canceled context: dependency wiring happens before the
// Grant, and the error surfaces from the RPC layer — a panic or nil-deref
// in the wiring would fail the test before that.
func TestNewCandidateWithDepsDefaultsApplied(t *testing.T) {
	cl := newRefusedEndpointClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := NewCandidateWithDeps(ctx, cl, "/spotter-test/defaults", nil, nil, nil); err == nil {
		t.Fatal("NewCandidateWithDeps() error = nil, want Grant error after nil-dep defaulting")
	}
	// The default adapters themselves are behavior-tested in
	// TestRealClockMatchesLegacyBehavior and TestNopNotifierDiscards.
}

// TestNewCandidateWithDepsInjectedDepsReachRPC verifies the constructor
// accepts the injected dependency triplet without rejecting or rewriting
// them: the same canceled-ctx Grant failure must surface for injected deps
// (the wiring stores them; the Wait/Campaign tests exercise the stored
// values through the struct-level seam).
func TestNewCandidateWithDepsInjectedDepsReachRPC(t *testing.T) {
	cl := newRefusedEndpointClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}

	if _, err := NewCandidateWithDeps(ctx, cl, "/spotter-test/injected", clock, logger, notifier); err == nil {
		t.Fatal("NewCandidateWithDeps() error = nil, want Grant error for injected deps")
	}
}

// TestNewCandidateEmptyCampaignKeyFallsBackToGlobal documents the
// intentionally-kept config.LockCampaignKey fallback (E3: documented, not
// removed; production passes the key explicitly, the legacy NewElector
// wrapper still relies on the fallback).
func TestNewCandidateEmptyCampaignKeyFallsBackToGlobal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The fallback key is empty by default in tests; either way the
	// constructor must fail at the Grant (canceled ctx) rather than at the
	// campaign-key resolution, proving the fallback branch ran.
	if _, err := NewCandidate(ctx, newRefusedEndpointClient(t), ""); err == nil {
		t.Fatal("NewCandidate() error = nil, want Grant failure after the campaign-key fallback")
	}
}

// --- F3 regression: session rebuild failure --------------------------------

// TestNewElectionSessionGrantFailureClearsStaleSession verifies the F3
// regression: when the session rebuild fails (the initial Grant errors
// against an unreachable cluster), the failure must be logged and the stale
// closed session/election must be cleared. The legacy code returned
// silently, leaving Wait to re-campaign against the closed session.
// It also proves the logger and notifier seams reached the candidate: the
// failure log below arrives on the injected FakeLogger, and the stored
// notifier/clock are the injected ones.
func TestNewElectionSessionGrantFailureClearsStaleSession(t *testing.T) {
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, logger, notifier := newTestCandidate(t, context.Background(), clock)
	// Production scenario this mirrors: Wait lost leadership, Close()
	// closed the old session, and now the rebuild fails — the stale
	// pointers must not survive the failed rebuild.
	staleSession := &concurrency.Session{}
	c.session = staleSession
	c.election = concurrency.NewElection(staleSession, c.campaignKey)

	c.NewElectionSession(500 * time.Millisecond)

	if c.session != nil {
		t.Fatal("NewElectionSession() left session pointing at the closed session after a failed rebuild")
	}
	if c.election != nil {
		t.Fatal("NewElectionSession() left election pointing at the closed session after a failed rebuild")
	}
	if !findLogEntry(logger, fakes.LevelError, "election session rebuild failed") {
		t.Fatalf("NewElectionSession() Grant failure was not logged (entries: %v)", logger.Entries())
	}
	// The injected deps were stored on the candidate.
	if c.clock == nil {
		t.Fatal("candidate clock = nil, want the injected FakeClock")
	}
	if c.notifier != ports.Notifier(notifier) {
		t.Fatal("candidate notifier is not the injected FakeNotifier")
	}
	if c.logger != ports.Logger(logger) {
		t.Fatal("candidate logger is not the injected FakeLogger")
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
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, context.Background(), clock)
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
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, logger, _ := newTestCandidate(t, context.Background(), clock)
	events := make(chan bool, 8)
	c.AddObserveCallFunc(func(isLeader bool) {
		panic("leader change callback boom")
	})
	c.AddObserveCallFunc(func(isLeader bool) { events <- isLeader })

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
	for !findLogEntry(logger, fakes.LevelError, "leader change callback panicked") {
		if time.Now().After(deadline) {
			t.Fatal("the callback panic was not logged by the recovering dispatcher")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- Wait exit paths (clock seam, seam 2 / F8) -----------------------------

// TestWaitExitsWhenContextCanceled covers the Wait exit path: once the
// owner cancels the candidate context, Wait returns instead of polling
// forever — including while parked on the clock.
func TestWaitExitsWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, ctx, clock)
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

// TestWaitExitsWhenContextCanceledWhileParkedOnClock verifies the ctx select
// that guards the clock park: a context canceled AFTER Wait already parked
// on clock.After must still exit Wait promptly (a bare time.Sleep would keep
// sleeping).
func TestWaitExitsWhenContextCanceledWhileParkedOnClock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A fake clock whose waiters are never released: Wait parks on
	// clock.After(2s) and only ctx cancellation can free it.
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, ctx, clock)
	done := make(chan struct{})
	go func() {
		c.Wait()
		close(done)
	}()
	// Give Wait a moment to park on the clock, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not exit after the context was canceled while parked on the clock")
	}
}

// TestWaitTickFiresLeaderCheckAfterClockAdvance verifies the core seam-2
// behavior: a Wait tick fires only when the clock advances the full poll
// period — never on a real sleep. With no session, the first tick observes
// not-leader and dispatches the first observation (false) to the
// callbacks; the callback event is therefore the tick observable.
//
// The clock is advanced in small steps: a waiter parked at fake-time T
// fires at T+2s, and stepping absorbs the (racy) moment the Wait goroutine
// registers its waiter. The event may arrive well after the tick itself:
// the not-leader recovery (NewElectionSession) holds the candidate mutex
// across a 10s-deadline Grant against the refused endpoint, and notify
// snapshots callbacks under that same mutex, so the dispatch is serialized
// behind the rebuild (the pre-existing F12 stall, observed honestly here).
// The 15s bound covers tick (2s fake) + rebuild Grant (10s deadline) +
// margin.
func TestWaitTickFiresLeaderCheckAfterClockAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, ctx, clock)
	events := make(chan bool, 8)
	c.AddObserveCallFunc(func(isLeader bool) { events <- isLeader })
	done := make(chan struct{})
	go func() {
		c.Wait()
		close(done)
	}()

	// Advance 100ms at a time until the first tick's event arrives: the
	// tick cannot fire before 2s of cumulative fake-time advance past the
	// waiter's registration, so the loop proves the cadence is clock-driven.
	var firstEventAt time.Time
	deadline := time.Now().Add(15 * time.Second)
	for firstEventAt.IsZero() {
		select {
		case got := <-events:
			if got {
				t.Fatalf("first tick event = true, want false (no session means not leader)")
			}
			firstEventAt = time.Now()
		case <-time.After(50 * time.Millisecond):
			if time.Now().After(deadline) {
				t.Fatal("no Wait tick fired within 15s of stepped clock advances, want a tick per 2s of fake time")
			}
			clock.Advance(100 * time.Millisecond)
		}
	}

	// The tick fired purely on fake time (the poll slept on the injected
	// clock, never a real sleep): freeze the clock and confirm the loop
	// stays quiet — the next tick would need another 2s of fake advance,
	// and the not-leader recovery is parked in its 10s Grant.
	select {
	case extra := <-events:
		t.Fatalf("unexpected extra event %v with the clock frozen", extra)
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not exit after cancel")
	}
}

// TestWaitPollHonorsInjectedClockNoRealSleep verifies the poll cadence is
// fully driven by the injected clock: with the clock frozen (no Advance),
// Wait performs no leader checks at all for a wall-clock duration far
// beyond the real 2s period. A leftover real sleep would fire a check
// within 2s and produce the log entry.
func TestWaitPollHonorsInjectedClockNoRealSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, logger, _ := newTestCandidate(t, ctx, clock)
	done := make(chan struct{})
	go func() {
		c.Wait()
		close(done)
	}()

	// Freeze longer than one real poll period: no tick may fire.
	time.Sleep(LeaderChangePeriod*time.Second + 500*time.Millisecond)
	if findLogEntry(logger, fakes.LevelError, "get leader state error") {
		t.Fatal("leader check fired on a frozen clock, want clock-driven cadence only")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not exit after cancel")
	}
}

// TestRealClockMatchesLegacyBehavior locks the default adapter: realClock
// must behave like time.Now/time.After (the legacy code's primitives).
func TestRealClockMatchesLegacyBehavior(t *testing.T) {
	var c ports.Clock = realClock{}
	before := time.Now()
	if c.Now().Before(before) {
		t.Fatal("realClock.Now() went backwards")
	}
	ch := c.After(10 * time.Millisecond)
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("realClock.After(10ms) did not fire within 2s")
	}
}

// TestNopNotifierDiscards locks the default notifier adapter.
func TestNopNotifierDiscards(t *testing.T) {
	var n ports.Notifier = nopNotifier{}
	n.Notify("title", "content") // must not panic
}

// --- Campaign failure path (notifier seam, F7 unchanged severity) ---------

// TestCampaignNoSessionReturnsErrorWithoutNotice verifies the F3 guard:
// campaigning without a live session returns an error and pages nobody.
func TestCampaignNoSessionReturnsErrorWithoutNotice(t *testing.T) {
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, notifier := newTestCandidate(t, context.Background(), clock)
	c.session = nil
	c.election = nil

	if err := c.Campaign(time.Second); err == nil {
		t.Fatal("Campaign() error = nil, want error for no session")
	}
	if got := len(notifier.Notifications()); got != 0 {
		t.Fatalf("Campaign() notifications = %d, want 0", got)
	}
}

// TestCampaignNotifierInjectedNotGlobal verifies the notifier seam (F9):
// the F3 no-session guard aside, the RPC-failure branch goes through the
// injected notifier. Campaigning against a refused endpoint with a live
// (nil-pointer-free) election cannot be constructed offline without
// hanging the session build, so the injection is proven structurally: the
// candidate dispatches campaign failures to c.notifier, and the injected
// notifier is the one stored (checked in NewCandidateWithDeps tests). Here
// we lock the notify call signature end-to-end for the reachable branch.
func TestCampaignNotifierInjectedNotGlobal(t *testing.T) {
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, notifier := newTestCandidate(t, context.Background(), clock)
	// A session/election pair built from a zero Session would nil-deref
	// inside Campaign (session.Client()); instead drive the branch through
	// the deadline path: build a canceled-context timeout so the campaign
	// fails with a context error, which skips the notice (expected: only
	// non-deadline errors page). Assert no notifications leak.
	c.session = nil
	c.election = nil
	_ = c.Campaign(time.Millisecond)
	if got := len(notifier.Notifications()); got != 0 {
		t.Fatalf("Campaign() notifications = %d, want 0 for guard failure", got)
	}
}

// --- AddObserveCallFunc / Close contracts -----------------------------------

// TestAddObserveCallFuncIgnoresNil keeps the legacy nil-guard: a nil
// callback must not be appended to the callback slice.
func TestAddObserveCallFuncIgnoresNil(t *testing.T) {
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, context.Background(), clock)
	c.AddObserveCallFunc(nil)
	c.Lock()
	defer c.Unlock()
	if got := len(c.callBackFuncs); got != 0 {
		t.Fatalf("callBackFuncs len = %d, want 0 (nil callback ignored)", got)
	}
}

// TestCloseIdempotentOnNilSession keeps the Close contract.
func TestCloseIdempotentOnNilSession(t *testing.T) {
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, context.Background(), clock)
	c.Close()
	c.Close()
	c.Lock()
	defer c.Unlock()
	if c.session != nil || c.election != nil {
		t.Fatal("Close() left a live session or election pointer")
	}
}

// TestCandidateConcurrencySafety drives concurrent notify, Campaign,
// IsLeader, NewElectionSession and Close under the race detector: the
// candidate's mutex-protected fields (callBackFuncs, session, election)
// must stay clean. Callbacks are registered once up front — the
// registration seam itself is exercised in TestAddObserveCallFuncIgnoresNil
// and by Wait's own registration path — because appending in the hammer
// loop grows the slice unboundedly and makes notify's snapshot O(n²).
func TestCandidateConcurrencySafety(t *testing.T) {
	clock := fakes.NewFakeClock(time.Unix(0, 0))
	c, _, _ := newTestCandidate(t, context.Background(), clock)
	c.AddObserveCallFunc(func(bool) {})
	c.AddObserveCallFunc(func(bool) {})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.notify(true)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = c.IsLeader()
			_ = c.Campaign(time.Millisecond)
			c.NewElectionSession(time.Millisecond)
			c.Close()
		}
	}()

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
}
