package worker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/fakes"
)

// The black-box tier for package worker exercises the retry-queue semantics
// of NewResourceWorker (Handle -> pusher.Push -> UnsyncedService retry) and
// the revision replacement contract of NewUnsyncedService through the public
// API only, using the fakes testkit doubles. The retry loop runs on a real
// 5s ticker, so the full retry cycle below uses a bounded polling wait
// (deadline 7s) instead of the fake clock; the cadence itself is documented
// in pkg/worker/unsynced_service.go (5 * time.Second).

// blackboxPushCall records one scripted-sink Push invocation: the trigger
// time and the instances exactly as received.
type blackboxPushCall struct {
	TriggerTime int64
	Instances   []*instance.Instance
}

// blackboxScriptedSink fails the first failCount Push calls and succeeds
// afterwards. It records the full payload of every Push (trigger time and
// instances) so tests can assert what was actually re-pushed. All access is
// mutex-guarded because the unsynced retry loop and the Handle path may push
// concurrently.
type blackboxScriptedSink struct {
	mu        sync.Mutex
	failCount int
	pushCalls []blackboxPushCall
}

func (s *blackboxScriptedSink) Push(triggerTime int64, instances []*instance.Instance) error {
	s.mu.Lock()
	s.pushCalls = append(s.pushCalls, blackboxPushCall{
		TriggerTime: triggerTime,
		Instances:   cloneInstances(instances),
	})
	fail := s.failCount > 0
	if fail {
		s.failCount--
	}
	s.mu.Unlock()

	if fail {
		return errors.New("scripted push failure")
	}
	return nil
}

func (s *blackboxScriptedSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	return nil
}

func (s *blackboxScriptedSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return &instance.InstanceList{}, nil
}

// pushCallCount returns the number of recorded Push calls.
func (s *blackboxScriptedSink) pushCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pushCalls)
}

// pushSnapshot returns an independent copy of the recorded Push calls.
func (s *blackboxScriptedSink) pushSnapshot() []blackboxPushCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := make([]blackboxPushCall, len(s.pushCalls))
	for i, call := range s.pushCalls {
		snapshot[i] = blackboxPushCall{
			TriggerTime: call.TriggerTime,
			Instances:   cloneInstances(call.Instances),
		}
	}
	return snapshot
}

// setFailCount reconfigures the failure script.
func (s *blackboxScriptedSink) setFailCount(count int) {
	s.mu.Lock()
	s.failCount = count
	s.mu.Unlock()
}

func cloneInstances(instances []*instance.Instance) []*instance.Instance {
	if instances == nil {
		return nil
	}
	cloned := make([]*instance.Instance, len(instances))
	for i, item := range instances {
		if item == nil {
			continue
		}
		copied := *item
		cloned[i] = &copied
	}
	return cloned
}

// TestBlackboxRetryQueueSemanticsFailedPushQueuedThenRetriedAndCleared: a
// Push failure puts the instance into the unsynced queue (observable via
// metrics QueueDepths > 0) and the real retry ticker re-pushes it; once the
// scripted sink succeeds, the queue drains to zero and the sink has seen the
// instance pushed again. The retry cadence is 5s of real time (see
// unsynced_service.go). The wait bound is 15s: two full retry ticks plus
// margin, because the process may be descheduled while other packages run
// in parallel under `make` (a tight 7s bound proved flaky under that load:
// the tick that publishes the queue depth is the same tick that retries, so
// one missed tick plus a late flip to success exhausts a single-tick bound).
func TestBlackboxRetryQueueSemanticsFailedPushQueuedThenRetriedAndCleared(t *testing.T) {
	sink := &blackboxScriptedSink{failCount: 1}
	metrics := fakes.NewFakeMetricsRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, metrics)
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	// First push fails: the handler must enqueue the instance for retry.
	w.Handle(&Event{
		Trigger: 123,
		Data:    []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		Operate: OperateTypeSync,
	})

	if got := sink.pushCallCount(); got != 1 {
		t.Fatalf("push calls after Handle = %d, want 1", got)
	}

	// The failed instance enters the unsynced queue: the retry ticker
	// publishes the queue depth on every tick, so a first depth > 0 proves
	// queueing. The depth is only published on the 5s tick, not at Handle
	// time, so poll for it (retry cadence 5s, bound 15s for descheduling
	// margin under parallel test load).
	queuedDeadline := time.Now().Add(15 * time.Second)
	depths := metrics.QueueDepths()
	for time.Now().Before(queuedDeadline) && (len(depths) == 0 || depths[0].Depth <= 0) {
		time.Sleep(25 * time.Millisecond)
		depths = metrics.QueueDepths()
	}
	if len(depths) == 0 || depths[0].Depth <= 0 {
		t.Fatalf("queue depths after failed push = %v, want a first recorded depth > 0", depths)
	}

	// Flip the scripted sink to success. The queued retry (the same 5s tick
	// that recorded the first depth, or the next one) then pushes the
	// instance again and drains the queue to zero. Bounded: one more retry
	// tick, cadence 5s, bound 15s (two ticks plus margin for parallel
	// package load).
	sink.setFailCount(0)

	drainDeadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(drainDeadline) {
		if sink.pushCallCount() >= 2 && lastDepth(metrics.QueueDepths()) == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	calls := sink.pushSnapshot()
	if got := len(calls); got < 2 {
		t.Fatalf("push calls after retry window = %d, want >= 2 (original + retry)", got)
	}
	// The retry must re-push the same instance (id and revision), with the
	// original trigger time carried through the queued event.
	retry := calls[len(calls)-1]
	if retry.TriggerTime != 123 {
		t.Fatalf("retry trigger time = %d, want the queued event trigger 123", retry.TriggerTime)
	}
	if len(retry.Instances) != 1 {
		t.Fatalf("retry instances = %#v, want exactly the one queued instance", retry.Instances)
	}
	if retry.Instances[0].InstanceId != "instance-1" || retry.Instances[0].Reversion != 42 {
		t.Fatalf("retry instance = %#v, want instance-1 with reversion 42", retry.Instances[0])
	}
	depths = metrics.QueueDepths()
	if len(depths) < 2 {
		t.Fatalf("queue depth observations = %v, want at least 2 (nonzero while queued, zero after drain)", depths)
	}
	if lastDepth(depths) != 0 {
		t.Fatalf("last queue depth = %d, want 0 after successful retry; all depths = %v", lastDepth(depths), depths)
	}

	cancel()
}

// TestBlackboxRetryQueueSemanticsKeepsInstanceOnPersistentFailure: while the
// sink keeps failing, the instance stays queued and each retry tick attempts
// a new push (the queue must not drain on failures).
func TestBlackboxRetryQueueSemanticsKeepsInstanceOnPersistentFailure(t *testing.T) {
	sink := &blackboxScriptedSink{failCount: 1000}
	metrics := fakes.NewFakeMetricsRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, metrics)
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	w.Handle(&Event{
		Trigger: 321,
		Data:    []*instance.Instance{{InstanceId: "instance-2", Reversion: 7}},
		Operate: OperateTypeSync,
	})

	// Bounded wait for one retry tick: cadence 5s, bound 15s (two ticks
	// plus margin for descheduling under parallel test load). The retry
	// must fire another push while the queue depth stays > 0.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if sink.pushCallCount() >= 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if got := sink.pushCallCount(); got < 2 {
		t.Fatalf("push calls after retry window = %d, want >= 2 (retry must re-attempt on failure)", got)
	}
	depths := metrics.QueueDepths()
	if len(depths) == 0 {
		t.Fatal("queue depth observations = none, want the retry loop to publish depths")
	}
	if lastDepth(depths) == 0 {
		t.Fatalf("last queue depth = 0, want > 0 while pushes keep failing; depths = %v", depths)
	}

	cancel()
}

// TestBlackboxRetryQueueSemanticsReversionWinsReplacesQueuedEvent: Add keeps
// the newer revision for the same instance ID and drops stale revisions; the
// surviving revision is the one actually re-pushed on retry.
func TestBlackboxRetryQueueSemanticsReversionWinsReplacesQueuedEvent(t *testing.T) {
	sink := &blackboxScriptedSink{failCount: 1000}
	service := NewUnsyncedService(context.Background(), sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())

	service.Add(1, []*instance.Instance{{InstanceId: "instance-3", Reversion: 10}}, nil)
	service.Add(2, []*instance.Instance{{InstanceId: "instance-3", Reversion: 20}}, nil)
	service.Add(3, []*instance.Instance{{InstanceId: "instance-3", Reversion: 15}}, nil)

	if got := service.Len(); got != 1 {
		t.Fatalf("queued events = %d, want 1 (same instance ID collapses)", got)
	}

	// The surviving event must be the newest revision: a successful sync
	// drains the queue and re-pushes exactly the revision-20 instance.
	// Note: Add replaces the queued instance's Data on a higher Reversion
	// but keeps the FIRST Add's trigger time for the event, so the re-push
	// carries trigger 1 (the original queue entry) with the newest data.
	sink.setFailCount(0)
	service.syncOnce()

	if got := service.Len(); got != 0 {
		t.Fatalf("queued events after successful retry = %d, want 0", got)
	}

	calls := sink.pushSnapshot()
	if got := len(calls); got != 1 {
		t.Fatalf("push calls = %d, want 1 (single re-push of the surviving event)", got)
	}
	if calls[0].TriggerTime != 1 {
		t.Fatalf("re-push trigger time = %d, want 1 (the queued event keeps its original trigger)", calls[0].TriggerTime)
	}
	if len(calls[0].Instances) != 1 {
		t.Fatalf("re-push instances = %#v, want exactly one instance", calls[0].Instances)
	}
	repushed := calls[0].Instances[0]
	if repushed.InstanceId != "instance-3" {
		t.Fatalf("re-pushed instance id = %q, want instance-3", repushed.InstanceId)
	}
	if repushed.Reversion != 20 {
		t.Fatalf("re-pushed reversion = %d, want 20 (highest revision wins; 10 and 15 dropped)", repushed.Reversion)
	}
}

func lastDepth(depths []fakes.QueueDepthObservation) int {
	if len(depths) == 0 {
		return -1
	}
	return depths[len(depths)-1].Depth
}

// permanentError is a test-local sink error exposing the Permanent() seam of
// the nacos APIError: a 4xx whose identical retry can never succeed.
type permanentError struct{}

func (permanentError) Error() string {
	return "sink answered status 400: param ip is required"
}

func (permanentError) Permanent() bool {
	return true
}

// TestBlackboxRetryQueueSemanticsDropsPermanentError: a push that fails with
// a permanent error (the nacos 4xx class) must be dropped from the retry
// queue after the failed attempt instead of spinning forever — the live
// incident retried an unregisterable DELETE 9624 times.
func TestBlackboxRetryQueueSemanticsDropsPermanentError(t *testing.T) {
	sink := &fakes.FakeInstanceSink{PushErr: permanentError{}}
	service := NewUnsyncedService(context.Background(), sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())

	service.Add(1, []*instance.Instance{{InstanceId: "instance-4", Reversion: 42}}, nil)
	if got := service.Len(); got != 1 {
		t.Fatalf("queued events = %d, want 1 before the retry", got)
	}

	service.syncOnce()

	if got := service.Len(); got != 0 {
		t.Fatalf("queued events after a permanent failure = %d, want 0 (dropped, not retried)", got)
	}
	// One failed push happened; the drop means the next cycle pushes nothing.
	if got := len(sink.PushCalls()); got != 1 {
		t.Fatalf("push calls = %d, want 1 (the failed attempt only)", got)
	}
}

// TestBlackboxRetryQueueSemanticsKeepsPlainErrorQueued: a plain (non-4xx)
// error stays queued for the next cycle — only permanent errors are dropped.
func TestBlackboxRetryQueueSemanticsKeepsPlainErrorQueued(t *testing.T) {
	sink := &fakes.FakeInstanceSink{PushErr: errors.New("transient failure")}
	service := NewUnsyncedService(context.Background(), sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())

	service.Add(1, []*instance.Instance{{InstanceId: "instance-5", Reversion: 42}}, nil)
	service.syncOnce()

	if got := service.Len(); got != 1 {
		t.Fatalf("queued events after a plain failure = %d, want 1 (kept for retry)", got)
	}
	if got := len(sink.PushCalls()); got != 1 {
		t.Fatalf("push calls = %d, want 1", got)
	}
}

// permanentTestError is a leaf error implementing Permanent(), the shape the
// nacos APIError carries: it is what the retry queue's drop check must see
// through every layer of aggregation and wrapping.
type permanentTestError struct{}

func (permanentTestError) Error() string {
	return "nacos: DELETE /nacos/v1/ns/instance answered status 400: Param 'ip' is required"
}

func (permanentTestError) Permanent() bool {
	return true
}

// TestBlackboxRetryQueueSemanticsDropsPermanentErrorThroughFanout: the
// production error shape — a nacos-positioned sink failing with the
// %w-wrapped Permanent error inside a REAL FanoutSink, further %w-wrapped by
// an adapter — must still classify as permanent. FanoutError.Unwrap is the
// contract that makes errors.As traverse the per-sink failures; deleting it
// silently reintroduces the F7 spin the moment error classification moves
// to any aggregate-inspecting site (audit AUDIT-A-2/D-7).
func TestBlackboxRetryQueueSemanticsDropsPermanentErrorThroughFanout(t *testing.T) {
	// The exact production nesting: the sink returns the fmt.Errorf the
	// nacos adapter's deregister builds (%w around the APIError-shaped
	// leaf).
	nacosErr := fmt.Errorf("nacos: deregister 10.0.0.1#8080/k8s: %w", permanentTestError{})
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: nacosErr}
	fanout, err := NewFanoutSink(nil,
		NamedSink{Name: stubSinkAtlas, Sink: atlas},
		NamedSink{Name: stubSinkNacos, Sink: nacos},
	)
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v", err)
	}

	fanoutErr := fanout.Push(123, []*instance.Instance{{InstanceId: "instance-7", Reversion: 42}})
	if fanoutErr == nil {
		t.Fatal("fanout Push() error = nil, want the aggregated failure")
	}

	// errors.As must find the leaf Permanent error directly through the
	// FanoutError...
	var direct interface{ Permanent() bool }
	if !errors.As(fanoutErr, &direct) || !direct.Permanent() {
		t.Fatalf("errors.As(FanoutError) did not find the permanent leaf, want it (FanoutError.Unwrap contract); err = %v", fanoutErr)
	}
	// ...and through a further %w-wrapped error — the shape any Handle-time
	// or adapter-side classifier actually inspects.
	wrapped := fmt.Errorf("push: %w", fanoutErr)
	var throughWrapper interface{ Permanent() bool }
	if !errors.As(wrapped, &throughWrapper) || !throughWrapper.Permanent() {
		t.Fatalf("errors.As(wrapped FanoutError) did not find the permanent leaf, want it; err = %v", wrapped)
	}
	// The FanoutError itself stays detectable through the same wrapper (the
	// §5.1 per-sink breakdown discipline is unaffected by the new Unwrap).
	var detected FanoutError
	if !errors.As(wrapped, &detected) || !reflect.DeepEqual(detected.FailedSinks(), []string{stubSinkNacos}) {
		t.Fatalf("errors.As(wrapped, &FanoutError) = %v, want the nacos-only breakdown", detected)
	}

	// The worker-level pin: UnsyncedService over the real FanoutSink, the
	// nacos member failing permanently. After one syncOnce cycle the nacos
	// key is dropped (permanent) while the atlas key — whose sink succeeds —
	// is deleted by its own success: the queue ends empty, and the failed
	// sink saw exactly its two pushes (the direct fanout attempt above plus
	// the one retry attempt), never a third.
	service := NewUnsyncedService(context.Background(), fanout, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	service.Add(123, []*instance.Instance{{InstanceId: "instance-7", Reversion: 42}}, nil)

	service.syncOnce()

	if got := service.Len(); got != 0 {
		t.Fatalf("queued events after the permanent fanout failure = %d, want 0 (nacos dropped, atlas drained)", got)
	}
	// The failed sink was attempted by the retry (the direct fanout push
	// above plus exactly one retry) and dropped; the healthy sink was
	// attempted by the same two fan-out rounds and never again.
	if got := len(nacos.PushCalls()); got != 2 {
		t.Fatalf("nacos push calls = %d, want 2 (fanout attempt + the single failed retry, then dropped)", got)
	}
	if got := len(atlas.PushCalls()); got != 2 {
		t.Fatalf("atlas push calls = %d, want 2 (fan-out attempt + its successful retry, then never again)", got)
	}
}

// TestBlackboxRetryQueueSemanticsDropsPermanentErrorThroughFanoutQueue: the
// fanout permanent-drop observed through the worker's Handle seam: the
// event's fan-out fails on the nacos member (atlas succeeds), the queue
// holds ONLY the nacos key, and the next cycle drops it — one failed
// attempt, no spin.
func TestBlackboxRetryQueueSemanticsDropsPermanentErrorThroughFanoutQueue(t *testing.T) {
	nacosErr := fmt.Errorf("nacos: deregister 10.0.0.1#8080/k8s: %w", permanentTestError{})
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: nacosErr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := NewResourceWorker(ctx, newTestFanout(t, atlas, nacos), &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	w.Handle(&Event{
		Trigger: 123,
		Data:    []*instance.Instance{{InstanceId: "instance-8", Reversion: 42}},
		Operate: OperateTypeSync,
	})

	// Only the failed sink is queued (the FanoutError breakdown), and after
	// one retry cycle the permanent nacos key is dropped rather than kept.
	if got, want := w.unsyncedService.Lens(), map[string]int{stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after failed fan-out = %v, want %v", got, want)
	}
	w.unsyncedService.syncOnce()
	if got := w.unsyncedService.Len(); got != 0 {
		t.Fatalf("queued events after the permanent retry = %d, want 0 (dropped, not spun)", got)
	}
	if got := len(nacos.PushCalls()); got != 2 {
		t.Fatalf("nacos push calls = %d, want 2 (fan-out attempt + the single failed retry, then dropped)", got)
	}
	if got := len(atlas.PushCalls()); got != 1 {
		t.Fatalf("atlas push calls = %d, want 1 (its fan-out push only)", got)
	}
}

// TestBlackboxRetryQueueSemanticsPermanentErrorKeepsMidCycleReAdd: the
// permanent-error drop uses the same delete-by-compare discipline as the
// success path: an entry re-added mid-cycle with a newer revision must
// survive the drop of the instance the cycle just failed to push. The
// re-add must land between syncOnce's snapshot and the push (the window the
// production retry loop really races in), so it is scripted inside the
// sink's Push itself: the fake sink is driven from the worker's own
// retryKeyed call stack.
func TestBlackboxRetryQueueSemanticsPermanentErrorKeepsMidCycleReAdd(t *testing.T) {
	readd := &instance.Instance{InstanceId: "instance-6", Reversion: 20}
	sink := &midCycleReAddSink{failErr: permanentError{}, reAdd: readd}
	service := NewUnsyncedService(context.Background(), sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	sink.addOwner = service

	stale := &instance.Instance{InstanceId: "instance-6", Reversion: 10}
	service.Add(1, []*instance.Instance{stale}, nil)

	service.syncOnce()

	if got := service.Len(); got != 1 {
		t.Fatalf("queued events after a permanent failure with mid-cycle re-add = %d, want 1 (the re-add survives)", got)
	}
	// The surviving entry must be the re-added newer revision: a further
	// cycle (now succeeding) pushes it and drains the queue.
	sink.setFailErr(nil)
	service.syncOnce()
	if got := service.Len(); got != 0 {
		t.Fatalf("queued events after the successful re-push = %d, want 0", got)
	}
	calls := sink.pushSnapshot()
	if len(calls) != 2 {
		t.Fatalf("push calls = %d, want 2 (the failed stale push and the successful re-add push)", len(calls))
	}
	repushed := calls[1].Instances[0]
	if repushed.InstanceId != "instance-6" || repushed.Reversion != 20 {
		t.Fatalf("re-pushed instance = %#v, want instance-6 with reversion 20 (the re-added revision)", repushed)
	}
}

// midCycleReAddSink pushes once and, from inside that Push, re-adds a newer
// revision to addOwner (the production Push runs outside the store lock, so
// the re-add races the push exactly like a real event does). The re-add runs
// only on the first push so later cycles are ordinary.
type midCycleReAddSink struct {
	mu        sync.Mutex
	failErr   error
	reAdd     *instance.Instance
	addOwner  *UnsyncedService
	pushCalls []blackboxPushCall
}

func (s *midCycleReAddSink) Push(triggerTime int64, instances []*instance.Instance) error {
	s.mu.Lock()
	s.pushCalls = append(s.pushCalls, blackboxPushCall{
		TriggerTime: triggerTime,
		Instances:   cloneInstances(instances),
	})
	err := s.failErr
	s.mu.Unlock()

	if err != nil {
		// Simulate the mid-cycle event: the newer revision arrives while the
		// cycle is pushing the stale snapshot. PushTo called us from
		// pushSinkOnce, outside the store lock.
		s.mu.Lock()
		reAdd := s.reAdd
		s.mu.Unlock()
		if reAdd != nil {
			s.setFailErr(nil)
			s.addOwner.Add(2, []*instance.Instance{reAdd}, nil)
		}
	}
	return err
}

func (s *midCycleReAddSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	return nil
}

func (s *midCycleReAddSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return &instance.InstanceList{}, nil
}

func (s *midCycleReAddSink) setFailErr(err error) {
	s.mu.Lock()
	s.failErr = err
	s.mu.Unlock()
}

func (s *midCycleReAddSink) pushSnapshot() []blackboxPushCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := make([]blackboxPushCall, len(s.pushCalls))
	for i, call := range s.pushCalls {
		snapshot[i] = blackboxPushCall{
			TriggerTime: call.TriggerTime,
			Instances:   cloneInstances(call.Instances),
		}
	}
	return snapshot
}
