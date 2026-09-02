package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"spotter/internal/testkit/fakes"
	v2 "spotter/pkg/beehive/service/v2"
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
	Instances   []*v2.Instance
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

func (s *blackboxScriptedSink) Push(triggerTime int64, instances []*v2.Instance) error {
	s.mu.Lock()
	s.pushCalls = append(s.pushCalls, blackboxPushCall{
		TriggerTime: triggerTime,
		Instances:   cloneV2Instances(instances),
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

func (s *blackboxScriptedSink) PushAll(triggerTime int64, instances []*v2.Instance) error {
	return nil
}

func (s *blackboxScriptedSink) GetAll(statuses []int32, provider string) (*v2.InstanceList, error) {
	return &v2.InstanceList{}, nil
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
			Instances:   cloneV2Instances(call.Instances),
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

func cloneV2Instances(instances []*v2.Instance) []*v2.Instance {
	if instances == nil {
		return nil
	}
	cloned := make([]*v2.Instance, len(instances))
	for i, item := range instances {
		if item == nil {
			continue
		}
		copied := *item
		cloned[i] = &copied
	}
	return cloned
}

// blackboxPusher adapts the scripted sink to the discoverycenter.Pusher
// contract expected by NewResourceWorker.
type blackboxPusher struct {
	sink *blackboxScriptedSink
}

func (p *blackboxPusher) Push(triggerTime int64, instances []*v2.Instance) error {
	return p.sink.Push(triggerTime, instances)
}

func (p *blackboxPusher) PushAll(triggerTime int64, instances []*v2.Instance) error {
	return p.sink.PushAll(triggerTime, instances)
}

func (p *blackboxPusher) GetAll(statuses []int32, provider string) (*v2.InstanceList, error) {
	return p.sink.GetAll(statuses, provider)
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

	w, err := NewResourceWorker(ctx, &blackboxPusher{sink: sink}, &fakes.FakeLogger{}, metrics)
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	// First push fails: the handler must enqueue the instance for retry.
	w.Handle(&Event{
		Trigger: 123,
		Data:    []*v2.Instance{{InstanceId: "instance-1", Reversion: 42}},
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
	for time.Now().Before(queuedDeadline) && (len(depths) == 0 || depths[0] <= 0) {
		time.Sleep(25 * time.Millisecond)
		depths = metrics.QueueDepths()
	}
	if len(depths) == 0 || depths[0] <= 0 {
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

	w, err := NewResourceWorker(ctx, &blackboxPusher{sink: sink}, &fakes.FakeLogger{}, metrics)
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	w.Handle(&Event{
		Trigger: 321,
		Data:    []*v2.Instance{{InstanceId: "instance-2", Reversion: 7}},
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
	service := NewUnsyncedService(context.Background(), &blackboxPusher{sink: sink}, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())

	service.Add(1, []*v2.Instance{{InstanceId: "instance-3", Reversion: 10}})
	service.Add(2, []*v2.Instance{{InstanceId: "instance-3", Reversion: 20}})
	service.Add(3, []*v2.Instance{{InstanceId: "instance-3", Reversion: 15}})

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

func lastDepth(depths []int) int {
	if len(depths) == 0 {
		return -1
	}
	return depths[len(depths)-1]
}
