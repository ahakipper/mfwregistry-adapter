package worker

// Per-sink retry tests (docs/nacos-sink-plan.md §5.4) driving the real
// FanoutSink of F4 — the F3→F4 contract: the scenarios were developed
// against a hand-rolled two-sink stub and now run unchanged against the
// real type. The contract tests below the scenarios pin the §5.1 error
// discipline: only fan-outs construct FanoutError, consumers detect it with
// errors.As, never a type assertion. The nine §6.4 FanoutSink tests sit
// between the helpers and the scenarios.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
)

const (
	stubSinkAtlas = "atlas"
	stubSinkNacos = "nacos"
)

// legacyErrorFanout wraps a real FanoutSink and returns a fixed plain error
// from Push/PushAll instead of aggregating: the one error surface the real
// fanout never produces, which the worker's conservative all-sinks fallback
// exists for (plan §5.2). Embedding keeps the stub to exactly the overridden
// behavior — PushTo, Sinks and GetAll stay the real fanout's — so it exists
// solely for TestWorkerLegacyErrorQueuesAllSinks.
type legacyErrorFanout struct {
	*FanoutSink
	plainErr error
}

func (f *legacyErrorFanout) Push(int64, []*instance.Instance) error { return f.plainErr }

func (f *legacyErrorFanout) PushAll(int64, []*instance.Instance) error { return f.plainErr }

// slowSink delays its first Push by delay after signaling entry on entered
// (buffered, signaled once). It bounds how long a retry-cycle push stays in
// flight without deadlocking the test if the store lock is wrongly held
// across pushes: a blocked Add would wait out the delay and fail the timing
// assertion instead of hanging.
type slowSink struct {
	inner   ports.InstanceSink
	delay   time.Duration
	entered chan struct{}
	once    sync.Once
}

func (s *slowSink) Push(triggerTime int64, instances []*instance.Instance) error {
	s.once.Do(func() {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		time.Sleep(s.delay)
	})
	return s.inner.Push(triggerTime, instances)
}

func (s *slowSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	return s.inner.PushAll(triggerTime, instances)
}

func (s *slowSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return s.inner.GetAll(statuses, provider)
}

// gatedSink gates its first Push: it signals entry on entered (buffered,
// once) and then blocks until release is closed, giving the test exact
// control over when a retry cycle is mid-push.
type gatedSink struct {
	inner   ports.InstanceSink
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedSink) Push(triggerTime int64, instances []*instance.Instance) error {
	g.once.Do(func() {
		select {
		case g.entered <- struct{}{}:
		default:
		}
		<-g.release
	})
	return g.inner.Push(triggerTime, instances)
}

func (g *gatedSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	return g.inner.PushAll(triggerTime, instances)
}

func (g *gatedSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return g.inner.GetAll(statuses, provider)
}

// waitForSignal receives from ch or fails the test after timeout.
func waitForSignal(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// --- F4: FanoutSink contract (plan §6.4) -----------------------------------
//
// The nine tests of the plan's F4 table. They exercise the real FanoutSink
// directly; the §5.4 scenarios below then run through it unchanged (the
// F3→F4 contract).

// newTestFanout builds the canonical two-sink fanout of the plan's tests:
// atlas first (the primary), nacos second.
func newTestFanout(t *testing.T, atlas, nacos ports.InstanceSink) *FanoutSink {
	t.Helper()
	fanout, err := NewFanoutSink(nil,
		NamedSink{Name: stubSinkAtlas, Sink: atlas},
		NamedSink{Name: stubSinkNacos, Sink: nacos},
	)
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v, want nil", err)
	}
	return fanout
}

// orderRecordingSink records each call into a shared, mutex-guarded log
// under its name, so one sequence spans both sinks — the fan-out order
// assertion needs exactly that.
type orderRecordingSink struct {
	name string
	mu   *sync.Mutex
	log  *[]string
}

func (s *orderRecordingSink) Push(triggerTime int64, instances []*instance.Instance) error {
	s.record("push:" + s.name)
	return nil
}

func (s *orderRecordingSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	s.record("pushAll:" + s.name)
	return nil
}

func (s *orderRecordingSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	s.record("getAll:" + s.name)
	return nil, nil
}

func (s *orderRecordingSink) record(event string) {
	s.mu.Lock()
	*s.log = append(*s.log, event)
	s.mu.Unlock()
}

// TestNewFanoutSinkRejectsEmptyAndDuplicateNames: the constructor errors
// on a malformed sink set — no sinks, an empty name, a nil sink, a
// duplicate name, and the metrics-reserved name "__total__". Every later
// lookup (PushTo, the primary view, the retry fallback, the depth metrics
// labels) is by name, so the name set must be a well-formed identity of
// the sink set (plan §6.2); "__total__" is the retry queue's depth-total
// label, so a sink claiming it would collide with that series.
func TestNewFanoutSinkRejectsEmptyAndDuplicateNames(t *testing.T) {
	sink := &fakes.FakeInstanceSink{}

	cases := []struct {
		name  string
		sinks []NamedSink
	}{
		{"no sinks", nil},
		{"empty name", []NamedSink{{Name: "", Sink: sink}}},
		{"nil sink", []NamedSink{{Name: stubSinkAtlas, Sink: nil}}},
		{"reserved total-metrics name", []NamedSink{{Name: totalSinkName, Sink: sink}}},
		{"duplicate names", []NamedSink{
			{Name: stubSinkAtlas, Sink: sink},
			{Name: stubSinkAtlas, Sink: &fakes.FakeInstanceSink{}},
		}},
	}
	for _, tc := range cases {
		fanout, err := NewFanoutSink(nil, tc.sinks...)
		if err == nil {
			t.Fatalf("NewFanoutSink(%s) error = nil, want non-nil", tc.name)
		}
		if fanout != nil {
			t.Fatalf("NewFanoutSink(%s) fanout = %#v, want nil", tc.name, fanout)
		}
	}
	if _, err := NewFanoutSink(nil, NamedSink{Name: totalSinkName, Sink: sink}); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("NewFanoutSink(__total__) error = %v, want the reserved-name error", err)
	}

	// The well-formed set still constructs.
	if fanout, err := NewFanoutSink(nil,
		NamedSink{Name: stubSinkAtlas, Sink: sink},
		NamedSink{Name: stubSinkNacos, Sink: &fakes.FakeInstanceSink{}},
	); err != nil || fanout == nil {
		t.Fatalf("NewFanoutSink(valid) = (%#v, %v), want a non-nil fanout and no error", fanout, err)
	}
}

// TestFanoutPushAllSinksInOrder: Push and PushAll reach the sinks in
// declaration order — atlas (the primary) first, then nacos.
func TestFanoutPushAllSinksInOrder(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	atlas := &orderRecordingSink{name: stubSinkAtlas, mu: &mu, log: &calls}
	nacos := &orderRecordingSink{name: stubSinkNacos, mu: &mu, log: &calls}
	fanout := newTestFanout(t, atlas, nacos)

	_ = fanout.Push(1, nil)
	_ = fanout.PushAll(1, nil)

	want := []string{
		"push:" + stubSinkAtlas, "push:" + stubSinkNacos,
		"pushAll:" + stubSinkAtlas, "pushAll:" + stubSinkNacos,
	}
	if got := calls; !reflect.DeepEqual(got, want) {
		t.Fatalf("sink call order = %v, want %v (declaration order, sequential)", got, want)
	}
}

// TestFanoutPushAggregatesPerSinkErrors: atlas ok, nacos failed → a
// FanoutError with exactly one failure naming nacos; FailedSinks()==["nacos"].
func TestFanoutPushAggregatesPerSinkErrors(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacosErr := errors.New("nacos down")
	nacos := &fakes.FakeInstanceSink{PushErr: nacosErr}
	fanout := newTestFanout(t, atlas, nacos)

	err := fanout.Push(123, []*instance.Instance{{InstanceId: "instance-1"}})

	var fanoutErr FanoutError
	if !errors.As(err, &fanoutErr) {
		t.Fatalf("Push() error = %v, want a FanoutError detected by errors.As", err)
	}
	if len(fanoutErr) != 1 {
		t.Fatalf("FanoutError failures = %d, want exactly one (atlas succeeded)", len(fanoutErr))
	}
	if fanoutErr[0].Sink != stubSinkNacos || fanoutErr[0].Err != nacosErr {
		t.Fatalf("failure = %#v, want {sink: %q, err: the nacos error}", fanoutErr[0], stubSinkNacos)
	}
	if got, want := fanoutErr.FailedSinks(), []string{stubSinkNacos}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FailedSinks() = %v, want %v", got, want)
	}
}

// TestFanoutPushAllSuccessReturnsNil: both sinks succeed → nil error, and
// both sinks received the push.
func TestFanoutPushAllSuccessReturnsNil(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{}
	fanout := newTestFanout(t, atlas, nacos)
	instances := []*instance.Instance{{InstanceId: "instance-1"}}

	if err := fanout.PushAll(123, instances); err != nil {
		t.Fatalf("PushAll() error = %v, want nil when every sink succeeds", err)
	}
	atlasCalls := atlas.PushAllCalls()
	if len(atlasCalls) != 1 || atlasCalls[0].TriggerTime != 123 || len(atlasCalls[0].Instances) != 1 {
		t.Fatalf("atlas PushAll calls = %#v, want one call with the event payload", atlasCalls)
	}
	nacosCalls := nacos.PushAllCalls()
	if len(nacosCalls) != 1 || nacosCalls[0].TriggerTime != 123 || len(nacosCalls[0].Instances) != 1 {
		t.Fatalf("nacos PushAll calls = %#v, want one call with the event payload", nacosCalls)
	}
}

// TestFanoutPushAllFanoutsAllDespiteFirstFailure: atlas fails → nacos still
// receives its push (no short-circuit, plan §6.2).
func TestFanoutPushAllFanoutsAllDespiteFirstFailure(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{PushAllErr: errors.New("atlas down")}
	nacos := &fakes.FakeInstanceSink{}
	fanout := newTestFanout(t, atlas, nacos)
	instances := []*instance.Instance{{InstanceId: "instance-1"}}

	err := fanout.PushAll(123, instances)

	var fanoutErr FanoutError
	if !errors.As(err, &fanoutErr) || len(fanoutErr) != 1 || fanoutErr[0].Sink != stubSinkAtlas {
		t.Fatalf("PushAll() error = %#v, want a FanoutError with exactly atlas's failure", err)
	}
	if got := len(nacos.PushAllCalls()); got != 1 {
		t.Fatalf("nacos PushAll calls after atlas failure = %d, want 1 (no short-circuit)", got)
	}
}

// TestFanoutGetAllReturnsPrimaryView: GetAll returns the first (primary)
// sink's list verbatim, forwarded with the original arguments; the
// secondary's GetAll is never called (plan §6.3, v1 semantics).
func TestFanoutGetAllReturnsPrimaryView(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	atlas.SetRemoteList(&instance.InstanceList{Instance: []*instance.Instance{{InstanceId: "atlas-view"}}})
	nacos := &fakes.FakeInstanceSink{}
	nacos.SetRemoteList(&instance.InstanceList{Instance: []*instance.Instance{{InstanceId: "nacos-view"}}})
	fanout := newTestFanout(t, atlas, nacos)

	list, err := fanout.GetAll([]int32{1, 2}, "k8s")

	if err != nil {
		t.Fatalf("GetAll() error = %v, want nil", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "atlas-view" {
		t.Fatalf("GetAll() list = %#v, want the primary's view verbatim", list)
	}
	atlasCalls := atlas.GetAllCalls()
	if len(atlasCalls) != 1 || !reflect.DeepEqual(atlasCalls[0].Statuses, []int32{1, 2}) || atlasCalls[0].Provider != "k8s" {
		t.Fatalf("primary GetAll calls = %#v, want the query forwarded verbatim", atlasCalls)
	}
	if got := len(nacos.GetAllCalls()); got != 0 {
		t.Fatalf("secondary GetAll calls = %d, want 0 (primary view only in v1)", got)
	}
}

// TestFanoutGetAllPrimaryErrorPropagates: a primary GetAll error surfaces
// unchanged — no wrapping, no fallback to the secondary (plan §6.3).
func TestFanoutGetAllPrimaryErrorPropagates(t *testing.T) {
	getAllErr := errors.New("atlas query failed")
	atlas := &fakes.FakeInstanceSink{GetAllErr: getAllErr}
	nacos := &fakes.FakeInstanceSink{}
	nacos.SetRemoteList(&instance.InstanceList{Instance: []*instance.Instance{{InstanceId: "nacos-view"}}})
	fanout := newTestFanout(t, atlas, nacos)

	list, err := fanout.GetAll([]int32{1}, "ecs")

	if err != getAllErr {
		t.Fatalf("GetAll() error = %v, want the primary's error unchanged", err)
	}
	if list != nil {
		t.Fatalf("GetAll() list = %#v, want nil alongside the error", list)
	}
	if got := len(nacos.GetAllCalls()); got != 0 {
		t.Fatalf("secondary GetAll calls = %d, want 0 (a primary error does not fall back)", got)
	}
}

// TestFanoutPushToTargetsSingleSink: PushTo reaches exactly the named sink
// with the original arguments; an unknown name errors (plan §6.4).
func TestFanoutPushToTargetsSingleSink(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{}
	fanout := newTestFanout(t, atlas, nacos)
	instances := []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}}

	if err := fanout.PushTo(stubSinkNacos, 123, instances); err != nil {
		t.Fatalf("PushTo(nacos) error = %v, want nil", err)
	}
	nacosCalls := nacos.PushCalls()
	if len(nacosCalls) != 1 || nacosCalls[0].TriggerTime != 123 ||
		len(nacosCalls[0].Instances) != 1 || nacosCalls[0].Instances[0].InstanceId != "instance-1" {
		t.Fatalf("nacos Push calls = %#v, want exactly the targeted push", nacosCalls)
	}
	if got := len(atlas.PushCalls()); got != 0 {
		t.Fatalf("atlas Push calls = %d, want 0 (PushTo targets one sink)", got)
	}

	if err := fanout.PushTo("bogus", 123, instances); err == nil {
		t.Fatal("PushTo(unknown sink) error = nil, want an error")
	}
}

// TestFanoutSingleSinkDegeneratesToPlainBehavior: one sink — the shipped
// configuration until F5 wires Nacos (§6.5) — behaves exactly like the
// pre-fanout worker: the same pushes, the same retry queue keys and drain,
// and an error surface that degenerates to nil-or-one (§6.5). The
// side-by-side worker comparison is the regression net for §6.5.
func TestFanoutSingleSinkDegeneratesToPlainBehavior(t *testing.T) {
	// Error surface: success → nil; failure → a FanoutError holding
	// exactly the one sink's failure.
	oneSink, err := NewFanoutSink(nil, NamedSink{Name: stubSinkAtlas, Sink: &fakes.FakeInstanceSink{}})
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v, want nil", err)
	}
	if err := oneSink.Push(1, nil); err != nil {
		t.Fatalf("Push() on success = %v, want nil", err)
	}

	pushErr := errors.New("push failed")
	oneSink, err = NewFanoutSink(nil, NamedSink{Name: stubSinkAtlas, Sink: &fakes.FakeInstanceSink{PushErr: pushErr}})
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v, want nil", err)
	}
	err = oneSink.Push(1, nil)
	var fanoutErr FanoutError
	if !errors.As(err, &fanoutErr) || len(fanoutErr) != 1 || fanoutErr[0].Sink != stubSinkAtlas || fanoutErr[0].Err != pushErr {
		t.Fatalf("Push() on failure = %#v, want a FanoutError with exactly atlas's failure", err)
	}
	if got, want := fanoutErr.FailedSinks(), []string{stubSinkAtlas}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FailedSinks() = %v, want %v (nil-or-one degeneration of §6.5)", got, want)
	}

	// Worker behavior, side by side: a worker over the one-sink fanout and
	// a worker over the plain sink observe a failed push identically —
	// same push count, same queued (instance, "atlas") key, same retry
	// drain through the identical second push.
	plainSink := &fakes.FakeInstanceSink{PushErr: pushErr}
	fanoutSink := &fakes.FakeInstanceSink{PushErr: pushErr}
	fanout, err := NewFanoutSink(nil, NamedSink{Name: stubSinkAtlas, Sink: fanoutSink})
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v, want nil", err)
	}

	newWorker := func(pusher ports.InstanceSink) *DefaultWorker {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		w, err := NewResourceWorker(ctx, pusher, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
		if err != nil {
			t.Fatalf("NewResourceWorker() error = %v, want nil", err)
		}
		return w
	}
	plainWorker := newWorker(plainSink)
	fanoutWorker := newWorker(fanout)
	event := &Event{
		Trigger: 123,
		Data:    []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		Operate: OperateTypeSync,
	}

	plainWorker.Handle(event)
	fanoutWorker.Handle(event)

	for name, w := range map[string]*DefaultWorker{"plain": plainWorker, "fanout": fanoutWorker} {
		if got := w.unsyncedService.Len(); got != 1 {
			t.Fatalf("%s worker queued keys = %d, want 1", name, got)
		}
		if got, want := w.unsyncedService.Lens(), map[string]int{stubSinkAtlas: 1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("%s worker Lens = %v, want %v (the same atlas retry key both ways)", name, got, want)
		}
	}
	if got := len(plainSink.PushCalls()); got != 1 {
		t.Fatalf("plain sink push calls = %d, want 1", got)
	}
	if got := len(fanoutSink.PushCalls()); got != 1 {
		t.Fatalf("fanout-wrapped sink push calls = %d, want 1", got)
	}

	// One retry cycle: both workers drain their single atlas key through
	// an identical second push (PushTo for the fanout, Push for the plain
	// path — same call on the sink).
	plainSink.SetErrors(nil, nil, nil)
	fanoutSink.SetErrors(nil, nil, nil)
	plainWorker.unsyncedService.syncOnce()
	fanoutWorker.unsyncedService.syncOnce()

	for name, w := range map[string]*DefaultWorker{"plain": plainWorker, "fanout": fanoutWorker} {
		if got := w.unsyncedService.Len(); got != 0 {
			t.Fatalf("%s worker queued keys after retry = %d, want 0 (drained)", name, got)
		}
	}
	if got := len(plainSink.PushCalls()); got != 2 {
		t.Fatalf("plain sink push calls after retry = %d, want 2 (original + retry)", got)
	}
	if got := len(fanoutSink.PushCalls()); got != 2 {
		t.Fatalf("fanout-wrapped sink push calls after retry = %d, want 2 (original + retry)", got)
	}
}

// TestUnsyncedAddRecordsPerSinkKeys: Add one instance with 2 failed sinks
// creates one entry per (instance, sink) — Len counts every key, Lens counts
// each sink (plan §5.4).
func TestUnsyncedAddRecordsPerSinkKeys(t *testing.T) {
	service := NewUnsyncedService(context.Background(),
		newTestFanout(t, &fakes.FakeInstanceSink{}, &fakes.FakeInstanceSink{}),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())

	service.Add(123, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		[]string{stubSinkAtlas, stubSinkNacos})

	if got := service.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2 (one key per failed sink)", got)
	}
	if got, want := service.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens = %v, want %v", got, want)
	}
}

// TestRetryAtlasSuccessNacosFailureRetriesNacosOnly: atlas succeeds while
// nacos fails in one cycle — after the cycle only nacos's key remains, and
// atlas is never re-pushed (the per-sink independence of plan §3.2).
func TestRetryAtlasSuccessNacosFailureRetriesNacosOnly(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacos),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	service.Add(123, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		[]string{stubSinkAtlas, stubSinkNacos})

	service.syncOnce()

	if got, want := service.Lens(), map[string]int{stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after cycle 1 = %v, want only %v (atlas's success deletes only its key)", got, want)
	}
	if got := len(atlas.PushCalls()); got != 1 {
		t.Fatalf("atlas push calls after cycle 1 = %d, want 1", got)
	}
	if got := len(nacos.PushCalls()); got != 1 {
		t.Fatalf("nacos push calls after cycle 1 = %d, want 1", got)
	}

	// A second cycle retries nacos only; the succeeded atlas key must not
	// re-enter the queue (the pre-F3 any-success-deletes defect, inverted).
	service.syncOnce()

	if got, want := service.Lens(), map[string]int{stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after cycle 2 = %v, want %v", got, want)
	}
	if got := len(atlas.PushCalls()); got != 1 {
		t.Fatalf("atlas push calls after cycle 2 = %d, want 1 (no re-push after success)", got)
	}
	if got := len(nacos.PushCalls()); got != 2 {
		t.Fatalf("nacos push calls after cycle 2 = %d, want 2 (retried each cycle)", got)
	}
}

// TestRetryBothSinksFailBothRemain: both sinks fail — both keys remain
// queued and each sink is attempted exactly once per cycle.
func TestRetryBothSinksFailBothRemain(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{PushErr: errors.New("atlas down")}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacos),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		[]string{stubSinkAtlas, stubSinkNacos})

	service.syncOnce()

	if got, want := service.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after cycle 1 = %v, want %v (both failures remain queued)", got, want)
	}
	if got := len(atlas.PushCalls()); got != 1 {
		t.Fatalf("atlas push calls after cycle 1 = %d, want 1", got)
	}
	if got := len(nacos.PushCalls()); got != 1 {
		t.Fatalf("nacos push calls after cycle 1 = %d, want 1", got)
	}

	service.syncOnce()

	if got, want := service.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after cycle 2 = %v, want %v", got, want)
	}
	if got := len(atlas.PushCalls()); got != 2 {
		t.Fatalf("atlas push calls after cycle 2 = %d, want 2 (one attempt per cycle)", got)
	}
	if got := len(nacos.PushCalls()); got != 2 {
		t.Fatalf("nacos push calls after cycle 2 = %d, want 2 (one attempt per cycle)", got)
	}
}

// TestNacosRecoversLaterDrainsItsQueue: nacos fails on cycle 1 and succeeds
// on cycle 2 — its key is deleted and the queue is empty.
func TestNacosRecoversLaterDrainsItsQueue(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacos),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		[]string{stubSinkAtlas, stubSinkNacos})

	service.syncOnce() // atlas succeeds, nacos fails
	if got := len(nacos.PushCalls()); got != 1 {
		t.Fatalf("nacos push calls after cycle 1 = %d, want 1", got)
	}

	// Nacos recovers; the next cycle drains its queue alone.
	nacos.SetErrors(nil, nil, nil)
	service.syncOnce()

	if got := service.Len(); got != 0 {
		t.Fatalf("Len after nacos recovers = %d, want 0 (its queue drained)", got)
	}
	if got := len(nacos.PushCalls()); got != 2 {
		t.Fatalf("nacos push calls after cycle 2 = %d, want 2", got)
	}
}

// TestKeepHighestReversionPerSinkKey: the same instanceId re-added to nacos
// with a higher Reversion updates only nacos's entry; atlas's queued entry
// keeps its own revision (plan §5.2: keep-highest-Reversion per key).
func TestKeepHighestReversionPerSinkKey(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{}
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacos),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())

	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 10}},
		[]string{stubSinkAtlas, stubSinkNacos})
	service.Add(2, []*instance.Instance{{InstanceId: "instance-1", Reversion: 20}},
		[]string{stubSinkNacos})
	service.Add(3, []*instance.Instance{{InstanceId: "instance-1", Reversion: 5}},
		[]string{stubSinkNacos}) // stale revision, dropped

	if got, want := service.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens = %v, want %v", got, want)
	}

	// Drain both queues; the pushed payloads reveal which revision each
	// sink's entry kept: atlas 10 (untouched), nacos 20 (updated, 5 dropped).
	service.syncOnce()

	atlasCalls := atlas.PushCalls()
	if len(atlasCalls) != 1 || atlasCalls[0].Instances[0].Reversion != 10 {
		t.Fatalf("atlas pushed %#v, want one call with reversion 10 (untouched)", atlasCalls)
	}
	nacosCalls := nacos.PushCalls()
	if len(nacosCalls) != 1 || nacosCalls[0].Instances[0].Reversion != 20 {
		t.Fatalf("nacos pushed %#v, want one call with reversion 20 (highest wins, 5 dropped)", nacosCalls)
	}
	if got := service.Len(); got != 0 {
		t.Fatalf("Len after drain = %d, want 0", got)
	}
}

// TestWorkerQueuesOnlyFailedSinks: worker.Handle(Sync) whose fan-out push
// returns a FanoutError (atlas ok, nacos failed) queues only the
// (instance, "nacos") key.
func TestWorkerQueuesOnlyFailedSinks(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := NewResourceWorker(ctx, newTestFanout(t, atlas, nacos), &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	w.Handle(&Event{
		Trigger: 123,
		Data:    []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		Operate: OperateTypeSync,
	})

	if got, want := w.unsyncedService.Lens(), map[string]int{stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens = %v, want %v (only the failed sink is queued)", got, want)
	}
	if got := w.unsyncedService.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	if got := len(atlas.PushCalls()); got != 1 {
		t.Fatalf("atlas push calls = %d, want 1 (the fan-out pushed it)", got)
	}
	if got := len(nacos.PushCalls()); got != 1 {
		t.Fatalf("nacos push calls = %d, want 1 (the fan-out attempted it)", got)
	}
}

// TestWorkerLegacyErrorQueuesAllSinks: a non-FanoutError from the push
// queues entries for every known sink — the conservative, backward-
// compatible fallback of plan §5.2. Both sinks keep failing so the assertion
// stays stable even if the 5s retry ticker fires mid-test.
func TestWorkerLegacyErrorQueuesAllSinks(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{PushErr: errors.New("atlas down")}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	// The real fanout never emits a plain error, so the stub exists for
	// exactly this scenario (see legacyErrorFanout).
	stub := &legacyErrorFanout{
		FanoutSink: newTestFanout(t, atlas, nacos),
		plainErr:   errors.New("legacy plain failure"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := NewResourceWorker(ctx, stub, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	w.Handle(&Event{
		Trigger: 123,
		Data:    []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		Operate: OperateTypeSync,
	})

	if got, want := w.unsyncedService.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens = %v, want %v (fallback queues every known sink)", got, want)
	}
}

// TestSyncOnceRecordsPerSinkQueueDepth: the recorder observes the queue
// depth with both sink labels, in registration order, on every cycle —
// atlas drains to 0 while nacos stays queued (the per-sink observability of
// plan §5.3) — plus the AUDIT-A-3 __total__ observation after each cycle's
// per-sink series (the whole queue, ghost-sink keys included).
func TestSyncOnceRecordsPerSinkQueueDepth(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	metrics := fakes.NewFakeMetricsRecorder()
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacos),
		&fakes.FakeLogger{}, metrics)
	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		[]string{stubSinkAtlas, stubSinkNacos})

	service.syncOnce() // depths recorded at cycle start: both 1; atlas then succeeds
	service.syncOnce() // atlas drained to 0, nacos still 1

	want := []fakes.QueueDepthObservation{
		{Sink: stubSinkAtlas, Depth: 1},
		{Sink: stubSinkNacos, Depth: 1},
		{Sink: totalSinkName, Depth: 2},
		{Sink: stubSinkAtlas, Depth: 0},
		{Sink: stubSinkNacos, Depth: 1},
		{Sink: totalSinkName, Depth: 1},
	}
	if got := metrics.QueueDepthObservations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("queue depth observations = %v, want %v (per sink in registration order, then the total)", got, want)
	}
}

// TestAddNotBlockedByInflightRetry: a sink whose PushTo blocks 500ms inside
// a retry cycle must not block a concurrent Add — the lock-discipline fix
// of plan §5.2 (snapshot under the lock, push outside it). The threshold
// (250ms) is half the block: a wrongly lock-held push would delay Add past
// the full 500ms.
func TestAddNotBlockedByInflightRetry(t *testing.T) {
	atlas := &slowSink{
		inner:   &fakes.FakeInstanceSink{},
		delay:   500 * time.Millisecond,
		entered: make(chan struct{}, 1),
	}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacos),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 1}},
		[]string{stubSinkAtlas, stubSinkNacos})

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.syncOnce()
	}()

	// Wait until the atlas push is in flight: the store lock must be free.
	waitForSignal(t, atlas.entered, "the in-flight atlas retry push")

	start := time.Now()
	service.Add(2, []*instance.Instance{{InstanceId: "instance-2", Reversion: 1}},
		[]string{stubSinkAtlas})
	elapsed := time.Since(start)

	if elapsed >= 250*time.Millisecond {
		t.Fatalf("Add blocked %v behind the in-flight retry push, want it to return well before the 500ms push", elapsed)
	}
	waitForSignal(t, done, "syncOnce to finish")

	// Cycle 1 outcome: instance-1 left atlas's queue (its slow push
	// succeeded), nacos kept its key, and instance-2 is queued for the next
	// cycle.
	if got, want := service.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after cycle = %v, want %v", got, want)
	}
}

// TestSyncOnceDeletesOnlySucceededKeysUnderContention: atlas succeeds while
// nacos fails in one cycle, and a re-Add for atlas lands mid-cycle — the
// re-added (newer) revision must survive the cycle's success
// (snapshot-then-delete compare, plan §5.2).
func TestSyncOnceDeletesOnlySucceededKeysUnderContention(t *testing.T) {
	atlasFake := &fakes.FakeInstanceSink{}
	nacosFake := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	atlas := &gatedSink{
		inner:   atlasFake,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	service := NewUnsyncedService(context.Background(), newTestFanout(t, atlas, nacosFake),
		&fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 1}},
		[]string{stubSinkAtlas, stubSinkNacos})

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.syncOnce()
	}()

	// The cycle's atlas push is mid-flight: re-add a newer revision for the
	// very key being pushed.
	waitForSignal(t, atlas.entered, "the gated atlas retry push")
	service.Add(2, []*instance.Instance{{InstanceId: "instance-1", Reversion: 2}},
		[]string{stubSinkAtlas})
	close(atlas.release)
	waitForSignal(t, done, "syncOnce to finish")

	// Nacos failed, so its key remains; atlas's push succeeded but the entry
	// was replaced mid-cycle — it must not be deleted.
	if got, want := service.Lens(), map[string]int{stubSinkAtlas: 1, stubSinkNacos: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after contended cycle = %v, want %v (the re-added atlas revision survives)", got, want)
	}

	// The surviving atlas entry is the re-added revision 2: with nacos
	// recovered, the next cycle drains both queues and atlas is pushed the
	// newer revision.
	nacosFake.SetErrors(nil, nil, nil)
	service.syncOnce()

	if got := service.Len(); got != 0 {
		t.Fatalf("Len after recovery cycle = %d, want 0", got)
	}
	atlasCalls := atlasFake.PushCalls()
	if len(atlasCalls) != 2 {
		t.Fatalf("atlas push calls = %d, want 2 (contended cycle + recovery cycle)", len(atlasCalls))
	}
	if got := atlasCalls[1].Instances[0].Reversion; got != 2 {
		t.Fatalf("re-pushed atlas reversion = %d, want 2 (the re-added revision was not lost)", got)
	}
}

// TestFanoutErrorAggregatesSinkPrefixedMessages: Error() joins the failures'
// messages with each sink's name prefixed.
func TestFanoutErrorAggregatesSinkPrefixedMessages(t *testing.T) {
	err := FanoutError{
		{Sink: "atlas", Err: errors.New("grpc boom")},
		{Sink: "nacos", Err: errors.New("http 500")},
	}

	msg := err.Error()
	for _, want := range []string{"atlas", "grpc boom", "nacos", "http 500"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Error() = %q, want it to contain %q", msg, want)
		}
	}
}

// TestFanoutErrorFailedSinksListsFailuresInOrder: FailedSinks names the
// failed sinks in failure order.
func TestFanoutErrorFailedSinksListsFailuresInOrder(t *testing.T) {
	err := FanoutError{
		{Sink: "nacos", Err: errors.New("down")},
		{Sink: "atlas", Err: errors.New("timeout")},
	}

	if got, want := err.FailedSinks(), []string{"nacos", "atlas"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FailedSinks() = %v, want %v", got, want)
	}
}

// TestFanoutErrorDetectedByErrorsAsThroughWrapping: the §5.1 discipline —
// consumers detect the aggregate with errors.As even through adapter-side
// wrapping, never a type assertion.
func TestFanoutErrorDetectedByErrorsAsThroughWrapping(t *testing.T) {
	inner := FanoutError{{Sink: "nacos", Err: errors.New("down")}}
	wrapped := fmt.Errorf("push failed: %w", inner)

	var fanoutErr FanoutError
	if !errors.As(wrapped, &fanoutErr) {
		t.Fatal("errors.As(wrapped FanoutError) = false, want true")
	}
	if got, want := fanoutErr.FailedSinks(), []string{"nacos"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("FailedSinks() through wrapping = %v, want %v", got, want)
	}
}

// TestFailedSinkNamesResolvesFanoutAndLegacyErrors: a FanoutError resolves
// to its named failures (also through wrapping); a plain error — and a
// degenerate empty FanoutError — fall back to every known sink.
func TestFailedSinkNamesResolvesFanoutAndLegacyErrors(t *testing.T) {
	all := []string{"atlas", "nacos"}
	fanoutErr := FanoutError{{Sink: "nacos", Err: errors.New("down")}}
	wrapped := fmt.Errorf("push failed: %w", fanoutErr)
	plain := errors.New("legacy failure")

	if got, want := failedSinkNames(fanoutErr, all), []string{"nacos"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("failedSinkNames(FanoutError) = %v, want %v", got, want)
	}
	if got, want := failedSinkNames(wrapped, all), []string{"nacos"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("failedSinkNames(wrapped FanoutError) = %v, want %v", got, want)
	}
	if got, want := failedSinkNames(plain, all), all; !reflect.DeepEqual(got, want) {
		t.Fatalf("failedSinkNames(plain error) = %v, want %v (all sinks)", got, want)
	}
	if got, want := failedSinkNames(FanoutError{}, all), all; !reflect.DeepEqual(got, want) {
		t.Fatalf("failedSinkNames(empty FanoutError) = %v, want %v (conservative fallback)", got, want)
	}
}

// -----------------------------------------------------------------------------
// The per-sink e2e recording decorator (dsca-2 §3 / §6 row 7)
// -----------------------------------------------------------------------------

// newRecordingFanout builds the canonical two-sink fanout with the metrics
// recorder the decorator observes on.
func newRecordingFanout(t *testing.T, atlas, nacos ports.InstanceSink, metrics ports.MetricsRecorder) *FanoutSink {
	t.Helper()
	fanout, err := NewFanoutSinkWithMetrics(nil, metrics,
		NamedSink{Name: stubSinkAtlas, Sink: atlas},
		NamedSink{Name: stubSinkNacos, Sink: nacos},
	)
	if err != nil {
		t.Fatalf("NewFanoutSinkWithMetrics() error = %v, want nil", err)
	}
	return fanout
}

// TestFanoutDecoratorObservesPerSinkE2E: one Push through the fan-out must
// produce one e2e observation PER SINK, each labeled with the sink's name,
// outcome "ok", and a duration measured from the (ns-epoch) trigger — the
// 500ms-old trigger below must observe ~500ms (not 0, not 1000ms: the
// pre-widening seconds truncation quantized exactly there). The sink label
// is the whole point: the sequential fan-out makes the two legs' latencies
// differ by the other's duration, so per-sink series are how DS-2-2 stays
// visible in production.
func TestFanoutDecoratorObservesPerSinkE2E(t *testing.T) {
	recorder := fakes.NewFakeMetricsRecorder()
	fanout := newRecordingFanout(t, &fakes.FakeInstanceSink{}, &fakes.FakeInstanceSink{}, recorder)

	trigger := time.Now().Add(-500 * time.Millisecond).UnixNano()
	ins := []*instance.Instance{{InstanceId: "pod-a"}}
	if err := fanout.Push(trigger, ins); err != nil {
		t.Fatalf("fanout.Push() error = %v, want nil", err)
	}

	obs := recorder.EventToStoreObservations()
	if len(obs) != 2 {
		t.Fatalf("e2e observations = %d, want 2 (one per sink); got %#v", len(obs), obs)
	}
	bySink := map[string]fakes.EventToStoreObservation{}
	for _, o := range obs {
		bySink[o.Sink] = o
	}
	for _, sink := range []string{stubSinkAtlas, stubSinkNacos} {
		o, ok := bySink[sink]
		if !ok {
			t.Fatalf("no e2e observation for sink %q; got %#v", sink, obs)
		}
		if o.Outcome != "ok" {
			t.Fatalf("sink %q outcome = %q, want %q", sink, o.Outcome, "ok")
		}
		// ~500ms with a generous margin for scheduler noise; the assertion
		// exists to catch the seconds-truncation regression (which would
		// observe ~0 or ~1000ms) and the unit mistake.
		if o.Duration < 400*time.Millisecond || o.Duration > 1500*time.Millisecond {
			t.Fatalf("sink %q e2e duration = %v, want ~500ms (the trigger is 500ms old)", sink, o.Duration)
		}
	}
}

// TestFanoutDecoratorObservesPushAllAndErrorOutcome: PushAll is covered
// with its own outcome derivation, and a failing sink labels its
// observation outcome="error" while the healthy sibling's stays "ok" —
// the label split that keeps 5s-ticker retry latencies out of the
// ok-series percentiles.
func TestFanoutDecoratorObservesPushAllAndErrorOutcome(t *testing.T) {
	recorder := fakes.NewFakeMetricsRecorder()
	failing := &fakes.FakeInstanceSink{}
	// Push fails, PushAll succeeds: the Push assertion below is the error
	// case; the PushAll call stays the success shape for both sinks.
	failing.SetErrors(errors.New("store down"), nil, nil)
	fanout := newRecordingFanout(t, failing, &fakes.FakeInstanceSink{}, recorder)

	trigger := time.Now().UnixNano()
	if err := fanout.Push(trigger, []*instance.Instance{{InstanceId: "pod-a"}}); err != nil {
		var fanoutErr FanoutError
		if !errors.As(err, &fanoutErr) || len(fanoutErr) != 1 {
			t.Fatalf("fanout.Push() error = %v, want a one-failure FanoutError", err)
		}
	}

	obs := recorder.EventToStoreObservations()
	if len(obs) != 2 {
		t.Fatalf("e2e observations = %d, want 2 (one per sink); got %#v", len(obs), obs)
	}
	bySink := map[string]fakes.EventToStoreObservation{}
	for _, o := range obs {
		bySink[o.Sink] = o
	}
	if o := bySink[stubSinkAtlas]; o.Outcome != "error" {
		t.Fatalf("atlas outcome = %q, want %q (the sink failed)", o.Outcome, "error")
	}
	if o := bySink[stubSinkNacos]; o.Outcome != "ok" {
		t.Fatalf("nacos outcome = %q, want %q (the sink succeeded)", o.Outcome, "ok")
	}

	// PushAll coverage: the same decorator observes the full-push path
	// (both sinks succeed here), each labeled with its own sink name.
	before := len(recorder.EventToStoreObservations())
	if err := fanout.PushAll(trigger, []*instance.Instance{{InstanceId: "pod-a"}}); err != nil {
		t.Fatalf("fanout.PushAll() error = %v, want nil (both sinks' PushAll succeed)", err)
	}
	obs = recorder.EventToStoreObservations()[before:]
	if len(obs) != 2 {
		t.Fatalf("PushAll e2e observations = %d, want 2 (one per sink); got %#v", len(obs), obs)
	}
	for _, o := range obs {
		if o.Outcome != "ok" {
			t.Fatalf("PushAll observation %q outcome = %q, want ok", o.Sink, o.Outcome)
		}
	}
}

// TestFanoutDecoratorCoversRetryPathThroughPushTo pins the retry coverage
// (dsca-2 §6 row 7): PushTo dispatches to the WRAPPED sink, so one retry
// push observes through the same decorator — one observation, sink label
// of the named sink, outcome by error. This is the structural coverage
// that makes the 5s-ticker retry path's latencies (measuring from the
// ORIGINAL pendingPush.Trigger) visible without a second seam.
func TestFanoutDecoratorCoversRetryPathThroughPushTo(t *testing.T) {
	recorder := fakes.NewFakeMetricsRecorder()
	fanout := newRecordingFanout(t, &fakes.FakeInstanceSink{}, &fakes.FakeInstanceSink{}, recorder)

	// A trigger 6s old: the retry shape (original trigger + the 5s tick
	// wait). Any later PushAll/Push observations of the same fan-out would
	// pollute the assertion, so PushTo is the only call made.
	trigger := time.Now().Add(-6 * time.Second).UnixNano()
	if err := fanout.PushTo(stubSinkNacos, trigger, []*instance.Instance{{InstanceId: "pod-retry"}}); err != nil {
		t.Fatalf("fanout.PushTo() error = %v, want nil", err)
	}

	obs := recorder.EventToStoreObservations()
	if len(obs) != 1 {
		t.Fatalf("e2e observations = %d, want exactly 1 (the PushTo retry leg); got %#v", len(obs), obs)
	}
	if obs[0].Sink != stubSinkNacos || obs[0].Outcome != "ok" {
		t.Fatalf("PushTo observation = %#v, want sink=%q outcome=%q", obs[0], stubSinkNacos, "ok")
	}
	if obs[0].Duration < 5*time.Second {
		t.Fatalf("PushTo e2e duration = %v, want >= 5s (the 6s-old original trigger)", obs[0].Duration)
	}
}

// TestFanoutDecoratorPassesTriggerThroughUnchanged pins the pass-through:
// the decorator must not mangle the trigger the inner sink receives (the
// retry queue's origin carry and every pass-through pin depends on it).
func TestFanoutDecoratorPassesTriggerThroughUnchanged(t *testing.T) {
	recorder := fakes.NewFakeMetricsRecorder()
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{}
	fanout := newRecordingFanout(t, atlas, nacos, recorder)

	trigger := time.Now().Add(-100 * time.Millisecond).UnixNano()
	ins := []*instance.Instance{{InstanceId: "pod-pass"}}
	if err := fanout.Push(trigger, ins); err != nil {
		t.Fatalf("fanout.Push() error = %v, want nil", err)
	}

	for _, calls := range [][]fakes.InstanceSinkCall{atlas.PushCalls(), nacos.PushCalls()} {
		if len(calls) != 1 {
			t.Fatalf("inner sink Push calls = %d, want 1", len(calls))
		}
		if calls[0].TriggerTime != trigger {
			t.Fatalf("inner sink trigger = %d, want %d (the decorator must pass it through)", calls[0].TriggerTime, trigger)
		}
	}
}

// TestFanoutSinkWithoutRecorderKeepsWorking pins the nil-recorder
// contract: NewFanoutSink (no metrics argument) substitutes the no-op
// recorder, so every existing call site keeps compiling and pushing —
// with zero observations produced.
func TestFanoutSinkWithoutRecorderKeepsWorking(t *testing.T) {
	fanout := newTestFanout(t, &fakes.FakeInstanceSink{}, &fakes.FakeInstanceSink{})
	// Prove the wrap is still in place (the decorator is installed
	// regardless of the recorder) by observing through a fanout built with
	// a real fake recorder instead — the no-recorder variant's assertion is
	// just "push works, nothing panics".
	if err := fanout.Push(time.Now().UnixNano(), []*instance.Instance{{InstanceId: "pod-nil"}}); err != nil {
		t.Fatalf("fanout.Push() without recorder error = %v, want nil", err)
	}
}
