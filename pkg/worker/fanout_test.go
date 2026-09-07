package worker

// Phase F3 per-sink retry tests (docs/nacos-sink-plan.md §5.4), driven by
// two fakes.FakeInstanceSink instances behind a hand-rolled two-sink fanout
// stub. F4 replaces the stub with the real FanoutSink and these tests run
// unchanged — that is the F3→F4 contract. The contract tests below the
// scenarios pin the §5.1 error discipline: only fan-outs construct
// FanoutError, consumers detect it with errors.As, never a type assertion.

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

// twoSinkFanoutStub is the hand-rolled two-sink fanout of plan §5.4: two
// sinks behind the sinkFanout seam, aggregating per-sink errors into
// FanoutError exactly as F4's FanoutSink will (§6.2: sequential, atlas
// first, no short-circuit, nil only when every sink succeeds). plainErr,
// when set, is returned by Push/PushAll verbatim — the legacy non-fanout
// error path.
type twoSinkFanoutStub struct {
	atlas    ports.InstanceSink
	nacos    ports.InstanceSink
	plainErr error
}

func newTwoSinkFanoutStub(atlas, nacos ports.InstanceSink) *twoSinkFanoutStub {
	return &twoSinkFanoutStub{atlas: atlas, nacos: nacos}
}

func (f *twoSinkFanoutStub) Push(triggerTime int64, instances []*instance.Instance) error {
	if f.plainErr != nil {
		return f.plainErr
	}
	var failures FanoutError
	if err := f.atlas.Push(triggerTime, instances); err != nil {
		failures = append(failures, SinkFailure{Sink: stubSinkAtlas, Err: err})
	}
	if err := f.nacos.Push(triggerTime, instances); err != nil {
		failures = append(failures, SinkFailure{Sink: stubSinkNacos, Err: err})
	}
	if len(failures) > 0 {
		return failures
	}
	return nil
}

func (f *twoSinkFanoutStub) PushAll(triggerTime int64, instances []*instance.Instance) error {
	if f.plainErr != nil {
		return f.plainErr
	}
	var failures FanoutError
	if err := f.atlas.PushAll(triggerTime, instances); err != nil {
		failures = append(failures, SinkFailure{Sink: stubSinkAtlas, Err: err})
	}
	if err := f.nacos.PushAll(triggerTime, instances); err != nil {
		failures = append(failures, SinkFailure{Sink: stubSinkNacos, Err: err})
	}
	if len(failures) > 0 {
		return failures
	}
	return nil
}

func (f *twoSinkFanoutStub) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return f.atlas.GetAll(statuses, provider)
}

func (f *twoSinkFanoutStub) PushTo(sink string, triggerTime int64, instances []*instance.Instance) error {
	switch sink {
	case stubSinkAtlas:
		return f.atlas.Push(triggerTime, instances)
	case stubSinkNacos:
		return f.nacos.Push(triggerTime, instances)
	default:
		return fmt.Errorf("unknown sink %q", sink)
	}
}

func (f *twoSinkFanoutStub) Sinks() []string {
	return []string{stubSinkAtlas, stubSinkNacos}
}

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

// TestUnsyncedAddRecordsPerSinkKeys: Add one instance with 2 failed sinks
// creates one entry per (instance, sink) — Len counts every key, Lens counts
// each sink (plan §5.4).
func TestUnsyncedAddRecordsPerSinkKeys(t *testing.T) {
	service := NewUnsyncedService(context.Background(),
		newTwoSinkFanoutStub(&fakes.FakeInstanceSink{}, &fakes.FakeInstanceSink{}),
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
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacos),
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
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacos),
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
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacos),
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
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacos),
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
	w, err := NewResourceWorker(ctx, newTwoSinkFanoutStub(atlas, nacos), &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
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
	stub := newTwoSinkFanoutStub(atlas, nacos)
	stub.plainErr = errors.New("legacy plain failure")
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
// plan §5.3).
func TestSyncOnceRecordsPerSinkQueueDepth(t *testing.T) {
	atlas := &fakes.FakeInstanceSink{}
	nacos := &fakes.FakeInstanceSink{PushErr: errors.New("nacos down")}
	metrics := fakes.NewFakeMetricsRecorder()
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacos),
		&fakes.FakeLogger{}, metrics)
	service.Add(1, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}},
		[]string{stubSinkAtlas, stubSinkNacos})

	service.syncOnce() // depths recorded at cycle start: both 1; atlas then succeeds
	service.syncOnce() // atlas drained to 0, nacos still 1

	want := []fakes.QueueDepthObservation{
		{Sink: stubSinkAtlas, Depth: 1},
		{Sink: stubSinkNacos, Depth: 1},
		{Sink: stubSinkAtlas, Depth: 0},
		{Sink: stubSinkNacos, Depth: 1},
	}
	if got := metrics.QueueDepthObservations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("queue depth observations = %v, want %v (per sink, in registration order)", got, want)
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
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacos),
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
	service := NewUnsyncedService(context.Background(), newTwoSinkFanoutStub(atlas, nacosFake),
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
