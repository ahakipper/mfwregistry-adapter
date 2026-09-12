package worker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
)

type staleSequenceSink struct {
	mu    sync.Mutex
	calls []*instance.Instance
	n     int
}

func (s *staleSequenceSink) Push(int64, []*instance.Instance) error { return nil }
func (s *staleSequenceSink) PushAll(_ int64, items []*instance.Instance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(items) > 0 {
		copy := *items[0]
		s.calls = append(s.calls, &copy)
	}
	s.n++
	if s.n == 1 {
		return errStaleFullPush
	}
	return nil
}
func (s *staleSequenceSink) GetAll([]int32, string) (*instance.InstanceList, error) {
	return &instance.InstanceList{}, nil
}

func (s *staleSequenceSink) snapshots() []*instance.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*instance.Instance(nil), s.calls...)
}

func TestWorkerSyncAllStaleWithoutRevalidateIsDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &fakes.FakeInstanceSink{PushAllErr: errStaleFullPush}
	w, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}
	w.Handle(&Event{Trigger: 1, Data: []*instance.Instance{{InstanceId: "full", Reversion: 1}}, Operate: OperateTypeSyncAll})
	if got := w.unsyncedService.Len(); got != 0 {
		t.Fatalf("stale SyncAll queue depth = %d, want 0 (stale snapshot must be discarded)", got)
	}
}

func TestWorkerSyncAllStaleRevalidatesLatestSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &staleSequenceSink{}
	w, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}
	latest := []*instance.Instance{{InstanceId: "full", Reversion: 2}}
	stale := []*instance.Instance{{InstanceId: "full", Reversion: 1}}
	var revalidations int
	w.Handle(&Event{
		Trigger: 1,
		Data:    []*instance.Instance{{InstanceId: "full", Reversion: 1}},
		Operate: OperateTypeSyncAll,
		Revalidate: func() ([]*instance.Instance, bool) {
			revalidations++
			if revalidations == 1 {
				return stale, true
			}
			return latest, true
		},
	})
	if got := w.unsyncedService.Len(); got != 0 {
		t.Fatalf("queue depth after successful revalidated SyncAll = %d, want 0", got)
	}
	calls := sink.snapshots()
	if len(calls) != 2 || calls[0].Reversion != 1 || calls[1].Reversion != 2 {
		t.Fatalf("PushAll snapshots = %#v, want stale then latest revisions", calls)
	}
}

func TestWorkerSyncAllMixedStaleAndTransientQueuesTransientSinkOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fanout, err := NewFanoutSink(
		&fakes.FakeLogger{},
		NamedSink{Name: "atlas", Sink: &fakes.FakeInstanceSink{PushAllErr: errStaleFullPush}},
		NamedSink{Name: "nacos", Sink: &fakes.FakeInstanceSink{PushAllErr: errors.New("timeout")}},
	)
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v", err)
	}
	w, err := NewResourceWorker(ctx, fanout, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}
	mixedBatch := []*instance.Instance{{Provider: "k8s", InstanceId: "mixed", Reversion: 1}}
	w.Handle(&Event{Trigger: 1, Scope: "k8s", BatchID: FullBatchID("k8s", mixedBatch), Sequence: 1, Operate: OperateTypeSyncAll,
		Data: mixedBatch})
	w.unsyncedService.RLock()
	defer w.unsyncedService.RUnlock()
	if len(w.unsyncedService.store) != 1 {
		t.Fatalf("queued retry entries = %d, want 1", len(w.unsyncedService.store))
	}
	for key := range w.unsyncedService.store {
		if key.Sink != "nacos" {
			t.Fatalf("queued sink = %q, want nacos", key.Sink)
		}
	}
}

func TestNewResourceWorkerRejectsNilPusher(t *testing.T) {
	worker, err := NewResourceWorker(context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("NewResourceWorker() error = nil, want non-nil")
	}
	if worker != nil {
		t.Fatalf("NewResourceWorker() worker = %#v, want nil", worker)
	}
}

func TestWorkerFailedEmptySyncEventDoesNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &fakes.FakeInstanceSink{PushErr: errors.New("push failed")}
	logger := &fakes.FakeLogger{}
	worker, err := NewResourceWorker(ctx, sink, logger, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	worker.Handle(&Event{Trigger: 123, Operate: OperateTypeSync})

	if got := len(sink.PushCalls()); got != 1 {
		t.Fatalf("Push calls = %d, want 1", got)
	}
	if got := worker.unsyncedService.Len(); got != 0 {
		t.Fatalf("queued events = %d, want 0 for empty data", got)
	}
	if got := len(logger.Entries()); got != 1 {
		t.Fatalf("log entries = %d, want 1", got)
	}
}

func TestWorkerHandlersDelegateAndQueueFailedSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &fakes.FakeInstanceSink{PushErr: errors.New("push failed")}
	worker, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}
	item := &instance.Instance{InstanceId: "instance-1", Reversion: 42}

	worker.Handle(&Event{Trigger: 123, Data: []*instance.Instance{item}, Operate: OperateTypeSync})

	calls := sink.PushCalls()
	if len(calls) != 1 || calls[0].TriggerTime != 123 || len(calls[0].Instances) != 1 || calls[0].Instances[0].InstanceId != "instance-1" {
		t.Fatalf("Push calls = %#v, want delegated event", calls)
	}
	if got := worker.unsyncedService.Len(); got != 1 {
		t.Fatalf("queued events = %d, want 1", got)
	}
}

func TestUnsyncedServiceRecordsQueueDepthBeforeRetry(t *testing.T) {
	sink := &fakes.FakeInstanceSink{}
	metrics := fakes.NewFakeMetricsRecorder()
	service := NewUnsyncedService(context.Background(), sink, &fakes.FakeLogger{}, metrics)
	service.Add(123, []*instance.Instance{{InstanceId: "instance-1", Reversion: 42}}, nil)

	service.syncOnce()

	// The per-sink series plus the AUDIT-A-3 total observation: the __total__
	// label reports the whole queue (including ghost-sink keys the per-sink
	// series cannot see).
	if got, want := metrics.QueueDepths(), []fakes.QueueDepthObservation{
		{Sink: plainSinkName, Depth: 1},
		{Sink: totalSinkName, Depth: 1},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("queue depths = %v, want %v", got, want)
	}
	if got := service.Len(); got != 0 {
		t.Fatalf("queued events after successful retry = %d, want 0", got)
	}
}

func TestRetryKeysDoNotMergeSamePodNameAcrossClusters(t *testing.T) {
	items := []*instance.Instance{
		{InstanceId: "pod-a", SourceKey: "cluster-a/uid-a", SourceCluster: "cluster-a", Reversion: 1},
		{InstanceId: "pod-a", SourceKey: "cluster-b/uid-b", SourceCluster: "cluster-b", Reversion: 1},
	}

	plain := NewUnsyncedService(context.Background(), &fakes.FakeInstanceSink{PushErr: errors.New("retry")}, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	plain.Add(1, items, nil)
	if got := plain.Len(); got != 2 {
		t.Fatalf("plain retry queue length = %d, want 2 distinct source identities", got)
	}
	seen := map[string]bool{}
	for key, pending := range plain.store {
		seen[key.InstanceID] = true
		if pending == nil || pending.Instance == nil {
			t.Fatalf("plain retry key %v has nil pending instance", key)
		}
	}
	if !seen["cluster-a/uid-a"] || !seen["cluster-b/uid-b"] {
		t.Fatalf("plain retry keys = %v, want both source keys", seen)
	}

	fanout := newTestFanout(t, &fakes.FakeInstanceSink{}, &fakes.FakeInstanceSink{PushErr: errors.New("nacos retry")})
	perSink := NewUnsyncedService(context.Background(), fanout, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	perSink.Add(1, items, []string{stubSinkNacos})
	if got := perSink.Len(); got != 2 {
		t.Fatalf("fanout retry queue length = %d, want 2 distinct source identities", got)
	}
}

func TestWorkerPushAllAndGetAllDelegate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &fakes.FakeInstanceSink{}
	sink.SetRemoteList(&instance.InstanceList{Instance: []*instance.Instance{{InstanceId: "remote-1"}}})
	worker, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	worker.Handle(&Event{Trigger: 456, Data: []*instance.Instance{{InstanceId: "instance-all"}}, Operate: OperateTypeSyncAll})
	calls := sink.PushAllCalls()
	if len(calls) != 1 || calls[0].TriggerTime != 456 || calls[0].Instances[0].InstanceId != "instance-all" {
		t.Fatalf("PushAll calls = %#v, want delegated event", calls)
	}
	list, err := worker.GetAll([]int32{1, 2}, "ecs")
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "remote-1" {
		t.Fatalf("GetAll() list = %#v, want remote list", list)
	}
}

// TestWorkerSyncAllFailureQueuesBatchForRetry: the SyncAll FAILURE path —
// the full push batch that fails must be queued in the unsynced retry
// store, or a failed full-push batch would silently wait the next full-push
// interval (hours) instead of the 5s retry cadence. SyncAll is also the
// only healer of the Nacos prune (the F8 reconcile), so losing its retry
// queues drift healing too. This pins the queueing line of the SyncAll
// handler (worker.go: unsyncedService.Add) against removal — mutation M8's
// escape.
func TestWorkerSyncAllFailureQueuesBatchForRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &fakes.FakeInstanceSink{PushAllErr: errors.New("nacos: prune list pay-user/k8s: answered status 500")}
	worker, err := NewResourceWorker(ctx, sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}

	batch := []*instance.Instance{
		{InstanceId: "instance-a", Reversion: 1},
		{InstanceId: "instance-b", Reversion: 2},
		{InstanceId: "instance-c", Reversion: 3},
	}
	worker.Handle(&Event{Trigger: 456, Scope: "k8s", BatchID: FullBatchID("k8s", batch), Sequence: 456, Data: batch, Operate: OperateTypeSyncAll})

	// The batch reached the sink as one full push and failed.
	calls := sink.PushAllCalls()
	if len(calls) != 1 || calls[0].TriggerTime != 456 || len(calls[0].Instances) != len(batch) {
		t.Fatalf("PushAll calls = %#v, want the one delegated full-push event", calls)
	}
	// The whole batch is queued under the plain sink name (the single-sink
	// wiring: a non-fanout error queues every known sink).
	if got := worker.unsyncedService.Len(); got != 1 {
		t.Fatalf("queued operations after failed SyncAll = %d, want 1 full operation", got)
	}
	if got, want := worker.unsyncedService.Lens(), map[string]int{plainSinkName: 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after failed SyncAll = %v, want %v", got, want)
	}

	// The retry re-pushes the original batch as one full operation.
	sink.SetErrors(nil, nil, nil)
	worker.unsyncedService.syncOnce()

	if got := worker.unsyncedService.Len(); got != 0 {
		t.Fatalf("queued events after the retry = %d, want 0 (the recovered batch drained)", got)
	}
	retries := sink.PushAllCalls()
	if len(retries) != 2 || len(retries[1].Instances) != len(batch) {
		t.Fatalf("retry PushAll calls = %#v, want one complete batch", retries)
	}
	retried := map[string]bool{}
	for _, call := range retries {
		if call.TriggerTime != 456 {
			t.Fatalf("retry trigger time = %d, want the queued event trigger 456", call.TriggerTime)
		}
		for _, item := range call.Instances {
			retried[item.InstanceId] = true
		}
	}
	for _, item := range batch {
		if !retried[item.InstanceId] {
			t.Fatalf("instance %q was never re-pushed; retried = %v", item.InstanceId, retried)
		}
	}
}

func TestSyncAllPruneFailureRetainsSyncAllOperation(t *testing.T) {
	sink := &fakes.FakeInstanceSink{PushAllErr: errors.New("catalog prune 500")}
	w, err := NewResourceWorker(context.Background(), sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}
	batch := []*instance.Instance{{Provider: "k8s", InstanceId: "pod", Reversion: 1}}
	w.Handle(&Event{Trigger: 10, Scope: "k8s", BatchID: FullBatchID("k8s", batch), Sequence: 10, Operate: OperateTypeSyncAll, Data: batch})
	ops := w.unsyncedService.DrainOperations()
	if len(ops) != 1 || ops[0].Operate != ports.OperateTypeSyncAll || ops[0].Scope != "k8s" {
		t.Fatalf("retry operations = %#v, want one scoped SyncAll operation", ops)
	}
}

func TestSyncAllRetryCallsPushAllNotPush(t *testing.T) {
	sink := &fakes.FakeInstanceSink{PushAllErr: errors.New("catalog prune 500")}
	w, err := NewResourceWorker(context.Background(), sink, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewResourceWorker() error = %v", err)
	}
	batch := []*instance.Instance{{Provider: "k8s", InstanceId: "pod", Reversion: 1}}
	w.Handle(&Event{Trigger: 10, Scope: "k8s", BatchID: FullBatchID("k8s", batch), Sequence: 10, Operate: OperateTypeSyncAll, Data: batch})
	sink.SetErrors(nil, nil, nil)
	w.unsyncedService.syncOnce()
	if len(sink.PushAllCalls()) != 2 || len(sink.PushCalls()) != 0 {
		t.Fatalf("PushAll calls=%d Push calls=%d, want one retry as PushAll", len(sink.PushAllCalls()), len(sink.PushCalls()))
	}
}

func TestIncrementalAndFullPushQueuesDoNotOverwriteEachOther(t *testing.T) {
	s := NewUnsyncedService(context.Background(), &fakes.FakeInstanceSink{}, nil, nil)
	s.Add(1, []*instance.Instance{{Provider: "k8s", InstanceId: "pod", Reversion: 1}}, nil)
	s.AddFull(2, []*instance.Instance{{Provider: "k8s", InstanceId: "pod", Reversion: 2}}, nil)
	if got := s.Len(); got != 2 {
		t.Fatalf("mixed retry queue length = %d, want 2 operations", got)
	}
	ops := s.DrainOperations()
	seenPush, seenFull := false, false
	for _, op := range ops {
		seenPush = seenPush || op.Operate == ports.OperateTypeSync
		seenFull = seenFull || op.Operate == ports.OperateTypeSyncAll
	}
	if !seenPush || !seenFull {
		t.Fatalf("mixed operations = %#v, want both Push and PushAll", ops)
	}
}

func TestPrune5xxRetriesAndDeletesGhostAfterRecovery(t *testing.T) {
	sink := &fakes.FakeInstanceSink{}
	sink.SetScript(fakes.ScriptStep{Operation: fakes.SinkOperationPushAll, Mode: fakes.FailureRetryable5xx}, fakes.ScriptStep{Operation: fakes.SinkOperationPushAll, Mode: fakes.FailureSuccess})
	s := NewUnsyncedService(context.Background(), sink, nil, nil)
	s.AddFull(1, []*instance.Instance{{Provider: "k8s", InstanceId: "ghost", Reversion: 1}}, nil)
	s.syncOnce()
	if s.Len() != 1 {
		t.Fatalf("queue after prune 5xx = %d, want 1", s.Len())
	}
	s.syncOnce()
	if s.Len() != 0 {
		t.Fatalf("queue after prune recovery = %d, want 0", s.Len())
	}
}

func TestFullRetryRevalidateCalledBeforeReplay(t *testing.T) {
	sink := &fakes.FakeInstanceSink{}
	sink.SetScript(fakes.ScriptStep{Operation: fakes.SinkOperationPushAll, Mode: fakes.FailureRetryable5xx}, fakes.ScriptStep{Operation: fakes.SinkOperationPushAll, Mode: fakes.FailureSuccess})
	fanout, err := NewFanoutSink(&fakes.FakeLogger{}, NamedSink{Name: "nacos", Sink: sink})
	if err != nil {
		t.Fatalf("NewFanoutSink() error = %v", err)
	}
	var revalidations int
	batch := []*instance.Instance{{Provider: "k8s", InstanceId: "pod", Reversion: 1}}
	s := NewUnsyncedService(context.Background(), fanout, nil, nil)
	s.AddFullWithMeta(1, batch, []string{"nacos"}, "k8s", "batch-1", 1, func() ([]*instance.Instance, bool) {
		revalidations++
		return batch, true
	}, false)
	s.syncOnce()
	if s.Len() != 1 || revalidations != 1 {
		t.Fatalf("after first replay len=%d revalidations=%d, want 1/1", s.Len(), revalidations)
	}
	s.syncOnce()
	if s.Len() != 0 || revalidations != 2 {
		t.Fatalf("after recovery len=%d revalidations=%d, want 0/2", s.Len(), revalidations)
	}
}

func TestPrune4xxDropsOnlyPermanentTask(t *testing.T) {
	sink := &fakes.FakeInstanceSink{PushErr: errors.New("incremental timeout"), PushAllErr: &nacosPermanentTestError{}}
	s := NewUnsyncedService(context.Background(), sink, nil, nil)
	s.AddFull(1, []*instance.Instance{{Provider: "k8s", InstanceId: "ghost", Reversion: 1}}, nil)
	s.Add(2, []*instance.Instance{{Provider: "k8s", InstanceId: "live", Reversion: 1}}, nil)
	s.syncOnce()
	ops := s.DrainOperations()
	if len(ops) != 1 || ops[0].Operate != ports.OperateTypeSync {
		t.Fatalf("remaining operations = %#v, want only the incremental retry", ops)
	}
}

func TestAddFullWithMetaRejectsMissingScopeOrBatchID(t *testing.T) {
	s := NewUnsyncedService(context.Background(), &fakes.FakeInstanceSink{}, nil, nil)
	items := []*instance.Instance{{Provider: "k8s", InstanceId: "pod", Reversion: 1}}
	s.AddFullWithMeta(1, items, nil, "", "", 1, nil, false)
	s.AddFullWithMeta(1, items, nil, "k8s", "", 1, nil, false)
	s.AddFullWithMeta(1, items, nil, "", "batch", 1, nil, false)
	if got := s.Len(); got != 0 {
		t.Fatalf("malformed full retry entries = %d, want 0", got)
	}
}

func TestAddOperationAcceptsScopedEmptyFull(t *testing.T) {
	s := NewUnsyncedService(context.Background(), &fakes.FakeInstanceSink{}, nil, nil)
	s.AddOperation(ports.RetryOperation{Sink: plainSinkName, Operate: ports.OperateTypeSyncAll, Scope: "k8s", BatchID: "empty-batch", Sequence: 4, Trigger: 4})
	if got := s.Len(); got != 1 {
		t.Fatalf("scoped empty full entries = %d, want 1", got)
	}
	ops := s.DrainOperations()
	if len(ops) != 1 || ops[0].Operate != ports.OperateTypeSyncAll || ops[0].BatchID != "empty-batch" {
		t.Fatalf("scoped empty operation = %#v, want typed empty full", ops)
	}
}
