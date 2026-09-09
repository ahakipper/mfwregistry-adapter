package worker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/fakes"
)

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
	worker.Handle(&Event{Trigger: 456, Data: batch, Operate: OperateTypeSyncAll})

	// The batch reached the sink as one full push and failed.
	calls := sink.PushAllCalls()
	if len(calls) != 1 || calls[0].TriggerTime != 456 || len(calls[0].Instances) != len(batch) {
		t.Fatalf("PushAll calls = %#v, want the one delegated full-push event", calls)
	}
	// The whole batch is queued under the plain sink name (the single-sink
	// wiring: a non-fanout error queues every known sink).
	if got := worker.unsyncedService.Len(); got != len(batch) {
		t.Fatalf("queued events after failed SyncAll = %d, want %d (the whole batch)", got, len(batch))
	}
	if got, want := worker.unsyncedService.Lens(), map[string]int{plainSinkName: len(batch)}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Lens after failed SyncAll = %v, want %v", got, want)
	}

	// The retry actually re-pushes the batch: one retry cycle drains the
	// queue through Push (the plain-sink retry path) once the sink recovers.
	sink.SetErrors(nil, nil, nil)
	worker.unsyncedService.syncOnce()

	if got := worker.unsyncedService.Len(); got != 0 {
		t.Fatalf("queued events after the retry = %d, want 0 (the recovered batch drained)", got)
	}
	retries := sink.PushCalls()
	if len(retries) != len(batch) {
		t.Fatalf("retry Push calls = %d, want %d (one per queued instance)", len(retries), len(batch))
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
