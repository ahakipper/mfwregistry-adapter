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

	if got, want := metrics.QueueDepths(), []fakes.QueueDepthObservation{{Sink: plainSinkName, Depth: 1}}; !reflect.DeepEqual(got, want) {
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
