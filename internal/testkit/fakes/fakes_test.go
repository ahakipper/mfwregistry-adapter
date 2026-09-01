package fakes

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"spotter/internal/domain/instance"
)

func TestFakeLoggerAndNotifierCaptureConcurrentCalls(t *testing.T) {
	logger := &FakeLogger{}
	notifier := &FakeNotifier{}

	const workers = 32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logger.Infof("entry-%d", i)
			notifier.Notify(fmt.Sprintf("title-%d", i), "content")
		}(i)
	}
	wg.Wait()

	entries := logger.Entries()
	if len(entries) != workers {
		t.Fatalf("logger captured %d entries, want %d", len(entries), workers)
	}
	if entries[0].Level != LevelInfo {
		t.Fatalf("entry level = %q, want %q", entries[0].Level, LevelInfo)
	}
	entries[0].Message = "mutated"
	if logger.Entries()[0].Message == "mutated" {
		t.Fatal("Entries returned internal storage")
	}

	notifications := notifier.Notifications()
	if len(notifications) != workers {
		t.Fatalf("notifier captured %d notifications, want %d", len(notifications), workers)
	}
	notifications[0].Content = "mutated"
	if notifier.Notifications()[0].Content == "mutated" {
		t.Fatal("Notifications returned internal storage")
	}
}

func TestFakeLoggerFormatsAllLevels(t *testing.T) {
	logger := &FakeLogger{}

	logger.Info("info", 1)
	logger.Infof("info-%d", 2)
	logger.Warn("warn", 3)
	logger.Warnf("warn-%d", 4)
	logger.Error("error", 5)
	logger.Errorf("error-%d", 6)

	want := []LogEntry{
		{Level: LevelInfo, Message: "info1"},
		{Level: LevelInfo, Message: "info-2"},
		{Level: LevelWarn, Message: "warn3"},
		{Level: LevelWarn, Message: "warn-4"},
		{Level: LevelError, Message: "error5"},
		{Level: LevelError, Message: "error-6"},
	}
	if got := logger.Entries(); !reflect.DeepEqual(got, want) {
		t.Fatalf("entries = %#v, want %#v", got, want)
	}
}

func TestFakeClockAdvanceReleasesOnlyDueWaiters(t *testing.T) {
	start := time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC)
	clock := NewFakeClock(start)
	first := clock.After(2 * time.Second)
	second := clock.After(5 * time.Second)

	clock.Advance(2 * time.Second)
	select {
	case got := <-first:
		if want := start.Add(2 * time.Second); !got.Equal(want) {
			t.Fatalf("first waiter received %v, want %v", got, want)
		}
	default:
		t.Fatal("first waiter was not released")
	}
	select {
	case got := <-second:
		t.Fatalf("second waiter released early at %v", got)
	default:
	}

	clock.Advance(3 * time.Second)
	if got := <-second; !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("second waiter received %v", got)
	}
	if got := clock.Now(); !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("Now = %v, want %v", got, start.Add(5*time.Second))
	}
}

func TestFakeClockNonPositiveAfterIsImmediatelyReady(t *testing.T) {
	start := time.Unix(100, 0)
	clock := NewFakeClock(start)

	select {
	case got := <-clock.After(0):
		if !got.Equal(start) {
			t.Fatalf("After(0) = %v, want %v", got, start)
		}
	default:
		t.Fatal("After(0) was not immediately ready")
	}
}

func TestFakeInstanceSinkRecordsDeepCopiesAndErrors(t *testing.T) {
	sink := &FakeInstanceSink{}
	pushErr := errors.New("push failed")
	sink.SetErrors(pushErr, nil, nil)
	original := sampleInstance("one", 1)

	if err := sink.Push(10, []*instance.Instance{original}); !errors.Is(err, pushErr) {
		t.Fatalf("Push error = %v, want %v", err, pushErr)
	}
	original.Label["key"] = "caller-mutated"
	original.Ports[0].Port = 9999

	calls := sink.PushCalls()
	if len(calls) != 1 {
		t.Fatalf("PushCalls length = %d, want 1", len(calls))
	}
	if calls[0].Instances[0].Label["key"] != "value" || calls[0].Instances[0].Ports[0].Port != 8080 {
		t.Fatalf("captured call was mutated: %#v", calls[0].Instances[0])
	}
	calls[0].Instances[0].Image["name"] = "returned-mutated"
	if sink.PushCalls()[0].Instances[0].Image["name"] != "image" {
		t.Fatal("PushCalls returned internal storage")
	}

	remote := sampleInstance("remote", 2)
	sink.SetRemoteList(&instance.InstanceList{Instance: []*instance.Instance{remote}})
	remote.Label["key"] = "caller-mutated"
	got, err := sink.GetAll([]int32{instance.InstanceStatusOnline}, instance.ProviderK8s)
	if err != nil {
		t.Fatalf("GetAll returned error: %v", err)
	}
	if got.Instance[0].Label["key"] != "value" {
		t.Fatal("SetRemoteList retained caller-owned data")
	}
	got.Instance[0].Label["key"] = "returned-mutated"
	again, _ := sink.GetAll(nil, "")
	if again.Instance[0].Label["key"] != "value" {
		t.Fatal("GetAll returned internal storage")
	}
}

func TestFakeInstanceSourceEmitsCopiesAndClosesIdempotently(t *testing.T) {
	source := NewFakeInstanceSource("k8s", 1)
	source.SetAll([]*instance.Instance{sampleInstance("all", 1)})
	watch := source.Watch(context.Background())
	original := sampleInstance("change", 2)

	source.Emit(original)
	original.Label["key"] = "caller-mutated"
	got := <-watch
	if got[0].Label["key"] != "value" {
		t.Fatal("Emit retained caller-owned data")
	}
	all := source.GetAll()
	if source.Name() != "k8s" || len(all) != 1 {
		t.Fatalf("source values not returned: name=%q all=%d", source.Name(), len(all))
	}
	all[0].Image["name"] = "returned-mutated"
	if source.GetAll()[0].Image["name"] != "image" {
		t.Fatal("GetAll returned internal storage")
	}

	source.Close()
	source.Close()
	if _, ok := <-watch; ok {
		t.Fatal("watch channel remains open after Close")
	}
}

func TestFakeLeaderElectorReportsWithoutBlockingCaller(t *testing.T) {
	elector := NewFakeLeaderElector(true, false)
	changes := make(chan bool, 3)
	elector.ElectWait(changes)
	elector.Emit(true)

	for i, want := range []bool{true, false, true} {
		select {
		case got := <-changes:
			if got != want {
				t.Fatalf("transition %d = %v, want %v", i, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for transition %d", i)
		}
	}
	elector.Stop()
	elector.Stop()
}

func TestFakeLeaderElectorStopWaitsForBlockedPump(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	for i := 0; i < 1000; i++ {
		elector := NewFakeLeaderElector(true)
		changes := make(chan bool)
		elector.ElectWait(changes)
		runtime.Gosched()

		elector.Stop()
		close(changes)
	}
}

func TestFakeMetricsRecorderCapturesConcurrentMetrics(t *testing.T) {
	recorder := NewFakeMetricsRecorder()

	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			recorder.ObserveSyncOnceDuration(time.Duration(i))
			recorder.ObserveSyncAllDuration("k8s", time.Duration(i))
			recorder.SetSyncErrorQueueDepth(i)
			recorder.MarkSyncOnce()
		}(i)
	}
	wg.Wait()

	if got := len(recorder.SyncOnceDurations()); got != workers {
		t.Fatalf("sync-once durations = %d, want %d", got, workers)
	}
	if got := len(recorder.SyncAllDurations("k8s")); got != workers {
		t.Fatalf("full-sync durations = %d, want %d", got, workers)
	}
	if got := len(recorder.QueueDepths()); got != workers {
		t.Fatalf("queue depths = %d, want %d", got, workers)
	}
	if got := recorder.SyncOnceCount(); got != workers {
		t.Fatalf("sync-once count = %d, want %d", got, workers)
	}
}

func TestFakeEventQueueKeepsHighestReversionAndDrainsSnapshot(t *testing.T) {
	queue := NewFakeEventQueue()
	queue.Add(10, []*instance.Instance{
		sampleInstance("one", 1),
		sampleInstance("two", 4),
	})
	queue.Add(20, []*instance.Instance{
		sampleInstance("one", 3),
		sampleInstance("two", 2),
	})

	if got := queue.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	events := queue.Drain()
	sort.Slice(events, func(i, j int) bool {
		return events[i].Data[0].InstanceId < events[j].Data[0].InstanceId
	})
	if events[0].Trigger != 20 || events[0].Data[0].Reversion != 3 {
		t.Fatalf("newer event = %#v, want trigger 20 reversion 3", events[0])
	}
	if events[1].Trigger != 10 || events[1].Data[0].Reversion != 4 {
		t.Fatalf("older event was incorrectly replaced: %#v", events[1])
	}
	if queue.Len() != 2 {
		t.Fatal("Drain removed queued events")
	}

	events[0].Data[0].Label["key"] = "returned-mutated"
	for _, event := range queue.Drain() {
		if event.Data[0].InstanceId == "one" && event.Data[0].Label["key"] != "value" {
			t.Fatal("Drain returned internal storage")
		}
	}
	queue.Remove("one")
	if queue.Len() != 1 {
		t.Fatalf("Len after Remove = %d, want 1", queue.Len())
	}
}

func sampleInstance(id string, reversion int64) *instance.Instance {
	return &instance.Instance{
		InstanceId: id,
		Reversion:  reversion,
		Provider:   instance.ProviderK8s,
		Status:     instance.InstanceStatusOnline,
		Ports:      []*instance.PortInfo{{Name: "http", Port: 8080}},
		Label:      map[string]string{"key": "value"},
		Image:      map[string]string{"name": "image"},
	}
}
