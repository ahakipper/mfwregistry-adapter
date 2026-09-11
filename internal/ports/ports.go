package ports

import (
	"context"
	"time"

	"spotter/internal/domain/instance"
)

// Logger is the narrow logging port.
type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}

// NopLogger discards all log messages.
type NopLogger struct{}

func (NopLogger) Info(args ...interface{}) {}

func (NopLogger) Infof(format string, args ...interface{}) {}

func (NopLogger) Warn(args ...interface{}) {}

func (NopLogger) Warnf(format string, args ...interface{}) {}

func (NopLogger) Error(args ...interface{}) {}

func (NopLogger) Errorf(format string, args ...interface{}) {}

// Notifier sends a notification.
type Notifier interface {
	Notify(title, content string)
}

// Clock provides controllable time operations.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// InstanceSource provides instances from one provider.
type InstanceSource interface {
	Name() string
	Run(ctx context.Context) error
	Watch(ctx context.Context) <-chan []*instance.Instance
	GetAll() []*instance.Instance
}

// InstanceSink pushes instances to the discovery center.
type InstanceSink interface {
	Push(triggerTime int64, instances []*instance.Instance) error
	PushAll(triggerTime int64, instances []*instance.Instance) error
	GetAll(statuses []int32, provider string) (*instance.InstanceList, error)
}

// LeaderElector coordinates leadership transitions.
type LeaderElector interface {
	ElectWait(changes chan<- bool)
	Stop()
}

// EventQueue stores failed push events for retry.
type EventQueue interface {
	Add(triggerTime int64, instances []*instance.Instance)
	Len() int
	Drain() []*Event
}

// Event describes an instance push operation.
type Event struct {
	Trigger int64
	Data    []*instance.Instance
	Operate OperateType
}

// OperateType identifies the push operation.
type OperateType string

const (
	OperateTypeSync    OperateType = "Sync"
	OperateTypeSyncAll OperateType = "SyncAll"
)

// MetricsRecorder records synchronization metrics.
type MetricsRecorder interface {
	ObserveSyncOnceDuration(time.Duration)
	ObserveSyncAllDuration(provider string, d time.Duration)
	// SetSyncErrorQueueDepth records the current depth of one sink's sync
	// error queue. The sink label is the accepted breaking metrics change of
	// plan §5.3: one number cannot say which registry diverges once a
	// second sink exists, so the unlabeled series is replaced by
	// per-sink series.
	SetSyncErrorQueueDepth(sink string, depth int)
	MarkSyncOnce()
	// ObserveEventToStoreDuration records one end-to-end observation —
	// event origin (the ns-epoch Trigger) to the sink's store-visible
	// completion — for one named sink, with the push outcome ("ok" |
	// "error"). The unified drop/latency spec of dsca-2 §6 (rows 4-5): the
	// latency series refuses to ship without its blind-spot detector, so
	// the e2e histogram and IncEventsDropped land together.
	ObserveEventToStoreDuration(sink, outcome string, d time.Duration)
	// IncEventsDropped counts one event dropped by a full queue, labeled by
	// the dropping cluster (the cluster label rides the argument — dsca-1
	// DS-1-1 fix item 1 and dsca-2 §6 row 4, the single unified spec both
	// tracks land).
	IncEventsDropped(cluster string)
}
