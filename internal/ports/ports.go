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
	SetSyncErrorQueueDepth(int)
	MarkSyncOnce()
}
