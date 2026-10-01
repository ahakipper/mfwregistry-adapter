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

// LegacyEventQueue stores failed push events using the pre-multi-sink shape.
// It remains only for old fixtures and migration adapters.
type LegacyEventQueue interface {
	AddLegacy(triggerTime int64, instances []*instance.Instance)
	Len() int
	DrainLegacy() []*Event
}

// EventQueue stores typed retry operations. Full operations retain their sink,
// scope and batch identity instead of being downgraded to individual pushes.
type EventQueue interface {
	Add(RetryOperation)
	Len() int
	Drain() []RetryOperation
}

// RetryOperation is the typed retry contract used by multi-sink workers. It
// keeps the operation kind and source scope alongside the payload so a failed
// full push cannot be replayed as unrelated single-instance pushes.
type RetryOperation struct {
	Sink           string
	Operate        OperateType
	Provider       string
	Scope          string
	BatchID        string
	Identity       string
	Revision       int64
	Sequence       uint64
	Trigger        int64
	Instances      []*instance.Instance
	Revalidate     func() ([]*instance.Instance, bool)
	EmptyConfirmed bool
}

// RetryOperationQueue is the worker-facing descriptive seam. It uses named
// methods so the legacy worker can coexist with the typed EventQueue adapter
// during migration.
type RetryOperationQueue interface {
	AddOperation(RetryOperation)
	Len() int
	DrainOperations() []RetryOperation
}

// FullOperationSink is the metadata-aware boundary for replaying a complete
// operation. Implementations must preserve Scope and BatchID through their
// adapter even when the underlying service API only accepts instances.
type FullOperationSink interface {
	PushAllOperation(RetryOperation) error
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
	// SetK8sQueueDepth records the current depth of the k8s robot's
	// coalescing event queue — the queueDepth gauge of dsca-1 DS-1-1 fix
	// item 1 ("plus a queueDepth gauge"). The k8s provider publishes it on
	// a short ticker, reading the robot's QueueDepth (distinct keys in
	// flight, one entry per key regardless of coalescing).
	SetK8sQueueDepth(depth int)
}

// ConsulMetricsRecorder is the optional metrics seam for the Consul source.
// It intentionally remains separate from MetricsRecorder so existing
// applications and test fakes do not need to grow their synchronization
// metrics surface. Implementations must use a stable logical source scope;
// endpoint addresses, ACL tokens, and other credentials are never label
// values.
type ConsulMetricsRecorder interface {
	// ObserveConsulCatalogReadDuration records one complete catalog read,
	// including failed and partial reads. Outcome is a bounded state such as
	// "healthy_nonempty", "healthy_empty", "source_error", or "partial".
	ObserveConsulCatalogReadDuration(source, outcome string, d time.Duration)
	// IncConsulConversionSkips counts endpoints rejected during conversion.
	IncConsulConversionSkips(source string, count int)
	// IncConsulSourceError counts a rejected source read. Outcome is a bounded
	// error class (for example "source_error" or "partial"), never an error
	// message that could contain deployment details or credentials.
	IncConsulSourceError(source, outcome string)
	// IncConsulHealthyEmptyConfirmation counts each healthy-empty confirmation
	// advancement. Outcome is "pending" until the confirmation threshold is
	// reached, then "confirmed".
	IncConsulHealthyEmptyConfirmation(source, outcome string)
	// ObserveConsulWatchToSyncDuration records source watch-return to provider
	// sync completion. Outcomes are bounded source states such as sync_ok or
	// sync_error; this is not the Nacos SDK acknowledgement outcome, which is
	// measured by the per-sink event-to-store metric.
	ObserveConsulWatchToSyncDuration(source, outcome string, d time.Duration)
}

// ConsulRequestMetricsRecorder is the optional request-observability seam for
// the Consul source. It is deliberately separate from ConsulMetricsRecorder:
// existing applications may implement the catalog/snapshot metrics interface
// without having to grow a new request-level method set. Implementations must
// receive only a logical source scope and bounded operation/outcome values;
// endpoint addresses, ACL tokens, and raw error strings are never labels.
type ConsulRequestMetricsRecorder interface {
	// ObserveConsulLeaderProbeDuration records a Status().Leader probe. The
	// outcome is one of the bounded values success, error, empty_leader, or
	// other source-defined diagnostics selected by the provider.
	ObserveConsulLeaderProbeDuration(source, outcome string, d time.Duration)
	// ObserveConsulRequestDuration records one Consul API request such as
	// catalog_services, health_service, or health_state. Operation and outcome
	// are bounded by the concrete recorder before they reach Prometheus.
	ObserveConsulRequestDuration(source, operation, outcome string, d time.Duration)
}
