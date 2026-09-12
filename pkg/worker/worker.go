package worker

import (
	"context"
	"errors"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

type DefaultWorker struct {
	Handlers        map[OperateType]EventResourceHandler
	ctx             context.Context
	unsyncedService *UnsyncedService
	pusher          ports.InstanceSink
	logger          ports.Logger
}

func NewResourceWorker(ctx context.Context, pusher ports.InstanceSink, logger ports.Logger, metrics ports.MetricsRecorder) (*DefaultWorker, error) {
	if pusher == nil {
		return nil, errors.New("worker: pusher is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if metrics == nil {
		metrics = nopMetricsRecorder{}
	}
	w := &DefaultWorker{
		ctx:    ctx,
		pusher: pusher,
		logger: logger,
	}
	w.unsyncedService = NewUnsyncedService(w.ctx, w.pusher, w.logger, metrics)
	w.InitEventHandlers()
	go w.ProcessUnsynced()
	return w, nil
}

func (w *DefaultWorker) AddEventHandler(opt OperateType, handler EventResourceHandler) {
	if handler != nil && opt != "" {
		w.Handlers[opt] = handler
	}
}

func (w *DefaultWorker) InitEventHandlers() {
	w.Handlers = make(map[OperateType]EventResourceHandler)
	w.AddEventHandler(OperateTypeSync, func(e *Event) error {
		if err := w.pusher.Push(e.Trigger, e.Data); err != nil {
			instanceID := ""
			if len(e.Data) > 0 && e.Data[0] != nil {
				instanceID = e.Data[0].InstanceId
			}
			w.logger.Errorf("wokderService sync failed, err:%v instance: %v", err, instanceID)
			if len(e.Data) > 0 {
				w.unsyncedService.Add(e.Trigger, e.Data, w.queuedSinks(err))
			}
			return err
		}
		return nil
	})
	w.AddEventHandler(OperateTypeSyncAll, func(e *Event) error {
		var err error
		if gated, ok := w.pusher.(interface {
			PushAllWithRevalidate(int64, []*instance.Instance, func() ([]*instance.Instance, bool)) error
		}); ok {
			err = gated.PushAllWithRevalidate(e.Trigger, e.Data, e.Revalidate)
		} else {
			if e.Revalidate != nil {
				data, valid := e.Revalidate()
				if !valid {
					return nil
				}
				e.Data = data
			}
			err = w.pusher.PushAll(e.Trigger, e.Data)
		}
		// A full snapshot rejected as stale must never be queued: retrying the
		// same snapshot can prune newer remote state forever. Revalidate once and
		// submit only the latest complete snapshot.
		if errors.Is(err, errStaleFullPush) && e.Revalidate != nil && !hasNonStaleFanoutFailure(err) {
			if gated, ok := w.pusher.(interface {
				PushAllWithRevalidate(int64, []*instance.Instance, func() ([]*instance.Instance, bool)) error
			}); ok {
				err = gated.PushAllWithRevalidate(time.Now().UnixNano(), e.Data, e.Revalidate)
			} else if data, valid := e.Revalidate(); valid {
				e.Data = data
				err = w.pusher.PushAll(time.Now().UnixNano(), data)
			}
			if err == nil {
				return nil
			}
			if errors.Is(err, errStaleFullPush) {
				w.logger.Warnf("dropping stale full push after revalidation; snapshot will be rebuilt on the next tick")
				return nil
			}
		}
		// A stale full snapshot has no safe retry representation when the
		// producer cannot revalidate it. Queuing the same partial/stale batch
		// would let a later retry prune newer remote state. Drop it and wait
		// for the next provider full snapshot instead.
		if errors.Is(err, errStaleFullPush) && !hasNonStaleFanoutFailure(err) {
			w.logger.Warnf("dropping stale full push without revalidation; snapshot will be rebuilt on the next tick")
			return nil
		}
		if err != nil {
			w.logger.Errorf("wokderService syncAll failed, instance: %v", e.Data)
			w.unsyncedService.AddFullWithMeta(e.Trigger, e.Data, w.queuedSinks(err), e.Scope, e.BatchID, e.Sequence, e.Revalidate, e.EmptyConfirmed)
			return err
		}
		return nil
	})
}

// hasNonStaleFanoutFailure keeps a mixed fan-out error retryable. A stale
// full snapshot can be discarded, but a sibling sink's timeout or other
// transient failure still needs to enter the per-sink retry queue.
func hasNonStaleFanoutFailure(err error) bool {
	var fanoutErr FanoutError
	if !errors.As(err, &fanoutErr) {
		return false
	}
	for _, failure := range fanoutErr {
		if !errors.Is(failure.Err, errStaleFullPush) {
			return true
		}
	}
	return false
}

// queuedSinks resolves which sinks a failed push queues retries for
// (plan §5.2): a FanoutError queues only its failed sinks (detected with
// errors.As, never a type assertion); any other error conservatively queues
// every known sink. With today's single plain sink both paths queue that
// one sink, so retry outcomes are unchanged.
func (w *DefaultWorker) queuedSinks(err error) []string {
	return failedSinkNames(err, w.unsyncedService.sinkNames())
}

func (w *DefaultWorker) Handle(d *Event) {
	if d == nil || d.Operate == "" {
		return
	}
	if call, ok := w.Handlers[d.Operate]; ok {
		_ = call(d)
	}
}

// ProcessUnsynced processes instances that have not been successfully pushed before.
func (w *DefaultWorker) ProcessUnsynced() {
	w.unsyncedService.Sync()
}

func (w *DefaultWorker) GetAll(enable []int32, provider string) (*instance.InstanceList, error) {
	return w.pusher.GetAll(enable, provider)
}

type nopMetricsRecorder struct{}

func (nopMetricsRecorder) ObserveSyncOnceDuration(time.Duration) {}

func (nopMetricsRecorder) ObserveSyncAllDuration(string, time.Duration) {}

func (nopMetricsRecorder) SetSyncErrorQueueDepth(string, int) {}

func (nopMetricsRecorder) MarkSyncOnce() {}

func (nopMetricsRecorder) ObserveEventToStoreDuration(string, string, time.Duration) {}

func (nopMetricsRecorder) IncEventsDropped(string) {}

func (nopMetricsRecorder) SetK8sQueueDepth(int) {}
