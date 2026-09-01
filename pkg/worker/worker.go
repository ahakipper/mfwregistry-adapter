package worker

import (
	"context"
	"errors"
	"time"

	"spotter/internal/ports"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
)

type DefaultWorker struct {
	Handlers        map[OperateType]EventResourceHandler
	ctx             context.Context
	unsyncedService *UnsyncedService
	pusher          discoverycenter.Pusher
	logger          ports.Logger
}

func NewResourceWorker(ctx context.Context, pusher discoverycenter.Pusher, logger ports.Logger, metrics ports.MetricsRecorder) (*DefaultWorker, error) {
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
				w.unsyncedService.Add(e.Trigger, e.Data)
			}
			return err
		}
		return nil
	})
	w.AddEventHandler(OperateTypeSyncAll, func(e *Event) error {
		if err := w.pusher.PushAll(e.Trigger, e.Data); err != nil {
			w.logger.Errorf("wokderService syncAll failed, instance: %v", e.Data)
			w.unsyncedService.Add(e.Trigger, e.Data)
			return err
		}
		return nil
	})
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

func (w *DefaultWorker) GetAll(enable []int32, provider string) (*v2.InstanceList, error) {
	return w.pusher.GetAll(enable, provider)
}

type nopMetricsRecorder struct{}

func (nopMetricsRecorder) ObserveSyncOnceDuration(time.Duration) {}

func (nopMetricsRecorder) ObserveSyncAllDuration(string, time.Duration) {}

func (nopMetricsRecorder) SetSyncErrorQueueDepth(int) {}

func (nopMetricsRecorder) MarkSyncOnce() {}
