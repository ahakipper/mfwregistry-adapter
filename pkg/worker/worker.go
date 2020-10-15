package worker

import (
    "context"
    "github.com/k0kubun/pp"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/mfwregistry"
)

type DefaultWorker struct {
    Handlers        map[OperateType]EventResourceHandler
    ctx             context.Context
    unsyncedService *UnsyncedService
    pusher          mfwregistry.Pusher
}

func NewResourceWorker(ctx context.Context) (w *DefaultWorker) {
    w = &DefaultWorker{
        ctx: ctx,
    }
    // init handlers
    w.InitEventHandlers()
    // create a registry pusher
    w.pusher = mfwregistry.NewMFWRegistry()

    // init the unsynced service to sync the instances that pushed failed before
    w.unsyncedService = NewUnsyncedService(w.ctx, w.pusher)
    go w.ProcessUnsynced()

    return
}

func (w *DefaultWorker) AddEventHandler(opt OperateType, handler EventResourceHandler) {
    if handler != nil && opt != "" {
        w.Handlers[opt] = handler
    }
}

func (w *DefaultWorker) InitEventHandlers() {
    w.Handlers = make(map[OperateType]EventResourceHandler)
    w.AddEventHandler(OperateTypeADD, func(e *Event) (err error) {
        pp.Println(e.Operate, e.Data.InstanceId)
        if err = w.pusher.Push(e.Trigger, e.Data); err != nil {
            log.Logger.Errorf("failed to push instance, appcode: %s, instance: %s, reversion: %d, err: %s",
                e.Data.AppCode, e.Data.InstanceId, e.Data.Reversion, err.Error())
            // if the event push fails, add it to UnsyncedService, and the UnsyncedService service completes the push
            w.unsyncedService.Add(e)
        }
        return
    })
    w.AddEventHandler(OperateTypeDELETE, func(e *Event) (err error) {
        w.pusher.Push(e.Trigger, e.Data)
        pp.Println(e.Operate, e.Data.InstanceId)
        if err = w.pusher.Push(e.Trigger, e.Data); err != nil {
            log.Logger.Errorf("failed to push instance, appcode: %s, instance: %s, reversion: %d, err: %s",
                e.Data.AppCode, e.Data.InstanceId, e.Data.Reversion, err.Error())
            // if the event push fails, add it to UnsyncedService, and the UnsyncedService service completes the push
            w.unsyncedService.Add(e)
        }
        return
    })
    w.AddEventHandler(OperateTypeUPDATE, func(e *Event) (err error) {
        w.pusher.Push(e.Trigger, e.Data)
        pp.Println(e.Operate, e.Data.InstanceId)
        if err = w.pusher.Push(e.Trigger, e.Data); err != nil {
            log.Logger.Errorf("failed to push instance, appcode: %s, instance: %s, reversion: %d, err: %s",
                e.Data.AppCode, e.Data.InstanceId, e.Data.Reversion, err.Error())
            // if the event push fails, add it to UnsyncedService, and the UnsyncedService service completes the push
            w.unsyncedService.Add(e)
        }
        return
    })
}

func (w *DefaultWorker) Handle(d *Event) {
    if d.Operate != "" {
        if call, ok := w.Handlers[d.Operate]; ok {
            _ = call(d)
        }
    }
}

// ProcessUnsynced process instances that have not been successfully pushed before
func (w *DefaultWorker) ProcessUnsynced() {
    w.unsyncedService.Sync()
}
