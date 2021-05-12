package worker

import (
    "context"
    v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/mfwregistry"
)

type FackPusher struct {
}

func NewFackPusher() *FackPusher {
    return &FackPusher{}
}

func (f FackPusher) Push(triggerTime int64, instance []*v2.Instance) (err error) {
    log.Logger.Info("implement me")
    return err
}

func (f FackPusher) PushAll(triggerTime int64, instance []*v2.Instance) (err error) {
    log.Logger.Info("implement me")
    return err
}

func (f FackPusher) GetAll(enable int32, provider string) (list *v2.InstanceList, err error) {
    log.Logger.Info("implement me")
    return nil, err
}

type FackWorker struct {
    Handlers        map[OperateType]EventResourceHandler
    ctx             context.Context
    unsyncedService *UnsyncedService
    pusher          mfwregistry.Pusher
}

func NewResourceFackWorker(ctx context.Context) (w *FackWorker) {
    w = &FackWorker{
        ctx: ctx,
    }
    // init handlers
    w.InitEventHandlers()
    // create a registry pusher
    w.pusher = NewFackPusher()

    // init the unsynced service to sync the instances that pushed failed before
    w.unsyncedService = NewUnsyncedService(w.ctx, w.pusher)
    go w.ProcessUnsynced()

    return
}

func (w *FackWorker) AddEventHandler(opt OperateType, handler EventResourceHandler) {
    if handler != nil && opt != "" {
        w.Handlers[opt] = handler
    }
}

func (w *FackWorker) InitEventHandlers() {
    w.Handlers = make(map[OperateType]EventResourceHandler)
    w.AddEventHandler(OperateTypeSync, func(e *Event) (err error) {
        if err = w.pusher.Push(e.Trigger, e.Data); err != nil {
            // if the event push fails, add it to UnsyncedService, and the UnsyncedService service completes the push
            log.Logger.Errorf("wokderService sync failed, err:%v instance: %v", err, e.Data[0].InstanceId)
            w.unsyncedService.Add(e.Trigger, e.Data)
        }
        return
    })
    w.AddEventHandler(OperateTypeSyncAll, func(e *Event) (err error) {
        if err = w.pusher.PushAll(e.Trigger, e.Data); err != nil {
            // if the event push fails, add it to UnsyncedService, and the UnsyncedService service completes the push
            log.Logger.Errorf("wokderService syncAll failed, instance: %v", e.Data)
            w.unsyncedService.Add(e.Trigger, e.Data)
        }
        return
    })
}

func (w *FackWorker) Handle(d *Event) {
    if d.Operate != "" {
        if call, ok := w.Handlers[d.Operate]; ok {
            _ = call(d)
        }
    }
}

// ProcessUnsynced process instances that have not been successfully pushed before
func (w *FackWorker) ProcessUnsynced() {
    w.unsyncedService.Sync()
}

func (w *FackWorker) GetAll(enable int32, provider string) (r *v2.InstanceList, err error) {
    r, err = w.pusher.GetAll(enable, provider)
    return
}
