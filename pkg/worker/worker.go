package worker

import (
    "context"
    v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
)

type Worker struct {
    Handlers map[OperateType]EventResourceHandler
    ctx      context.Context
}

type EventResource struct {
    Operate OperateType
    Data    v2.Instance
}

type EventResourceHandler func(ins Event) (err error)

type OperateType string

const (
    OperateTypeADD    OperateType = "ADD"
    OperateTypeDELETE OperateType = "DELETE"
    OperateTypeUPDATE OperateType = "UPDATE"
)

type Event struct {
    Trigger int64        // trigger time
    Data    *v2.Instance // data
    Operate OperateType  // operate type
}

func NewResourceWorker(ctx context.Context) (w *Worker) {
    w = &Worker{
        ctx: ctx,
    }
    w.InitEventHandlers()
    return
}

func (w *Worker) AddEventHandler(opt OperateType, handler EventResourceHandler) {
    if handler != nil && opt != "" {
        w.Handlers[opt] = handler
    }
}

func (w *Worker) InitEventHandlers() {
    w.Handlers = make(map[OperateType]EventResourceHandler)
    w.AddEventHandler(OperateTypeADD, func(e Event) (err error) {
        // pp.Println(e.Operate, e.Data.InstanceId)
        return
    })
    w.AddEventHandler(OperateTypeDELETE, func(e Event) (err error) {
        // pp.Println(e.Operate, e.Data.InstanceId)
        return
    })
    w.AddEventHandler(OperateTypeUPDATE, func(e Event) (err error) {
        // pp.Println(e.Operate, e.Data.InstanceId)
        return
    })
}

func (w *Worker) Handle(d Event) {
    if d.Operate != "" {
        if call, ok := w.Handlers[d.Operate]; ok {
            _ = call(d)
        }
    }
}
