package worker

import v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"

type Worker interface {
    AddEventHandler(opt OperateType, handler EventResourceHandler)
    Handle(d *Event)
    ProcessUnsynced() // ProcessUnsynced process instances that have not been successfully pushed before
}

type EventResourceHandler func(ins *Event) (err error)

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

type EventResource struct {
    Operate OperateType
    Data    v2.Instance
}
