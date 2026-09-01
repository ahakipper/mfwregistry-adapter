package worker

import v2 "spotter/pkg/beehive/service/v2"

type Worker interface {
	AddEventHandler(opt OperateType, handler EventResourceHandler)
	Handle(d *Event)
	ProcessUnsynced() // ProcessUnsynced process instances that have not been successfully pushed before
	GetAll(enable []int32, provider string) (r *v2.InstanceList, err error)
}

type EventResourceHandler func(ins *Event) (err error)

type OperateType string

const (
	OperateTypeSync    OperateType = "Sync"
	OperateTypeSyncAll OperateType = "SyncAll"
)

type Event struct {
	Trigger int64          // trigger time
	Data    []*v2.Instance // data
	Operate OperateType    // operate type
}

type EventResource struct {
	Operate OperateType
	Data    v2.Instance
}
