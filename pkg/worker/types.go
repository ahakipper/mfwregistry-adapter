package worker

import (
	"spotter/internal/domain/instance"
)

type Worker interface {
	AddEventHandler(opt OperateType, handler EventResourceHandler)
	Handle(d *Event)
	ProcessUnsynced() // ProcessUnsynced process instances that have not been successfully pushed before
	GetAll(enable []int32, provider string) (r *instance.InstanceList, err error)
}

type EventResourceHandler func(ins *Event) (err error)

type OperateType string

const (
	OperateTypeSync    OperateType = "Sync"
	OperateTypeSyncAll OperateType = "SyncAll"
)

type Event struct {
	Trigger int64                // trigger time
	Data    []*instance.Instance // data
	Operate OperateType          // operate type
}

type EventResource struct {
	Operate OperateType
	Data    instance.Instance
}
