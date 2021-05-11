package mfwregistry

import (
    v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
)

type Pusher interface {
    Push(triggerTime int64, instance []*v2.Instance) (err error)
    PushAll(triggerTime int64, instance []*v2.Instance) (err error)
    GetAll(enable int32, provider string) (list *v2.InstanceList, err error)
}

type MFWRegistry struct {
    C *Client
}

func NewMFWRegistry() *MFWRegistry {
    if c, err := NewInstance(); err != nil {
        panic(err)
    } else {
        return &MFWRegistry{
            C: c,
        }
    }
}

func (mr *MFWRegistry) Push(triggerTime int64, instance []*v2.Instance) (err error) {
    // simulate push failure
    //err = errors.New("the registery not exists now")
    res, err := mr.C.Sync(instance)
    if err != nil {
        return err
    }
    log.Logger.Info(res)
    return nil
}

func (mr *MFWRegistry) PushAll(triggerTime int64, instance []*v2.Instance) (err error) {
    // simulate push failure
    //err = errors.New("the registery not exists now")
    res, err := mr.C.SyncAll(instance)
    if err != nil {
        return err
    }
    log.Logger.Info(res)
    return nil
}

func (mr *MFWRegistry) GetAll(enable int32, provider string) (r *v2.InstanceList, err error) {
    r, err = mr.C.GetAll(enable, provider)
    return
}
