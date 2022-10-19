package mfwregistry

import (
    v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/notice"
    "strings"
)

type Pusher interface {
    Push(triggerTime int64, instance []*v2.Instance) (err error)
    PushAll(triggerTime int64, instance []*v2.Instance) (err error)
    GetAll(enable []int32, provider string) (list *v2.InstanceList, err error)
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
    if config.DisablePushWorker {
        log.Logger.Infof("perform a fake push operation, instances: %v", instance)
        return nil
    }
    // simulate push failure
    // err = errors.New("the registery not exists now")
    ids := []string{}
    for _, ins := range instance {
        ids = append(ids, ins.InstanceId)
    }
    res, err := mr.C.Sync(instance)
    if err != nil {
        // Synced instance to mfwregistry failed,need notice
        notice.Notice("增量同步数据失败", err.Error())
        log.Logger.Infof("synced instance to mfwregistry failed, instance ids: %s, rpc code: %d, rpc error: %s", strings.Join(ids, ","), res.GetCode(), res.GetMsg())
        return err
    } else {
        log.Logger.Infof("synced instance to mfwregistry successfully, instance ids: %s", strings.Join(ids, ","))
        return nil
    }
}

func (mr *MFWRegistry) PushAll(triggerTime int64, instance []*v2.Instance) (err error) {
    // simulate push failure
    // err = errors.New("the registery not exists now")
    res, err := mr.C.SyncAll(instance)
    if err != nil {
        // Synced all instance to mfwregistry failed,need notice
        notice.Notice("全量同步数据失败", err.Error())
        return err
    }
    log.Logger.Info(res)
    return nil
}

func (mr *MFWRegistry) GetAll(statuses []int32, provider string) (r *v2.InstanceList, err error) {
    r, err = mr.C.GetAll(statuses, provider)
    return
}
