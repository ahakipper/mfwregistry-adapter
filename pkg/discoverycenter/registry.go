package discoverycenter

import (
    "strings"

    v2 "github.com/ahakipper/spotter/pkg/beehive/service/v2"
    "github.com/ahakipper/spotter/config"
    "github.com/ahakipper/spotter/pkg/log"
    "github.com/ahakipper/spotter/pkg/notice"
)

// Pusher is the contract used by the workers to talk to the discovery center.
type Pusher interface {
    Push(triggerTime int64, instance []*v2.Instance) (err error)
    PushAll(triggerTime int64, instance []*v2.Instance) (err error)
    GetAll(enable []int32, provider string) (list *v2.InstanceList, err error)
}

// DiscoveryCenter is the client of the discovery center (Atlas).
type DiscoveryCenter struct {
    C *Client
}

// NewDiscoveryCenter creates a pusher bound to the discovery center.
// It panics when the gRPC connection to the discovery center cannot be
// established, mirroring the previous constructor behaviour.
func NewDiscoveryCenter() *DiscoveryCenter {
    if c, err := NewInstance(); err != nil {
        panic(err)
    } else {
        return &DiscoveryCenter{
            C: c,
        }
    }
}

func (mr *DiscoveryCenter) Push(triggerTime int64, instance []*v2.Instance) (err error) {
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
        // Synced instance to the discovery center failed, need notice
        notice.Notice("Failed to sync data incrementally", err.Error())
        log.Logger.Infof("synced instance to the discovery center failed, instance ids: %s, rpc code: %d, rpc error: %s", strings.Join(ids, ","), res.GetCode(), res.GetMsg())
        return err
    } else {
        log.Logger.Infof("synced instance to the discovery center successfully, instance ids: %s", strings.Join(ids, ","))
        return nil
    }
}

func (mr *DiscoveryCenter) PushAll(triggerTime int64, instance []*v2.Instance) (err error) {
    // simulate push failure
    // err = errors.New("the registery not exists now")
    res, err := mr.C.SyncAll(instance)
    if err != nil {
        // Synced all instance to the discovery center failed, need notice
        notice.Notice("Failed to sync all data", err.Error())
        return err
    }
    log.Logger.Info(res)
    return nil
}

func (mr *DiscoveryCenter) GetAll(statuses []int32, provider string) (r *v2.InstanceList, err error) {
    r, err = mr.C.GetAll(statuses, provider)
    return
}
