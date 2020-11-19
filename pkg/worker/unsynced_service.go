package worker

import (
    "context"
    v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/mfwregistry"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools"
    "sync"
    "time"
)

type UnsyncedService struct {
    ctx    context.Context
    pusher mfwregistry.Pusher
    store  map[string]*Event
    sync.RWMutex
}

func NewUnsyncedService(ctx context.Context, pusher mfwregistry.Pusher) *UnsyncedService {
    us := &UnsyncedService{
        ctx:    ctx,
        pusher: pusher,
        store:  make(map[string]*Event),
    }

    return us
}

func (s *UnsyncedService) Add(triggerTime int64, instance []*v2.Instance) {
    log.Logger.Infof("unsyncService add instance, instance: %v", instance)
    if instance != nil {
        if s.store == nil {
            s.store = make(map[string]*Event)
        }
        s.Lock()
        defer s.Unlock()
        // just store the latest event, the old event is not needed
        for _,item := range instance{
            if old, ok := s.store[item.InstanceId]; ok {
                oldIns := old.Data[0]
                if item.Reversion > oldIns.Reversion {
                    old.Data[0] = item
                    s.store[item.InstanceId] = old
                }
            } else {
                newIns := make([]*v2.Instance,1)
                newIns[0] = item
                newEvent := &Event{Trigger:triggerTime,Data:newIns,Operate:OperateTypeSync}
                s.store[item.InstanceId] = newEvent
            }
        }
    }
}

func (us *UnsyncedService) Sync() {
    ticker := time.NewTicker(5000 * time.Millisecond)
    for {
        select {
        case <-us.ctx.Done():
            ticker.Stop()
            return
        case <-ticker.C:
            tools.WithRecover(func() {
                us.Lock()
                defer us.Unlock()
                // call with WithRecover is to prevent subsequent function calls from panic
                log.Logger.Infof("unsync service worked count :%d \n",len(us.store))
                for k, v := range us.store {
                    // if the push is successful, delete the event
                    if err := us.pusher.Push(v.Trigger, v.Data); err == nil {
                        delete(us.store, k)
                    } else {
                        log.Logger.Errorf("retry trying to push instance failed again, data: %v, err: %s", v.Data, err.Error())
                    }
                }
            })
        }
    }
}
