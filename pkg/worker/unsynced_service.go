package worker

import (
    "context"
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

func (s *UnsyncedService) Add(e *Event) {
    if e != nil {
        if s.store == nil {
            s.store = make(map[string]*Event)
        }
        s.Lock()
        defer s.Unlock()
        // just store the latest event, the old event is not needed
        if old, ok := s.store[e.Data.InstanceId]; ok {
            if e.Data.Reversion > old.Data.Reversion {
                s.store[e.Data.InstanceId] = e
            }
        } else {
            s.store[e.Data.InstanceId] = e
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
            // call with WithRecover is to prevent subsequent function calls from panic
            tools.WithRecover(func() {
                us.Lock()
                defer us.Unlock()
                for k, v := range us.store {
                    // if the push is successful, delete the event
                    if err := us.pusher.Push(v.Trigger, v.Data); err == nil {
                        delete(us.store, k)
                    } else {
                        log.Logger.Errorf("retry trying to push instance failed again, appcode: %s, instance: %s, reversion: %d, err: %s", v.Data.AppCode, v.Data.InstanceId, v.Data.Reversion, err.Error())
                    }
                }
            })
        }
    }
}
