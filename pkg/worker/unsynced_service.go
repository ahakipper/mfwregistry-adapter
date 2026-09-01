package worker

import (
	"context"
	"sync"
	"time"

	"spotter/internal/ports"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
	"spotter/tools"
)

type UnsyncedService struct {
	ctx     context.Context
	pusher  discoverycenter.Pusher
	logger  ports.Logger
	metrics ports.MetricsRecorder
	store   map[string]*Event
	sync.RWMutex
}

func NewUnsyncedService(ctx context.Context, pusher discoverycenter.Pusher, logger ports.Logger, metrics ports.MetricsRecorder) *UnsyncedService {
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if metrics == nil {
		metrics = nopMetricsRecorder{}
	}
	return &UnsyncedService{
		ctx:     ctx,
		pusher:  pusher,
		logger:  logger,
		metrics: metrics,
		store:   make(map[string]*Event),
	}
}

func (s *UnsyncedService) Add(triggerTime int64, instances []*v2.Instance) {
	s.logger.Infof("unsyncService add instance, instance: %v", instances)
	if instances == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	if s.store == nil {
		s.store = make(map[string]*Event)
	}
	for _, item := range instances {
		if item == nil {
			continue
		}
		if old, ok := s.store[item.InstanceId]; ok {
			oldIns := old.Data[0]
			if item.Reversion > oldIns.Reversion {
				old.Data[0] = item
				s.store[item.InstanceId] = old
			}
			continue
		}
		s.store[item.InstanceId] = &Event{
			Trigger: triggerTime,
			Data:    []*v2.Instance{item},
			Operate: OperateTypeSync,
		}
	}
}

func (s *UnsyncedService) Len() int {
	if s == nil {
		return 0
	}
	s.RLock()
	defer s.RUnlock()
	return len(s.store)
}

func (s *UnsyncedService) Sync() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			tools.WithRecover(s.syncOnce)
		}
	}
}

func (s *UnsyncedService) syncOnce() {
	s.Lock()
	defer s.Unlock()
	if len(s.store) > 0 {
		s.logger.Infof("unsync service worked count :%d \n", len(s.store))
	}
	s.metrics.SetSyncErrorQueueDepth(len(s.store))
	for key, event := range s.store {
		if err := s.pusher.Push(event.Trigger, event.Data); err == nil {
			delete(s.store, key)
		} else {
			s.logger.Errorf("retry trying to push instance failed again, data: %v, err: %s", event.Data, err.Error())
		}
	}
}
