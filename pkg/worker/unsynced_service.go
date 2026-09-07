package worker

import (
	"context"
	"sync"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/tools"
)

// Per-sink retry state (plan §5.2): one entry per (instance, failed sink);
// an entry is deleted only when that sink's own push succeeds, so one sink
// failing never loses another sink's retry. Keep-highest-Reversion applies
// per key: the same instanceId may hold different revisions for different
// sinks, and a re-add with a higher Reversion replaces only that sink's
// entry.

// plainSinkName is the queue's sink name for a legacy (non-fanout)
// pusher: the single-sink deployment of today, where every failed push
// belongs to the one and only sink.
const plainSinkName = "atlas"

// retryKey identifies one pending push: the instance and the sink that
// failed to receive it.
type retryKey struct {
	InstanceID string
	Sink       string
}

// pendingPush is one queued retry: the original trigger time and the
// instance to re-push.
type pendingPush struct {
	Trigger  int64
	Instance *instance.Instance
}

// UnsyncedService retries failed pushes per sink every 5s. The store lock
// is never held across a push: syncOnce snapshots the pending keys under
// the lock, pushes outside it, and deletes only the keys that both
// succeeded and were not re-added mid-cycle (delete-by-compare under the
// lock), so a slow or dead sink cannot block Add.
type UnsyncedService struct {
	ctx     context.Context
	pusher  ports.InstanceSink
	fanout  sinkFanout
	logger  ports.Logger
	metrics ports.MetricsRecorder
	store   map[retryKey]*pendingPush
	sync.RWMutex
}

func NewUnsyncedService(ctx context.Context, pusher ports.InstanceSink, logger ports.Logger, metrics ports.MetricsRecorder) *UnsyncedService {
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if metrics == nil {
		metrics = nopMetricsRecorder{}
	}
	// A pusher exposing the §6.4 retry seam (F4's FanoutSink) is driven
	// per sink; a plain ports.InstanceSink keeps the pre-F3 single-sink
	// behavior under the plain sink name.
	fanout, _ := pusher.(sinkFanout)
	return &UnsyncedService{
		ctx:     ctx,
		pusher:  pusher,
		fanout:  fanout,
		logger:  logger,
		metrics: metrics,
		store:   make(map[retryKey]*pendingPush),
	}
}

// Add queues one entry per (instance, failed sink). An empty sinks list
// means every known sink: the conservative plain-error fallback of
// plan §5.2.
func (s *UnsyncedService) Add(triggerTime int64, instances []*instance.Instance, sinks []string) {
	if len(instances) == 0 {
		return
	}
	targets := sinks
	if len(targets) == 0 {
		targets = s.sinkNames()
	}
	if len(targets) == 0 {
		return
	}
	s.logger.Infof("unsyncService add instance, instance: %v, sinks: %v", instances, targets)
	s.Lock()
	defer s.Unlock()
	if s.store == nil {
		s.store = make(map[retryKey]*pendingPush)
	}
	for _, item := range instances {
		if item == nil {
			continue
		}
		for _, sink := range targets {
			key := retryKey{InstanceID: item.InstanceId, Sink: sink}
			if old, ok := s.store[key]; ok {
				if item.Reversion > old.Instance.Reversion {
					old.Instance = item
				}
				continue
			}
			s.store[key] = &pendingPush{
				Trigger:  triggerTime,
				Instance: item,
			}
		}
	}
}

// Len reports the total number of queued (instance, sink) keys.
func (s *UnsyncedService) Len() int {
	if s == nil {
		return 0
	}
	s.RLock()
	defer s.RUnlock()
	return len(s.store)
}

// Lens reports the queued key count per sink name.
func (s *UnsyncedService) Lens() map[string]int {
	if s == nil {
		return map[string]int{}
	}
	s.RLock()
	defer s.RUnlock()
	return lensOf(s.store)
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

// syncOnce pushes every pending instance to its own sink only, and deletes
// exactly the keys whose own push succeeded (plan §3.2). The store lock is
// held only for the snapshot, the depth metrics and the deletes — never
// across a network push (the plan §5.2 lock-discipline fix).
//
// The snapshot is by value: the batch must pin the cycle's view of each
// entry, because Add updates a queued entry in place and a mid-cycle re-add
// must leave the newer entry in the store (the delete-by-compare in
// retryKeyed decides, by instance identity, whether the entry the cycle
// just pushed is still the queued one).
func (s *UnsyncedService) syncOnce() {
	s.RLock()
	if len(s.store) > 0 {
		s.logger.Infof("unsync service worked count :%d \n", len(s.store))
	}
	s.recordDepths(s.store)
	// snapshot: each key is pushed to its own sink only, outside the lock
	batch := make(map[retryKey]pendingPush, len(s.store))
	for key, pending := range s.store {
		if pending != nil {
			batch[key] = *pending
		}
	}
	s.RUnlock()

	for _, sink := range s.sinkNames() {
		s.pushSinkOnce(sink, batch)
	}
}

// pushSinkOnce pushes one sink's pending instances to that sink and
// deletes the keys that succeeded and were not re-added mid-cycle.
func (s *UnsyncedService) pushSinkOnce(sink string, batch map[retryKey]pendingPush) {
	keys := keysForSink(batch, sink)
	if len(keys) == 0 {
		return
	}
	if s.fanout == nil {
		// Legacy path (single-sink deployments): a plain pusher has no
		// per-sink surface, so every queued key pushes through Push
		// directly, behavior-identical to pre-F3.
		for _, key := range keys {
			pending := batch[key]
			s.retryKeyed(sink, key, pending,
				s.pusher.Push(pending.Trigger, []*instance.Instance{pending.Instance}))
		}
		return
	}
	for _, key := range keys {
		pending := batch[key]
		s.retryKeyed(sink, key, pending,
			s.fanout.PushTo(sink, pending.Trigger, []*instance.Instance{pending.Instance}))
	}
}

// retryKeyed applies one push attempt's outcome: nil error deletes the key
// unless the entry was re-added mid-cycle; an error keeps it queued for the
// next cycle.
func (s *UnsyncedService) retryKeyed(sink string, key retryKey, pending pendingPush, err error) {
	if err != nil {
		s.logger.Errorf("retry trying to push instance failed again, sink: %s, data: %v, err: %s",
			sink, pending.Instance, err.Error())
		return
	}
	s.Lock()
	// delete only if the queued instance is still the one just pushed: a
	// mid-cycle re-add (e.g. a newer revision) must survive the delete.
	if current, ok := s.store[key]; ok && current.Instance == pending.Instance {
		delete(s.store, key)
	}
	s.Unlock()
}

// recordDepths reports the per-sink queue-depth metrics; it is called with
// the lock held so the depths and the snapshot agree. The series are the
// per-sink ones of plan §5.3.
func (s *UnsyncedService) recordDepths(store map[retryKey]*pendingPush) {
	lens := lensOf(store)
	for _, sink := range s.sinkNames() {
		s.metrics.SetSyncErrorQueueDepth(sink, lens[sink])
	}
}

// sinkNames returns the sink names in fanout registration order (or the
// single plain-sink name), so per-sink work is deterministic and metrics
// observations are ordered. It reads only construction-immutable fields, so
// it is safe to call with or without the lock held.
func (s *UnsyncedService) sinkNames() []string {
	if s.fanout != nil {
		return s.fanout.Sinks()
	}
	return []string{plainSinkName}
}

// keysForSink returns the batch's keys for one sink in map iteration order.
func keysForSink(batch map[retryKey]pendingPush, sink string) []retryKey {
	var keys []retryKey
	for key := range batch {
		if key.Sink == sink {
			keys = append(keys, key)
		}
	}
	return keys
}

// lensOf counts queued keys per sink name.
func lensOf(store map[retryKey]*pendingPush) map[string]int {
	lens := make(map[string]int, len(store))
	for key := range store {
		lens[key.Sink]++
	}
	return lens
}
