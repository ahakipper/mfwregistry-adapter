package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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
// belongs to the one and only sink. It matches the fanout's AtlasSinkName
// so queue keys and metrics series agree on both paths.
const plainSinkName = AtlasSinkName

// retryKey identifies one pending push: the instance and the sink that
// failed to receive it.
type retryKey struct {
	InstanceID string
	Sink       string
	Operation  RetryOperationKind
	Scope      string
	BatchID    string
}

type RetryOperationKind string

const (
	RetryPush    RetryOperationKind = "Push"
	RetryPushAll RetryOperationKind = "PushAll"
)

// pendingPush is one queued retry: the original trigger time and the
// instance to re-push.
type pendingPush struct {
	Trigger      int64
	Instance     *instance.Instance
	Full         bool
	Batch        []*instance.Instance
	Operation    RetryOperationKind
	Scope        string
	BatchID      string
	FullRevision uint64
	Revalidate   func() ([]*instance.Instance, bool)
}

// AddFull preserves a failed full synchronization as a full operation so
// catalog/prune semantics are not downgraded to per-instance retries.
func (s *UnsyncedService) AddFull(trigger int64, instances []*instance.Instance, sinks []string) {
	if len(instances) == 0 {
		return
	}
	for _, item := range instances {
		if item != nil {
			goto nonEmpty
		}
	}
	return
nonEmpty:
	scope := fullScope(instances)
	s.AddFullWithMeta(trigger, instances, sinks, scope, fullBatchID(scope, instances), uint64(maxPositive(trigger)), nil)
}

// AddFullWithMeta preserves the complete operation and its source metadata so
// retries can rebuild the snapshot instead of replaying a stale batch.
func (s *UnsyncedService) AddFullWithMeta(trigger int64, instances []*instance.Instance, sinks []string, scope, batchID string, sequence uint64, revalidate func() ([]*instance.Instance, bool)) {
	if scope == "" || batchID == "" {
		return
	}
	allNil := true
	for _, item := range instances {
		if item != nil {
			allNil = false
			break
		}
	}
	if allNil && scope == "" {
		return
	}
	targets := sinks
	if len(targets) == 0 {
		targets = s.sinkNames()
	}
	s.Lock()
	defer s.Unlock()
	if s.store == nil {
		s.store = make(map[retryKey]*pendingPush)
	}
	if sequence == 0 {
		if trigger > 0 {
			sequence = uint64(trigger)
		}
	}
	for _, sink := range targets {
		// BatchID is metadata used to compare generations; the map key is the
		// logical full-operation scope so a newer batch replaces an older one.
		key := retryKey{InstanceID: "__full__", Sink: sink, Operation: RetryPushAll, Scope: scope}
		if old := s.store[key]; old != nil && old.FullRevision > sequence {
			continue
		}
		batch := append([]*instance.Instance(nil), instances...)
		s.store[key] = &pendingPush{Trigger: trigger, Full: true, Batch: batch, Operation: RetryPushAll, Scope: scope, BatchID: batchID, FullRevision: sequence, Revalidate: revalidate}
	}
}

func maxPositive(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func fullScope(instances []*instance.Instance) string {
	set := make(map[string]struct{})
	for _, item := range instances {
		if item == nil {
			continue
		}
		scope := item.Provider
		if scope == "" {
			scope = item.SourceCluster
		}
		if scope == "" {
			scope = "unknown"
		}
		set[scope] = struct{}{}
	}
	scopes := make([]string, 0, len(set))
	for scope := range set {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	if len(scopes) == 0 {
		return "unknown"
	}
	return strings.Join(scopes, "+")
}

func fullBatchID(scope string, instances []*instance.Instance) string {
	ids := make([]string, 0, len(instances))
	for _, item := range instances {
		if item != nil {
			payload, err := json.Marshal(item)
			if err != nil {
				payload = []byte(fmt.Sprintf("%s/%d", instance.IdentityKey(item), item.Reversion))
			}
			ids = append(ids, fmt.Sprintf("%s:%s", instance.IdentityKey(item), payload))
		}
	}
	sort.Strings(ids)
	h := sha256.Sum256([]byte(scope + ":" + strings.Join(ids, ",")))
	return fmt.Sprintf("%x", h[:8])
}

// FullBatchID is the stable identity of one complete snapshot operation.
// Providers set it on SyncAll events so a retry never has to guess the batch
// boundary from only the first instance.
func FullBatchID(scope string, instances []*instance.Instance) string {
	return fullBatchID(scope, instances)
}

// AddOperation implements the typed ports retry contract while retaining the
// worker's per-sink coalescing store internally.
func (s *UnsyncedService) AddOperation(op ports.RetryOperation) {
	if op.Sink == "" {
		return
	}
	if op.Operate == ports.OperateTypeSyncAll {
		s.AddFullWithMeta(op.Trigger, op.Instances, []string{op.Sink}, op.Scope, op.BatchID, op.Sequence, op.Revalidate)
		return
	}
	s.Add(op.Trigger, op.Instances, []string{op.Sink})
}

// DrainOperations exposes a stable typed snapshot for migration/replay tests.
// Production retry consumption remains in syncOnce so successful-delete and
// mid-cycle re-add semantics stay centralized.
func (s *UnsyncedService) DrainOperations() []ports.RetryOperation {
	s.RLock()
	defer s.RUnlock()
	result := make([]ports.RetryOperation, 0, len(s.store))
	for key, pending := range s.store {
		if pending == nil {
			continue
		}
		op := ports.RetryOperation{
			Sink:       key.Sink,
			Operate:    ports.OperateTypeSync,
			Provider:   pending.Scope,
			Scope:      pending.Scope,
			BatchID:    pending.BatchID,
			Revision:   int64(pending.FullRevision),
			Sequence:   pending.FullRevision,
			Trigger:    pending.Trigger,
			Revalidate: pending.Revalidate,
		}
		if pending.Full {
			op.Operate = ports.OperateTypeSyncAll
			op.Instances = append([]*instance.Instance(nil), pending.Batch...)
		} else if pending.Instance != nil {
			op.Identity = instance.IdentityKey(pending.Instance)
			op.Revision = pending.Instance.Reversion
			op.Instances = []*instance.Instance{pending.Instance}
		}
		result = append(result, op)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Sink != result[j].Sink {
			return result[i].Sink < result[j].Sink
		}
		return result[i].Scope+result[i].BatchID+result[i].Identity < result[j].Scope+result[j].BatchID+result[j].Identity
	})
	return result
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

var _ ports.RetryOperationQueue = (*UnsyncedService)(nil)

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
			key := retryKey{InstanceID: instance.IdentityKey(item), Sink: sink, Operation: RetryPush}
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

	s.dropGhostSinkKeys(batch)

	for _, sink := range s.sinkNames() {
		s.pushSinkOnce(sink, batch)
	}
}

// dropGhostSinkKeys removes the batch's keys whose sink is no longer in the
// CURRENT sink set (AUDIT-A-3). A queued key for a sink that no longer
// exists can never be pushed — PushTo answers "unknown sink" forever — so
// leaving it queued leaks the entry for the process lifetime while the
// depth metrics (which report registered names only) never show it. The
// drop happens under the store lock with a warning naming the sink and the
// instance ids, and uses the delete-by-compare discipline so a mid-cycle
// re-add of a newer revision still survives (a re-add targeting the same
// ghost sink is dropped by the next cycle's snapshot — the condition cannot
// heal, since the sink set is construction-immutable).
func (s *UnsyncedService) dropGhostSinkKeys(batch map[retryKey]pendingPush) {
	known := make(map[string]struct{}, len(s.sinkNames()))
	for _, name := range s.sinkNames() {
		known[name] = struct{}{}
	}
	var ghosts []retryKey
	for key := range batch {
		if _, ok := known[key.Sink]; !ok {
			ghosts = append(ghosts, key)
		}
	}
	if len(ghosts) == 0 {
		return
	}
	s.Lock()
	for _, key := range ghosts {
		attempted := batch[key]
		if current, ok := s.store[key]; ok && samePending(current, &attempted) {
			delete(s.store, key)
		}
	}
	s.Unlock()
	for _, key := range ghosts {
		id := key.InstanceID
		if batch[key].Instance != nil {
			id = batch[key].Instance.InstanceId
		}
		s.logger.Warnf("dropping queued push for unknown sink %q (the sink is not registered; the entry can never be pushed), instance: %s",
			key.Sink, id)
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
			if pending.Full {
				s.retryKeyed(sink, key, pending, s.pusher.PushAll(pending.Trigger, pending.Batch))
				continue
			}
			s.retryKeyed(sink, key, pending,
				s.pusher.Push(pending.Trigger, []*instance.Instance{pending.Instance}))
		}
		return
	}
	for _, key := range keys {
		pending := batch[key]
		if pending.Full {
			var err error
			if opSink, ok := s.fanout.(ports.FullOperationSink); ok {
				err = opSink.PushAllOperation(ports.RetryOperation{
					Sink: sink, Operate: ports.OperateTypeSyncAll, Provider: pending.Scope,
					Scope: pending.Scope, BatchID: pending.BatchID, Sequence: pending.FullRevision,
					Trigger: pending.Trigger, Instances: pending.Batch, Revalidate: pending.Revalidate,
				})
			} else if withRevalidate, ok := s.fanout.(interface {
				PushAllToWithRevalidate(string, int64, string, string, []*instance.Instance, func() ([]*instance.Instance, bool)) error
			}); ok {
				err = withRevalidate.PushAllToWithRevalidate(sink, pending.Trigger, pending.Scope, pending.BatchID, pending.Batch, pending.Revalidate)
			} else {
				err = s.fanout.PushAllTo(sink, pending.Trigger, pending.Scope, pending.BatchID, pending.Batch)
			}
			s.retryKeyed(sink, key, pending, err)
			continue
		}
		s.retryKeyed(sink, key, pending,
			s.fanout.PushTo(sink, pending.Trigger, []*instance.Instance{pending.Instance}))
	}
}

// retryKeyed applies one push attempt's outcome: nil error deletes the key
// unless the entry was re-added mid-cycle; an error keeps it queued for the
// next cycle, except a permanent error, which drops the entry.
func (s *UnsyncedService) retryKeyed(sink string, key retryKey, pending pendingPush, err error) {
	data := "<full batch>"
	if pending.Instance != nil {
		data = pending.Instance.InstanceId
	}
	if pending.Full && errors.Is(err, errStaleFullPush) {
		s.logger.Warnf("dropping stale full retry for scope %q; the next full snapshot will rebuild it", pending.Scope)
		s.Lock()
		if current, ok := s.store[key]; ok && samePending(current, &pending) {
			delete(s.store, key)
		}
		s.Unlock()
		return
	}
	if err != nil {
		s.logger.Errorf("retry trying to push operation failed again, sink: %s, data: %s, err: %s",
			sink, data, err.Error())
		// A 4xx from the sink client (e.g. the nacos APIError) is permanent:
		// the request itself is rejected, so an identical retry can never
		// succeed and would spin forever (the live incident: 9624 futile
		// retries of an unregisterable DELETE). Drop the entry from the queue
		// instead of re-queuing it for the next cycle.
		var p interface{ Permanent() bool }
		if errors.As(err, &p) && p.Permanent() {
			if pending.Full {
				s.logger.Errorf("permanent_full_operation: dropping non-retryable full operation sink=%s scope=%s batch_id=%s", sink, pending.Scope, pending.BatchID)
			}
			s.Lock()
			// delete only if the queued instance is still the one just pushed:
			// a mid-cycle re-add (e.g. a newer revision) must survive the drop.
			if current, ok := s.store[key]; ok && samePending(current, &pending) {
				delete(s.store, key)
				// the drop is claimed only now that the key was actually
				// deleted; a declined compare (a newer re-added revision)
				// logs nothing and leaves the entry queued.
				s.logger.Errorf("dropping permanently-failed operation from the retry queue, sink: %s, data: %s, err: %s",
					sink, data, err.Error())
			}
			s.Unlock()
		}
		return
	}
	s.Lock()
	// delete only if the queued instance is still the one just pushed: a
	// mid-cycle re-add (e.g. a newer revision) must survive the delete.
	if current, ok := s.store[key]; ok && samePending(current, &pending) {
		delete(s.store, key)
	}
	s.Unlock()
}

func samePending(current, attempted *pendingPush) bool {
	if current == nil || attempted == nil || current.Full != attempted.Full || current.Operation != attempted.Operation {
		return false
	}
	if current.Full {
		return current.Scope == attempted.Scope && current.BatchID == attempted.BatchID && current.FullRevision == attempted.FullRevision && fullBatchID(current.Scope, current.Batch) == fullBatchID(attempted.Scope, attempted.Batch)
	}
	return current.Instance == attempted.Instance
}

// totalSinkName is the metrics label for the TOTAL queue depth across all
// sinks — including keys whose sink no longer exists (ghost keys are
// invisible in the per-sink series, which report registered names only).
// It reuses the sync_error_gauge series with a label no sink can claim (the
// fanout constructor rejects this exact name), so no new Prometheus series
// definition is needed (the recorder's SetSyncErrorQueueDepth contract is
// unchanged).
const totalSinkName = "__total__"

// recordDepths reports the per-sink queue-depth metrics plus one
// total-across-sinks observation; it is called with the lock held so the
// depths and the snapshot agree. The series are the per-sink ones of
// plan §5.3; the __total__ observation is the AUDIT-A-3 addition that keeps
// ghost-sink keys (unpushable, unreported per-sink) visible to dashboards.
func (s *UnsyncedService) recordDepths(store map[retryKey]*pendingPush) {
	lens := lensOf(store)
	for _, sink := range s.sinkNames() {
		s.metrics.SetSyncErrorQueueDepth(sink, lens[sink])
	}
	s.metrics.SetSyncErrorQueueDepth(totalSinkName, len(store))
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
