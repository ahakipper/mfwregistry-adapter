// Package k8srobot is a local, self-contained replacement of the former private
// "servicemesh/robot" multi-cluster Kubernetes client module.
//
// It exposes the same surface that this repository consumes:
//
//	client.NewRobot([]client.Cluster{{ConfigPath: ..., Resources: []client.RN{{client.Pods, ""}}}}, debug)
//	robot.Run() / robot.Stop() / robot.HasSynced() / robot.Pop()
//	robot.Finish(obj) / robot.GetByKey(client.Pods, key) / robot.List(client.Pods)
//
// The implementation is a real one built on top of k8s.io/client-go informers:
// for every cluster a kubeconfig is loaded, a clientset is created and a shared
// pod informer (across all namespaces) feeds a work queue. Pop() blocks until an
// event is available, mirroring the semantics of the original robot, and
// HasSynced() only reports true once every cluster informer store is synced
// (the "must wait for all clusters ready" behaviour documented in the README).
package k8srobot

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
)

// ResourceType is the kind of Kubernetes resource watched by the robot.
type ResourceType string

// Pods is the only resource type consumed by this repository.
const Pods ResourceType = "pods"

// String implements fmt.Stringer.
func (r ResourceType) String() string { return string(r) }

// EventType is the kind of change that produced a queue object.
type EventType string

// The supported event types.
const (
	EventAdd    EventType = "add"
	EventUpdate EventType = "update"
	EventDelete EventType = "delete"
)

// String implements fmt.Stringer.
func (e EventType) String() string { return string(e) }

// RN describes a resource to watch inside a namespace ("").
// The fields are ordered so the positional literal
// client.RN{client.Pods, ""} keeps compiling.
type RN struct {
	Resource  ResourceType
	Namespace string
}

// Cluster describes one Kubernetes cluster of the multi-cluster set.
type Cluster struct {
	ConfigPath string
	Resources  []RN
}

// QueueObject is the unit of work handed out by Pop().
type QueueObject struct {
	RType    ResourceType
	Key      string // "<namespace>/<name>"
	Event    EventType
	CreateAt time.Time
}

// Robot is the multi-cluster watcher contract consumed by the providers.
type Robot interface {
	// Run starts every cluster informer. It blocks until Stop() is called.
	Run() error
	// Stop shuts the robot down. After Stop, Pop returns an error.
	Stop()
	// HasSynced reports whether every cluster store has been synced.
	HasSynced() bool
	// Pop blocks until an event is available or the robot is stopped.
	Pop() (QueueObject, error)
	// Finish acknowledges a previously popped object.
	Finish(obj QueueObject)
	// GetByKey returns the objects stored under "<namespace>/<name>".
	GetByKey(resource ResourceType, key string) ([]interface{}, bool)
	// List returns all objects of the given resource across all clusters.
	List(resource ResourceType) []interface{}
	// QueueDepth reports how many DISTINCT KEYS are currently queued — the
	// coalescing queue's depth, one entry per key regardless of how many
	// superseded events collapsed into it (dsca-1 DS-1-1 fix item 1, the
	// queueDepth gauge the k8s_queue_depth series reports). Read-only: safe
	// to call from any goroutine, including the metrics reporter's.
	QueueDepth() int
}

// queueSize bounds the coalescing queue's distinct-key capacity: the drop
// arm fires only when this many DISTINCT pod keys are in flight (dsca-1
// DS-1-1 fix item 2(a) — coalescing raises the effective capacity from
// 4,096 events to 4,096 keys, so a 4-event-per-pod rolling update now fits
// 4,096 pods in flight, not 1,024; the count is deliberately left at the
// contract's original number).
const queueSize = 4096

// dropLogEvery is the rate limit of the drop warning: the FIRST drop of a
// burst logs at Warn, then one line every 10k drops (dsca-1 DS-1-1 fix item
// 1: "log at rate (first drop per burst at Warn, then every 10k)") — a
// 20k-events/s burst must not spend the informer's producer goroutine
// formatting 20k warnings per second.
const dropLogEvery = 10000

// dropObserver is the process-wide observer notified on every queue-full
// drop, set through SetDropObserver (the injection seam of dsca-2 §6 row 8
// and dsca-1 DS-1-1 fix item 1). Package-level, not a Robot method,
// because the seam is deliberately minimal: k8srobot takes no metrics
// dependency, the provider wiring closes over the recorder.
//
// CONTRACT: set once, before Run. The atomic read path is race-safe for
// any later SetDropObserver call (so tests can swap observers), but the
// intended production pattern is a single set during wiring.
var dropObserver atomic.Value // func(cluster string)

// SetDropObserver installs the drop observer notified with the dropping
// cluster's identifier (the watcher's kubeconfig path) on every queue-full
// drop. Set it before Run; a nil observer disables notification (the drop
// counter simply does not count).
func SetDropObserver(observer func(cluster string)) {
	if observer == nil {
		dropObserver.Store((func(string))(nil))
		return
	}
	dropObserver.Store(observer)
}

// notifyDrop invokes the installed observer (if any) for one dropped event.
func notifyDrop(cluster string) {
	if observer, ok := dropObserver.Load().(func(string)); ok && observer != nil {
		observer(cluster)
	}
}

// droppedTotal counts drops since process start for the rate-limited
// warning (see dropLogEvery). It counts ALL clusters' drops — the
// per-cluster count is the metrics label's job, this one only decides when
// the next warning line is worth printing.
var droppedTotal uint64

// dropWarningRate returns whether this drop's ordinal warrants a warning
// line: the first drop, then every dropLogEvery-th. Call it exactly once
// per drop so the counter advances per event.
func dropWarningRate() bool {
	n := atomic.AddUint64(&droppedTotal, 1)
	return n == 1 || n%dropLogEvery == 0
}

// clusterWatcher bundles everything needed to watch one cluster.
type clusterWatcher struct {
	cluster  Cluster
	factory  informers.SharedInformerFactory
	informer cache.SharedIndexInformer
}

// robot is the default Robot implementation.
type robot struct {
	clusters []*clusterWatcher

	queue   *coalescingQueue
	done    chan struct{}
	stopMu  sync.Mutex
	stopped bool
}

// NewRobot validates every kubeconfig, builds the clientsets and the pod
// informers, and returns a ready-to-run Robot. A cluster whose kubeconfig
// cannot be loaded makes NewRobot fail, which matches the original robot's
// "must wait for all clusters ready" semantics (fail fast on configuration
// errors instead of silently ignoring a cluster).
func NewRobot(clusters []Cluster, debug bool) (Robot, error) {
	if len(clusters) == 0 {
		return nil, errors.New("k8srobot: no cluster configured")
	}
	r := &robot{
		queue: newCoalescingQueue(queueSize),
		done:  make(chan struct{}),
	}
	for _, c := range clusters {
		watcher, err := newClusterWatcher(c, r.queue)
		if err != nil {
			return nil, err
		}
		r.clusters = append(r.clusters, watcher)
	}
	return r, nil
}

// newClusterWatcher loads the kubeconfig and wires the pod informer event
// handlers of a single cluster. The produced events are pushed on the robot's
// shared coalescing queue.
func newClusterWatcher(c Cluster, queue *coalescingQueue) (*clusterWatcher, error) {
	if c.ConfigPath == "" {
		return nil, errors.New("k8srobot: empty kubeconfig path")
	}
	if _, err := os.Stat(c.ConfigPath); err != nil {
		return nil, fmt.Errorf("k8srobot: kubeconfig %s is not readable: %w", c.ConfigPath, err)
	}
	restConfig, err := clientcmd.BuildConfigFromFlags("", c.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("k8srobot: build rest config from %s failed: %w", c.ConfigPath, err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("k8srobot: create clientset from %s failed: %w", c.ConfigPath, err)
	}
	// No re-sync period: events are only produced by real cluster changes,
	// which is what the original robot did.
	factory := informers.NewSharedInformerFactory(clientset, 0)
	informer := factory.Core().V1().Pods().Informer()
	w := &clusterWatcher{
		cluster:  c,
		factory:  factory,
		informer: informer,
	}
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			w.enqueue(EventAdd, obj, queue)
		},
		UpdateFunc: func(_, newObj interface{}) {
			w.enqueue(EventUpdate, newObj, queue)
		},
		DeleteFunc: func(obj interface{}) {
			// Deleted objects may arrive wrapped in a cache.DeletedFinalStateUnknown.
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			w.enqueue(EventDelete, obj, queue)
		},
	})
	return w, nil
}

// enqueue converts an informer callback into a queued item. The queue is the
// KEYED COALESCING structure of dsca-1 DS-1-1 fix item 2(a) / DS-1-4: the
// map holds key→NEWEST event and the FIFO holds the keys, so a later event
// for a key already queued SUPERSEDES the queued one in place (no second
// entry, no re-ordering of the key's FIFO position) — the informer's store
// is always newer, and Pop's GetByKey reads the live store anyway, so only
// the latest event of a key is ever worth converting.
//
// The supersede keeps the EARLIEST CreateAt of the key's coalesced set (the
// e2e histogram's origin: dsca-2 §3's "a coalescing dequeue must keep the
// earliest CreateAt of the coalesced set, not the latest, or the metric
// under-reports queue wait" — the pending wait is the whole point of
// measuring it) and the LATEST event type (add→update→delete supersede in
// arrival order: an ADD then a DELETE for one key leaves a queued DELETE, so
// the pair can never be reordered into a register-after-deregister).
func (w *clusterWatcher) enqueue(event EventType, obj interface{}, queue *coalescingQueue) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return
	}
	item := QueueObject{
		RType:    Pods,
		Key:      pod.Namespace + "/" + pod.Name,
		Event:    event,
		CreateAt: time.Now(),
	}
	if dropped := queue.Offer(item); dropped {
		// The queue is full of DISTINCT KEYS: drop the event instead of
		// blocking the informer. The drop is OBSERVABLE (dsca-1 DS-1-1 fix
		// item 1, the unified spec with dsca-2 §6 row 8): the installed drop
		// observer counts it on the events_dropped_total series (per-cluster
		// label = this watcher's kubeconfig path, the only identity Cluster
		// carries today), and the warning is rate-limited (first drop per
		// burst, then every 10k) so a burst cannot turn the informer's
		// producer goroutine into a log formatter.
		//
		// RECOVERY (dsca-1 DS-1-1 fix item 3): the dropped key is recorded
		// by the queue and RE-OFFERED the moment capacity frees (the
		// targeted re-GetByKey scan the contract designs — Pop reads the
		// live informer store, so the key's newest state propagates without
		// waiting for the full-push interval), and ProcessIntervalFullPush
		// remains the unconditional backstop (60s demo / 6h default).
		// Coalescing makes this arm rare: it now takes 4,096 distinct pods
		// in flight, not 4,096 events.
		notifyDrop(w.cluster.ConfigPath)
		if dropWarningRate() {
			log.Printf("[WARN] k8srobot: queue full (%d distinct keys), dropped one event of cluster %s; the next full-push tick is the healer", queueSize, w.cluster.ConfigPath)
		}
	}
}

// Run starts all cluster informer factories and blocks until Stop.
func (r *robot) Run() error {
	for _, w := range r.clusters {
		w.factory.Start(r.done)
	}
	<-r.done
	return nil
}

// Stop terminates all informers and unblocks Run and Pop.
func (r *robot) Stop() {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	close(r.done)
}

// HasSynced reports whether every cluster informer store is synced.
func (r *robot) HasSynced() bool {
	for _, w := range r.clusters {
		if !w.informer.HasSynced() {
			return false
		}
	}
	return true
}

// Pop blocks until an event is available. It returns an error once the robot
// has been stopped. The single Pop consumer (the k8s monitor loop) is the
// queue's contract — see coalescingQueue.
func (r *robot) Pop() (QueueObject, error) {
	for {
		if obj, ok := r.queue.pop(); ok {
			return obj, nil
		}
		select {
		case <-r.queue.notify:
			// An Offer arrived (or a stale token): re-check under the lock.
			continue
		case <-r.done:
			// Drain the events that are already queued before giving up.
			if obj, ok := r.queue.pop(); ok {
				return obj, nil
			}
			return QueueObject{}, errors.New("k8srobot: the robot has been stopped")
		}
	}
}

// Finish acknowledges a popped object. The queue is coalescing and keyed, so
// there is no deferred work to perform; the method is kept to preserve the
// original robot's API.
func (r *robot) Finish(obj QueueObject) {
	_ = obj // no-op: nothing to acknowledge with a keyed coalescing queue
}

// QueueDepth reports the coalescing queue's distinct-key depth — the
// k8s_queue_depth gauge (dsca-1 DS-1-1 fix item 1: "plus a queueDepth
// gauge"). It is a read of a mutex-guarded map length, safe from any
// goroutine.
func (r *robot) QueueDepth() int {
	return r.queue.Depth()
}

// GetByKey returns the objects stored under "<namespace>/<name>" in every
// cluster store.
func (r *robot) GetByKey(resource ResourceType, key string) ([]interface{}, bool) {
	if resource != Pods {
		return nil, false
	}
	var items []interface{}
	for _, w := range r.clusters {
		if obj, exists, err := w.informer.GetIndexer().GetByKey(key); err == nil && exists {
			items = append(items, obj)
		}
	}
	if len(items) == 0 {
		return nil, false
	}
	return items, true
}

// List returns all pods of every cluster store.
func (r *robot) List(resource ResourceType) []interface{} {
	if resource != Pods {
		return nil
	}
	var items []interface{}
	for _, w := range r.clusters {
		items = append(items, w.informer.GetIndexer().List()...)
	}
	return items
}

// coalescingQueue is the keyed coalescing work queue of dsca-1 DS-1-1 fix
// item 2(a) and DS-1-4 ("a keyed map + FIFO of keys, exactly the shape the
// informer's store makes cheap"): a map of key→newest QueueObject plus a
// FIFO of the keys currently queued.
//
// OFFERING: an event whose key is not queued takes a FIFO slot (bounded by
// capacity); an event whose key is already queued REPLACES the stored
// object in place — the key's FIFO position is untouched, so per-key FIFO
// ordering is preserved across coalescing (the add→delete pair of one pod
// pops as ONE delete, never as a reordered delete-then-add or as two
// entries).
//
// The replacement keeps the EARLIEST CreateAt and the LATEST event type of
// the coalesced set (see enqueue). With this shape the drop arm fires only
// when `capacity` DISTINCT KEYS are in flight — a rolling update's 4 events
// per pod collapse to at most 2 keys' worth of queue entries (dsca-1's
// corrected arithmetic: ~2x, not 3x), which removes the drop cliff at the
// 3,000-instance tier without touching the sink.
//
// POPPING: Pop is the dequeuer — one entry leaves the map and the FIFO
// atomically under the queue's lock, and Pop blocks on the queue's notify
// channel (a one-token wakeup Offer signals) exactly like the old channel
// send blocked. The contract is the ORIGINAL one: a SINGLE Pop consumer
// (the k8s monitor loop's one goroutine, k8s.go:104-130). Two concurrent
// Pop callers could race a wakeup token between them; the production shape
// has exactly one, and the previous channel-based implementation is
// superseded by this structure precisely because its capacity counted
// events, not keys.
type coalescingQueue struct {
	mu     sync.Mutex
	notify chan struct{} // one-token wakeup, signaled by Offer
	items  map[string]*queuedItem
	order  *list.List // of *queuedItem, in arrival order of the keys
	// recovering holds the QueueObjects dropped by a full queue, FIFO — the
	// DS-1-1 fix item 3 recovery scan's record of dropped keys. Bounded at
	// `capacity` entries (see recoveryCapacity); entries past the bound fall
	// off and heal only at the next full-push tick (today's behavior).
	recovering []QueueObject
	capacity   int
}

// queuedItem is one key's coalesced entry: the FIFO element and the newest
// event of the key (with the earliest CreateAt of its coalesced set).
type queuedItem struct {
	key   string
	event QueueObject
}

// newCoalescingQueue builds the queue with the given distinct-key capacity.
func newCoalescingQueue(capacity int) *coalescingQueue {
	return &coalescingQueue{
		notify:   make(chan struct{}, 1),
		items:    make(map[string]*queuedItem, capacity),
		order:    list.New(),
		capacity: capacity,
	}
}

// Offer inserts or coalesces one item. It returns true when the item was
// DROPPED (the distinct-key capacity is exhausted — the caller's drop arm
// observes and counts). Offer never blocks: the enqueue path runs on the
// informer's producer goroutine and must never wait.
//
// A dropped item is recorded in the recovery buffer (the DS-1-1 fix item 3
// scan's key record): it is re-offered the moment capacity frees, so a
// dropped event that was the key's LAST change — the exact case where
// coalescing's "a later event supersedes" reasoning fails, because no later
// event exists — still reaches Pop without waiting for the full-push tick.
//
// The coalesced entry keeps the LATEST event and the EARLIEST CreateAt of
// the key's coalesced set: the event is the state worth converting at Pop
// time (the informer's store is strictly newer than anything queued
// earlier), while the CreateAt is the e2e histogram's origin — dsca-2 §3's
// stated constraint that "a coalescing dequeue must keep the earliest
// CreateAt of the coalesced set, not the latest, or the metric
// under-reports queue wait".
func (q *coalescingQueue) Offer(item QueueObject) bool {
	q.mu.Lock()
	if existing, ok := q.items[item.Key]; ok {
		// Supersede in place: the newest event wins, the FIFO position of
		// the key is untouched, and the earliest CreateAt is kept.
		if existing.event.CreateAt.Before(item.CreateAt) {
			item.CreateAt = existing.event.CreateAt
		}
		existing.event = item
		q.mu.Unlock()
		return false
	}
	if len(q.items) >= q.capacity {
		// Record the dropped key for the recovery scan (bounded: past
		// capacity entries the tick heals, as it always did).
		if len(q.recovering) < q.capacity {
			q.recovering = append(q.recovering, item)
		}
		q.mu.Unlock()
		return true // drop: capacity many DISTINCT KEYS are in flight
	}
	q.insert(item)
	q.mu.Unlock()
	// Wake the consumer (non-blocking: a stale token keeps it awake anyway).
	select {
	case q.notify <- struct{}{}:
	default:
	}
	return false
}

// insert places a NEW key's entry at the FIFO tail. Callers hold q.mu.
func (q *coalescingQueue) insert(item QueueObject) {
	entry := &queuedItem{key: item.Key, event: item}
	q.order.PushBack(entry)
	q.items[item.Key] = entry
}

// pop dequeues the OLDEST queued key's coalesced event — removed from the
// map and the FIFO atomically under the lock — and, because a FIFO slot
// just freed, re-admits recovery-recorded dropped keys while there is room:
// the DS-1-1 fix item 3 "immediate targeted re-GetByKey scan for those
// keys" — each re-admitted entry's Pop performs GetByKey(key) on the live
// informer store, so the key's newest state propagates without waiting for
// the full-push interval. A recovery entry whose key is already queued
// again (a later event admitted it) is skipped: the queued event is at
// least as new. Recovery re-admission never fires the drop observer (it is
// a recovery, not a loss).
func (q *coalescingQueue) pop() (QueueObject, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	front := q.order.Front()
	if front == nil {
		return QueueObject{}, false
	}
	entry := front.Value.(*queuedItem)
	q.order.Remove(front)
	delete(q.items, entry.key)
	for len(q.recovering) > 0 && len(q.items) < q.capacity {
		item := q.recovering[0]
		q.recovering = q.recovering[1:]
		if _, queued := q.items[item.Key]; queued {
			continue // a later event of the key is already queued: skip
		}
		q.insert(item)
	}
	return entry.event, true
}

// Depth reports the current distinct-key depth: entries queued and not yet
// popped. It is the number the k8s_queue_depth gauge reports.
func (q *coalescingQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}
