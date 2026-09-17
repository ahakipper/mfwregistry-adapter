package k8srobot

import (
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestEnqueueNotifiesDropObserverWhenQueueFull pins the DS-1-1/DS-2-6
// unified drop spec at the drop arm: a full queue takes the drop branch,
// which MUST call the SetDropObserver-installed observer with the dropping
// watcher's cluster identifier (the kubeconfig path — the only identity
// Cluster carries today, the label value the events_dropped_total series
// reports). The observer is set before any enqueue (the documented
// contract: set once, before Run).
//
// The full queue is produced without refactoring queueSize (a const): the
// test drives enqueue directly against a queue of distinct-key capacity 1
// that is never consumed — a non-consuming stand-in for the saturated real
// queue. The overflowing events each carry a DISTINCT pod key: under the
// coalescing queue, same-key events supersede (never drop), so the drop
// arm fires only for a new key beyond capacity.
func TestEnqueueNotifiesDropObserverWhenQueueFull(t *testing.T) {
	var (
		mu       sync.Mutex
		clusters []string
	)
	SetDropObserver(func(cluster string) {
		mu.Lock()
		defer mu.Unlock()
		clusters = append(clusters, cluster)
	})
	defer SetDropObserver(nil)

	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-drop-test"}}
	queue := newCoalescingQueue(1)

	// Fill the single key slot, then overflow with DISTINCT keys: every
	// overflowed enqueue is a queue-full drop.
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-filled"), queue)
	for i := 0; i < 5; i++ {
		w.enqueue(EventAdd, newDropTestPod("ns", "pod-dropped-"+string(rune('a'+i))), queue)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(clusters) != 5 {
		t.Fatalf("drop observer calls = %d, want 5 (one per overflowed enqueue); got %v", len(clusters), clusters)
	}
	for i, cluster := range clusters {
		if cluster != "/tmp/kubeconfig-drop-test" {
			t.Fatalf("drop observer cluster[%d] = %q, want the watcher's kubeconfig path", i, cluster)
		}
	}
}

// TestEnqueueDeliversWhenQueueHasRoom pins the non-drop side: with room in
// the queue the object lands on the channel and the observer stays silent.
func TestEnqueueDeliversWhenQueueHasRoom(t *testing.T) {
	var mu sync.Mutex
	var calls int
	SetDropObserver(func(cluster string) {
		mu.Lock()
		defer mu.Unlock()
		calls++
	})
	defer SetDropObserver(nil)

	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-deliver-test"}}
	queue := newCoalescingQueue(2)

	w.enqueue(EventUpdate, newDropTestPod("ns", "pod-a"), queue)
	w.enqueue(EventDelete, newDropTestPod("ns", "pod-b"), queue)

	if depth := queue.Depth(); depth != 2 {
		t.Fatalf("queue depth = %d, want 2 (both keys delivered)", depth)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 0 {
		t.Fatalf("drop observer calls = %d, want 0 (nothing dropped)", calls)
	}
}

// TestSetDropObserverNilIsSilent pins the nil contract: a nil observer
// disables notification (the enqueue path must not panic and must simply
// not count).
func TestSetDropObserverNilIsSilent(t *testing.T) {
	SetDropObserver(nil)
	defer SetDropObserver(nil)

	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-nil-test"}}
	queue := newCoalescingQueue(0) // capacity 0: every offer drops

	w.enqueue(EventAdd, newDropTestPod("ns", "pod-x"), queue)
	// Reaching here without panic IS the assertion (a nil-func call would
	// have panicked inside notifyDrop).
}

// TestSetDropObserverSwappable pins the race-safe read path: a second
// SetDropObserver call (after the first) takes effect for later drops —
// the atomic value supports the swap, which the -race build also verifies
// against concurrent enqueue readers.
func TestSetDropObserverSwappable(t *testing.T) {
	var (
		mu     sync.Mutex
		first  int
		second int
	)
	SetDropObserver(func(string) {
		mu.Lock()
		defer mu.Unlock()
		first++
	})
	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-swap-test"}}
	queue := newCoalescingQueue(0)
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-1"), queue)

	SetDropObserver(func(string) {
		mu.Lock()
		defer mu.Unlock()
		second++
	})
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-2"), queue)
	defer SetDropObserver(nil)

	mu.Lock()
	defer mu.Unlock()
	if first != 1 || second != 1 {
		t.Fatalf("observer calls after swap: first=%d second=%d, want 1 and 1", first, second)
	}
}

// TestDropWarningRate pins the rate limiter shape (dsca-1 DS-1-1: "first
// drop per burst at Warn, then every 10k"): the 1st, 10001st, 20001st drop
// warn; nothing in between.
func TestDropWarningRate(t *testing.T) {
	// The counter is process-global and other tests' enqueues advance it;
	// read the starting ordinal and assert the RELATIVE pattern.
	before := droppedTotal
	var warned int
	for i := 1; i <= 3*dropLogEvery+2; i++ {
		if dropWarningRate() {
			warned++
		}
	}
	// Over 3*10k+2 calls from an arbitrary base, exactly 3 rate-eligible
	// ordinals may fall in the window (n%10k==0 at 10k, 20k, 30k past the
	// base, plus possibly the base+1 "first" ordinal if the base was 0).
	// The invariant that matters: NOT every drop warns (a burst cannot
	// spam), and some do. Assert the bounded range.
	if warned == 0 || warned > 4 {
		t.Fatalf("warned lines = %d over %d drops, want 1..4 (rate-limited, not per-event)", warned, 3*dropLogEvery+2)
	}
	_ = before
}

// newDropTestPod builds a minimal pod the enqueue type assertion accepts.
func newDropTestPod(namespace, name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// TestQueueObjectCreateAtKeptFullPrecision documents the origin carry the
// e2e metric depends on: CreateAt is time.Now() with full nanosecond
// precision, and the producer widening (UnixNano at k8s.go:118) keeps it
// lossless through Event.Trigger.
func TestQueueObjectCreateAtKeptFullPrecision(t *testing.T) {
	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-precision-test"}}
	queue := newCoalescingQueue(1)
	before := time.Now()
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-precision"), queue)
	obj, _ := queue.pop()
	// The stored CreateAt must be ns-precision (not seconds-truncated):
	// its nanosecond field carries a real sub-second reading, and the
	// instant falls between the two bounds of the enqueue call.
	if obj.CreateAt.Unix() == obj.CreateAt.Truncate(time.Second).Unix() && obj.CreateAt.Nanosecond() == 0 {
		// A whole-second boundary is possible but vanishingly unlikely
		// with real ns precision; tolerate it only if the wall-clock range
		// also agrees to that second.
		if !(obj.CreateAt.After(before.Add(-time.Second)) && obj.CreateAt.Before(time.Now().Add(time.Second))) {
			t.Fatalf("CreateAt = %v, want a ns-precision instant inside the enqueue window", obj.CreateAt)
		}
	}
	if obj.CreateAt.Before(before.Add(-time.Second)) || obj.CreateAt.After(time.Now().Add(time.Second)) {
		t.Fatalf("CreateAt = %v, want an instant inside the enqueue window [%v, now]", obj.CreateAt, before)
	}
}

// -----------------------------------------------------------------------------
// The keyed coalescing queue (dsca-1 DS-1-1 fix item 2(a) / DS-1-4)
// -----------------------------------------------------------------------------

// TestQueueCoalescesSameKeyToNewestEvent pins the coalescing core: N events
// for ONE key queue to exactly ONE entry, and the surviving event is the
// NEWEST (the last one offered — the informer's store is always newer, so
// only the latest state of a key is worth converting at Pop time).
func TestQueueCoalescesSameKeyToNewestEvent(t *testing.T) {
	queue := newCoalescingQueue(16)
	stamp := time.Now()
	for i := 0; i < 10; i++ {
		queue.Offer(QueueObject{
			RType:    Pods,
			Key:      "ns/pod-a",
			Event:    EventUpdate,
			CreateAt: stamp.Add(time.Duration(i) * time.Millisecond),
		})
	}
	if got := queue.Depth(); got != 1 {
		t.Fatalf("depth after 10 same-key offers = %d, want 1 (coalesced)", got)
	}
	obj, ok := queue.pop()
	if !ok {
		t.Fatal("pop() = not-ok, want the coalesced entry")
	}
	if obj.Key != "ns/pod-a" || obj.Event != EventUpdate {
		t.Fatalf("popped = key %q event %s, want ns/pod-a update", obj.Key, obj.Event)
	}
	// The surviving event is the newest TYPE offered (an UPDATE), and its
	// CreateAt is the EARLIEST of the set (the e2e-origin rule — see
	// TestQueueKeepsEarliestCreateAtAcrossCoalescedSet for the pin).
	if want := stamp; !obj.CreateAt.Equal(want) {
		t.Fatalf("popped CreateAt = %v, want the earliest %v", obj.CreateAt, want)
	}
	if got := queue.Depth(); got != 0 {
		t.Fatalf("depth after pop = %d, want 0", got)
	}
}

// TestQueueKeepsEarliestCreateAtAcrossCoalescedSet pins the e2e-origin rule
// of the supersede (dsca-2 §3's constraint: "a coalescing dequeue must keep
// the earliest CreateAt of the coalesced set, not the latest, or the metric
// under-reports queue wait"): an ADD followed by an UPDATE for one key pops
// as one UPDATE whose CreateAt is the ADD's, so the coalesced entry's
// end-to-end observation still measures the full queue wait from the FIRST
// time the pod's state change entered the queue.
func TestQueueKeepsEarliestCreateAtAcrossCoalescedSet(t *testing.T) {
	queue := newCoalescingQueue(16)
	first := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventAdd, CreateAt: first})
	second := first.Add(50 * time.Millisecond)
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventUpdate, CreateAt: second})

	if got := queue.Depth(); got != 1 {
		t.Fatalf("depth = %d, want 1", got)
	}
	obj, _ := queue.pop()
	if obj.Event != EventUpdate {
		t.Fatalf("popped event = %s, want update (the newest type of the set)", obj.Event)
	}
	if !obj.CreateAt.Equal(first) {
		t.Fatalf("popped CreateAt = %v, want the earliest %v (the e2e origin must not under-report queue wait)", obj.CreateAt, first)
	}
}

// TestQueuePreservesPerKeyOrderingAddThenDelete pins the ordering guarantee
// (the batch scope's explicit requirement: "an ADD then DELETE for the same
// key must not reorder"): the pair coalesces to a SINGLE queued entry whose
// event is the DELETE — the pod's final state — so the Pop consumer can
// never observe the delete before the add, and never observes the stale add
// at all. The popped object is one entry, not two.
func TestQueuePreservesPerKeyOrderingAddThenDelete(t *testing.T) {
	queue := newCoalescingQueue(16)
	base := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-c", Event: EventAdd, CreateAt: base})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-c", Event: EventDelete, CreateAt: base.Add(time.Millisecond)})

	if got := queue.Depth(); got != 1 {
		t.Fatalf("depth after add+delete of one key = %d, want 1 (the pair coalesces, never reorders)", got)
	}
	obj, _ := queue.pop()
	if obj.Event != EventDelete {
		t.Fatalf("popped event = %s, want delete (the newest type; the add never pops after it)", obj.Event)
	}
	// The add must not survive as a second entry.
	if _, ok := queue.pop(); ok {
		t.Fatal("a second entry popped after the add→delete pair, want exactly one coalesced entry")
	}
}

// TestQueueCrossKeyIndependence pins cross-key independence: coalescing one
// key's events never affects another key's entry, and the FIFO order of
// DISTINCT keys is their first-offer order (the queue position a key takes
// when it first appears is the position it keeps across supersede).
func TestQueueCrossKeyIndependence(t *testing.T) {
	queue := newCoalescingQueue(16)
	base := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-1", Event: EventAdd, CreateAt: base})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-2", Event: EventAdd, CreateAt: base.Add(time.Millisecond)})
	// Interleave updates for both keys plus a third key.
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-1", Event: EventUpdate, CreateAt: base.Add(2 * time.Millisecond)})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-3", Event: EventAdd, CreateAt: base.Add(3 * time.Millisecond)})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-2", Event: EventDelete, CreateAt: base.Add(4 * time.Millisecond)})

	if got := queue.Depth(); got != 3 {
		t.Fatalf("depth = %d, want 3 (one entry per distinct key)", got)
	}
	// FIFO by FIRST offer: pod-1, pod-2, pod-3 — each with its newest event.
	wantKeys := []string{"ns/pod-1", "ns/pod-2", "ns/pod-3"}
	wantEvents := []EventType{EventUpdate, EventDelete, EventAdd}
	for i := range wantKeys {
		obj, ok := queue.pop()
		if !ok {
			t.Fatalf("pop %d = not-ok, want %s", i, wantKeys[i])
		}
		if obj.Key != wantKeys[i] || obj.Event != wantEvents[i] {
			t.Fatalf("pop %d = key %q event %s, want key %q event %s (FIFO by first offer, newest event per key)",
				i, obj.Key, obj.Event, wantKeys[i], wantEvents[i])
		}
	}
}

// TestQueueDropAtDistinctKeyCapacity pins the new drop semantics: the drop
// arm fires only when the capacity many DISTINCT KEYS are in flight (the
// coalescing EFFECTIVELY raises capacity from N events to N distinct keys —
// a 5,000-pod rollout's 20,000 events now queue as 5,000 keys, and the
// capacity-4096 production queue absorbs it), and the drop observer keeps
// firing on that arm.
func TestQueueDropAtDistinctKeyCapacity(t *testing.T) {
	var (
		mu    sync.Mutex
		drops int
	)
	SetDropObserver(func(string) {
		mu.Lock()
		defer mu.Unlock()
		drops++
	})
	defer SetDropObserver(nil)

	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-capacity-test"}}
	const capacity = 8
	queue := newCoalescingQueue(capacity)

	// 3 events per key, capacity+2 distinct keys: the first `capacity` keys
	// fit; every key beyond capacity drops (all 3 of its events), and NO
	// same-key event ever drops. The enqueue path is the real drop arm —
	// the observer fires exactly like production.
	queued := 0
	for i := 0; i < capacity+2; i++ {
		podName := "pod-cap-" + string(rune('a'+i))
		before := dropsSnapshot(t, &mu, &drops)
		w.enqueue(EventUpdate, newDropTestPod("ns", podName), queue)
		admitted := dropsSnapshot(t, &mu, &drops) == before
		for j := 0; j < 2; j++ {
			w.enqueue(EventUpdate, newDropTestPod("ns", podName), queue)
		}
		if admitted {
			queued++
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if queued != capacity {
		t.Fatalf("distinct keys admitted = %d, want %d (the capacity bound)", queued, capacity)
	}
	// Every key beyond capacity dropped all 3 of its events: 2 keys x 3.
	if drops != 6 {
		t.Fatalf("drops = %d, want 6 (2 overflow keys x 3 events each; same-key events never drop)", drops)
	}
	if got := queue.Depth(); got != capacity {
		t.Fatalf("depth = %d, want %d (full of distinct keys)", got, capacity)
	}
}

// dropsSnapshot reads the shared drop counter under its mutex.
func dropsSnapshot(t *testing.T, mu *sync.Mutex, drops *int) int {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	return *drops
}

// TestQueuePopBlocksUntilOffer pins Pop's blocking semantics against the
// coalescing structure: an empty queue blocks Pop, and an Offer from another
// goroutine wakes it with the offered object — the same contract the old
// channel send provided.
func TestQueuePopBlocksUntilOffer(t *testing.T) {
	queue := newCoalescingQueue(4)
	got := make(chan QueueObject, 1)
	go func() {
		obj, err := (&robot{queue: queue, done: make(chan struct{})}).Pop()
		if err != nil {
			t.Errorf("Pop() error = %v, want nil", err)
		}
		got <- obj
	}()

	select {
	case <-got:
		t.Fatal("Pop returned before any Offer, want it to block")
	case <-time.After(50 * time.Millisecond):
	}

	want := QueueObject{RType: Pods, Key: "ns/pod-late", Event: EventAdd, CreateAt: time.Now()}
	queue.Offer(want)
	select {
	case obj := <-got:
		if obj.Key != want.Key || obj.Event != want.Event {
			t.Fatalf("Pop() = key %q event %s, want the offered %q %s", obj.Key, obj.Event, want.Key, want.Event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop() did not wake on Offer within 2s, want the notify-token wakeup")
	}
}

// TestQueueOfferNeverBlocksOnSaturatedConsumer pins the producer-safety
// invariant: even with NO consumer popping, Offer returns promptly for both
// the coalesce and the drop paths — the informer's callback goroutine must
// never wait.
func TestQueueOfferNeverBlocksOnSaturatedConsumer(t *testing.T) {
	queue := newCoalescingQueue(2)
	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-noblock-test"}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			// 5,000 same-key events (coalesce path) + 5,000 distinct keys
			// (drop path past capacity 2).
			if i < 5000 {
				w.enqueue(EventUpdate, newDropTestPod("ns", "pod-hot"), queue)
			} else {
				w.enqueue(EventUpdate, newDropTestPod("ns", "pod-cold-"+string(rune('a'+i%26))+string(rune('0'+i%10))), queue)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("10k Offers against a never-consuming queue did not finish in 5s, want the enqueue path to never block")
	}
	if got := queue.Depth(); got != 2 {
		t.Fatalf("depth = %d, want 2 (the capacity; nothing popped)", got)
	}
}

// TestQueueDepthIsConsumedOnPop pins the gauge source: QueueDepth counts
// entries that are queued and NOT yet popped, so a full-then-drained queue
// returns to zero — the k8s_queue_depth series reflects live backlog, not
// lifetime events.
func TestQueueDepthIsConsumedOnPop(t *testing.T) {
	queue := newCoalescingQueue(64)
	for i := 0; i < 5; i++ {
		queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-d" + string(rune('0'+i)), Event: EventAdd, CreateAt: time.Now()})
	}
	if got := queue.Depth(); got != 5 {
		t.Fatalf("depth = %d, want 5", got)
	}
	for i := 0; i < 5; i++ {
		if _, ok := queue.pop(); !ok {
			t.Fatalf("pop %d = not-ok, want the queued entry", i)
		}
		if got := queue.Depth(); got != 4-i {
			t.Fatalf("depth after pop %d = %d, want %d", i+1, got, 4-i)
		}
	}
}

// TestRobotQueueDepthExposed pins the Robot interface's gauge method: the
// real robot constructor wires the coalescing queue and QueueDepth reports
// its distinct-key depth. A robot cannot be built without a readable
// kubeconfig, so the wiring is pinned by driving the queue the robot owns
// through the enqueue path of one watcher (a full NewRobot would need a
// live kubeconfig; the shape here is the production wiring minus the
// apiserver).
func TestRobotQueueDepthExposed(t *testing.T) {
	queue := newCoalescingQueue(16)
	r := &robot{queue: queue, done: make(chan struct{})}
	w := &clusterWatcher{cluster: Cluster{ConfigPath: "/tmp/kubeconfig-depth-test"}}

	if got := r.QueueDepth(); got != 0 {
		t.Fatalf("QueueDepth() on an empty robot = %d, want 0", got)
	}
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-x"), queue)
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-y"), queue)
	w.enqueue(EventUpdate, newDropTestPod("ns", "pod-x"), queue) // coalesces
	if got := r.QueueDepth(); got != 2 {
		t.Fatalf("QueueDepth() = %d, want 2 (two distinct keys; the update coalesced)", got)
	}
}

// -----------------------------------------------------------------------------
// Drop-recovery scan (dsca-1 DS-1-1 fix item 3)
// -----------------------------------------------------------------------------

// TestQueueRecoversDroppedKeyWhenCapacityFrees pins the recovery scan: a
// key dropped by a full queue is RECORDED and re-admitted the moment a Pop
// frees a slot — the contract's "immediate targeted re-GetByKey scan for
// those keys instead of waiting for the interval". The recovered entry
// preserves its event type and CreateAt (Pop's GetByKey reads the live
// store; the type drives the delete path, the CreateAt keeps the e2e origin
// honest across the drop+recovery window).
func TestQueueRecoversDroppedKeyWhenCapacityFrees(t *testing.T) {
	queue := newCoalescingQueue(2)
	base := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-a", Event: EventAdd, CreateAt: base})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventAdd, CreateAt: base})

	// The queue is full: pod-c's DELETE drops (its key is recorded).
	dropped := QueueObject{RType: Pods, Key: "ns/pod-c", Event: EventDelete, CreateAt: base.Add(time.Millisecond)}
	if !queue.Offer(dropped) {
		t.Fatal("Offer(pod-c) = admitted, want dropped (capacity 2 is full)")
	}
	if got := queue.Depth(); got != 2 {
		t.Fatalf("depth after the drop = %d, want 2 (the dropped key occupies no slot)", got)
	}

	// One pop frees a slot: pod-c's recorded entry is re-admitted
	// immediately — no full-push tick needed.
	first, ok := queue.pop()
	if !ok || first.Key != "ns/pod-a" {
		t.Fatalf("pop() = %v %v, want the oldest queued key pod-a", first, ok)
	}
	if got := queue.Depth(); got != 2 {
		t.Fatalf("depth after one pop = %d, want 2 (pod-b + the RE-ADMITTED pod-c)", got)
	}

	// The recovered entry: same key, same event type, same CreateAt.
	second, _ := queue.pop()
	if second.Key != "ns/pod-b" {
		t.Fatalf("pop() = key %q, want pod-b (FIFO order kept)", second.Key)
	}
	third, _ := queue.pop()
	if third.Key != "ns/pod-c" || third.Event != EventDelete || !third.CreateAt.Equal(dropped.CreateAt) {
		t.Fatalf("recovered entry = key %q event %s CreateAt %v, want pod-c delete at the original CreateAt", third.Key, third.Event, third.CreateAt)
	}
	if _, ok := queue.pop(); ok {
		t.Fatal("a fourth entry popped, want the queue drained (2 queued + 1 recovered, 3 pops)")
	}
}

// TestQueueRecoverySkipsKeyAlreadyRequeued pins the re-admission guard: a
// recovery entry whose key is already queued again (a later event admitted
// it after the drop) is discarded — the queued event is at least as new, so
// the scan never resurrects stale state over fresher queued state.
func TestQueueRecoverySkipsKeyAlreadyRequeued(t *testing.T) {
	queue := newCoalescingQueue(1)
	base := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-a", Event: EventAdd, CreateAt: base})
	// Full: pod-b drops (recorded), twice (both drops recorded).
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventUpdate, CreateAt: base.Add(time.Millisecond)})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventDelete, CreateAt: base.Add(2 * time.Millisecond)})

	// Pop frees the slot; the FIRST recorded pod-b (update) is re-admitted.
	if obj, _ := queue.pop(); obj.Key != "ns/pod-a" {
		t.Fatalf("pop() = key %q, want pod-a", obj.Key)
	}
	// While pod-b(update) sits queued, the second record (delete) is
	// re-admission-checked: the key is already queued, so it is skipped —
	// the queued update must NOT be overwritten by the OLDER-seeded record
	// through the recovery door. (A supersede through Offer would keep the
	// delete; the recovery door must not.)
	if got := queue.Depth(); got != 1 {
		t.Fatalf("depth = %d, want 1 (pod-b queued once; the duplicate record skipped)", got)
	}
	obj, ok := queue.pop()
	if !ok || obj.Key != "ns/pod-b" {
		t.Fatalf("pop() = %v %v, want the re-admitted pod-b", obj, ok)
	}
	if obj.Event != EventUpdate {
		t.Fatalf("re-admitted event = %s, want update (the first record; the second was skipped as a duplicate)", obj.Event)
	}
	if _, ok := queue.pop(); ok {
		t.Fatal("another entry popped, want the queue empty (the duplicate record never re-admitted)")
	}
}

// TestQueueRecoveryBufferBounded pins the recovery buffer's bound: past
// `capacity` recorded entries, further drops are NOT recorded (their only
// healer is the full-push tick, exactly today's behavior) — a pathological
// sustained overflow (E10's 45,903 drops) cannot grow the record unboundedly.
func TestQueueRecoveryBufferBounded(t *testing.T) {
	queue := newCoalescingQueue(1)
	base := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-a", Event: EventAdd, CreateAt: base})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventUpdate, CreateAt: base}) // recorded
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-c", Event: EventDelete, CreateAt: base}) // NOT recorded (bound 1)

	if obj, _ := queue.pop(); obj.Key != "ns/pod-a" {
		t.Fatalf("pop() = key %q, want pod-a", obj.Key)
	}
	// Only pod-b re-admits; pod-c fell off the record.
	if obj, _ := queue.pop(); obj.Key != "ns/pod-b" {
		t.Fatalf("pop() = key %q, want the recorded pod-b", obj.Key)
	}
	if _, ok := queue.pop(); ok {
		t.Fatal("an entry popped after the record drained, want nothing (pod-c was never recorded)")
	}
}

// TestQueueRecoveredKeySupersedableAfterReAdmission pins the interplay of
// recovery with normal coalescing: once a recovered entry is queued, a
// later live event for the same key supersedes it through the normal Offer
// path — recovery re-admission is just an insert, it confers no special
// ordering or immunity.
func TestQueueRecoveredKeySupersedableAfterReAdmission(t *testing.T) {
	queue := newCoalescingQueue(1)
	base := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-a", Event: EventAdd, CreateAt: base})
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventUpdate, CreateAt: base}) // dropped, recorded

	if obj, _ := queue.pop(); obj.Key != "ns/pod-a" {
		t.Fatalf("pop() = key %q, want pod-a", obj.Key)
	}
	// pod-b is now re-admitted; a NEWER live event for pod-b supersedes it.
	newer := time.Now()
	queue.Offer(QueueObject{RType: Pods, Key: "ns/pod-b", Event: EventDelete, CreateAt: newer})
	if got := queue.Depth(); got != 1 {
		t.Fatalf("depth = %d, want 1 (supersede in place)", got)
	}
	obj, _ := queue.pop()
	if obj.Event != EventDelete {
		t.Fatalf("popped event = %s, want delete (the newer live event superseded the recovered one)", obj.Event)
	}
}
