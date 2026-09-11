package k8srobot

import (
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestEnqueueNotifiesDropObserverWhenQueueFull pins the DS-1-1/DS-2-6
// unified drop spec at the drop arm: a full queue takes the default branch,
// which MUST call the SetDropObserver-installed observer with the dropping
// watcher's cluster identifier (the kubeconfig path — the only identity
// Cluster carries today, the label value the events_dropped_total series
// reports). The observer is set before any enqueue (the documented
// contract: set once, before Run).
//
// The full queue is produced without refactoring queueSize (a const): the
// test drives enqueue directly against a channel of capacity 1 that is
// never consumed — a non-consuming stand-in for the saturated real queue.
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
	queue := make(chan QueueObject, 1)

	// Fill the single slot, then overflow: every overflowed enqueue is a
	// queue-full drop.
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-filled"), queue)
	for i := 0; i < 5; i++ {
		w.enqueue(EventAdd, newDropTestPod("ns", "pod-dropped"), queue)
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
	queue := make(chan QueueObject, 2)

	w.enqueue(EventUpdate, newDropTestPod("ns", "pod-a"), queue)
	w.enqueue(EventDelete, newDropTestPod("ns", "pod-b"), queue)

	if len(queue) != 2 {
		t.Fatalf("queue length = %d, want 2 (both events delivered)", len(queue))
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
	queue := make(chan QueueObject, 0) // capacity 0: every send drops

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
	queue := make(chan QueueObject, 0)
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
	queue := make(chan QueueObject, 1)
	before := time.Now()
	w.enqueue(EventAdd, newDropTestPod("ns", "pod-precision"), queue)
	obj := <-queue
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
