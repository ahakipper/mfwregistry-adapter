package providers

import (
	"context"
	"sync"
	"time"
)

// OverflowQueue is a bounded, identity-keyed requeue for provider work that
// could not be submitted to a non-blocking worker pool.  There is at most one
// pending task per identity; a newer task replaces an older task for the same
// identity.  This is important for watch streams where replaying every
// intermediate update would only add stale work and pressure the sink again.
//
// submit must be non-blocking (ants.Submit with WithNonblocking is the
// production implementation).  Tasks remain in the queue until submit
// succeeds, so a transient pool saturation cannot silently lose an event.
type OverflowQueue struct {
	mu       sync.Mutex
	entries  map[string]overflowEntry
	order    []string
	max      int
	submit   func(func()) error
	onDrop   func(string)
	retry    time.Duration
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	closed   bool
	ctx      context.Context
}

type overflowEntry struct {
	task       func()
	generation uint64
	inflight   bool
}

const defaultOverflowRetry = 20 * time.Millisecond

// NewOverflowQueue creates and starts an overflow dispatcher. max is the
// number of distinct identities retained; non-positive values use a bounded
// default rather than allowing an accidental unbounded queue.
func NewOverflowQueue(ctx context.Context, max int, submit func(func()) error, onDrop func(string)) *OverflowQueue {
	if ctx == nil {
		ctx = context.Background()
	}
	if max <= 0 {
		max = PoolBenchSize * 4
	}
	q := &OverflowQueue{
		entries: make(map[string]overflowEntry),
		max:     max,
		submit:  submit,
		onDrop:  onDrop,
		retry:   defaultOverflowRetry,
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		ctx:     ctx,
	}
	go q.run()
	return q
}

// Offer retains the latest task for key. It returns false only when key is a
// new identity and the bounded queue is full (or the queue is closed). Empty
// keys are still accepted under a stable sentinel so malformed legacy models
// remain observable instead of being discarded accidentally.
func (q *OverflowQueue) Offer(key string, task func()) bool {
	if task == nil {
		return false
	}
	if key == "" {
		key = "<empty-identity>"
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	if existing, ok := q.entries[key]; ok {
		existing.task = task
		existing.generation++
		q.entries[key] = existing
		q.mu.Unlock()
		q.signal()
		return true
	}
	if len(q.entries) >= q.max {
		q.mu.Unlock()
		if q.onDrop != nil {
			q.onDrop(key)
		}
		return false
	}
	q.entries[key] = overflowEntry{task: task, generation: 1}
	q.order = append(q.order, key)
	q.mu.Unlock()
	q.signal()
	return true
}

// Len reports the number of identities retained, including a task currently
// being submitted. It is safe to call from metrics and shutdown paths.
func (q *OverflowQueue) Len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Close stops dispatching and returns the number of tasks that could not be
// submitted before shutdown. The count is intentionally returned to callers
// so provider shutdown can emit an explicit loss/deferral record.
func (q *OverflowQueue) Close() int {
	if q == nil {
		return 0
	}
	q.stopOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		q.mu.Unlock()
		close(q.stop)
	})
	<-q.done
	return q.Len()
}

func (q *OverflowQueue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *OverflowQueue) run() {
	defer close(q.done)
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-q.stop:
			return
		case <-q.wake:
		}
		for {
			key, entry, ok := q.next()
			if !ok {
				break
			}
			if q.submit == nil {
				q.finish(key, entry.generation, false)
				q.waitRetry()
				break
			}
			if err := q.submit(entry.task); err != nil {
				q.finish(key, entry.generation, false)
				if !q.waitRetry() {
					return
				}
				break
			}
			q.finish(key, entry.generation, true)
		}
	}
}

func (q *OverflowQueue) next() (string, overflowEntry, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.order) > 0 {
		key := q.order[0]
		q.order = q.order[1:]
		entry, exists := q.entries[key]
		if !exists || entry.inflight {
			continue
		}
		entry.inflight = true
		q.entries[key] = entry
		return key, entry, true
	}
	return "", overflowEntry{}, false
}

func (q *OverflowQueue) finish(key string, generation uint64, submitted bool) {
	q.mu.Lock()
	entry, exists := q.entries[key]
	if exists && entry.generation == generation {
		if submitted {
			delete(q.entries, key)
		} else {
			entry.inflight = false
			q.entries[key] = entry
			q.order = append(q.order, key)
		}
	} else if exists {
		// A newer offer arrived while this task was in-flight. Preserve it;
		// the newer generation is independently eligible for submission.
		entry.inflight = false
		q.entries[key] = entry
		q.order = append(q.order, key)
	}
	q.mu.Unlock()
	if exists && (!submitted || entry.generation != generation) {
		q.signal()
	}
}

func (q *OverflowQueue) waitRetry() bool {
	timer := time.NewTimer(q.retry)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-q.ctx.Done():
		return false
	case <-q.stop:
		return false
	}
}
