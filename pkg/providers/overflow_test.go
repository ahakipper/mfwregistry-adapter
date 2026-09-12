package providers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestOverflowQueueCoalescesByIdentityAndRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var attempts int
	var calls []string
	ready := false
	q := NewOverflowQueue(ctx, 2, func(task func()) error {
		mu.Lock()
		attempts++
		ok := ready
		mu.Unlock()
		if !ok {
			return errors.New("pool saturated")
		}
		task()
		return nil
	}, nil)
	defer q.Close()
	q.Offer("k8s/cluster-a/pod-a", func() {
		mu.Lock()
		calls = append(calls, "old")
		mu.Unlock()
	})
	q.Offer("k8s/cluster-a/pod-a", func() {
		mu.Lock()
		calls = append(calls, "latest")
		mu.Unlock()
	})
	if got := q.Len(); got != 1 {
		t.Fatalf("queue depth after same-key offers = %d, want 1", got)
	}
	time.Sleep(25 * time.Millisecond)
	mu.Lock()
	ready = true
	mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(calls) == 1
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 || calls[0] != "latest" {
		t.Fatalf("replayed calls = %v, want only latest task", calls)
	}
	if attempts < 2 {
		t.Fatalf("submit attempts = %d, want a retry after saturation", attempts)
	}
}

func TestOverflowQueueBoundsDistinctIdentitiesAndReportsDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var dropped []string
	q := NewOverflowQueue(ctx, 1, func(func()) error { return errors.New("saturated") }, func(key string) {
		mu.Lock()
		dropped = append(dropped, key)
		mu.Unlock()
	})
	defer q.Close()
	if !q.Offer("first", func() {}) {
		t.Fatal("first offer rejected")
	}
	if q.Offer("second", func() {}) {
		t.Fatal("second distinct identity accepted beyond bound")
	}
	if q.Offer("first", func() {}) == false {
		t.Fatal("same identity update rejected while queue is full")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dropped) != 1 || dropped[0] != "second" {
		t.Fatalf("drop reports = %v, want [second]", dropped)
	}
}

func TestOverflowQueueCloseCancelsRetryWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := NewOverflowQueue(ctx, 1, func(func()) error { return errors.New("saturated") }, nil)
	q.Offer("identity", func() {})
	// The dispatcher is in its retry wait. Close must return immediately and
	// report the retained task instead of leaving a goroutine behind.
	time.Sleep(5 * time.Millisecond)
	started := time.Now()
	remaining := q.Close()
	cancel()
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("Close took %s, want context-cancellable shutdown", elapsed)
	}
	if remaining != 1 {
		t.Fatalf("remaining queue depth = %d, want 1", remaining)
	}
}
