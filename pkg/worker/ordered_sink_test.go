package worker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

type gateProbeSink struct {
	mu          sync.Mutex
	active, max int
	started     chan struct{}
	block       <-chan struct{}
}

func (p *gateProbeSink) Push(int64, []*instance.Instance) error                 { return p.enter() }
func (p *gateProbeSink) PushAll(int64, []*instance.Instance) error              { return p.enter() }
func (p *gateProbeSink) GetAll([]int32, string) (*instance.InstanceList, error) { return nil, nil }
func (p *gateProbeSink) enter() error {
	p.mu.Lock()
	p.active++
	if p.active > p.max {
		p.max = p.active
	}
	p.mu.Unlock()
	if p.started != nil {
		p.started <- struct{}{}
	}
	if p.block != nil {
		<-p.block
	}
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	return nil
}

var _ ports.InstanceSink = (*gateProbeSink)(nil)

func probeInstance(key string) *instance.Instance {
	return &instance.Instance{SourceKey: key, Reversion: 1}
}

func TestOrderedSinkAllowsDifferentIdentitiesToOverlap(t *testing.T) {
	block := make(chan struct{})
	p := &gateProbeSink{started: make(chan struct{}, 2), block: block}
	s := &orderedSink{inner: p}
	done := make(chan struct{}, 2)
	go func() { _ = s.Push(1, []*instance.Instance{probeInstance("a")}); done <- struct{}{} }()
	go func() { _ = s.Push(1, []*instance.Instance{probeInstance("b")}); done <- struct{}{} }()
	<-p.started
	<-p.started
	close(block)
	<-done
	<-done
	if p.max != 2 {
		t.Fatalf("max concurrency=%d, want 2", p.max)
	}
}

func TestOrderedSinkSerializesSameIdentity(t *testing.T) {
	block := make(chan struct{})
	p := &gateProbeSink{started: make(chan struct{}, 2), block: block}
	s := &orderedSink{inner: p}
	done := make(chan struct{}, 2)
	go func() { _ = s.Push(1, []*instance.Instance{probeInstance("a")}); done <- struct{}{} }()
	<-p.started
	go func() { _ = s.Push(2, []*instance.Instance{probeInstance("a")}); done <- struct{}{} }()
	select {
	case <-p.started:
		t.Fatal("same identity overlapped")
	case <-time.After(30 * time.Millisecond):
	}
	close(block)
	<-done
	<-done
}

func TestOrderedSinkFullPushExcludesIncremental(t *testing.T) {
	block := make(chan struct{})
	p := &gateProbeSink{started: make(chan struct{}, 2), block: block}
	s := &orderedSink{inner: p}
	done := make(chan struct{}, 2)
	go func() { _ = s.PushAll(1, []*instance.Instance{probeInstance("a")}); done <- struct{}{} }()
	<-p.started
	go func() { _ = s.Push(2, []*instance.Instance{probeInstance("b")}); done <- struct{}{} }()
	select {
	case <-p.started:
		t.Fatal("incremental push started during full push")
	case <-time.After(30 * time.Millisecond):
	}
	close(block)
	<-done
	<-done
}

type countingFullSink struct {
	fullCalls int
	pushCalls int
}

func (s *countingFullSink) Push(int64, []*instance.Instance) error {
	s.pushCalls++
	return nil
}
func (s *countingFullSink) PushAll(int64, []*instance.Instance) error {
	s.fullCalls++
	return nil
}
func (s *countingFullSink) GetAll([]int32, string) (*instance.InstanceList, error) {
	return nil, nil
}

func TestOrderedSinkRejectsFullSnapshotOmittingKnownIdentity(t *testing.T) {
	inner := &countingFullSink{}
	s := &orderedSink{inner: inner}
	if err := s.Push(2, []*instance.Instance{probeInstance("b")}); err != nil {
		t.Fatalf("Push(B) error = %v", err)
	}
	if err := s.PushAll(3, []*instance.Instance{probeInstance("a")}); !errors.Is(err, errStaleFullPush) {
		t.Fatalf("PushAll(A) error = %v, want errStaleFullPush", err)
	}
	if inner.fullCalls != 0 {
		t.Fatalf("inner PushAll calls = %d, want 0", inner.fullCalls)
	}
}

func TestOrderedSinkTrustedFullTombstoneAllowsLaterCompleteSnapshot(t *testing.T) {
	inner := &countingFullSink{}
	s := &orderedSink{inner: inner}
	a := probeInstance("a")
	b := probeInstance("b")
	if err := s.PushAllWithRevalidate(1, []*instance.Instance{a, b}, func() ([]*instance.Instance, bool) {
		return []*instance.Instance{a, b}, true
	}); err != nil {
		t.Fatalf("initial trusted full push: %v", err)
	}
	if err := s.PushAllWithRevalidate(2, []*instance.Instance{a}, func() ([]*instance.Instance, bool) {
		return []*instance.Instance{a}, true
	}); err != nil {
		t.Fatalf("trusted delete full push: %v", err)
	}
	if err := s.PushAll(3, []*instance.Instance{a}); err != nil {
		t.Fatalf("ordinary complete snapshot after tombstone: %v", err)
	}
	if inner.fullCalls != 3 {
		t.Fatalf("full calls = %d, want 3 after legitimate deletion", inner.fullCalls)
	}
	if err := s.Push(2, []*instance.Instance{b}); err != nil {
		t.Fatalf("old tombstoned revision returned error: %v", err)
	}
	if inner.fullCalls != 3 || inner.pushCalls != 0 {
		t.Fatal("old tombstoned revision reached sink")
	}
	newB := probeInstance("b")
	newB.Reversion = 2
	if err := s.Push(3, []*instance.Instance{newB}); err != nil {
		t.Fatalf("new revision after tombstone: %v", err)
	}
	if inner.pushCalls != 1 {
		t.Fatalf("new revision Push calls = %d, want 1", inner.pushCalls)
	}
}
