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

func TestOrderedSinkIncrementalOfflineTombstonesSameRevision(t *testing.T) {
	inner := &countingFullSink{}
	s := &orderedSink{inner: inner}
	online := probeInstance("k8s/uid-a")
	online.Status = instance.InstanceStatusOnline
	online.Reversion = 7
	if err := s.Push(1, []*instance.Instance{online}); err != nil {
		t.Fatalf("seed online push: %v", err)
	}
	offline := *online
	offline.Status = instance.InstanceStatusOffline
	if err := s.Push(2, []*instance.Instance{&offline}); err != nil {
		t.Fatalf("incremental offline push: %v", err)
	}
	if err := s.Push(3, []*instance.Instance{online}); err != nil {
		t.Fatalf("same-revision stale online push: %v", err)
	}
	if inner.pushCalls != 2 {
		t.Fatalf("inner push calls = %d, want online+offline only", inner.pushCalls)
	}
	newer := *online
	newer.Reversion = 8
	if err := s.Push(4, []*instance.Instance{&newer}); err != nil {
		t.Fatalf("newer online push: %v", err)
	}
	if inner.pushCalls != 3 {
		t.Fatalf("inner push calls = %d, want newer revision accepted", inner.pushCalls)
	}
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

type scopedOperationSink struct {
	countingFullSink
	operations []ports.RetryOperation
}

func (s *scopedOperationSink) PushAllOperation(operation ports.RetryOperation) error {
	s.operations = append(s.operations, operation)
	return nil
}

func TestOrderedSinkScopesTrustedFullTombstonesByProvider(t *testing.T) {
	inner := &scopedOperationSink{}
	s := &orderedSink{inner: inner}
	k8s := probeInstance("k8s/uid")
	k8s.Provider = "k8s"
	ecs := probeInstance("ecs/service")
	ecs.Provider = "ecs"
	if err := s.PushAllOperation(ports.RetryOperation{Sink: "nacos", Operate: ports.OperateTypeSyncAll, Scope: "k8s", BatchID: "k8s-1", Trigger: 1, Instances: []*instance.Instance{k8s}}); err != nil {
		t.Fatalf("k8s full operation: %v", err)
	}
	if err := s.PushAllOperation(ports.RetryOperation{Sink: "nacos", Operate: ports.OperateTypeSyncAll, Scope: "ecs", BatchID: "ecs-1", Trigger: 2, Instances: []*instance.Instance{ecs}}); err != nil {
		t.Fatalf("ecs full operation: %v", err)
	}
	// The ECS complete snapshot must not tombstone the K8s identity. A
	// same-revision K8s incremental remains valid and must reach the sink.
	if err := s.Push(1, []*instance.Instance{k8s}); err != nil {
		t.Fatalf("k8s incremental after ecs full: %v", err)
	}
	if inner.pushCalls != 1 {
		t.Fatalf("k8s incremental calls = %d, want 1 (cross-provider tombstone must not suppress it)", inner.pushCalls)
	}
}

func TestOrderedSinkMissingScopeCannotTombstoneOtherProviders(t *testing.T) {
	inner := &scopedOperationSink{}
	s := &orderedSink{inner: inner}
	k8s := probeInstance("k8s/uid")
	if err := s.PushAllOperation(ports.RetryOperation{Sink: "nacos", Operate: ports.OperateTypeSyncAll, Scope: "k8s", BatchID: "k8s-1", Trigger: 1, Instances: []*instance.Instance{k8s}}); err != nil {
		t.Fatalf("seed scoped full operation: %v", err)
	}
	if err := s.PushAllOperation(ports.RetryOperation{Sink: "nacos", Operate: ports.OperateTypeSyncAll, BatchID: "unscoped", Trigger: 2, Instances: nil}); !errors.Is(err, errStaleFullPush) {
		t.Fatalf("unscoped destructive operation error = %v, want stale/untrusted rejection", err)
	}
	if err := s.Push(1, []*instance.Instance{k8s}); err != nil {
		t.Fatalf("k8s incremental after unscoped full: %v", err)
	}
	if inner.pushCalls != 1 {
		t.Fatalf("k8s incremental calls = %d, want 1", inner.pushCalls)
	}
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
