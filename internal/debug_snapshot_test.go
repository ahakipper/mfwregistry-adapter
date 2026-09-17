package internal

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/providers"
)

type debugSnapshotProvider struct {
	instances []*v2.Instance
}

func TestObserveDebugEventsExposeOrderedCanonicalBoundary(t *testing.T) {
	recorder := newObserveEventRecorder(2)
	s := &Server{observeDebug: recorder}
	first := &v2.Instance{InstanceId: "pod-a", AppCode: "pay-user", Provider: "k8s", Reversion: 42, Status: 1, Label: map[string]string{"team": "payments"}}
	recorder.record(time.Now().Add(-time.Second).UnixNano(), first)
	recorder.record(time.Now().UnixNano(), &v2.Instance{InstanceId: "pod-b", AppCode: "pay-user", Provider: "k8s", Reversion: 43, Status: 1})
	recorder.record(time.Now().UnixNano(), &v2.Instance{InstanceId: "pod-c", AppCode: "pay-user", Provider: "k8s", Reversion: 44, Status: 3})

	req := httptest.NewRequest("GET", "/debug/spotter/events?after=0&wait=0s", nil)
	resp := httptest.NewRecorder()
	s.handleObserveDebugEvents(resp, req)
	if resp.Code != 200 {
		t.Fatalf("debug events status = %d, want 200", resp.Code)
	}
	var got observeEventBatch
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode debug events: %v", err)
	}
	if !got.Gap || got.EarliestSequence != 2 || got.LatestSequence != 3 || len(got.Events) != 2 {
		t.Fatalf("event batch = %+v, want bounded sequence 2..3 with gap", got)
	}
	last := got.Events[1]
	if last.Sequence != 3 || last.InstanceID != "pod-c" || last.Status != 3 || last.Reversion != 44 || last.CanonicalPayload == "" || last.ObservedAt == "" {
		t.Fatalf("last event = %+v, want complete pod-c boundary", last)
	}
}

func (p debugSnapshotProvider) Run() error             { return nil }
func (p debugSnapshotProvider) CompareAndFlush()       {}
func (p debugSnapshotProvider) GetAll() []*v2.Instance { return p.instances }

func TestObserveDebugSnapshotReturnsProviderProjection(t *testing.T) {
	want := &v2.Instance{InstanceId: "pod-a", Provider: providers.ProviderK8s, AppCode: "pay-user", Reversion: 42, Label: map[string]string{"env": "test"}}
	s := &Server{Providers: []providers.Provider{
		debugSnapshotProvider{instances: []*v2.Instance{want, {InstanceId: "ecs-a", Provider: providers.ProviderEcs}}},
	}}
	req := httptest.NewRequest("GET", "/debug/spotter/k8s", nil)
	resp := httptest.NewRecorder()
	s.handleObserveDebugSnapshot(resp, req)
	if resp.Code != 200 {
		t.Fatalf("debug snapshot status = %d, want 200", resp.Code)
	}
	var got struct {
		Instances []*v2.Instance `json:"instances"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode debug snapshot: %v", err)
	}
	if len(got.Instances) != 1 || got.Instances[0].InstanceId != want.InstanceId || got.Instances[0].Reversion != want.Reversion || got.Instances[0].Label["env"] != "test" {
		t.Fatalf("debug snapshot instances = %#v, want the complete K8s projection", got.Instances)
	}
}

func TestObserveDebugSnapshotRejectsNonGet(t *testing.T) {
	s := &Server{}
	req := httptest.NewRequest("POST", "/debug/spotter/k8s", nil)
	resp := httptest.NewRecorder()
	s.handleObserveDebugSnapshot(resp, req)
	if resp.Code != 405 {
		t.Fatalf("debug snapshot POST status = %d, want 405", resp.Code)
	}
}
