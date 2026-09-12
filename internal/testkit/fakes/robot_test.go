package fakes

import (
	"testing"
	"time"

	"spotter/pkg/k8srobot"
)

type nestedRobotObject struct {
	Labels map[string]string
	Ports  []int
}

func TestFakeRobotEnqueuePopAndGetByKey(t *testing.T) {
	r := NewFakeRobot(4)
	events := MultiClusterDeleteRecreateScript("ns", "worker", "cluster-a")
	for _, event := range events {
		if !r.Enqueue(event) {
			t.Fatalf("Enqueue(%#v) unexpectedly rejected", event)
		}
	}
	if got := len(r.Events()); got != 3 {
		t.Fatalf("Events length = %d, want 3", got)
	}
	for i, want := range []k8srobot.EventType{k8srobot.EventAdd, k8srobot.EventDelete, k8srobot.EventAdd} {
		obj, err := r.Pop()
		if err != nil {
			t.Fatalf("Pop[%d] error = %v", i, err)
		}
		if obj.Event != want || obj.Key != "ns/worker" {
			t.Fatalf("Pop[%d] = %#v, want %s ns/worker", i, obj, want)
		}
	}
	if _, ok := r.GetByKey(k8srobot.Pods, "ns/worker"); !ok {
		t.Fatal("GetByKey lost recreated object")
	}
}

func TestFakeRobotMultiClusterScriptAndStopBoundary(t *testing.T) {
	r := NewFakeRobot(8)
	script := MultiClusterDeleteRecreateScript("ns", "worker", "cluster-a", "cluster-b")
	for _, event := range script {
		r.Enqueue(event)
	}
	snapshot := r.Events()
	snapshot[0].UID = "mutated"
	if r.Events()[0].UID == "mutated" {
		t.Fatal("Events leaked internal storage")
	}
	if snapshot[0].ClusterID == snapshot[3].ClusterID || snapshot[0].UID == snapshot[3].UID {
		t.Fatal("same-name events from clusters were merged")
	}
	r.Stop()
	if _, err := r.Pop(); err == nil {
		t.Fatal("Pop after Stop succeeded; queued events must not be consumed")
	}
	if r.Enqueue(script[0]) {
		t.Fatal("Enqueue after Stop succeeded")
	}
}

func TestFakeRobotSnapshotsCloneNestedObjectState(t *testing.T) {
	r := NewFakeRobot(1)
	object := &nestedRobotObject{Labels: map[string]string{"role": "api"}, Ports: []int{80}}
	r.Enqueue(RobotEvent{Namespace: "ns", Name: "pod", Type: k8srobot.EventAdd, Object: object})
	event := r.Events()
	event[0].Object.(*nestedRobotObject).Labels["role"] = "mutated"
	event[0].Object.(*nestedRobotObject).Ports[0] = 999
	byKey, ok := r.GetByKey(k8srobot.Pods, "ns/pod")
	if !ok || byKey[0].(*nestedRobotObject).Labels["role"] != "api" || byKey[0].(*nestedRobotObject).Ports[0] != 80 {
		t.Fatalf("nested object alias leaked through Events/GetByKey: %#v", byKey)
	}
	byKey[0].(*nestedRobotObject).Labels["role"] = "returned"
	again, _ := r.GetByKey(k8srobot.Pods, "ns/pod")
	if again[0].(*nestedRobotObject).Labels["role"] != "api" {
		t.Fatal("GetByKey returned internal nested state")
	}
}

func TestFakeRobotStopUnblocksBlockedPopWithPriority(t *testing.T) {
	r := NewFakeRobot(1)
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); _, err := r.Pop(); result <- err }()
	<-started
	r.Stop()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("blocked Pop returned success after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("Pop was not unblocked by Stop")
	}
	if r.Enqueue(RobotEvent{Namespace: "ns", Name: "late", Type: k8srobot.EventAdd}) {
		t.Fatal("Enqueue after Stop succeeded")
	}
}

func TestFakeRobotStopDoesNotConsumeBufferedQueue(t *testing.T) {
	r := NewFakeRobot(1)
	if !r.Enqueue(RobotEvent{Namespace: "ns", Name: "queued", Type: k8srobot.EventAdd}) {
		t.Fatal("failed to enqueue boundary event")
	}
	r.Stop()
	if _, err := r.Pop(); err == nil {
		t.Fatal("Pop after Stop consumed a buffered event")
	}
	if got := r.QueueDepth(); got != 1 {
		t.Fatalf("QueueDepth after Stop = %d, want 1 (event remains unconsumed)", got)
	}
}
