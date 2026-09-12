package fakes

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// TestRobotSameKeyDifferentClustersRemainDistinct protects the multi-cluster
// lookup seam used by the Kubernetes event path. A same namespace/name is a
// valid collision across clusters and each lookup must return an independent
// DeepCopy rather than exposing the fixture's stored pod.
func TestRobotSameKeyDifferentClustersRemainDistinct(t *testing.T) {
	r := NewFakeRobot(4)
	key := "ns/worker"
	podA := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "worker", UID: "uid-a"}}
	podB := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "worker", UID: "uid-b"}}
	if !r.Enqueue(RobotEvent{ClusterID: "cluster-a", Namespace: "ns", Name: "worker", Object: podA, Type: k8srobot.EventAdd}) ||
		!r.Enqueue(RobotEvent{ClusterID: "cluster-b", Namespace: "ns", Name: "worker", Object: podB, Type: k8srobot.EventAdd}) {
		t.Fatal("failed to enqueue same-name pods from both clusters")
	}
	itemsA, ok := r.GetByClusterKey(k8srobot.Pods, "cluster-a", key)
	if !ok || len(itemsA) != 1 {
		t.Fatalf("cluster-a lookup = %#v, %v; want one pod", itemsA, ok)
	}
	itemsB, ok := r.GetByClusterKey(k8srobot.Pods, "cluster-b", key)
	if !ok || len(itemsB) != 1 {
		t.Fatalf("cluster-b lookup = %#v, %v; want one pod", itemsB, ok)
	}
	if got := string(itemsA[0].(*corev1.Pod).UID); got != "uid-a" {
		t.Fatalf("cluster-a UID = %q, want uid-a", got)
	}
	if got := string(itemsB[0].(*corev1.Pod).UID); got != "uid-b" {
		t.Fatalf("cluster-b UID = %q, want uid-b", got)
	}
	itemsA[0].(*corev1.Pod).Labels = map[string]string{"mutated": "true"}
	itemsA[0].(*corev1.Pod).UID = "mutated"
	againA, _ := r.GetByClusterKey(k8srobot.Pods, "cluster-a", key)
	if got := string(againA[0].(*corev1.Pod).UID); got != "uid-a" {
		t.Fatalf("cluster-a lookup leaked mutation, UID = %q", got)
	}
	if _, ok := r.GetByClusterKey(k8srobot.ResourceType("services"), "cluster-a", key); ok {
		t.Fatal("non-Pod GetByClusterKey unexpectedly returned an object")
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
