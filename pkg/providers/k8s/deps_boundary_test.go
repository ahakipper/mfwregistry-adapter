package k8s

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"spotter/internal/ports"
	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/k8srobot"
	"spotter/pkg/providers"
)

type depsTestLogger struct{ ports.NopLogger }
type depsTestNotifier struct{ called bool }

func (*depsTestNotifier) Notify(string, string) {}

func TestK8SProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	logger := depsTestLogger{}
	notifier := &depsTestNotifier{}
	provider, err := NewK8SProviderWithDeps(context.Background(), nil, 17, []string{"/definitely/missing/config"}, logger, notifier, []string{"explicit"})
	if err == nil || provider != nil {
		t.Fatalf("constructor result=(%v,%v), want nil provider and configuration error", provider, err)
	}
}

func TestFormatInstanceWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	pod := &v1.Pod{}
	_ = formatInstanceWithDeps(nil, pod, []string{"explicit"}, depsTestLogger{})
}

func TestConvertPodUsesCompleteProductionInstanceProjection(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "default", UID: "uid-a", ResourceVersion: "42", Labels: map[string]string{"app-code": "pay-user", "env-type": "test", "version": "v7", "app": "pay-user", "custom": "kept"}},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Name: "application", Image: "repo/app:v7", Ports: []v1.ContainerPort{{Name: "http", ContainerPort: 8080}}, Env: []v1.EnvVar{{Name: "K8S_CLUSTER_TYPE", Value: "test"}}}}},
		Status:     v1.PodStatus{Phase: v1.PodRunning, PodIP: "10.0.0.7", ContainerStatuses: []v1.ContainerStatus{{Ready: true, State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}},
	}
	got := ConvertPod(&k8srobot.QueueObject{ClusterID: "cluster-a"}, pod, nil, ports.NopLogger{})
	if got == nil || got.Reversion != 42 || got.Label["custom"] != "kept" || got.Image["application"] != "repo/app:v7" || len(got.Ports) != 2 {
		t.Fatalf("ConvertPod() = %#v, want full labels/reversion/image/ports projection", got)
	}
}

func TestInstanceEventObserverReceivesConvertedEventBoundary(t *testing.T) {
	worker := &fakeWorker{}
	provider := &k8s{
		filters: providers.InitInstanceFilters(), logger: ports.NopLogger{},
		cache: providers.NewCache(2), worker: worker,
	}
	trigger := time.Now().Add(-time.Second).UnixNano()
	pod := newValidPod("msp", "pod-a")
	pod.Labels["team"] = "payments"
	instance := ConvertPod(&k8srobot.QueueObject{ClusterID: "cluster-a"}, pod, nil, ports.NopLogger{})
	provider.ProcessCache(k8srobot.EventUpdate, instance)
	var gotTrigger int64
	var got *sv.Instance
	provider.SetInstanceEventObserver(func(eventTrigger int64, instance *sv.Instance) {
		if len(worker.handleSnapshot()) != 0 {
			t.Fatal("observer ran after worker.Handle; want the cache-applied/pre-worker boundary")
		}
		cached, _ := provider.ObserveCacheSnapshot()
		if len(cached) != 1 || cached[0].InstanceId != "pod-a" {
			t.Fatalf("observer saw cache snapshot %#v, want pod-a already applied", cached)
		}
		gotTrigger = eventTrigger
		got = instance
	})
	provider.eventSync(instance, trigger)
	if gotTrigger != trigger || got == nil || got.InstanceId != pod.Name || got.Reversion != 42 || got.Label["team"] != "payments" {
		t.Fatalf("observer event = trigger:%d instance:%#v, want complete pod-a event at %d", gotTrigger, got, trigger)
	}
	if len(worker.handleSnapshot()) != 1 {
		t.Fatalf("worker.Handle calls = %d, want 1 after observer", len(worker.handleSnapshot()))
	}
}
