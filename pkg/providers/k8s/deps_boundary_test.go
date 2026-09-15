package k8s

import (
	"context"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/internal/ports"
	"spotter/pkg/k8srobot"
	"testing"
)

type depsTestLogger struct{ ports.NopLogger }
type depsTestNotifier struct{ called bool }

func (*depsTestNotifier) Notify(string, string) {}

func TestK8SProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	logger := depsTestLogger{}
	notifier := &depsTestNotifier{}
	provider, err := NewK8SProviderWithDeps(context.Background(), nil, 17, []string{"/definitely/missing/config"}, logger, notifier, []string{"explicit"})
	if err == nil || provider != nil {
		t.Fatalf("constructor result=(%v,%v), want nil provider and configuration error", provider, err)
	}
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
}

func TestFormatInstanceWithDepsDoesNotReadLegacyGlobals(t *testing.T) {
	legacycompat.ResetAccessCounts()
	pod := &v1.Pod{}
	_ = formatInstanceWithDeps(nil, pod, []string{"explicit"}, depsTestLogger{})
	if got := legacycompat.AccessCountsSnapshot().Reads; got != 0 {
		t.Fatalf("legacy reads=%d", got)
	}
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
