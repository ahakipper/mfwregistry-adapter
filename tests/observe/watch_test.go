//go:build observe
// +build observe

package observe

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spotternacos "spotter/pkg/nacos"
)

func TestObserveUnitWatchTimelineCorrelatesThreePlanes(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent, 1)
	spotterEvents := make(chan spotterWatchEvent, 1)
	nacosEvents := make(chan nacosWatchEvent, 1)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
	issued := time.Now().Add(-time.Second)
	sourceAt := issued.Add(100 * time.Millisecond)
	spotterAt := issued.Add(200 * time.Millisecond)
	nacosAt := issued.Add(300 * time.Millisecond)
	k8sEvents <- k8sWatchEvent{Type: "ADDED", Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a"}}, At: sourceAt}
	spotterEvents <- spotterWatchEvent{InstanceID: "pod-a", Status: 1, ObservedAt: spotterAt.Format(time.RFC3339Nano)}
	nacosEvents <- nacosWatchEvent{Hosts: []spotternacos.Host{{Metadata: map[string]string{"instanceId": "pod-a"}}}, At: nacosAt}
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gotSource, gotSpotter, gotNacos, ok := timeline.boundaryTimes([]string{"pod-a"}, true, issued)
		if ok {
			if !gotSource.Equal(sourceAt) || !gotSpotter.Equal(spotterAt) || !gotNacos.Equal(nacosAt) {
				t.Fatalf("boundaries = %v/%v/%v, want %v/%v/%v", gotSource, gotSpotter, gotNacos, sourceAt, spotterAt, nacosAt)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("three-plane boundary correlation did not become ready")
}
