//go:build observe
// +build observe

package observe

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	domaininstance "spotter/internal/domain/instance"
	sv "spotter/pkg/beehive/service/v2"
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
	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", Status: 1, TriggerAt: sourceAt.Format(time.RFC3339Nano), ObservedAt: spotterAt.Format(time.RFC3339Nano)}
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

func TestObserveUnitWatchTimelineTracksPartialNacosDelete(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent, 1)
	spotterEvents := make(chan spotterWatchEvent, 1)
	nacosEvents := make(chan nacosWatchEvent, 2)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)

	createdAt := time.Now().Add(-time.Second)
	deletedAt := time.Now()
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{
		{Metadata: map[string]string{"instanceId": "pod-a"}},
		{Metadata: map[string]string{"instanceId": "pod-b"}},
	}, At: createdAt}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{
		{Metadata: map[string]string{"instanceId": "pod-b"}},
	}, At: deletedAt}
	k8sEvents <- k8sWatchEvent{Type: "DELETED", Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a"}}, At: deletedAt}
	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", Status: 3, TriggerAt: deletedAt.Format(time.RFC3339Nano), ObservedAt: deletedAt.Format(time.RFC3339Nano)}
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, _, nacosAt, ok := timeline.boundaryTimes([]string{"pod-a"}, false, deletedAt.Add(-time.Millisecond))
		if ok {
			if !nacosAt.Equal(deletedAt) {
				t.Fatalf("Nacos delete boundary = %v, want %v", nacosAt, deletedAt)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("partial Nacos snapshot removal was not observed as a delete boundary")
}

func TestObserveUnitWatchTimelineRequiresRevisionStatusAndCanonicalPayload(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent, 1)
	spotterEvents := make(chan spotterWatchEvent, 2)
	nacosEvents := make(chan nacosWatchEvent, 2)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
	issued := time.Now().Add(-time.Second)

	target := &sv.Instance{
		InstanceId: "pod-a", AppCode: "app-a", Provider: "k8s", SourceKey: "cluster-a/uid-a",
		SourceCluster: "cluster-a", Reversion: 42, Status: 2, Enabled: false,
		Label: map[string]string{"team": "payments"},
	}
	canonical := domaininstance.CanonicalPayload(target)
	wrong := *target
	wrong.Reversion = 41

	k8sEvents <- k8sWatchEvent{Type: "MODIFIED", Pod: &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a", ResourceVersion: "42"},
		Status:     v1.PodStatus{Phase: v1.PodRunning, PodIP: "10.0.0.1", ContainerStatuses: []v1.ContainerStatus{{Ready: false}}},
	}, At: issued.Add(100 * time.Millisecond)}
	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", Reversion: 41, Status: 2, CanonicalPayload: domaininstance.CanonicalPayload(&wrong), TriggerAt: issued.Add(50 * time.Millisecond).Format(time.RFC3339Nano), ObservedAt: issued.Add(200 * time.Millisecond).Format(time.RFC3339Nano)}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{{Metadata: map[string]string{
		"instanceId": "pod-a", "status": "2", "reversion": "41", "spotter.instance": domaininstance.CompressedCanonicalPayload(&wrong),
	}}}, At: issued.Add(300 * time.Millisecond)}

	time.Sleep(10 * time.Millisecond)
	if _, ok := timeline.exactBoundary("pod-a", true, 2, canonical, issued); ok {
		t.Fatal("wrong Reversion/canonical payload satisfied exact watch correlation")
	}

	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", Reversion: 42, Status: 2, CanonicalPayload: canonical, TriggerAt: issued.Add(50 * time.Millisecond).Format(time.RFC3339Nano), ObservedAt: issued.Add(400 * time.Millisecond).Format(time.RFC3339Nano)}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{{Metadata: map[string]string{
		"instanceId": "pod-a", "status": "2", "reversion": "42", "spotter.instance": domaininstance.CompressedCanonicalPayload(target),
	}}}, At: issued.Add(500 * time.Millisecond)}
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		boundary, ok := timeline.exactBoundary("pod-a", true, 2, canonical, issued)
		if ok {
			if boundary.Reversion != 42 || !boundary.SpotterSeen.Equal(issued.Add(400*time.Millisecond)) || !boundary.NacosSeen.Equal(issued.Add(500*time.Millisecond)) {
				t.Fatalf("exact boundary = %+v, want revision 42 and matching Spotter/Nacos observations", boundary)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("matching revision/status/canonical payload did not satisfy exact watch correlation")
}

func TestObserveUnitWatchTimelineRejectsSyncAllAndReconcileAsMutationEvents(t *testing.T) {
	for _, origin := range []string{"syncall-cache-snapshot", "reconcile-output"} {
		t.Run(origin, func(t *testing.T) {
			k8sEvents := make(chan k8sWatchEvent, 1)
			spotterEvents := make(chan spotterWatchEvent, 1)
			nacosEvents := make(chan nacosWatchEvent, 1)
			timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
			issued := time.Now().Add(-time.Second)
			target := &sv.Instance{InstanceId: "pod-a", AppCode: "app-a", Provider: "k8s", SourceKey: "cluster-a/uid-a", SourceCluster: "cluster-a", Reversion: 42, Status: 1, Enabled: true}
			canonical := domaininstance.CanonicalPayload(target)
			k8sEvents <- k8sWatchEvent{Type: "MODIFIED", Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", UID: "uid-a", ResourceVersion: "42"}, Status: v1.PodStatus{Phase: v1.PodRunning, PodIP: "10.0.0.1", ContainerStatuses: []v1.ContainerStatus{{Ready: true, State: v1.ContainerState{Running: &v1.ContainerStateRunning{}}}}}}, At: issued.Add(100 * time.Millisecond)}
			spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: origin, InstanceID: "pod-a", SourceKey: "cluster-a/uid-a", Reversion: 42, Status: 1, CanonicalPayload: canonical, TriggerAt: issued.Add(50 * time.Millisecond).Format(time.RFC3339Nano), ObservedAt: issued.Add(200 * time.Millisecond).Format(time.RFC3339Nano)}
			nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{{Metadata: map[string]string{"instanceId": "pod-a", "status": "1", "reversion": "42", "spotter.instance": domaininstance.CompressedCanonicalPayload(target)}}}, At: issued.Add(300 * time.Millisecond)}
			close(k8sEvents)
			close(spotterEvents)
			close(nacosEvents)
			time.Sleep(10 * time.Millisecond)
			if _, ok := timeline.exactBoundary("pod-a", true, 1, canonical, issued); ok {
				t.Fatalf("origin %q incorrectly satisfied incremental mutation correlation", origin)
			}
		})
	}
}

func TestObserveUnitWatchTimelineReportsPrematureClosure(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent)
	spotterEvents := make(chan spotterWatchEvent)
	nacosEvents := make(chan nacosWatchEvent)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if errors := timeline.healthErrors(); len(errors) >= 3 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("health errors = %v, want all premature closures", timeline.healthErrors())
}

func TestObserveUnitWatchTimelineDeleteUsesUIDWhenTombstoneRevisionIsCached(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent, 1)
	spotterEvents := make(chan spotterWatchEvent, 1)
	nacosEvents := make(chan nacosWatchEvent, 2)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
	issued := time.Now().Add(-time.Second)

	registered := &sv.Instance{InstanceId: "pod-a", AppCode: "app-a", Provider: "k8s", SourceKey: "cluster-a/uid-a", SourceCluster: "cluster-a", Reversion: 42, Status: 1, Enabled: true}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{{Metadata: map[string]string{
		"instanceId": "pod-a", "status": "1", "reversion": "42", "spotter.instance": domaininstance.CompressedCanonicalPayload(registered),
	}}}, At: issued}
	k8sEvents <- k8sWatchEvent{Type: "DELETED", Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", UID: "uid-a", ResourceVersion: "43"}}, At: issued.Add(100 * time.Millisecond)}
	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", SourceKey: "cluster-a/uid-a", Reversion: 42, Status: 3, TriggerAt: issued.Add(50 * time.Millisecond).Format(time.RFC3339Nano), ObservedAt: issued.Add(200 * time.Millisecond).Format(time.RFC3339Nano)}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: nil, At: issued.Add(300 * time.Millisecond)}
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		boundary, ok := timeline.exactBoundary("pod-a", false, 3, "", issued)
		if ok {
			if boundary.Reversion != 42 {
				t.Fatalf("delete boundary reversion = %d, want cached tombstone revision 42", boundary.Reversion)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("UID-correlated delete did not tolerate the cached tombstone revision")
}

func TestObserveUnitWatchTimelineSeparatesSameNamePodGenerations(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent, 2)
	spotterEvents := make(chan spotterWatchEvent, 2)
	nacosEvents := make(chan nacosWatchEvent, 4)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
	base := time.Now().Add(-time.Second)

	host := func(sourceKey string, reversion int64) spotternacos.Host {
		ins := &sv.Instance{InstanceId: "pod-a", AppCode: "app-a", Provider: "k8s", SourceKey: sourceKey, SourceCluster: "cluster-a", Reversion: reversion, Status: 1, Enabled: true}
		return spotternacos.Host{Metadata: map[string]string{
			"instanceId": "pod-a", "status": "1", "reversion": fmt.Sprint(reversion), "spotter.instance": domaininstance.CompressedCanonicalPayload(ins),
		}}
	}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{host("cluster-a/uid-old", 10)}, At: base}
	k8sEvents <- k8sWatchEvent{Type: "DELETED", Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", UID: "uid-old", ResourceVersion: "11"}}, At: base.Add(100 * time.Millisecond)}
	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", SourceKey: "cluster-a/uid-old", Reversion: 10, Status: 3, TriggerAt: base.Add(50 * time.Millisecond).Format(time.RFC3339Nano), ObservedAt: base.Add(200 * time.Millisecond).Format(time.RFC3339Nano)}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: nil, At: base.Add(300 * time.Millisecond)}

	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: []spotternacos.Host{host("cluster-a/uid-new", 20)}, At: base.Add(400 * time.Millisecond)}
	newDeleteIssued := base.Add(500 * time.Millisecond)
	k8sEvents <- k8sWatchEvent{Type: "DELETED", Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", UID: "uid-new", ResourceVersion: "21"}}, At: base.Add(600 * time.Millisecond)}
	spotterEvents <- spotterWatchEvent{Boundary: "provider-output/pre-worker", Operation: "Sync", Origin: "event-cache-applied", InstanceID: "pod-a", SourceKey: "cluster-a/uid-new", Reversion: 20, Status: 3, TriggerAt: base.Add(550 * time.Millisecond).Format(time.RFC3339Nano), ObservedAt: base.Add(700 * time.Millisecond).Format(time.RFC3339Nano)}
	nacosEvents <- nacosWatchEvent{Service: "app-a", Hosts: nil, At: base.Add(800 * time.Millisecond)}
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		boundary, ok := timeline.exactBoundary("pod-a", false, 3, "", newDeleteIssued)
		if ok {
			if boundary.Reversion != 20 || !boundary.NacosSeen.Equal(base.Add(800*time.Millisecond)) {
				t.Fatalf("new generation boundary = %+v, want uid-new/reversion 20 removal", boundary)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("same-name new UID deletion was not correlated to its own Nacos generation")
}

func TestObserveUnitWatchSummaryFailsClosedOnMissingMutation(t *testing.T) {
	k8sEvents := make(chan k8sWatchEvent)
	spotterEvents := make(chan spotterWatchEvent)
	nacosEvents := make(chan nacosWatchEvent)
	timeline := newWatchTimeline(k8sEvents, spotterEvents, nacosEvents)
	issued := time.Now()
	evidence, records := timeline.summarizeMutations([]ledgerEntry{{Op: "create", AppCode: "app-a", PodName: "pod-a", IssuedAt: issued}}, 1, 1)
	if evidence.Mutations != 1 || evidence.Correlated != 0 || evidence.Missing != 1 {
		t.Fatalf("watch evidence = %+v, want one fail-closed missing mutation", evidence)
	}
	if len(records) != 1 || records[0].Missing == "" {
		t.Fatalf("mutation records = %+v, want explicit missing reason", records)
	}
	close(k8sEvents)
	close(spotterEvents)
	close(nacosEvents)
}
