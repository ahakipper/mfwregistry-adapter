package k8s

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"spotter/config"
	"spotter/internal/testkit/fakes"
	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/k8srobot"
	"spotter/pkg/log"
	"spotter/pkg/notice"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
)

// TestMain isolates the legacy package globals the k8s provider depends on
// (docs/testing.md section 7, "legacy globals"): pkg/log writes app.log into
// config.LogFilePath and pkg/notice delivers through log.Logger. Point both at
// a per-test-run temporary directory so no artifacts land in the repository.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "k8s-whitebox-")
	if err != nil {
		panic(err)
	}
	config.LogFilePath = dir + string(os.PathSeparator) // trailing separator, like the e2e suite (pkg/log concatenates the file name)
	config.LogToStd = false
	if err := log.LoggerInit(); err != nil {
		panic(err)
	}
	notice.InitNoticeClient("test")
	code := m.Run()
	// Remove the temporary directory (app.log included) before exiting:
	// os.Exit skips deferred calls, so the cleanup cannot be a defer.
	os.RemoveAll(dir)
	os.Exit(code)
}

// -----------------------------------------------------------------------------
// Test doubles (same-package fakes, docs/testing.md 4.4 / backlog item 1)
// -----------------------------------------------------------------------------

// fakeRobot is an in-package k8srobot.Robot double. It serves the contents of
// the (id-keyed) byKey map from GetByKey and the pods slice from List, so it
// covers both the "pod present" and the "DELETE event, pod gone" shapes of
// pod2Instance and GetAll. Pop honors the real robot's blocking contract: it
// blocks until a scripted event is enqueued or Stop is called, and it never
// returns a zero object with a nil error (which would make the monitor loop
// busy-spin).
type fakeRobot struct {
	byKey map[string][]interface{}
	pods  []interface{}

	hasSynced  bool
	stopped    bool
	queue      chan k8srobot.QueueObject // scripted events served by Pop
	stoppedCh  chan struct{}             // closed by Stop to unblock Pop
	finishedOb []k8srobot.QueueObject
	mu         sync.Mutex
}

// newFakeRobot builds a robot serving byKey from GetByKey and pods from List,
// with hasSynced as the initial sync state.
func newFakeRobot(byKey map[string][]interface{}, pods []interface{}, hasSynced bool) *fakeRobot {
	return &fakeRobot{
		byKey:     byKey,
		pods:      pods,
		hasSynced: hasSynced,
		queue:     make(chan k8srobot.QueueObject, 16),
		stoppedCh: make(chan struct{}),
	}
}

func (r *fakeRobot) Run() error {
	return nil
}

func (r *fakeRobot) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	close(r.stoppedCh)
}

// wasStopped reports whether Stop was called (locked read).
func (r *fakeRobot) wasStopped() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped
}

func (r *fakeRobot) HasSynced() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hasSynced
}

// enqueue delivers a scripted event through Pop. It never blocks: a full
// scripted queue drops the event, like the real robot does.
func (r *fakeRobot) enqueue(obj k8srobot.QueueObject) {
	select {
	case r.queue <- obj:
	default:
	}
}

// Pop blocks until a scripted event is available or the robot is stopped,
// mirroring the real robot's semantics: after Stop it first drains the events
// already queued, then returns an error. There is no path that returns a zero
// object with a nil error, so the monitor loop can never busy-spin on it.
func (r *fakeRobot) Pop() (k8srobot.QueueObject, error) {
	select {
	case obj := <-r.queue:
		return obj, nil
	case <-r.stoppedCh:
		select {
		case obj := <-r.queue:
			return obj, nil
		default:
			return k8srobot.QueueObject{}, errors.New("fake robot has been stopped")
		}
	}
}

func (r *fakeRobot) Finish(obj k8srobot.QueueObject) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishedOb = append(r.finishedOb, obj)
}

func (r *fakeRobot) GetByKey(resource k8srobot.ResourceType, key string) ([]interface{}, bool) {
	if resource != k8srobot.Pods {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	items, ok := r.byKey[key]
	if !ok || len(items) == 0 {
		return nil, false
	}
	return items, true
}

// finishedCount returns the number of Finish calls (locked read).
func (r *fakeRobot) finishedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.finishedOb)
}

// waitForFinish polls until n Finish calls are recorded. Finish is called in
// the monitor loop right after the pool submission, so it may lag the Handle
// observation the tests wait on first.
func (r *fakeRobot) waitForFinish(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && r.finishedCount() != n {
		time.Sleep(2 * time.Millisecond)
	}
	if got := r.finishedCount(); got != n {
		t.Fatalf("robot.Finish calls = %d, want %d", got, n)
	}
}

func (r *fakeRobot) List(resource k8srobot.ResourceType) []interface{} {
	if resource != k8srobot.Pods {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]interface{}(nil), r.pods...)
}

// QueueDepth reports the scripted queue's length — the k8s_queue_depth
// gauge's source (dsca-1 DS-1-1 fix item 1), mirroring the real robot's
// distinct-key depth against the fake's channel.
func (r *fakeRobot) QueueDepth() int {
	return len(r.queue)
}

// fakeWorker records every Handle call and serves a scripted GetAll response.
type fakeWorker struct {
	handles []*worker.Event
	mu      sync.Mutex

	getAllResponse *sv.InstanceList
	getAllErr      error
	getAllCalls    int
}

func (w *fakeWorker) AddEventHandler(opt worker.OperateType, handler worker.EventResourceHandler) {}

func (w *fakeWorker) Handle(d *worker.Event) {
	if d == nil {
		return
	}
	w.mu.Lock()
	w.handles = append(w.handles, d)
	w.mu.Unlock()
}

func (w *fakeWorker) ProcessUnsynced() {}

func (w *fakeWorker) GetAll(enable []int32, provider string) (*sv.InstanceList, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.getAllCalls++
	return w.getAllResponse, w.getAllErr
}

// getAllCallCount returns the number of GetAll calls (locked read).
func (w *fakeWorker) getAllCallCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.getAllCalls
}

func (w *fakeWorker) handleSnapshot() []*worker.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]*worker.Event(nil), w.handles...)
}

// waitForHandles polls the fake worker until n Handle calls are recorded.
// The bound must accommodate the pool-submitted asynchronous pushes.
func (w *fakeWorker) waitForHandles(t *testing.T, n int) []*worker.Event {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events := w.handleSnapshot()
		if len(events) >= n {
			return events
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("worker.Handle calls = %d, want >= %d; recorded = %#v", len(w.handleSnapshot()), n, w.handleSnapshot())
	return nil
}

// waitForHandleInterval polls the fake worker for pushes driven by a ticker
// (ProcessIntervalFullPush): the bound covers one interval tick plus margin.
func (w *fakeWorker) waitForHandleInterval(t *testing.T, n, interval int) []*worker.Event {
	t.Helper()
	deadline := time.Now().Add(time.Duration(interval)*time.Second + 2*time.Second)
	for time.Now().Before(deadline) {
		events := w.handleSnapshot()
		if len(events) >= n {
			return events
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("worker.Handle calls = %d, want >= %d; recorded = %#v", len(w.handleSnapshot()), n, w.handleSnapshot())
	return nil
}

// newTestProvider builds the provider the way NewK8SProvider does, minus the
// real robot: same filters, cache, pool and worker seams.
func newTestProvider(robot k8srobot.Robot, w worker.Worker) *k8s {
	pool, _ := ants.NewPool(providers.PoolBenchSize, withExpiryDuration(time.Second*providers.PoolExpireTime))
	return &k8s{
		providerName: "k8s",
		robot:        robot,
		ctx:          context.Background(),
		worker:       w,
		interval:     0,
		filters:      providers.InitInstanceFilters(),
		cache:        providers.NewCache(2),
		pool:         pool,
	}
}

// newValidPod builds a pod that passes every instance filter: valid appcode,
// env type, running/ready phase, IP, nonzero reversion.
func newValidPod(namespace, name string) *corev1.Pod {
	pod := &corev1.Pod{}
	pod.Name = name
	pod.Namespace = namespace
	pod.Labels = map[string]string{
		"app-code": "pay-user",
		"env-type": providers.EnvTest,
		"version":  "v1",
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.PodIP = "172.17.0.9"
	pod.Spec.Containers = []corev1.Container{
		{
			Name: "application",
			Env: []corev1.EnvVar{
				{Name: "K8S_CLUSTER_TYPE", Value: providers.EnvTest},
			},
		},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Ready:        true,
			State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			RestartCount: 0,
		},
	}
	pod.ResourceVersion = "42"
	return pod
}

// instanceFromPod runs formatInstance on the pod (nil queue object, like
// GetAll) and returns the instance.
func instanceFromPod(t *testing.T, pod *corev1.Pod) *sv.Instance {
	t.Helper()
	ins := formatInstance(nil, pod)
	if ins == nil {
		t.Fatalf("formatInstance(pod %s) = nil, want an instance", pod.Name)
	}
	return ins
}

// -----------------------------------------------------------------------------
// hasInstanceDiff (docs/testing.md backlog item 7)
// -----------------------------------------------------------------------------

func TestHasInstanceDiffCases(t *testing.T) {
	k := &k8s{}

	base := func() *sv.Instance {
		return &sv.Instance{
			InstanceId: "pod-a",
			EnvType:    "test",
			EnvGroup:   "7",
			State:      providers.InstanceStateRunning,
			Status:     providers.InstanceStatusOnline,
			Ip:         "1.1.1.1",
			Reversion:  10,
			Cpu:        1,
			Memory:     1024,
		}
	}

	tests := []struct {
		name    string
		mutate  func(new *sv.Instance)
		oldDiff func(old *sv.Instance)
		want    bool
	}{
		{
			name:   "identical instances report no diff",
			mutate: func(*sv.Instance) {},
			want:   false,
		},
		{
			name: "both offline report no diff even when fields differ",
			oldDiff: func(old *sv.Instance) {
				old.Status = providers.InstanceStatusOffline
			},
			mutate: func(new *sv.Instance) {
				new.Status = providers.InstanceStatusOffline
				new.State = providers.InstanceStateTerminated
				new.Ip = "9.9.9.9"
				new.Reversion = 99
			},
			want: false,
		},
		{
			name: "newer reversion reports a diff",
			mutate: func(new *sv.Instance) {
				new.Reversion = 11
			},
			want: true,
		},
		{
			name: "older reversion with equal fields reports no diff",
			mutate: func(new *sv.Instance) {
				new.Reversion = 9
			},
			want: false,
		},
		{
			name: "env type change reports a diff",
			mutate: func(new *sv.Instance) {
				new.EnvType = "beta"
			},
			want: true,
		},
		{
			name: "state change reports a diff",
			mutate: func(new *sv.Instance) {
				new.State = providers.InstanceStateProbing
			},
			want: true,
		},
		{
			name: "status change reports a diff",
			mutate: func(new *sv.Instance) {
				new.Status = providers.InstanceStatusUnhealthy
			},
			want: true,
		},
		{
			name: "env group change reports a diff",
			mutate: func(new *sv.Instance) {
				new.EnvGroup = "8"
			},
			want: true,
		},
		{
			name: "instance id change reports a diff",
			mutate: func(new *sv.Instance) {
				new.InstanceId = "pod-b"
			},
			want: true,
		},
		{
			name: "ip change reports a diff",
			mutate: func(new *sv.Instance) {
				new.Ip = "2.2.2.2"
			},
			want: true,
		},
		{
			name: "cpu and memory are excluded from the comparison",
			mutate: func(new *sv.Instance) {
				new.Cpu = 8
				new.Memory = 8192
			},
			want: false,
		},
		{
			// Pure enabled flip: same status/state/reversion/ip/env, only
			// Enabled differs. No current conversion shape produces it (the
			// k8s conversion derives Enabled from status), so this is a
			// hardening pin — the flag is part of the diff field set and must
			// propagate rather than be swallowed by the equality fallthrough.
			name: "enabled flip reports a diff",
			mutate: func(new *sv.Instance) {
				new.Enabled = !new.Enabled
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			old := base()
			if tc.oldDiff != nil {
				tc.oldDiff(old)
			}
			new := base()
			tc.mutate(new)
			if got := k.hasInstanceDiff(old, new); got != tc.want {
				t.Fatalf("hasInstanceDiff(%+v, %+v) = %v, want %v", old, new, got, tc.want)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// pod2Instance (docs/testing.md backlog item 7)
// -----------------------------------------------------------------------------

func TestPod2InstanceAddProducesInstanceAndCachesIt(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	obj := k8srobot.QueueObject{
		RType:    k8srobot.Pods,
		Key:      "msp/pod-a",
		Event:    k8srobot.EventAdd,
		CreateAt: time.Now(),
	}
	ins := k.pod2Instance(obj)
	if ins == nil {
		t.Fatal("pod2Instance(add with live pod) = nil, want an instance")
	}
	if ins.InstanceId != "pod-a" || ins.AppCode != "pay-user" || ins.Status != providers.InstanceStatusOnline {
		t.Fatalf("converted instance = %#v, want pod-a/pay-user/online", ins)
	}

	// The instance must be served from the cache afterwards.
	if cached := k.cache.Get("pod-a"); cached == nil {
		t.Fatal("cache.Get(pod-a) = nil after an add event, want the instance")
	} else if cached.InstanceId != "pod-a" || cached.Status != providers.InstanceStatusOnline {
		t.Fatalf("cached instance = %#v, want pod-a with online status", cached)
	}

	// A second add of identical data must not be reported: no diff, cache hit.
	if again := k.pod2Instance(obj); again != nil {
		t.Fatalf("pod2Instance(repeat add) = %#v, want nil (no diff)", again)
	}
}

func TestPod2InstanceUpdateWithDiffProducesInstance(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventUpdate}
	if first := k.pod2Instance(obj); first == nil {
		t.Fatal("first update produced no instance")
	}

	// Same pod, higher reversion: now a diff exists.
	pod.ResourceVersion = "43"
	if second := k.pod2Instance(obj); second == nil {
		t.Fatal("pod2Instance(update with newer reversion) = nil, want the instance")
	} else if second.Reversion != 43 {
		t.Fatalf("updated instance reversion = %d, want 43", second.Reversion)
	}
}

func TestPod2InstanceDeleteFromCacheMarksOffline(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	// Seed the cache with the live instance (the add path).
	add := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventAdd}
	if k.pod2Instance(add) == nil {
		t.Fatal("add path did not produce an instance")
	}

	// The delete event: the robot no longer serves the pod, so the cache
	// fallback must produce the offline instance.
	delete(robot.byKey, "msp/pod-a")
	del := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	ins := k.pod2Instance(del)
	if ins == nil {
		t.Fatal("pod2Instance(delete with cache hit) = nil, want the offline instance")
	}
	if ins.Status != providers.InstanceStatusOffline {
		t.Fatalf("deleted instance status = %d, want %d", ins.Status, providers.InstanceStatusOffline)
	}
	if ins.InstanceId != "pod-a" {
		t.Fatalf("deleted instance id = %q, want pod-a", ins.InstanceId)
	}

	// A repeated delete must not report again: the cached instance is already
	// offline (offline-equal, hasInstanceDiff == false).
	if again := k.pod2Instance(del); again != nil {
		t.Fatalf("pod2Instance(repeated delete) = %#v, want nil (already offline)", again)
	}
}

func TestPod2InstanceDeleteWithoutCacheReturnsNil(t *testing.T) {
	robot := newFakeRobot(nil, nil, false) // no cache, no byKey entry
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	if ins := k.pod2Instance(obj); ins != nil {
		t.Fatalf("pod2Instance(delete with empty cache) = %#v, want nil", ins)
	}
}

// newDeletedIPlessPod builds a valid pod that was deleted before it ever
// received an IP: DeletionTimestamp set, Status.PodIP empty. formatStatus
// then converts it to an offline instance whose Ip is "".
func newDeletedIPlessPod(namespace, name string) *corev1.Pod {
	pod := newValidPod(namespace, name)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	pod.Status.PodIP = ""
	pod.ResourceVersion = "50"
	return pod
}

// TestPod2InstanceDeleteIPlessPodWithCachedIPMergesOfflineInstance: the live
// incident's shape — the informer still holds a pod whose DeletionTimestamp
// is set but that never received an IP (a Pending pod churned away mid
// rolling-update). The cache holds the prior online instance with an IP, so
// the guard must merge the cached fields (Ip, Ports, AppCode) with the
// offline semantics (Status 3, terminated state, disabled, the pod's newest
// reversion) and return that instance, caching it.
func TestPod2InstanceDeleteIPlessPodWithCachedIPMergesOfflineInstance(t *testing.T) {
	pod := newDeletedIPlessPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	// Seed the cache with the prior online instance (an add of the live pod).
	live := newValidPod("msp", "pod-a")
	live.ResourceVersion = "42"
	if instanceFromPod(t, live) == nil {
		t.Fatal("live pod did not convert")
	}
	k.cache.ReplaceOrInsert(instanceFromPod(t, live))

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	ins := k.pod2Instance(obj)
	if ins == nil {
		t.Fatal("pod2Instance(delete of IP-less pod with cached IP) = nil, want the merged offline instance")
	}
	if ins.InstanceId != "pod-a" {
		t.Fatalf("merged instance id = %q, want pod-a", ins.InstanceId)
	}
	if ins.Ip != "172.17.0.9" {
		t.Fatalf("merged instance ip = %q, want the cached ip 172.17.0.9 (the deregister target)", ins.Ip)
	}
	if ins.Status != providers.InstanceStatusOffline {
		t.Fatalf("merged instance status = %d, want %d", ins.Status, providers.InstanceStatusOffline)
	}
	if ins.State != providers.InstanceStateTerminated {
		t.Fatalf("merged instance state = %q, want %q", ins.State, providers.InstanceStateTerminated)
	}
	if ins.Enabled {
		t.Fatal("merged instance enabled = true, want false")
	}
	if ins.Reversion != 50 {
		t.Fatalf("merged instance reversion = %d, want 50 (the pod's newest observed resource version)", ins.Reversion)
	}
	if ins.AppCode != "pay-user" {
		t.Fatalf("merged instance appcode = %q, want the cached pay-user", ins.AppCode)
	}

	// The cache must hold the full record afterwards (the diff flow inserted it).
	cached := k.cache.Get("pod-a")
	if cached == nil {
		t.Fatal("cache.Get(pod-a) = nil after the merged delete, want the full record")
	}
	if cached.Ip != "172.17.0.9" || cached.Status != providers.InstanceStatusOffline {
		t.Fatalf("cached instance = %#v, want the merged offline record with the recovered ip", cached)
	}
}

// TestPod2InstanceDeleteIPlessPodMergedIsIdempotent: a second delete event
// for the same pod, after the first one merged and cached the full offline
// record, must return nil and not re-push — the cached entry is already
// offline with the recovered ip, so hasInstanceDiff reports no diff.
func TestPod2InstanceDeleteIPlessPodMergedIsIdempotent(t *testing.T) {
	pod := newDeletedIPlessPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	// Seed the cache with the prior online instance (an add of the live pod).
	live := newValidPod("msp", "pod-a")
	live.ResourceVersion = "42"
	k.cache.ReplaceOrInsert(instanceFromPod(t, live))

	del := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	if first := k.pod2Instance(del); first == nil {
		t.Fatal("first delete produced no merged instance")
	}
	if cached := k.cache.Get("pod-a"); cached == nil || cached.Status != providers.InstanceStatusOffline || cached.Ip != "172.17.0.9" {
		t.Fatalf("cached instance after the first delete = %#v, want the offline record with the recovered ip", cached)
	}

	// A repeated delete must not report again: the cached instance is already
	// offline (offline-equal, hasInstanceDiff == false).
	if again := k.pod2Instance(del); again != nil {
		t.Fatalf("pod2Instance(repeated IP-less delete) = %#v, want nil (already offline)", again)
	}

	// The cache still holds the full merged record after the repeat.
	if cached := k.cache.Get("pod-a"); cached == nil || cached.Ip != "172.17.0.9" || cached.Status != providers.InstanceStatusOffline {
		t.Fatalf("cached instance after the repeated delete = %#v, want the merged offline record with the recovered ip", cached)
	}
}

// TestPod2InstanceDeleteIPlessPodWithoutCacheReturnsNil: no prior registered
// instance (empty cache) means nothing was ever pushed to a sink — the
// IP-less offline shell must be dropped, not reported as a syncable instance.
func TestPod2InstanceDeleteIPlessPodWithoutCacheReturnsNil(t *testing.T) {
	pod := newDeletedIPlessPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	if ins := k.pod2Instance(obj); ins != nil {
		t.Fatalf("pod2Instance(delete of IP-less pod with empty cache) = %#v, want nil", ins)
	}
	if k.cache.Get("pod-a") != nil {
		t.Fatal("cache contains pod-a after dropping the shell instance, want no cache write")
	}
}

// TestPod2InstanceDeleteIPlessPodWithIPlessCacheReturnsNil: a cached entry
// whose own Ip is empty cannot be a deregister target either — the drop
// applies exactly as with the empty cache.
func TestPod2InstanceDeleteIPlessPodWithIPlessCacheReturnsNil(t *testing.T) {
	pod := newDeletedIPlessPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	k.cache.ReplaceOrInsert(&sv.Instance{
		InstanceId: "pod-a",
		AppCode:    "pay-user",
		EnvType:    "test",
		Status:     providers.InstanceStatusUnhealthy,
		State:      providers.InstanceStatePending,
		Ip:         "",
		Reversion:  42,
	})

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	if ins := k.pod2Instance(obj); ins != nil {
		t.Fatalf("pod2Instance(delete of IP-less pod with IP-less cache) = %#v, want nil", ins)
	}
}

// TestPod2InstanceDeletedPodWithIPOwnIPWins: regression — a deleted pod that
// has its own IP keeps the unchanged behavior: the instance is reported with
// the pod's own IP (no cache merge), and pushed offline.
func TestPod2InstanceDeletedPodWithIPOwnIPWins(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventDelete}
	ins := k.pod2Instance(obj)
	if ins == nil {
		t.Fatal("pod2Instance(delete of pod with IP) = nil, want the offline instance")
	}
	if ins.Ip != pod.Status.PodIP {
		t.Fatalf("deleted-with-IP instance ip = %q, want the pod's own ip %q", ins.Ip, pod.Status.PodIP)
	}
	if ins.Status != providers.InstanceStatusOffline {
		t.Fatalf("deleted-with-IP instance status = %d, want %d", ins.Status, providers.InstanceStatusOffline)
	}
}

func TestPod2InstanceAddWithoutPodInRobotReturnsNil(t *testing.T) {
	robot := newFakeRobot(nil, nil, false) // GetByKey miss
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	add := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-a", Event: k8srobot.EventAdd}
	if ins := k.pod2Instance(add); ins != nil {
		t.Fatalf("pod2Instance(add without pod in robot) = %#v, want nil", ins)
	}
}

func TestPod2InstanceSkipsInvalidInstance(t *testing.T) {
	// A pod whose instance has no env type is rejected by the instance
	// filter (InitInstanceFilters): pod2Instance must return nil without
	// caching the instance. formatEnvType derives the env type from the
	// container env "K8S_CLUSTER_TYPE", so a pod without it (and without an
	// env-type label) converts to an instance with an empty EnvType.
	pod := newValidPod("msp", "pod-b")
	pod.Spec.Containers = nil
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-b": {pod}}, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-b", Event: k8srobot.EventAdd}
	if ins := k.pod2Instance(obj); ins != nil {
		t.Fatalf("pod2Instance(pod without env type) = %#v, want nil", ins)
	}
	if k.cache.Get("pod-b") != nil {
		t.Fatal("cache contains pod-b after an invalid add, want no cache write")
	}
}

// -----------------------------------------------------------------------------
// ProcessCache
// -----------------------------------------------------------------------------

func TestProcessCacheReplaceOrInsertOnAddUpdateDelete(t *testing.T) {
	w := &fakeWorker{}
	k := newTestProvider(newFakeRobot(nil, nil, false), w)

	ins := &sv.Instance{InstanceId: "pod-a", AppCode: "pay-user", EnvType: "test", Status: providers.InstanceStatusOnline, Reversion: 1}

	for _, event := range []k8srobot.EventType{k8srobot.EventAdd, k8srobot.EventUpdate, k8srobot.EventDelete} {
		k.ProcessCache(event, ins)
		cached := k.cache.Get("pod-a")
		if cached == nil {
			t.Fatalf("cache.Get(pod-a) after %s event = nil, want the instance", event)
		}
		if cached.Status != providers.InstanceStatusOnline {
			t.Fatalf("cached status after %s event = %d, want %d", event, cached.Status, providers.InstanceStatusOnline)
		}
	}

	// Mutating the source instance after ProcessCache must not leak into the
	// cache (the btree cache stores a deep copy).
	ins.Status = providers.InstanceStatusOffline
	if cached := k.cache.Get("pod-a"); cached.Status != providers.InstanceStatusOnline {
		t.Fatalf("cache leaked a mutated status: %d, want %d", cached.Status, providers.InstanceStatusOnline)
	}
}

// -----------------------------------------------------------------------------
// eventSync
// -----------------------------------------------------------------------------

func TestEventSyncForwardsInstanceToWorker(t *testing.T) {
	w := &fakeWorker{}
	k := newTestProvider(newFakeRobot(nil, nil, false), w)

	ins := &sv.Instance{InstanceId: "pod-a", Status: providers.InstanceStatusOnline, Reversion: 1}
	k.eventSync(ins, 1234567890)

	events := w.handleSnapshot()
	if len(events) != 1 {
		t.Fatalf("worker.Handle calls = %d, want 1", len(events))
	}
	e := events[0]
	if e.Operate != worker.OperateTypeSync {
		t.Fatalf("event operate = %q, want %q", e.Operate, worker.OperateTypeSync)
	}
	if e.Trigger != 1234567890 {
		t.Fatalf("event trigger = %d, want 1234567890", e.Trigger)
	}
	if len(e.Data) != 1 || e.Data[0].InstanceId != "pod-a" {
		t.Fatalf("event data = %#v, want the pod-a instance", e.Data)
	}
}

// -----------------------------------------------------------------------------
// VerifyInstance / GetAll (docs/testing.md backlog item 7)
// -----------------------------------------------------------------------------

func TestVerifyInstanceUsesFilters(t *testing.T) {
	k := newTestProvider(newFakeRobot(nil, nil, false), &fakeWorker{})

	valid := &sv.Instance{
		InstanceId: "pod-a",
		AppCode:    "pay-user",
		EnvType:    "test",
		State:      providers.InstanceStateRunning,
		Status:     providers.InstanceStatusOnline,
		Ip:         "1.1.1.1",
		Reversion:  1,
	}
	if err := k.VerifyInstance(valid); err != nil {
		t.Fatalf("VerifyInstance(valid) error = %v, want nil", err)
	}

	noAppcode := &sv.Instance{InstanceId: "pod-b", EnvType: "test", Status: providers.InstanceStatusOnline, Reversion: 1}
	if err := k.VerifyInstance(noAppcode); err == nil {
		t.Fatal("VerifyInstance(instance without appcode) = nil error, want an error")
	}

	zeroReversion := *valid
	zeroReversion.Reversion = 0
	if err := k.VerifyInstance(&zeroReversion); err == nil {
		t.Fatal("VerifyInstance(instance with zero reversion) = nil error, want an error")
	}
}

func TestGetAllFiltersInvalidInstances(t *testing.T) {
	valid := newValidPod("msp", "pod-valid")
	// Pending pod with an empty IP: filtered out by the default filter.
	invalid := newValidPod("msp", "pod-invalid")
	invalid.Status.Phase = corev1.PodPending
	invalid.Status.PodIP = ""
	invalid.Status.ContainerStatuses = nil

	robot := newFakeRobot(nil, []interface{}{valid, invalid}, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	all := k.GetAll()
	if len(all) != 1 {
		t.Fatalf("GetAll() = %d instances, want 1 (invalid filtered)", len(all))
	}
	if all[0].InstanceId != "pod-valid" {
		t.Fatalf("GetAll()[0].InstanceId = %q, want pod-valid", all[0].InstanceId)
	}
}

// -----------------------------------------------------------------------------
// CompareAndFlush (docs/testing.md backlog item 1)
// -----------------------------------------------------------------------------

// remoteInstance builds an instance as the discovery center would report it.
func remoteInstance(id string, status int32, reversion int64) *sv.Instance {
	return &sv.Instance{
		InstanceId: id,
		AppCode:    "pay-user",
		EnvType:    "test",
		EnvGroup:   "7",
		State:      providers.InstanceStateRunning,
		Status:     status,
		Ip:         "1.1.1.1",
		Reversion:  reversion,
	}
}

// findLastEvent returns the most recent event carrying the given instance id.
func findLastEvent(t *testing.T, events []*worker.Event, instanceID string) *worker.Event {
	t.Helper()
	var found *worker.Event
	for _, e := range events {
		if len(e.Data) == 1 && e.Data[0].InstanceId == instanceID {
			found = e
		}
	}
	return found
}

func findEvent(t *testing.T, events []*worker.Event, instanceID string) *worker.Event {
	t.Helper()
	for _, e := range events {
		if len(e.Data) == 1 && e.Data[0].InstanceId == instanceID {
			return e
		}
	}
	return nil
}

func TestCompareAndFlushEmptyRemoteListPushesAll(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}} // remote: empty
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 1)
	if e := findEvent(t, events, "pod-a"); e == nil {
		t.Fatalf("no push for pod-a after CompareAndFlush with empty remote list; events = %#v", events)
	} else {
		if e.Operate != worker.OperateTypeSync {
			t.Fatalf("push operate = %q, want %q", e.Operate, worker.OperateTypeSync)
		}
		if len(e.Data) != 1 || e.Data[0].Status != providers.InstanceStatusOnline {
			t.Fatalf("pushed instance = %#v, want the online k8s instance", e.Data)
		}
	}

	// The pushed instance must also be in the provider cache.
	if k.cache.Get("pod-a") == nil {
		t.Fatal("cache does not contain pod-a after CompareAndFlush")
	}
}

func TestCompareAndFlushGetAllErrorPushesAll(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllErr: errFakeGetAll}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 1)
	if findEvent(t, events, "pod-a") == nil {
		t.Fatalf("no push for pod-a when worker.GetAll fails; events = %#v", events)
	}
}

var errFakeGetAll = errors.New("atlas unreachable")

func TestCompareAndFlushPushesNewerInstanceWhenBothExist(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	pod.ResourceVersion = "100" // k8s reversion 100

	// Remote copy of the same instance, older revision.
	remote := remoteInstance("pod-a", providers.InstanceStatusOnline, 50)
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	// The reconciliation must have consulted the remote list exactly once.
	if got := w.getAllCallCount(); got != 1 {
		t.Fatalf("worker.GetAll calls = %d, want 1", got)
	}

	events := w.waitForHandles(t, 1)
	e := findEvent(t, events, "pod-a")
	if e == nil {
		t.Fatalf("no push for the newer pod-a; events = %#v", events)
	}
	if e.Data[0].Reversion != 100 {
		t.Fatalf("pushed reversion = %d, want 100 (k8s data wins)", e.Data[0].Reversion)
	}
}

func TestCompareAndFlushSkipsEqualInstanceWhenBothExist(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	pod.ResourceVersion = "100"

	// Remote copy that matches the converted k8s instance exactly (the
	// converter derives the fields deterministically from the pod).
	remote := instanceFromPod(t, pod)
	remote.Status = providers.InstanceStatusOnline
	remote.State = providers.InstanceStateRunning
	remote.Ip = pod.Status.PodIP
	remote.Reversion = 100

	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	// No diff: nothing must be pushed for pod-a.
	time.Sleep(100 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (no diff between equal instances)", len(events))
	}
}

func TestCompareAndFlushPushesK8sOnlyOnlineInstance(t *testing.T) {
	pod := newValidPod("msp", "pod-new")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	// Remote reports some other, unrelated instance.
	remote := remoteInstance("pod-other", providers.InstanceStatusOnline, 5)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 2)
	if e := findEvent(t, events, "pod-new"); e == nil {
		t.Fatalf("no push for the k8s-only online instance pod-new; events = %#v", events)
	}
	// The unrelated remote instance is also remote-only: pushed offline.
	if e := findEvent(t, events, "pod-other"); e == nil {
		t.Fatalf("no offline push for the remote-only instance pod-other; events = %#v", events)
	} else {
		if e.Data[0].Status != providers.InstanceStatusOffline {
			t.Fatalf("remote-only push status = %d, want %d", e.Data[0].Status, providers.InstanceStatusOffline)
		}
	}
}

func TestCompareAndFlushMarksRemoteOnlyInstanceOffline(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	remote := remoteInstance("pod-ghost", providers.InstanceStatusOnline, 9)
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	// Two pushes: pod-a (k8s-only, online) and pod-ghost (remote-only, offline).
	events := w.waitForHandles(t, 2)
	e := findEvent(t, events, "pod-ghost")
	if e == nil {
		t.Fatalf("no push for the remote-only instance pod-ghost; events = %#v", events)
	}
	got := e.Data[0]
	if got.Status != providers.InstanceStatusOffline {
		t.Fatalf("remote-only instance status = %d, want %d (offline)", got.Status, providers.InstanceStatusOffline)
	}
	if got.State != providers.InstanceStateTerminated {
		t.Fatalf("remote-only instance state = %q, want %q", got.State, providers.InstanceStateTerminated)
	}
	if got.Enabled {
		t.Fatal("remote-only instance enabled = true, want false")
	}
}

func TestCompareAndFlushPushAppCodesFiltersRemoteList(t *testing.T) {
	pod := newValidPod("msp", "pod-a") // appcode pay-user, reversion 42

	// Remote contains one instance of the allowed appcode (newer than k8s)
	// and one instance of a different appcode.
	remoteAllowed := remoteInstance("pod-a", providers.InstanceStatusOnline, 100)
	remoteAllowed.AppCode = "pay-user"
	remoteOther := remoteInstance("pod-other", providers.InstanceStatusOnline, 5)
	remoteOther.AppCode = "somebody-else"

	savedPushAppCodes := config.PushAppCodes
	config.PushAppCodes = []string{"pay-user"}
	defer func() { config.PushAppCodes = savedPushAppCodes }()

	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remoteAllowed, remoteOther}}}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 1)
	// pod-a exists on both sides. The remote copy carries an env group that
	// differs from the k8s one (""), so a field diff exists and the k8s data
	// wins: exactly one push for pod-a, carrying the k8s reversion (42).
	// The remote-only pod-other instance was filtered out of the remote list
	// by PushAppCodes, so it must NOT be marked offline.
	if e := findEvent(t, events, "pod-other"); e != nil {
		t.Fatalf("push for appcode-filtered remote instance pod-other = %#v, want none", e)
	}
	if len(events) != 1 {
		t.Fatalf("pushed events = %d, want 1 (the pod-a field-diff push)", len(events))
	}
	e := events[0]
	if e.Data[0].InstanceId != "pod-a" {
		t.Fatalf("pushed instance = %q, want pod-a", e.Data[0].InstanceId)
	}
	if e.Data[0].Reversion != 42 {
		t.Fatalf("pushed pod-a reversion = %d, want 42 (k8s data wins the diff push)", e.Data[0].Reversion)
	}
	if e.Data[0].Status != providers.InstanceStatusOnline {
		t.Fatalf("pushed pod-a status = %d, want %d (online, k8s data wins)", e.Data[0].Status, providers.InstanceStatusOnline)
	}
}

func TestCompareAndFlushEmptyPodListDoesNothing(t *testing.T) {
	robot := newFakeRobot(nil, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	k.CompareAndFlush()

	time.Sleep(50 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (no pods)", len(events))
	}
}

// -----------------------------------------------------------------------------
// buildAndSendEvent
// -----------------------------------------------------------------------------

func TestBuildAndSendEventSkipsStatusZero(t *testing.T) {
	w := &fakeWorker{}
	k := newTestProvider(newFakeRobot(nil, nil, false), w)

	k.buildAndSendEvent(&sv.Instance{InstanceId: "pod-a", Status: 0})

	// The pool submission runs asynchronously: wait a bounded time and then
	// assert that no Handle call happened.
	time.Sleep(200 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (status 0 must not be pushed)", len(events))
	}
}

func TestBuildAndSendEventPushesInstance(t *testing.T) {
	w := &fakeWorker{}
	k := newTestProvider(newFakeRobot(nil, nil, false), w)

	k.buildAndSendEvent(&sv.Instance{InstanceId: "pod-a", Status: providers.InstanceStatusOnline})

	events := w.waitForHandles(t, 1)
	if e := findEvent(t, events, "pod-a"); e == nil {
		t.Fatalf("no push for pod-a; events = %#v", events)
	} else if e.Operate != worker.OperateTypeSync {
		t.Fatalf("push operate = %q, want %q", e.Operate, worker.OperateTypeSync)
	}
}

// -----------------------------------------------------------------------------
// flushInstances
// -----------------------------------------------------------------------------

func TestFlushInstancesClearsAndRefillsCacheAndPushesAll(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	// Stale cache entry that must be gone after the flush.
	k.cache.ReplaceOrInsert(&sv.Instance{InstanceId: "pod-stale", Status: providers.InstanceStatusOffline})

	k.flushInstances()

	events := w.waitForHandles(t, 1)
	if len(events) != 1 || events[0].Operate != worker.OperateTypeSyncAll {
		t.Fatalf("events = %#v, want a single SyncAll event", events)
	}
	if len(events[0].Data) != 1 || events[0].Data[0].InstanceId != "pod-a" {
		t.Fatalf("SyncAll data = %#v, want the pod-a instance", events[0].Data)
	}

	if k.cache.Get("pod-stale") != nil {
		t.Fatal("stale cache entry survived the flush")
	}
	if k.cache.Get("pod-a") == nil {
		t.Fatal("flushed instance not present in the cache")
	}
}

func TestFlushInstancesWithNoPodsDoesNothing(t *testing.T) {
	robot := newFakeRobot(nil, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	k.flushInstances()

	time.Sleep(50 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (no pods)", len(events))
	}
}

// -----------------------------------------------------------------------------
// ProcessIntervalFullPush (bounded: short interval, ctx cancel)
// -----------------------------------------------------------------------------

func TestProcessIntervalFullPushRunsUntilContextCancel(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx
	// 2 second interval: the goroutine must return after ctx cancel.
	k.interval = 2

	done := make(chan struct{})
	go func() {
		k.ProcessIntervalFullPush()
		close(done)
	}()

	// The first tick fires a CompareAndFlush (empty remote: one push).
	events := w.waitForHandleInterval(t, 1, 2)
	if findEvent(t, events, "pod-a") == nil {
		t.Fatalf("no push after the first interval tick; events = %#v", events)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}
}

func TestProcessIntervalFullPushDefaultIntervalWhenZero(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx
	k.interval = 0 // default: 21600s, no tick within the test

	done := make(chan struct{})
	go func() {
		k.ProcessIntervalFullPush()
		close(done)
	}()

	// No push may happen: cancel and the goroutine must return without any
	// CompareAndFlush run.
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (no tick with the default interval)", len(events))
	}
}

// -----------------------------------------------------------------------------
// monitor / Run (bounded through the ctx seam and a synced robot)
// -----------------------------------------------------------------------------

func TestMonitorStopsOnContextCancel(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, []interface{}{pod}, true)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx

	done := make(chan struct{})
	go func() {
		k.Run()
		close(done)
	}()

	// Run -> monitor: full push happens (empty remote list).
	events := w.waitForHandles(t, 1)
	if findEvent(t, events, "pod-a") == nil {
		t.Fatalf("no full push from monitor; events = %#v", events)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return after context cancel")
	}
	if !robot.wasStopped() {
		t.Fatal("robot.Stop() was not called by monitor")
	}
}

func TestMonitorProcessesPoppedEvents(t *testing.T) {
	// The initial (List-served) pod snapshot has reversion 7; after the full
	// push caches it, the pod is updated in the cluster (the robot's GetByKey
	// store) to reversion 8. The popped update event must then produce a
	// second sync push for pod-a with the new reversion.
	pod := newValidPod("msp", "pod-a")
	pod.ResourceVersion = "7"
	updatedPod := newValidPod("msp", "pod-a")
	updatedPod.ResourceVersion = "8"

	robot := newFakeRobot(map[string][]interface{}{"msp/pod-a": {pod}}, []interface{}{pod}, true)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx

	// After the full push caches the reversion-7 instance, the cluster update
	// lands: the GetByKey store starts serving the reversion-8 pod and the
	// update event is enqueued, which unblocks the monitor's Pop loop (Pop
	// blocks on the scripted queue while it is empty; it never busy-spins).
	// The context cancel ends the loop for good: monitor's deferred
	// robot.Stop() makes Pop return an error, and k.stopped is already true
	// at that point, so the loop breaks instead of retrying.
	swap := func() {
		robot.mu.Lock()
		robot.byKey["msp/pod-a"] = []interface{}{updatedPod}
		robot.mu.Unlock()
		robot.enqueue(k8srobot.QueueObject{
			RType:    k8srobot.Pods,
			Key:      "msp/pod-a",
			Event:    k8srobot.EventUpdate,
			CreateAt: time.Now(),
		})
	}

	done := make(chan struct{})
	go func() {
		k.Run()
		close(done)
	}()

	// Run -> monitor: full push happens (empty remote list) and caches
	// reversion 7. Then swap in the update and wait for the second push.
	events := w.waitForHandles(t, 1)
	if e := findEvent(t, events, "pod-a"); e == nil {
		t.Fatalf("no full push from monitor; events = %#v", events)
	} else if e.Data[0].Reversion != 7 {
		t.Fatalf("full push reversion = %d, want 7", e.Data[0].Reversion)
	}
	swap()

	events = w.waitForHandles(t, 2)
	if e := findLastEvent(t, events, "pod-a"); e == nil {
		t.Fatalf("popped update event was not synced; events = %#v", events)
	} else if e.Data[0].Reversion != 8 {
		t.Fatalf("popped update push reversion = %d, want 8", e.Data[0].Reversion)
	}
	// The popped object must have been acknowledged.
	robot.waitForFinish(t, 1)

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run() did not return after context cancel")
	}
}

// -----------------------------------------------------------------------------
// obj2InstanceId
// -----------------------------------------------------------------------------

func TestObj2InstanceId(t *testing.T) {
	k := &k8s{}

	tests := []struct {
		key  string
		want string
	}{
		{"msp/pod-a", "pod-a"},
		{"ns1/name-with-dashes", "name-with-dashes"},
		{"pod-a", ""},
		{"", ""},
		// Multi-slash shapes: the contract is keys[1] (the SECOND slash
		// segment), not the last. These PIN the current behavior — fragile
		// (a key not shaped "namespace/name" yields something that is not
		// an instance id) but latent: the k8s robot only ever enqueues
		// two-segment keys. The audit chose documenting over changing.
		{"ns/sub/pod", "sub"},
		{"ns/a/b/c/pod", "a"},
		{"/pod", "pod"},
	}
	for _, tc := range tests {
		if got := k.obj2InstanceId(k8srobot.QueueObject{Key: tc.key}); got != tc.want {
			t.Fatalf("obj2InstanceId(key %q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

// -----------------------------------------------------------------------------
// ProcessIntervalFullPush SyncAll trigger (plan §7.4)
// -----------------------------------------------------------------------------

// findSyncAllEvents returns every recorded SyncAll event.
func findSyncAllEvents(events []*worker.Event) []*worker.Event {
	var found []*worker.Event
	for _, e := range events {
		if e.Operate == worker.OperateTypeSyncAll {
			found = append(found, e)
		}
	}
	return found
}

// findSyncEvent returns the first incremental (OperateTypeSync) event
// carrying the given instance id.
func findSyncEvent(events []*worker.Event, instanceID string) *worker.Event {
	for _, e := range events {
		if e.Operate != worker.OperateTypeSync {
			continue
		}
		if len(e.Data) == 1 && e.Data[0].InstanceId == instanceID {
			return e
		}
	}
	return nil
}

// TestProcessIntervalFullPushEmitsSyncAllAfterCompareAndFlush: after each
// interval tick's CompareAndFlush, the provider also emits exactly one
// OperateTypeSyncAll event carrying the full GetAll list — the dormant
// worker path plan §7.4 revives so the fan-out (and the Nacos PushAll prune)
// runs every push interval.
func TestProcessIntervalFullPushEmitsSyncAllAfterCompareAndFlush(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx
	k.interval = 2

	done := make(chan struct{})
	go func() {
		k.ProcessIntervalFullPush()
		close(done)
	}()

	// One tick: the CompareAndFlush pushes (empty remote list: one Sync
	// event for pod-a) plus the SyncAll event carrying the full list. The
	// incremental push is pool-submitted (asynchronous), so wait for each
	// observable separately with tick-specific conditions: exactly one
	// SyncAll, and at least one Sync event for pod-a.
	interval := 2 * time.Second
	deadline := time.Now().Add(interval + 2*time.Second)
	var events []*worker.Event
	for time.Now().Before(deadline) {
		events = w.handleSnapshot()
		if len(findSyncAllEvents(events)) == 1 && findSyncEvent(events, "pod-a") != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The SyncAll event must be present exactly once, carrying the full list.
	syncAlls := findSyncAllEvents(events)
	if len(syncAlls) != 1 {
		t.Fatalf("SyncAll events = %d, want exactly 1 per tick; events = %#v", len(syncAlls), events)
	}
	// The tick's CompareAndFlush also ran: its incremental push for pod-a.
	if e := findSyncEvent(events, "pod-a"); e == nil {
		t.Fatalf("no incremental push after the tick; events = %#v", events)
	}
	event := syncAlls[0]
	if len(event.Data) != 1 || event.Data[0].InstanceId != "pod-a" {
		t.Fatalf("SyncAll data = %#v, want the full provider list (pod-a)", event.Data)
	}
	if event.Trigger <= 0 {
		t.Fatalf("SyncAll trigger = %d, want the tick timestamp", event.Trigger)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}
}

// TestProcessIntervalFullPushEmitsEmptySyncAllWithoutInstances: a tick whose
// provider source is empty still emits exactly one SyncAll event carrying an
// EMPTY data list (AUDIT-B-4): the emission keeps the full-push reconcile
// cadence uniform — every sink's PushAll runs each interval. The empty list
// itself is a conservative no-op at the Nacos sink (a bare empty push
// carries no provider identity, so nothing remembered is swept — see the
// sink's remembered field); the vanished-service heal rides the provider's
// non-empty pushes. No incremental (Sync) events may be emitted. The
// pre-B-4 behavior (suppressing the event entirely) broke the cadence for
// no benefit: the next non-empty push would reconcile everything anyway.
func TestProcessIntervalFullPushEmitsEmptySyncAllWithoutInstances(t *testing.T) {
	robot := newFakeRobot(nil, nil, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx
	k.interval = 1

	done := make(chan struct{})
	go func() {
		k.ProcessIntervalFullPush()
		close(done)
	}()

	// Wait past one tick for the empty SyncAll event.
	deadline := time.Now().Add(3 * time.Second)
	var events []*worker.Event
	for time.Now().Before(deadline) {
		events = w.handleSnapshot()
		if len(findSyncAllEvents(events)) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	syncAlls := findSyncAllEvents(events)
	if len(syncAlls) != 1 {
		t.Fatalf("SyncAll events = %d, want exactly 1 per tick (an empty list still reconciles); events = %#v", len(syncAlls), events)
	}
	event := syncAlls[0]
	if event.Data == nil {
		t.Fatalf("SyncAll data = nil, want an empty non-nil list (the empty reconcile signal)")
	}
	if len(event.Data) != 0 {
		t.Fatalf("SyncAll data = %#v, want an empty list", event.Data)
	}
	if event.Trigger <= 0 {
		t.Fatalf("SyncAll trigger = %d, want the tick timestamp", event.Trigger)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ProcessIntervalFullPush did not return after context cancel")
	}
	// The tick's CompareAndFlush also stayed quiet: an empty provider list
	// produces no incremental pushes.
	events = w.handleSnapshot()
	if n := len(events) - len(findSyncAllEvents(events)); n != 0 {
		t.Fatalf("non-SyncAll events = %d, want 0 (empty provider list: no incremental pushes); events = %#v", n, events)
	}
}

// -----------------------------------------------------------------------------
// Conversion boundary: formatStatus / formatState / formatContainerEnabled
// (audit batch 2, AUDIT-C-1/C-2/C-3)
// -----------------------------------------------------------------------------

// containerStatus is a helper for building one ContainerStatus: a container in
// the given waiting reason with the given last-termination reason, not ready.
func containerStatus(waiting, lastTermination string, ready bool) corev1.ContainerStatus {
	cs := corev1.ContainerStatus{Ready: ready}
	if waiting != "" {
		cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: waiting}
	}
	if lastTermination != "" {
		cs.LastTerminationState.Terminated = &corev1.ContainerStateTerminated{Reason: lastTermination}
	}
	if ready {
		cs.State.Running = &corev1.ContainerStateRunning{}
	}
	return cs
}

// TestFormatStateRunningContainers: the Running branch of formatState — the
// crash/error precedence (a CrashLoopBackOff state set inside the container
// scan must survive the scan, and Error must win over Crash when both are
// present), the probing fallback for plain not-ready containers, and running
// only when all containers are ready.
func TestFormatStateRunningContainers(t *testing.T) {
	tests := []struct {
		name     string
		statuses []corev1.ContainerStatus
		want     string
	}{
		{
			name:     "all containers ready is running",
			statuses: []corev1.ContainerStatus{containerStatus("", "", true)},
			want:     providers.InstanceStateRunning,
		},
		{
			name:     "crash loop with last termination Error is error",
			statuses: []corev1.ContainerStatus{containerStatus("CrashLoopBackOff", "Error", false)},
			want:     providers.InstanceStateError,
			// AUDIT-C-1: CrashLoopBackOff + LastTerminationState Reason
			// Error must surface as error.
		},
		{
			name:     "crash loop without last termination state is crash",
			statuses: []corev1.ContainerStatus{containerStatus("CrashLoopBackOff", "", false)},
			want:     providers.InstanceStateCrash,
			// AUDIT-C-1: plain CrashLoopBackOff must surface as crash, never
			// unknown.
		},
		{
			name: "error wins over crash when both appear across containers",
			statuses: []corev1.ContainerStatus{
				containerStatus("CrashLoopBackOff", "", false),
				containerStatus("CrashLoopBackOff", "Error", false),
			},
			want: providers.InstanceStateError,
			// AUDIT-C-1: error precedence over crash is deliberate (the
			// stronger signal).
		},
		{
			name: "crash over probing when both appear across containers",
			statuses: []corev1.ContainerStatus{
				containerStatus("ContainerCreating", "", false),
				containerStatus("CrashLoopBackOff", "", false),
			},
			want: providers.InstanceStateCrash,
		},
		{
			name:     "not ready with ContainerCreating is probing",
			statuses: []corev1.ContainerStatus{containerStatus("ContainerCreating", "", false)},
			want:     providers.InstanceStateProbing,
		},
		{
			name:     "not ready with no waiting reason is probing",
			statuses: []corev1.ContainerStatus{containerStatus("", "", false)},
			want:     providers.InstanceStateProbing,
		},
		{
			name:     "running pod with empty ContainerStatuses is probing (kubelet not caught up)",
			statuses: nil,
			want:     providers.InstanceStateProbing,
			// AUDIT-C-3: n>0 containers expected but none reported.
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newValidPod("msp", "pod-state")
			pod.Status.ContainerStatuses = tc.statuses
			if got := formatState(pod); got != tc.want {
				t.Fatalf("formatState() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestFormatStateTerminalPhases pins the non-Running branches of formatState.
func TestFormatStateTerminalPhases(t *testing.T) {
	tests := []struct {
		name   string
		phase  corev1.PodPhase
		reason string
		want   string
	}{
		{name: "pending", phase: corev1.PodPending, want: providers.InstanceStatePending},
		{name: "unknown", phase: corev1.PodUnknown, want: providers.InstanceStateUnknown},
		{name: "failed evicted", phase: corev1.PodFailed, reason: "Evicted", want: providers.InstanceStateEvicted},
		{name: "failed other reason", phase: corev1.PodFailed, reason: "OOMKilled", want: providers.InstanceStateFailed},
		{name: "failed no reason", phase: corev1.PodFailed, want: providers.InstanceStateFailed},
		{name: "succeeded", phase: corev1.PodSucceeded, want: providers.InstanceStateTerminated},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newValidPod("msp", "pod-phase")
			pod.Status.Phase = tc.phase
			pod.Status.Reason = tc.reason
			pod.Status.ContainerStatuses = nil
			if got := formatState(pod); got != tc.want {
				t.Fatalf("formatState() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestFormatStatusPinTerminalSemantics pins the Succeeded/Failed(non-Evicted)
// OFFLINE decision (AUDIT-C-2): completed/failed pods no longer serve traffic,
// so the sink receives a deregistration for the live object on purpose. A
// mutation flipping them online (or dropping the branch) must fail here.
func TestFormatStatusPinTerminalSemantics(t *testing.T) {
	tests := []struct {
		name   string
		phase  corev1.PodPhase
		reason string
		want   int32
	}{
		{
			name:  "succeeded converts offline (completed pods no longer serve)",
			phase: corev1.PodSucceeded,
			want:  providers.InstanceStatusOffline,
		},
		{
			name:   "failed non-evicted converts offline",
			phase:  corev1.PodFailed,
			reason: "OOMKilled",
			want:   providers.InstanceStatusOffline,
		},
		{
			name:  "failed without reason converts offline",
			phase: corev1.PodFailed,
			want:  providers.InstanceStatusOffline,
		},
		{
			name:   "failed evicted converts unhealthy (recreated by the controller)",
			phase:  corev1.PodFailed,
			reason: "Evicted",
			want:   providers.InstanceStatusUnhealthy,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newValidPod("msp", "pod-terminal")
			pod.Status.Phase = tc.phase
			pod.Status.Reason = tc.reason
			pod.Status.ContainerStatuses = nil
			if got := formatStatus(nil, pod); got != tc.want {
				t.Fatalf("formatStatus() = %d, want %d", got, tc.want)
			}
			// The state side of the same shape: formatState must agree with
			// the terminal state for the pin (offline/terminated pair).
			if tc.phase == corev1.PodSucceeded {
				if got := formatState(pod); got != providers.InstanceStateTerminated {
					t.Fatalf("formatState(succeeded) = %q, want %q", got, providers.InstanceStateTerminated)
				}
			}
			if tc.phase == corev1.PodFailed && tc.reason != "Evicted" {
				if got := formatState(pod); got != providers.InstanceStateFailed {
					t.Fatalf("formatState(failed) = %q, want %q", got, providers.InstanceStateFailed)
				}
			}
		})
	}
}

// TestFormatStatusRunningPhases pins the Running branch including the
// AUDIT-C-3 fix: a Running pod with zero ContainerStatuses is NOT ready (the
// vacuous "all containers ready" of the empty loop), so status is unhealthy
// and enabled false — whether or not the PodIP was already allocated (the
// early-start shape is exactly C-4's precondition; a real instance awaiting
// readiness, as opposed to the empty-IP offline shell the sink guard skips).
func TestFormatStatusRunningPhases(t *testing.T) {
	tests := []struct {
		name        string
		statuses    []corev1.ContainerStatus
		ip          string
		want        int32
		wantEnabled bool
		wantState   string
		pending     bool
	}{
		{
			name:        "all ready converts online and enabled",
			statuses:    []corev1.ContainerStatus{containerStatus("", "", true)},
			ip:          "172.17.0.9",
			want:        providers.InstanceStatusOnline,
			wantEnabled: true,
			wantState:   providers.InstanceStateRunning,
		},
		{
			name:        "not ready container converts unhealthy and disabled",
			statuses:    []corev1.ContainerStatus{containerStatus("ContainerCreating", "", false)},
			ip:          "172.17.0.9",
			want:        providers.InstanceStatusUnhealthy,
			wantEnabled: false,
			wantState:   providers.InstanceStateProbing,
		},
		{
			name:        "running with nil statuses converts unhealthy, not vacuously online",
			statuses:    nil,
			ip:          "172.17.0.9",
			want:        providers.InstanceStatusUnhealthy,
			wantEnabled: false,
			wantState:   providers.InstanceStateProbing,
			// AUDIT-C-3: empty ContainerStatuses must not count as ready=true.
		},
		{
			name:        "running with empty statuses slice is the same vacuous-online escape",
			statuses:    []corev1.ContainerStatus{},
			ip:          "172.17.0.9",
			want:        providers.InstanceStatusUnhealthy,
			wantEnabled: false,
			wantState:   providers.InstanceStateProbing,
		},
		{
			name:        "running with nil statuses and pod ip set (early start) still unhealthy",
			statuses:    nil,
			ip:          "10.0.0.5",
			want:        providers.InstanceStatusUnhealthy,
			wantEnabled: false,
			wantState:   providers.InstanceStateProbing,
			// AUDIT-C-4 precondition: status 2 + ip set is a real instance
			// awaiting readiness, not an empty-IP shell.
		},
		{
			name:        "running with nil statuses and no ip (C-4 empty-ip shape)",
			statuses:    nil,
			ip:          "",
			want:        providers.InstanceStatusUnhealthy,
			wantEnabled: false,
			wantState:   providers.InstanceStateProbing,
		},
		{
			name:        "pending converts unhealthy",
			statuses:    nil,
			ip:          "",
			want:        providers.InstanceStatusUnhealthy,
			wantEnabled: false,
			wantState:   providers.InstanceStatePending,
			pending:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := newValidPod("msp", "pod-status")
			pod.Status.ContainerStatuses = tc.statuses
			pod.Status.PodIP = tc.ip
			if tc.pending {
				pod.Status.Phase = corev1.PodPending
			}
			if got := formatStatus(nil, pod); got != tc.want {
				t.Fatalf("formatStatus() = %d, want %d", got, tc.want)
			}
			if got := formatContainerEnabled(pod); got != tc.wantEnabled {
				t.Fatalf("formatContainerEnabled() = %v, want %v", got, tc.wantEnabled)
			}
			if got := formatState(pod); got != tc.wantState {
				t.Fatalf("formatState() = %q, want %q", got, tc.wantState)
			}
		})
	}
}

// TestFormatInstanceRunningEmptyStatusesFlowsThrough: the converted instance
// (through formatInstance, the production entry) for a Running pod with no
// container statuses carries unhealthy/probing/disabled — the
// vacuous-online-on-the-wire shape (AUDIT-C-3 end to end at the boundary).
func TestFormatInstanceRunningEmptyStatusesFlowsThrough(t *testing.T) {
	pod := newValidPod("msp", "pod-early")
	pod.Status.ContainerStatuses = nil
	pod.Status.PodIP = "10.0.0.5"

	ins := formatInstance(nil, pod)
	if ins == nil {
		t.Fatal("formatInstance(early running pod) = nil, want an instance")
	}
	if ins.Status != providers.InstanceStatusUnhealthy {
		t.Fatalf("instance status = %d, want %d (unhealthy, not vacuously online)", ins.Status, providers.InstanceStatusUnhealthy)
	}
	if ins.State != providers.InstanceStateProbing {
		t.Fatalf("instance state = %q, want %q", ins.State, providers.InstanceStateProbing)
	}
	if ins.Enabled {
		t.Fatal("instance enabled = true, want false (no container has started)")
	}
	if ins.Ip != "10.0.0.5" {
		t.Fatalf("instance ip = %q, want the pod ip (a real instance awaiting readiness)", ins.Ip)
	}
}

// TestGetAllRunningEmptyStatusesKeepsInstance: the C-4 precondition — a
// Running+empty-statuses pod with a PodIP converts to status 2 (unhealthy)
// with the ip present, which the default filter ACCEPTS (it only rejects
// online+empty-ip and pending-state). That is fine: it is a real instance
// awaiting readiness, and the register path (status 2, ip set) is a normal
// unhealthy upsert. Only the empty-IP variant is the sink guard's concern.
func TestGetAllRunningEmptyStatusesKeepsInstance(t *testing.T) {
	early := newValidPod("msp", "pod-early")
	early.Status.ContainerStatuses = nil
	early.Status.PodIP = "10.0.0.5"

	robot := newFakeRobot(nil, []interface{}{early}, false)
	k := newTestProvider(robot, &fakeWorker{})

	all := k.GetAll()
	if len(all) != 1 {
		t.Fatalf("GetAll() = %d instances, want 1 (unhealthy early pod is a real instance)", len(all))
	}
	if all[0].Status != providers.InstanceStatusUnhealthy || all[0].Ip != "10.0.0.5" {
		t.Fatalf("GetAll()[0] = status %d ip %q, want status 2 with the pod ip", all[0].Status, all[0].Ip)
	}
}

// -----------------------------------------------------------------------------
// Guard against repository artifacts (docs/testing.md section 7).
// -----------------------------------------------------------------------------

func TestNoArtifactsInRepository(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("os.ReadDir(.) error = %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() && (entry.Name() == "logfiles" || strings.HasPrefix(entry.Name(), "k8s-whitebox-")) {
			t.Fatalf("artifact %s leaked into the repository", entry.Name())
		}
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			t.Fatalf("log artifact %s leaked into the repository", entry.Name())
		}
	}
}

// -----------------------------------------------------------------------------
// Trigger unit widening: ns-since-epoch producers (dsca-2 §3 Option b)
// -----------------------------------------------------------------------------

// TestMonitorEmitsNsEpochTriggerOnK8sEventPath pins the unit widening at
// the event path (k8s.go's `obj.CreateAt.UnixNano()`): the Trigger of an
// event produced through the REAL monitor loop — Pop -> pod2Instance ->
// eventSync — must be a ns-since-epoch value. The magnitude check is the
// mutation pin: a regression back to Unix() (seconds) yields ~1.7e9, five
// orders of magnitude below the ns epoch (~1.7e18); a mixed-unit producer
// would make the fan-out's e2e decorator observe ~57 years (the documented
// hazard dsca-2 §6 row 2 exists to prevent).
func TestMonitorEmitsNsEpochTriggerOnK8sEventPath(t *testing.T) {
	// Reversion 8 in the store, 7 in the cache: the update event must pass
	// the diff and produce a Pop-path push (the event path whose Trigger is
	// obj.CreateAt — NOT a CompareAndFlush/buildAndSendEvent tick-time
	// stamp, whose producer site is pinned separately below).
	pod := newValidPod("msp", "pod-ns-epoch")
	pod.ResourceVersion = "7"
	updatedPod := newValidPod("msp", "pod-ns-epoch")
	updatedPod.ResourceVersion = "8"

	robot := newFakeRobot(map[string][]interface{}{"msp/pod-ns-epoch": {pod}}, []interface{}{pod}, true)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	k := newTestProvider(robot, w)
	k.ctx = ctx

	done := make(chan struct{})
	go func() {
		k.Run()
		close(done)
	}()

	// The full push (CompareAndFlush, empty remote list) caches reversion
	// 7; then the store serves reversion 8 and the update event is
	// enqueued — the Pop loop consumes it and eventSync emits the event
	// whose Trigger is the popped object's CreateAt.
	events := w.waitForHandles(t, 1)
	if e := findEvent(t, events, "pod-ns-epoch"); e == nil {
		t.Fatalf("no full push from monitor; events = %#v", events)
	} else if e.Data[0].Reversion != 7 {
		t.Fatalf("full push reversion = %d, want 7", e.Data[0].Reversion)
	}
	stampNs := time.Now().UnixNano()
	robot.mu.Lock()
	robot.byKey["msp/pod-ns-epoch"] = []interface{}{updatedPod}
	robot.mu.Unlock()
	robot.enqueue(k8srobot.QueueObject{
		RType:    k8srobot.Pods,
		Key:      "msp/pod-ns-epoch",
		Event:    k8srobot.EventUpdate,
		CreateAt: time.Now(),
	})

	// The popped-event push: its Trigger is the QueueObject.CreateAt (the
	// informer-callback origin), widened to ns-since-epoch.
	events = w.waitForHandles(t, 2)
	e := findLastEvent(t, events, "pod-ns-epoch")
	if e == nil {
		t.Fatalf("popped update event was not synced; events = %#v", events)
	}
	if e.Data[0].Reversion != 8 {
		t.Fatalf("popped update push reversion = %d, want 8", e.Data[0].Reversion)
	}
	if e.Trigger <= 1e15 {
		t.Fatalf("event trigger = %d, want a ns-since-epoch magnitude (> 1e15; a seconds-valued Unix() trigger is ~1.7e9) — the producer regressed to whole seconds", e.Trigger)
	}
	// And it is not merely large but a plausible "now in ns": the enqueued
	// CreateAt was stamped within this test's window.
	if e.Trigger < stampNs-int64(time.Hour) || e.Trigger > time.Now().UnixNano()+int64(time.Hour) {
		t.Fatalf("event trigger = %d, want within one hour of the enqueue-time ns clock", e.Trigger)
	}

	// Terminate like the existing monitor tests: the context cancel ends
	// the monitor loop (its deferred robot.Stop makes Pop error out, and
	// k.stopped is already true at that point so the loop breaks).
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("k.Run() did not return after context cancel")
	}
}

// TestEmitSyncAllEmitsNsEpochTrigger pins the same widening at the
// full-push producer (emitSyncAll's time.Now().UnixNano()): the SyncAll
// event's Trigger must be ns-epoch too — the fan-out decorator cannot
// tell which producer emitted an Event, so every producer site must agree
// on the unit.
func TestEmitSyncAllEmitsNsEpochTrigger(t *testing.T) {
	pod := newValidPod("msp", "pod-syncall-ns")
	robot := newFakeRobot(map[string][]interface{}{"msp/pod-syncall-ns": {pod}}, []interface{}{pod}, true)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)

	k.emitSyncAll()

	events := w.handleSnapshot()
	syncAlls := findSyncAllEvents(events)
	if len(syncAlls) != 1 {
		t.Fatalf("SyncAll events = %d, want 1; events = %#v", len(syncAlls), events)
	}
	if syncAlls[0].Trigger <= 1e15 {
		t.Fatalf("SyncAll trigger = %d, want a ns-since-epoch magnitude (> 1e15) — the full-push producer regressed to whole seconds", syncAlls[0].Trigger)
	}
}

// -----------------------------------------------------------------------------
// The k8s_queue_depth gauge (dsca-1 DS-1-1 fix item 1, the queueDepth
// observability)
// -----------------------------------------------------------------------------

// TestQueueDepthReporterPublishesRobotDepth pins the gauge's core seam:
// reportQueueDepth (the per-tick call the provider's own goroutine makes)
// reads the robot's QueueDepth and hands exactly that number to the
// recorder's SetK8sQueueDepth. Without a recorder installed, the call is a
// silent no-op (the in-package construction path).
func TestQueueDepthReporterPublishesRobotDepth(t *testing.T) {
	robot := newFakeRobot(nil, nil, false)
	w := &fakeWorker{}
	k := newTestProvider(robot, w)
	metrics := fakes.NewFakeMetricsRecorder()

	// No recorder: no-op, no panic.
	k.reportQueueDepth()
	if got := len(metrics.K8sQueueDepthObservations()); got != 0 {
		t.Fatalf("observations without a recorder = %d, want 0", got)
	}

	k.SetQueueDepthReporter(metrics)
	// The fake robot's scripted queue is empty: depth 0 is published.
	k.reportQueueDepth()
	// Two queued keys, one of them superseded: the depth is 2 (distinct
	// keys — the coalescing semantics the gauge documents).
	robot.enqueue(k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-d1", Event: k8srobot.EventAdd, CreateAt: time.Now()})
	robot.enqueue(k8srobot.QueueObject{RType: k8srobot.Pods, Key: "msp/pod-d2", Event: k8srobot.EventAdd, CreateAt: time.Now()})
	k.reportQueueDepth()

	obs := metrics.K8sQueueDepthObservations()
	if len(obs) != 2 || obs[0].Depth != 0 || obs[1].Depth != 2 {
		t.Fatalf("k8s queue depth observations = %#v, want [{0} {2}] (the robot's distinct-key depth per publication)", obs)
	}
}

// TestQueueDepthLoopPublishesOnTickerAndStops pins the gauge's publication
// loop: the 5s cadence is too slow for a unit test to wait out, so the
// loop's contract is pinned by driving one tick directly (the loop body is
// reportQueueDepth — covered above) and by verifying the loop exits on
// context cancel (the provider's lifetime, not a goroutine leak).
func TestQueueDepthLoopPublishesOnTickerAndStops(t *testing.T) {
	robot := newFakeRobot(nil, nil, false)
	w := &fakeWorker{}
	ctx, cancel := context.WithCancel(context.Background())
	k := newTestProvider(robot, w)
	k.ctx = ctx
	metrics := fakes.NewFakeMetricsRecorder()
	k.SetQueueDepthReporter(metrics)

	done := make(chan struct{})
	go func() {
		k.reportQueueDepthLoop()
		close(done)
	}()

	// The loop's first tick is 5s away; the assertion is that it exits
	// promptly on cancel and never published anything before it.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reportQueueDepthLoop did not exit after context cancel")
	}
	if got := len(metrics.K8sQueueDepthObservations()); got != 0 {
		t.Fatalf("observations before the first tick = %d, want 0 (5s cadence)", got)
	}
}

// TestNewK8SProviderSatisfiesQueueDepthReporter pins the wiring seam:
// the provider NewK8SProvider returns satisfies the exported
// QueueDepthReporter interface internal/server.go asserts against — the
// minimal-plumbing decision (no constructor signature change; the wiring
// calls the setter after construction). The construction needs a readable
// kubeconfig, so a minimal throwaway file stands in (the robot never
// connects at construction; Run would).
func TestNewK8SProviderSatisfiesQueueDepthReporter(t *testing.T) {
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(
		"apiVersion: v1\nkind: Config\nclusters:\n- name: test\n  cluster:\n    server: http://127.0.0.1:1\ncontexts:\n- name: test\n  context:\n    cluster: test\n    user: test\ncurrent-context: test\nusers:\n- name: test\n  user: {}\n"), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	provider, err := NewK8SProvider(context.Background(), &fakeWorker{}, 0, []string{kubeconfig})
	if err != nil {
		t.Fatalf("NewK8SProvider() error = %v, want nil (construction never dials)", err)
	}
	reporter, ok := provider.(QueueDepthReporter)
	if !ok {
		t.Fatalf("NewK8SProvider() = %T, want a QueueDepthReporter (the internal/server.go wiring seam)", provider)
	}
	// The setter is callable through the seam (the gauge wiring itself is
	// pinned above on the concrete provider).
	reporter.SetQueueDepthReporter(nil)
}

// -----------------------------------------------------------------------------
// The nacos-authoritative reconcile (dsca-3 §3.3 R2 / DS-3-6 / §3.5)
// -----------------------------------------------------------------------------

// newUnhealthyPod builds a Running pod whose containers are NOT ready: the
// conversion yields Status=2, Enabled=false, State=probing — the local
// mirror image of what the nacos wire holds for a spotter-pushed unhealthy
// instance (register forces enabled=false; the metadata carries status "2"
// and state probing).
func newUnhealthyPod(namespace, name string) *corev1.Pod {
	pod := newValidPod(namespace, name)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Ready:        false,
			State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			RestartCount: 0,
		},
	}
	return pod
}

// nacosViewInstance builds the NACOS-SHAPED remote view entry for a local
// instance: the reconstruction of what the sink registered — metadata
// reversion/status/state equal to the local values, Enabled mirroring the
// wire projection (register forces enabled=false for every status-2
// instance), Provider carrying the cluster, Cluster empty (the dsca-3 §3.2
// fidelity correction), ports beyond the first dropped.
func nacosViewInstance(local *sv.Instance) *sv.Instance {
	remote := *local
	remote.Cluster = ""
	remote.Enabled = local.Enabled && local.Status != providers.InstanceStatusUnhealthy
	remote.Provider = local.Provider
	return &remote
}

// TestCompareAndFlushNacosReconcileEqualSteadyStateNoPush: the steady-state
// pin of the nacos-source compare — a local instance against its own
// nacos-shaped reconstruction (every compared field equal, reversion equal,
// wire-projected Enabled equal) produces NO diff and NO push. This is the
// every-cycle-loop guard: the round-trip asymmetries the compare used to
// trip over (Cluster, raw Enabled) are neutralized by the §3.2/§3.3 fixes,
// and a regression back to any of them re-arms the user's "every cycle has
// changes" loop against this pin.
func TestCompareAndFlushNacosReconcileEqualSteadyStateNoPush(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	pod.ResourceVersion = "100"

	local := instanceFromPod(t, pod)
	remote := nacosViewInstance(local)

	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	k.CompareAndFlush()

	// No diff: nothing must be pushed for pod-a.
	time.Sleep(150 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (the steady nacos view matches the local instance; the every-cycle loop guard)", len(events))
	}
}

// TestCompareAndFlushNacosReconcileUnhealthySteadyNoPush: the §3.5 k8s
// status-2 steady pin — a local UNHEALTHY pod (Status=2, locally
// Enabled=false by readiness, State=probing) against its own reconstruction
// (Status=2, wire Enabled=false, State=probing) compares Enabled
// false-vs-false and produces no diff, no push. Pins the §3.3 k8s-leg
// symmetry (local Enabled derives from the same pod readiness that drives
// status) so a future conversion change cannot silently re-arm DS-5-2 on
// this leg: if the conversion ever made a status-2 pod locally
// enabled=true, the wire projection of this compare would start diffing.
func TestCompareAndFlushNacosReconcileUnhealthySteadyNoPush(t *testing.T) {
	pod := newUnhealthyPod("msp", "pod-uh")
	pod.ResourceVersion = "100"

	local := instanceFromPod(t, pod)
	if local.Status != providers.InstanceStatusUnhealthy || local.Enabled {
		t.Fatalf("fixture: local status/enabled = %d/%v, want 2/false (the unhealthy mirror precondition)", local.Status, local.Enabled)
	}
	remote := nacosViewInstance(local)

	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	k.CompareAndFlush()

	time.Sleep(150 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (the status-2 pair compares enabled false-vs-false — the k8s-leg symmetry pin)", len(events))
	}
}

// TestCompareAndFlushNacosReconcileReversionMismatchEitherDirectionPushes:
// rule R2 of dsca-3 §3.3 — in the nacos-source compare, ANY reversion
// mismatch counts as drift and the local value wins. Two shapes:
//   - the nacos reversion BELOW the local one (the ordinary local-newer
//     drift): previously caught by the strictly-higher gate, still a push;
//   - the nacos reversion ABOVE the local one (the forged/edited revision,
//     reachable only out-of-band): the strictly-higher gate called it
//     "no push" — R2 calls it drift, and the push heals it by overwriting
//     the remote value with the local one.
func TestCompareAndFlushNacosReconcileReversionMismatchEitherDirectionPushes(t *testing.T) {
	t.Run("remote lower pushes local", func(t *testing.T) {
		pod := newValidPod("msp", "pod-a")
		pod.ResourceVersion = "100"
		remote := nacosViewInstance(instanceFromPod(t, pod))
		remote.Reversion = 50 // the ordinary local-newer drift

		robot := newFakeRobot(nil, []interface{}{pod}, false)
		w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
		k := newTestProvider(robot, w)
		k.SetNacosReconcileSource(true)

		k.CompareAndFlush()

		events := w.waitForHandles(t, 1)
		if e := findEvent(t, events, "pod-a"); e == nil {
			t.Fatalf("no push for pod-a; events = %#v", events)
		} else if e.Data[0].Reversion != 100 {
			t.Fatalf("pushed reversion = %d, want 100 (local wins)", e.Data[0].Reversion)
		}
	})

	t.Run("remote higher pushes local (R2, the forged-revision heal)", func(t *testing.T) {
		pod := newValidPod("msp", "pod-a")
		pod.ResourceVersion = "100"
		remote := nacosViewInstance(instanceFromPod(t, pod))
		remote.Reversion = 999999 // forged/edited above the local ceiling

		robot := newFakeRobot(nil, []interface{}{pod}, false)
		w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
		k := newTestProvider(robot, w)
		k.SetNacosReconcileSource(true)

		k.CompareAndFlush()

		events := w.waitForHandles(t, 1)
		e := findEvent(t, events, "pod-a")
		if e == nil {
			t.Fatalf("no push for the forged-revision instance; events = %#v (R2: any mismatch either direction is drift)", events)
		}
		if e.Data[0].Reversion != 100 {
			t.Fatalf("pushed reversion = %d, want 100 (the local value overwrites the forged remote one)", e.Data[0].Reversion)
		}
	})
}

// TestCompareAndFlushNacosReconcileFieldDiffUpdatePushesLocal: the UPDATE
// case the nacos-source design exists to provide — a same-id, field-level
// drift on the remote side (a manual nacos metadata edit, an enabled flip)
// produces a diff and the push carries the LOCAL instance (R1: local is the
// source of truth; the re-register rewrites the metadata).
func TestCompareAndFlushNacosReconcileFieldDiffUpdatePushesLocal(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	pod.ResourceVersion = "100"

	remote := nacosViewInstance(instanceFromPod(t, pod))
	remote.EnvType = "beta" // the manual console edit

	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{remote}}}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 1)
	e := findEvent(t, events, "pod-a")
	if e == nil {
		t.Fatalf("no push for the field-drifted instance; events = %#v", events)
	}
	if e.Data[0].EnvType != "test" {
		t.Fatalf("pushed envType = %q, want test (local wins the field diff)", e.Data[0].EnvType)
	}
}

// TestCompareAndFlushNacosReconcileAddLocalOnlyPushes: the ADD case — local
// has an instance the nacos view lacks (an API-side delete, a first
// registration): case 2 as-is, the push carries the local online instance.
func TestCompareAndFlushNacosReconcileAddLocalOnlyPushes(t *testing.T) {
	pod := newValidPod("msp", "pod-new")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	// The remote view holds nothing of this provider.
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{
		remoteInstance("srv-foreign", providers.InstanceStatusOnline, 5),
	}}}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 2)
	if e := findEvent(t, events, "pod-new"); e == nil {
		t.Fatalf("no push for the local-only instance pod-new; events = %#v", events)
	} else if e.Data[0].Status != providers.InstanceStatusOnline {
		t.Fatalf("pushed status = %d, want online (the local truth)", e.Data[0].Status)
	}
}

// TestCompareAndFlushNacosReconcileDeleteRemoteOnlyDeregisters: the DELETE
// case — the nacos view has an instance the local list lacks (deleted while
// spotter was down, or a mid-flight delete lost): case 3 pushes the
// remote-sourced instance with Status=3/State=terminated/Enabled=false, and
// the push carries the reconstruction's own ip so the nacos sink's
// deregister by composite id is well-formed.
func TestCompareAndFlushNacosReconcileDeleteRemoteOnlyDeregisters(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	ghost := remoteInstance("pod-ghost", providers.InstanceStatusOnline, 9)
	ghost.Ip = "10.42.0.99" // the reconstruction's own host ip
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{ghost}}}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 2)
	e := findEvent(t, events, "pod-ghost")
	if e == nil {
		t.Fatalf("no push for the remote-only ghost; events = %#v", events)
	}
	got := e.Data[0]
	if got.Status != providers.InstanceStatusOffline {
		t.Fatalf("remote-only instance status = %d, want %d (deregister)", got.Status, providers.InstanceStatusOffline)
	}
	if got.State != providers.InstanceStateTerminated {
		t.Fatalf("remote-only instance state = %q, want %q", got.State, providers.InstanceStateTerminated)
	}
	if got.Enabled {
		t.Fatal("remote-only instance enabled = true, want false")
	}
	if got.Ip != "10.42.0.99" {
		t.Fatalf("remote-only instance ip = %q, want the reconstruction's own ip (a well-formed composite id)", got.Ip)
	}
}

// TestCompareAndFlushNacosReconcileGetAllErrorSkipsTick: the DS-3-6 repin —
// in nacos-reconcile mode a source error SKIPS the tick (consul's shape)
// instead of degrading to push-everything: with nacos as the source at
// scale, one transient 5xx would otherwise trigger a full re-push burst to
// both sinks precisely when the system is degraded. The cache refill still
// happened (the local source was read); only the pushes are skipped.
func TestCompareAndFlushNacosReconcileGetAllErrorSkipsTick(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllErr: errFakeGetAll}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	k.CompareAndFlush()

	time.Sleep(150 * time.Millisecond)
	if events := w.handleSnapshot(); len(events) != 0 {
		t.Fatalf("pushed events = %d, want 0 (a source error skips the nacos-reconcile tick, never push-everything)", len(events))
	}
	// The local cache was still refilled: the skip is of the compare, not
	// of the local source read.
	if k.cache.Get("pod-a") == nil {
		t.Fatal("cache does not contain pod-a after the skipped tick (the local source read still ran)")
	}
}

// TestCompareAndFlushAtlasModeGetAllErrorStillPushesAll: the Atlas-primary
// world keeps its historical semantics — an unreachable Atlas was a "resync
// everything" trigger, and that world's behavior stays untouched by the
// dsca-3 changes (the mode gate, not the call site, carries the switch).
func TestCompareAndFlushAtlasModeGetAllErrorStillPushesAll(t *testing.T) {
	pod := newValidPod("msp", "pod-a")
	robot := newFakeRobot(nil, []interface{}{pod}, false)
	w := &fakeWorker{getAllErr: errFakeGetAll}
	k := newTestProvider(robot, w) // nacosReconcile stays false (the default)

	k.CompareAndFlush()

	events := w.waitForHandles(t, 1)
	if findEvent(t, events, "pod-a") == nil {
		t.Fatalf("no push for pod-a when the Atlas GetAll fails; events = %#v (the Atlas-world behavior is unchanged)", events)
	}
}

// TestCompareAndFlushNacosBootHealsDowntimeDrift: the boot-reconcile proof
// of dsca-3 §4.2 — with the nacos view as the diff source, the boot
// CompareAndFlush heals downtime drift with NO code change beyond the
// source routing: nacos state holds a ghost (a pod deleted while spotter
// was down — the remembered map died with the process, the catalog did
// not), the freshly synced local list lacks it, and the provider's own
// existing case-3 path deregisters it. The catalog replaces the in-memory
// remembered memory as the boot-time ownership record. Driven through
// exactly the boot path's shape: robot synced (GetAll non-empty), one
// synchronous CompareAndFlush, no interval tick.
func TestCompareAndFlushNacosBootHealsDowntimeDrift(t *testing.T) {
	// The local list after HasSynced: one live pod of pay-user.
	live := newValidPod("msp", "pod-live")
	live.ResourceVersion = "100"

	// The nacos view: the live pod's registration PLUS a ghost of a service
	// that vanished entirely while spotter was down (a whole service — the
	// 416e62a remembered-map residual the catalog-as-ownership-record
	// closes). The ghost reconstruction carries its own host fields, so the
	// deregister is well-formed by construction.
	liveRemote := nacosViewInstance(instanceFromPod(t, live))
	vanished := &sv.Instance{
		InstanceId: "pod-gone", AppCode: "pay-gone", Ip: "10.42.0.50",
		Ports: []*sv.PortInfo{{Port: 7096}}, Provider: "k8s", Cluster: "",
		EnvType: "test", State: "running", Status: providers.InstanceStatusOnline,
		Enabled: true, Reversion: 7,
	}

	robot := newFakeRobot(nil, []interface{}{live}, false)
	w := &fakeWorker{getAllResponse: &sv.InstanceList{Instance: []*sv.Instance{liveRemote, vanished}}}
	k := newTestProvider(robot, w)
	k.SetNacosReconcileSource(true)

	// The boot path: one synchronous CompareAndFlush after HasSynced. The
	// pushes ride the provider's ants pool (buildAndSendEvent submits
	// asynchronously), so wait a bounded time for the ghost's deregister.
	k.CompareAndFlush()

	deadline := time.Now().Add(3 * time.Second)
	var events []*worker.Event
	for time.Now().Before(deadline) {
		events = w.handleSnapshot()
		if findEvent(t, events, "pod-gone") != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The ghost is deregistered (case 3): the boot heal.
	e := findEvent(t, events, "pod-gone")
	if e == nil {
		t.Fatalf("no deregister push for the vanished-while-down ghost; events = %#v (the boot-time heal)", events)
	} else if e.Data[0].Status != providers.InstanceStatusOffline {
		t.Fatalf("ghost push status = %d, want %d (deregister)", e.Data[0].Status, providers.InstanceStatusOffline)
	}
	// The live pod is steady against its own reconstruction: no push for
	// it — the ONLY event of this compare is the ghost's deregister (the
	// degenerate push-all of the empty-Atlas world is gone with the
	// nacos-source routing).
	if e := findEvent(t, events, "pod-live"); e != nil {
		t.Fatalf("unexpected push for the steady live pod; events = %#v", events)
	}
	if len(events) != 1 {
		t.Fatalf("pushed events = %d, want exactly 1 (the ghost's deregister; the live pod is steady)", len(events))
	}
}
