package k8s

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	corev1 "k8s.io/api/core/v1"

	"spotter/config"
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
	}
	for _, tc := range tests {
		if got := k.obj2InstanceId(k8srobot.QueueObject{Key: tc.key}); got != tc.want {
			t.Fatalf("obj2InstanceId(key %q) = %q, want %q", tc.key, got, tc.want)
		}
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
