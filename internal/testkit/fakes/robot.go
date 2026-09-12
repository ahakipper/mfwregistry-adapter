package fakes

import (
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"spotter/pkg/k8srobot"
)

// RobotEvent is a scripted Kubernetes event with the source-cluster and UID
// retained for assertions. The production QueueObject predates those fields;
// keeping them in the fixture lets A0 tests model the collision shape without
// changing the production identity model ahead of A1.
type RobotEvent struct {
	ClusterID string
	Namespace string
	Name      string
	UID       string
	Type      k8srobot.EventType
	Object    interface{}
	CreateAt  time.Time
}

// MultiClusterDeleteRecreateScript returns a deterministic sequence for one
// same-named resource in each cluster: add old UID, delete old UID, add new
// UID. It is deliberately a fixture constructor so tests can replay the
// burst-delete/recreate sequence with no wall-clock sleeps.
func MultiClusterDeleteRecreateScript(namespace, name string, clusters ...string) []RobotEvent {
	if len(clusters) == 0 {
		clusters = []string{"cluster-a", "cluster-b"}
	}
	result := make([]RobotEvent, 0, len(clusters)*3)
	base := time.Unix(0, 0)
	for i, cluster := range clusters {
		oldUID := cluster + "-uid-old"
		newUID := cluster + "-uid-new"
		result = append(result,
			RobotEvent{ClusterID: cluster, Namespace: namespace, Name: name, UID: oldUID, Type: k8srobot.EventAdd, CreateAt: base.Add(time.Duration(i*3) * time.Millisecond)},
			RobotEvent{ClusterID: cluster, Namespace: namespace, Name: name, UID: oldUID, Type: k8srobot.EventDelete, CreateAt: base.Add(time.Duration(i*3+1) * time.Millisecond)},
			RobotEvent{ClusterID: cluster, Namespace: namespace, Name: name, UID: newUID, Type: k8srobot.EventAdd, CreateAt: base.Add(time.Duration(i*3+2) * time.Millisecond)},
		)
	}
	return result
}

// FakeRobot is a deterministic Robot implementation for provider tests. It
// exposes the scripted source-cluster/UID events through Events while also
// satisfying k8srobot.Robot for code paths that only need QueueObject.
type FakeRobot struct {
	mu       sync.Mutex
	queue    []k8srobot.QueueObject
	capacity int
	cond     *sync.Cond
	events   []RobotEvent
	byKey    map[string][]interface{}
	list     []interface{}
	synced   bool
	stopped  bool
	stopOnce sync.Once
}

// NewFakeRobot creates a robot with a buffered event queue.
func NewFakeRobot(buffer int) *FakeRobot {
	if buffer < 0 {
		buffer = 0
	}
	r := &FakeRobot{capacity: buffer, byKey: make(map[string][]interface{})}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Enqueue appends a metadata-rich event and makes its legacy QueueObject
// visible to Pop/GetByKey. Delete events remove the key's object; add/update
// events replace it. Tests needing separate same-name cluster state should
// inspect Events, which preserves ClusterID and UID losslessly.
func (r *FakeRobot) Enqueue(event RobotEvent) bool {
	r.mu.Lock()
	r.ensureCondLocked()
	if r.stopped {
		r.mu.Unlock()
		return false
	}
	if len(r.queue) >= r.capacity {
		r.mu.Unlock()
		return false
	}
	r.events = append(r.events, event)
	key := event.Namespace + "/" + event.Name
	storageKey := event.ClusterID + "\x00" + key
	if event.Type == k8srobot.EventDelete {
		delete(r.byKey, storageKey)
	} else if event.Object != nil {
		r.byKey[storageKey] = []interface{}{cloneRobotObject(event.Object)}
	} else {
		// A metadata-only fixture still represents a live object for
		// GetByKey assertions; production tests can provide a real *Pod via
		// RobotEvent.Object when conversion is required.
		r.byKey[storageKey] = []interface{}{event}
	}
	obj := k8srobot.QueueObject{RType: k8srobot.Pods, Key: key, Event: event.Type, CreateAt: event.CreateAt}
	obj.ClusterID, obj.UID = event.ClusterID, event.UID
	if obj.CreateAt.IsZero() {
		obj.CreateAt = time.Unix(0, int64(len(r.events)))
	}
	r.queue = append(r.queue, obj)
	r.cond.Signal()
	r.mu.Unlock()
	return true
}

// Events returns an independent script snapshot.
func (r *FakeRobot) Events() []RobotEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]RobotEvent, len(r.events))
	for i, event := range r.events {
		result[i] = event
		result[i].Object = cloneRobotObject(event.Object)
	}
	return result
}

func cloneRobotObject(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	return cloneRobotValue(reflect.ValueOf(value)).Interface()
}

func cloneRobotValue(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(cloneRobotValue(v.Elem()))
		return out
	case reflect.Ptr:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(cloneRobotValue(v.Elem()))
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		it := v.MapRange()
		for it.Next() {
			out.SetMapIndex(cloneRobotValue(it.Key()), cloneRobotValue(it.Value()))
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(cloneRobotValue(v.Index(i)))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if out.Field(i).CanSet() && v.Field(i).CanInterface() {
				out.Field(i).Set(cloneRobotValue(v.Field(i)))
			}
		}
		return out
	default:
		return v
	}
}

func (r *FakeRobot) Run() error {
	r.mu.Lock()
	r.synced = true
	r.mu.Unlock()
	return nil
}

func (r *FakeRobot) Stop() {
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.ensureCondLocked()
		r.stopped = true
		r.cond.Broadcast()
		r.mu.Unlock()
	})
}

func (r *FakeRobot) HasSynced() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.synced
}

func (r *FakeRobot) Pop() (k8srobot.QueueObject, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureCondLocked()
	for len(r.queue) == 0 && !r.stopped {
		r.cond.Wait()
	}
	if r.stopped {
		return k8srobot.QueueObject{}, errors.New("fake robot has been stopped")
	}
	obj := r.queue[0]
	r.queue = r.queue[1:]
	return obj, nil
}

func (r *FakeRobot) ensureCondLocked() {
	if r.cond == nil {
		r.cond = sync.NewCond(&r.mu)
	}
}

func (r *FakeRobot) Finish(k8srobot.QueueObject) {}

func (r *FakeRobot) GetByKey(_ k8srobot.ResourceType, key string) ([]interface{}, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []interface{}
	for stored, items := range r.byKey {
		if strings.HasSuffix(stored, "\x00"+key) {
			for _, item := range items {
				result = append(result, cloneRobotObject(item))
			}
		}
	}
	return result, len(result) > 0
}

func (r *FakeRobot) GetByClusterKey(resource k8srobot.ResourceType, clusterID, key string) ([]interface{}, bool) {
	if resource != k8srobot.Pods {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	items, ok := r.byKey[clusterID+"\x00"+key]
	if !ok || len(items) == 0 {
		return nil, false
	}
	result := make([]interface{}, len(items))
	for i, item := range items {
		result[i] = cloneRobotObject(item)
	}
	return result, true
}

func (r *FakeRobot) List(_ k8srobot.ResourceType) []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]interface{}, len(r.list))
	for i, item := range r.list {
		result[i] = cloneRobotObject(item)
	}
	return result
}

func (r *FakeRobot) QueueDepth() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.queue)
}

var _ k8srobot.Robot = (*FakeRobot)(nil)
