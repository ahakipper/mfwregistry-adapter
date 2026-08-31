// Package k8srobot is a local, self-contained replacement of the former private
// "servicemesh/robot" multi-cluster Kubernetes client module.
//
// It exposes the same surface that this repository consumes:
//
//	client.NewRobot([]client.Cluster{{ConfigPath: ..., Resources: []client.RN{{client.Pods, ""}}}}, debug)
//	robot.Run() / robot.Stop() / robot.HasSynced() / robot.Pop()
//	robot.Finish(obj) / robot.GetByKey(client.Pods, key) / robot.List(client.Pods)
//
// The implementation is a real one built on top of k8s.io/client-go informers:
// for every cluster a kubeconfig is loaded, a clientset is created and a shared
// pod informer (across all namespaces) feeds a work queue. Pop() blocks until an
// event is available, mirroring the semantics of the original robot, and
// HasSynced() only reports true once every cluster informer store is synced
// (the "must wait for all clusters ready" behaviour documented in the README).
package k8srobot

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
)

// ResourceType is the kind of Kubernetes resource watched by the robot.
type ResourceType string

// Pods is the only resource type consumed by this repository.
const Pods ResourceType = "pods"

// String implements fmt.Stringer.
func (r ResourceType) String() string { return string(r) }

// EventType is the kind of change that produced a queue object.
type EventType string

// The supported event types.
const (
	EventAdd    EventType = "add"
	EventUpdate EventType = "update"
	EventDelete EventType = "delete"
)

// String implements fmt.Stringer.
func (e EventType) String() string { return string(e) }

// RN describes a resource to watch inside a namespace ("").
// The fields are ordered so that the positional literal
// client.RN{client.Pods, ""} keeps compiling.
type RN struct {
	Resource  ResourceType
	Namespace string
}

// Cluster describes one Kubernetes cluster of the multi-cluster set.
type Cluster struct {
	ConfigPath string
	Resources  []RN
}

// QueueObject is the unit of work handed out by Pop().
type QueueObject struct {
	RType    ResourceType
	Key      string // "<namespace>/<name>"
	Event    EventType
	CreateAt time.Time
}

// Robot is the multi-cluster watcher contract consumed by the providers.
type Robot interface {
	// Run starts every cluster informer. It blocks until Stop() is called.
	Run() error
	// Stop shuts the robot down. After Stop, Pop returns an error.
	Stop()
	// HasSynced reports whether every cluster store has been synced.
	HasSynced() bool
	// Pop blocks until an event is available or the robot is stopped.
	Pop() (QueueObject, error)
	// Finish acknowledges a previously popped object.
	Finish(obj QueueObject)
	// GetByKey returns the objects stored under "<namespace>/<name>".
	GetByKey(resource ResourceType, key string) ([]interface{}, bool)
	// List returns all objects of the given resource across all clusters.
	List(resource ResourceType) []interface{}
}

// queueSize is the buffer of the internal event channel.
const queueSize = 4096

// clusterWatcher bundles everything needed to watch one cluster.
type clusterWatcher struct {
	cluster  Cluster
	factory  informers.SharedInformerFactory
	informer cache.SharedIndexInformer
}

// robot is the default Robot implementation.
type robot struct {
	clusters []*clusterWatcher

	queue   chan QueueObject
	done    chan struct{}
	stopMu  sync.Mutex
	stopped bool
}

// NewRobot validates every kubeconfig, builds the clientsets and the pod
// informers, and returns a ready-to-run Robot. A cluster whose kubeconfig
// cannot be loaded makes NewRobot fail, which matches the original robot's
// "must wait for all clusters ready" semantics (fail fast on configuration
// errors instead of silently ignoring a cluster).
func NewRobot(clusters []Cluster, debug bool) (Robot, error) {
	if len(clusters) == 0 {
		return nil, errors.New("k8srobot: no cluster configured")
	}
	r := &robot{
		queue: make(chan QueueObject, queueSize),
		done:  make(chan struct{}),
	}
	for _, c := range clusters {
		watcher, err := newClusterWatcher(c, r.queue)
		if err != nil {
			return nil, err
		}
		r.clusters = append(r.clusters, watcher)
	}
	return r, nil
}

// newClusterWatcher loads the kubeconfig and wires the pod informer event
// handlers of a single cluster. The produced events are pushed on the robot's
// shared queue channel.
func newClusterWatcher(c Cluster, queue chan QueueObject) (*clusterWatcher, error) {
	if c.ConfigPath == "" {
		return nil, errors.New("k8srobot: empty kubeconfig path")
	}
	if _, err := os.Stat(c.ConfigPath); err != nil {
		return nil, fmt.Errorf("k8srobot: kubeconfig %s is not readable: %w", c.ConfigPath, err)
	}
	restConfig, err := clientcmd.BuildConfigFromFlags("", c.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("k8srobot: build rest config from %s failed: %w", c.ConfigPath, err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("k8srobot: create clientset from %s failed: %w", c.ConfigPath, err)
	}
	// No re-sync period: events are only produced by real cluster changes,
	// which is what the original robot did.
	factory := informers.NewSharedInformerFactory(clientset, 0)
	informer := factory.Core().V1().Pods().Informer()
	w := &clusterWatcher{
		cluster:  c,
		factory:  factory,
		informer: informer,
	}
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			w.enqueue(EventAdd, obj, queue)
		},
		UpdateFunc: func(_, newObj interface{}) {
			w.enqueue(EventUpdate, newObj, queue)
		},
		DeleteFunc: func(obj interface{}) {
			// Deleted objects may arrive wrapped in a cache.DeletedFinalStateUnknown.
			if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			w.enqueue(EventDelete, obj, queue)
		},
	})
	return w, nil
}

// enqueue converts an informer callback into a QueueObject.
func (w *clusterWatcher) enqueue(event EventType, obj interface{}, queue chan QueueObject) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return
	}
	item := QueueObject{
		RType:    Pods,
		Key:      pod.Namespace + "/" + pod.Name,
		Event:    event,
		CreateAt: time.Now(),
	}
	select {
	case queue <- item:
	default:
		// The queue is full: drop the event instead of blocking the informer.
	}
}

// Run starts all cluster informer factories and blocks until Stop.
func (r *robot) Run() error {
	for _, w := range r.clusters {
		w.factory.Start(r.done)
	}
	<-r.done
	return nil
}

// Stop terminates all informers and unblocks Run and Pop.
func (r *robot) Stop() {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	if r.stopped {
		return
	}
	r.stopped = true
	close(r.done)
}

// HasSynced reports whether every cluster informer store is synced.
func (r *robot) HasSynced() bool {
	for _, w := range r.clusters {
		if !w.informer.HasSynced() {
			return false
		}
	}
	return true
}

// Pop blocks until an event is available. It returns an error once the robot
// has been stopped.
func (r *robot) Pop() (QueueObject, error) {
	select {
	case obj, ok := <-r.queue:
		if !ok {
			return QueueObject{}, errors.New("k8srobot: the robot queue is closed")
		}
		return obj, nil
	case <-r.done:
		// Drain the events that are already queued before giving up.
		select {
		case obj, ok := <-r.queue:
			if ok {
				return obj, nil
			}
		default:
		}
		return QueueObject{}, errors.New("k8srobot: the robot has been stopped")
	}
}

// Finish acknowledges a popped object. The queue is channel-based and
// unbounded-per-object, so there is no deferred work to perform; the method is
// kept to preserve the original robot's API.
func (r *robot) Finish(obj QueueObject) {
	_ = obj // no-op: nothing to acknowledge with a channel based queue
}

// GetByKey returns the objects stored under "<namespace>/<name>" in every
// cluster store.
func (r *robot) GetByKey(resource ResourceType, key string) ([]interface{}, bool) {
	if resource != Pods {
		return nil, false
	}
	var items []interface{}
	for _, w := range r.clusters {
		if obj, exists, err := w.informer.GetIndexer().GetByKey(key); err == nil && exists {
			items = append(items, obj)
		}
	}
	if len(items) == 0 {
		return nil, false
	}
	return items, true
}

// List returns all pods of every cluster store.
func (r *robot) List(resource ResourceType) []interface{} {
	if resource != Pods {
		return nil
	}
	var items []interface{}
	for _, w := range r.clusters {
		items = append(items, w.informer.GetIndexer().List()...)
	}
	return items
}
