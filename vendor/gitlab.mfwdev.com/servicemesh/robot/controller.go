package robot

import (
	"errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"

	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"strings"
	"context"
)

// Robot is an interface for monitor k8s multi-cluster resources.
type Robot interface {
	// Discover define which resources will be discovered under the fixed namespace of k8s
	// If the namespace is empty, it will discover all k8s namespaces
	// Discover(resources []Resource, resourceName []string)

	// Run start up the robot.
	// Start monitoring resources and sending events to the queue.
	Run()

	HasSynced() bool

	// Stop stop monitoring resources
	// Empty queue and recycle
	Stop()

	queue

	store
}

type controller struct {
	clients   []*kubernetes.Clientset
	informers informerSet

	stop chan struct{}

	queue

	store
}

var _ Robot = &controller{}

// triggerEventWhenSynced
func NewRobot(clusters []Cluster, triggerEventWhenSynced bool) (Robot, error) {
	core := &controller{
		queue: newWorkQueue(),
		stop:  make(chan struct{}, 1),
	}

	store := make(mapIndexerSet)
	informers := make(informerSet, 0)

	for _, c := range clusters {
		client, err := c.newClient()
		if err != nil {
			return nil, err
		}
		for _, r := range c.Resources {
			indexer, informer := r.createIndexInformer(client, core.queue, triggerEventWhenSynced)

			store[r.RType] = append(store[r.RType], indexer)
			informers = append(informers, informer)
		}
	}

	core.informers = informers
	core.store = store

	return core, nil
}

func initHandle(resource Resource, worker queue) cache.ResourceEventHandlerFuncs {
	handler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				worker.push(QueueObject{EventAdd, resource, key, time.Now()})
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				if resource == Endpoints {
					oldE := old.(*v1.Endpoints)
					curE := new.(*v1.Endpoints)
					if !reflect.DeepEqual(oldE.Subsets, curE.Subsets) {
						worker.push(QueueObject{EventUpdate, resource, key, time.Now()})
					}
				} else {
					worker.push(QueueObject{EventUpdate, resource, key, time.Now()})
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				worker.push(QueueObject{EventDelete, resource, key, time.Now()})
			}
		},
	}
	return handler
}

// Not blocking
// Block until cache is synced
func (c *controller) Run() {
	c.informers.run(c.stop)
}

func (c *controller) HasSynced() bool {
	synced := true
	for _, informer := range c.informers {
		if informer.HasSynced() == false {
			synced = false
			break
		}
	}

	return synced
}

func (c *controller) Stop() {
	defer runtime.HandleCrash()

	c.stop <- struct{}{}
	c.queue.close()
}

type informerSet []cache.Controller

func (s informerSet) run(done chan struct{}) {
	for _, one := range s {
		go one.Run(done)
		if !cache.WaitForCacheSync(done, one.HasSynced) {
			panic("Timed out waiting for caches to sync")
		}
	}
}

type RN struct {
	RType     Resource
	Namespace string
}

func (r *RN) createIndexInformer(client *kubernetes.Clientset, worker queue, triggerEventWhenSynced bool) (indexer cache.Indexer, informer cache.Controller) {
	lw := cache.NewListWatchFromClient(client.CoreV1().RESTClient(), r.RType.String(), r.Namespace, fields.Everything())
	switch r.RType {
	case Services:
		indexer, informer = NewCustomInformer(lw, &v1.Service{}, 0, initHandle(Services, worker), cache.Indexers{}, triggerEventWhenSynced)
	case Pods:
		indexer, informer = NewCustomInformer(lw, &v1.Pod{}, 0, initHandle(Pods, worker), cache.Indexers{}, triggerEventWhenSynced)
	case Endpoints:
		indexer, informer = NewCustomInformer(lw, &v1.Endpoints{}, 0, initHandle(Endpoints, worker), cache.Indexers{}, triggerEventWhenSynced)
	case ConfigMaps:
		indexer, informer = NewCustomInformer(lw, &v1.ConfigMap{}, 0, initHandle(ConfigMaps, worker), cache.Indexers{}, triggerEventWhenSynced)
	}
	return
}

type Cluster struct {
	ConfigPath string
	MasterUrl  string
	Resources  []RN
}

func (c *Cluster) newClient() (*kubernetes.Clientset, error) {
	if c.ConfigPath != "" || c.MasterUrl != "" {
		config, err := clientcmd.BuildConfigFromFlags(c.MasterUrl, c.ConfigPath)
		if err != nil {
			return nil, err
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			return nil, err
		}
		// used to check if it can connect to the cluster
		ctx := context.Background()
		if _, err = clientset.CoreV1().Nodes().Get(ctx, "probe", metav1.GetOptions{}); err != nil {
			if strings.Contains(err.Error(), "timeout") {
				return nil, errors.New("Can`t connect to cluster ")
			}
		}
		return clientset, nil
	}
	return nil, errors.New("Can`t find a way to access to k8s api. Please make sure ConfigPath or MasterUrl in cluster ")
}
