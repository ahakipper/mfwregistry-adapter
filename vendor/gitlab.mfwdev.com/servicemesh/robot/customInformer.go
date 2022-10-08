package robot

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"time"
)

var FirstStartTime = time.Now().Unix()

func NewCustomInformer(lw cache.ListerWatcher, objType runtime.Object, resyncPeriod time.Duration,
	h cache.ResourceEventHandler, indexers cache.Indexers, triggerEventWhenSynced bool) (cache.Indexer, cache.Controller) {

	indexer := cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, indexers)

	// This will hol¬d incoming changes. Note how we pass clientState in as a
	// KeyLister, that way resync operations will result in the correct set
	// of update/delete deltas.
	fifo := cache.NewDeltaFIFO(cache.MetaNamespaceKeyFunc, indexer)

	cfg := &cache.Config{
		Queue:            fifo,
		ListerWatcher:    lw,
		ObjectType:       objType,
		FullResyncPeriod: resyncPeriod,
		RetryOnError:     false,

		Process: func(obj interface{}) error {
			// from oldest to newest
			for _, d := range obj.(cache.Deltas) {
				switch d.Type {
				case cache.Sync, cache.Added, cache.Updated:
					if old, exists, err := indexer.Get(d.Object); err == nil && exists {
						// Update cache indexer
						if err := indexer.Update(d.Object); err != nil {
							return err
						}
						// The
						if d.Type == cache.Sync {
							if triggerEventWhenSynced == true {
								h.OnUpdate(old, d.Object)
							} else {
								if time.Now().Unix()-FirstStartTime > 10 {
									h.OnUpdate(old, d.Object)
								}
							}
						} else {
							h.OnUpdate(old, d.Object)
						}
					} else {
						// Update cache indexer
						if err := indexer.Add(d.Object); err != nil {
							return err
						}
						if d.Type == cache.Sync {
							if triggerEventWhenSynced == true {
								h.OnAdd(d.Object)
							} else {
								if time.Now().Unix()-FirstStartTime > 10 {
									h.OnAdd(d.Object)
								}
							}
						} else {
							h.OnUpdate(old, d.Object)
						}
					}
				case cache.Deleted:
					if err := indexer.Delete(d.Object); err != nil {
						return err
					}
					h.OnDelete(d.Object)
				}
			}
			return nil
		},
	}

	return indexer, cache.New(cfg)
}
