package providers

import (
	"github.com/google/btree"
	"github.com/mohae/deepcopy"
	sv "spotter/pkg/beehive/service/v2"
	"strings"
	"sync"
)

type CacheIterface interface {
	Get(id string) *sv.Instance
	Delete(id string) *sv.Instance
	ReplaceOrInsert(ins *sv.Instance) *sv.Instance
	List() []*sv.Instance
	Clear()
}

// Cache
// The internal implementation of the cache can use data structures such as map and btree
// The reason why map is not used is because golang map does not release memory space as elements
// are deleted. (In fact, there is no problem under normal circumstances)
// Therefore, if the current adapter runs for a long time, its memory may continue to increase (unless the map is cleared at the right time)

// NewCache init a Cache
// if degree is 2, it is a bit like a binary tree
func NewCache(degree int) CacheIterface {
	if degree < 2 {
		degree = 2
	}
	return &CacheBtree{
		btree: btree.New(degree),
	}
}

type CacheBtree struct {
	btree *btree.BTree
	sync.RWMutex
}

func (cache *CacheBtree) Get(id string) (ins *sv.Instance) {
	cache.RLock()
	defer cache.RUnlock()
	key := &InstanceCacheItem{
		Instance: &sv.Instance{
			InstanceId: id,
			Label:      map[string]string{"sourceKey": id},
		},
	}
	// avoid generate npe
	if item := cache.btree.Get(key); item != nil {
		tins := item.(*InstanceCacheItem).Instance
		sins := cache.deepCopy(*tins)
		ins = &sins
	}
	// Legacy callers may still ask by wire ID; accept only an unambiguous
	// match. Source-aware production paths pass IdentityKey explicitly.
	if ins == nil && !strings.Contains(id, ":") && !strings.Contains(id, "/") {
		var candidate *sv.Instance
		matches := 0
		cache.btree.Ascend(func(item btree.Item) bool {
			value := item.(*InstanceCacheItem).Instance
			if value.InstanceId == id {
				candidate = value
				matches++
			}
			return true
		})
		if matches == 1 {
			copy := cache.deepCopy(*candidate)
			ins = &copy
		}
	}

	return ins
}

// List return all the instances
func (cache *CacheBtree) List() []*sv.Instance {
	cache.RLock()
	defer cache.RUnlock()
	var all []*sv.Instance
	cache.btree.Ascend(func(item btree.Item) bool {
		if all == nil {
			all = []*sv.Instance{}
		}
		if item != nil {
			tins := item.(*InstanceCacheItem).Instance
			sins := cache.deepCopy(*tins)
			all = append(all, &sins)
		}

		return true
	})

	return all
}

func (cache *CacheBtree) Delete(id string) *sv.Instance {
	cache.Lock()
	defer cache.Unlock()
	key := &InstanceCacheItem{
		Instance: &sv.Instance{
			InstanceId: id,
			Label:      map[string]string{"sourceKey": id},
		},
	}
	item := cache.btree.Delete(key)

	return item.(*InstanceCacheItem).Instance
}

func (cache *CacheBtree) ReplaceOrInsert(ins *sv.Instance) *sv.Instance {
	cache.Lock()
	defer cache.Unlock()
	if ins == nil {
		return nil
	} else {
		sins := cache.deepCopy(*ins)
		ins = &sins
	}
	item := &InstanceCacheItem{
		Instance: ins,
	}
	cache.btree.ReplaceOrInsert(item)

	return ins
}

func (cache *CacheBtree) Clear() {
	cache.Lock()
	defer cache.Unlock()
	cache.btree.Clear(false)
}

func (cache *CacheBtree) deepCopy(ins sv.Instance) (nsp sv.Instance) {
	cp := deepcopy.Copy(ins)
	nsp = cp.(sv.Instance)

	return nsp
}

type InstanceCacheItem struct {
	Instance *sv.Instance
}

func (ins *InstanceCacheItem) Less(item btree.Item) bool {
	if strings.Compare(IdentityKey(ins.Instance), IdentityKey(item.(*InstanceCacheItem).Instance)) < 0 {
		return true
	} else {
		return false
	}
}
