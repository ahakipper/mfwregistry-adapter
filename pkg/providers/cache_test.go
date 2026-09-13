package providers

import (
	"testing"

	sv "spotter/pkg/beehive/service/v2"
)

func TestCacheDeleteMissingReturnsNil(t *testing.T) {
	cache := NewCache(2)
	if got := cache.Delete("k8s:missing"); got != nil {
		t.Fatalf("Delete(missing) = %#v, want nil", got)
	}
}

func TestCacheDeleteReturnsDeepCopy(t *testing.T) {
	cache := NewCache(2).(*CacheBtree)
	ins := &sv.Instance{
		InstanceId: "pod-a", SourceKey: "cluster-a/uid-a", SourceCluster: "cluster-a",
		Label: map[string]string{"owner": "spotter"}, Image: map[string]string{"app": "v1"},
		Ports: []*sv.PortInfo{{Name: "http", Port: 8080}},
	}
	cache.ReplaceOrInsert(ins)
	key := IdentityKey(ins)
	cache.RLock()
	storedItem := cache.btree.Get(&InstanceCacheItem{Instance: &sv.Instance{InstanceId: key, Label: map[string]string{"sourceKey": key}}}).(*InstanceCacheItem)
	stored := storedItem.Instance
	cache.RUnlock()
	deleted := cache.Delete(key)
	if deleted == nil {
		t.Fatal("Delete(existing) = nil")
	}
	if deleted == stored || deleted.Ports[0] == stored.Ports[0] {
		t.Fatal("Delete returned aliased cache data")
	}
	deleted.Label["owner"] = "caller"
	deleted.Image["app"] = "v2"
	deleted.Ports[0].Port = 9000
	if stored.Label["owner"] != "spotter" || stored.Image["app"] != "v1" || stored.Ports[0].Port != 8080 {
		t.Fatalf("mutating deleted copy changed stored value: %#v", stored)
	}
	if got := cache.Get(key); got != nil {
		t.Fatalf("Get(%q) after Delete = %#v, want nil", key, got)
	}
}
