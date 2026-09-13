package nacos

import (
	"sync"

	"spotter/internal/domain/instance"
)

// MaxPersistentBatchSize is the maximum number of desired instances carried by
// one application-scoped full-sync batch. It bounds memory, retry scope, and
// the amount of work represented by one scheduling unit; it is not a Nacos
// protocol batch size (persistent instances still use one official SDK call
// per item).
const MaxPersistentBatchSize = 100

type batchOperation string

const (
	batchRegister   batchOperation = "register"
	batchDeregister batchOperation = "deregister"
)

type persistentBatchKey struct {
	Namespace string
	Group     string
	Service   string
	Cluster   string
	Operation batchOperation
}

type persistentBatch struct {
	Key     persistentBatchKey
	Items   []*instance.Instance
	Indexes []int
}

// splitPersistentBatches partitions a snapshot using the default Nacos
// namespace and group. The Sink-specific helper below supplies configured
// values when the full-sync executor is called in production.
func splitPersistentBatches(instances []*instance.Instance, maxItems int) []persistentBatch {
	return splitPersistentBatchesForScope(instances, maxItems, DefaultNamespaceID, DefaultGroup)
}

func splitPersistentBatchesForScope(instances []*instance.Instance, maxItems int, namespace, group string) []persistentBatch {
	if maxItems < 1 {
		maxItems = 1
	}
	if maxItems > MaxPersistentBatchSize {
		maxItems = MaxPersistentBatchSize
	}
	namespace = effectiveNamespace(namespace)
	group = effectiveGroup(group)

	// A slice-backed index preserves first-seen key order. A map is used only
	// as an index into that slice, never for iteration, so map iteration cannot
	// perturb batch or item order.
	batches := make([]persistentBatch, 0)
	lastByKey := make(map[persistentBatchKey]int)
	for index, ins := range instances {
		if ins == nil {
			continue
		}
		operation, ok := persistentBatchOperation(ins.Status)
		if !ok {
			continue
		}
		key := persistentBatchKey{
			Namespace: namespace,
			Group:     group,
			Service:   ins.AppCode,
			Cluster:   clusterOf(ins),
			Operation: operation,
		}
		batchIndex, exists := lastByKey[key]
		if !exists || len(batches[batchIndex].Items) >= maxItems {
			batchIndex = len(batches)
			batches = append(batches, persistentBatch{Key: key})
			lastByKey[key] = batchIndex
		}
		batches[batchIndex].Items = append(batches[batchIndex].Items, ins)
		batches[batchIndex].Indexes = append(batches[batchIndex].Indexes, index)
	}
	return batches
}

func persistentBatchOperation(status int32) (batchOperation, bool) {
	switch status {
	case instance.InstanceStatusOnline, instance.InstanceStatusUnhealthy:
		return batchRegister, true
	case instance.InstanceStatusOffline:
		return batchDeregister, true
	default:
		return "", false
	}
}

type persistentBatchScope struct {
	namespace string
	group     string
	service   string
	cluster   string
}

// pushPersistentBatches executes a full snapshot as application-scoped,
// bounded batches. Batches sharing namespace/group/service/cluster are
// serialized in first-seen order; independent scopes overlap. A single global
// semaphore limits actual SDK item calls, rather than multiplying a worker
// pool by the number of batches. Every item is attempted and the first error
// in original snapshot order is returned after all scopes finish.
func (s *Sink) pushPersistentBatches(instances []*instance.Instance) error {
	namespace, group := DefaultNamespaceID, s.groupName
	if s.client != nil {
		namespace = effectiveNamespace(s.client.config.NamespaceID)
		if group == "" {
			group = effectiveGroup(s.client.config.GroupName)
		}
	}
	batches := splitPersistentBatchesForScope(instances, MaxPersistentBatchSize, namespace, group)
	if len(batches) == 0 {
		return nil
	}

	// Keep scope order separate from the map used to append batches. This lets
	// us launch one worker per independent application scope without relying on
	// nondeterministic map iteration for execution order within a scope.
	scopeOrder := make([]persistentBatchScope, 0)
	scopeBatches := make(map[persistentBatchScope][]persistentBatch)
	for _, batch := range batches {
		scope := persistentBatchScope{namespace: batch.Key.Namespace, group: batch.Key.Group, service: batch.Key.Service, cluster: batch.Key.Cluster}
		if _, exists := scopeBatches[scope]; !exists {
			scopeOrder = append(scopeOrder, scope)
		}
		scopeBatches[scope] = append(scopeBatches[scope], batch)
	}

	semaphore := make(chan struct{}, currentPushConcurrency())
	errs := make([]error, len(instances))
	var scopes sync.WaitGroup
	for _, scope := range scopeOrder {
		batchesForScope := scopeBatches[scope]
		scopes.Add(1)
		go func() {
			defer scopes.Done()
			for _, batch := range batchesForScope {
				var items sync.WaitGroup
				for position, ins := range batch.Items {
					position, ins := position, ins
					items.Add(1)
					go func() {
						defer items.Done()
						semaphore <- struct{}{}
						err := s.pushOne(ins)
						<-semaphore
						errs[batch.Indexes[position]] = err
					}()
				}
				items.Wait()
			}
		}()
	}
	scopes.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
