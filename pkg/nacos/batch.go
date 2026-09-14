package nacos

import (
	"fmt"
	"sync"
	"time"

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

func waitForPersistentBatch(c *Client, key persistentBatchKey, params []InstanceParams) error {
	want := make(map[string]InstanceParams, len(params))
	for _, p := range params {
		want[instanceID(p)] = p
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		hosts, err := c.ListCatalogInstances(key.Service, key.Cluster)
		if err != nil {
			return err
		}
		matched := make(map[string]bool, len(want))
		for _, h := range hosts {
			p, ok := want[h.InstanceID]
			groupedService := effectiveGroup(p.GroupName) + "@@" + p.ServiceName
			if !ok || matched[h.InstanceID] || h.IP != p.IP || h.Port != p.Port || h.ClusterName != p.ClusterName || (h.ServiceName != p.ServiceName && h.ServiceName != groupedService) || h.Ephemeral || h.Enabled != p.Enabled || h.Healthy != p.Enabled {
				continue
			}
			matched[h.InstanceID] = true
		}
		if len(matched) == len(want) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("nacos: persistent batch convergence timeout for %s/%s (%d items)", key.Service, key.Cluster, len(params))
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
			for batchNumber, batch := range batchesForScope {
				// Keep the batch boundary observable through the existing logger
				// seam. This is intentionally metadata-only: persistent instances
				// still use one official SDK call per item, while startup/full-sync
				// tests can prove application-scoped partitioning and ordering.
				s.logger.Infof("nacos persistent batch scope=%s group=%s service=%s cluster=%s operation=%s index=%d size=%d",
					batch.Key.Namespace, batch.Key.Group, batch.Key.Service, batch.Key.Cluster, batch.Key.Operation, batchNumber, len(batch.Items))
				if batch.Key.Operation == batchRegister && s.client != nil && s.client.sdk != nil && s.client.sdk.hasPersistentVendor() {
					params := make([]InstanceParams, 0, len(batch.Items))
					for position, ins := range batch.Items {
						if ins.Ip == "" {
							errs[batch.Indexes[position]] = nil
							continue
						}
						enabled := ins.Enabled
						if ins.Status == instance.InstanceStatusUnhealthy {
							enabled = false
						}
						if s.shouldApplyHealthPolicy() {
							if err := s.ensureClusterHealthCheckDisabled(ins.AppCode, clusterOf(ins)); err != nil {
								errs[batch.Indexes[position]] = err
								continue
							}
						}
						params = append(params, InstanceParams{ServiceName: ins.AppCode, IP: ins.Ip, Port: firstPort(ins), ClusterName: clusterOf(ins), GroupName: batch.Key.Group, NamespaceID: batch.Key.Namespace, Enabled: enabled, Ephemeral: false, Metadata: metadataOf(ins)})
					}
					if len(params) > 0 {
						validIndexes := make([]int, 0, len(params))
						for position, item := range batch.Items {
							if item.Ip != "" && errs[batch.Indexes[position]] == nil {
								validIndexes = append(validIndexes, batch.Indexes[position])
							}
						}
						var wg sync.WaitGroup
						var mu sync.Mutex
						for pos, p := range params {
							pos, p := pos, p
							wg.Add(1)
							go func() {
								defer wg.Done()
								semaphore <- struct{}{}
								err := s.client.sdk.RegisterPersistent(p)
								<-semaphore
								if err != nil {
									mu.Lock()
									errs[validIndexes[pos]] = err
									mu.Unlock()
								}
							}()
						}
						wg.Wait()
						failed := false
						for _, idx := range validIndexes {
							if errs[idx] != nil {
								failed = true
								break
							}
						}
						if !failed && s.client.sdk.hasPersistentVendor() {
							if err := waitForPersistentBatch(s.client, batch.Key, params); err != nil {
								for _, idx := range validIndexes {
									errs[idx] = err
								}
							}
						}
					}
					continue
				}
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
