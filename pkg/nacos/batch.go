package nacos

import (
	"fmt"
	"strings"
	"sync"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
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

// BatchMetricsSnapshot is the sink-local accounting exported for Observe and
// test harnesses. LogicalBatches and Items are cumulative full-sync work;
// AttemptedItems counts items admitted to an application batch, while the
// success/failure fields classify the resulting item outcomes. SkippedItems
// counts source records intentionally ignored because they have no wire IP.
// RetryCount
// counts failed executions that the worker retry queue may replay, and
// PruneSkippedScopes counts application scopes deliberately protected from
// prune after an incomplete batch. ConcurrencyCap is the configured global
// item-call limit observed by the latest execution.
type BatchMetricsSnapshot struct {
	LogicalBatches       uint64
	Items                uint64
	AttemptedItems       uint64
	SucceededItems       uint64
	SkippedItems         uint64
	TransientFailedItems uint64
	PermanentFailedItems uint64
	RetryCount           uint64
	PruneSkippedScopes   uint64
	ConcurrencyCap       uint64
}

// BatchMetrics returns cumulative logical batch accounting for this sink.
// Reads are atomic and safe while a full sync is running.
func (s *Sink) BatchMetrics() BatchMetricsSnapshot {
	if s == nil {
		return BatchMetricsSnapshot{}
	}
	return BatchMetricsSnapshot{
		LogicalBatches:       s.batchLogicalCount.Load(),
		Items:                s.batchItemCount.Load(),
		AttemptedItems:       s.batchAttemptedCount.Load(),
		SucceededItems:       s.batchSucceededCount.Load(),
		SkippedItems:         s.batchSkippedCount.Load(),
		TransientFailedItems: s.batchTransientCount.Load(),
		PermanentFailedItems: s.batchPermanentCount.Load(),
		RetryCount:           s.batchFailedCount.Load(),
		PruneSkippedScopes:   s.batchPruneSkipped.Load(),
		ConcurrencyCap:       s.batchConcurrencyCap.Load(),
	}
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

type batchApplyFailure struct {
	scope persistentBatchScope
	err   error
}

// batchApplyError preserves failure scope while retaining errors.Is/errors.As
// compatibility with the first underlying SDK error. PushAll can therefore
// prune successful application scopes without treating a failed scope as a
// complete desired snapshot.
type batchApplyError struct {
	failures []batchApplyFailure
}

func (e *batchApplyError) Error() string {
	parts := make([]string, 0, len(e.failures))
	for _, failure := range e.failures {
		parts = append(parts, fmt.Sprintf("%s/%s/%s/%s: %v", failure.scope.namespace, failure.scope.group, failure.scope.service, failure.scope.cluster, failure.err))
	}
	return "nacos: persistent batch application failed: " + strings.Join(parts, "; ")
}

func (e *batchApplyError) Unwrap() []error {
	errs := make([]error, 0, len(e.failures))
	for _, failure := range e.failures {
		errs = append(errs, failure.err)
	}
	return errs
}

func (e *batchApplyError) Permanent() bool {
	if len(e.failures) == 0 {
		return false
	}
	for _, failure := range e.failures {
		if !ports.IsPermanentError(failure.err) {
			return false
		}
	}
	return true
}

func (e *batchApplyError) failedScopes() map[persistentBatchScope]bool {
	result := make(map[persistentBatchScope]bool, len(e.failures))
	for _, failure := range e.failures {
		result[failure.scope] = true
	}
	return result
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
	s.batchConcurrencyCap.Store(uint64(currentPushConcurrency()))
	admittedItems, skippedItems := 0, 0
	for _, batch := range batches {
		s.batchLogicalCount.Add(1)
		s.batchItemCount.Add(uint64(len(batch.Items)))
		admittedItems += len(batch.Items)
		if batch.Key.Operation == batchRegister {
			for _, item := range batch.Items {
				if item.Ip == "" {
					skippedItems++
				}
			}
		}
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

	errs := make([]error, len(instances))
	scopesByIndex := make([]persistentBatchScope, len(instances))
	batchIndexPresent := make([]bool, len(instances))
	for _, batch := range batches {
		scope := persistentBatchScope{namespace: batch.Key.Namespace, group: batch.Key.Group, service: batch.Key.Service, cluster: batch.Key.Cluster}
		for _, index := range batch.Indexes {
			scopesByIndex[index] = scope
			batchIndexPresent[index] = true
		}
	}
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
						metadata := metadataOf(ins)
						if metadataErr := validateMetadataSize(metadata); metadataErr != nil {
							errs[batch.Indexes[position]] = metadataErr
							continue
						}
						enabled, healthy := persistentWireFlags(ins, true)
						params = append(params, InstanceParams{ServiceName: ins.AppCode, IP: ins.Ip, Port: firstPort(ins), ClusterName: clusterOf(ins), GroupName: batch.Key.Group, NamespaceID: batch.Key.Namespace, Enabled: enabled, Healthy: &healthy, Ephemeral: false, Metadata: metadata})
					}
					if len(params) > 0 {
						validIndexes := make([]int, 0, len(params))
						validInstances := make([]*instance.Instance, 0, len(params))
						for position, item := range batch.Items {
							if item.Ip != "" && errs[batch.Indexes[position]] == nil {
								validIndexes = append(validIndexes, batch.Indexes[position])
								validInstances = append(validInstances, item)
							}
						}
						var wg sync.WaitGroup
						var mu sync.Mutex
						for pos, p := range params {
							pos, p := pos, p
							wg.Add(1)
							go func() {
								defer wg.Done()
								ins := validInstances[pos]
								if ownershipErr := s.recoverWireOwnership(ins); ownershipErr != nil {
									mu.Lock()
									errs[validIndexes[pos]] = ownershipErr
									mu.Unlock()
									return
								}
								releaseWireIdentity, claimErr := s.claimWireIdentity(ins)
								if claimErr != nil {
									mu.Lock()
									errs[validIndexes[pos]] = claimErr
									mu.Unlock()
									return
								}
								writeSucceeded := false
								defer func() { releaseWireIdentity(writeSucceeded) }()
								err := s.withPushPermit(func() error {
									if s.shouldApplyHealthPolicy() {
										if err := s.ensureClusterHealthCheckDisabled(p.ServiceName, p.ClusterName); err != nil {
											return err
										}
									}
									return s.client.sdk.RegisterPersistent(p)
								})
								writeSucceeded = err == nil
								if err != nil {
									mu.Lock()
									errs[validIndexes[pos]] = err
									mu.Unlock()
								}
							}()
						}
						wg.Wait()
						// A successful official-SDK RPC acknowledgement completes this
						// write attempt. Do not synchronously poll SelectAll for this
						// exact batch: SDK/catalog visibility may lag an acknowledged RPC
						// under sustained churn, which made a healthy, already-converged
						// service look like a failed write and queued obsolete full
						// snapshots for retry. PushAll's prune uses
						// the local desired identity set and cannot delete a desired item
						// merely because a catalog read lags. Periodic canonical
						// reconcile and the external Observe oracle own convergence proof.
					}
					continue
				}
				var items sync.WaitGroup
				for position, ins := range batch.Items {
					position, ins := position, ins
					items.Add(1)
					go func() {
						defer items.Done()
						err := s.withPushPermit(func() error { return s.pushOne(ins) })
						errs[batch.Indexes[position]] = err
					}()
				}
				items.Wait()
			}
		}()
	}
	scopes.Wait()
	var succeeded, transientFailed, permanentFailed uint64
	var failures []batchApplyFailure
	for index, err := range errs {
		if !batchIndexPresent[index] {
			continue
		}
		if err != nil {
			if isPermanentBatchError(err) {
				permanentFailed++
			} else {
				transientFailed++
			}
			failures = append(failures, batchApplyFailure{scope: scopesByIndex[index], err: err})
		} else {
			succeeded++
		}
	}
	// Every item represented by a persistent batch is counted as an admitted
	// application attempt. This includes a permanent pre-write validation
	// error: it was intentionally evaluated by the executor and must remain
	// visible in the item outcome accounting.
	s.batchAttemptedCount.Add(uint64(admittedItems - skippedItems))
	s.batchSucceededCount.Add(succeeded - uint64(skippedItems))
	s.batchSkippedCount.Add(uint64(skippedItems))
	s.batchTransientCount.Add(transientFailed)
	s.batchPermanentCount.Add(permanentFailed)
	if len(failures) > 0 {
		s.batchFailedCount.Add(1)
		return &batchApplyError{failures: failures}
	}
	return nil
}

func isPermanentBatchError(err error) bool {
	return ports.IsPermanentError(err)
}
