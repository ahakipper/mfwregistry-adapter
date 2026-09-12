package k8s

import (
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	v1 "k8s.io/api/core/v1"
	"spotter/config"
	"spotter/internal/ports"
	sv "spotter/pkg/beehive/service/v2"
	k8srobot "spotter/pkg/k8srobot"
	"spotter/pkg/log"
	"spotter/pkg/metrics"
	"spotter/pkg/notice"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
	"spotter/tools/unit"
	"strings"
	"sync"
	"time"
)

// K8S provider implement
type k8s struct {
	providerName       string
	robot              k8srobot.Robot             // the K8s multi-cluster aggregator
	ctx                context.Context            // context
	worker             worker.Worker              // the role of worker is to synchronize provider changes to the discovery center
	stopped            bool                       // whether the current provider has stopped
	sync.Mutex                                    // lock mutex
	interval           int                        // the time interval for full synchronization. default 600s(10m)
	filters            []providers.InstanceFilter // filters is a collection of functions used to filter invalid instances
	cache              providers.CacheIterface    // pod cache
	generation         uint64
	emptyConfirmations uint32
	pool               *ants.Pool // goroutine pool
	// queueDepthMetrics publishes the robot's coalescing-queue depth on the
	// k8s_queue_depth gauge (dsca-1 DS-1-1 fix item 1: "plus a queueDepth
	// gauge"). Nil disables publication — the in-package tests construct the
	// provider without a recorder and must keep compiling.
	queueDepthMetrics ports.MetricsRecorder
	// nacosReconcile records that the periodic CompareAndFlush's remote view
	// is the nacos sink (dsca-3 §3.1, --reconcile-source nacos). It switches
	// the reconcile semantics onto the R1-R4 rule set of dsca-3 §3.3:
	//   - R2: any reversion mismatch — either direction — counts as drift
	//     (reversion joins the compared set at the compare call site, local
	//     wins; a remote reversion above the local one heals by overwriting
	//     and additionally logs the forged-revision companion signal).
	//   - DS-3-6: a GetAll error skips the tick instead of degrading to
	//     push-everything (a remote store that blips must not trigger a full
	//     re-push burst at scale).
	// The event-path cache diff (pod2Instance -> hasInstanceDiff) compares
	// two local conversion outputs and is NOT affected: local reversion
	// monotonicity keeps the old strictly-higher shape there.
	nacosReconcile bool
}

// queueDepthReportInterval is the gauge's publication cadence: short enough
// to catch a burst's rise and fall between full-push ticks, cheap enough
// (one mutex-guarded map-length read) to run forever.
const queueDepthReportInterval = 5 * time.Second

// NewK8SProvider Init k8s provider
func NewK8SProvider(ctx context.Context, worker worker.Worker, pushInterval int, configPath []string) (provider providers.Provider, err error) {
	clusters := make([]k8srobot.Cluster, len(configPath))
	for idx, path := range configPath {
		clusters[idx] = k8srobot.Cluster{
			ConfigPath: path,
			Resources: []k8srobot.RN{
				{
					k8srobot.Pods,
					"",
				},
			},
		}
	}
	var kr k8srobot.Robot
	kr, err = k8srobot.NewRobot(clusters, false)
	if err != nil {
		// If walk here, server init failed
		return nil, err
	}

	// init k8s obj
	k := &k8s{
		providerName: "k8s",
		robot:        kr,
		ctx:          ctx,
		worker:       worker,
		interval:     pushInterval,
		cache:        providers.NewCache(2),
	}
	p, _ := ants.NewPool(providers.PoolBenchSize, withExpiryDuration(time.Second*providers.PoolExpireTime))
	k.pool = p
	k.filters = providers.InitInstanceFilters()

	provider = k

	return
}

// QueueDepthReporter is the wiring seam of the queueDepth gauge: the
// exported interface internal/server.go asserts the constructed k8s
// provider against, so the provider's concrete (unexported) type never has
// to be exported for one setter. NewK8SProvider's returned value satisfies
// it (the *k8s receiver implements SetQueueDepthReporter).
type QueueDepthReporter interface {
	SetQueueDepthReporter(metrics ports.MetricsRecorder)
}

// SetQueueDepthReporter installs the recorder the provider publishes the
// robot's coalescing-queue depth on (the k8s_queue_depth gauge, dsca-1
// DS-1-1 fix item 1). The provider package stays free of a metrics
// dependency at construction: NewK8SProvider's signature (the providers'
// shared seam, internal/server.go's InitializeProviders) is unchanged, and
// the wiring that owns the recorder (internal/server.go, which already
// closes over it for the drop observer) calls this setter after
// construction, before Run. A nil recorder disables publication.
func (k *k8s) SetQueueDepthReporter(metrics ports.MetricsRecorder) {
	k.Lock()
	k.queueDepthMetrics = metrics
	k.Unlock()
}

// NacosReconcileSwitch is the k8s leg's wiring seam for the nacos
// reconcile mode (dsca-3 §3.3): internal/server.go asserts the constructed
// provider against it and calls SetNacosReconcileSource(true) exactly when
// the resolved config designates the nacos sink as the reconcile source
// (--reconcile-source nacos), before Run. The switch exists so the
// provider's compare semantics can follow the read routing without
// changing NewK8SProvider's shared signature.
type NacosReconcileSwitch interface {
	SetNacosReconcileSource(enabled bool)
}

// SetNacosReconcileSource turns the nacos-reconcile compare semantics on or
// off (see the nacosReconcile field). Call before Run — mid-flight flips
// are not part of any contract.
func (k *k8s) SetNacosReconcileSource(enabled bool) {
	k.Lock()
	k.nacosReconcile = enabled
	k.Unlock()
}

// reportQueueDepth publishes the current depth once. Called from the
// provider's own ticker goroutine (monitor below).
func (k *k8s) reportQueueDepth() {
	k.Lock()
	recorder := k.queueDepthMetrics
	k.Unlock()
	if recorder == nil {
		return
	}
	recorder.SetK8sQueueDepth(k.robot.QueueDepth())
}

// Run starts to monitor k8s cluster pod changes
func (k *k8s) Run() (err error) {
	log.Logger.Infof("start to run k8s provider")
	k.monitor()
	log.Logger.Info("k8s providers worker stopped")

	return nil
}

// monitor k8s pod changes
// Perform full instances synchronization periodically
// Perform instances comparison for single synchronization one by one. Note: this operation will only be executed once.
func (k *k8s) monitor() {
	go k.robot.Run()
	for {
		if k.robot.HasSynced() {
			break
		} else {
			// Notice handling
			log.Logger.Warnf("the robot has not synced yet")
			notice.Notice("robot failed to sync the K8s clusters", "While the robot is syncing the K8s clusters, the K8s clusters are not fully synced")
			time.Sleep(15 * time.Second)
		}
	}
	log.Logger.Infof("the robot has finished synced of all the k8s data, start to compare and sync instanes")
	go k.ProcessIntervalFullPush()
	// The queueDepth gauge's publication ticker (dsca-1 DS-1-1 fix item 1):
	// every queueDepthReportInterval the provider reads the robot's
	// coalescing-queue depth (a read-only distinct-key count, race-safe by
	// the queue's mutex) and hands it to the recorder installed through
	// SetQueueDepthReporter. Runs even when no recorder is installed (the
	// per-tick call is a nil check and a map-length read).
	go k.reportQueueDepthLoop()
	// start with full update(CompareAndFlush()),then do incremental update,and every 6 hours to full push(ProcessIntervalFullPush())
	k.CompareAndFlush()
	defer k.robot.Stop()
	// fork a goroutine to monitor pod change
	go func() {
		for {
			// get pod changes from k8s client
			obj, err := k.robot.Pop()
			if err != nil {
				log.Logger.Errorf("k8s client watch error: %s", err.Error())
				if k.stopped {
					break
				}
				time.Sleep(1 * time.Second)
				continue
			}
			log.Logger.Infof("get changes from k8s robot client, resource type: %s, key: %s, event: %s", obj.RType.String(), obj.Key, obj.Event.String())
			// trigger time. UnixNano, not Unix: dsca-2 §3 Option (b) widens
			// the Trigger unit to ns-since-epoch at every producer site so the
			// fan-out's e2e decorator (time.Since(time.Unix(0, trigger))) sees
			// sub-second latency instead of a seconds-quantized 0-or-1000ms.
			// Trigger stays int64; no consumer converts it back (audit §3).
			triggerTime := obj.CreateAt.UnixNano()
			// instance format
			var ins *sv.Instance
			if ins = k.pod2Instance(obj); ins == nil {
				continue
			}
			// rsync
			k.pool.Submit(func() {
				k.eventSync(ins, triggerTime)
			})
			k.robot.Finish(obj)
		}
	}()

	// wait to stop
	select {
	case <-k.ctx.Done():
		k.stopped = true
		break
	}

	log.Logger.Info("exit the k8s monitor")
}

func (k *k8s) ProcessCache(event k8srobot.EventType, ins *sv.Instance) {
	k.Lock()
	defer k.Unlock()
	switch event {
	case k8srobot.EventAdd:
		k.cache.ReplaceOrInsert(ins)
	case k8srobot.EventUpdate:
		k.cache.ReplaceOrInsert(ins)
	case k8srobot.EventDelete:
		k.cache.ReplaceOrInsert(ins)
	}
	k.generation++
}

func (k *k8s) snapshotForFullPush() ([]*sv.Instance, uint64, bool, bool) {
	all := k.cache.List()
	if k.robot != nil && k.robot.HasSynced() {
		if source := k.GetAll(); source != nil {
			all = source
		}
	}
	k.Lock()
	defer k.Unlock()
	if all == nil {
		all = []*sv.Instance{}
	}
	if len(all) == 0 {
		k.emptyConfirmations++
	} else {
		k.emptyConfirmations = 0
	}
	return all, k.generation, true, k.emptyConfirmations >= 3
}

// VerifyInstance checks wether the instance is valid
func (k *k8s) VerifyInstance(ins *sv.Instance) error {
	if k.filters != nil && len(k.filters) > 0 {
		for _, f := range k.filters {
			if err := f(ins); err != nil {
				return err
			}
		}
	}

	return nil
}

// eventSync sync the event to the finder
func (k *k8s) eventSync(ins *sv.Instance, triggerTime int64) {
	sequence := uint64(0)
	scope := "k8s"
	if ins != nil && ins.Reversion > 0 {
		sequence = uint64(ins.Reversion)
	}
	if ins != nil && ins.SourceCluster != "" {
		scope = ins.SourceCluster
	}
	k.worker.Handle(&worker.Event{
		Trigger:  triggerTime,
		Data:     []*sv.Instance{ins},
		Operate:  worker.OperateTypeSync,
		Scope:    scope,
		Identity: providers.IdentityKey(ins),
		Revision: ins.Reversion,
		Sequence: sequence,
	})
}

func (k *k8s) pod2Instance(obj k8srobot.QueueObject) (ins *sv.Instance) {
	// get pod info from k8s robot
	items, ok := k.robot.GetByClusterKey(k8srobot.Pods, obj.ClusterID, obj.Key)
	if obj.ClusterID == "" {
		items, ok = k.robot.GetByKey(k8srobot.Pods, obj.Key)
	}
	if ok && len(items) == 1 && obj.UID != "" {
		pod, valid := items[0].(*v1.Pod)
		if !valid || string(pod.UID) != obj.UID {
			ok = false
		}
	}
	if len(items) > 1 {
		return nil
	} // ambiguous legacy lookup must never select another cluster
	if ok && len(items) > 0 {
		if len(items) != 1 {
			return nil
		}
		pod, valid := items[0].(*v1.Pod)
		if !valid || pod == nil {
			return nil
		}
		instance := formatInstance(&obj, pod)
		if instance == nil {
			log.Logger.Errorf("formatting instance to be nil, pod name: %s", pod.Name)
			return nil
		}
		if ver := k.VerifyInstance(instance); ver != nil {
			log.Logger.Warnf("invalid instance, instanceid: %s, reason: %s", instance.InstanceId, ver.Error())
			return nil
		}
		// A pod that dies before ever receiving an IP (e.g. a Pending pod churned away mid
		// rolling-update) converts to an offline instance with an empty Ip. Such a shell cannot be
		// deregistered downstream: the nacos DELETE derives its target from the ip parameter, so an
		// empty ip is rejected with a permanent 400. And when the cache holds no last-known ip
		// either, the instance was never pushed to any sink, so there is nothing to deregister —
		// drop it instead of poisoning the retry queue. Otherwise recover the last-known fields
		// from the cache (a value copy; the cached pointer is never mutated in place), mirroring
		// the offline semantics of the CompareAndFlush case-3 branch, and let the diff flow
		// deregister the instance that was actually registered.
		if instance.Status == providers.InstanceStatusOffline && instance.Ip == "" {
			cached := k.cache.Get(providers.IdentityKey(instance))
			if cached == nil || cached.Ip == "" {
				log.Logger.Infof("drop offline instance %s with empty ip, nothing was registered downstream", instance.InstanceId)
				return nil
			}
			merged := *cached
			merged.Status = providers.InstanceStatusOffline
			merged.State = providers.InstanceStateTerminated
			merged.Enabled = false
			merged.Reversion = instance.Reversion
			instance = &merged
		}
		cacheInstance := k.cache.Get(providers.IdentityKey(instance))
		if cacheInstance == nil || k.hasInstanceDiff(cacheInstance, instance) {
			// put all exist instance to cache, purpose for get cache don't make npe
			k.ProcessCache(obj.Event, instance)
			ins = instance
		}
	} else {
		// If we cannot get the instance data, it means that this may be a DELETE event. At this point, the data in the
		// K8s robot no longer exists. However, in this scenario, the consumer of the instance needs
		// not only status = 2 but also the complete field data of the instance. Therefore, we have to fetch it from the cache.
		switch obj.Event {
		case k8srobot.EventAdd:
			fallthrough
			// log error
		case k8srobot.EventUpdate:
			// log error
		case k8srobot.EventDelete:
			instanceId := k.obj2InstanceId(obj)
			log.Logger.Infof("delete event, instanceid: %s", instanceId)
			key := obj.ClusterID + "/" + obj.UID
			if obj.ClusterID == "" || obj.UID == "" {
				key = "k8s:" + instanceId
			}
			if instance := k.cache.Get(key); instance != nil {
				if instance.Status != providers.InstanceStatusOffline {
					// set instance status
					instance.Status = providers.InstanceStatusOffline
					// delete cache
					k.ProcessCache(k8srobot.EventDelete, instance)
					ins = instance
				}
			}
		}
	}

	return ins
}

func (k *k8s) hasInstanceDiff(old, new *sv.Instance) (diff bool) {
	// If K8s instance Version > Finder instance version
	if old.Status == new.Status && old.Status == 3 { // if instance status is offline, we need not treat the change as difference
		diff = false
	} else if new.Reversion > old.Reversion {
		diff = true
	} else if new.EnvType != old.EnvType || new.State != old.State || new.Status != old.Status ||
		new.EnvGroup != old.EnvGroup || new.InstanceId != old.InstanceId || new.Ip != old.Ip ||
		new.Enabled != old.Enabled {
		// Excluding the CPU and memory comparison new.Cpu != old.Cpu || new.Memory != old.Memory, because the discovery center stores them as int type, so every comparison shows a difference
		// Enabled joins the compared set so a pure enabled flip (no status, state, reversion or ip change) still propagates — a no-op for every shape the current conversion produces (Enabled is derived from status there), pure hardening for future sources.
		diff = true
	}

	return diff
}

func (k *k8s) obj2InstanceId(obj k8srobot.QueueObject) string {
	if obj.Key != "" {
		keys := strings.Split(obj.Key, "/")
		if keys != nil && len(keys) >= 2 {
			return keys[1]
		}
	}

	return ""
}

// flush all instances
func (k *k8s) flushInstances() {
	// The guard on an empty list is intentionally KEPT here (unlike
	// emitSyncAll, AUDIT-B-4): flushInstances also owns the local cache
	// refill, and flushing the cache to empty on a transiently-empty source
	// would drop every cached instance only to re-add them one event at a
	// time. The vanished-service reconcile is emitSyncAll's job on the
	// interval tick; flushInstances is the startup path where an empty
	// source means "not synced yet" far more often than "everything died".
	if all := k.GetAll(); all != nil && len(all) > 0 {
		// flush the original cache and fill it
		before := time.Now()
		newCache := providers.NewCache(2)
		for _, ins := range all {
			newCache.ReplaceOrInsert(ins)
		}
		k.Lock()
		k.cache = newCache
		k.generation++
		generation := k.generation
		k.Unlock()
		log.Logger.Infof("flush k8s cache spend time: %s", unit.RelTime(before, time.Now(), "", ""))
		// push all. Origin is tick-time (time.Now at Event construction),
		// not CreateAt — the documented full-push origin semantics of
		// dsca-2 §6; UnixNano per the same unit widening.
		event := &worker.Event{
			Trigger:  time.Now().UnixNano(),
			Data:     all,
			Operate:  worker.OperateTypeSyncAll,
			Scope:    "k8s",
			BatchID:  worker.FullBatchID("k8s", all),
			Sequence: generation,
		}
		k.worker.Handle(event)
	}
}

// CompareAndFlush compare and find diff instances then flush
func (k *k8s) CompareAndFlush() {
	log.Logger.Infof("%s: trying to compare and find diff instances then flush", k.providerName)
	if all := k.GetAll(); all != nil && len(all) > 0 {
		// process the cache
		newCache := providers.NewCache(2)
		onlineCount := 0
		for _, item := range all {
			newCache.ReplaceOrInsert(item)
			if item.Status == 1 {
				onlineCount++
			}
		}
		k.Lock()
		k.cache = newCache
		k.generation++
		k.Unlock()
		// compare diffs and sync incrementally
		// the worker is exactly what communicates with Atlas (fetch from the discovery center, push data)
		list, err := k.worker.GetAll([]int32{providers.InstanceStatusOnline, providers.InstanceStatusUnhealthy}, providers.ProviderK8s)
		if err != nil {
			// DS-3-6 (dsca-3 §3.3 error-path alignment): in nacos-reconcile
			// mode the source is a remote store that can blip — a transient
			// 5xx or a slow catalog page — and treating the error as
			// "remote empty → push everything" would fire a full re-push
			// burst to both sinks precisely when the system is degraded
			// (the thundering herd the scale/latency tracks drive out). The
			// tick is skipped instead; the next interval retries the heal.
			// In the Atlas-primary world the historical push-everything
			// behavior is kept: an unreachable Atlas was a "resync
			// everything" trigger, and that world's semantics stay
			// untouched.
			if k.nacosReconcile {
				log.Logger.Errorf("get all instances from the reconcile source failed, skipping this compare tick: %s", err.Error())
				return
			}
			log.Logger.Errorf("get all instances from atlas failed")
		}
		if list == nil || list.Instance == nil || len(list.Instance) == 0 {
			for _, ins := range all {
				k.buildAndSendEvent(ins)
			}
			return
		} else if len(config.PushAppCodes) > 0 {
			instances := []*sv.Instance{}
			for _, ins := range list.Instance {
				for _, appcode := range config.PushAppCodes {
					if appcode == ins.AppCode {
						instances = append(instances, ins)
					}
				}
			}
			list.Instance = instances
		}
		// compare
		servMap, servAmbiguous := providers.StrictListToMap(list.GetInstance())
		k8sMap, k8sAmbiguous := providers.StrictListToMap(all)
		if len(servAmbiguous) > 0 || len(k8sAmbiguous) > 0 {
			log.Logger.Errorf("k8s: ambiguous instance identities; quarantining compare (remote=%v provider=%v)", servAmbiguous, k8sAmbiguous)
			return
		}
		log.Logger.Infof("discovery center online k8s instances size :%d  k8s online instance size :%d  total :%d", len(servMap), onlineCount, len(k8sMap))
		// bothExist,k8sExist two flag to notice
		bothExist := false
		k8sExist := false
		registryExist := false
		for k8sKey, k8sIns := range k8sMap {
			// case1: instance is both in K8s and the discovery center.
			// Instance data information to be pushed, subject to the data in K8s
			if servIns := providers.LookupIdentity(servMap, list.GetInstance(), k8sIns); servIns != nil {
				diff := k.hasInstanceDiff(servIns, k8sIns)
				// R2 (dsca-3 §3.3), nacos-reconcile mode only: reversion is
				// provider-owned monotonic state, not an authority token —
				// the "strictly-higher wins" gate was Atlas's database guard
				// (a receiver-side protection against out-of-order pushes;
				// nacos v1 register is an unconditional upsert with no
				// server-side rejection to respect). ANY reversion mismatch,
				// either direction, is drift, and the local value wins: a
				// remote reversion above the local ceiling is reachable only
				// out-of-band (a forged/edited metadata reversion — spotter
				// is the single writer under leader election), and adopting
				// Atlas's "remote higher wins" here would let one forged
				// bump permanently blind the reconcile.
				if !diff && k.nacosReconcile && servIns.Reversion != k8sIns.Reversion {
					diff = true
					// The R2 companion signal (dsca-5 DS-5-4, adopted per
					// dsca-3's lead ruling): a remote reversion ABOVE the
					// local one witnesses an out-of-band write — R2's push
					// silently normalizes the value, so the event deserves
					// its own log line. A notice on top of the push, never
					// instead of it.
					if servIns.Reversion > k8sIns.Reversion {
						log.Logger.Warnf("the instance: %s of appcode: %s carries a nacos reversion %d above the local %d (out-of-band edit suspected); the reconcile push overwrites it with the local value", k8sIns.InstanceId, k8sIns.AppCode, servIns.Reversion, k8sIns.Reversion)
						notice.Notice("Forged remote reversion", fmt.Sprintf("The k8s instance %s of appcode %s holds nacos reversion %d above the local %d (out-of-band edit suspected); the reconcile overwrites the remote value with the local one", k8sIns.InstanceId, k8sIns.AppCode, servIns.Reversion, k8sIns.Reversion))
					}
				}
				if diff {
					log.Logger.Infof("the instance: %s of appcode: %s is newer, trigger a push.", k8sIns.InstanceId, k8sIns.AppCode)
					bothExist = true
					k.buildAndSendEvent(k8sIns)
				}
				delete(k8sMap, k8sKey)
				delete(servMap, providers.IdentityKey(servIns))
			} else {
				// case2: instance is both in K8s, but not in the discovery center.
				// Instance is newer than the discovery center, subject to the data in K8s
				log.Logger.Infof("k8s match much id: %v , status : %v \n", k8sIns.InstanceId, k8sIns.Status)
				if k8sIns.Status == 1 {
					k8sExist = true
					k.buildAndSendEvent(k8sIns)
				}
				delete(k8sMap, k8sKey)
			}
		}
		// case 1 notice
		if bothExist {
			notice.Notice("Instance data inconsistency", "Full push: data inconsistency between the discovery center and the K8s clusters, the instances are the same in the discovery center and the K8s clusters, but some data fields of the instances differ")
		}
		// case 2 notice
		if k8sExist {
			notice.Notice("Instance data inconsistency", "Full push: data inconsistency between the discovery center and the K8s clusters, the instances differ between the discovery center and the K8s clusters, some instances exist in the K8s clusters but not in the discovery center")
		}
		// case3: instance is not is K8s, but in the discovery center.
		// Then instances should not be exists in the discovery center, just delete it.
		if len(servMap) > 0 {
			log.Logger.Infof("atlas server pre delete instance size :%d \n", len(servMap))
			for _, servIns := range servMap {
				servIns.Enabled = false
				servIns.Status = 3
				servIns.State = providers.InstanceStateTerminated
				registryExist = true
				k.buildAndSendEvent(servIns)

			}
			// case 3 notice
			if registryExist {
				notice.Notice("Instance data inconsistency", "Full push: data inconsistency between the discovery center and the K8s clusters, the instances differ between the discovery center and the K8s clusters, some instances exist in the discovery center but not in the K8s clusters")
			}
		}
	}
}

func (k *k8s) buildAndSendEvent(instance *sv.Instance) {
	// if instance status is 0 , don't send event
	k.pool.Submit(func() {
		if instance.Status == 0 {
			return
		}
		ins := make([]*sv.Instance, 1)
		ins[0] = instance
		// Tick-time origin (this builds the reconcile push, not a watch
		// event) + ns unit: dsca-2 §3 Option (b) / §6 origin semantics.
		triggerTime := time.Now().UnixNano()
		event := &worker.Event{
			Trigger: triggerTime,
			Data:    ins,
			Operate: worker.OperateTypeSync,
		}
		k.worker.Handle(event)
	})
}

func (k *k8s) GetAll() (result []*sv.Instance) {
	// Always a non-nil list: an empty source is the reconcile signal of
	// AUDIT-B-4, and emitSyncAll's event must carry an empty slice, not a
	// nil one, so the worker/sink seam observes a well-formed batch.
	result = []*sv.Instance{}
	items := k.robot.List(k8srobot.Pods)
	if source, ok := k.robot.(interface {
		ListSourceObjects(k8srobot.ResourceType) []k8srobot.SourceObject
	}); ok {
		items = nil
		for _, item := range source.ListSourceObjects(k8srobot.Pods) {
			items = append(items, item)
		}
	}
	for _, item := range items {
		var obj *k8srobot.QueueObject
		if source, ok := item.(k8srobot.SourceObject); ok {
			obj = &source.QueueObject
			item = source.Object
		}
		pod := item.(*v1.Pod)
		instance := formatInstance(obj, pod)
		if instance == nil {
			continue
		}
		if ver := k.VerifyInstance(instance); ver == nil {
			if result == nil {
				result = []*sv.Instance{}
			}
			result = append(result, instance)
		} else {
			log.Logger.Warnf("invalid instance, instanceid: %s, reason: %s", instance.InstanceId, ver.Error())
		}
	}
	log.Logger.Infof("k8s get all size: %d", len(result))

	return
}

// ProcessIntervalFullPush will sync all Instances of the current Provider to the the discovery center.
// Note: The current synchronization behavior is not to directly call the SyncAll method of the discovery center,
// but to perform instances comparison and do instance synchronization one by one using Method CompareAndFlush.
// After CompareAndFlush completes, the tick also emits one OperateTypeSyncAll event carrying the
// provider's full instance list (plan §7.4): that event revives the worker's dormant SyncAll handler,
// so every sink's full-push reconcile (for example the Nacos PushAll prune) runs each push interval.
func (k *k8s) ProcessIntervalFullPush() {
	interval := providers.FullPushInterval
	if k.interval != 0 {
		interval = time.Duration(k.interval) * time.Second
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			before := time.Now()
			k.CompareAndFlush()
			k.emitSyncAll()
			after := time.Now()
			offset := after.Sub(before).Milliseconds()
			metrics.SyncAllK8sDurationsHistogram.Observe(float64(offset))
			log.Logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
		case <-k.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// emitSyncAll pushes the provider's full instance list as one SyncAll event
// through the existing worker.Handle seam. The event is emitted even when
// the list is EMPTY (AUDIT-B-4): the emission keeps the full-push reconcile
// cadence uniform — every sink's PushAll runs each interval, and an empty
// list is a conservative no-op at the Nacos sink (a bare empty push carries
// no provider identity, so the sink sweeps nothing remembered; wiping every
// remembered pair on it would be the cross-provider incident — one
// provider's empty list deleting every other provider's instances, see the
// Nacos Sink's remembered field). The vanished-service heal rides this
// event's NON-empty pushes: the pushed instances carry the provider tag
// (Provider -> clusterName), and the sink prunes the remembered pairs of
// exactly that cluster whose desired set went empty — the provider's
// vanished services, never another provider's.
func (k *k8s) emitSyncAll() {
	all, generation, valid, emptyConfirmed := k.snapshotForFullPush()
	if !valid {
		return
	}
	// Tick-time origin + ns unit (dsca-2 §3 Option (b), §6 origin semantics):
	// a PushAll observation measures "age of the full push at completion",
	// not event age.
	k.worker.Handle(&worker.Event{
		Trigger:        time.Now().UnixNano(),
		Data:           all,
		Operate:        worker.OperateTypeSyncAll,
		Scope:          "k8s",
		BatchID:        worker.FullBatchID("k8s", all),
		Sequence:       generation,
		EmptyConfirmed: emptyConfirmed,
		Revalidate: func() ([]*sv.Instance, bool) {
			latest, current, ok, _ := k.snapshotForFullPush()
			if !ok {
				return nil, false
			}
			if current != generation {
				return latest, true
			}
			return all, true
		},
	})
}

// reportQueueDepthLoop publishes the robot's coalescing-queue depth on the
// k8s_queue_depth gauge until the provider's context ends (see
// queueDepthReportInterval for the cadence choice).
func (k *k8s) reportQueueDepthLoop() {
	ticker := time.NewTicker(queueDepthReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			k.reportQueueDepth()
		case <-k.ctx.Done():
			return
		}
	}
}

// withExpiryDuration sets up the interval time of cleaning up goroutines.
func withExpiryDuration(expiryDuration time.Duration) ants.Option {
	return func(opts *ants.Options) {
		opts.ExpiryDuration = expiryDuration
	}
}
