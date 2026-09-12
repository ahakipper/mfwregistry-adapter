package consul

import (
	"context"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"spotter/internal/ports"
	"spotter/pkg/beehive/service/v2"
	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/log"
	"spotter/pkg/metrics"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
	"spotter/tools/unit"
	"sync"
	"time"
)

// K8S provider implement
type consul struct {
	providerName       string
	clientFactory      ConsulClientFactory        // consul api factory
	monitor            Monitor                    // monitor
	ctx                context.Context            // context
	worker             worker.Worker              // the role of worker is to synchronize provider changes to the discovery center
	stopped            bool                       // whether the current provider has stopped
	sync.Mutex                                    // lock mutex
	interval           int                        // the time interval for full synchronization. default 7200s(2h)
	filters            []providers.InstanceFilter // filters is a collection of functions used to filter invalid instances
	cache              providers.CacheIterface    // consul endpoint cache
	generation         uint64
	emptyConfirmations uint32
	pool               *ants.Pool // goroutine pool
	overflowMu         sync.Mutex
	overflow           *providers.OverflowQueue
	runWG              sync.WaitGroup
	done               chan struct{}
	stopOnce           sync.Once
	depsOnce           sync.Once
	initDone           bool
	logger             ports.Logger
	notifier           ports.Notifier
	metricsRecorder    ports.MetricsRecorder

	// sourceErr records the error of the last GetAll when the consul source
	// could not be read (nil after a successful read, empty or not): the
	// error/empty distinction GetAll's nil result cannot carry itself. Every
	// access — the writes inside GetAll and the reads in emitSyncAll and the
	// tests' sourceError helper — happens under the provider lock: a mutex
	// orders only accesses that BOTH take it, so the tick path (emitSyncAll)
	// must lock just like the handler paths (syncInstance, CompareAndFlush)
	// already do. See GetAll's comment for the caller contract.
	sourceErr error

	// nacosReconcile records that the periodic CompareAndFlush's remote view
	// is the nacos sink (dsca-3 §3.1, --reconcile-source nacos). It switches
	// the compare onto the dsca-3 §3.3 rule set:
	//   - the wire projection wireEnabled(local) = local.Enabled &&
	//     local.Status != 2 is compared against remote.Enabled instead of
	//     the raw field (DS-5-2: the consul converter hardcodes local
	//     Enabled=true while the nacos register forces wire enabled=false
	//     for every status-2 instance — a raw compare would diff every
	//     cycle forever);
	//   - Provider replaces Cluster in the compared set (DS-5-3/DS-3-4: the
	//     reconstruction leaves Cluster empty, clusterName lands in
	//     Provider, which round-trips exactly);
	//   - R2: any reversion mismatch, either direction, is drift (local
	//     wins; a remote reversion above the local one additionally logs
	//     the forged-revision companion signal);
	//   - case 3 pushes Status=3 (deregister) instead of Status=2 — the
	//     Atlas "old instance, mark not-ready" semantics would heal
	//     remote-only ecs instances into permanent disabled zombies against
	//     nacos (DS-3-5).
	// The [online] status request and the case-2 Status==1 gate stay as-is:
	// both are REQUIRED invariants in their own right (pinned by tests), and
	// the wire projection makes the compare correct regardless of them.
	nacosReconcile bool
}

func (c *consul) ensureDeps() {
	c.depsOnce.Do(func() {
		if c.done == nil {
			c.done = make(chan struct{})
		}
		if c.logger == nil {
			c.logger = ports.NopLogger{}
		}
		if c.notifier == nil {
			c.notifier = nopNotifier{}
		}
	})
}

// NacosReconcileSwitch is the consul leg's wiring seam for the nacos
// reconcile mode (dsca-3 §3.3): internal/server.go asserts the constructed
// provider against it and calls SetNacosReconcileSource(true) exactly when
// the resolved config designates the nacos sink as the reconcile source
// (--reconcile-source nacos), before Run. The switch exists so the compare
// semantics can follow the read routing without changing
// NewConsulProvider's shared signature.
type NacosReconcileSwitch interface {
	SetNacosReconcileSource(enabled bool)
}

// MetricsReporter is the explicit metrics wiring seam. The compatibility
// constructor leaves it nil and retains package collectors; production
// composition installs the injected recorder before Run.
type MetricsReporter interface {
	SetMetricsRecorder(ports.MetricsRecorder)
}

func (c *consul) SetMetricsRecorder(recorder ports.MetricsRecorder) {
	c.Lock()
	c.metricsRecorder = recorder
	c.Unlock()
}

// SetNacosReconcileSource turns the nacos-reconcile compare semantics on or
// off (see the nacosReconcile field). Call before Run — mid-flight flips
// are not part of any contract.
func (c *consul) SetNacosReconcileSource(enabled bool) {
	c.Lock()
	c.nacosReconcile = enabled
	c.Unlock()
}

// NewConsulProvider creates consul provider
func NewConsulProvider(ctx context.Context, worker worker.Worker, pushInterval int, addrs []string) (provider providers.Provider, err error) {
	return NewConsulProviderWithDeps(ctx, worker, pushInterval, addrs, log.Logger, legacyNotifier{})
}

// NewConsulProviderWithDeps constructs a provider from explicit runtime
// collaborators. The legacy constructor above is retained only as a
// compatibility wrapper for old global-based callers.
func NewConsulProviderWithDeps(ctx context.Context, worker worker.Worker, pushInterval int, addrs []string, logger ports.Logger, notifier ports.Notifier) (provider providers.Provider, err error) {
	if ctx == nil || len(addrs) == 0 || worker == nil {
		err = errors.New("params invalid")
		return nil, err
	}
	var cf ConsulClientFactory
	if cf, err = NeweClientFacotorySimple(addrs); err != nil {
		return nil, err
	}
	var monitor Monitor
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if notifier == nil {
		notifier = nopNotifier{}
	}
	if monitor, err = NewConsulMonitor(cf, logger, notifier, nil); err != nil {
		return nil, err
	}
	consulProvider := &consul{
		providerName:  "consul",
		ctx:           ctx,
		monitor:       monitor,
		worker:        worker,
		clientFactory: cf,
		// interval honors --push-interval for the ecs leg exactly like the
		// k8s provider does (NewK8SProvider assigns the same field): it
		// bounds the periodic CompareAndFlush + SyncAll cadence. The field
		// was declared but never assigned here, so the periodic path fell
		// back to the 21600s default and ignored the flag.
		interval: pushInterval,
		cache:    providers.NewCache(8),
		done:     make(chan struct{}),
		logger:   logger,
		notifier: notifier,
	}
	// Create pool for sending instance events to the discovery center
	p, _ := ants.NewPool(providers.PoolBenchSize, withExpiryDuration(time.Second*providers.PoolExpireTime), ants.WithNonblocking(true))
	consulProvider.pool = p
	// Init instance fiter
	consulProvider.filters = providers.InitInstanceFilters()

	// Watch the change events to refresh local caches
	// monitor.AppendServiceHandler(provider.ServiceChanged)
	monitor.AppendInstanceHandler(consulProvider.InstanceChanged)

	//return &controller, err
	return consulProvider, nil
}

// overflowQueue lazily creates the bounded identity-keyed requeue used when
// the non-blocking ants pool is saturated. A source update is retained until
// the pool accepts it; repeated updates for one identity replace the older
// task, preventing stale bursts after a short Consul callback stall.
func (c *consul) overflowQueue() *providers.OverflowQueue {
	c.ensureDeps()
	c.overflowMu.Lock()
	defer c.overflowMu.Unlock()
	if c.overflow != nil {
		return c.overflow
	}
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	c.overflow = providers.NewOverflowQueue(ctx, providers.PoolBenchSize*4, func(task func()) error {
		if c.pool == nil {
			return ants.ErrPoolClosed
		}
		return c.pool.Submit(task)
	}, func(key string) {
		c.logger.Warnf("consul event overflow identity %s dropped after bounded requeue filled", key)
	})
	return c.overflow
}

func (c *consul) requeueEvent(key string, task func()) {
	c.ensureDeps()
	if c.overflowQueue().Offer(key, task) {
		c.logger.Warnf("consul worker pool saturated; requeued identity %s", key)
	}
}

// shutdown stops the provider-owned dispatcher and ants workers. Closing the
// dispatcher first waits out its retry timer, so pool.Release cannot race a
// retry submission and pending identities are explicitly observable.
func (c *consul) shutdown() {
	c.ensureDeps()
	c.stopOnce.Do(func() {
		if c.done != nil {
			close(c.done)
		}
	})
	c.Lock()
	c.stopped = true
	c.Unlock()
	c.overflowMu.Lock()
	q := c.overflow
	c.overflowMu.Unlock()
	if q != nil {
		if remaining := q.Close(); remaining > 0 {
			c.logger.Warnf("consul provider stopped with %d overflow identities pending", remaining)
		}
	}
	if c.pool != nil {
		c.pool.Release()
	}
}

func (c *consul) Run() (err error) {
	c.ensureDeps()
	defer c.shutdown()
	c.logger.Infof("start to run ecs provider")
	// Perform full instances synchronization periodically
	c.runWG.Add(2)
	go func() {
		defer c.runWG.Done()
		c.ProcessIntervalFullPush()
	}()
	// Perform instances comparison for single synchronization one by one. Note: this operation will only be executed once.
	go func() {
		defer c.runWG.Done()
		c.CompareAndFlush()
	}()
	// monitor will hang
	err = c.monitor.Start(c.ctx)
	// Monitor cancellation is the provider lifecycle boundary. Join both
	// background reconcile goroutines before releasing the worker pool so Run
	// returning proves no late callback can submit more work.
	c.runWG.Wait()
	if err != nil {
		c.notifier.Notify("consul stopped working", err.Error())
		err = errors.WithMessage(err, "consul provider stopped")
		c.logger.Errorf("%s", err.Error())
	}
	c.logger.Info("consul providers worker stopped")

	return err
}

// syncInstance will sync consul endpoints to the discovery center.
// It will get all the services tagged as "microservice", and convert consul endpoint model to the discovery center Model.
// Then compare the new instances list with the instances we cached before so that we can generate the instances which are
// added, updated and deleted.
func (c *consul) syncInstance() (err error) {
	c.Lock()
	defer c.Unlock()
	oldCache := c.cache
	newCache := providers.NewCache(8)
	// Get all services from consul
	currentInss := c.GetAll()
	// Here, we assume that the consul data is impossible to be empty. Once it is empty,
	// no operation is performed except for return an error.
	if currentInss == nil || len(currentInss) == 0 {
		err = errors.New("get empty consul endpoints")
		c.logger.Warnf("%s", err.Error())
		return err
	}
	for _, ins := range currentInss {
		newCache.ReplaceOrInsert(ins)
	}
	// Compare to generate events
	addEvents, updateEvents, deleteEvents := c.extractDiff(oldCache, newCache)
	//pp.Println(map[string][]*sv.Instance{"add": addEvents, "update": updateEvents, "delete": deleteEvents})
	// push events
	c.EventsSync(addEvents, updateEvents, deleteEvents)
	// Update cache
	c.cache = newCache
	c.generation++

	return err
}

func (c *consul) toInstance(endpoints []*api.ServiceEntry) (inss []*sv.Instance) {
	// get pod info from k8s robot
	inss = []*sv.Instance{}
	if len(endpoints) > 0 {
		for _, ep := range endpoints {
			if ins, err := convertInstance(ep); err != nil {
				c.logger.Errorf("%s", err.Error())
				continue
			} else {
				inss = append(inss, ins)
			}
		}
	}

	return inss
}

// GetAll returns the full consul instance list. A nil result is ambiguous
// between "source errored" (monitor GetServices/GetServiceEntries failure)
// and "source legitimately empty": the sourceErr flag below distinguishes
// the two.
//
// Lock discipline: every caller holds the provider lock across its GetAll
// call — syncInstance and CompareAndFlush take it themselves, and
// emitSyncAll takes it around its own read — so GetAll runs entirely under
// the lock and writes the flag in-lock (a mutex orders only accesses that
// BOTH take it; an unlocked tick-path write racing the handler-goroutine
// writes was the round-2 review's data race). Callers outside this file
// must not call GetAll without the lock.
func (c *consul) GetAll() (result []*v2.Instance) {
	// Get all services from consul
	var err error
	var consulServices map[string][]string
	consulServices, err = c.monitor.GetServices()
	if err != nil {
		err = errors.WithMessage(err, "get services from consul")
		c.logger.Errorf("%s", err.Error())
		c.sourceErr = err
		return nil
	}
	// Process new cache
	if len(consulServices) > 0 {
		result = []*sv.Instance{}
		for serviceName := range consulServices {
			// get endpoints of a service from consul
			var endpoints []*api.ServiceEntry
			endpoints, err = c.monitor.GetServiceEntries(serviceName, nil)
			if err != nil {
				err = errors.WithMessage(err, "get service endpoints from consul")
				c.logger.Errorf("%s", err.Error())
				c.sourceErr = err
				return nil
			}
			if instances := c.toInstance(endpoints); instances != nil {
				for _, ins := range instances {
					result = append(result, ins)
				}
			}
		}
	}
	c.sourceErr = nil
	c.logger.Infof("consul getall size: %d", len(result))

	return result
}

// sourceError returns the error of the last GetAll, if any. Test-only
// observation helper for the flag emitSyncAll decides on: it takes the
// provider lock like every other sourceErr access (the discipline
// documented on the field).
func (c *consul) sourceError() error {
	c.Lock()
	defer c.Unlock()
	return c.sourceErr
}

func (c *consul) InstanceChanged(instance *api.CatalogService) (err error) {
	err = c.syncInstance()
	return err
}

func (c *consul) ServiceChanged(instances []*api.CatalogService) (err error) {
	err = c.syncInstance()
	return err
}

func (c *consul) extractDiff(old, new providers.CacheIterface) (add []*v2.Instance, update []*sv.Instance, del []*sv.Instance) {
	// pp.Println(len(old.List()), len(new.List()))
	add = []*sv.Instance{}
	update = []*sv.Instance{}
	del = []*sv.Instance{}
	if old == nil && new != nil {
		for _, ins := range new.List() {
			add = append(add, ins)
		}
		return
	} else if old != nil && new == nil {
		return
	} else if old != nil && new != nil {
		// add & update events
		for _, newIns := range new.List() {
			// new cache has the instance in old cache
			if oldIns := old.Get(providers.IdentityKey(newIns)); oldIns != nil {
				// update events
				if newIns.Reversion > oldIns.Reversion {
					if ver := c.VerifyInstance(newIns); ver == nil {
						update = append(update, newIns)
					} else {
						c.logger.Warnf("invalid instance, instanceid: %s, reason: %s", newIns.InstanceId, ver.Error())
					}
				}
			} else {
				// add events
				if ver := c.VerifyInstance(newIns); ver == nil {
					newIns.Status = providers.InstanceStatusOnline
					newIns.Enabled = true
					newIns.State = providers.InstanceStateRunning
					add = append(add, newIns)
				} else {
					c.logger.Warnf("invalid instance, instanceid: %s, reason: %s", newIns.InstanceId, ver.Error())
				}
			}
		}
		// delete events
		for _, oldIns := range old.List() {
			// old cache has the instance which not in new cache
			if newIns := new.Get(providers.IdentityKey(oldIns)); newIns == nil {
				// delete events
				oldIns.Status = providers.InstanceStatusOffline
				oldIns.Enabled = false
				oldIns.State = providers.InstanceStateTerminated
				del = append(del, oldIns)
			}
		}
	}

	return
}

// VerifyInstance checks wether the instance is valid
func (c *consul) VerifyInstance(ins *sv.Instance) error {
	if c.filters != nil && len(c.filters) > 0 {
		for _, f := range c.filters {
			if err := f(ins); err != nil {
				return err
			}
		}
	}

	return nil
}

// EventsSync sync the event to the finder
func (c *consul) EventsSync(add, update, del []*sv.Instance) {
	// time.Now per slice so each event carries its own emission instant.
	// UnixNano, not Unix: dsca-2 §3 Option (b) widens Trigger to
	// ns-since-epoch at every producer site (the consul sites are REQUIRED,
	// not optional: the sink-side e2e decorator cannot tell which provider
	// emitted an Event, and a seconds-valued Trigger would observe ~57
	// years — dsca-2 §6 row 2).
	if len(add) > 0 {
		for _, ins := range add {
			c.eventSync(ins, time.Now().UnixNano())
		}
	}
	if len(update) > 0 {
		for _, ins := range update {
			c.eventSync(ins, time.Now().UnixNano())
		}
	}
	if len(del) > 0 {
		for _, ins := range del {
			c.eventSync(ins, time.Now().UnixNano())
		}
	}
}

// eventSync sync the event to the finder
func (c *consul) eventSync(ins *sv.Instance, triggerTime int64) {
	sequence := uint64(0)
	scope := "ecs"
	if ins != nil && ins.Reversion > 0 {
		sequence = uint64(ins.Reversion)
	}
	if ins != nil && ins.Provider != "" {
		scope = ins.Provider
	}
	c.worker.Handle(&worker.Event{
		Trigger:  triggerTime,
		Data:     []*sv.Instance{ins},
		Operate:  worker.OperateTypeSync,
		Scope:    scope,
		Identity: providers.IdentityKey(ins),
		Revision: ins.Reversion,
		Sequence: sequence,
	})
}

// ProcessIntervalFullPush will sync all Instances of the current Provider to the the discovery center.
// Note: The current synchronization behavior is not to directly call the SyncAll method of the discovery center,
// but to perform instances comparison and do instance synchronization one by one using Method CompareAndFlush.
// After CompareAndFlush completes, the tick also emits one OperateTypeSyncAll event carrying the
// provider's full instance list (plan §7.4): that event revives the worker's dormant SyncAll handler,
// so every sink's full-push reconcile (for example the Nacos PushAll prune) runs each push interval.
// TODO: the Provider methods are duplicated, this needs to be optimized later
func (c *consul) ProcessIntervalFullPush() {
	var interval time.Duration = providers.FullPushInterval
	if c.interval != 0 {
		interval = time.Duration(c.interval) * time.Second
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			before := time.Now()
			c.CompareAndFlush()
			c.emitSyncAll()
			after := time.Now()
			offset := after.Sub(before).Milliseconds()
			c.Lock()
			recorder := c.metricsRecorder
			c.Unlock()
			if recorder != nil {
				recorder.ObserveSyncAllDuration(providers.ProviderEcs, after.Sub(before))
			} else {
				metrics.SyncAllEcsDurationsHistogram.Observe(float64(offset))
			}
			c.logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
		case <-c.ctx.Done():
			ticker.Stop()
			return
		case <-c.done:
			ticker.Stop()
			return
		}
	}
}

// emitSyncAll pushes the provider's full instance list as one SyncAll event
// through the existing worker.Handle seam. The event is emitted even when
// the list is EMPTY (AUDIT-B-4, mirroring the k8s provider): the emission
// keeps the full-push reconcile cadence uniform — every sink's PushAll runs
// each interval, and an empty list is a conservative no-op at the Nacos
// sink (a bare empty push carries no provider identity, so the sink sweeps
// nothing remembered; wiping every remembered pair on it would be the
// cross-provider incident — one provider's empty list deleting every other
// provider's instances, see the Nacos Sink's remembered field). The
// vanished-service heal rides this event's NON-empty pushes: the pushed
// instances carry the provider tag (Provider -> clusterName), and the sink
// prunes the remembered pairs of exactly that cluster whose desired set
// went empty — this provider's vanished services, never another's.
//
// The one exception is a FAILED source read (agent-2 review of the B-4
// fix): consul's GetAll returns nil on monitor errors too, and an
// error-time empty SyncAll would assert a false "everything vanished"
// full state to every sink — the Nacos sink treats an empty push
// conservatively (no remembered sweep), but the Atlas full-sync carries
// the empty list to the server, whose semantics spotter does not own.
// When the last GetAll errored, emission is skipped and a warning
// is logged instead; the next tick retries. (The k8s provider needs no
// equivalent: its informer-cache List cannot error, and the full-push loop
// starts only after HasSynced.)
//
// The whole read-then-decide sequence runs under the provider lock (the
// round-2 review's fix): the lock pairs this goroutine's sourceErr write
// (inside GetAll) with the handler-goroutine writes under
// syncInstance/CompareAndFlush — the tick path must take the same lock or
// the flag has no happens-before edge. Holding it across worker.Handle is
// safe: Handle is a plain handler-table dispatch, and CompareAndFlush
// already holds this lock across the worker.GetAll RPC (a strictly wider
// footprint), so no new lock-ordering surface is introduced.
func (c *consul) emitSyncAll() {
	all, generation, valid, emptyConfirmed := c.snapshotForFullPush()
	if !valid {
		return
	}
	// Tick-time origin + ns unit (dsca-2 §3 Option (b), §6 origin semantics).
	c.worker.Handle(&worker.Event{
		Trigger:        time.Now().UnixNano(),
		Data:           all,
		Operate:        worker.OperateTypeSyncAll,
		Scope:          "ecs",
		BatchID:        worker.FullBatchID("ecs", all),
		Sequence:       generation,
		EmptyConfirmed: emptyConfirmed,
		Revalidate: func() ([]*v2.Instance, bool) {
			latest, current, ok, _ := c.snapshotForFullPush()
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

func (c *consul) snapshotForFullPush() ([]*v2.Instance, uint64, bool, bool) {
	c.Lock()
	defer c.Unlock()
	all := c.cache.List()
	if c.monitor != nil {
		all = c.GetAll()
	}
	if c.sourceErr != nil {
		return nil, c.generation, false, false
	}
	if all == nil {
		all = []*v2.Instance{}
	}
	if len(all) == 0 {
		c.emptyConfirmations++
	} else {
		c.emptyConfirmations = 0
	}
	return all, c.generation, true, c.emptyConfirmations >= 3
}

// CompareAndFlush compare and find diff instances then flush
func (c *consul) CompareAndFlush() {
	c.Lock()
	defer c.Unlock()
	c.logger.Infof("%s: trying to compare and find diff instances then flush", c.providerName)
	// Here, we assume that the consul data is impossible to be empty. Once it is empty,
	// no operation is performed.
	if all := c.GetAll(); all != nil && len(all) > 0 {
		// process the cache
		c.cache.Clear()
		onlineCount := 0
		for _, item := range all {
			c.cache.ReplaceOrInsert(item)
			if item.Status == 1 {
				onlineCount++
			}
		}
		c.generation++
		// compare diffs and sync incrementally
		registryList, err := c.worker.GetAll([]int32{providers.InstanceStatusOnline}, providers.ProviderEcs)
		if err != nil {
			err = errors.WithMessage(err, "get all instances from the discovery center")
			c.logger.Errorf("%s", err.Error())
			return
		}
		if registryList == nil || registryList.Instance == nil || len(registryList.Instance) == 0 {
			for _, ins := range all {
				c.buildAndSendEvent(ins)
			}
			return
		}
		remoteInstances, remoteAmbiguous := providers.StrictListToMap(registryList.GetInstance())
		// pp.Println(remoteInstances)
		currentProviderInstances, providerAmbiguous := providers.StrictListToMap(all)
		if len(remoteAmbiguous) > 0 || len(providerAmbiguous) > 0 {
			c.logger.Errorf("%s: ambiguous instance identities; quarantining compare (remote=%v provider=%v)", c.providerName, remoteAmbiguous, providerAmbiguous)
			return
		}
		c.logger.Infof("discovery center online ecs instances size :%d  consul online instance size :%d  total :%d", len(remoteInstances), onlineCount, len(currentProviderInstances))
		//bothExist,k8sExist two flag to notice
		bothExist := false
		ecsExist := false
		registryExist := false
		for consulKey, consulIns := range currentProviderInstances {
			// For these instances in both Provider and the discovery center, if the information in Provider is newer, push is performed.
			if servIns := providers.LookupIdentity(remoteInstances, registryList.GetInstance(), consulIns); servIns != nil {
				diff := false
				// The R2 rule of dsca-3 §3.3, applied in nacos-reconcile
				// mode: reversion is provider-owned monotonic state, not an
				// authority token (the strictly-higher/equal gates were
				// Atlas's database guard; nacos v1 register is an
				// unconditional upsert with no server-side rejection to
				// respect). ANY reversion mismatch — either direction — is
				// drift and the local value wins: a remote reversion above
				// the local ceiling is reachable only out-of-band (a forged
				// console edit — spotter is the single writer under leader
				// election), and the old equal-revision field gate would
				// have suppressed the heal forever (DS-5-4's complete
				// suppression on the consul leg).
				if c.nacosReconcile && consulIns.Reversion != servIns.Reversion {
					diff = true
					// The R2 companion signal (dsca-5 DS-5-4, adopted per
					// dsca-3's lead ruling): a remote reversion ABOVE the
					// local one witnesses an out-of-band write — R2's push
					// silently normalizes the value, so the event deserves
					// its own log line. A notice on top of the push, never
					// instead of it.
					if servIns.Reversion > consulIns.Reversion {
						c.logger.Warnf("the instance: %s of appcode: %s carries a nacos reversion %d above the local %d (out-of-band edit suspected); the reconcile push overwrites it with the local value", consulIns.InstanceId, consulIns.AppCode, servIns.Reversion, consulIns.Reversion)
						c.notifier.Notify("Forged remote reversion", fmt.Sprintf("The ecs instance %s of appcode %s holds nacos reversion %d above the local %d (out-of-band edit suspected); the reconcile overwrites the remote value with the local one", consulIns.InstanceId, consulIns.AppCode, servIns.Reversion, consulIns.Reversion))
					}
				} else if consulIns.Reversion > servIns.Reversion {
					diff = true
				} else if consulIns.Reversion == servIns.Reversion {
					// The wire projection of Enabled (dsca-3 §3.3 / dsca-5
					// DS-5-2), nacos-reconcile mode only: compare
					// wireEnabled(local) = local.Enabled && local.Status !=
					// InstanceStatusUnhealthy against remote.Enabled —
					// exactly the derivation the nacos register applies
					// before writing (register forces enabled=false for
					// every status-2 instance) — so the compare asserts
					// precisely "would the next push write a different
					// enabled than the remote holds". The raw compare is
					// asymmetric on the consul leg (the converter hardcodes
					// local Enabled=true, convertion.go) and would diff
					// every status-2 pair every cycle forever.
					//
					// Provider replaces Cluster (dsca-5 §4.2-3, DS-3-4):
					// the nacos reconstruction leaves Cluster empty (the
					// metadata never carried it; clusterName lands in
					// Provider, which round-trips exactly — clusterOf <->
					// clusterName), so comparing the reconstructed Cluster
					// against a local Cluster that both converters leave
					// "" would false-positive every instance every cycle.
					// In Atlas-primary mode the remote view round-trips the
					// model verbatim, so the original Cluster compare stays.
					consulEnabled := consulIns.Enabled
					remoteCluster := servIns.Cluster
					if c.nacosReconcile {
						consulEnabled = consulIns.Enabled && consulIns.Status != providers.InstanceStatusUnhealthy
						remoteCluster = servIns.Provider
					}
					// If env-type not equal
					if consulIns.EnvType != servIns.EnvType ||
						consulIns.EnvGroup != servIns.EnvGroup ||
						consulIns.Status != servIns.Status ||
						consulIns.State != servIns.State ||
						consulIns.Ip != servIns.Ip ||
						consulIns.Idc != servIns.Idc ||
						remoteCluster != consulClusterOf(c.nacosReconcile, consulIns) ||
						consulEnabled != servIns.Enabled ||
						consulIns.AppCode != servIns.AppCode ||
						consulIns.Cpu != servIns.Cpu {
						diff = true
					}
				}
				if diff {
					c.logger.Infof("the instance: %s of appcode: %s is newer, trigger a push.", consulIns.InstanceId, consulIns.AppCode)
					c.buildAndSendEvent(consulIns)
					bothExist = true
				}
				delete(currentProviderInstances, consulKey)
				delete(remoteInstances, providers.IdentityKey(servIns))
			} else {
				// For these instances in both Provider but not in the discovery center, the instance should be added to the discovery center.
				c.logger.Infof("consul match much id : %s, status: %d", consulIns.InstanceId, consulIns.Status)
				if consulIns.Status == 1 {
					ecsExist = true
					c.buildAndSendEvent(consulIns)
				}
				delete(currentProviderInstances, consulKey)
			}
		}
		// case 1 notice
		if bothExist {
			c.notifier.Notify("Instance data inconsistency", "Data inconsistency between the discovery center and the ecs cluster: the instances are the same in the discovery center and the ecs cluster, but some data fields of the instances differ")
		}
		//case 2 notice
		if ecsExist {
			c.notifier.Notify("Instance data inconsistency", "Data inconsistency between the discovery center and the ecs cluster: the instances differ between the discovery center and the ecs cluster, some instances exist in the ecs cluster but not in the discovery center")
		}
		// The instances remaining in the the discovery center variable (remoteInstances) are either old or not in the Provider instance list.
		// In this case, we should delete it from the discovery center.
		if len(remoteInstances) > 0 {
			c.logger.Infof("process discovery center instance deleting. instance size: %d", len(remoteInstances))
			for _, servIns := range remoteInstances {
				if c.nacosReconcile {
					// dsca-3 §3.3 / DS-3-5, nacos-reconcile mode: push
					// Status=3 (deregister). The Atlas-primary world's
					// Status=2 ("old instance, mark not-ready") heals a
					// remote-only ecs instance against nacos into a
					// permanent disabled zombie instead — an upsert with
					// enabled=false, hidden from consumers, present in the
					// catalog forever. Status 3 makes the nacos sink
					// deregister by composite id; the reconstructed instance
					// carries the host's own ip/port/cluster, so the
					// deregister is well-formed by construction.
					servIns.Status = providers.InstanceStatusOffline
					servIns.Enabled = false
					servIns.State = providers.InstanceStateTerminated
				} else {
					servIns.Status = providers.InstanceStatusUnhealthy
				}
				c.logger.Infof("the discovery center has the old instance, set its Status filed as %d, and trigger a push. instance: %v", servIns.Status, servIns)
				registryExist = true
				c.buildAndSendEvent(servIns)
			}
			//case 3 notice
			if registryExist {
				c.notifier.Notify("Instance data inconsistency", "Data inconsistency between the discovery center and the ecs cluster: the instances differ between the discovery center and the ecs cluster, some instances exist in the discovery center but not in the ecs cluster")
			}
		}
	}
}

// consulClusterOf resolves the local side of the Cluster comparison:
// the local instance's Cluster in Atlas-primary mode (the remote view
// round-trips the model verbatim there), and the local instance's
// Provider in nacos-reconcile mode — the symmetric replacement for the
// Cluster field the nacos reconstruction never carries (dsca-5 §4.2-3:
// clusterOf <-> clusterName round-trips Provider exactly; Cluster
// round-trips as "" against a local "" from both converters).
func consulClusterOf(nacosReconcile bool, ins *sv.Instance) string {
	if nacosReconcile {
		return ins.Provider
	}
	return ins.Cluster
}

func (c *consul) buildAndSendEvent(instance *sv.Instance) { // if instance status is 0 , don't send event
	if err := c.pool.Submit(func() {
		if instance.Status == 0 {
			return
		}
		ins := make([]*sv.Instance, 1)
		ins[0] = instance
		// Tick-time origin + ns unit (dsca-2 §3 Option (b), §6 origin
		// semantics): the reconcile push, not a watch event.
		triggerTime := time.Now().UnixNano()
		event := &worker.Event{
			Trigger: triggerTime,
			Data:    ins,
			Operate: worker.OperateTypeSync,
		}
		c.worker.Handle(event)
	}); err != nil {
		key := providers.IdentityKey(instance)
		c.requeueEvent(key, func() {
			if instance.Status == 0 {
				return
			}
			ins := make([]*sv.Instance, 1)
			ins[0] = instance
			c.worker.Handle(&worker.Event{
				Trigger: time.Now().UnixNano(),
				Data:    ins,
				Operate: worker.OperateTypeSync,
			})
		})
	}
}

// withExpiryDuration sets up the interval time of cleaning up goroutines.
func withExpiryDuration(expiryDuration time.Duration) ants.Option {
	return func(opts *ants.Options) {
		opts.ExpiryDuration = expiryDuration
	}
}
