package consul

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"sort"
	domaininstance "spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/pkg/beehive/service/v2"
	sv "spotter/pkg/beehive/service/v2"
	"spotter/pkg/metrics"
	"spotter/pkg/providers"
	"spotter/pkg/worker"
	"spotter/tools/unit"
	"strings"
	"sync"
	"time"
)

// K8S provider implement
type consul struct {
	providerName  string
	sourceCluster string
	// acceptLegacyRemote is enabled only for the single-source compatibility
	// constructor. Multiple-source construction disables legacy records whose
	// missing SourceCluster makes ownership ambiguous.
	acceptLegacyRemote bool
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
	// A full-push tick reads the source in CompareAndFlush and again in
	// emitSyncAll. This marker lets the second read consume the confirmation
	// already counted by the first read instead of counting it twice.
	emptyConfirmationPending bool
	emptyRetryInterval       time.Duration
	emptyRetryTimer          *time.Timer
	pool                     *ants.Pool // goroutine pool
	overflowMu               sync.Mutex
	overflow                 *providers.OverflowQueue
	runWG                    sync.WaitGroup
	done                     chan struct{}
	stopOnce                 sync.Once
	depsOnce                 sync.Once
	initDone                 bool
	logger                   ports.Logger
	notifier                 ports.Notifier
	metricsRecorder          ports.MetricsRecorder

	// sourceErr records the error of the last GetAll when the consul source
	// could not be read (nil after a successful read, empty or not): the
	// error/empty distinction GetAll's nil result cannot carry itself. Every
	// access — the writes inside GetAll and the reads in emitSyncAll and the
	// tests' sourceError helper — happens under the provider lock: a mutex
	// orders only accesses that BOTH take it, so the tick path (emitSyncAll)
	// must lock just like the handler paths (syncInstance, CompareAndFlush)
	// already do. See GetAll's comment for the caller contract.
	sourceErr error
	// snapshotState records the outcome of the most recent catalog read. A
	// nil GetAll result is intentionally not the only signal: a failed or
	// partially converted catalog must never be treated as a legitimate empty
	// desired state by a reconcile caller.
	snapshotState consulSnapshotState

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

// consulSnapshotState is the closed set of outcomes from one Consul catalog
// read. Healthy empty is a valid source state, while partial/unstable means
// that the read cannot safely replace the previous cache.
type consulSnapshotState uint8

const (
	consulSnapshotUnknown consulSnapshotState = iota
	consulSnapshotSourceError
	consulSnapshotHealthyEmpty
	consulSnapshotHealthyNonEmpty
	consulSnapshotPartial
)

const (
	consulMetricOutcomeHealthyEmpty    = "healthy_empty"
	consulMetricOutcomeHealthyNonEmpty = "healthy_nonempty"
	consulMetricOutcomeSourceError     = "source_error"
	consulMetricOutcomePartial         = "partial"
	consulMetricOutcomePending         = "pending"
	consulMetricOutcomeConfirmed       = "confirmed"
)

type consulSnapshot struct {
	instances []*sv.Instance
	state     consulSnapshotState
	err       error
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

// consulMetricsLocked returns the optional source-scoped recorder. Callers
// in the read/snapshot and empty-confirmation paths hold c.Lock; the
// recorder is deliberately optional so existing MetricsRecorder
// implementations remain source-compatible.
func (c *consul) consulMetricsLocked() ports.ConsulMetricsRecorder {
	if c == nil || c.metricsRecorder == nil {
		return nil
	}
	recorder, _ := c.metricsRecorder.(ports.ConsulMetricsRecorder)
	return recorder
}

// metricSourceScope returns only the logical source ID. A source ID is
// normalized by the constructor and never contains an address or credential;
// the fallback keeps hand-built test providers from producing an empty label.
func (c *consul) metricSourceScope() string {
	if c == nil {
		return "consul"
	}
	scope := domaininstance.SanitizeWireScope(c.sourceCluster)
	if scope == "" {
		return "consul"
	}
	return scope
}

func consulSnapshotMetricOutcome(state consulSnapshotState) string {
	switch state {
	case consulSnapshotHealthyEmpty:
		return consulMetricOutcomeHealthyEmpty
	case consulSnapshotHealthyNonEmpty:
		return consulMetricOutcomeHealthyNonEmpty
	case consulSnapshotSourceError:
		return consulMetricOutcomeSourceError
	case consulSnapshotPartial:
		return consulMetricOutcomePartial
	default:
		return "unknown"
	}
}

func (c *consul) recordHealthyEmptyConfirmationLocked() {
	recorder := c.consulMetricsLocked()
	if recorder == nil {
		return
	}
	outcome := consulMetricOutcomePending
	if c.emptyConfirmations >= 3 {
		outcome = consulMetricOutcomeConfirmed
	}
	recorder.IncConsulHealthyEmptyConfirmation(c.metricSourceScope(), outcome)
}

// NacosReconcileSwitch is the consul leg's wiring seam for the nacos
// reconcile mode (dsca-3 §3.3): internal/server.go asserts the constructed
// provider against it and calls SetNacosReconcileSource(true) exactly when
// the resolved config designates the nacos sink as the reconcile source
// (--reconcile-source nacos), before Run. The switch exists so the compare
// semantics can follow the read routing without changing the provider
// constructor's shared signature.
type NacosReconcileSwitch interface {
	SetNacosReconcileSource(enabled bool)
}

// MetricsReporter is the explicit metrics wiring seam. Production composition
// installs the injected recorder before Run.
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

// NewConsulProviderWithDeps constructs a provider from explicit runtime
// collaborators.
func NewConsulProviderWithDeps(ctx context.Context, worker worker.Worker, pushInterval int, addrs []string, logger ports.Logger, notifier ports.Notifier) (provider providers.Provider, err error) {
	return NewConsulProviderWithSourceID(ctx, worker, pushInterval, addrs, "", logger, notifier)
}

// NewConsulProviderWithSourceID constructs one logical Consul source. The
// address list remains an HA endpoint list; sourceID distinguishes a separate
// Consul catalog from another logical source using the same Spotter process.
func NewConsulProviderWithSourceID(ctx context.Context, worker worker.Worker, pushInterval int, addrs []string, sourceID string, logger ports.Logger, notifier ports.Notifier) (provider providers.Provider, err error) {
	return newConsulProvider(ctx, worker, pushInterval, ConsulSource{ID: sourceID, Addresses: addrs}, logger, notifier)
}

// NewConsulProviderWithSource constructs one logical Consul source with its
// source-scoped authentication, TLS and tenancy settings.
func NewConsulProviderWithSource(ctx context.Context, worker worker.Worker, pushInterval int, source ConsulSource, logger ports.Logger, notifier ports.Notifier) (provider providers.Provider, err error) {
	return newConsulProvider(ctx, worker, pushInterval, source, logger, notifier)
}

func newConsulProvider(ctx context.Context, worker worker.Worker, pushInterval int, source ConsulSource, logger ports.Logger, notifier ports.Notifier) (provider providers.Provider, err error) {
	addrs := source.Addresses
	sourceID := source.ID
	if ctx == nil || len(addrs) == 0 || worker == nil || pushInterval < 0 {
		err = errors.New("params invalid")
		return nil, err
	}
	sourceCluster, sourceErr := domaininstance.ConsulSourceID(sourceID, addrs)
	if sourceErr != nil {
		return nil, sourceErr
	}
	var cf ConsulClientFactory
	if cf, err = NewClientFactoryWithOptions(addrs, source.clientOptions(), logger); err != nil {
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
		providerName:       "consul",
		sourceCluster:      sourceCluster,
		acceptLegacyRemote: true,
		ctx:                ctx,
		monitor:            monitor,
		worker:             worker,
		clientFactory:      cf,
		// interval honors --push-interval for the ecs leg exactly like the
		// k8s provider does (the provider constructor assigns the same field): it
		// bounds the periodic CompareAndFlush + SyncAll cadence. The field
		// was declared but never assigned here, so the periodic path fell
		// back to the 21600s default and ignored the flag.
		interval:           pushInterval,
		emptyRetryInterval: 5 * time.Second,
		cache:              providers.NewCache(8),
		done:               make(chan struct{}),
		logger:             logger,
		notifier:           notifier,
	}
	// Create pool for sending instance events to the discovery center
	p, poolErr := ants.NewPool(providers.PoolBenchSize, withExpiryDuration(time.Second*providers.PoolExpireTime), ants.WithNonblocking(true))
	if poolErr != nil {
		return nil, errors.WithMessage(poolErr, "create worker pool")
	}
	consulProvider.pool = p
	// Init instance fiter
	consulProvider.filters = providers.InitInstanceFilters()

	// Watch the change events to refresh local caches
	// monitor.AppendServiceHandler(provider.ServiceChanged)
	if timestampedMonitor, ok := monitor.(interface {
		AppendTimestampedInstanceChangeHandler(TimestampedInstanceChangeHandler)
	}); ok {
		timestampedMonitor.AppendTimestampedInstanceChangeHandler(func(watchAt time.Time) error {
			err := consulProvider.syncInstance()
			consulProvider.recordWatchToSync(watchAt, err)
			return err
		})
	} else if changeMonitor, ok := monitor.(interface{ AppendInstanceChangeHandler(InstanceChangeHandler) }); ok {
		changeMonitor.AppendInstanceChangeHandler(func() error { return consulProvider.syncInstance() })
	} else {
		monitor.AppendInstanceHandler(consulProvider.InstanceChanged)
	}

	//return &controller, err
	return consulProvider, nil
}

// recordWatchToSync publishes a real blocking-watch-return to sync-completion
// duration. A zero origin is rejected so legacy/manual handlers cannot create
// synthetic samples; negative durations are clamped only for an injected
// clock moving backwards.
func (c *consul) recordWatchToSync(watchAt time.Time, syncErr error) {
	if watchAt.IsZero() {
		return
	}
	duration := time.Since(watchAt)
	if duration < 0 {
		duration = 0
	}
	outcome := "ok"
	if syncErr != nil {
		outcome = "error"
	}
	c.Lock()
	recorder := c.consulMetricsLocked()
	source := c.metricSourceScope()
	c.Unlock()
	if recorder != nil {
		recorder.ObserveConsulWatchToSyncDuration(source, outcome, duration)
	}
}

func consulSourceClusterID(addrs []string, configured string) string {
	if strings.TrimSpace(configured) != "" {
		return strings.TrimSpace(configured)
	}
	canonical := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if trimmed := strings.TrimSpace(addr); trimmed != "" {
			canonical = append(canonical, trimmed)
		}
	}
	sort.Strings(canonical)
	digest := sha256.Sum256([]byte(strings.Join(canonical, ",")))
	return "consul-" + hex.EncodeToString(digest[:6])
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
	if c.emptyRetryTimer != nil {
		c.emptyRetryTimer.Stop()
		c.emptyRetryTimer = nil
	}
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

// scheduleEmptyRetryLocked schedules at most one bounded retry for a healthy
// empty snapshot. The caller holds the provider lock; the callback releases
// that lock before performing the remote Consul read via syncInstance.
func (c *consul) scheduleEmptyRetryLocked() {
	if c.emptyRetryTimer != nil || c.stopped || c.emptyRetryInterval <= 0 {
		return
	}
	interval := c.emptyRetryInterval
	c.emptyRetryTimer = time.AfterFunc(interval, func() {
		c.Lock()
		c.emptyRetryTimer = nil
		stopped := c.stopped
		c.Unlock()
		if stopped {
			return
		}
		select {
		case <-c.done:
			return
		default:
		}
		_ = c.syncInstance()
	})
}

// syncInstance will sync consul endpoints to the discovery center.
// It will get all the services tagged as "microservice", and convert consul endpoint model to the discovery center Model.
// Then compare the new instances list with the instances we cached before so that we can generate the instances which are
// added, updated and deleted.
func (c *consul) syncInstance() (err error) {
	c.Lock()
	recovered := false
	if c.stopped {
		c.Unlock()
		return errors.New("consul provider stopped")
	}
	oldCache := c.cache
	newCache := providers.NewCache(8)
	// Get all services from consul
	currentInss := c.getAllLocked()
	if currentInss == nil {
		c.emptyConfirmations = 0
		c.emptyConfirmationPending = false
		if c.emptyRetryTimer != nil {
			c.emptyRetryTimer.Stop()
			c.emptyRetryTimer = nil
		}
		err = errors.New("get consul endpoints snapshot failed")
		c.logger.Warnf("%s", err.Error())
		c.Unlock()
		return err
	}
	if len(currentInss) == 0 {
		if c.snapshotState != consulSnapshotHealthyEmpty {
			err = errors.New("get empty consul endpoints without healthy empty snapshot")
			c.logger.Warnf("%s", err.Error())
			c.Unlock()
			return err
		}
		// A watch callback may be the first independent empty observation.
		// Mark it pending so the following full-push compare consumes the
		// observation instead of counting the same source state twice.
		c.emptyConfirmations++
		c.recordHealthyEmptyConfirmationLocked()
		c.emptyConfirmationPending = true
		err = fmt.Errorf("get empty consul endpoints; waiting for confirmation %d/3", c.emptyConfirmations)
		c.logger.Warnf("%s", err.Error())
		if c.emptyConfirmations < 3 {
			c.scheduleEmptyRetryLocked()
			c.Unlock()
			return err
		}
		// Three independent healthy-empty reads now authorize the empty
		// snapshot to replace the cache and emit deletions. The pending marker
		// belongs to this completed confirmation window and must not leak into
		// the next full-push tick.
		c.emptyConfirmationPending = false
		if c.emptyRetryTimer != nil {
			c.emptyRetryTimer.Stop()
			c.emptyRetryTimer = nil
		}
	} else {
		recovered = c.emptyConfirmations > 0
		c.emptyConfirmations = 0
		c.emptyConfirmationPending = false
		if c.emptyRetryTimer != nil {
			c.emptyRetryTimer.Stop()
			c.emptyRetryTimer = nil
		}
		currentInss = c.mergeMonotonicInstances(oldCache, currentInss)
	}
	for _, ins := range currentInss {
		newCache.ReplaceOrInsert(ins)
	}
	// Compare to generate events
	addEvents, updateEvents, deleteEvents := c.extractDiff(oldCache, newCache)
	// Update cache
	c.cache = newCache
	c.generation++
	c.Unlock()
	// Dispatch after releasing c.Lock. Worker sinks may synchronously acquire
	// their full-operation gate and invoke Revalidate, which reads this
	// provider snapshot and therefore needs c.Lock; dispatching under the lock
	// would invert that order during an incremental/full-push race.
	c.EventsSync(addEvents, updateEvents, deleteEvents)
	if recovered {
		// An incremental recovery with the same revision can be rejected by
		// the sink's delete tombstone. Emit a trusted complete snapshot after
		// releasing the provider lock so the sink can validate and heal it.
		c.emitSyncAll()
	}
	return err
}

// mergeMonotonicInstances prevents a temporarily stale Consul read from
// downgrading the provider cache. Omitted identities still remain omitted so
// deletion semantics are preserved; only an identity present in both views
// keeps its newer cached projection.
func (c *consul) mergeMonotonicInstances(old providers.CacheIterface, current []*sv.Instance) []*sv.Instance {
	if old == nil || len(current) == 0 {
		return current
	}
	merged := make([]*sv.Instance, 0, len(current))
	for _, ins := range current {
		if ins == nil {
			continue
		}
		if previous := old.Get(providers.IdentityKey(ins)); previous != nil && ins.Reversion < previous.Reversion {
			merged = append(merged, previous)
			continue
		}
		merged = append(merged, ins)
	}
	return merged
}

func (c *consul) toInstance(endpoints []*api.ServiceEntry) (inss []*sv.Instance) {
	inss, _ = c.toInstanceWithSkipped(endpoints)
	return inss
}

// toInstanceWithSkipped keeps conversion failures visible to the snapshot
// reader. Silently dropping one malformed endpoint would otherwise turn a
// partial catalog into a smaller, apparently authoritative full state.
func (c *consul) toInstanceWithSkipped(endpoints []*api.ServiceEntry) (inss []*sv.Instance, skipped int) {
	// get pod info from k8s robot
	inss = []*sv.Instance{}
	if len(endpoints) > 0 {
		for _, ep := range endpoints {
			if ins, err := convertInstanceForSource(ep, c.sourceCluster); err != nil {
				c.logger.Errorf("%s", err.Error())
				skipped++
				continue
			} else {
				inss = append(inss, ins)
			}
		}
	}
	if skipped > 0 {
		if recorder := c.consulMetricsLocked(); recorder != nil {
			recorder.IncConsulConversionSkips(c.metricSourceScope(), skipped)
		}
	}

	return inss, skipped
}

// GetAll returns the full Consul instance list under the provider lock. A nil result is ambiguous
// between "source errored" (monitor GetServices/GetServiceEntries failure)
// and "source legitimately empty": the sourceErr flag below distinguishes
// the two.
func (c *consul) GetAll() []*v2.Instance {
	c.Lock()
	defer c.Unlock()
	return c.getAllLocked()
}

// getAllLocked performs the source read and updates snapshot state. Callers
// must hold c.Lock; the public GetAll wrapper provides the safe external path.
func (c *consul) getAllLocked() (result []*v2.Instance) {
	snapshot := c.readSnapshot()
	c.snapshotState = snapshot.state
	c.sourceErr = snapshot.err
	if snapshot.err != nil {
		c.emptyConfirmations = 0
		c.emptyConfirmationPending = false
		if c.emptyRetryTimer != nil {
			c.emptyRetryTimer.Stop()
			c.emptyRetryTimer = nil
		}
		if recorder := c.consulMetricsLocked(); recorder != nil {
			recorder.IncConsulSourceError(c.metricSourceScope(), consulSnapshotMetricOutcome(snapshot.state))
		}
		c.logger.Errorf("consul catalog snapshot rejected: %s", snapshot.err.Error())
		return nil
	}
	c.logger.Infof("consul getall size: %d", len(snapshot.instances))
	return snapshot.instances
}

// readSnapshot reads a complete catalog while retaining enough provenance to
// distinguish a valid empty catalog from an unsafe partial result. It assumes
// the caller holds the provider lock, just like GetAll did historically.
func (c *consul) readSnapshot() (snapshot consulSnapshot) {
	started := time.Now()
	defer func() {
		if recorder := c.consulMetricsLocked(); recorder != nil {
			recorder.ObserveConsulCatalogReadDuration(c.metricSourceScope(), consulSnapshotMetricOutcome(snapshot.state), time.Since(started))
		}
	}()
	services, err := c.monitor.GetServices()
	if err != nil {
		return consulSnapshot{state: consulSnapshotSourceError, err: errors.WithMessage(err, "get services from consul")}
	}
	result := make([]*sv.Instance, 0)
	serviceNames := make([]string, 0, len(services))
	for serviceName := range services {
		serviceNames = append(serviceNames, serviceName)
	}
	sort.Strings(serviceNames)
	for _, serviceName := range serviceNames {
		endpoints, readErr := c.monitor.GetServiceEntries(serviceName, nil)
		if readErr != nil {
			return consulSnapshot{state: consulSnapshotPartial, err: errors.WithMessagef(readErr, "get service endpoints from consul: %s", serviceName)}
		}
		instances, skipped := c.toInstanceWithSkipped(endpoints)
		if skipped > 0 {
			return consulSnapshot{state: consulSnapshotPartial, err: fmt.Errorf("convert consul service %s: skipped %d malformed endpoint(s)", serviceName, skipped)}
		}
		result = append(result, instances...)
	}
	state := consulSnapshotHealthyNonEmpty
	if len(result) == 0 {
		state = consulSnapshotHealthyEmpty
	}
	return consulSnapshot{instances: result, state: state}
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
				// Reversion remains monotonic: lower revisions never win. For
				// equal revisions, compare the complete Spotter-owned canonical
				// payload so field drift is repaired as well.
				if newIns.Reversion > oldIns.Reversion ||
					(newIns.Reversion == oldIns.Reversion && domaininstance.CanonicalPayload(newIns) != domaininstance.CanonicalPayload(oldIns)) {
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
	scope := c.sourceCluster
	if ins != nil && ins.Reversion > 0 {
		sequence = uint64(ins.Reversion)
	}
	if ins != nil && ins.SourceCluster != "" {
		scope = ins.SourceCluster
	} else if ins != nil && ins.Provider != "" && scope == "" {
		scope = ins.Provider
	}
	if scope == "" {
		scope = "ecs"
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
	if interval <= 0 {
		c.logger.Errorf("consul: refusing invalid full-push interval %s", interval)
		return
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
// The snapshot read and source-state decision run under the provider lock;
// worker.Handle runs after that lock is released. This lock order is required
// because ordered sinks hold their full-operation gate while Revalidate reads
// this provider snapshot.
func (c *consul) emitSyncAll() {
	all, generation, valid, emptyConfirmed := c.snapshotForFullPush()
	if !valid {
		return
	}
	scope := c.sourceCluster
	if scope == "" {
		scope = providers.ProviderEcs
	}
	// Tick-time origin + ns unit (dsca-2 §3 Option (b), §6 origin semantics).
	c.worker.Handle(&worker.Event{
		Trigger:        time.Now().UnixNano(),
		Data:           all,
		Operate:        worker.OperateTypeSyncAll,
		Scope:          scope,
		BatchID:        worker.FullBatchID(scope, all),
		Sequence:       generation,
		EmptyConfirmed: emptyConfirmed,
		Revalidate: func() ([]*v2.Instance, bool) {
			latest, _, ok, _ := c.snapshotForFullPushReadOnly()
			if !ok {
				return nil, false
			}
			return latest, true
		},
	})
}

func (c *consul) snapshotForFullPush() ([]*v2.Instance, uint64, bool, bool) {
	return c.snapshotForFullPushMode(true)
}

func (c *consul) snapshotForFullPushReadOnly() ([]*v2.Instance, uint64, bool, bool) {
	return c.snapshotForFullPushMode(false)
}

func (c *consul) snapshotForFullPushMode(advanceEmptyConfirmation bool) ([]*v2.Instance, uint64, bool, bool) {
	c.Lock()
	defer c.Unlock()
	all := c.cache.List()
	if c.monitor != nil {
		cached := c.cache
		all = c.getAllLocked()
		if all != nil {
			all = c.mergeMonotonicInstances(cached, all)
		}
	}
	if c.sourceErr != nil {
		return nil, c.generation, false, false
	}
	if all == nil {
		all = []*v2.Instance{}
	}
	if len(all) == 0 && advanceEmptyConfirmation {
		if c.emptyConfirmationPending {
			c.emptyConfirmationPending = false
		} else {
			c.emptyConfirmations++
			c.recordHealthyEmptyConfirmationLocked()
		}
	} else if len(all) > 0 {
		c.emptyConfirmations = 0
		c.emptyConfirmationPending = false
	}
	return all, c.generation, true, c.emptyConfirmations >= 3
}

// CompareAndFlush compare and find diff instances then flush
func (c *consul) CompareAndFlush() {
	c.Lock()
	c.logger.Infof("%s: trying to compare and find diff instances then flush", c.providerName)
	all := c.getAllLocked()
	if all == nil {
		c.Unlock()
		return
	}
	if len(all) == 0 {
		if c.snapshotState != consulSnapshotHealthyEmpty {
			c.Unlock()
			return
		}
		if !c.emptyConfirmationPending {
			c.emptyConfirmations++
			c.recordHealthyEmptyConfirmationLocked()
			c.emptyConfirmationPending = true
		}
		if c.emptyConfirmations < 3 {
			c.scheduleEmptyRetryLocked()
			c.logger.Warnf("%s: healthy empty catalog confirmation %d/3; retaining cache and remote state", c.providerName, c.emptyConfirmations)
			c.Unlock()
			return
		}
	} else {
		all = c.mergeMonotonicInstances(c.cache, all)
		c.emptyConfirmations = 0
		c.emptyConfirmationPending = false
	}
	{
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
		c.Unlock()
		// Compare diffs and sync incrementally. Atlas historically returned only
		// online ECS entries; Nacos must also return unhealthy entries so the
		// canonical comparator can prove their steady state and heal field or
		// Reversion drift instead of treating an all-unhealthy service as empty.
		statuses := []int32{providers.InstanceStatusOnline}
		if c.nacosReconcile {
			statuses = append(statuses, providers.InstanceStatusUnhealthy)
		}
		registryList, err := c.worker.GetAll(statuses, providers.ProviderEcs)
		if err != nil {
			err = errors.WithMessage(err, "get all instances from the discovery center")
			c.logger.Errorf("%s", err.Error())
			return
		}
		registryList = c.sourceScopedRemote(registryList)
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
				if c.nacosReconcile {
					// Nacos is authoritative only as a read-back view; the
					// Consul provider remains the source of truth. Compare the
					// complete canonical domain projection so labels, images,
					// every port, source identity/resource fields and Reversion
					// drift in either direction are healed. The shared predicate
					// deliberately excludes Nacos-owned Healthy and handles the
					// SDK's transport-enabled unhealthy wire shape through the
					// reconstructed canonical Enabled value.
					diff = domaininstance.DiffNacosReconcile(consulIns, servIns)
				}
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
				if c.nacosReconcile && diff && consulIns.Reversion != servIns.Reversion {
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
				} else if !c.nacosReconcile && consulIns.Reversion > servIns.Reversion {
					diff = true
				} else if !c.nacosReconcile && consulIns.Reversion == servIns.Reversion {
					// Atlas is authoritative for the full Spotter-owned model.
					// Compare its complete canonical fingerprint at equal
					// Reversion so labels, ports, images, hostname, and source
					// identity drift are repaired; Nacos-owned wire fields are
					// outside this projection.
					diff = domaininstance.CanonicalPayload(consulIns) != domaininstance.CanonicalPayload(servIns)
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

// sourceScopedRemote constrains reconciliation authority to this catalog.
// Source-aware records from another catalog are never candidates for updates
// or deletion. Legacy records are admitted only by the single-source adapter.
func (c *consul) sourceScopedRemote(list *sv.InstanceList) *sv.InstanceList {
	if list == nil || c.sourceCluster == "" {
		return list
	}
	filtered := make([]*sv.Instance, 0, len(list.GetInstance()))
	for _, item := range list.GetInstance() {
		if item == nil {
			continue
		}
		source := item.SourceCluster
		if source == "" && item.Label != nil {
			source = item.Label["sourceCluster"]
		}
		if source == c.sourceCluster || (source == "" && c.acceptLegacyRemote) {
			filtered = append(filtered, item)
		}
	}
	return &sv.InstanceList{Instance: filtered}
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
	if instance == nil {
		return
	}
	scope := c.sourceCluster
	if scope == "" {
		scope = instance.SourceCluster
	}
	if scope == "" {
		scope = instance.Provider
	}
	sequence := uint64(0)
	if instance.Reversion > 0 {
		sequence = uint64(instance.Reversion)
	}
	identity := providers.IdentityKey(instance)
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
			Trigger:  triggerTime,
			Data:     ins,
			Operate:  worker.OperateTypeSync,
			Scope:    scope,
			Identity: identity,
			Revision: instance.Reversion,
			Sequence: sequence,
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
				Trigger:  time.Now().UnixNano(),
				Data:     ins,
				Operate:  worker.OperateTypeSync,
				Scope:    scope,
				Identity: identity,
				Revision: instance.Reversion,
				Sequence: sequence,
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
