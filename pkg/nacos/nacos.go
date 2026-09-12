package nacos

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

// SinkName is the fanout sink name of the Nacos adapter: the value the
// server wiring registers it under, the retry queue keys its per-sink
// entries by, and the sync_error_gauge label uses. Exported from pkg/nacos
// (not pkg/worker) because the adapter owns its own name; the wiring site
// is its only consumer.
const SinkName = "nacos"

// DefaultPushConcurrency is the bounded parallelism of the sink's
// per-instance pushes (dsca-2 DS-2-1 fix design A: "an errgroup with a
// semaphore of 16-32 ... start 8-16, tune by the §3 metric"; 8 is the
// contract's stated starting point — well below the real server's measured
// parallel-load knee, which the demo's own 50-way probes showed degrading
// hard and unstably). Nacos v1/v2 have NO batch register endpoint (verified
// against the 2.1.0 OpenAPI: the only "batch" endpoints are metadata-only
// Beta), so the parallelism is a worker-group over single-instance calls.
//
// The knob is a package-level default, NOT a CLI flag (the contract adds no
// flag): production tunes it by editing this constant against the
// event_to_store_e2e_duration_seconds p99 on the target deployment
// (dsca-2 §4's drift caveat — the knee moves with load, so the default is
// deliberately conservative); tests override it with SetPushConcurrency.
//
// Semantics under parallelism: upserts and deregisters are idempotent, and
// ordering matters only PER INSTANCE (distinct composite ids are
// order-independent) — every worker's calls are for instances disjoint
// from every other worker's, so order-of-completion nondeterminism is
// acceptable and documented. Errors follow the contract's first-error-wins
// shape: every instance is still attempted (no short-circuit — a partial
// failure must stay expressible through the retry-friendly FanoutError),
// and the FIRST error in INSTANCE ORDER is returned (deterministic despite
// nondeterministic completion).
var DefaultPushConcurrency = 8

// SetPushConcurrency overrides the bounded parallelism of the sink's
// per-instance pushes. It exists for tests (the wall-clock bound tests
// cannot exercise the default 8 meaningfully against 10ms mocks); call it
// before the pushes under test and restore the default afterwards.
// Production must not call it mid-flight: the value is read per Push, so a
// concurrent change applies to subsequent pushes only — harmless, but the
// intended tuning path is DefaultPushConcurrency at build time.
func SetPushConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	pushConcurrency.Store(int32(n))
}

// pushConcurrency is the atomic holder of the current bound (initialized to
// DefaultPushConcurrency's value at package init; the variable stays the
// single source of truth for tests that swap it).
var pushConcurrency atomic.Int32

func init() { pushConcurrency.Store(int32(DefaultPushConcurrency)) }

// currentPushConcurrency reads the effective bound (min 1).
func currentPushConcurrency() int {
	if n := pushConcurrency.Load(); n > 0 {
		return int(n)
	}
	return 1
}

// pushInstances pushes instances through pushOne with bounded parallelism:
// a worker-group of `workers` goroutines over a closed over index channel,
// first-error-in-instance-order, every instance attempted. The group is
// sized by currentPushConcurrency() at call time.
func (s *Sink) pushInstances(instances []*instance.Instance) error {
	workers := currentPushConcurrency()
	if workers > len(instances) {
		workers = len(instances)
	}
	if workers <= 0 {
		return nil
	}
	indexes := make(chan int, len(instances))
	for i := range instances {
		indexes <- i
	}
	close(indexes)

	errs := make([]error, len(instances)) // indexed by instance position: deterministic
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range indexes {
				if instances[i] == nil {
					continue // a nil slot is a skip, not an error (Push's contract)
				}
				errs[i] = s.pushOne(instances[i])
			}
		}()
	}
	wg.Wait()

	var firstErr error
	for _, err := range errs {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Sink is the Nacos ports.InstanceSink: it maps domain instances onto the
// Nacos v1 OpenAPI (plan §7.3) and owns the PushAll prune reconcile
// (plan §7.4). It also configures every (service, cluster) pair it
// registers with the NONE health checker (UpdateCluster), so Nacos's own
// server-side health check never runs on spotter-managed data — spotter is
// the health authority.
//
// Instances are registered PERSISTENT (ephemeral=false): spotter is a
// replicator asserting the desired state of other people's instances, not a
// service instance — an ephemeral registration would mass-expire live
// instances on a spotter hiccup, the opposite of the sink's contract. The
// cost (drift persists if spotter dies) is bounded by the retry queue and
// the full-push prune.
type Sink struct {
	client *Client
	logger ports.Logger

	// remembered (AUDIT-B-4) records every (service, cluster) pair the sink
	// has ever pushed non-offline instances for — the pairs whose remote
	// registrations this sink owns. The prune sweeps the pushed pairs UNION
	// the remembered pairs of the CLUSTERS PRESENT IN THE PUSH: a pair of a
	// present cluster whose desired set becomes empty is the "every instance
	// of this service vanished upstream" reconcile signal, and without the
	// memory the prune would have no key for it (the vanished service drops
	// out of the desired map entirely), leaving a decommissioned app's
	// remote registrations as permanent drift.
	//
	// The sweep is scoped to the pushed clusters because the cluster name IS
	// the provider tag (clusterOf returns ins.Provider) while pushes arrive
	// PER-PROVIDER (each provider's emitSyncAll pushes its own full list
	// through one shared sink): a push from provider X may only prune pairs
	// provider X owns. The pre-scoping shape unioned every remembered pair
	// into every push, so a k8s-only full push gave the ecs pair
	// (demo-pay-service, ecs) an empty desired set and DELETEd every remote
	// instance of it — the live delete/re-register flapping incident (the
	// collision is at the PAIR level, which the host loop's ClusterName
	// guard cannot see: an ecs pair's hosts ARE ecs-cluster hosts).
	//
	// An EMPTY pushed list carries no cluster and therefore no provider
	// identity (worker.Event has no provider field), so it sweeps NOTHING
	// remembered — the conservative no-op. The batch-3 semantics (an empty
	// push wipes every remembered pair) re-opened the cross-provider bug
	// through the empty door: one provider's empty SyncAll — a k8s informer
	// glitch, a consul source blip — deleted every OTHER provider's
	// instances. The vanished-service heal instead rides the same
	// provider's NON-empty full push: its remaining instances keep the
	// provider's cluster present in the push, so its vanished services are
	// swept (and the providers' offline markers — k8s CompareAndFlush
	// case-3 — delete vanished instances per-instance through Push). The
	// residual is deliberate: a provider whose ENTIRE source list is empty
	// no longer auto-heals its remote registrations — a total-empty source
	// (a cluster with zero pods, a consul catalog with zero services) is
	// indistinguishable from a broken source view, and its blast radius
	// must not be every spotter-managed registration; the drift heals the
	// first time any instance of the pair re-registers, or stays until a
	// manual clean (documented residual, plan §7.4 language).
	//
	// Survivability is one push interval, not a restart: the memory rebuilds
	// from the next register of each pair (a live service always re-pushes),
	// so the only window where a restart loses the sweep target is a service
	// that vanishes exactly while spotter is down — its drift then heals the
	// first time any instance of the pair re-registers, or stays until a
	// manual clean (documented residual, plan §7.4 language).
	remembered map[clusterKeyOf]bool
	// healthCheckDone records every (service, cluster) pair whose cluster
	// configuration the sink has successfully applied — the UpdateCluster
	// PUT that switches Nacos's own server-side health check off (see
	// ensureClusterHealthCheckDisabled). Applied once per pair per process:
	// re-applying after a restart (fresh process, empty map) is safe because
	// the PUT is idempotent, the same survivability contract as remembered.
	healthCheckDone map[clusterKeyOf]bool
	// healthCheckClaims records pairs whose UpdateCluster PUT is currently
	// IN FLIGHT — the check-and-set-before-HTTP claim that keeps the
	// bounded-parallelism register group from duplicating a pair's PUT (see
	// ensureClusterHealthCheckDisabled). Guarded by rememberedMu.
	healthCheckClaims map[clusterKeyOf]bool
	// rememberedMu guards remembered, healthCheckDone and healthCheckClaims.
	rememberedMu sync.Mutex
}

// clusterKeyOf is the prune's (service, cluster) pair identity. It is the
// exported-shaped twin of the local type prune() builds, hoisted so the
// remembered map can share it.
type clusterKeyOf struct {
	service string
	cluster string
}

// Sink satisfies the internal/ports.InstanceSink port exactly (the
// compile-time check fails on any future signature drift).
var _ ports.InstanceSink = (*Sink)(nil)

// NewSink creates a Nacos sink bound to addr. A nil logger is defaulted.
func NewSink(addr string, logger ports.Logger) (*Sink, error) {
	client, err := NewClient(addr, logger)
	if err != nil {
		return nil, err
	}
	return &Sink{
		client:            client,
		logger:            client.logger,
		remembered:        map[clusterKeyOf]bool{},
		healthCheckDone:   map[clusterKeyOf]bool{},
		healthCheckClaims: map[clusterKeyOf]bool{},
	}, nil
}

// Push applies the per-instance policy of plan §7.3:
//
//   - online (1)   → upsert register, enabled per Enabled
//   - unhealthy (2)→ upsert register, enabled=false (the consul "old
//     instance, mark not-ready" semantics — Status 2 is pushed, not deleted)
//   - offline (3)  → deregister, DELETE by composite id
//   - unknown (0)  → skipped defensively (upstream filters already reject it)
//
// The per-instance calls run with BOUNDED PARALLELISM (dsca-2 DS-2-1, fix
// design A: a worker-group of DefaultPushConcurrency single-instance calls,
// since the v1 API has no batch endpoint): every instance is attempted, and
// the first error in INSTANCE order is returned (deterministic despite
// nondeterministic completion). Completion order across DISTINCT instances
// is nondeterministic and harmless: the composite ids are distinct, the
// calls are idempotent upserts/deletes, and per-instance ordering never
// crosses worker boundaries (each instance is pushed exactly once by
// exactly one worker).
func (s *Sink) Push(triggerTime int64, instances []*instance.Instance) error {
	return s.pushInstances(instances)
}

// PushAll upserts every pushed instance and then prunes: for every
// (serviceName, clusterName) pair present in the pushed set it lists
// Nacos's current instances of that service and DELETEs every remote
// instance of that cluster not in the pushed set. The prune is scoped to
// clusters present in the data — both the desired pairs and the remembered
// sweep — so a k8s full push never touches ecs-cluster instances:
// per-provider ownership is preserved end-to-end (plan §7.4).
//
// The vanished-service semantics (AUDIT-B-4): a remembered pair of a
// cluster PRESENT in this push, whose desired set is empty, is the "every
// instance of this service vanished upstream" signal and has every remote
// instance pruned — the pushing provider still owns that pair, its other
// instances keep the cluster in the push, and the vanished service's
// remote registrations heal within one push interval. An EMPTY pushed list
// is a conservative no-op: it carries no (service, cluster) key and no
// provider identity, so the prune sweeps nothing remembered (see the
// remembered field for why the empty door must not wipe).
func (s *Sink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	if err := s.Push(triggerTime, instances); err != nil {
		return err
	}
	return s.prune(instances)
}

// prune removes the remote instances of the pushed (service, cluster) pairs
// that the pushed set no longer contains. Offline (Status 3) pushed
// instances are treated as absent — their pair still gets an (empty) desired
// set, so their remote counterparts are pruned even if the per-instance
// deregister failed to run. Remembered pairs of the CLUSTERS PRESENT IN
// THIS PUSH are swept with their (possibly empty) desired set — the
// vanished-service reconcile of AUDIT-B-4, correctly scoped: the cluster
// name is the provider tag, so a k8s push sweeps k8s pairs only, never the
// ecs pairs the consul provider owns. An empty push carries no cluster, so
// it sweeps nothing remembered.
//
// The listing is the CATALOG view (ListCatalogInstances), not the instance
// list: the instance list hides enabled=false entries — the exact state
// spotter's own unhealthy pushes write (register forces enabled=false, plan
// §7.3) — so a list-based prune would never remove an instance that once
// went unhealthy (the F8 drift). The catalog sees enabled=false instances
// too, so the prune reconciles them.
func (s *Sink) prune(instances []*instance.Instance) error {
	type clusterKey struct {
		service string
		cluster string
	}

	desired := map[clusterKey]map[string]bool{}
	pushedClusters := map[string]bool{}
	for _, ins := range instances {
		if ins == nil {
			continue
		}
		key := clusterKey{service: ins.AppCode, cluster: clusterOf(ins)}
		if desired[key] == nil {
			desired[key] = map[string]bool{}
		}
		pushedClusters[key.cluster] = true
		if ins.Status != instance.InstanceStatusOffline {
			desired[key][compositeID(ins)] = true
		}
	}

	// Remember the pairs this push asserts ownership of, and union them with
	// the remembered pairs of the pushed clusters: a pair of a pushed
	// cluster that drops out of the pushed set entirely still gets swept
	// (desired stays absent for it), which is the vanished-service signal —
	// while a remembered pair of a cluster NOT in this push belongs to a
	// different provider and must never be swept here (the live incident:
	// a k8s-only push deleting the ecs pair the consul provider owns).
	s.rememberedMu.Lock()
	for key := range desired {
		s.remembered[clusterKeyOf(key)] = true
	}
	union := make(map[clusterKey]map[string]bool, len(desired)+len(s.remembered))
	for key, wanted := range desired {
		union[key] = wanted
	}
	for rememberedKey := range s.remembered {
		if !pushedClusters[rememberedKey.cluster] {
			continue // another provider's pair: this push may not sweep it
		}
		key := clusterKey(rememberedKey)
		if _, ok := union[key]; !ok {
			union[key] = map[string]bool{} // remembered pair, empty desired: prune everything remote
		}
	}
	s.rememberedMu.Unlock()

	// The sweep's deregisters run with the same bounded parallelism as the
	// registers (DS-2-1: "the same bounded group applies to the
	// PushAll/Push path AND the deregister path"): distinct remote hosts
	// have distinct composite ids, so the DELETEs are order-independent and
	// idempotent, and first-error-in-remote-order keeps the surfaced error
	// deterministic.
	var firstErr error
	type pruneTask struct {
		key  clusterKey
		host Host
	}
	var tasks []pruneTask
	for key, wanted := range union {
		hosts, err := s.client.ListCatalogInstances(key.service, key.cluster)
		if err != nil {
			// A real Nacos answers HTTP 500 with a "cluster ... is not
			// found" / "service ... is not found" body when the (service,
			// cluster) pair has no catalog entry at all — the steady state
			// of a fully pruned service. Treat it as an empty list instead
			// of an error, or every interval would log a spurious failure
			// once the prune has done its job. The 500 stays retriable in
			// every other case (APIError classification is unchanged).
			if isCatalogNotFound(err) {
				s.logger.Infof("nacos: prune catalog list %s/%s: no catalog entry (treated as empty)", key.service, key.cluster)
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("nacos: prune list %s/%s: %w", key.service, key.cluster, err)
			}
			continue
		}
		for _, host := range hosts {
			if host.ClusterName != key.cluster {
				continue // another provider's cluster: never pruned here
			}
			if wanted[host.InstanceID] {
				continue
			}
			tasks = append(tasks, pruneTask{key: key, host: host})
		}
	}
	if len(tasks) > 0 {
		workers := currentPushConcurrency()
		if workers > len(tasks) {
			workers = len(tasks)
		}
		errs := make([]error, len(tasks))
		indexes := make(chan int, len(tasks))
		for i := range tasks {
			indexes <- i
		}
		close(indexes)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range indexes {
					task := tasks[i]
					errs[i] = s.deregister(task.key.service, task.host.IP, task.host.Port, task.key.cluster)
				}
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// isCatalogNotFound reports whether err is the real-server 500 answer whose
// body marks the addressed (service, cluster) as absent from the catalog
// ("... is not found"). It matches on the *APIError type and the body text,
// never on the wrapped message alone.
func isCatalogNotFound(err error) bool {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 500 {
		return false
	}
	return strings.Contains(apiErr.Body, "is not found")
}

// GetAll reconstructs domain instances from Nacos's CATALOG view (dsca-3
// §3.2): list all services of the group (paginated), read each service's
// hosts of the requested provider's CLUSTER (clusterName == provider — the
// per-provider/cluster scoping; a real Nacos requires clusterName on the
// catalog endpoint and answers HTTP 500 "… is not found" for an absent
// (service, cluster) pair, tolerated as an empty list exactly like the
// prune's walk), and rebuild the instances from the composite fields plus
// the metadata map. Status filtering uses the status metadata with an
// enabled→1/else→2 fallback (plan §7.3).
//
// Why the catalog and not instance/list: the instance list HIDES
// enabled=false hosts (client.go's ListInstances, the F8 blind spot) — the
// exact state spotter's own unhealthy pushes write and the exact state a
// console-disable heal needs to see. A diff built on the hiding view would
// misread every pushed-unhealthy instance as "absent" (a spurious ADD push
// every cycle) and could never heal a console-disabled one. The prune
// already walks this same catalog view; the diff shares the discipline.
//
// Fidelity limits, documented as acceptable (dsca-3 §3.2): ports beyond the
// first, labels and images are not recoverable, and every field the k8s
// diff compares survives the round trip; the compared sets deliberately
// stay inside that round-trippable intersection.
func (s *Sink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	services, err := s.client.ListServices(100)
	if err != nil {
		return nil, err
	}
	instances := []*instance.Instance{}
	for _, service := range services {
		// clusterName == provider: the per-provider/cluster scoping. A pair
		// with no catalog entry is the real server's steady state and is
		// skipped (empty), exactly like the prune (nacos.go's prune walk);
		// any other error aborts the view — a partial diff input must never
		// be mistaken for a complete one.
		hosts, err := s.client.ListCatalogInstances(service, provider)
		if err != nil {
			if isCatalogNotFound(err) {
				continue
			}
			return nil, err
		}
		for _, host := range hosts {
			ins := reconstruct(service, host)
			if !statusAllowed(statuses, ins.Status) {
				continue
			}
			instances = append(instances, ins)
		}
	}
	return &instance.InstanceList{Instance: instances}, nil
}

// Close releases the sink's resources. The HTTP client is stateless, so
// this is a no-op kept for the construction/cleanup symmetry of the server
// wiring (plan §6.5); it is safe to call repeatedly.
func (s *Sink) Close() error {
	return nil
}

// pushOne applies the per-instance policy for one instance.
func (s *Sink) pushOne(ins *instance.Instance) error {
	switch ins.Status {
	case instance.InstanceStatusOffline:
		// An offline instance without an ip cannot be deregistered: the v1
		// DELETE derives its composite id from the ip parameter, and Nacos
		// answers 400 "Param 'ip' is required" forever (nothing was ever
		// registered under an empty ip). The PushAll prune owns the remote
		// cleanup for real now: its catalog listing (ListCatalogInstances)
		// sees enabled=false instances too, so even drift the instance list
		// hides is reconciled — skip the deregister instead of poisoning the
		// retry queue.
		if ins.Ip == "" {
			s.logger.Warnf("nacos: skipping deregister of instance %s with empty ip, the PushAll prune owns the remote cleanup", ins.InstanceId)
			return nil
		}
		return s.deregister(ins.AppCode, ins.Ip, firstPort(ins), clusterOf(ins))
	case instance.InstanceStatusOnline, instance.InstanceStatusUnhealthy:
		// An online/unhealthy instance without an ip cannot be registered: the
		// v1 POST derives its composite id from the ip parameter, and Nacos
		// answers 400 "Param 'ip' is required" forever (nothing was ever
		// registered under an empty ip, so there is nothing to keep in sync).
		// Skip the register instead of poisoning the retry queue with a
		// permanently unfixable request — the source-side guards own keeping
		// empty-ip shells out of the stream.
		if ins.Ip == "" {
			s.logger.Warnf("nacos: skipping register of instance %s with empty ip, nothing was registered under an empty ip", ins.InstanceId)
			return nil
		}
		return s.register(ins)
	default:
		// Status 0 (unknown): upstream filters reject it (rules.go:99-101);
		// skip defensively rather than assert unknown state remotely.
		s.logger.Warnf("nacos: skipping instance %s with unknown status %d", ins.InstanceId, ins.Status)
		return nil
	}
}

// register upserts one instance: the enabled flag follows the status
// policy — Enabled for online, forced false for unhealthy.
func (s *Sink) register(ins *instance.Instance) error {
	enabled := ins.Enabled
	if ins.Status == instance.InstanceStatusUnhealthy {
		enabled = false
	}
	err := s.client.RegisterInstance(InstanceParams{
		ServiceName: ins.AppCode,
		IP:          ins.Ip,
		Port:        firstPort(ins),
		ClusterName: clusterOf(ins),
		Enabled:     enabled,
		Ephemeral:   false, // persistent: the sink owns the lifecycle (§7.1)
		Metadata:    metadataOf(ins),
	})
	if err != nil {
		return fmt.Errorf("nacos: register %s: %w", ins.InstanceId, err)
	}
	s.logger.Infof("nacos: registered instance %s as %s", ins.InstanceId, compositeID(ins))
	s.ensureClusterHealthCheckDisabled(ins.AppCode, clusterOf(ins))
	return nil
}

// ensureClusterHealthCheckDisabled applies the cluster configuration that
// turns Nacos's server-side health check off for one (service, cluster)
// pair, once per pair per process: the FIRST successful register of a pair
// follows up its registration with UpdateCluster (healthChecker NONE).
//
// WHY (the product decision): spotter's service discovery already owns
// health authority — the K8s readiness / consul health state arrives as the
// instance status and drives the enabled flag this sink pushes — so Nacos's
// independent TCP probes duplicate that authority with worse information:
// they test ip:port reachability from the Nacos server's own network
// position, a vantage the workload's clients never share. The audit and the
// live demo both showed the two channels disagreeing (a persistent instance
// spotter asserts healthy gets flipped unhealthy by Nacos's probe, with no
// path back but re-registration). Under NONE, registration alone decides.
//
// Trigger discipline: the application rides the register path, never the
// prune's remember path, so offline-only pushes never configure a pair — a
// pair becomes this sink's to configure through the same first non-offline
// push that registers it. There is no batch apply at startup: pairs are
// configured as they are first pushed, and clusters that existed before this
// change get configured on the first register after the restart.
//
// Failure discipline: a failed PUT logs a warning and does NOT fail the
// register push — the instance registration itself succeeded, and the
// cluster configuration is configuration-of-record, not per-instance data —
// and the pair's marker is RELEASED so the next push retries the update
// (bounded: at most one attempt per pushed instance until it succeeds,
// self-healing, no retry-queue poisoning). The PUT is idempotent, so a
// mid-push retry or a restart's fresh-process re-apply is always safe. The
// mutex is not held across the HTTP call (a 10s-timeout request must not
// block the prune's remember sweep).
//
// Concurrency discipline (dsca-2 DS-2-1 interplay): the marker is
// CLAIMED check-and-set under the mutex BEFORE the HTTP call, so under the
// bounded-parallelism register group at most ONE worker per pair issues the
// PUT — the old check-after shape (read done, PUT, then set) let every
// concurrent first-register of a pair race past the read and issue a
// duplicate idempotent PUT. The claim is released on failure so the retry
// semantics above survive; the residual duplicate window is a second Push
// call racing a FAILED first attempt (bounded, idempotent, harmless).
func (s *Sink) ensureClusterHealthCheckDisabled(service, cluster string) {
	key := clusterKeyOf{service: service, cluster: cluster}
	s.rememberedMu.Lock()
	if s.healthCheckDone[key] {
		s.rememberedMu.Unlock()
		return
	}
	// Claim before the HTTP call: exactly one in-flight attempt per pair.
	claimed := s.healthCheckClaims[key]
	s.healthCheckClaims[key] = true
	s.rememberedMu.Unlock()
	if claimed {
		return // another worker's PUT for this pair is in flight
	}
	if err := s.client.UpdateCluster(service, cluster); err != nil {
		s.logger.Warnf("nacos: disabling server-side health check for %s/%s failed, will retry on the next push: %v",
			service, cluster, err)
		// Release the claim so the next push retries (failure discipline).
		s.rememberedMu.Lock()
		delete(s.healthCheckClaims, key)
		s.rememberedMu.Unlock()
		return
	}
	s.rememberedMu.Lock()
	s.healthCheckDone[key] = true
	delete(s.healthCheckClaims, key) // the claim graduates into the done marker
	s.rememberedMu.Unlock()
	s.logger.Infof("nacos: disabled server-side health check for %s/%s", service, cluster)
}

// deregister deletes one instance by its composite id parameters — the
// same derivation the register used, so the ids round-trip.
func (s *Sink) deregister(service, ip string, port int, cluster string) error {
	err := s.client.DeregisterInstance(InstanceParams{
		ServiceName: service,
		IP:          ip,
		Port:        port,
		ClusterName: cluster,
		Ephemeral:   false,
	})
	if err != nil {
		return fmt.Errorf("nacos: deregister %s#%d/%s: %w", ip, port, cluster, err)
	}
	s.logger.Infof("nacos: deregistered instance %s#%d/%s", ip, port, cluster)
	return nil
}

// clusterOf maps the provider to the Nacos clusterName: the collision
// policy of plan §7.3 — the same app-code from k8s and ecs coexists in one
// service under distinct clusters, yielding distinct composite ids.
func clusterOf(ins *instance.Instance) string {
	if ins.Provider != "" {
		return ins.Provider
	}
	return DefaultCluster
}

// firstPort derives the wire port: Ports[0].Port, else 0 — the same rule
// on register and deregister so the composite id round-trips (plan §7.3).
func firstPort(ins *instance.Instance) int {
	if len(ins.Ports) > 0 && ins.Ports[0] != nil {
		return int(ins.Ports[0].Port)
	}
	return 0
}

// compositeID derives the Nacos composite instance id of a domain instance
// (ip#port#cluster#group@@service), the identity the prune compares remote
// hosts against.
func compositeID(ins *instance.Instance) string {
	return fmt.Sprintf("%s#%d#%s#%s@@%s", ins.Ip, firstPort(ins), clusterOf(ins), DefaultGroup, ins.AppCode)
}

// metadataSchemaVersion is the metadata schema marker of dsca-5 §4.1
// (Option C, adopted by §4.2-1): register writes "1" alongside the field
// keys, and reconstruct reads it to know the writer's shape. A missing key
// is treated as v1 — backward compatible with the live entries on the demo
// server and any older binary's writes — so the marker only needs to be
// checked, never enforced.
const metadataSchemaVersion = "1"

// metadataOf renders the metadata map carried for the GetAll round-trip
// (plan §7.3): identity and every compared field, as strings, plus the
// schemaVersion marker of dsca-5 §4.2-1 (the vehicle that makes any future
// metadata extension safely evolvable: a mixed fleet is detectable and a
// compare can later assert "entry at schema < required → re-push once to
// upgrade").
func metadataOf(ins *instance.Instance) map[string]string {
	metadata := map[string]string{
		"sourceKey":     ins.SourceKey,
		"sourceCluster": ins.SourceCluster,
		"instanceId":    ins.InstanceId,
		"envType":       ins.EnvType,
		"envGroup":      ins.EnvGroup,
		"reversion":     strconv.FormatInt(ins.Reversion, 10),
		"status":        strconv.FormatInt(int64(ins.Status), 10),
		"state":         ins.State,
		"idc":           ins.Idc,
		"cpu":           strconv.FormatFloat(float64(ins.Cpu), 'f', -1, 32),
		"version":       ins.Version,
		"schemaVersion": metadataSchemaVersion,
	}
	return metadata
}

// reconstruct rebuilds a domain instance from one remote host. The status
// comes from the metadata with the enabled→online/else→unhealthy fallback
// (plan §7.3), so unmetadataed state still classifies.
//
// Cluster is deliberately left EMPTY (dsca-3 §3.2, the DS-3-4/DS-5-3
// fidelity correction): the metadata never carried it — both providers'
// conversions write Cluster "" (k8s's formatCluster reads a label/env the
// pods do not set; consul hardcodes "") — while clusterName already lands
// in Provider (the scoping key the wire round-trips exactly). Synthesizing
// Cluster from clusterName here would false-positive every consul compare
// every cycle.
//
// A mismatched or missing schemaVersion (dsca-5 §4.2-1) is a degraded
// writer, not an error: the reconstruction still participates in the diff,
// degraded exactly like the parseInt64-garbage case — its reversion is
// zeroed so R2's reversion-in-the-compared-set (or the strictly-higher
// gate) fires once and the re-push rewrites the full metadata at the
// current schema. One-shot, self-healing, never a loop.
func reconstruct(service string, host Host) *instance.Instance {
	cluster := host.ClusterName
	status := parseStatus(host.Metadata["status"], host.Enabled)
	reversion := parseInt64(host.Metadata["reversion"])
	if v, ok := host.Metadata["schemaVersion"]; !ok || v != metadataSchemaVersion {
		// Unknown/missing schema: the entry predates the marker or comes
		// from a different writer. Degrade the reconstruction's
		// diff-participation (reversion 0) instead of trusting a shape this
		// binary may not know how to read; the compare heals by re-pushing.
		reversion = 0
	}
	ins := &instance.Instance{
		InstanceId:    host.Metadata["instanceId"],
		SourceKey:     host.Metadata["sourceKey"],
		SourceCluster: host.Metadata["sourceCluster"],
		AppCode:       service,
		Ip:            host.IP,
		Ports:         []*instance.PortInfo{{Port: int32(host.Port)}},
		Provider:      cluster,
		Cluster:       "", // never synthesized from clusterName (see the doc comment)
		Enabled:       host.Enabled,
		EnvType:       host.Metadata["envType"],
		EnvGroup:      host.Metadata["envGroup"],
		State:         host.Metadata["state"],
		Idc:           host.Metadata["idc"],
		Version:       host.Metadata["version"],
		Reversion:     reversion,
		Status:        status,
	}
	if cpu, err := strconv.ParseFloat(host.Metadata["cpu"], 32); err == nil {
		ins.Cpu = float32(cpu)
	}
	return ins
}

// parseStatus resolves the instance status: the metadata status when
// present, else enabled→online (1) / disabled→unhealthy (2).
func parseStatus(raw string, enabled bool) int32 {
	if raw != "" {
		if status, err := strconv.ParseInt(raw, 10, 32); err == nil {
			return int32(status)
		}
	}
	if enabled {
		return instance.InstanceStatusOnline
	}
	return instance.InstanceStatusUnhealthy
}

// statusAllowed reports whether status is selected: an empty status list
// selects everything (the caller's "" statuses convention).
func statusAllowed(statuses []int32, status int32) bool {
	if len(statuses) == 0 {
		return true
	}
	for _, wanted := range statuses {
		if wanted == status {
			return true
		}
	}
	return false
}

// parseInt64 parses a metadata integer field, defaulting to zero.
func parseInt64(raw string) int64 {
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}
