package worker

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

// This file holds the multi-sink machinery of the plan's §5 and §6: the
// per-sink error contract (§5.1), the sinkFanout retry seam (§6.4) and F4's
// FanoutSink — the fan-out the worker talks to, which is itself a
// ports.InstanceSink so it nests.

// SinkFailure names one sink's failure inside a fan-out push.
type SinkFailure struct {
	Sink string // sink name, e.g. "atlas", "nacos"
	Err  error
}

// FanoutError aggregates the per-sink failures of one fan-out push. A plain
// (non-Fanout) error means "all sinks failed / no fan-out happened".
//
// Unwrap (below) makes errors.As traverse into the per-sink failures, so a
// classification property attached to a sink's error — the nacos APIError's
// Permanent() is the load-bearing one — survives the aggregation: whoever
// inspects the fan-out error (the retry queue's permanent-drop check, any
// adapter-side %w wrapper) sees the leaf errors exactly as if the sinks had
// been pushed individually.
type FanoutError []SinkFailure

// Error joins the failures' messages, each prefixed with its sink's name.
func (e FanoutError) Error() string {
	parts := make([]string, 0, len(e))
	for _, failure := range e {
		parts = append(parts, failure.Sink+": "+failure.Err.Error())
	}
	return strings.Join(parts, "; ")
}

// Unwrap returns the per-sink errors in failure order, the multi-error
// contract of errors.As/errors.Is: Permanent classification and any other
// error-level test on a sink failure still holds through the aggregate.
func (e FanoutError) Unwrap() []error {
	errs := make([]error, 0, len(e))
	for _, failure := range e {
		errs = append(errs, failure.Err)
	}
	return errs
}

// FailedSinks returns the names of the failed sinks in failure order.
func (e FanoutError) FailedSinks() []string {
	sinks := make([]string, 0, len(e))
	for _, failure := range e {
		sinks = append(sinks, failure.Sink)
	}
	return sinks
}

// failedSinkNames resolves which sinks a push error queues retries for: the
// named failures of a FanoutError (detected with errors.As so adapter-side
// wrapping cannot hide the per-sink breakdown — never a type assertion), or
// every known sink for a non-fanout error, the conservative backward-
// compatible fallback of plan §5.2.
func failedSinkNames(err error, known []string) []string {
	if err == nil || len(known) == 0 {
		return nil
	}
	var fanoutErr FanoutError
	if errors.As(err, &fanoutErr) && len(fanoutErr) > 0 {
		var sinks []string
		for _, failure := range fanoutErr {
			if errors.Is(failure.Err, errStaleFullPush) {
				continue
			}
			sinks = append(sinks, failure.Sink)
		}
		return sinks
	}
	return known
}

// sinkFanout is the retry seam of plan §6.4: the named-sink surface the
// retry queue drives, exposed by the FanoutSink below. Sinks() also supplies
// the sink names of the fallback path and the per-sink queue-depth metrics.
type sinkFanout interface {
	// PushTo pushes to exactly one named sink; an unknown name errors.
	PushTo(name string, triggerTime int64, instances []*instance.Instance) error
	PushAllTo(name string, triggerTime int64, scope, batchID string, instances []*instance.Instance) error
	// Sinks exposes the registered sink names.
	Sinks() []string
}

// NamedSink binds one instance sink to the name the retry queue and the
// metrics address it by (plan §6.2).
type NamedSink struct {
	Name string // "atlas" | "nacos"
	Sink ports.InstanceSink
}

// AtlasSinkName is the fanout sink name of the Atlas discovery center, the
// primary sink (plan §6.2: the first registered sink). The plain-sink retry
// path uses the same name, so the queue keys and the per-sink metrics
// series agree whether the pusher is a fanout or a single legacy sink.
const AtlasSinkName = "atlas"

// FanoutSink fans pushes out to a fixed set of named sinks and is itself a
// ports.InstanceSink, so the worker knows sinks only as the port plus a
// name — and a fanout nests inside another fanout. Placement in pkg/worker
// is decision D3 (plan §6.1): the fan-out and the retry queue co-own the
// FanoutError/SinkFailure contract (§5.1) and the PushTo seam (§6.4), so
// splitting them across packages would export that contract anyway.
//
// The first registered sink is the primary (Atlas): it pushes first and is
// the sink whose view GetAll returns by default (§6.3). The reconcile
// designation (reconcile, below — dsca-3 §3.1) can hand that READ role to
// another sink (the nacos sink under --reconcile-source nacos) without
// touching the push fan-out at all. Construct with NewFanoutSink; the zero
// value is not usable.
type FanoutSink struct {
	sinks  []NamedSink
	logger ports.Logger
	// metrics observes the per-sink e2e decorator below. Nil is a no-op
	// (recordingSink tolerates it), so callers that construct the fanout
	// without a recorder keep compiling and working.
	metrics ports.MetricsRecorder
	// reconcile is the DESIGNATED reconcile sink of dsca-3 §3.1: the sink
	// whose view GetAll returns, looked up by name through
	// SetReconcileSource. nil keeps the v1 primary semantics (sinks[0],
	// Atlas) — the default, production-unchanged configuration. The
	// designation changes ONLY the read view: every push (Push, PushAll,
	// PushTo) still fans out to every sink exactly as before, so the
	// designated sink gains a read role, never loses its write role, and
	// the primary keeps receiving every push.
	//
	// Set once, before Run: the wiring calls SetReconcileSource during
	// provider startup (internal/server.go, before any provider goroutine
	// exists), matching the set-before-Run discipline of the k8s
	// provider's SetQueueDepthReporter. Later reads are therefore
	// unsynchronized-by-design; call SetReconcileSource mid-flight at your
	// own risk (it is not part of any contract).
	reconcile *NamedSink
}

// FanoutSink satisfies the sink port (it is a sink itself, nestable). It
// also satisfies the sinkFanout retry seam: the second assertion is what
// keeps the unsynced_service.go type assertion from silently degrading to
// the legacy single-sink path if the seam ever drifts.
var (
	_ ports.InstanceSink      = (*FanoutSink)(nil)
	_ sinkFanout              = (*FanoutSink)(nil)
	_ ports.FullOperationSink = (*FanoutSink)(nil)
)

// NewFanoutSink creates a fan-out over named sinks. It rejects zero sinks,
// empty names, duplicate names and nil sinks (plan §6.2): every later
// lookup — PushTo, the primary view, the plain-error fallback, the metrics
// labels — is by name, so the name set must be a well-formed identity of
// the sink set. The name "__total__" is reserved for the retry queue's
// depth-total metrics label (unsynced_service.go), so a sink claiming it
// would collide with that series and is rejected too. The sinks are
// copied; the caller's slice is not retained.
//
// dsca-2 §6 row 7: every sink is wrapped in the per-sink recording
// decorator (recordingSink below) before being stored, so Push, PushAll
// AND PushTo — the retry path, whose PushTo dispatches to the wrapped
// named.Sink.Push — all produce one e2e observation each, with the origin
// preserved from the (ns-epoch) trigger parameter.
func NewFanoutSink(logger ports.Logger, sinks ...NamedSink) (*FanoutSink, error) {
	return newFanoutSink(logger, nil, sinks...)
}

// NewFanoutSinkWithMetrics is NewFanoutSink plus the metrics recorder the
// per-sink e2e decorator observes on. The production wiring
// (internal/server.go) passes the real recorder; the nil-default call
// sites keep NewFanoutSink.
func NewFanoutSinkWithMetrics(logger ports.Logger, metrics ports.MetricsRecorder, sinks ...NamedSink) (*FanoutSink, error) {
	return newFanoutSink(logger, metrics, sinks...)
}

func newFanoutSink(logger ports.Logger, metrics ports.MetricsRecorder, sinks ...NamedSink) (*FanoutSink, error) {
	if len(sinks) == 0 {
		return nil, errors.New("worker: fanout requires at least one sink")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if metrics == nil {
		// Substitute BEFORE the wrap below: the decorator stores the
		// recorder by value at wrap time, so a nil passed in must never
		// reach a recordingSink (its observe call would panic). The
		// nopMetricsRecorder turns the decorator into a pass-through.
		metrics = nopMetricsRecorder{}
	}
	seen := make(map[string]struct{}, len(sinks))
	owned := make([]NamedSink, len(sinks))
	for i, named := range sinks {
		if named.Name == "" {
			return nil, fmt.Errorf("worker: fanout sink %d has an empty name", i)
		}
		if named.Name == totalSinkName {
			return nil, fmt.Errorf("worker: fanout sink name %q is reserved for the queue-depth total metrics label", named.Name)
		}
		if named.Sink == nil {
			return nil, fmt.Errorf("worker: fanout sink %q is nil", named.Name)
		}
		if _, duplicate := seen[named.Name]; duplicate {
			return nil, fmt.Errorf("worker: fanout sink %q is registered twice", named.Name)
		}
		seen[named.Name] = struct{}{}
		// Wrap BEFORE storing: every later dispatch (Push, PushAll, PushTo,
		// the nested-fanout GetAll chain) goes through the decorator.
		owned[i] = NamedSink{
			Name: named.Name,
			Sink: &orderedSink{inner: &recordingSink{name: named.Name, inner: named.Sink, metrics: metrics}},
		}
	}
	return &FanoutSink{sinks: owned, logger: logger, metrics: metrics}, nil
}

// orderedSink is the per-sink write gate. Providers and retry workers may
// call Push concurrently; Nacos has no compare-and-swap on instance writes,
// so serializing the complete operation prevents a late register from
// overtaking a newer deregister/update. The latest revision table also drops
// work that was already superseded before it reached the network.
type orderedSink struct {
	inner ports.InstanceSink
	// fullGate allows independent identities to proceed concurrently while
	// making a full snapshot an exclusive operation.
	fullGate        sync.RWMutex
	keysMu          sync.Mutex
	keyLocks        map[string]*keyLockEntry
	latestMu        sync.Mutex
	latest          map[string]identityRevision
	closed          bool
	lastFullScope   string
	lastFullBatchID string
}

type keyLockEntry struct {
	mu   sync.Mutex
	refs int
}

var _ ports.FullOperationSink = (*orderedSink)(nil)

// identityRevision records the last accepted revision and whether the
// identity was explicitly removed by a trusted complete snapshot. Tombstones
// keep late pre-delete events from resurrecting an instance while allowing a
// subsequent complete snapshot to omit the already-deleted identity.
type identityRevision struct {
	revision  int64
	tombstone bool
}

var errStaleFullPush = errors.New("worker: stale full push rejected")

func (s *orderedSink) Push(trigger int64, items []*instance.Instance) error {
	return s.run(trigger, items, false)
}

func (s *orderedSink) PushAll(trigger int64, items []*instance.Instance) error {
	return s.run(trigger, items, true)
}

// PushAllWithRevalidate obtains this sink's write gate before rebuilding the
// snapshot. This closes the window in which an incremental write could land
// after revalidation but before PushAll, then be pruned by the old snapshot.
func (s *orderedSink) PushAllWithRevalidate(trigger int64, items []*instance.Instance, revalidate func() ([]*instance.Instance, bool)) error {
	s.fullGate.Lock()
	defer s.fullGate.Unlock()
	if s.closed {
		return errors.New("worker: sink is closed")
	}
	if revalidate != nil {
		fresh, ok := revalidate()
		if !ok {
			return nil
		}
		items = fresh
	}
	return s.runLocked(trigger, items, true, true)
}

func (s *orderedSink) PushAllOperation(op ports.RetryOperation) error {
	s.fullGate.Lock()
	defer s.fullGate.Unlock()
	if s.closed {
		return errors.New("worker: sink is closed")
	}
	if op.Revalidate != nil {
		fresh, ok := op.Revalidate()
		if !ok {
			return nil
		}
		op.Instances = fresh
	}
	s.lastFullScope = op.Scope
	s.lastFullBatchID = op.BatchID
	return s.runLocked(op.Trigger, op.Instances, true, true)
}

func (s *orderedSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return s.inner.GetAll(statuses, provider)
}

func (s *orderedSink) run(trigger int64, items []*instance.Instance, full bool) error {
	if full {
		s.fullGate.Lock()
		defer s.fullGate.Unlock()
		return s.runLocked(trigger, items, true, false)
	}
	s.fullGate.RLock()
	defer s.fullGate.RUnlock()
	keys := identityKeys(items)
	unlock := s.lockKeys(keys)
	defer unlock()
	return s.runLocked(trigger, items, false, false)
}

func identityKeys(items []*instance.Instance) []string {
	seen := make(map[string]struct{}, len(items))
	keys := make([]string, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		key := instance.IdentityKey(item)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (s *orderedSink) lockKeys(keys []string) func() {
	entries := make([]*keyLockEntry, 0, len(keys))
	s.keysMu.Lock()
	if s.keyLocks == nil {
		s.keyLocks = make(map[string]*keyLockEntry)
	}
	for _, key := range keys {
		entry := s.keyLocks[key]
		if entry == nil {
			entry = &keyLockEntry{}
			s.keyLocks[key] = entry
		}
		entry.refs++
		entries = append(entries, entry)
	}
	s.keysMu.Unlock()
	for _, entry := range entries {
		entry.mu.Lock()
	}
	return func() {
		for i := len(entries) - 1; i >= 0; i-- {
			entries[i].mu.Unlock()
		}
		s.keysMu.Lock()
		for i, key := range keys {
			entries[i].refs--
			if entries[i].refs == 0 && s.keyLocks[key] == entries[i] {
				delete(s.keyLocks, key)
			}
		}
		s.keysMu.Unlock()
	}
}

func (s *orderedSink) runLocked(trigger int64, items []*instance.Instance, full bool, trustedComplete bool) error {
	if s.closed {
		return errors.New("worker: sink is closed")
	}
	s.latestMu.Lock()
	if s.latest == nil {
		s.latest = make(map[string]identityRevision)
	}
	filtered := make([]*instance.Instance, 0, len(items))
	accepted := make(map[string]identityRevision, len(items))
	seen := make(map[string]struct{}, len(items))
	stale := false
	for _, item := range items {
		if item == nil {
			continue
		}
		key := instance.IdentityKey(item)
		if full {
			if _, exists := seen[key]; exists {
				s.latestMu.Unlock()
				return fmt.Errorf("worker: duplicate identity %q in full push", key)
			}
			seen[key] = struct{}{}
		}
		rev := item.Reversion
		if rev == 0 {
			rev = trigger
		}
		if previous, ok := s.latest[key]; ok {
			if (previous.tombstone && rev <= previous.revision) || (!previous.tombstone && rev < previous.revision) {
				stale = true
				continue
			}
		}
		accepted[key] = identityRevision{revision: rev}
		filtered = append(filtered, item)
	}
	// A regular full push is only a safe prune snapshot when it contains every
	// identity this gate has already accepted. A missing key may be a newer
	// incremental write whose identity was omitted by a stale provider tick;
	// forwarding that subset would let the sink prune the live remote entry.
	// Revalidated snapshots are trusted to represent the provider's complete
	// current state and may legitimately omit identities (deletes).
	if full && !trustedComplete {
		for key, state := range s.latest {
			if state.tombstone {
				continue
			}
			if _, present := seen[key]; !present {
				s.latestMu.Unlock()
				return errStaleFullPush
			}
		}
	}
	// A full push is a complete desired-state snapshot. Passing a silently
	// filtered subset to a sink that performs pruning can delete newer remote
	// instances. Reject the batch so the event's revalidation/retry path can
	// obtain a fresh snapshot instead.
	if full && stale {
		s.latestMu.Unlock()
		return errStaleFullPush
	}
	if len(filtered) == 0 && len(items) != 0 {
		s.latestMu.Unlock()
		return nil
	}
	s.latestMu.Unlock()
	var err error
	if full {
		err = s.inner.PushAll(trigger, filtered)
	} else {
		err = s.inner.Push(trigger, filtered)
	}
	if err == nil {
		s.latestMu.Lock()
		if full && trustedComplete {
			for key, state := range s.latest {
				if state.tombstone {
					continue
				}
				if _, present := seen[key]; !present {
					s.latest[key] = identityRevision{revision: state.revision, tombstone: true}
				}
			}
		}
		for key, state := range accepted {
			s.latest[key] = state
		}
		s.latestMu.Unlock()
	}
	return err
}

func (s *orderedSink) Close() error {
	s.fullGate.Lock()
	defer s.fullGate.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.latestMu.Lock()
	s.latest = nil
	s.latestMu.Unlock()
	s.keysMu.Lock()
	s.keyLocks = nil
	s.keysMu.Unlock()
	if c, ok := s.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// recordingSink is the per-sink e2e timing decorator of dsca-2 §3: it
// wraps one named sink, observes time.Since(trigger reconstructed as
// wall-clock) around each Push/PushAll, and labels the observation with
// the sink's name and the push outcome ("ok" | "error").
//
// The origin-reconstruction limitation is documented (dsca-2 §3): the
// trigger is ns-since-epoch (UnixNano widened at the producers), so
// time.Unix(0, trigger) reconstructs a wall-clock instant with NO
// monotonic reading — an NTP step during an in-flight push skews that one
// observation (rare, accepted price of not widening the type). The
// full-push paths (PushAll/SyncAll) measure "age of the full push at
// completion" (tick-time origins), not event age — dsca-2 §6's origin
// semantics, so dashboards do not misread the SyncAll series.
type recordingSink struct {
	name    string
	inner   ports.InstanceSink
	metrics ports.MetricsRecorder
}

var _ ports.InstanceSink = (*recordingSink)(nil)

func (s *recordingSink) Push(triggerTime int64, instances []*instance.Instance) error {
	err := s.inner.Push(triggerTime, instances)
	s.observe(triggerTime, err)
	return err
}

func (s *recordingSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	err := s.inner.PushAll(triggerTime, instances)
	s.observe(triggerTime, err)
	return err
}

func (s *recordingSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return s.inner.GetAll(statuses, provider)
}

func (s *recordingSink) Close() error {
	if c, ok := s.inner.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// observe records one e2e observation for this sink with the outcome
// derived from the push error. The metrics recorder is never nil (the
// constructor substitutes the nop), and a nil-safe call here keeps the
// zero-value fanout from panicking if one is ever constructed directly.
func (s *recordingSink) observe(triggerTime int64, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	s.metrics.ObserveEventToStoreDuration(s.name, outcome, time.Since(time.Unix(0, triggerTime)))
}

// Push fans the instances out to every sink CONCURRENTLY, one goroutine per
// sink (dsca-2 DS-2-2, fix design A: the sequential fan-out added the
// primary's latency 1:1 to every secondary leg — a slow Atlas gRPC call,
// up to its 10s timeout, delayed every nacos registration while nacos
// itself was healthy; measured 1:1, zero isolation). One sink's error never
// short-circuits the others: every sink is attempted, Push returns only
// after all sinks complete, and the failures are aggregated into a
// FanoutError whose SLICE ORDER is the sinks' declaration order — each
// goroutine writes its own result into a fixed-size, position-indexed
// results slice (never a shared append), so the aggregate is deterministic
// despite nondeterministic completion (plan §6.2's contract preserved; the
// retry queue's FailedSinks() keys stay stable).
//
// Completion order across sinks is nondeterministic and harmless: each sink
// receives the pushes of its own serial caller chain in Trigger order
// (per-sink ordering is the documented invariant the fan-out must keep,
// and parallel dispatch keeps it — one goroutine per sink, so a sink's
// pushes stay serialized on that goroutine), and cross-sink ordering was
// never part of any contract. The sinks are NOT reordered: sinks[0] stays
// the primary (Atlas) and remains the sink whose view GetAll returns by
// default (the reconcile designation can hand the read role to another
// sink; the primary keeps every push either way).
func (f *FanoutSink) Push(triggerTime int64, instances []*instance.Instance) error {
	results := make([]error, len(f.sinks))
	var wg sync.WaitGroup
	for i, named := range f.sinks {
		wg.Add(1)
		go func(i int, named NamedSink) {
			defer wg.Done()
			results[i] = named.Sink.Push(triggerTime, instances)
		}(i, named)
	}
	wg.Wait()
	return f.collectFailures("push", results)
}

// Close stops all ordered sinks and releases their latest-revision state.
func (f *FanoutSink) Close() error {
	var failures []error
	for _, named := range f.sinks {
		if c, ok := named.Sink.(interface{ Close() error }); ok {
			if err := c.Close(); err != nil {
				failures = append(failures, fmt.Errorf("%s: %w", named.Name, err))
			}
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	return nil
}

// PushAll fans a full push out exactly like Push (concurrently, one
// goroutine per sink), over each sink's own PushAll: the reconcile
// semantics — including the Nacos prune sweep of plan §7.4, landed with F5
// — stay owned by each sink, and the remembered-pairs state each sink's
// prune touches is per-sink, so the two PushAll calls running at the same
// time share nothing (the fan-out's own field set is read-only after
// construction).
func (f *FanoutSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	results := make([]error, len(f.sinks))
	var wg sync.WaitGroup
	for i, named := range f.sinks {
		wg.Add(1)
		go func(i int, named NamedSink) {
			defer wg.Done()
			results[i] = named.Sink.PushAll(triggerTime, instances)
		}(i, named)
	}
	wg.Wait()
	return f.collectFailures("pushAll", results)
}

// PushAllWithRevalidate lets each ordered sink rebuild the snapshot while
// holding its own write gate, preventing an incremental update from racing a
// full-push prune.
func (f *FanoutSink) PushAllWithRevalidate(triggerTime int64, instances []*instance.Instance, revalidate func() ([]*instance.Instance, bool)) error {
	results := make([]error, len(f.sinks))
	var wg sync.WaitGroup
	for i, named := range f.sinks {
		wg.Add(1)
		go func(i int, named NamedSink) {
			defer wg.Done()
			if gated, ok := named.Sink.(interface {
				PushAllWithRevalidate(int64, []*instance.Instance, func() ([]*instance.Instance, bool)) error
			}); ok {
				results[i] = gated.PushAllWithRevalidate(triggerTime, instances, revalidate)
				return
			}
			results[i] = named.Sink.PushAll(triggerTime, instances)
		}(i, named)
	}
	wg.Wait()
	return f.collectFailures("pushAll", results)
}

// collectFailures aggregates the per-sink results into a FanoutError in
// DECLARATION order (results is position-indexed by the sinks' order), with
// one log line per failed sink — the per-sink log discipline of the
// sequential fan-out, preserved.
func (f *FanoutSink) collectFailures(operation string, results []error) error {
	var failures FanoutError
	for i, err := range results {
		if err != nil {
			f.logger.Errorf("fanout %s to sink %q failed: %s", operation, f.sinks[i].Name, err)
			failures = append(failures, SinkFailure{Sink: f.sinks[i].Name, Err: err})
		}
	}
	if len(failures) > 0 {
		return failures
	}
	return nil
}

// GetAll returns the view the reconcile designation selects: the DESIGNATED
// sink's view when one is set (dsca-3 §3.1, --reconcile-source nacos), else
// the primary (first, Atlas-positioned) sink's — the v1 semantics of plan
// §6.3, byte-identical to the pre-designation behavior when no sink is
// designated (the production default). Only the designated (or primary) sink
// is consulted: the other sinks' GetAll is never called, and an error from
// the consulted sink propagates unchanged — no fallback, matching the
// pre-fanout behavior where a failing Atlas GetAll aborts or logs per
// provider. The returned view also feeds the worker's GetAll, which the
// providers' comparisons consume.
//
// Failure window, stated honestly (plan §3.3): without a designation only
// the primary's view feeds the comparisons, so secondary-sink drift that
// involves no local instance change (e.g. an out-of-band Nacos deletion) is
// invisible there. The designation exists precisely to close that window
// for one named sink: with --reconcile-source nacos the periodic
// CompareAndFlush of both providers reads the nacos catalog view, making
// nacos the authoritative external store the reconcile converges against
// (heals manual metadata edits, enabled flips and API-side deletes within
// one push interval). The heal still rides the next CompareAndFlush tick
// for drift that involves no local change; per-sink comparison for the
// remaining sinks is an explicit follow-up (plan §10).
func (f *FanoutSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	if f.reconcile != nil {
		return f.reconcile.Sink.GetAll(statuses, provider)
	}
	return f.sinks[0].Sink.GetAll(statuses, provider)
}

// SetReconcileSource designates the sink whose view GetAll returns
// (dsca-3 §3.1). An empty name resets the designation to the primary (the
// default v1 semantics). A name that matches no registered sink is an
// error — the designation is resolved by name exactly like PushTo, so an
// unknown name must fail fast instead of silently designating nothing
// (the same discipline as NewFanoutSink's name validation). Designating
// the primary by its own name is legal and equivalent to the default.
//
// The designation changes only the READ view; see the reconcile field's
// comment for the set-before-Run discipline.
func (f *FanoutSink) SetReconcileSource(name string) error {
	if name == "" {
		f.reconcile = nil
		return nil
	}
	for i := range f.sinks {
		if f.sinks[i].Name == name {
			f.reconcile = &f.sinks[i]
			return nil
		}
	}
	return fmt.Errorf("worker: fanout reconcile source %q is not a registered sink (known sinks: %s)",
		name, strings.Join(f.Sinks(), ", "))
}

// PushTo pushes to exactly one named sink — the retry seam
// UnsyncedService.syncOnce drives (plan §6.4). An unknown name is an error.
func (f *FanoutSink) PushTo(name string, triggerTime int64, instances []*instance.Instance) error {
	for _, named := range f.sinks {
		if named.Name == name {
			return named.Sink.Push(triggerTime, instances)
		}
	}
	return fmt.Errorf("worker: fanout PushTo unknown sink %q (known sinks: %s)",
		name, strings.Join(f.Sinks(), ", "))
}

func (f *FanoutSink) PushAllTo(name string, triggerTime int64, scope, batchID string, instances []*instance.Instance) error {
	for _, named := range f.sinks {
		if named.Name == name {
			return named.Sink.PushAll(triggerTime, instances)
		}
	}
	return fmt.Errorf("worker: fanout PushAllTo unknown sink %q", name)
}

// PushAllToWithRevalidate replays one full operation for a named sink while
// rebuilding the snapshot inside that sink's exclusive full-push gate.
func (f *FanoutSink) PushAllToWithRevalidate(name string, triggerTime int64, scope, batchID string, instances []*instance.Instance, revalidate func() ([]*instance.Instance, bool)) error {
	for _, named := range f.sinks {
		if named.Name != name {
			continue
		}
		if gated, ok := named.Sink.(interface {
			PushAllWithRevalidate(int64, []*instance.Instance, func() ([]*instance.Instance, bool)) error
		}); ok {
			return gated.PushAllWithRevalidate(triggerTime, instances, revalidate)
		}
		return named.Sink.PushAll(triggerTime, instances)
	}
	return fmt.Errorf("worker: fanout PushAllToWithRevalidate unknown sink %q", name)
}

// PushAllOperation preserves the typed retry metadata at the fanout boundary.
func (f *FanoutSink) PushAllOperation(op ports.RetryOperation) error {
	if op.Sink == "" {
		return errors.New("worker: full operation sink is empty")
	}
	for _, named := range f.sinks {
		if named.Name != op.Sink {
			continue
		}
		if operationSink, ok := named.Sink.(ports.FullOperationSink); ok {
			return operationSink.PushAllOperation(op)
		}
		return f.PushAllToWithRevalidate(op.Sink, op.Trigger, op.Scope, op.BatchID, op.Instances, op.Revalidate)
	}
	return fmt.Errorf("worker: full operation sink %q is not registered", op.Sink)
}

// Sinks returns the registered sink names in declaration (construction)
// order — the deterministic order the plain-error all-sinks fallback and
// the per-sink queue-depth metrics rely on (plan §6.4). The slice is a
// copy; callers cannot mutate the fanout's registration.
func (f *FanoutSink) Sinks() []string {
	names := make([]string, 0, len(f.sinks))
	for _, named := range f.sinks {
		names = append(names, named.Name)
	}
	return names
}
