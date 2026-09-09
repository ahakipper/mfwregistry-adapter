package worker

import (
	"errors"
	"fmt"
	"strings"

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
		return fanoutErr.FailedSinks()
	}
	return known
}

// sinkFanout is the retry seam of plan §6.4: the named-sink surface the
// retry queue drives, exposed by the FanoutSink below. Sinks() also supplies
// the sink names of the fallback path and the per-sink queue-depth metrics.
type sinkFanout interface {
	// PushTo pushes to exactly one named sink; an unknown name errors.
	PushTo(name string, triggerTime int64, instances []*instance.Instance) error
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
// the only sink whose view GetAll returns (§6.3). Construct with
// NewFanoutSink; the zero value is not usable.
type FanoutSink struct {
	sinks  []NamedSink
	logger ports.Logger
}

// FanoutSink satisfies the sink port (it is a sink itself, nestable). It
// also satisfies the sinkFanout retry seam: the second assertion is what
// keeps the unsynced_service.go type assertion from silently degrading to
// the legacy single-sink path if the seam ever drifts.
var (
	_ ports.InstanceSink = (*FanoutSink)(nil)
	_ sinkFanout         = (*FanoutSink)(nil)
)

// NewFanoutSink creates a fan-out over named sinks. It rejects zero sinks,
// empty names, duplicate names and nil sinks (plan §6.2): every later
// lookup — PushTo, the primary view, the plain-error fallback, the metrics
// labels — is by name, so the name set must be a well-formed identity of
// the sink set. The name "__total__" is reserved for the retry queue's
// depth-total metrics label (unsynced_service.go), so a sink claiming it
// would collide with that series and is rejected too. The sinks are
// copied; the caller's slice is not retained.
func NewFanoutSink(logger ports.Logger, sinks ...NamedSink) (*FanoutSink, error) {
	if len(sinks) == 0 {
		return nil, errors.New("worker: fanout requires at least one sink")
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
		owned[i] = named
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	return &FanoutSink{sinks: owned, logger: logger}, nil
}

// Push fans the instances out to every sink sequentially, in declaration
// order (the first sink — Atlas — is the primary and pushes first). One
// sink's error never short-circuits the others: every sink is attempted,
// the failures are aggregated into a FanoutError, and nil is returned only
// when all sinks succeeded (plan §6.2). Sequential, not parallel: push
// volume is per-event and small, deterministic ordering keeps logs and
// tests stable, and a slow secondary must not reorder the primary's pushes
// relative to today.
func (f *FanoutSink) Push(triggerTime int64, instances []*instance.Instance) error {
	var failures FanoutError
	for _, named := range f.sinks {
		if err := named.Sink.Push(triggerTime, instances); err != nil {
			f.logger.Errorf("fanout push to sink %q failed: %s", named.Name, err)
			failures = append(failures, SinkFailure{Sink: named.Name, Err: err})
		}
	}
	if len(failures) > 0 {
		return failures
	}
	return nil
}

// PushAll fans a full push out exactly like Push, over each sink's own
// PushAll: the reconcile semantics — including the Nacos prune sweep of
// plan §7.4, landed with F5 — stay owned by each sink.
func (f *FanoutSink) PushAll(triggerTime int64, instances []*instance.Instance) error {
	var failures FanoutError
	for _, named := range f.sinks {
		if err := named.Sink.PushAll(triggerTime, instances); err != nil {
			f.logger.Errorf("fanout pushAll to sink %q failed: %s", named.Name, err)
			failures = append(failures, SinkFailure{Sink: named.Name, Err: err})
		}
	}
	if len(failures) > 0 {
		return failures
	}
	return nil
}

// GetAll returns the primary (first, Atlas-positioned) sink's view — the
// v1 semantics of plan §6.3. Only the primary is consulted: secondary sinks'
// GetAll is never called, and a primary error propagates unchanged,
// matching the pre-fanout behavior where a failing Atlas GetAll aborts or
// logs per provider. The primary view also feeds the worker's GetAll, which
// the providers' comparisons consume.
//
// Failure window, stated honestly (plan §3.3): in v1 only the primary's
// view feeds the comparisons, so secondary-sink drift that involves no
// local instance change (e.g. an out-of-band Nacos deletion) is invisible
// here. It heals at the next full-push tick: ProcessIntervalFullPush runs
// CompareAndFlush and, once plan §7.4's SyncAll trigger lands with F5, also
// emits the OperateTypeSyncAll event whose fan-out runs the secondary
// sink's PushAll prune sweep. Per-sink comparison is an explicit follow-up
// (plan §10).
func (f *FanoutSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	return f.sinks[0].Sink.GetAll(statuses, provider)
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
