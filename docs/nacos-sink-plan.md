# Multi-Sink Plan: Nacos as the Second InstanceSink

> **Current-state addendum (2026-09-13):** This document contains the original F5
> implementation plan. Its D4 “hand-rolled HTTP client” decision is superseded:
> all production Nacos operations must now go through the official Nacos SDK or
> an explicitly versioned SDK facade. The existing HTTP client may remain only
> as a migration-period compatibility/rollback adapter. See
> [system-readiness-consistency-remediation-plan-2026-09-12.md](system-readiness-consistency-remediation-plan-2026-09-12.md) §10 for the
> mandatory migration and complete test gate.
>
> The remaining F5/soak descriptions in this document describe the historical
> local implementation stage; they do not constitute a production PASS.
> Production readiness is governed by remediation plan B2/B3 and
> `ID-NACOS-SDK-MANDATE`.

Status: authoritative implementation plan for the multi-sink initiative on
`refactor/all` (current implementation baseline `b1d9e2f`). The lead implements it
phase-by-phase (F2..F6) under agent review; each phase's exit criteria are
the review contract. Companions: [ddd-architecture.md](ddd-architecture.md)
(target layering, §4 decisions), [architecture.md](architecture.md),
[testing.md](testing.md). This document changes no code by itself. All
file:line citations were verified against the working tree at HEAD
`b976356`; facts labeled "F0-verified" were validated by the lead on the
local docker/colima stack and are ground truth.

---

## 1. Executive Summary

spotter pushes provider instances (k8s pods, consul catalog entries) to a
single discovery center ("Atlas") over gRPC. Three structural defects block
adding a second destination such as Nacos:

1. **Duplicated domain types.** `Instance` exists twice, field-for-field:
   `v2.Instance` (pkg/beehive/service/v2/v2.go:27-52, the proto mirror) and
   `instance.Instance` (internal/domain/instance/model.go:4-29, the DDD
   domain model). Thirteen production files still import the mirror, so the
   domain type is used only by new code while the entire sink path runs on
   the mirror. Tests pay a concrete tax: pkg/worker/worker_test.go:108-148
   is a `fakeSinkPusher` adapter whose whole job is copying between the two
   identical structs — lossily (only `InstanceId` and `Reversion` survive,
   worker_test.go:134, 145).
2. **The sink bypasses its own port.** `ports.InstanceSink` is declared over
   domain types (internal/ports/ports.go:54-59), but the concrete sink
   `discoverycenter.DiscoveryCenter` (pkg/discoverycenter/registry.go:18-24)
   implements `Push`/`PushAll`/`GetAll` over `v2` types (registry.go:45-82)
   and carries no `var _ ports.InstanceSink` assertion — it cannot, the
   types do not unify. The worker holds the concrete
   `discoverycenter.Pusher` (pkg/worker/worker.go:17), so nothing between
   the providers and the wire is port-polymorphic. Sink #2 today would mean
   another concrete type threaded through the worker.
3. **Single-sink retry state.** `UnsyncedService` dedups failed pushes by
   `instanceId` alone (pkg/worker/unsynced_service.go:19, keyed insert at
   unsynced_service.go:61) and its retry loop deletes an entry when *any*
   push succeeds (unsynced_service.go:99-101). Correct with one sink; wrong
   with two: "Atlas succeeded, Nacos failed" deletes the retry entry and
   Nacos silently diverges until the next full push.

Meanwhile the extracted domain diff primitives (`DiffNewerReversion`,
`DiffEqualReversion`, `CompareThreeWay`, `InitInstanceFilters` —
internal/domain/instance/rules.go:108-172, rules.go:78-105) have zero
production callers: k8s and consul still run inline diff policies
(pkg/providers/k8s/k8s.go:226-239, pkg/providers/consul/consul.go:346-364).
The multi-sink work must not force-unify those policies — the taken decision
in ddd-architecture.md §4(j).

**Goal.** Nacos becomes a second `ports.InstanceSink` alongside Atlas,
behind a `FanoutSink` the worker talks to, with per-sink retry state — plus
a full local end-to-end environment (real Nacos, consul, k3s, etcd
election) and a 1-hour continuous soak with extreme edge cases, runnable on
a developer macOS + colima machine. The Atlas path's wire behavior is
preserved; the Nacos sink is purely additive (`--nacos-addr` empty = exactly
today's behavior). Five key decisions, each justified where marked:

| # | Decision | Where |
| --- | --- | --- |
| D1 | `pkg/beehive/service/v2` becomes thin type aliases over `internal/domain/instance` (not a 13-file import migration) | §4.1 |
| D2 | `GetAll` stays in the `InstanceSink` port | §3.3 |
| D3 | `FanoutSink` lives in `pkg/worker` (not `internal/app`) | §6.1 |
| D4 | **Superseded:** production Nacos operations use the official SDK/facade; HTTP is migration-only compatibility | §7.2 and remediation plan §10 |
| D5 | Soak is a `make test-soak` target: compose stack + tagged Go harness driving the real binary, excluded from `test-all` | §8.1 |

## 2. Current-State Audit (evidence)

### 2.1 The type split

| Claim | Evidence |
| --- | --- |
| Two field-identical `Instance` structs (PortInfo, InstanceList+getter, CommonResponse+getters, request types all duplicated) | v2.go:27-111 vs model.go:4-88 |
| 13 production files import the `v2` mirror | `pkg/discoverycenter/{client,registry}.go`, `pkg/providers/{cache,common,iface}.go`, `pkg/providers/consul/{consul,convertion}.go`, `pkg/providers/k8s/{k8s,conversion}.go`, `pkg/worker/{worker,types,unsynced_service,worker_fack}.go` (verified: `rg -l 'beehive/service/v2' --type go`, minus 8 test files) |
| The mirror documents its own wire-format limitation: plain structs, not proto messages; requires a server-side JSON codec; real proto marshaling is a follow-up | v2.go:1-16 |
| The port exists but nothing satisfies it on the sink path (`discoverycenter.Pusher` is declared over `v2` types) | ports.go:54-59; registry.go:12-16 |

### 2.2 Wiring around the sink

| Claim | Evidence |
| --- | --- |
| Worker holds the concrete infra interface; `Worker.GetAll` returns `*v2.InstanceList` | worker.go:17, 21; pkg/worker/types.go:9 |
| No compile-time port check on the sink — the elector's assertion is the precedent to copy | `rg 'var _ ports\.'` within pkg/discoverycenter and pkg/worker matches only pkg/worker/elector.go:74 |
| Composition dials and wires per leadership term; dial retry 3x/5s | internal/server.go:271-297 (dial → NewDiscoveryCenter → NewResourceWorker), server.go:357-385, constants server.go:334-338 |
| Providers consume `GetAll` for the comparison: k8s `[online, unhealthy]`/`k8s`, consul `[online]`/`ecs` | k8s.go:286; consul.go:323 |
| Inline diff policies shadow the domain rules; three-way compare duplicated | k8s.go:226-239 vs rules.go:108-125; consul.go:346-364 vs rules.go:128-142; rules.go:146-172 vs k8s.go:272-363 / consul.go:306-406 |
| Offline markers differ per provider (policy, keep) | k8s: `Enabled=false, Status=3, State=terminated` (k8s.go:350-352); consul incremental: same shape (consul.go:226-229); consul remote-only cleanup: `Status=2` only (consul.go:394-396) |

### 2.3 Retry semantics (single-sink today)

| Claim | Evidence |
| --- | --- |
| Queue keyed by instanceId only; keep-highest-Reversion on re-add | `store map[string]*Event` unsynced_service.go:19, keyed insert unsynced_service.go:61-65, reversion rule unsynced_service.go:53-59 |
| Any-success-deletes entry | unsynced_service.go:99-101 |
| 5s fixed retry cadence, lock held across pushes | ticker unsynced_service.go:79; `syncOnce` holds `s.Lock()` through the network loop unsynced_service.go:91-104 |
| Queue depth metric is unlabeled | `sync_error_gauge` GaugeVec (pkg/metrics/stat.go:49-56), written `WithLabelValues("sync_error_gauge")` (internal/infra/metrics/metrics.go:72-74) from syncOnce unsynced_service.go:97 |
| Test scaffolding converts between the twin types | worker_test.go:108-148 |
| `worker_fack.go` is dead code in a production package (zero callers; ddd-architecture.md §2.6/§4(g) prescribes deletion) | pkg/worker/worker_fack.go:10-97 |

### 2.4 Test infrastructure to build on

| Asset | Evidence |
| --- | --- |
| `FakeInstanceSink` (scriptable, call-recording) | internal/testkit/fakes/fakes.go:161-224 |
| `consulmock` httptest server with drift/index control (`AdvanceIndex`) | internal/testkit/consulmock/server.go:26-131, 105-111 |
| `discoverymock` bufconn gRPC server with the JSON codec | internal/testkit/discoverymock/server.go:50-62 |
| Embedded-etcd election mock (in-process, loopback, dynamic ports; boot ~0.6s, exactly one leader) | internal/testkit/etcdmock; tests/e2e/elector_e2e_test.go:22-101 |
| Tiered Makefile matrix with explicit package allowlist (~3 min aggregate) | Makefile: `TEST_PKGS`, `test-unit`, `test-blackbox`, `test-e2e`, `test-all` |
| e2e tier is build-tag guarded | tests/e2e/e2e_test.go:1-8 |

## 3. Target Architecture

### 3.1 Diagram

```
+--------------------+      +---------------------------------------------------------------+
| InstanceSource     |      |                     pkg/worker (application)                  |
| (pkg/providers)    |      |                                                               |
|  k8s adapter  -----+----->|  DefaultWorker.Handle(event)                                  |
|  consul adapter ---+----->|      │                                                        |
|                    |      |      ▼                                                        |
| CompareAndFlush    |      |  FanoutSink (ports.InstanceSink, F4)                           |
| (per provider,     |      |      │ fan-out, per-sink errors        ▲ GetAll (primary view)  |
|  stays in the      |      |      ▼                               │                       |
|  provider/app      |      |  ┌─ AtlasSink   ─ pkg/discoverycenter (gRPC, existing)         |
|  layer per ddd     |      |  │   var _ ports.InstanceSink (F2)                              |
|  §4(j))            |      |  └─ NacosSink   ─ pkg/nacos (HTTP v1 OpenAPI, F5)              |
|                    |      |                                                               |
+--------------------+      |  UnsyncedService (F3): keyed (instanceId, sink),               |
        │                   |  per-sink retry every 5s via FanoutSink.PushTo                 |
        └── worker.GetAll ──┼───────────────────────────────────────────────────────────────+
                            +---------------------------------------------------------------+
```

Ownership rules: providers remain source-shaped adapters with
provider-specific comparison policies (k8s.go:226-239, consul.go:346-364),
exactly per ddd-architecture.md §4(j) — the fan-out never inspects or
unifies them. The worker owns event routing, fan-out and retry, and knows
sinks only as `ports.InstanceSink` plus a name. Each sink owns its
transport, error surface and reconcile semantics; one sink's failure never
aborts the others' pushes.

### 3.2 Per-sink retry state

`UnsyncedService` keys become `(instanceId, sinkName)`; an entry is deleted
when *that sink* succeeds for *that instance* (§5) — the direct fix for the
any-success-deletes defect (unsynced_service.go:99-101) that multi-sink
makes visible.

### 3.3 Decision D2 — `GetAll` stays in `InstanceSink`

The port keeps its exact shape (ports.go:54-59): `Push`, `PushAll`,
`GetAll(statuses []int32, provider string)`. Rationale:

- Both concrete sinks can answer it: Atlas via `GetAllInstance`
  (pkg/discoverycenter/client.go:132-152), Nacos via
  `/nacos/v1/ns/instance/list` per service (§7.4).
- The comparison consumers need exactly one remote view per provider run
  (k8s.go:286, consul.go:323), which `FanoutSink.GetAll` supplies from the
  primary sink (§6.3).
- Splitting into a push-only sink plus a query-capable variant now adds a
  second port and a wiring decision at every construction site for zero
  current benefit. A future sink that cannot query is handled then (a
  sub-interface, or a documented sentinel error); the fan-out hides the
  choice from the worker either way.
- Churn: the three-method shape is already replicated across
  `ports.InstanceSink`, `discoverycenter.Pusher` (registry.go:12-16),
  `worker.Worker` (types.go:5-10) and the fakes; a variant multiplies that
  surface.

Failure window, stated honestly: in v1 only the primary sink's (Atlas) view
feeds `CompareAndFlush`. Nacos drift involving no local instance change
(e.g. an out-of-band Nacos deletion) heals at the next full-push tick:
`ProcessIntervalFullPush` (default 21600s — k8s.go:408-428,
consul.go:283-303) runs `CompareAndFlush` and, once §7.4's SyncAll trigger
lands, also emits an `OperateTypeSyncAll` event whose fan-out runs
`NacosSink.PushAll`'s prune sweep. That path is dormant today: `PushAll`'s
only production caller is the worker's SyncAll handler (worker.go:67-75),
no production code emits the event — the one full-list emitter that existed
(`flushInstances`, k8s.go:253-269) is dead code (ddd-architecture.md
§4(g)) — and the periodic path only ever emits per-instance Sync events.
This initiative deliberately revives it (§7.4). Per-sink comparison is an
explicit follow-up (§10).

## 4. Phase F2 — Sink-Path Type Unification + Port Wiring

Goal: make the existing Atlas sink satisfy `ports.InstanceSink` with the
domain types, without a 13-file import migration and without touching the
wire format.

### 4.1 Decision D1 — alias bridge, not import migration

**Decision.** `pkg/beehive/service/v2/v2.go` is rewritten as a thin
re-export: every data type becomes a type alias over
`internal/domain/instance`; the gRPC client surface (v2.go:113-159) stays
verbatim — its request/response types are now the domain types. The package
doc comment (v2.go:1-16) is **kept**, including the wire-format limitation
paragraph: aliases are compile-time identity and the JSON codec marshals
the same structs.

```go
// pkg/beehive/service/v2/v2.go (target shape, ~60 lines total)
package v2

import (
    "context"
    "google.golang.org/grpc"
    "spotter/internal/domain/instance"
)

// The mirror types ARE the domain types; legacy import sites keep
// compiling while the sink path migrates.
type (
    Instance               = instance.Instance
    PortInfo               = instance.PortInfo
    InstanceList           = instance.InstanceList
    CommonResponse         = instance.CommonResponse
    SynInstancesRequest    = instance.SynInstancesRequest
    SynAllInstancesRequest = instance.SynAllInstancesRequest
    GetAllInstancesRequest = instance.GetAllInstancesRequest
)
// InstanceServiceClient, instanceServiceClient, NewInstanceServiceClient
// remain verbatim (v2.go:113-159).
```

Rationale:

1. **Aliasability is already complete.** The domain model has every type
   and accessor the mirror has (§2.1), so no struct definition survives in
   `v2`.
2. **One type, both vocabularies.** `[]*v2.Instance` and
   `[]*instance.Instance` become the same slice type, so `DiscoveryCenter`
   can carry the port assertion unchanged and `NewResourceWorker` can take
   `ports.InstanceSink` while the providers keep compiling unmodified.
3. **Minimal, reviewable churn.** Rewriting imports in 13 production + 8
   test files is a large mechanical diff with real conflict risk against
   in-flight work, and buys nothing functional; providers migrate
   opportunistically later and the alias file is deletable once the last
   import disappears.
4. **Zero wire risk.** Same structs, same JSON codec, same RPC paths
   (v2.go:134-159), guarded by the e2e tier
   (tests/e2e/consul_pipeline_test.go). The pain is already paid in tests
   (worker_test.go:108-148), which F2 deletes.

Trade-off, stated: `pkg/beehive/service/v2` outlives ddd-architecture.md's
end-state (§3.3 there wants the package dead). Accepted: it becomes a pure
re-export shim, and its removal is a later file-by-file cleanup, not a
phase.

### 4.2 File-by-file changes

| File | Change |
| --- | --- |
| `pkg/beehive/service/v2/v2.go` | Struct definitions → alias block (§4.1); package doc and client surface kept; ~160 lines → ~60 |
| `pkg/discoverycenter/registry.go` | Add `var _ ports.InstanceSink = (*DiscoveryCenter)(nil)` (precedent: elector.go:74); switch the `v2` import to `instance` (required — the import switch is what makes exit criterion 1 meaningful, not a cosmetic); `Pusher` (registry.go:12-16) stays one phase, comment-marked deprecated |
| `pkg/discoverycenter/client.go` + tests | Import switches from `v2` to `instance` (required, same as registry.go; nothing else changes — the types are identical via the aliases); `Sync`/`SyncAll`/`GetAll` (client.go:95-152) untouched |
| `pkg/worker/worker.go` | `DefaultWorker.pusher` and the `NewResourceWorker` param become `ports.InstanceSink` (worker.go:17, 21); handler bodies unchanged |
| `pkg/worker/types.go`, `unsynced_service.go` | Textually switch to domain types (`*instance.InstanceList` at types.go:9, `[]*instance.Instance` at types.go:23, `ports.InstanceSink` at unsynced_service.go:16); the package stops importing `v2`; no behavior change in F2 |
| `pkg/worker/worker_fack.go` | **Delete.** Zero callers (grep-verified; ddd-architecture.md §4(g)); it holds the exact `Pusher` field F2 removes |
| `pkg/worker/worker_test.go`, `blackbox_test.go` | Delete `fakeSinkPusher`/`toDomainInstances`/`toV2Instances` (worker_test.go:108-148); pass `fakes.FakeInstanceSink` directly — the types now unify; mechanical updates elsewhere |
| `internal/server.go` | None required: server.go:293 passes `*discoverycenter.DiscoveryCenter`, which now satisfies `ports.InstanceSink` |

Deliberately NOT touched in F2: `pkg/providers/**` (they keep the `sv`
alias import — same type via the bridge), `internal/domain/instance/**`,
`cmd/**`, `internal/infra/config/**`.

### 4.3 Exit criteria and risk

1. Sink path is mirror-free, scoped to the two sink-path packages:
   `rg -l 'beehive/service/v2' pkg/discoverycenter pkg/worker` returns
   nothing, and `rg '\bv2\.'` in those two packages matches nothing.
   Providers' `sv` alias references are out of scope by design — they still
   compile against the aliases (a whole-repo `rg '\bv2\.Instance\b'` is a
   weak check precisely because providers spell the import `sv`).
2. `go build ./...` / `go vet ./...` clean; the `var _ ports.InstanceSink`
   assertion compiles.
3. `make test-unit test-blackbox test-e2e` green (the discoverymock
   JSON-codec e2e proves the wire format survived).
4. `worker_fack.go` and the worker_test.go conversion adapter are gone.

Risk note: wire-format regression is impossible at runtime from aliases
(compile-time identity; the JSON-codec path discoverymock/server.go:166-196
is exercised by `test-e2e`), and the production-Atlas limitation
(v2.go:9-16) still stands, restated in the alias file's doc. Merge surface:
5 files materially changed, 1 deleted.

## 5. Phase F3 — Per-Sink Retry State

Goal: `UnsyncedService` tracks failures per sink; "any success deletes"
becomes "that sink's success deletes that entry".

### 5.1 Shared error contract (introduced here, consumed by F4)

New types in `pkg/worker`, so F3 and F4 share them:

```go
// SinkFailure names one sink's failure inside a fan-out push.
type SinkFailure struct {
    Sink string // sink name, e.g. "atlas", "nacos"
    Err  error
}

// FanoutError aggregates the per-sink failures of one fan-out call. A plain
// (non-Fanout) error means "all sinks failed / no fan-out happened".
type FanoutError []SinkFailure

func (e FanoutError) Error() string // joined messages, sink-prefixed
func (e FanoutError) FailedSinks() []string
```

Wrapping discipline, one line: only the fanout constructs `FanoutError`;
sinks return their own plain (possibly wrapped) errors, and every consumer
detects the aggregate with `errors.As` — never a type assertion — so
adapter-side wrapping cannot hide the per-sink breakdown.

### 5.2 Store shape and semantics

```go
type retryKey struct {
    InstanceID string
    Sink       string
}
type pendingPush struct {
    Trigger  int64
    Instance *instance.Instance
}
// UnsyncedService.store becomes map[retryKey]*pendingPush
```

- `Add(triggerTime int64, instances []*instance.Instance, sinks []string)`
  — one entry per (instance, failed sink); the legacy two-arg `Add`
  (unsynced_service.go:39) disappears (every caller is in-tree).
- Keep-highest-Reversion applies **per key** (today per instanceId,
  unsynced_service.go:53-59): the same instanceId may hold different
  revisions for different sinks; a re-add with higher `Reversion` replaces
  only that sink's entry.
- Retry loop: 5s cadence kept (unsynced_service.go:79); for each key, push
  that instance to that sink only via `FanoutSink.PushTo` (§6.4); delete
  the key on that push's success.
- Worker handlers (worker.go:53-66 Sync, worker.go:67-75 SyncAll): on
  `FanoutError`, queue only the failed sinks; on a non-fanout error, queue
  all sinks (conservative, backward-compatible fallback). `Len()` reports
  total keys; add `Lens() map[string]int` for per-sink metrics and tests.
- **Lock discipline fix (contained):** today `syncOnce` holds `s.Lock()`
  across every network push (unsynced_service.go:91-104). With per-sink
  retry this becomes acute: a dead Nacos makes every 5s tick hold the lock
  through a timeout while the Sync handler's `Add` blocks. Fix: snapshot
  keys under the lock, push outside it, delete-by-compare under the lock.

### 5.3 Metrics change (documented as breaking, accepted)

`sync_error_gauge` gains a `sink` label. Port:
`MetricsRecorder.SetSyncErrorQueueDepth(sink string, depth int)`
(ports.go:93 signature change — internal interface; all implementers are
in-tree: internal/infra/metrics/metrics.go:72-74, the two
`nopMetricsRecorder` types, fakes.FakeMetricsRecorder). Series:
`sync_error_gauge{sink="atlas"}` / `sync_error_gauge{sink="nacos"}` replace
the single unlabeled series (the gauge name and the label-value-as-name
convention are retired for this one metric).

This is a **breaking change to the metrics surface** (ddd-architecture.md
§4(i).4 lists metrics as invariants). Accepted now: the unlabeled series is
semantically wrong the moment a second sink exists — one number cannot say
which registry diverges, and per-sink observability is the point of
per-sink retry. Dashboards add one label filter; this is the second
accepted metrics exception after the `SyncAllDurationsHistogram` deletion
(ddd-architecture.md §4(i).4). The Nacos sink does **not** touch
`sync_once_durations_histogram` / `sync_once_gauge` in v1 (they stay
Atlas-scoped, recorded at discoverycenter client.go:107-108); per-sink
push latency is a follow-up.

### 5.4 TDD test list (pkg/worker)

| Test name | Scenario |
| --- | --- |
| `TestUnsyncedAddRecordsPerSinkKeys` | Add one instance with 2 failed sinks → `Len()==2`, `Lens()` both 1 |
| `TestRetryAtlasSuccessNacosFailureRetriesNacosOnly` | A succeeds, B fails → after `syncOnce`, A's key gone, B's remains; A received exactly one push (no re-push) |
| `TestRetryBothSinksFailBothRemain` | both fail → both keys remain, each sink attempted once per cycle |
| `TestNacosRecoversLaterDrainsItsQueue` | B fails on cycle 1, succeeds on cycle 2 → B's key deleted, queue empty |
| `TestKeepHighestReversionPerSinkKey` | same instanceId re-added to sink B with higher Reversion → B's entry updated, A's untouched |
| `TestWorkerQueuesOnlyFailedSinks` | `worker.Handle(Sync)` with fanout A-ok/B-fail → only the (instance, "nacos") key queued |
| `TestWorkerLegacyErrorQueuesAllSinks` | non-FanoutError → entries for every known sink (fallback) |
| `TestSyncOnceRecordsPerSinkQueueDepth` | recorder observed with both sink labels, in order |
| `TestAddNotBlockedByInflightRetry` | a sink fake blocking 500ms inside `PushTo`; concurrent `Add` returns well before it (guards the lock fix) |
| `TestSyncOnceDeletesOnlySucceededKeysUnderContention` | A succeeds while B fails in one cycle; a re-Add for A during the cycle is not lost (snapshot-then-delete compare) |

Driven by two `fakes.FakeInstanceSink` instances behind a hand-rolled
two-sink fanout stub; F4 replaces the stub with the real `FanoutSink` and
the tests run unchanged — that is the F3→F4 contract.

## 6. Phase F4 — FanoutSink

### 6.1 Decision D3 — placement in `pkg/worker`

**Decision.** `FanoutSink` lives in `pkg/worker/fanout.go`, next to
`DefaultWorker` and `UnsyncedService`. Rationale: the fan-out and the retry
queue co-own the `FanoutError`/`SinkFailure` contract (§5.1) and the
`PushTo` seam, so splitting them across packages exports that contract
anyway; `internal/app/...` does not exist yet (ddd Phase C pending) and a
one-file layer now means import gymnastics with no behavioral gain; the
Makefile already allowlists `./pkg/worker` (`TEST_PKGS`), so the matrix is
unchanged; when ddd Phase C moves the worker package, the fanout moves with
it — it imports only `ports` and domain types.

### 6.2 Shape

```go
type NamedSink struct {
    Name string              // "atlas" | "nacos"
    Sink ports.InstanceSink
}
type FanoutSink struct { /* sinks []NamedSink, logger ports.Logger */ }
func NewFanoutSink(logger ports.Logger, sinks ...NamedSink) (*FanoutSink, error)
// error on zero sinks or duplicate names.
```

`Push`/`PushAll`: **sequential** fan-out in declaration order (Atlas
first — the primary); collect per-sink failures into a `FanoutError`;
return nil only if all sinks succeeded. Sequential, not parallel: push
volume is per-event and small; deterministic ordering keeps logs and tests
stable; a slow Nacos must not reorder Atlas pushes relative to today. One
sink's error never short-circuits the others — the invariant the
single-sink path trivially satisfied and the fanout must preserve.

### 6.3 `GetAll` semantics for v1

Returns the **primary (first) sink's** view. If the primary errors, the
error propagates — matching today's behavior where a failing Atlas
`GetAll` aborts or logs per provider (k8s.go:287-289, consul.go:324-327).
Secondary sinks' `GetAll` is not called in v1. The failure window is
§3.3's, exercised by soak scenario (d) (§8.5).

### 6.4 Retry seam

`PushTo(name string, triggerTime int64, instances []*instance.Instance) error`
pushes to exactly one named sink (unknown name → error); used only by
`UnsyncedService.syncOnce`. `Sinks() []string` exposes names for the
fallback path and metrics.

### 6.5 Wiring

`internal/server.go:startProviders` (server.go:258-332) constructs
`atlasSink := registry` (server.go:276) and, when `cfg.NacosAddr != ""`,
`nacosSink` (F5); `NewResourceWorker` receives the fanout. With
`--nacos-addr` empty the fanout holds exactly one sink — behavior identical
to today (single error surface; `FanoutError` degenerates to nil-or-one).

### 6.6 TDD test list (pkg/worker/fanout_test.go)

| Test name | Scenario |
| --- | --- |
| `TestNewFanoutSinkRejectsEmptyAndDuplicateNames` | constructor errors |
| `TestFanoutPushAllSinksInOrder` | call order == declaration order |
| `TestFanoutPushAggregatesPerSinkErrors` | A ok, B fails → `FanoutError` with exactly one failure naming B; `FailedSinks()==["nacos"]` |
| `TestFanoutPushAllSuccessReturnsNil` | both ok → nil error, both called |
| `TestFanoutPushAllFanoutsAllDespiteFirstFailure` | A fails, B still receives its push (no short-circuit) |
| `TestFanoutGetAllReturnsPrimaryView` | primary list returned verbatim; secondary `GetAll` never called |
| `TestFanoutGetAllPrimaryErrorPropagates` | primary error surfaces unchanged |
| `TestFanoutPushToTargetsSingleSink` | only the named sink sees the push; unknown name errors |
| `TestFanoutSingleSinkDegeneratesToPlainBehavior` | one sink → errors identical to pre-fanout worker behavior (regression net for §6.5) |

## 7. Phase F5 — Nacos Adapter (`pkg/nacos`)

Goal: a second `ports.InstanceSink` over the Nacos v1 OpenAPI, following
the `pkg/discoverycenter` precedent (adapter package, constructors return
errors, injected logger/notifier, own transport, no panics —
ddd-architecture.md §4(c)).

### 7.1 Verified Nacos facts (F0)

- Image `nacos/nacos-server:v2.1.0`, standalone, host port **18848** (host
  8848 is occupied by a colima SSH tunnel — documented trap, §8.2).
- v1 OpenAPI verified: `POST /nacos/v1/ns/instance` (register),
  `DELETE /nacos/v1/ns/instance` (deregister),
  `GET /nacos/v1/ns/instance/list` (per-service query),
  `GET /nacos/v1/ns/service/list` (paginated service names).
- Composite instance id format: `ip#port#cluster#group@@service`, e.g.
  `10.0.0.1#8080#DEFAULT#DEFAULT_GROUP@@spike-app`.
- **Persistent instances (`ephemeral=false`) are the right choice for a
  sink that owns lifecycle.** Rationale: ephemeral instances are heartbeat
  contracts — the registrant must renew them and Nacos auto-expires them
  when heartbeats stop (~15-30s). spotter is a *replicator* asserting the
  desired state of other people's instances, not a service instance: if
  spotter's process or connection hiccups, ephemeral registration would
  mass-expire live instances — the opposite of the sink's contract.
  Persistent instances change only when we say so and our DELETE is
  authoritative. The cost — drift persists if spotter dies — is exactly
  what the retry queue and the full-push reconcile (§7.4) bound.

### 7.2 Decision D4 — official SDK/facade is mandatory; HTTP is compatibility-only

**Current decision.** `pkg/nacos` must depend on a `NacosTransport` and an
official `nacos-sdk-go/v2` facade for naming operations: register,
deregister, service list, query, subscribe and batch. Admin/Catalog/prune and
readiness must use an official Admin/Maintainer SDK when the target Nacos
version provides one, or a separately versioned and audited facade with an
explicit, time-bounded `ID-NACOS-SDK-MANDATE` exception. Business code must
not construct Nacos URLs, HTTP methods or query forms directly.

The current `net/http` v1 client is retained only as a migration-period
compatibility/rollback adapter. It must be isolated behind the transport
interface, explicitly selected (for example `http-compat`), marked
`NON_PRODUCTION_COMPAT`, and covered by comparison tests. It is not a valid
final production implementation and must not be used to claim SDK compliance.

The SDK migration must preserve the F5 persistent-instance contract
(`ephemeral=false`) unless a separately approved compatibility decision proves
that changing lifecycle semantics is safe. SDK gRPC support, batch behavior,
auth/TLS, namespace/group, reconnect/redo/cache, catalog visibility and
final-state consistency all require the B3 test gate; SDK compilation alone is
not evidence of server compatibility.

### 7.3 Field mapping (domain → Nacos)

| `instance.Instance` field | Nacos target | Notes |
| --- | --- | --- |
| `AppCode` | `serviceName` | user requirement; group `DEFAULT_GROUP` fixed |
| `Ip` | `ip` | |
| `Ports[0].Port` (else `0`) | `port` | deterministic derivation — the same rule on register and deregister so the composite id round-trips; port-less instances register with port 0 (legal for persistent instances, no health check attached) |
| `Provider` (`"k8s"` / `"ecs"`) | `clusterName` | **collision policy**: same app-code from both providers coexists in one service under distinct clusters → distinct composite ids `ip#port#k8s#...` vs `ip#port#ecs#...` |
| `Enabled` (and `Status != offline`) | `enabled` | |
| fixed | `ephemeral=false`, `groupName=DEFAULT_GROUP`, `namespaceId=public` | persistent rationale §7.1 |
| `InstanceId`, `EnvType`, `EnvGroup`, `Reversion`, `Status`, `State`, `Idc`, `Cpu`, `Version` | `metadata` map (JSON-encoded form param) | round-trip fidelity for `GetAll`; consul's diff compares `Idc`/`Cpu` (consul.go:357-361), so `idc`/`cpu` are carried for a future per-sink compare |

Push-side policy by `Status`: **online (1)** → upsert register, `enabled`
per `Enabled`; **unhealthy (2)** → upsert register, `enabled=false`
(mirrors the consul path's "old instance, mark not-ready" semantics,
consul.go:394-396, which pushes Status=2 rather than deleting); **offline
(3)** → **deregister** — DELETE by composite id (ip, port, clusterName,
groupName, serviceName), matching k8s offline markers k8s.go:350-352 and
consul incremental deletes consul.go:226-229; **unknown (0)** → skip
defensively (upstream filters already reject it, rules.go:99-101).

`GetAll(statuses, provider)` reconstruction: list all services
(`service/list` pagination) → per service `instance/list` → keep instances
whose `clusterName == provider` → rebuild `*instance.Instance` from
`serviceName` (AppCode), `clusterName` (Provider/Cluster), `ip`, `port`
(first `PortInfo`), and the metadata map (`instanceId`, `envType`,
`envGroup`, `reversion`, `status`, `state`, `idc`, `cpu`, `version`);
filter by the requested `statuses` (status from metadata, fallback
`enabled`→1 else 2). Fidelity limits (Ports beyond the first, Labels,
Image) are documented as acceptable: v1 comparisons never run against the
Nacos view (§3.3).

### 7.4 Push vs PushAll (reconcile semantics)

`Push` (incremental) applies the per-instance policy above — upsert or
deregister. `PushAll` (full), for every `(serviceName, clusterName)` pair
present in the pushed set: upsert all pushed instances, then **prune** —
list Nacos's current instances for that service+cluster and DELETE every
remote instance not in the pushed set. Pruning is scoped to clusters
present in the data, so a k8s full push never touches ecs-cluster
instances (the consul provider owns those) — per-provider ownership is
preserved end-to-end. This prune is the Nacos drift self-heal that §3.3
and §6.3 refer to: out-of-band drift is corrected at every full push, not
only when a local change happens to touch the instance.

**SyncAll trigger (change scoped in by this initiative; the prune above is
dead code without it).** `PushAll`'s only production caller is the worker's
`OperateTypeSyncAll` handler (worker.go:67-75), and no production code
emits that event: the one full-list emitter that existed
(`flushInstances`, k8s.go:253-269) is dead (ddd-architecture.md §4(g)), so
the periodic path only ever emits per-instance `Sync` events. F5 therefore
revives the dormant path that ddd-architecture.md §4(d) deliberately kept:
after `CompareAndFlush` completes, `ProcessIntervalFullPush` in both
providers (k8s.go:408-428, consul.go:283-303) emits one
`worker.Event{Trigger: now, Data: <provider>.GetAll(),
Operate: OperateTypeSyncAll}` through the existing `worker.Handle` seam.
Consequences, stated: the prune sweep runs every `--push-interval` (that
is the §8.5 reconciliation bound); Atlas also receives one
`SynAllInstance` RPC per tick (client.go:118-130 — the RPC that has
existed for exactly this and was never invoked in production) — a
deliberate, documented behavior delta (§7.6); a failed full sync queues
per-instance retries for the failed sinks only, so queue depth can
temporarily equal the pushed set size (visible in §5.3's per-sink gauge);
`CompareAndFlush` and its per-instance policies are untouched (ddd §4(j)).
Rejected alternative: a periodic `PushAll` owned by the worker/fanout —
neither holds the full instance list (providers do, via `GetAll`), and the
event path is the existing seam.

### 7.5 Mock: `internal/testkit/nacosmock`

An `httptest` server like `consulmock` (server.go:26-131), not a real
container (the real container is the soak tier's job, §8):

- `Start() *Server` + `URL() string` (loopback); state: instances keyed by
  composite id, register is upsert (same id twice → one entry, so the
  no-duplicate-ids invariant is assertable).
- Endpoints: `POST /nacos/v1/ns/instance` (form params: serviceName, ip,
  port, clusterName, groupName, ephemeral, enabled, metadata JSON),
  `DELETE /nacos/v1/ns/instance`, `GET /nacos/v1/ns/instance/list`
  (`{"count":N,"hosts":[...]}` shapes matching v2.1.0, including
  `instanceId`, `healthy`, `enabled`, `metadata`, `clusterName`),
  `GET /nacos/v1/ns/service/list` with real pagination semantics.
- Drift/observation control (the consulmock `AdvanceIndex` equivalent):
  `SetInstances(...)` tampers state out-of-band (drift or out-of-band
  deletion), `Instances()` snapshots state, `Requests()` records calls
  (method, path, form), `SetStatus(code)` injects HTTP failures for
  retry-path tests, `SetDelay(d)` for timeout tests.

### 7.6 Configuration: additive `--nacos-addr`

`cmd/adapter.go` registers `--nacos-addr` (default `""`; help: "the Nacos
OpenAPI address, e.g. 127.0.0.1:18848; empty disables the Nacos sink")
alongside the existing block (cmd/adapter.go:113-122 — those eight flags,
shorthands and defaults untouched, per ddd-architecture.md §4(i).1).
`infraconfig.Flags` gains `NacosAddr string` and `Config` gains `NacosAddr`
(config.go:137-185 / 191-228, additive; resolution:
`strOrDefault(flags.NacosAddr, "")`, no preset involvement).
`internal/server.go:startProviders` builds the Nacos sink iff non-empty
(§6.5). **Invariant: `--nacos-addr` empty ⇒ binary behavior identical to
pre-F5, with exactly one deliberate, documented delta** — the full-push
tick now also emits one `OperateTypeSyncAll` event (§7.4), so Atlas
receives one idempotent `SynAllInstance` RPC per `--push-interval`: the
revival of the dormant path (ddd-architecture.md §4(d)). Guarded by
§6.6's degenerate-behavior test. If even that delta is unacceptable, the
recorded fallback is gating the emission on `cfg.NacosAddr != ""` in
`InitializeProviders` (server.go:432-468) — rejected as primary because it
threads sink-awareness into provider wiring, violating the policy-blind
fan-out principle (§3.1).

### 7.7 TDD test list

`internal/testkit/nacosmock` (server_test.go):

| Test name | Scenario |
| --- | --- |
| `TestRegisterUpsertsWithoutDuplicate` / `TestDeregisterRemovesInstance` | same composite id twice → one entry; DELETE removes exactly the addressed id |
| `TestListInstancesReturnsServiceInstances` / `TestServiceListPaginates` | per-service hosts with metadata/cluster round-trip; pageSize honored, short page ends iteration |
| `TestRequestsRecording` / `TestFailureInjection` | `Requests()` snapshots method/path/form; `SetStatus(500)` / `SetDelay` produce client-observable failures |

`pkg/nacos` blackbox (client + sink, against nacosmock):

| Test name | Scenario |
| --- | --- |
| `TestClientRegisterSendsPersistentFormParams` / `TestClientDeregisterUsesCompositeIdParams` | form contains `ephemeral=false`, group, cluster, metadata JSON; DELETE carries ip/port/cluster/group/serviceName |
| `TestClientListInstancesParsesHosts` / `TestClientListServicesPaginatesAll` / `TestClientTimeoutReturnsError` | hosts → typed instances; 250 services collected across pages; delayed mock → error within client timeout |
| `TestSinkPushOnlineInstanceRegisters` | full field mapping asserted from mock state (§7.3 table) |
| `TestSinkPushOfflineInstanceDeregisters` / `TestSinkPushUnhealthyInstanceUpsertsDisabled` | Status=3 → DELETE observed, no register; Status=2 → present with enabled=false |
| `TestSinkPushAllPrunesStaleRemoteInstances` / `TestSinkPushAllUpsertsAllPushed` | remote extra instance in same service+cluster deleted, other cluster untouched; all pushed instances present after PushAll |
| `TestSinkMapsProviderToClusterCoexist` | same AppCode, k8s + ecs → both present, distinct composite ids |
| `TestSinkGetAllReconstructsInstances` / `TestSinkGetAllFiltersProviderCluster` | metadata round-trip (instanceId/envType/envGroup/reversion/status/state); clusterName and status filters honored |
| `TestSinkErrorPropagates` / `TestSinkPortlessInstanceUsesPortZero` | mock 500 → Push returns error (feeds the F3 retry integration); no Ports → port 0, register then deregister address the same id |
| `TestSinkSatisfiesInstanceSink` | `var _ ports.InstanceSink = (*NacosSink)(nil)` (assertion also in the prod file) |

## 8. Phase F6 — Local Full-Stack E2E + 1-Hour Soak

### 8.1 Decision D5 — harness shape

A `make test-soak` target, **not** part of `test-all` (aggregate budget
~3 min, Makefile; the soak is 1h wall clock + stack boot), documented next
to the target. The stack runs via docker compose
(`tests/e2e/soak/docker-compose.yml`) on the colima docker engine. The
harness is a build-tag-guarded Go test (`tests/e2e/soak/soak_test.go`,
`//go:build soak`) so churn driving and assertions stay in Go
(`net/http` + `os/exec` kubectl), reusing the repo's assertion style.
`make test-soak` = compose up + health-wait + build + the tagged test —
which owns the embedded etcd, the Atlas stand-in, the spotter binary's
exec/kill/restart and all dynamic teardown — then compose down and the
summary. The spotter **binary** is the system under test (not in-process
wiring): the soak must exercise flags, config resolution, election,
dial-retry (server.go:357-385) and process restart exactly as shipped,
which is why the tagged test execs the binary as a child process rather
than wiring it in-process.

### 8.2 Verified environment facts and prerequisites (document, do not re-derive)

- **colima resource floor**: 6 vCPU / ~10 GiB / `--vz-rosetta`
  (`colima start --cpu 6 --memory 10 --vz-rosetta`); docker Server 27.4.0.
  On a 4-CPU VM the Nacos 2.1.0 JVM hung >10 min — a hard requirement.
- **Nacos JVM env**: `MODE=standalone`, `JVM_XMS=512m`, `JVM_XMX=512m`,
  `JVM_XMN=256m`; readiness poll against
  `/nacos/v1/console/health/readiness`, ~3 min budget.
- **The 8848 trap**: host 8848 is already bound by a colima SSH tunnel
  from earlier work; mapping `8848:8848` fails or hijacks silently.
  Therefore host **18848**→8848 (HTTP/console/OpenAPI), **19848**→9848
  (gRPC), **19849**→9849. The harness pre-flights `lsof -i :18848 -i
  :19848` and fails fast pointing at this section.
- **k3s**: `rancher/k3s:v1.28.8-k3s1` container, `--privileged`, 6443
  published; kubeconfig via `docker exec <k3s> cat
  /etc/rancher/k3s/k3s.yaml` into the harness workdir (server reachable at
  `127.0.0.1:6443`; F0 verified host kubectl works). Pod recipe (F0
  verified): labels `app-code=spike-app` + `K8S_CLUSTER_TYPE=test`
  (underscore key, exactly as the converter reads it at
  conversion.go:255) produce an assigned pod IP and a convertible
  instance (`InstanceId` = pod name,
  conversion.go:104; AppCode from labels, conversion.go:62).
- **consul**: `hashicorp/consul:1.15`, host **18500**→8500. Churn
  registrations must use the full schema the converter requires
  (convertion.go:13-20 meta keys): `Node{Node, Address}` +
  `Service{ID, Service, Port, Tags, Meta{appCode, envType, envGroup,
  instanceId, version, ports-JSON}}` — otherwise `InitInstanceFilters`
  (rules.go:78-105) drops the instance and the soak sees nothing.
- **etcd for real election — decision**: **primary: in-process embedded
  etcd** (the `internal/testkit/etcdmock` machinery proven by
  tests/e2e/elector_e2e_test.go:22-101: boot ~0.6s, loopback dynamic
  ports). The soak boots it in-process and passes its client URL to the
  binary via `--etcd-endpoints` — zero image pulls, no port conflicts,
  same election code path. **Fallback: `bitnami/etcd` (3.5.x)** container
  on host 12379→2379 with `ETCD_ALLOW_NONE_AUTHENTICATION=yes` (chosen
  over `gcr.io/etcd-development/etcd` for smaller multi-arch images; the
  gcr image is the second fallback). **Last resort:
  `--leader-elect=false`**, which reduces scenario (f) to a
  restart-convergence check (documented in the summary). If `etcdmock`
  does not export its client URL, add an accessor — a testkit-only change.

### 8.3 Stack topology and port table

| Component | Image / source | Host port | Notes |
| --- | --- | --- | --- |
| nacos | nacos/nacos-server:v2.1.0 | 18848 (→8848), 19848 (→9848), 19849 (→9849) | JVM env §8.2; `restart: on-failure` |
| consul | hashicorp/consul:1.15 | 18500 (→8500) | dev server |
| k3s | rancher/k3s:v1.28.8-k3s1 | 6443 | privileged, readyz healthcheck |
| etcd | in-process embedded (etcdmock) | dynamic loopback | fallback bitnami/etcd: 12379 |
| Atlas stand-in | in-process Go gRPC server | 15051 | see below |
| spotter | `go build -o build/spotter .` | metrics 18090 | child process of the harness |

**Atlas stand-in (required, easy to miss):** the binary's startup
*requires* a dialable gRPC address — `dialDiscoveryWithRetry` fails the
provider start after 3 attempts (server.go:271-275, 357-385). The local
stack has no Atlas, and `--disable-worker` (registry.go:46-49) would turn
the pushes we want to observe into logs. So the harness runs a small
in-process gRPC server speaking the exact JSON codec the production client
uses (`service.v2.InstanceService` — the same surface as
`internal/testkit/discoverymock`, discoverymock/server.go:50-62, but on a
TCP listener instead of bufconn). This is not a mock of something else: it
is the documented wire contract (v2.go:9-16) exercised by the real client,
and it lets the soak assert that the fanout does not regress Atlas pushes
while Nacos churns. Implementation: a thin `net.Listen` re-export of the
discoverymock service implementation (testkit change if needed).

**Production dial-path fix (F6 change, three lines).** `discoverycenter.Dial`'s
zero-option branch sets only `grpc.WithInsecure()` + `grpc.WithBlock()`
(client.go:50-65) — no codec — so the production client invokes with the
default proto codec, which rejects the plain mirror structs client-side
(the documented v2.go:9-16 limitation): the production dial path has never
been wire-functional. The stand-in closes the loop only if the client can
actually talk to it, so F6 also fixes the client: the zero-option branch
gains `grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec))` — the same
codec discoverymock uses (discoverymock/server.go:166-178); the ~10-line
codec implementation moves to a shared location or is duplicated in
pkg/discoverycenter. A `--grpc-codec` flag was considered and rejected: the
plain structs admit exactly one codec, so a flag is config surface with one
legal value, and the existing-flags invariant (§7.6) stays intact. Scope
note: this is a fix of a documented non-functional path, not a behavior
change to a working one; whether a production Atlas accepts JSON-encoded
service.v2 frames is a server-side deployment property — the fix aligns
the client with its own documented contract and is verified against the
stand-in, while real proto marshaling stays the follow-up (§10.5).

### 8.4 Soak invocation (concrete)

```
make test-soak
  # 1. colima preflight (cpus>=6, mem>=9G, ports free)
  # 2. docker compose -f tests/e2e/soak/docker-compose.yml up -d
  #    (nacos, consul, k3s only; etcd and the Atlas stand-in are NOT in
  #    compose — the tagged test below owns them in-process)
  # 3. health-wait: nacos readiness (<=3min), consul /v1/status/leader,
  #    k3s readyz via kubectl; kubeconfig extracted to $WORK/kubeconfig
  # 4. go build -o build/spotter .
  # 5. go test -tags=soak -run '^TestSoakLocalStack$' -timeout 90m ./tests/e2e/soak/...
  #    The tagged test owns everything dynamic, in this order: boot the
  #    embedded etcd, start the Atlas stand-in listener, exec the spotter
  #    binary as a child with the flags below, run the churn + scenario
  #    window (including the kill/restart scenarios), then tear all of it
  #    down via defers (binary first, then stand-in, then embedded etcd).
  # 6. compose down; print + persist the soak summary (§8.6)

# Flags the tagged test passes when it execs the binary (documentation of
# the child invocation; the test constructs and owns it):
  ./build/spotter adapter \
      --env test \
      --providers k8s,ecs \
      --kubeconfig     $WORK/kubeconfig \
      --consul-addr    127.0.0.1:18500 \
      --etcd-endpoints <embedded-etcd-url> \
      --nacos-addr     127.0.0.1:18848 \
      --grpc-addr      127.0.0.1:15051 \
      --push-interval  120 \
      --metrics-addr   :18090 \
      --leader-elect
```

`--push-interval 120` makes the full-push reconciliation bound (2 min)
observable inside the soak instead of the shipped 21600s default; it bounds
both the `CompareAndFlush` cadence and the §7.4 SyncAll prune. All
other flags shown exist today (cmd/adapter.go:113-122) except three
additive local-source flags F6 introduces: `--kubeconfig` (comma list,
overrides the preset's `KubeConfigPath` used at server.go:443),
`--consul-addr` (comma list, overrides the preset used at
server.go:451-455), and `--etcd-endpoints` (comma list). The etcd flag
carries one extra resolution rule, required to avoid a startup death:
every env preset has NON-empty TLS paths (config.go:44-46, 58-60, 76-78),
which `NewElectorWithDeps` receives (server.go:96-98) — against a
plain-HTTP embedded etcd a TLS dial fails and the binary never becomes
leader. So a non-empty `--etcd-endpoints` override resolves
`CertFile`/`KeyFile`/`CAFile` to empty (insecure local mode); an empty
flag keeps the preset values verbatim, TLS and all. All three flags are
additive with empty defaults = preset behavior exactly as today; the §7.6
"existing flags untouched" invariant is preserved. Rejected alternative: a
`local` env preset — it would hardcode machine-local ports into the
shipped config package (config.go:41-93) that every environment carries
forever; per-invocation flags are explicit and test-only in spirit.

### 8.5 The 1-hour window: churn, assertions, edge scenarios

**Continuous churn (the whole hour, concurrently with scenarios):** k8s —
every ~20s rotate `spike-app` pods (`kubectl create` / `delete` / `scale
--replicas=N` against labeled pods `app-code=spike-app` +
`K8S_CLUSTER_TYPE=test` (§8.2), sizes oscillating 1..10); consul — every
~20s register/deregister services with the full meta schema (§8.2) under 2
stable app-codes plus a rotating one.

**Standing assertion loop (every 10s):**

- For every app-code seen so far: fetch
  `GET /nacos/v1/ns/instance/list?serviceName=<code>&groupName=DEFAULT_GROUP`
  and compare the per-cluster instance multisets (k8s cluster vs live
  pods, ecs cluster vs live consul entries) against expected state derived
  from kubectl/consul directly.
- Reconciliation bounds: incremental divergence heals ≤30s; divergence
  whose heal path is the full push heals ≤ `--push-interval` (120s) — that
  tick is `ProcessIntervalFullPush` running `CompareAndFlush` AND emitting
  the §7.4 `OperateTypeSyncAll` event whose Nacos `PushAll` prune sweeps
  every cluster it owns. Violations are recorded as divergences (not
  immediate failures) so the
  soak reports a complete picture; final convergence is a hard criterion.
- No duplicate composite instance ids per (service, cluster) — ever.
  Atlas stand-in call log monotonically grows (fanout not starved). Each
  check writes one timestamped line to the rolling soak log
  (`build/soak-YYYYMMDD-HHMM.log`): time, checked services, divergence
  count, worst divergence age.

**Extreme edge scenarios (each an asserted case inside the window):**

| # | Scenario | Mechanics | Assertion |
| --- | --- | --- | --- |
| (a) | Pod restart storm | delete 50% of spike-app pods at once | every removed pod's Nacos instance deregisters within the incremental bound; survivors untouched; no duplicates |
| (b) | Service with 0 healthy instances | scale the second app-code to 0; deregister all entries of one consul service | service listed with 0 instances; no stale instances past bound |
| (c) | Same app-code from BOTH providers | `spike-app` pods AND a consul service with appCode `spike-app` | both coexist under clusters `k8s` and `ecs` in one service; distinct composite ids; each provider's prune never deletes the other cluster's instances (§7.4) |
| (d) | Nacos restart mid-soak | `docker restart` the nacos container (restart, not recreate — the embedded derby store survives the container filesystem) | push errors land in the per-sink retry queue (5s cadence) AND the next `--push-interval` tick emits the §7.4 SyncAll event whose Nacos `PushAll` prune restores the full desired state; full convergence within the full-push bound; Atlas push cadence (including the tick's SynAllInstance) unaffected during the outage |
| (e) | Consul outage 60s | `docker stop consul` for 60s, then start | during the outage k8s-sourced services keep converging; consul errors contained; after start consul-sourced state converges within bound |
| (f) | spotter restart | kill the binary; etcd lease expires; harness restarts it | re-election observed on the embedded etcd campaign; startup `CompareAndFlush` (k8s.go:101 / consul.go:77) heals the downtime gap; convergence within bound |
| (g) | Rapid flap | register/deregister the same consul service 20x back-to-back | final state matches the last op; keep-highest-Reversion retry never pushes the stale version (unsynced_service.go:53-59 generalized per §5.2); no duplicates |
| (h) | Scale to 100 replicas | `kubectl scale --replicas=100` | all 100 instances present within a generous batch bound (≤300s documented; events serialize through the worker pool, k8s.go:125-128); no duplicates; soak log records convergence time |

**Success criteria (the whole soak):** every observed divergence heals
within its bound; zero duplicate composite ids; scenarios (a)–(h) each
report pass; final state converges (the final full comparison loop returns
zero divergence); the Atlas stand-in received pushes throughout.

### 8.6 Evidence policy

Raw 1h log stays local (`build/soak-*.log`; `build/` is already the smoke
binary's artifact dir and is not committed). Committed: a soak **summary**
written by the harness to `tests/e2e/soak/results/<date>-local.md` — run
window, stack versions, flags, per-scenario pass/fail, checks count,
divergence count, max divergence age, max heal time per scenario, spotter
restarts, and the final convergence table; key metrics only, no raw lines.
Rationale: 1h of 10-second-granularity log lines is review noise; the
summary is the reviewable artifact, the log the debugging artifact —
mirroring how the elector e2e reports timings
(tests/e2e/elector_e2e_test.go:101).

## 9. Sequencing, Effort, Risk

### 9.1 Sequence and effort

| Phase | Depends on | Effort | Landmark |
| --- | --- | --- | --- |
| F2 type unification + port wiring | — | M (1–2 days) | port assertion compiles; matrix green; worker_fack deleted |
| F3 per-sink retry state | F2 | M (1–2 days, TDD first) | §5.4 tests green against the two-sink stub |
| F4 FanoutSink | F3 (shared error contract) | S (0.5–1 day) | §6.6 tests; §5.4 tests re-run green against the real fanout |
| F5 Nacos adapter | F2 (port); own tests independent of F3/F4 | L (3–4 days incl. nacosmock) | `pkg/nacos` + `--nacos-addr` wired through server.go |
| F6 e2e + soak | F4 + F5 + local-source flags | L (4–5 days incl. one full 1h run + fixes) | `make test-soak` green; summary committed |

F2→F3→F4→F5→F6 is the strict landing order; F5's mock and adapter unit
tests can be developed in parallel with F3/F4 since they only need F2. F3
and F4 may land as an interleaved PR pair sharing the §5.1 types. Each
phase ends green on `make test-unit test-blackbox test-e2e` (F5 adds
`pkg/nacos` to `TEST_PKGS`; F6 adds nothing to `test-all`).

### 9.2 Risk table

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Nacos 2.1.0 JVM flaky on ARM/colima | high | soak blocked | resource floor documented (§8.2); pinned JVM env; readiness poll 3 min; compose `restart: on-failure`; if persistently flaky, fall back to nacosmock-driven e2e for CI-shaped runs and keep the real-container run as a documented manual gate |
| Host port conflicts (the 8848 tunnel trap) | high | silent misrouting | 18848/19848/19849/18500/6443/12379/15051 remap table; pre-flight `lsof` fails fast (§8.2) |
| 1h wall clock | certain | iteration cost | excluded from `test-all`; scenarios individually runnable via `-run 'TestSoakLocalStack/scenario'`; churn knobs configurable via env for shakedown runs (the real run stays 1h) |
| k3s container weight (privileged, slow boot, kubeconfig quirks) | medium | soak blocked on the k8s leg | healthcheck-gated start; kubeconfig extraction verified in F0; documented consul-only degraded mode (scenarios c/e/g/h partially cover without k8s), decision recorded in the summary if used |
| etcd image pull fails | low | election scenarios degrade | embedded-etcd primary needs no pull (§8.2); bitnami→gcr fallback; final `--leader-elect=false` fallback documented |
| Persistent instances + derby durability across restart | medium | scenario (d) semantics | use `docker restart` (container filesystem survives); scenario (d) asserts the post-restart reconcile either way — if derby does not survive, the §7.4 SyncAll prune at the next tick is the asserted heal path |
| Metrics label change breaks dashboards | certain (documented) | operational | §5.3: accepted breaking change, one label filter; called out in the F3 commit message |
| Wire-format regression from F2 aliases | low | Atlas path broken | compile-time-only change; discoverymock JSON-codec e2e + consul pipeline e2e are the guards (§4.3) |
| Compose/colima version drift across machines | medium | non-reproducibility | image tags pinned in the compose file; colima prereqs documented (§8.2) and checked by preflight |
| Per-sink retry starvation / lock contention | medium | retry latency | §5.2 lock-discipline fix + `TestAddNotBlockedByInflightRetry` |

## 10. Explicit Non-Goals

1. **Per-sink comparison** — `CompareAndFlush` compares against the
   primary sink's view only (§3.3); per-sink compare loops are a follow-up
   the `FanoutSink`/`retryKey` shapes are designed to admit.
2. **Nacos auth** (username/password, OpenAPI tokens) — historical F5 local
   scope only; current B2/B3 requires SDK auth/TLS coverage before production.
3. **Non-default Nacos namespaces** (`namespaceId` stays `public`) — historical
   F5 local scope only; current B2/B3 requires namespace/group coverage before
   production.
4. **Nacos 2.x gRPC protocol / official Go SDK** — no longer a non-goal;
   mandatory migration and compatibility gate is defined by D4 (§7.2) and
   remediation plan §10.
5. **Production Atlas wire format** — real proto marshaling remains the
   documented follow-up of v2.go:9-16; F2's aliases change nothing about
   it.
6. **Nacos health-check registration** (TCP/HTTP checks) — instance
   liveness stays spotter's responsibility, consistent with the persistent
   instance rationale (§7.1).
7. **Unifying the k8s/consul diff policies** — forbidden by
   ddd-architecture.md §4(j); the sink fan-out is policy-blind.
8. **Migrating the remaining provider files off the `v2` aliases** — F2
   deliberately leaves `pkg/providers/**` on the alias import (§4.1); their
   cleanup is separate, opportunistic work.
9. **Graceful shutdown** — still out of scope (ddd-architecture.md §4(h));
   the soak kills the binary hard, by design.
10. **Soak in CI** — the 1h run is a local, evidence-producing activity
    (§8.6); CI keeps the offline tiers of the Makefile matrix.

---

*End of plan. File:line citations verified against `refactor/all` at HEAD
`b976356`. F0 environment facts verified by the lead on the local
colima/docker stack (§8.2).*
