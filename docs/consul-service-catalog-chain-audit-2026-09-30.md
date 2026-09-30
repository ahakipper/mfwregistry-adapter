# Consul Service Catalog → Nacos Chain Audit and P0–P2 Optimization Plan

Date: 2026-09-30  
Branch: `refactor/all`  
Audited HEAD: `e1ae3b5`

## Current execution status — 2026-09-30

Stage 1 correctness work is implemented in the working tree and has passed
the independent code review and the Consul package race suite. The provider
now distinguishes source errors, partial catalog reads, healthy empty
catalogs, and healthy non-empty catalogs. Source errors and partial reads
retain the previous cache and cannot authorize a destructive Nacos operation.
Healthy empty state requires three independent confirmations; a provider-owned
5-second retry timer bounds the cleanup attempt without changing the global
6-hour full-push interval. The timer is single-instance, stopped on recovery
or shutdown, and guarded by the provider lock plus lifecycle state.

The remaining P1/P2 work is still open: Consul-to-Nacos watch latency
percentiles, real Consul plus Nacos 3 qualification, multiple logical Consul
sources, source metrics, and Consul connection security settings. Blocking
query edge handling is closed in the working tree; this status is still an
implementation checkpoint, not a production-readiness claim.

Stage 2 has now closed the first two items in that list. Equal `Reversion`
updates use the complete Spotter canonical payload, so same-index changes to
labels, ports, images, hostname, and other owned fields are delivered while a
lower revision remains ignored. The Consul health contract is explicitly
`passingOnly=true`: unhealthy entries leave the desired catalog and are
removed through the normal deletion path; a completely unhealthy catalog is
still protected by the Stage 1 healthy-empty confirmation gate. A legacy Atlas
snapshot that omits canonical source fields may receive one corrective push
to republish the complete projection.

The blocking-query edge cases are now hardened in the working tree. The watch
tracks the last valid positive `X-Consul-Index` separately from the next
`WaitIndex`: a missing or zero query meta never emits a change and floors the
next request at index `1`; a rollback emits exactly one change token, resets
`WaitIndex` to `0`, and immediately re-establishes a fresh baseline. The
baseline response is not emitted a second time, and unchanged responses,
timeouts, and errors continue through the existing `periodicCheckTime` wait so
rapid unchanged responses cannot form a tight loop. A clock-injected token
bucket allows two rapid change deliveries, then requires a complete 15-second
refill interval; rollback baseline retries bypass this delivery limiter so the
reset remains immediate. Focused and race tests cover zero, rollback,
unchanged timeout/error, burst refill, cancellation, and rapid changes.

Stage 4 closes the provider identity and revision-ordering hazards found by
the independent chain confirmation. The Consul cache and full-push snapshot
now preserve a higher cached `Reversion` when a later read is stale. SourceKey
contains the logical source, stable node identity, and service ID, while a
conservative source/provider/application/IP fallback keeps old explicit
SourceKey records matchable during migration. Recovery after a confirmed empty
state emits a trusted complete snapshot after releasing the provider lock, so
an equal-revision sink tombstone can be healed immediately. The remaining
evidence gap is end-to-end DefaultWorker/Fanout/orderedSink recovery coverage
and a deliberate transient-omission test; legitimate source omission remains
the deletion signal after a complete, non-partial read.

## Executive assessment

The refactor branch already contains a real Consul provider and a DDD-style
provider port. The current chain is:

```text
Consul blocking health-state query
  -> 50 ms debounce
  -> Catalog.Services
  -> Health.Service(service, tag=microservice, passingOnly=true)
  -> ServiceEntry conversion
  -> Consul cache keyed by SourceKey/IdentityKey
  -> add/update/delete diff
  -> worker event / bounded overflow queue
  -> FanoutSink / ordered sink gate
  -> Nacos SDK register/deregister or PushAll prune
```

The core implementation is reusable, but Consul-to-Nacos production readiness
is not yet proved. The current test suite proves most local mechanics and one
loopback fan-out path; it does not yet prove end-to-end latency percentiles,
all health transitions, complete source disappearance, or a real Nacos 3
Consul-backed run.

There is no `main` branch ref in this checkout. The historical baseline is
`origin/master`, which also contains an older Consul provider. `refactor/all`
contains the more complete dependency-injected provider, monitor, client
factory, cache diff, Nacos reconcile, source identity, and E2E changes.

## Current implementation map

| Layer | Current implementation | Assessment |
| --- | --- | --- |
| Provider abstraction | `pkg/providers/iface.go` exposes `Run`, `CompareAndFlush`, `GetAll` | Reusable and already wired |
| Consul client | `ClientFactorySimple` probes leader and fails over configured addresses | Good local failover coverage; ACL/TLS config is incomplete |
| Watch | Blocking `/v1/health/state/any`, 5 s wait, 50 ms debounce, burst-2/15 s rapid-change limiter | Focused and race-tested; nil/zero metadata, rollback reset, timeout/error, and rapid-change behavior are pinned |
| Catalog read | `Catalog.Services` then `Health.Service(..., passingOnly=true)` | Healthy-only semantics need an explicit contract |
| Conversion | Service metadata → `Instance`; `ModifyIndex` → `Reversion`; source ID propagation exists | Good shape; malformed metadata is skipped and needs metrics |
| Cache/diff | Source-aware `IdentityKey`; add/update/delete event generation | Same-name source identities are covered; health/equal-revision behavior needs more proof |
| Worker/Fanout | Bounded pool, overflow queue, ordered sink gate, full retry metadata | Reusable and tested |
| Nacos | Source-qualified cluster name, SDK persistent writes, batch/prune/reconcile | Consul path inherits Nacos correctness; live Consul + Nacos evidence is missing |

## Findings

### P0 — no confirmed P0, with stop-ship conditions

No unconditional P0 is confirmed by the current code. The following conditions
must remain stop-ship conditions during implementation:

- a Consul read error must never be converted into an empty desired state;
- an incomplete catalog result must never authorize destructive Nacos prune;
- an end-to-end source mutation must not be reported as successful without a
  Nacos observation boundary;
- the same source identity must not be registered under two incompatible wire
  scopes during a migration.

### P1 findings

#### P1-1 — Complete source disappearance is delayed and semantically split

`GetAll` returns an empty slice after a successful Consul catalog read. The
incremental `syncInstance` path treats an empty result as an error and keeps the
old cache. `CompareAndFlush` also skips the comparison when `len(all) == 0`.
Only the periodic full path advances an empty confirmation counter, and Nacos
cleanup requires repeated confirmed empty snapshots. With the default
`--push-interval=21600` (six hours), a complete Consul source disappearance can
remain in Nacos for a long time.

Required decision: distinguish `source read failed`, `source healthy and empty`,
and `source partially readable`; define the confirmation interval and the exact
delete SLA for a healthy empty catalog.

#### P1-2 — Health transitions are filtered before conversion

The monitor requests `Health.Service` with `passingOnly=true`. A critical or
warning endpoint therefore disappears from the returned set instead of being
converted to an unhealthy `Instance`. When a service still has healthy
siblings, this becomes a delete event; when the whole service becomes
unhealthy, the empty-source guard retains old state. The intended behavior must
be explicit: remove unhealthy instances, publish `Status=Unhealthy`, or use a
two-view policy.

#### P1-3 — Equal-Reversion field changes are not fully proved

`extractDiff` primarily treats `new.Reversion > old.Reversion` as an update.
The conversion uses `Service.ModifyIndex` as `Reversion`, but the test matrix
does not prove that every Consul health, address, port, tag, or metadata change
advances that index for the returned endpoint. A same-index state change can be
silently missed before it reaches Nacos.

#### P1-4 — No Consul Watch → Nacos Watch latency measurement

The existing E2E fan-out test proves eventual delivery to a Nacos mock, but it
does not record source mutation time, Consul watch observation time, worker
submission, Nacos SDK acknowledgement, and Nacos Watch visibility as one
sample. There are no Consul-specific P90/P95/P99 results.

#### P1-5 — Real Consul + Nacos 3 qualification is absent

The current Consul E2E tests use `consulmock` and `nacosmock`. Historical local
soak records include Consul, but they are not current Nacos 3/source-qualified
release evidence. A guarded real Consul catalog plus Nacos 3 SDK run is needed
for the final release claim.

#### P1-6 — Multiple logical Consul sources are not a single-process model yet

`ConsulAddress` is an HA address list for one client/source. The new
`--consul-cluster-id` identifies that source, but the provider composition still
constructs one Consul provider for one address list. Multiple independent
Consul catalogs in one Spotter process require a source descriptor list and one
provider/cache/watch scope per descriptor.

### P2 findings

- Consul ACL token, TLS CA, server name, datacenter, namespace, and partition
  settings are not exposed through the current provider configuration.
- Blocking-query index rollback, zero-index sanity, and rapid-change rate
  limiting are closed in the working tree; the real Consul latency and load
  qualification remains open.
- Conversion skips endpoints with missing `ports`, `appCode`, `version`, or
  required metadata; the skip is logged but not exposed as a source metric.
- The current Consul evidence does not include a scale run comparable to the
  K8s KWOK run. This remains an accepted scope boundary until Consul scale is
  required.
- Client leader probing and repeated catalog reads need request/latency metrics
  before tuning concurrency or refresh intervals.

## P0–P2 execution plan

### Stage 0 — Contract and baseline

1. Freeze the source state contract: `error`, `healthy-empty`, `healthy-nonempty`
   and `partial/unknown`.
2. Define desired behavior for Consul health failures and complete source
   disappearance.
3. Capture the current unit, race, blackbox, tagged E2E and `make test-all`
   baseline.

### Stage 1 — P1 correctness closure

1. Introduce a typed Consul source snapshot result so empty and failed reads
   cannot share one `nil`/empty representation.
2. Add explicit empty-source confirmation and deletion timing, preserving
   fail-closed behavior during read errors.
3. Decide and implement `passingOnly` versus `HealthAny` semantics. Preserve
   complete source health state when the contract requires unhealthy instances
   to remain visible.
4. Compare a complete projected fingerprint when `Reversion` is unchanged;
   keep `Reversion` as the ordering field and use field drift as the equal-index
   safety net.
5. Add Consul source metrics: catalog read duration, watch-to-conversion,
   conversion skips, source error, empty confirmation, event queue, and Nacos
   delivery outcome.

### Stage 2 — Latency and observation harness

1. Add a mutation ledger at the Consul test source boundary.
2. Observe Consul blocking-watch return, provider conversion, worker submission,
   Nacos SDK acknowledgement, and Nacos Subscribe visibility.
3. Produce P50/P90/P95/P99 for create, update, health transition, delete, and
   recovery. Report source API throttling separately from the Spotter path.
4. Keep the every-tick full Instance equality oracle, including labels,
   Reversion, source identity, and status.

### Stage 3 — Test matrix expansion

#### Unit tests

- conversion of all required and missing metadata;
- `ModifyIndex` changes and equal-index field drift;
- passing, warning, critical, deregistration and recovery states;
- source error versus healthy-empty snapshots;
- blocking index rollback, zero index, timeout and rapid changes;
- cache identity, duplicate source IDs, and source-scoped reversion.

#### Blackbox tests

- Consul mock catalog create/update/delete;
- one endpoint becoming unhealthy while siblings remain healthy;
- all endpoints disappearing and later recovering;
- client failover during a watch and during a catalog read;
- overflow queue saturation and latest-revision replay;
- Nacos prune isolation after a complete Consul source disappears.

#### E2E tests

- Consul catalog → real provider → worker → Nacos mock with timestamped
  assertions;
- the same pipeline with Nacos 3 SDK and source-qualified cluster name;
- full Instance comparison after every mutation;
- explicit failure injection for Consul outage, Nacos outage, and recovery.

### Stage 4 — Multiple Consul source model

Introduce a `ConsulSource` configuration object with `ID`, address list,
datacenter/namespace, ACL and TLS settings. Construct one provider, monitor,
cache and source scope per object. Keep the current single-source flags as a
compatibility adapter that produces one `ConsulSource`.

### Stage 5 — Release qualification

1. Focused and race tests for each changed package.
2. `go vet ./...`, `go test ./... -count=1`, `go test -race ./...`, and
   `make test-all`.
3. Guarded real Consul + Nacos 3 run with create/update/health/delete/recovery
   and P90/P95/P99 output.
4. Update this audit with hashes, exact environment, cleanup status, and
   explicit remaining deployment boundaries.

## Evidence boundary

Consul's blocking-query API uses an index and can return after a timeout without
guaranteeing that the response changed; its documented implementation guidance
also requires handling index rollback and zero-index cases. See the [official
Consul blocking query documentation](https://developer.hashicorp.com/consul/api-docs/features/blocking).
Consul health endpoints are filtered views while catalog endpoints expose raw
entries, which is relevant to the `passingOnly` decision. See the [official
Consul health API documentation](https://developer.hashicorp.com/consul/api-docs/health).

The current evidence proves local provider mechanics and one loopback fan-out
path. It does not yet prove production Consul credentials/TLS, a multiple-source
process, or Consul-specific end-to-end latency percentiles.
