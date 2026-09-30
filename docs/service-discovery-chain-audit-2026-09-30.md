# Service Discovery Chain Comprehensive Audit — 2026-09-30

## 1. Executive verdict

Audit baseline: branch `refactor/all`, commit `a638bb5`.

**Overall status: CODE PASS WITH P1 FOLLOW-UP.** The active Nacos-only data
plane is structurally complete for the tested K8s path, but it is not correct
to call every production boundary closed. Four issues still require an
explicit engineering decision or implementation before a stronger production
claim:

1. the Nacos composite identity does not include the source K8s cluster;
2. the complete canonical metadata has no adversarial size guard;
3. `healthCheckEnabled=false` is a deployment precondition, not verified by
   the product SDK startup path;
4. one permanent item failure in a full application batch can suppress prune
   and cause the full operation to be dropped from retry.

The earlier burst-delete race, canonical/reversion comparison gap, Observe
method problem, Nacos SDK data-path migration, and the two historical vet
diagnostics are closed in code or evidence. Atlas wire compatibility and
AppCenter remain explicitly excluded, and Consul equal-scale observation
remains an accepted non-goal.

## 2. Evidence boundary

### Current static evidence

The current tree passes:

```text
go vet ./...                         PASS
go test ./... -count=1                PASS
go test -race ./...                  PASS
git diff --check                     PASS
```

Commit `a638bb5` adds regression coverage for unknown-status prune safety,
partial multi-container readiness, nil-context defaults, invalid provider
inputs, and non-Pod informer items.

### Real data-plane evidence

The strongest retained Nacos 3 + KWOK evidence is
[20260921-0146-summary.md](../tests/observe/results/20260921-0146-summary.md):
one hour, 1,000 Pods, 20 services, 319/319 exact ticks, 7,266/7,266 mutation
correlations, zero divergent ticks, zero observation errors, zero drops, and
complete Instance equality including labels and Reversion. The earlier
20-minute and two-hour records remain historical evidence with their own
commit baselines.

That run predates `a638bb5`; therefore this audit does **not** silently claim a
new full-scale runtime qualification after the defensive fixes. The current
commit has full unit and race evidence; a fresh Nacos 3 + KWOK smoke or the
next scheduled Observe gate should be recorded before a release artifact is
re-certified.

### Remediation status after the audit baseline

The first four remediation stages have now landed as code:

- `d1320d4`: collision fail-closed guard while preserving `clusterName=k8s`;
- `429fe9f`: Nacos metadata capacity validation before any write;
- `20ec2d2`: explicit verified health-policy mode and injected preflight seam;
- `6a0035a`: failed-application-scope isolation for full prune/retry.

These commits close the corresponding code hazards, but they do not by
themselves prove a source-qualified wire migration, provide a deployment's
health-policy verifier, or replace the required post-remediation Nacos 3 +
KWOK runtime evidence. Those are the remaining Stage 5 gates.

## 3. End-to-end chain review

```text
Kubernetes client-go List/Watch
  -> k8srobot source-aware QueueObject
  -> bounded coalescing queue (4,096 distinct keys)
  -> single Pop consumer
  -> GetByClusterKey + Pod conversion + validation
  -> processed provider cache + generation fence
  -> non-blocking provider pool / bounded overflow queue
  -> worker.Handle(Sync)
  -> ordered per-sink identity gate
  -> Nacos application batches (max 100 items, same app/group/cluster)
  -> SDK gRPC persistent register/deregister
  -> canonical Nacos read/reconcile and scoped prune
```

The periodic full path is:

```text
processed cache snapshot
  -> CompareAndFlush against designated Nacos view
  -> revalidated SyncAll under the sink full gate
  -> PushAll application partitioning
  -> register/deregister completion
  -> owned catalog prune
```

The important correctness boundaries are present:

- `QueueObject` carries cluster ID, UID, event type, and earliest coalesced
  creation time (`pkg/k8srobot/k8srobot.go`).
- K8s full snapshots are taken from the processed cache, not directly from the
  raw informer store; generation fencing prevents an older rebuild from
  replacing a newer tombstone (`pkg/providers/k8s/k8s.go`).
- `orderedSink` serializes same-identity operations and makes full pushes
  exclusive; revalidation occurs inside the full gate
  (`pkg/worker/fanout.go`).
- Full retries preserve scope, batch ID, sequence, revalidation, and empty
  confirmation instead of degrading to independent stale writes
  (`pkg/worker/unsynced_service.go`).
- Nacos full writes are partitioned by namespace/group/application/cluster and
  capped at `MaxPersistentBatchSize = 100`; this is an application batch, not
  a Nacos persistent protocol batch (`pkg/nacos/batch.go`).
- Nacos authoritative compare uses the complete canonical payload, including
  labels, all ports, images, source identity, lifecycle fields, and Reversion
  (`internal/domain/instance/metadata.go` and `rules.go`).

## 4. Closed items

| Area | Current judgement | Evidence |
| --- | --- | --- |
| Burst delete/register race | **CLOSED / TESTED** | Per-identity ordering, full-gate revalidation, scoped tombstone ledger, and typed full retry; prior KWOK collision closure and final Observe records. |
| Reversion handling | **CLOSED in Nacos reconcile** | Any canonical Reversion mismatch in either direction is drift; local provider value wins. |
| Labels and complete Instance projection | **CLOSED in code** | Compressed canonical payload round-trip and Observe full-field oracle. |
| Nacos production data path | **SDK-only by default** | Persistent register/deregister, query, service list, catalog, Subscribe, and readiness use the pinned v3 gRPC seam. HTTP is explicit compatibility/admin tooling. |
| Application batch | **CLOSED for current contract** | Same application scope, max 100 items, bounded global request concurrency, ordered scope execution. |
| Unknown status prune | **CLOSED by `a638bb5`** | Unknown instances are ignored while building prune desired sets; regression test prevents destructive DELETE. |
| Multi-container readiness partial report | **CLOSED by `a638bb5`** | Every declared container must have a reported status before Online/Enabled/Running. |
| `go vet` diagnostics | **CLOSED** | Current `go vet ./...` exits successfully. |
| Atlas/AppCenter | **EXCLUDED, not incomplete** | User decision; no release gate. |
| Consul equal-scale evidence | **ACCEPTED NON-GOAL** | No machine/ECS deployment in the current scope. |

## 5. Remaining findings

### F1 — Source-cluster collision in the Nacos wire identity (guard closed; migration decision open)

The internal identity is source-aware (`SourceCluster + UID`), but
`pkg/nacos/nacos.go:clusterOf` maps every K8s instance to the single Nacos
cluster name `k8s`. The custom gRPC request derives the remote ID as
`ip#port#cluster#group@@service` (`pkg/nacos/nacos3_grpc.go:342`).

If two K8s clusters use overlapping Pod CIDRs and expose the same application
at the same IP/port, the two distinct source Pods produce the same Nacos
composite ID. One registration overwrites the other even though the internal
cache and canonical payload have different `SourceKey` values. This is a real
multi-cluster boundary, not a test-only identity mismatch.

The compatibility-safe guard now rejects the colliding full snapshot and
tracks successful/in-flight owners for incremental and application-batch
writes. It prevents silent overwrite but intentionally does not pretend that
two same-address sources can coexist. Required follow-up:

- choose and document a stable mapping such as a source-cluster-qualified
  Nacos cluster/group, or prove a deployment invariant that Pod IP/port tuples
  are globally unique;
- add a two-source-cluster, same-IP/port integration test;
- do not change the wire identity without a client compatibility decision.

### F2 — Canonical metadata capacity (guard closed)

`metadataOf` writes scalar compatibility keys plus the compressed complete
payload (`pkg/nacos/nacos.go:945-974`). The only size test uses one ordinary
instance and asserts the serialized map is below 1,024 bytes
(`pkg/nacos/identity_whitebox_test.go`). There is no adversarial test for
high-entropy labels, long image names, many ports, or large source metadata.

Before the remediation, an over-limit Nacos request became a permanent 4xx.
The current guard rejects before any Admin/SDK registration. Silent truncation
would be worse because it would make canonical equality false. The current
policy is permanent pre-write rejection with measured size and limit fields;
high-entropy negative tests cover labels and resource fields. If larger
canonical payloads become a requirement, a versioned envelope or explicit
lossless external store must be designed as a separate wire change.

### F3 — Health-check policy (verified mode added; deployment verifier open)

The Spotter data path correctly sends persistent instances through Nacos 3
gRPC. However, the default `HealthPolicyDeploymentOwned` intentionally does
not verify the Nacos 3 global `healthCheckEnabled=false` switch at product
startup. If that switch is still true, Nacos can actively probe the published
IP/port (the observed `10.0.x.x:7096`/`limactl` symptom) and overwrite the
health state independently of Spotter.

The Observe fixture performs an explicit Admin compatibility set/readback before
the SDK run. That proves the fixture, not every deployment. The product now
has an explicit `verified` policy and injected verifier seam; the deployment
must supply one of these explicit release contracts:

- a deployment-owned preflight/runbook with versioned readback evidence; or
- an approved Admin/Maintainer SDK/facade that verifies the switch before
  writes.

The production naming path must not silently fall back to raw HTTP. This is a
deployment boundary, not Nacos HA/TLS/auth work in the current Spotter scope.

### F4 — Full-batch partial failure and prune isolation (code closed)

`pushPersistentBatches` now returns a structured error containing failed
application scopes. `PushAll` prunes successful scopes but leaves failed
scopes untouched, and the worker retains the structured full operation for its
existing permanent/transient retry decision. A single malformed/over-limit
item no longer suppresses cleanup for unrelated applications.

The remaining follow-up is runtime qualification and metrics for partial
application: distinguish attempted, successful, transient-failed,
permanent-failed, prune-skipped, and retried scopes, and add a real Nacos
failure-injection run for one permanent item plus healthy siblings.

### F5 — Overflow recovery is bounded, not lossless under arbitrary bursts (P2)

The informer queue holds 4,096 distinct keys. Its recovery buffer is also
bounded; entries beyond that bound wait for the next full-push healer
(`pkg/k8srobot/k8srobot.go:500-509, 560-568`). This is a safe, observable
degradation and the 1,000-Pod evidence recorded zero drops, but it is not a
proof of zero event loss for a larger simultaneous burst.

Keep `events_dropped_total`, queue depth, and full-push recovery alarms. Add a
deliberate >4,096-key KWOK stress case before making a stronger scale claim.

### F6 — Fresh SDK read sessions add connection/cache churn (P2)

`Sink.GetAll` and prune create a fresh SDK facade and temporary cache directory
for each authoritative read. This isolates stale SDK caches and passed the
current evidence, but a large number of services or a short reconcile interval
can create avoidable gRPC connect/close churn. Measure active connections and
read latency under the intended interval; then consider a bounded reusable read
session with an explicit cache invalidation rule.

### F7 — Ownership marker is deployment-wide, not writer-specific (P2)

The prune filters `spotterOwner=spotter`, which protects foreign non-Spotter
entries. Two independent Spotter deployments sharing the same namespace/group
still have the same owner marker and can prune one another's registrations.
Use a deployment identity or an explicitly enforced single-writer invariant
before sharing a Nacos scope.

### F8 — Source conversion still has permissive input edges (P2)

- `formatAppCode` can fall back to `namespace + "-" + labels["name"]`, which
  accepts a weak value such as `namespace-`.
- Pods with no usable application port can reach the sink with wire port `0`.
- A non-decimal Kubernetes `ResourceVersion` is converted to zero and then
  rejected by the normal filter, silently removing that source object from the
  publication stream.

These should become explicit validation/metrics decisions rather than implicit
filters, with tests for malformed labels, zero ports, and resource-version
parse failures.

### F9 — Compatibility seams and duplicated provider policy remain cleanup debt (P3)

The active composition root uses injected logger/config/notifier dependencies,
and the legacy global packages/aggregate scaffold are gone. The exported
`internal.InitializeProviders` wrapper and duplicated K8s/Consul full-push
policy remain for compatibility/tests. They are not on the active Nacos-only
hot path, but should be removed or clearly marked after downstream callers are
proven absent.

## 6. Reconcile closure matrix

| Scenario | Current behavior | Verdict |
| --- | --- | --- |
| K8s create/update | Cache diff emits `Sync`; ordered Nacos register/upsert; full snapshot is a backstop. | Closed for valid source states. |
| K8s delete | UID/source-key cache tombstone reuses last IP when available; Nacos deregisters; prune handles residual ID. | Closed, with empty-IP shell intentionally no-op. |
| Crash/recovery | Pod status/state changes are carried in canonical metadata and Reversion; unhealthy stays query-visible in SDK wire form. | Closed and observed. |
| Same-ID Nacos field drift | Nacos-authoritative `DiffNacosReconcile` compares the complete canonical payload and local Reversion wins. | Closed when Nacos is the designated source. |
| Changed IP/port/composite ID | New ID is registered; scoped catalog prune removes the old owned ID. | Closed subject to F1 identity policy. |
| Nacos read error | Nacos reconcile skips the compare tick; it does not interpret an error as empty. | Safe delay, not data loss. |
| Full register error | Typed full operation is retained for retry; prune is intentionally deferred. | F4 partial-application semantics remain. |
| Confirmed empty provider | Three empty confirmations and explicit provider scope authorize empty prune; unconfirmed empty is a no-op. | Safe but should retain source-health metrics. |
| Foreign Nacos entries | `spotterOwner` and provider cluster filters exclude ordinary foreign entries. | Closed for one writer; F7 remains for multiple writers. |

## 7. Recommended execution order

1. Decide and test the source-cluster-to-Nacos identity mapping (F1).
2. Add metadata-size envelope/guard and adversarial property tests (F2).
3. Define item-level permanent failure and full-prune retry semantics (F4).
4. Turn the Nacos health switch into a versioned deployment preflight or an
   approved SDK Admin capability (F3).
5. Run >4,096-key overflow stress and capture drop/recovery behavior (F5).
6. Measure/reduce fresh SDK read-session churn (F6), then tighten source input
   validation (F8).
7. Synchronize stale historical status documents and remove proven-dead
   compatibility wrappers (F9).

## 8. Scope decisions

The following are deliberately not open implementation work for this audit:

- AppCenter real alert delivery;
- Atlas real protobuf wire compatibility;
- Nacos deployment HA, multi-node failover, TLS, production auth, non-public
  namespace authorization, and leaderless recovery;
- Consul equal-scale KWOK-style observation while no machine/ECS deployment is
  in scope.

Those items must not be used to block the current Spotter data-plane audit,
but they also must not be described as tested production guarantees.
