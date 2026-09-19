# Nacos-Only Stability and E2E Completion Plan

**Date:** 2026-09-19  
**Branch:** `refactor/all`  
**Owner:** lead agent with delegated implementation and review agents

## Execution status

| Stage | Status | Commit / evidence |
|---|---|---|
| 0 — baseline and plan | **PASS** | `6b37bec` |
| 1 — Nacos-only composition | **PASS** | `91fb3a7`; independent review PASS |
| 2 — canonical all-field reconcile | **PASS** | `52b787c`; independent adversarial review PASS |
| 3 — batch/ordering/failure recovery | **PASS** | `92712fb`; independent adversarial review PASS |
| 4 — E2E/Observe qualification | **RUNNING** | Expanded E2E landed; durable final-code two-hour run is executing after the batch-polling fix |
| 5 — cleanup/release gate | **IN PROGRESS** | AppCenter removal `d80aa42`; aggregate deletion `0e6de37`; batch-polling fix `bb59885`; final matrix/docs pending |

## Scope decision

The active Spotter release is the Nacos 3 data plane. Atlas and AppCenter are
explicitly excluded: Atlas may remain only behind an intentional compatibility
boundary; AppCenter transport/configuration is removed, and only the generic
notice port remains fail-closed. Nacos deployment HA,
TLS/auth policy, non-public namespace authorization, leaderless recovery, and
Consul machine-scale observation remain deployment or future-scope concerns.

The existing 20-minute/1000-Pod KWOK run is evidence for the current data plane,
not a substitute for the implementation gates below. Every implementation
stage must add or strengthen an E2E seam where the behavior crosses a process,
provider, worker, or Nacos boundary.

## Stage 0 — Freeze the baseline and test contract

1. Record the current HEAD, clean-worktree state, `go test ./...`, `go test
   -race ./...`, `go vet ./...`, and the legacy-boundary checker.
2. Keep the formal evidence artifacts immutable; do not classify historical
   failed Observe runs as PASS.
3. Add a machine-readable acceptance matrix covering Nacos-only startup,
   full-field reconcile, application batches, partial failures, retry replay,
   crash/restart recovery, and exact E2E cleanup.

**Exit gate:** baseline commands pass and every later stage has an explicit
focused test plus a corresponding E2E or process-boundary test.

## Stage 1 — Make Nacos-only the real active composition

### Implementation

- Stop requiring an Atlas gRPC connection when the active configuration is
  Nacos-only.
- Make Nacos the default reconcile source whenever it is the active Sink.
- Preserve Atlas only as an explicit compatibility mode until the final code
  removal decision; it must not be an implicit startup dependency.
- Remove AppCenter endpoint/token/retry delivery from the product path. Keep a
  generic notifier/log fail-closed port for internal diagnostics.
- Treat `healthChecker=NONE` as deployment-owned. Normal SDK register,
  deregister, query, subscribe, and application-batch operations must not fail
  startup because an optional cluster-admin facade is unavailable.

### E2E gates

- Start a real server composition with only a Nacos 3 scratch target and a
  fake K8s provider; assert no Atlas dial is attempted.
- Start the same composition with Atlas unavailable; Nacos-only startup and
  writes must still succeed.
- Assert the default reconcile read source is Nacos and that no Atlas call is
  required to heal a Nacos-side drift.
- Assert product startup never creates an AppCenter HTTP request.

**Exit gate:** Nacos-only process startup, first write, reconcile, shutdown,
and cleanup pass in a real process-boundary E2E; legacy compatibility tests are
explicitly tagged and isolated.

## Stage 2 — Canonical full-Instance reconcile

### Implementation

- Introduce one normalized canonical comparison for the Nacos-authoritative
  path. Compare every persisted Spotter field: identity/source fields,
  endpoints and all ports, labels, images, resource fields, lifecycle/status,
  Enabled projection, and Reversion.
- Exclude only Nacos-owned transport `Healthy`; normalize the status-2
  transport representation before comparison.
- Use the comparator for both K8s and Consul Nacos-reconcile paths. Keep any
  Atlas-specific historical policy isolated from this path.

### E2E gates

For one live instance, mutate each field out of band while keeping Reversion
equal, then verify exactly one local-heal publication and a stable second
reconcile tick. Repeat with remote Reversion lower and higher than local.
Verify labels, ports, Image, Memory, SourceKey, SourceCluster, Status,
Enabled, and delete/recreate identity. Assert no infinite push loop.

**Exit gate:** full canonical equality is proven by unit, race, in-process E2E,
and a guarded real Nacos 3 catalog/Subscribe replay.

## Stage 3 — Application batch, ordering, and failure recovery

### Implementation

- Keep logical application batches capped at 100; do not invent a persistent
  protocol batch that the target SDK/server does not support.
- Make the Nacos item-call concurrency limit a Sink-wide invariant across
  incremental pushes, full batches, and retry replays.
- Preserve per-application ordering and per-identity ordering while allowing
  independent applications to overlap.
- Ensure unhealthy persistent instances use the same transport projection in
  incremental and batch paths (`enabled=true`, `healthy=false` where required
  for query visibility).
- Preserve the full operation type, scope, batch ID, and prune barrier across
  partial failures and retries.
- Treat an acknowledged official SDK write as the write-attempt boundary. Do
  not synchronously read the same batch back from Nacos; catalog visibility is
  eventually verified by prune/reconcile and the external three-watch oracle.

### E2E gates

- 201 persistent instances: verify `100 + 100 + 1`, exact catalog identity set,
  metadata/labels/Reversion, and no `/v1/ns` business write in SDK mode.
- Inject failure in item 37, a whole batch, catalog read, and prune delete;
  assert no premature prune and exact replay after recovery.
- Run concurrent incremental updates, full batches, and retry replay; record
  maximum in-flight SDK calls and assert the configured cap.
- Kill Spotter at each write/prune boundary, restart it, and prove eventual
  convergence without duplicates or ghosts.

**Exit gate:** deterministic unit/race tests, tagged in-process E2E, and a
guarded real Nacos 3 application-batch run all pass with cleanup residuals
false.

## Stage 4 — Complete the E2E/Observe qualification

1. Run the corrected three-watch harness for 1/10/100/500/1000 Pods and
   Create/Delete/Crash/Recovery, retaining per-instance P90/P95/P99 and batch
   min/median/max separately.
2. Add a process-restart/leader-handoff scenario to the E2E matrix.
3. Run the sustained 2-hour/1000-Pod window only after Stages 1–3 pass. Every
   stable tick must compare K8s live state, Spotter provider-output/cache state,
   and Nacos Subscribe/catalog state with full canonical equality.
4. Separate source/API pressure from Spotter and Nacos latency in the report;
   define explicit SLOs before treating a performance result as PASS.
5. Verify teardown: no child process, container, KWOK Pod, listener, or retry
   artifact remains.

**Exit gate:** zero missing mutation correlations, zero dropped events, zero
unbounded divergences, drained retry queue, exact terminal snapshot, and a
reviewed latency report.

## Stage 5 — Documentation, cleanup, and release gate

- Update all authoritative docs to Nacos SDK v3 and the actual active graph;
  remove stale v2/Admin-blocker claims.
- Delete the build-gated aggregate and empty legacy directories only after the
  in-repository reference scan and compatibility decision are recorded.
- Keep Atlas/AppCenter/deployment-owned items explicitly excluded.
- Run `go vet ./...`, full unit, race, black-box, E2E, static-boundary, and
  Observe unit gates; publish a final evidence index with hashes.

## Commit and review protocol

Each stage is one or more atomic commits. Every commit must use a detailed
English subject/body with `Problem:`, `Changes:`, `Verification:`,
`Compatibility / Rollback:`, and `Documentation:` sections. A separate review
agent checks the diff, tests, and evidence before the lead pushes the stage.
No force-push or history rewrite is part of this plan.
