# Remediation Execution Status — 2026-09-13

This document is the current execution ledger for the remediation plan. It
supersedes stale HEAD labels in historical audit sections; historical findings
remain unchanged and are not promoted to current PASS evidence.

## Authoritative current addendum — 2026-09-20

- Current branch: `refactor/all`; implementation/evidence baseline `bb59885`.
- Nacos target: `nacos/nacos-server:v3.2.4-slim` on `linux/arm64`, official
  Go SDK gRPC naming path, application-scoped batches capped at 100.
- The fresh Nacos 3 + KWOK 20-minute run passed with 106/106 exact ticks,
  3368/3368 mutation correlation, zero missing boundaries, zero drops, final
  source population 1000, and an exact post-quiescence snapshot. See
  [nacos3-kwok-20m-pass-2026-09-19.md](evidence/nacos3-kwok-20m-pass-2026-09-19.md).
- A prior final-code two-hour attempt was externally interrupted after 266
  exact ticks; it is retained as interrupted evidence, not a two-hour verdict.
  That run exposed false retry amplification from synchronous catalog polling
  after successful SDK writes. Commit `bb59885` removed that hot-path poll,
  added vendor-error and lagging-catalog safety regressions, and changed the
  guarded real-Nacos verifier to bounded fresh-client polling. The replacement
  durable run passed; see
  [nacos3-kwok-2h-pass-2026-09-20.md](evidence/nacos3-kwok-2h-pass-2026-09-20.md).
- The active graph is now Nacos-only by default. Atlas requires the explicit
  `--atlas-compat` escape hatch; the obsolete aggregate scaffold and
  package-global logger/notice/config bridge are deleted.
- AppCenter endpoint/auth/retry/HTTP delivery code and CLI/config surfaces are
  deleted. Operational notices remain a generic fail-closed injected port.
- AppCenter delivery, Nacos deployment HA/TLS/auth/namespace/leaderless work,
  Atlas real wire compatibility, and Consul scale observation are explicitly
  excluded by the current user decision; none is a current release task.

## Current baseline

- Branch: `refactor/all`
- Implementation baseline: `bb59885` (Nacos-only graph, canonical reconcile,
  Sink-wide batch limits, fail-closed notices, and aggregate deletion)
- SDK: `github.com/nacos-group/nacos-sdk-go/v3`
  `v3.0.0-20260831100852-93a93504cc2f` (upstream commit `93a93504cc2f`)

## Execution matrix

### Release blockers vs deferred scope

| Category | Items |
|---|---|
| Current release blockers | None for the in-scope Nacos data plane after the final OBS-full 2h/1000+ PASS. |
| Out of scope by decision | Atlas real wire contract; deployment-level Nacos HA/multi-node/TLS/auth/namespace/leaderless validation; AppCenter endpoint/payload/auth/SLA; Consul scale evidence. |

| Area | Status | Evidence / boundary |
|---|---|---|
| K8s identity and ordering | **CODE PASS** | Source-aware identity, UID/cluster keys, ordered per-identity writes, tombstones, full revalidation and retry retention are covered by unit/race/e2e tests. |
| Burst delete and reconcile | **CODE PASS; 20M REAL PASS; 2H REAL PASS** | Revalidated full snapshots, confirmed-empty retry authority, canonical all-field comparison, and the final 654-tick three-watch evidence close the known races. |
| Nacos production transport | **SDK-ONLY CODE PASS; NACOS 3 REAL PASS** | Nacos is the default/only active Sink. Persistent writes use the pinned official v3 gRPC seam; raw HTTP is explicit test/rollback only and forbidden in product. Deployment-owned health policy requires no cluster-admin facade. |
| Nacos authoritative reads | **CODE PASS** | GetAll and prune create one fresh SDK read session per snapshot, use an isolated temporary cache directory, and close it before returning. |
| Nacos ARM64 persistent lifecycle | **NACOS 3 SCRATCH PASS** | `nacos/nacos-server:v3.2.4-slim` on `linux/arm64`; persistent request lifecycle, canonical read-back, 201-entry application batch, and cleanup passed. |
| Nacos reconnect | **SCRATCH PASS** | Single-client outage→write→fresh-query gate passed under `-race`; deployment-level HA/multi-node failover and production TLS/auth are outside this release scope. |
| Nacos batch | **SPOTTER APPLICATION BATCH CODE PASS; PROTOCOL BATCH NOT REQUIRED** | Initial snapshots and retries group one application scope at a time, split at 100 items, serialize within that scope, and use global bounded concurrency (8) over official SDK persistent single-instance calls. A successful SDK RPC is the write boundary; synchronous catalog read-after-write was removed because visibility may lag under churn. Nacos protocol-level batch remains unsupported/irrelevant to the persistent application contract. |
| Observe deterministic engine | **CODE PASS** | Zap timestamp parsing, ledger-before-apply/delete, live source snapshots, divergence ageing and per-tick verdicts are unit/race tested. |
| Observe lifecycle | **CODE PASS; 20M PASS; 2H PASS** | Commit `bb59885` passed the durable 2h0m3s Nacos 3/KWOK run: 654/654 exact ticks, 13,366/13,366 correlated mutations, zero drops/errors, drained queues, and teardown residuals false. |
| DDD active graph | **PASS** | Production and tests use explicit dependencies; legacy package globals and the build-tagged aggregate scaffold are deleted and statically forbidden. |
| Notifications | **FAIL-CLOSED BY SCOPE** | AppCenter/HTTP transport and its CLI/config secrets are deleted. The generic injected Notifier port remains; the default performs no external I/O. |
| Atlas | **EXCLUDED** | Existing compatibility Sink/mock only; real protobuf/method/TLS/auth compatibility is not a current task or release gate. |
| Consul scale | **ACCEPTED NON-GOAL** | No machine/ECS deployment is in scope; reopen only when that deployment mode returns. |
| Consul monitor change payload | **CODE PASS** | Additive payload-free `InstanceChangeHandler`; legacy and new handlers both dispatch, with notifier/Warnf error semantics covered by race tests. Historical fabricated empty `CatalogService` finding is closed for migrated callers; legacy compatibility boundary remains. |
| Static quality | **PASS** | `go vet ./...`, `go test ./... -count=1`, `go test -race ./... -count=1`, `make test-all`, Observe unit/race and lifecycle shell tests pass. |

## Reproducible gates

```bash
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
make test-all
go test -race -vet=off -tags=observe -run '^TestObserveUnit' ./tests/observe/... -count=1
bash -n scripts/observe-up.sh scripts/observe-down.sh scripts/observe_lifecycle_test.sh
bash scripts/observe_lifecycle_test.sh
```

The real ARM64 scratch commands and cleanup output are recorded in
[nacos-arm64-scratch-2026-09-13.md](evidence/nacos-arm64-scratch-2026-09-13.md).
Missing external targets cause `NOT VERIFIED`/`EnvError` and never count as a
product PASS.

## Release blockers and required owners

1. **Observe-full evidence:** completed by the final-code durable run; retain
   the evidence report and hashes as the release record.
2. **SDK pin owner:** replace the v3 pseudo-version with the first compatible
   official tag, or renew the pin exception with fresh lifecycle/race evidence.

The in-scope code and local data-plane evidence gates are satisfied. This does
not promote the explicitly excluded deployment-level Nacos or external
integration scope to production evidence.

Deployment-level Nacos HA/TLS/auth/namespace/leaderless evidence, Atlas real
wire contract, and AppCenter endpoint/payload/auth/SLA are explicitly excluded scope;
they become required only if a future release explicitly re-enables those
deployment or integration targets.
