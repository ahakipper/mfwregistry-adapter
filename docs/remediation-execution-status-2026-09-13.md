# Remediation Execution Status — 2026-09-13

This document is the current execution ledger for the remediation plan. It
supersedes stale HEAD labels in historical audit sections; historical findings
remain unchanged and are not promoted to current PASS evidence.

## Authoritative current addendum — 2026-09-19

- Current branch: `refactor/all`; evidence baseline `436a4af`.
- Nacos target: `nacos/nacos-server:v3.2.4-slim` on `linux/arm64`, official
  Go SDK gRPC naming path, application-scoped batches capped at 100.
- The fresh Nacos 3 + KWOK 20-minute run passed with 106/106 exact ticks,
  3368/3368 mutation correlation, zero missing boundaries, zero drops, final
  source population 1000, and an exact post-quiescence snapshot. See
  [nacos3-kwok-20m-pass-2026-09-19.md](evidence/nacos3-kwok-20m-pass-2026-09-19.md).
- The legacy compatibility-tree deletion and separately authorized history
  rewrite are migration decisions, not blockers for the completed data-plane
  gate.
- AppCenter delivery, Nacos deployment HA/TLS/auth/namespace/leaderless work,
  Atlas real wire compatibility, and Consul scale observation are explicitly
  excluded by the current user decision; none is a current release task.

## Current baseline

- Branch: `refactor/all`
- Implementation baseline: `c96ab4a` (legacy-boundary and constructor assertion tightening)
- Latest documentation baseline: `8236f0e` (batch and dependency-boundary release status)
- Latest smoke fix: `2dab1da` (preserve nonfatal server-construction exit behavior)
- SDK: `github.com/nacos-group/nacos-sdk-go/v2`
  `v2.3.6-0.20260902123754-002486583df5` (upstream commit `0024865`)
- Worktree and remote: clean and synchronized at the time of this update.

## Execution matrix

### Release blockers vs deferred scope

| Category | Items |
|---|---|
| Current release blockers | Nacos Admin/Maintainer health-check capability; definitive OBS-full 2h/1000+ evidence; legacy shim retirement where required by deployment policy. |
| Out of scope by decision | Atlas real wire contract; deployment-level Nacos HA/multi-node/TLS/auth/namespace/leaderless validation; AppCenter endpoint/payload/auth/SLA; Consul scale evidence. |

| Area | Status | Evidence / boundary |
|---|---|---|
| K8s identity and ordering | **CODE PASS** | Source-aware identity, UID/cluster keys, ordered per-identity writes, tombstones, full revalidation and retry retention are covered by unit/race/e2e tests. |
| Burst delete and reconcile | **CODE PASS; REAL OBS PENDING** | `PushAll` retains register/catalog/prune/deregister operation type; ownership and read-error fail-safe are covered. A sustained production observation is still required. |
| Nacos production transport | **SDK-ONLY CODE PASS; RELEASE BLOCKED** | Production constructors and server wiring use the official SDK. Raw HTTP is restricted to explicit compatibility helpers. Cluster health-check Admin operation is absent from the pinned Go SDK, so product startup fails closed before readiness/canary writes. |
| Nacos authoritative reads | **CODE PASS** | GetAll and prune create one fresh SDK read session per snapshot, use an isolated temporary cache directory, and close it before returning. |
| Nacos ARM64 persistent lifecycle | **SCRATCH PASS** | `nacos/nacos-server:v2.1.0-slim`, manifest digest `sha256:e689b1c79ca4a391fc478b6b28eac74916bfe569f37abaa8145c156cefe45067`, host `60848/61848/61849`; `TestNacosReal -race` passed with cleanup residual=false. |
| Nacos reconnect | **SCRATCH PASS** | Single-client outage→write→fresh-query gate passed under `-race`; deployment-level HA/multi-node failover and production TLS/auth are outside this release scope. |
| Nacos batch | **SPOTTER APPLICATION BATCH CODE PASS; PROTOCOL BATCH TARGET-DEPENDENT** | Initial snapshots and retries group one application scope at a time, split at 100 items, serialize within that scope, and use global bounded concurrency (8) over official SDK persistent single-instance calls. Nacos 2.1.0 protocol-level ephemeral batch returned `RequestHandler Not Found`; this does not invalidate Spotter application batching. |
| Observe deterministic engine | **CODE PASS** | Zap timestamp parsing, ledger-before-apply/delete, live source snapshots, divergence ageing and per-tick verdicts are unit/race tested. |
| Observe lifecycle | **CODE PASS; 2H NOT VERIFIED** | `observe-up.sh/down.sh` own a scratch kwok cluster, validate daemon/ports/readyz/node/capacity/state hash and clean failures. External `OBS_KUBECONFIG` mode is read-only. No 2h/1000+ run has executed because `kwokctl` is unavailable on this host. |
| DDD active graph | **CODE PASS; EXTERNAL SHIM GATE PENDING** | Active composition/providers/elector/metrics use explicit ports; package-global logger/config wiring is bypassed by production composition. Legacy globals are confined to the compatibility boundary and aggregate is build-gated; final deletion still requires external caller migration evidence. |
| Notifications | **CODE PASS; LOG-ONLY BY SCOPE** | HTTP notifier has timeout/retry/Close/counters/redaction and fail-closed construction. AppCenter endpoint, payload, auth and SLA are deliberately not part of this Spotter release. |
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

1. **Nacos Admin capability:** provide an official Go Admin/Maintainer SDK or
   an approved, versioned adapter for cluster health-check configuration.
2. **Observe-full evidence:** install/enable `kwokctl` on a clean host and run
   the required 30m/100 rehearsal followed by the 2h/1000+ definitive run.
3. **Legacy shim retirement:** migrate remaining external callers, then remove
   compatibility constructors only after CLI/env/metrics compatibility replay.

Until these external gates are satisfied, the repository is suitable for code
review and local scratch validation but must not be labeled production-ready.

Deployment-level Nacos HA/TLS/auth/namespace/leaderless evidence, Atlas real
wire contract, and AppCenter endpoint/payload/auth/SLA are explicitly excluded scope;
they become required only if a future release explicitly re-enables those
deployment or integration targets.
