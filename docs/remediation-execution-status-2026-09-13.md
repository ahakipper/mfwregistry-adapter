# Remediation Execution Status — 2026-09-13

This document is the current execution ledger for the remediation plan. It
supersedes stale HEAD labels in historical audit sections; historical findings
remain unchanged and are not promoted to current PASS evidence.

## Current baseline

- Branch: `refactor/all`
- Implementation baseline: `33606fa` (Consul payload-free monitor dispatch)
- Latest documentation baseline: `e62e1cd` (Observe current-status provenance synchronization)
- SDK: `github.com/nacos-group/nacos-sdk-go/v2`
  `v2.3.6-0.20260902123754-002486583df5` (upstream commit `0024865`)
- Worktree and remote: clean and synchronized at the time of this update.

## Execution matrix

| Area | Status | Evidence / boundary |
|---|---|---|
| K8s identity and ordering | **CODE PASS** | Source-aware identity, UID/cluster keys, ordered per-identity writes, tombstones, full revalidation and retry retention are covered by unit/race/e2e tests. |
| Burst delete and reconcile | **CODE PASS; REAL OBS PENDING** | `PushAll` retains register/catalog/prune/deregister operation type; ownership and read-error fail-safe are covered. A sustained production observation is still required. |
| Nacos production transport | **SDK-ONLY CODE PASS; RELEASE BLOCKED** | Production constructors and server wiring use the official SDK. Raw HTTP is restricted to explicit compatibility helpers. Cluster health-check Admin operation is absent from the pinned Go SDK, so product startup fails closed before readiness/canary writes. |
| Nacos authoritative reads | **CODE PASS** | GetAll and prune create one fresh SDK read session per snapshot, use an isolated temporary cache directory, and close it before returning. |
| Nacos ARM64 persistent lifecycle | **SCRATCH PASS** | `nacos/nacos-server:v2.1.0-slim`, manifest digest `sha256:e689b1c79ca4a391fc478b6b28eac74916bfe569f37abaa8145c156cefe45067`, host `60848/61848/61849`; `TestNacosReal -race` passed with cleanup residual=false. |
| Nacos reconnect | **SCRATCH PASS / PRODUCTION NOT VERIFIED** | Single-client outage→write→fresh-query gate passed under `-race` with the pinned SDK; HA, multi-node failover and production TLS/auth remain unverified. |
| Nacos batch | **TARGET-DEPENDENT / NOT VERIFIED** | Nacos 2.1.0 returned `RequestHandler Not Found`; the gate records `NOT_VERIFIED/unsupported` and verified no residual. Newer server versions require an independent run. Spotter's persistent sink does not depend on batch registration. |
| Observe deterministic engine | **CODE PASS** | Zap timestamp parsing, ledger-before-apply/delete, live source snapshots, divergence ageing and per-tick verdicts are unit/race tested. |
| Observe lifecycle | **CODE PASS; 2H NOT VERIFIED** | `observe-up.sh/down.sh` own a scratch kwok cluster, validate daemon/ports/readyz/node/capacity/state hash and clean failures. External `OBS_KUBECONFIG` mode is read-only. No 2h/1000+ run has executed because `kwokctl` is unavailable on this host. |
| DDD active graph | **CODE PASS; SHIM RETIREMENT PENDING** | Active composition/providers/elector/metrics use explicit ports; legacy globals are confined to the compatibility boundary and aggregate is build-gated. |
| Notifications | **CODE PASS; REAL DELIVERY NOT VERIFIED** | HTTP notifier has timeout/retry/Close/counters/redaction and fail-closed construction. AppCenter endpoint, payload, auth and SLA are deployment-owned and absent. |
| Atlas | **FAIL-CLOSED / NOT VERIFIED** | Local Atlas is a JSON discoverymock stand-in. Real protobuf wire, TLS, auth and method compatibility have no supplied target endpoint. |
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
2. **Nacos production evidence:** run the pinned SDK against the target HA
   topology with TLS, authentication policy, namespace/group authorization,
   leaderless/recovery and final catalog hash.
3. **Atlas contract:** provide the real protobuf definitions, method path,
   TLS/auth contract and a scratch/pre-production endpoint.
4. **AppCenter contract:** provide endpoint, payload schema, authentication and
   delivery/SLA acceptance; then run the guarded notifier scratch gate.
5. **Observe-full evidence:** install/enable `kwokctl` on a clean host and run
   the required 30m/100 rehearsal followed by the 2h/1000+ definitive run.
6. **Legacy shim retirement:** migrate remaining external callers, then remove
   compatibility constructors only after CLI/env/metrics compatibility replay.

Until these external gates are satisfied, the repository is suitable for code
review and local scratch validation but must not be labeled production-ready.
