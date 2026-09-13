# Nacos ARM64 Scratch Evidence — 2026-09-13

Status: **REAL SCRATCH EVIDENCE ONLY — NOT PRODUCTION PASS**

This artifact records the bounded local scratch run used to validate the
Nacos SDK lifecycle gate. It does not establish production availability,
security, HA, or compatibility with an external deployment.

## Target and immutable runtime

| Item | Observed value |
|---|---|
| Nacos image | `nacos/nacos-server:v2.1.0-slim` |
| Immutable manifest digest | `sha256:e689b1c79ca4a391fc478b6b28eac74916bfe569f37abaa8145c156cefe45067` |
| Docker platform | `linux/arm64` (inspect architecture `arm64`) |
| Host endpoint | `127.0.0.1:28848` |
| HTTP mapping | host `28848` → container `8848` |
| gRPC mapping | host `29848` → container `9848` |
| control/Raft mapping | host `29849` → container `9849` |
| SDK | `github.com/nacos-group/nacos-sdk-go/v2` pseudo-pin `v2.3.6-0.20260902123754-002486583df5` (commit `0024865`) |
| Scope | disposable local ARM64 scratch container; no corporate endpoint |

The harness validated the local image architecture and every reported
`RepoDigest`, then ran the canonical `repo@sha256` reference with
`--platform linux/arm64`; mutable tags were not used for the container run.

## Commands and result

```bash
go test -race -vet=off -tags=nacos_real ./tests/e2e/... -count=1
go test -race -vet=off -tags=nacos_sdk_eval ./tests/e2e/... -count=1
go vet -tags=nacos_real ./tests/e2e/...
go vet -tags=nacos_sdk_eval ./tests/e2e/...
```

Both lifecycle gates completed **PASS** against the scratch target:

- `nacos_real`: SDK persistent register, catalog query, SelectAll query,
  service list, subscribe/unsubscribe, and deregister.
- `nacos_sdk_eval`: SDK persistent register, SelectAll query, service list,
  subscribe/unsubscribe, and deregister.
- Cleanup evidence for each gate: `cleanup_attempted=true`,
  `status=passed`, `residual_unknown=false`.

The gates register cleanup before the first write attempt, so an ambiguous
transport result still triggers deregistration before the SDK client closes.
No raw HTTP compatibility transport is used by these tests.

## Additional authenticated and scoped runs

The same immutable ARM64 image was also exercised on the scratch host with
the Nacos 2.1.0 authentication profile. The host mappings were
`38848 → 8848`, `39848 → 9848`, and `39849 → 9849`. The run supplied a random
local auth token through the guarded environment, username `nacos`, and
password `nacos`, together with `NACOS_REAL_SCRATCH=1`,
`NACOS_REAL_ALLOW_WRITE=1`, and `NACOS_ALLOW_INSECURE_AUTH=1`. Both
`nacos_real` and `nacos_sdk_eval` completed **PASS**; each reported
`cleanup_attempted=true`, `status=passed`, and `residual_unknown=false`.

A second `nacos_real` run used namespace `tenant-a` and group `blue` against
the same scratch container and completed **PASS** with the same cleanup
status. Secret token values are intentionally not recorded.

As a negative security check, a plaintext-auth attempt without the complete
scratch/write/insecure-auth guard was rejected before any Nacos client write;
this rejection is evidence of the guard, not a protocol success.

After these runs Docker inspection confirmed the scratch container was
stopped and removed; no residual container remained. Future evidence must
retain the same post-removal verification.

## What this evidence does not prove

The following remain unverified or blocked and must not be inferred from this
scratch result:

- TLS certificate/hostname validation and production authentication policy
  (the scratch username/password and token checks are not production auth
  evidence);
- namespace/group authorization beyond the single `tenant-a`/`blue` scratch
  pair;
- Nacos HA, leaderless behavior, restart/redo/cache recovery, and multi-node
  failover;
- an official Admin/Maintainer SDK capability for cluster health-check update;
  production startup remains fail-closed without an approved adapter;
- Atlas protobuf/JSON wire compatibility;
- the complete 2h/1000+ Observe run;
- real AppCenter endpoint, payload contract, authentication, and SLA.

The scratch container was stopped and removed after the run; any future run
must retain equivalent cleanup status and residual-container verification.

The repository also contains a separate `nacos_restart` gate for persistent
canary survival across a single-node restart. It is guarded by a strict
`dsca-*`/`test-*` container name and explicit restart/write flags. Against the
same ARM64 Nacos image, the non-race run used host mappings
`48848 → 8848`, `49848 → 9848`, and `49849 → 9849` and **PASS**ed persistent
restart/new-client visibility; cleanup reported
`cleanup_attempted=true`, `status=passed`, `residual_unknown=false`, and Docker
inspection confirmed the container was removed.

The corresponding race-enabled command exposed a real data race in the
vendor `nacos-sdk-go/v2` pseudo-pin `0024865` RpcClient reconnect path and therefore **FAILED /
RACE_BLOCKED**. The gate deliberately closes the old SDK client before restart,
warms its bounded service-list session, and creates a fresh one afterwards;
automatic reconnect is explicitly `NOT VERIFIED/RACE_BLOCKED`, never masked or
suppressed. This vendor blocker prevents any production SDK lifecycle PASS.

Commands and outcomes:

```bash
go test -vet=off -tags=nacos_restart ./tests/e2e/... -run TestNacosRealRestartPersistence -count=1  # PASS
go test -race -vet=off -tags=nacos_restart ./tests/e2e/... -run TestNacosRealRestartPersistence -count=1  # FAIL: vendor RpcClient race
```
