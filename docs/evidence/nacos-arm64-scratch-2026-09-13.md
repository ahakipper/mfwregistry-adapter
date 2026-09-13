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
| SDK | `github.com/nacos-group/nacos-sdk-go/v2` v2.3.5 |
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

## What this evidence does not prove

The following remain unverified or blocked and must not be inferred from this
scratch result:

- TLS certificate/hostname validation and production authentication policy;
- non-public namespace and custom group authorization;
- Nacos HA, leaderless behavior, restart/redo/cache recovery, and multi-node
  failover;
- an official Admin/Maintainer SDK capability for cluster health-check update;
  production startup remains fail-closed without an approved adapter;
- Atlas protobuf/JSON wire compatibility;
- the complete 2h/1000+ Observe run;
- real AppCenter endpoint, payload contract, authentication, and SLA.

The scratch container was stopped and removed after the run; any future run
must retain equivalent cleanup status and residual-container verification.
