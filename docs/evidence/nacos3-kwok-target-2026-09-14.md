# Nacos 3 / KWOK target evidence (2026-09-14)

The approved active target is Nacos 3.2.4-slim on ARM64.  No released
`github.com/nacos-group/nacos-sdk-go/v3` version is advertised by the module
proxy (`go list -m -versions` returned no tagged versions), so this change pins
the official `v3.x-dev` source at pseudo-version
`v3.0.0-20260831100852-93a93504cc2f`, resolving to commit
`93a93504cc2fc450c702e60c82d7f81acffe5f28`.

Persistent naming operations are intentionally exposed through a
Spotter-owned vendor seam (`RegisterPersistent`, `DeregisterPersistent`,
`SelectAll`, `ListServices`, `Subscribe`, `Unsubscribe`, `Close`). The adapter
must emit the Nacos 3 `PersistentInstanceRequest` gRPC payload for individual
persistent writes with `Ephemeral=false`; raw HTTP business operations remain
outside the production path. `BatchInstanceRequest` is not used for the
persistent sink because the target treats it as a complete publication and the
high-level SDK rejects persistent items on that method.

## Current gate status

| Gate | Status | Required evidence |
|---|---|---|
| ARM64 image | SCRATCH PASS | `nacos/nacos-server:v3.2.4-slim`, pulled with `--platform linux/arm64`, digest `sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5`, inspected as `linux/arm64` |
| Persistent gRPC lifecycle | SCRATCH PASS | Nacos 3.2.4 `TestNacosReal` register, deregister, complete read, service list, subscribe, and cleanup; logs show `PersistentInstanceRequest` and `Ephemeral=false` |
| Application batch | SCRATCH PASS | Fresh production-shaped exact-barrier run verified 201 persistent entries with logical `100 + 100 + 1` scheduling, injected retry, exact identity/field convergence, and cleanup |
| Kwok scale | NOT VERIFIED | Real `kwokctl`, at least 1000 Pods across 20 application/service scopes, at least two hours, and complete JSONL tick record |
| Health policy | DEPLOYMENT PREREQUISITE | Nacos service/cluster `healthChecker=NONE` is provisioned outside Spotter or verified by an approved Admin/Maintainer preflight; naming runtime does not require cluster-admin |

## Required per-tick record

Each Observe JSONL record must contain:

```text
tick, observed_at, source_count, source_sha256,
spotter_observed_count, spotter_observed_sha256,
nacos_count, nacos_sha256, divergence_count, divergence_age_seconds,
inflight_count, batch_sizes, retry_count, dropped_event_count,
queue_depth, verdict, cleanup_status
```

Definitions are fixed for the gate:

- `source_sha256` is the canonical sorted hash of the current kwok Pod
  identities and projected instance fields.
- `nacos_sha256` is the canonical sorted hash of the SDK `SelectAll` complete
  view, including disabled/unhealthy persistent entries.
- A `ghost` is a Nacos composite identity absent from the current confirmed
  source snapshot and older than `max(10s, push interval)`; a younger entry is
  `inflight` and must still disappear within the same bound.
- `verdict=CONSISTENT` requires zero ghosts, zero missing confirmed entries,
  zero dropped events, and no source/read error for that tick.

The 2-hour result is `PASS` only when every tick satisfies these rules and the
final cleanup reports no residual kwok cluster, Pod, container, temporary
state, or Nacos registration. Any missing prerequisite or incomplete window is
recorded as `NOT VERIFIED`, never as a partial PASS.

## Scratch limitations

The current scratch PASS uses a standalone local Nacos 3.2.4 container with
client authentication disabled and temporary server identity values. It does
not prove TLS, production authentication/authorization, multi-node HA,
leaderless recovery, or the two-hour kwok observation. Those remain separate
gates even though the ARM64 image and persistent gRPC data path now have fresh
local evidence.

## Fresh local command evidence

```text
docker pull --platform linux/arm64 nacos/nacos-server:v3.2.4-slim
Digest: sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5
docker image inspect: os=linux arch=arm64

NACOS_SERVER=127.0.0.1:38848 NACOS_REAL_SCRATCH=1 NACOS_REAL_ALLOW_WRITE=1 \
  go test -vet=off -tags=nacos_real ./tests/e2e \
  -run '^TestNacosReal$' -count=1
PASS: persistent gRPC register/list/query/subscribe/deregister; residual_unknown=false

NACOS_SERVER=127.0.0.1:38848 NACOS_REAL_SCRATCH=1 NACOS_REAL_ALLOW_WRITE=1 \
  go test -vet=off -tags=nacos_real ./tests/e2e \
  -run '^TestNacosRealPersistentApplicationBatch$' -count=1
PASS: entries=201, batch_max=100, batches=3, retry=passed,
      final_catalog_hash=ff7c0cead49d390e63d0fe3685868c0d148de4dacbae7d1996c410a858604111,
      residual_unknown=false

An earlier exact-barrier run timed out because the SDK read returned the
grouped service name (`DEFAULT_GROUP@@service`) while the barrier compared the
raw name. That run was downgraded to NOT VERIFIED; the current result is after
the normalization fix and a clean Nacos container readiness wait.
```

The local container used client authentication disabled with temporary
development identity values. This is a protocol/lifecycle scratch result, not
production security or HA evidence.
