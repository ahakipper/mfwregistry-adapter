# Nacos 3 / KWOK target evidence (2026-09-14)

The approved active target is Nacos 3.2.4-slim on ARM64.  No released
`github.com/nacos-group/nacos-sdk-go/v3` version is advertised by the module
proxy (`go list -m -versions` returned no tagged versions), so this change pins
the official `v3.x-dev` source at pseudo-version
`v3.0.0-20260831100852-93a93504cc2f`, resolving to commit
`93a93504cc2fc450c702e60c82d7f81acffe5f28`.

Persistent naming operations are intentionally exposed through a
Spotter-owned vendor seam (`RegisterPersistent`, `DeregisterPersistent`,
`SelectAll`, `ListServices`, `Subscribe`, `Unsubscribe`, `Close`).  The seam is
the contract for the follow-up adapter that must emit Nacos gRPC request
messages with `Ephemeral=false`; raw HTTP business operations remain outside
the production path.

## Current gate status

| Gate | Status | Required evidence |
|---|---|---|
| ARM64 image | NOT VERIFIED in this branch | `docker pull --platform linux/arm64 nacos/nacos-server:v3.2.4-slim`, immutable image digest, and `docker image inspect` architecture `arm64` |
| Persistent gRPC lifecycle | NOT VERIFIED | Real Nacos 3 register, deregister, complete read, retry, and cleanup with `Ephemeral=false`; no `/v1/ns` request |
| Application batch | NOT VERIFIED | One application with 201 instances, logical chunks `100 + 100 + 1`, all 201 present after the final chunk, and no prune after injected failure |
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
