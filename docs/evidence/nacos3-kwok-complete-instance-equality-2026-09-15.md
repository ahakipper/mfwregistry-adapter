# Nacos 3 + Kwok complete Instance equality evidence (2026-09-15)

## Verdict

`PASS` for the requested one-hour strict observation window after the
complete-Instance and linearized-read changes. This is a one-hour result, not
the separate two-hour plan gate.

The emitted tick is the unit of evidence. The observer reads one live kwok
source snapshot and one fresh official Nacos Go SDK snapshot; if a complete
mutation-attributed mismatch is found, it retries both reads inside the same
tick. Only the final snapshot is emitted. A retry that remains non-exact past
`OBS_BOUND` is emitted as `DIVERGENT` and fails the run.

## Run configuration

| Item | Value |
|---|---|
| Nacos | `nacos/nacos-server:v3.2.4-slim` |
| Nacos platform | `linux/arm64` |
| Nacos image digest | `sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5` |
| Kwok | `kwokctl/kwok v0.8.0`, Go 1.26.4, darwin/arm64 |
| Source scale | 1,000 Pods |
| Application/service scopes | 20 (`obs-app-0` through `obs-app-19`) |
| Window | 1h0m1s |
| Tick cadence | 10s |
| Churn | 5%/minute, every 20s, create-before-delete |
| OBS_BOUND | 1m0s (`max(10s SLO, 60s push interval)`) |
| Reconcile source | Nacos |
| Code revision | `fe78b44` (remote `refactor/all`) |

## Per-tick result

| Metric | Result |
|---|---:|
| Window ticks | 361 |
| Exact-equal ticks | 361 / 361 (100%) |
| Transitional ticks emitted | 0 |
| DIVERGENT ticks | 0 |
| OBSERR ticks | 0 |
| Source count | 1,000 on every tick |
| Expected representable count | 1,000 on every tick |
| Nacos count | 1,000 on every tick |
| Source/Nacos field divergences | 0 |
| Source fingerprint length | 64 hex characters on every tick |
| Remote fingerprint length | 64 hex characters on every tick |
| Maximum snapshot attempts | 5 |
| Maximum same-tick wait | 6,790ms |
| Maximum retry queue depth | 1 |
| Maximum kwok queue depth | 0 |
| Dropped events | 0 |
| Churn creates/deletes | 2,983 / 2,983 |
| Churn errors | 0 |
| Maximum observed heal | 0s (no mismatch remained in an emitted tick) |
| Final retry drain | PASS |
| Final cleanup | PASS; no owned state, kwok cluster, Pod, Nacos container, or child remained |

The single retry-queue sample at depth 1 recovered without producing a
non-exact emitted tick. Internal snapshot retries are visible in each JSONL
record through `snapshotAttempts` and `snapshotWaitMs`; they are not hidden
latency.

## Complete field contract

The source side is constructed through the same exported production K8s
conversion boundary used by the informer path. New Nacos writes include the
compressed, versioned `spotter.instance` metadata payload. Its canonical JSON
contains every `internal/domain/instance.Instance` property:

- `SourceKey`, `SourceCluster`, `InstanceId`, `Level`;
- the complete `Ports` slice (`Name`, `Protocol`, `Port`, `ServicePort`);
- `Ip`, `EnvCode`, `EnvType`, `EnvGroup`, `Cluster`, `Provider`;
- `Version`, `Enabled`, `State`, `HealthState`, `AppCode`;
- the complete `Label` map, including arbitrary source labels and derived
  compatibility labels;
- `Hostname`, `Cpu`, `Memory`, `Disk`, `Os`, and the complete `Image` map;
- `Idc`, `Reversion`, and `Status`.

The observer compares the compressed canonical payload byte-for-byte, in
addition to explicit wire identity, endpoint, service/cluster scope, enabled,
ephemeral, cardinality, and metadata checks. `reversion` and labels are
therefore part of every tick's equality decision, not merely startup tests.

Nacos `healthy` is read and included in the remote audit fingerprint but is
not a Spotter equality failure. For persistent instances it is maintained by
Nacos's server-side health checker; selecting `healthChecker=NONE` is an
Admin/Maintainer/deployment control-plane operation not exposed by the
official Go Naming SDK. This does not relax any Spotter-owned field.

## Provenance

The compact artifacts from this run are intentionally kept outside Git's raw
results directory. Their SHA-256 values are:

```text
78eaecb2ddc0a33089cc7cda57e5a504936fe4710ce249057518f3b4e4e42521  20260915-1720-summary.json
4d7eb03df5a70961cec5bd730922527649451949e7a82574dae2d9fca6207a21  20260915-1720-summary.md
4cd6c7ca6caab29ce91b2c8e61c4b28256456fa8e1d0b7ab6383802caad264d9  20260915-1720-ticks.jsonl
```

The authoritative local paths are:

- `tests/observe/results/20260915-1720-summary.json`
- `tests/observe/results/20260915-1720-summary.md`
- `tests/observe/results/20260915-1720-ticks.jsonl`

The raw JSONL is retained locally for audit and is not committed as a large
generated artifact. The complete-payload implementation and the maintained
contract are committed in `3648d55` and `fe78b44`.

## Earlier evidence classification

- `20260915-1052` was a one-hour PASS for the old reduced projection and is
  subset-only.
- `20260915-1441` exercised the complete payload but exposed two stale-cache
  transitional ticks before fresh SDK snapshots were added.
- `20260915-1637` was intentionally stopped after reproducing the same
  source/remote read-window race.
- `20260915-1720` is the first run after both complete payload and
  linearized-read fixes and is the result reported above.

The separate plan requirement for a two-hour burst-augmented window remains
open; this document does not promote the one-hour result to that gate.

## Independent review

The final reviewer confirmed the complete payload coverage, legacy fallback,
Nacos metadata-size test, and the 20260915-1720 result. The reviewer also
confirmed that the observed p99 event-to-store latency reached the 10-second
histogram boundary but did not cause a consistency or drain failure.
