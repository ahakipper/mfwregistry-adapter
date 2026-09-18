# Nacos 3 + KWOK 20-minute failure root-cause report

Date: 2026-09-19  
Failed run: `20260918-2320`  
Remediation commit: `c2cc482` (`K8s/Observe: fence stale snapshots and harden churn evidence`)

## Executive verdict

The `20260918-2320` run is **FAIL evidence**, not a successful reliability
qualification. It exposed two independent problems:

1. **Harness defect:** burst rollback, ordinary churn, and crash mutation actors
   did not share ownership. The harness therefore requested deletes for Pods
   that another actor had already deleted, journaled those no-op requests as
   real mutations, and allowed one physical watch boundary to satisfy multiple
   ledger records.
2. **Product defect:** a concurrent `CompareAndFlush` rebuild could replace the
   active K8s cache after an incremental delete had already written an offline
   tombstone. The ordered sink also failed to retain incremental offline
   revisions as tombstones. A stale online instance could consequently be
   accepted and briefly resurrected.

The Codex application restart was **not causal**. The product divergence was
recorded at `23:38:17+08:00`, the last test log was written at `23:41:05`, and
the summary was complete at `23:42:16`. macOS unified logging records the old
Codex process exiting by SIGTERM at `23:44:52.653` and the new process appearing
at `23:44:54.449`; an empty replacement-run ticks file appeared at `23:46:37`.
The restart and replacement therefore happened after the failed tick and the
completed summary. The replacement was nevertheless a separate
test-infrastructure weakness and is now guarded by immutable terminal RUN_ID
status. The curated excerpt preserves this chronology and its source commands.

The code and harness fixes passed local unit, race, vet, lifecycle, and
independent review gates. They are cleared for a short targeted collision run.
They do **not** turn the failed 20-minute run into PASS evidence. A fresh
20-minute run remains required.

## Committed result artifacts and local supporting snapshots

- [summary JSON](../../tests/observe/results/20260918-2320-summary.json) —
  SHA256 `11ef1ded48d890b40497bde4ae8ef7793597839077e6df6f197afa7dcb47b240`
- [summary Markdown](../../tests/observe/results/20260918-2320-summary.md) —
  SHA256 `6335a8a426937f87239481c79c941f7ebd4012fa9d37546468815ba771e23c35`
- [per-tick JSONL](../../tests/observe/results/20260918-2320-ticks.jsonl) —
  SHA256 `a7d5841732ac02d9f1031a06710edfce858101cf3ed62ac40363909549e27d57`
- [per-mutation JSONL](../../tests/observe/results/20260918-2320-events.jsonl) —
  SHA256 `83488d103cc81c44b1c840b982a7a574f4d508f6f28e6951210eda75b5411357`
- [curated root-cause excerpt](../../tests/observe/results/evidence/20260918-2320-root-cause.log.txt) —
  SHA256 `c180252c777b668cb11cb9030ef6d36f30556c54c9b9853514a8921b1706d4ba`
- Local full harness log: `build/observe/observe-20260918-2320.log`, SHA256
  `faf973a6b66ebc8779de66c0ff994da861d0a0bdb0eddd028b5e48e656626521`
- Local cumulative Spotter child-log snapshot: `build/observe/spotter-child.log`, SHA256
  `d5d254b93469e069e8eb284842d2d0297f4c5c67dd7d83649d556f0b4f86d4f5`

The committed result files and curated excerpt are immutable through Git. The
1.7 GiB cumulative child log is appendable and intentionally not committed; its
hash identifies the local snapshot used by this analysis rather than promising
that the live file can never change. The curated excerpt records the exact
source line numbers and only the fields needed for this incident.

## What the run actually proved

| Gate | Observed | Verdict |
|---|---:|---|
| Configured window | 20m; actual 20m4s | PASS |
| Window ticks | 99 | INFO |
| Consistent ticks | 98 | WARN |
| Product-divergent ticks | 1 | **FAIL** |
| Observation errors | 0 | PASS |
| K8s/Spotter/Nacos watch mutations | 3,368 | INFO |
| Correlated by the old algorithm | 3,313 | **FAIL** |
| Reported missing mutations | 55 | **FAIL** |
| Last window tick (99) source population | 1,066, expected 1,000 | **FAIL** |
| Observed retry / robot queue / dropped at tick 87 | 0 / 0 / 0 | rules out the instrumented queue backlogs, not every possible internal delay |

Tick 87 was not a K8s-versus-Nacos catalog mismatch. K8s and Nacos both held
1,066 instances, while Spotter's active projection still held 1,075. The tick
retried 14 times for 61.742 seconds and still ended divergent. Tick 88 returned
to exact 1,066/1,066/1,066 after another 22.024-second retry window.

## Root cause A: the 55 missing records were harness-generated no-op deletes

The first burst created `obs-pod-1034..1133`. Ordinary churn was still allowed
to select burst-owned Pods:

- churn deleted `1034..1049` (16), `1050..1066` (17), and `1067..1083`
  (17) before burst rollback: **50 Pods**;
- burst rollback later journaled delete requests for all 100 burst Pods even
  though those 50 no longer existed;
- the next churn delete overlapped rollback for `1085..1100` (16 Pods):
  `1085..1089` produced no second physical boundary and appeared as the other
  **5 missing** records;
- `1090..1100` reused the same K8s/Spotter/Nacos timestamps for both ledger
  entries. The old correlator counted both records as successful, hiding
  another **11 invalid correlations**.

Therefore the correct accounting is:

```text
50 already-gone rollback deletes
+ 5 second deletes with no boundary
+ 11 second deletes that reused an existing boundary
= 66 invalid/no-op mutation records
```

This also closes the population drift exactly: ordinary churn created 66
replacement Pods but deleted burst-owned Pods which rollback was already going
to remove, leaving `1000 + 66 = 1066` source Pods.

### Remediation

- `mutationDriverMu` serializes churn, burst, and crash ownership decisions.
- Churn explicitly excludes burst-owned and crash-protected Pods.
- The driver maintains `live` and `deleting` ownership maps.
- Mutation journal entries are appended only after a successful API operation.
- Delete returns the number of Pods claimed by the driver's ownership map whose
  `kubectl` chunk returned success; skipped/partial work cannot be reported as
  full success. Because `--ignore-not-found=true` does not provide per-Pod
  server acknowledgement, this is intentionally a single-writer KWOK harness
  count, not a universal proof that every Kubernetes object was deleted.
- One physical K8s/Spotter/Nacos boundary may be consumed only once, even when
  ledger operations have different names such as `create` and `recover`.
- Per-chunk issue clocks replace one misleading clock for a large serial API
  operation.

## Root cause B: tick 87 was a real cache resurrection race

`obs-pod-2404` is the smallest concrete witness:

1. the 200-Pod rollback was issued at `23:37:18.196675+08:00`;
2. its K8s delete became visible at `23:37:50.651816+08:00` and the Spotter
   offline event at `23:37:50.554719+08:00`;
3. Nacos successfully deregistered `10.0.0.13:7096` at `23:37:50.761`;
4. Spotter then registered the same identity again at `23:37:53.216`, with the
   same Reversion `16289` as its original create;
5. it was deregistered again at `23:37:57.534`.

The code path had two cooperating defects:

- `CompareAndFlush` read the informer source and built a replacement cache
  outside the provider lock, then unconditionally replaced `k.cache`. A delete
  processed during the rebuild could therefore be overwritten by the older
  online snapshot.
- `orderedSink` remembered an incremental offline item without
  `tombstone=true`. It could accept a stale online item carrying the same
  Reversion immediately after the delete.

A related full-push defect was found during review: `snapshotForFullPush` read
the source first and the generation later, so an old list could be labeled with
a newer generation and pass revalidation. The failed run did not need this
third path to explain `obs-pod-2404`, but leaving it open would preserve an
equivalent prune/resurrection race.

### Remediation

- `CompareAndFlush` now captures a starting generation and installs its rebuilt
  cache only through a generation compare-and-swap fence.
- Full-push snapshots require the same generation before and after the source
  read; three unstable attempts cause the interval to be skipped safely.
- Incremental offline items are retained as ordered-sink tombstones, rejecting
  a same-Reversion stale online item.
- Deterministic tests force both interleavings rather than relying on timing.

## Measurement defects closed with the product fix

The failed run also revealed ways the old report could overstate success:

- burst DOWN convergence checked global K8s/Nacos equality and was stamped at
  tick 87 even though Spotter still contained nine stale instances;
- burst UP/DOWN did not prove that every target Pod was respectively present or
  absent in all three planes;
- Spotter-only divergence left the source/Nacos divergence list empty, so
  `maxHeal` misleadingly remained `0s`;
- no strict post-quiescence population/equality proof existed;
- a terminal proof initially risked diluting window ratios. It is now recorded
  with scope `post-quiescence` and is not folded into window tick counters;
- a submitted launchd runner could be replaced after process exit. A terminal
  `EXIT_CODE` now makes RUN_ID immutable and the replacement exits immediately.

## Verification completed on remediation commit

| Verification | Result |
|---|---|
| `go test ./...` | PASS |
| `go test -race ./pkg/providers/k8s ./pkg/worker` | PASS |
| `go test -race -tags=observe -run '^TestObserveUnit' ./tests/observe/...` | PASS |
| `go vet ./...` | PASS |
| `bash scripts/observe_lifecycle_test.sh` | PASS |
| `git diff --check` | PASS |
| Independent code review of all must-fix items | PASS |

The independent reviewer specifically confirmed the generation fences,
incremental tombstone, ownership-claimed/partial mutation accounting,
target-level burst membership, cross-operation boundary consumption,
post-quiescence isolation, clock placement, and lock ordering.

## Required next validation

The remediation is ready for execution in this order:

1. Run a short targeted KWOK/Nacos collision test with burst, ordinary churn,
   CrashLoopBackOff/recovery, and periodic reconcile all enabled.
2. Require zero invented mutation records, zero reused physical boundaries,
   exact burst target membership on both legs, final source population equal to
   the configured base, and an exact post-quiescence three-plane proof.
3. If the short collision run passes, run the fresh 20-minute/1,000-Pod gate.
4. Treat the new run as PASS only when it has zero product-divergent ticks,
   zero Spotter mismatches, zero missing mutation correlations, zero dropped
   events, a drained retry queue, all burst legs within `OBS_BOUND`, final source
   count 1,000, and a stable exact K8s/Spotter/Nacos terminal proof.

Until steps 1-4 pass, the repository status is **REMEDIATED / REVALIDATION
PENDING**, not reliability-qualified.
