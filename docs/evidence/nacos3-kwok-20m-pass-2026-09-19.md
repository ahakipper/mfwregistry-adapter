# Nacos 3 + KWOK 20-minute reliability gate — PASS

Date: 2026-09-19  
Run: `20260919-0429`  
Target: Nacos 3 ARM64 + KWOK, 1,000 base Pods, 20 applications, Nacos
reconcile-source, Crash/Recovery, sustained churn, and four burst scenarios  
Code: `f5682b5` (`Observe: anchor final watch boundaries after the last opposite state`)

## Verdict

**PASS.** This is the first fresh 20-minute run whose final report passes every
acceptance gate after the mixed-time reconcile fix, scoped-prune fix, empty
confirmation fix, crash ownership fix, and exact-watch burst timing fix.

The earlier failed runs remain preserved in
[the reconcile-race closure report](nacos3-kwok-reconcile-race-closure-2026-09-19.md);
they are not overwritten or reclassified.

## Acceptance evidence

| Gate | Result |
|---|---:|
| Actual observation duration | 20m9s |
| Stable window ticks | 106 |
| Consistent ticks | 106/106 |
| Product-divergent ticks | 0 |
| Spotter projection mismatches | 0 |
| Observation errors | 0 |
| Mutation watch correlation | 3,368/3,368 |
| Missing mutation boundaries | 0 |
| K8s queue max / dropped events | 0 / 0 |
| Nacos retry max / final drain | 0 / true |
| Churn creates / deletes / errors | 1,683 / 1,683 / 0 |
| Final source population | 1,000 |
| Final K8s/Spotter/Nacos exact proof | true |
| Overall summary verdict | **PASS** |

Every tick compared the full Instance projection, including endpoint fields,
status/state/enabled, labels and metadata, SourceKey/SourceCluster, canonical
payload, and Reversion. The Spotter middle plane was independently observed at
the provider-output boundary; Nacos was independently observed through 20
official SDK Subscribe streams.

## Burst convergence from exact watch boundaries

Burst timings are not derived from the time a periodic stable-cut tick finally
completed. They use the final exact K8s Watch + Spotter output + Nacos Subscribe
boundary for every target Pod. Batch wall time is measured from the first chunk
issue to the latest target boundary; max-per-item is measured from each Pod's
own chunk issue time.

| Scenario | Size | UP wall | UP max/item | DOWN wall | DOWN max/item |
|---|---:|---:|---:|---:|---:|
| Storm | 100 | 1.599s | 1.599s | 18.632s | 18.632s |
| 25% batch | 200 | 2.792s | 1.731s | 37.419s | 19.353s |
| 50% batch | 200 | 3.773s | 2.872s | 36.862s | 18.761s |
| 75% batch | 200 | 2.686s | 1.687s | 37.515s | 19.434s |

All burst wall times are below `OBS_BOUND=60s`. Target-level membership was
proved by stable exact ticks, while the exact-watch boundary state machine
handles out-of-order callbacks, repeated healthy refreshes, and
delete→resurrect→delete without under-reporting.

## Continuous latency percentiles

These are per-mutation API issue → Nacos Subscribe observations collected over
the complete 20-minute run:

| Operation | Samples | P90 | P95 | P99 | Max |
|---|---:|---:|---:|---:|---:|
| Create | 1,683 | 1.565s | 1.876s | 2.555s | 3.036s |
| Delete | 1,683 | 13.782s | 16.222s | 18.760s | 19.434s |
| CrashLoopBackOff | 1 | 0.589s | 0.589s | 0.589s | 0.589s |
| Recovery | 1 | 0.677s | 0.677s | 0.677s | 0.677s |

The percentile sample set includes ordinary churn and burst chunks. Burst wall
completion is reported separately because the four burst legs are not enough
to claim a statistically meaningful batch P99.

## Raw artifacts

- [Summary JSON](../../tests/observe/results/20260919-0429-summary.json) —
  SHA256 `ab8866dfa8d9b996cc3948db1e3c3ebb45758a9fd2e2393a451bad109dbb4c02`
- [Summary Markdown](../../tests/observe/results/20260919-0429-summary.md) —
  SHA256 `ce65852e88fb48f385a40bd4871ac6043c85e8ca1e2481fa41fe9ab9b34f888b`
- [Mutation watch JSONL](../../tests/observe/results/20260919-0429-events.jsonl) —
  SHA256 `6ee937b5a4ab7a062f8aac4ea73dd65869f2544bb6f9768815f25031ecdbe871`
- [Tick JSONL](../../tests/observe/results/20260919-0429-ticks.jsonl) —
  SHA256 `c75ca9349a39a8bd61b5075987b460c9b54a4904080c298cee66c5def4a0ada4`

The owned KWOK cluster, Nacos container, Spotter child, and all harness Pods
were cleaned up after the run; the runner recorded `EXIT_CODE=0`.

## Scope boundary

This PASS covers the Spotter data-plane reliability path under the stated
single-node local KWOK/Nacos test vehicle. It does not claim Nacos deployment
HA/TLS/auth/namespace production readiness, AppCenter alert integration, or
Atlas wire compatibility; those remain outside the current agreed scope.
