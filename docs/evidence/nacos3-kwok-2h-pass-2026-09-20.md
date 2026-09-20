# Nacos 3 + KWOK two-hour final-code evidence

**Run:** `20260920-0113`  
**Code:** `bb59885` (`Nacos: remove stale batch convergence polling`)  
**Target:** `nacos/nacos-server:v3.2.4-slim`, `linux/arm64`; KWOK; Nacos-authoritative reconcile  
**Harness:** corrected three-watch Observe (`K8s live -> Spotter projection/cache -> Nacos Subscribe/catalog`)  
**Window:** 2h0m3s; scale 1000; 20 services; 5% churn/min; full push interval 60s; observation bound 60s

## Verdict

**PASS.** The final-code run met every §5.2 acceptance condition:

- 654/654 ticks `CONSISTENT`; 0 divergent ticks; 0 observation errors;
- 654/654 exact K8s/Spotter/Nacos canonical projections, including persisted
  metadata, labels, lifecycle fields and `Reversion`;
- 294/294 steady-state ticks exact;
- 13,366/13,366 K8s mutations correlated through Spotter and Nacos, with zero
  missing correlations and zero watcher errors;
- retry depth maximum 0, dropped events 0, final queues drained;
- final source population 1000 and exact post-quiescence snapshot;
- all 20 requested Nacos subscriptions started;
- teardown completed with no owned KWOK cluster, Nacos container, Spotter
  child, listener, or retry artifact remaining; runner terminal status was
  `EXIT_CODE=0`.

The run exercised the post-`bb59885` write boundary: an acknowledged official
SDK persistent RPC is not synchronously read back as the same batch. No
`persistent batch convergence timeout` occurred in the new run, and no false
retry was created while catalog visibility and source churn overlapped.

## Latency

All values are seconds, measured from the K8s API mutation to the Nacos
watch/catalog observation unless otherwise noted.

| Operation | Samples | P50 | P90 | P95 | P99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Create API → Nacos | 6,683 | 0.939 | 1.588 | 2.257 | 15.112 | 56.181 |
| Delete API → Nacos | 6,683 | 0.844 | 3.760 | 11.693 | 18.358 | 75.307 |
| Create Spotter → Nacos | 6,683 | 0.576 | 0.662 | 0.763 | 14.644 | 55.676 |
| Delete Spotter → Nacos | 6,683 | 0.532 | 0.626 | 0.673 | 14.062 | 74.240 |

The harness-wide `event_to_store_e2e` histogram contained 13,776 successful
observations: P50 0.025s, P90 0.100s, P95 0.500s, P99 10.000s. The API-to-Nacos
watch measurements above are the primary end-to-end delivery SLO evidence.

## Burst and failure-shape coverage

| Burst | Create convergence | Delete convergence |
| --- | ---: | ---: |
| 100 | 1.600s | 18.628s |
| 200 at 25% | 3.036s | 37.014s |
| 200 at 50% | 4.768s | 36.841s |
| 200 at 75% | 2.649s | 36.845s |

The sustained churn stream generated 6,683 creates and 6,683 deletes with zero
driver errors. The run also crossed populations 1000 → 1100 → 1200 and back,
while every stable observation remained exact. Crash/recovery and delete/create
transitions are represented by the KWOK mutation stream and the three-watch
correlation; no transitional tick was accepted as a stable exact tick.

## Evidence hashes

The harness emitted these raw files before cleanup. SHA256 values are retained
here as provenance; the large untracked raw files are intentionally not part of
the source repository.

| Artifact | SHA256 |
| --- | --- |
| `20260920-0113-summary.json` | `edb09229456019807017ba59e8ee0836e6fb5a283a6be023af2098eeb7a5c2da` |
| `20260920-0113-summary.md` | `68b2bdc3023955c163fae175a629797c6c96e6e7973c3ff8142056fb2419e0ea` |
| `20260920-0113-events.jsonl` | `536e6579a5a3072d03749596b5076eddc43b51721b9aa7896978e8a175ddd10a` |
| `20260920-0113-ticks.jsonl` | `9621aa8e570dbf72e0f29cf27bd652d4d1ffa93d4a2587de580bc9fa256959f3` |

## Boundary

This is local ARM64 scratch/KWOK data-plane evidence. It does not claim Nacos
deployment HA, TLS/auth policy, non-public namespace authorization, leaderless
recovery, AppCenter delivery, Atlas wire compatibility, or Consul machine-scale
evidence; those remain explicitly outside the current Spotter scope.
