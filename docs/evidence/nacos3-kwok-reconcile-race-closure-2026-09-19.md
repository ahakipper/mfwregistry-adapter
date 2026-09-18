# Nacos 3 + KWOK reconcile-race closure evidence

Date: 2026-09-19  
Target: Nacos 3 ARM64 + KWOK, 1,000 base Pods, 20 applications  
Final short-gate verdict: **PASS**  
Long-gate status: **20-minute revalidation pending**

## Executive summary

Three consecutively configured 10-minute runs were used as a fail-find-fix
proof chain (actual durations were 10m45s, 10m2s, and 10m1s):

| Run | Product state | Observer state | Verdict |
|---|---|---|---|
| `20260919-0125` | K8s/Spotter 1000, Nacos 995 after five erroneous deregisters | correctly detected persistent divergence | **FAIL — product reconcile race** |
| `20260919-0229` | 41/41 window ticks exact; terminal 1000/1000/1000 | 2,366/2,368 mutations correlated | **FAIL — crash/recovery test ownership overlap** |
| `20260919-0250` | 38/38 window ticks exact; terminal 1000/1000/1000 | 2,368/2,368 mutations correlated | **PASS** |

The PASS does not rewrite either failed run. Each failure remains preserved as
evidence that the gate caught a distinct problem. The PASS authorizes the fresh
20-minute run; it is not itself the requested 20-minute reliability claim.

## Run 1: mixed-time CompareAndFlush deregistered five live instances

[Raw summary](../../tests/observe/results/20260919-0125-summary.json) reported:

- 35/36 consistent window ticks and one product-divergent tick;
- K8s=1000, Spotter=1000, Nacos=995;
- terminal snapshot still non-exact;
- zero Spotter projection mismatch, zero dropped events, and source population
  exactly 1000;
- three watcher errors, four missing mutation boundaries, and a final
  not-drained verdict were additional independent acceptance failures in that
  run.

The five missing identities were:

```text
obs-pod-2169
obs-pod-2170
obs-pod-2171
obs-pod-2172
obs-pod-2174
```

The exact product sequence was:

1. `CompareAndFlush` captured processed local count 1002 at
   `01:35:30.762`.
2. New incremental instances continued to register successfully.
3. The later Nacos read returned 1007.
4. The old implementation compared old local=1002 to newer remote=1007 and
   misclassified the five as remote-only.
5. It emitted `Status=3` for the live identities and successfully deregistered
   them around `01:35:31.980..987`.
6. The erroneous equal-Reversion tombstones then prevented automatic healing.

This was not a Nacos composite-ID mismatch and not a sampling artifact. The
register and deregister service/IP/port/cluster values were identical; K8s and
Spotter continued to contain all five identities through post-quiescence.

### Product remediation: `c56577e`

- raw informer `GetAll` is restricted to generation-zero bootstrap;
- runtime compare and destructive full push use the processed active cache;
- remote read completion rechecks the processed-cache generation;
- Nacos remote-only deletion is performed only by exclusive, revalidated full
  prune, not by unconditional incremental case-3 tombstones;
- actual revalidation is required before equal-Reversion online state can
  correct a tombstone;
- fresh-process Nacos pruning discovers every Spotter-owned pair in the
  explicit provider scope rather than relying on the lost in-memory
  `remembered` map;
- cluster and owner are checked again on destructive paths;
- confirmed-empty scoped operations can remove a previous process's owned
  state, while unconfirmed or plain unscoped empty pushes remain no-ops;
- K8s and Consul empty confirmation advances once per producer emit, not once
  per fan-out sink revalidation.

## Run 2: product passed, but Recovery and Delete were not independent samples

[Raw summary](../../tests/observe/results/20260919-0229-summary.json) proved:

- 41/41 window ticks consistent;
- zero product divergence and zero Spotter mismatch;
- terminal source/Spotter/Nacos = 1000/1000/1000;
- queue drained; zero watcher errors; final population exactly 1000;
- four burst UP/DOWN legs all within the 60-second bound.

The only failure was two missing mutation boundaries for `obs-pod-1148`:

```text
recover issued 02:32:27.858004
delete  issued 02:32:28.791659
gap                         0.934 seconds
```

The crash actor cleared its ownership immediately after the recovery API call,
allowing ordinary churn to delete the same Pod before recovery became visible
on all three watch planes. Nacos executed recovery register and delete within
about 21 ms; its official Subscribe API reports complete snapshots and
legitimately coalesced the intermediate state. The observer correctly refused
to invent two boundaries.

### Harness remediation: `ae139df`

The crash target remains excluded from ordinary churn until the exact Recovery
boundary exists in K8s Watch, Spotter provider output, and Nacos Subscribe. If
the boundary does not arrive within `OBS_BOUND`, the target remains protected
until window shutdown and the original missing boundary fails acceptance.

## Run 3: short collision gate PASS

[Raw summary](../../tests/observe/results/20260919-0250-summary.json) is the
first clean run after both remediations:

| Gate | Result |
|---|---:|
| Duration | 10m1s |
| Window ticks | 38/38 CONSISTENT |
| Product-divergent ticks | 0 |
| Spotter-mismatch ticks | 0 |
| Observation errors | 0 |
| Mutation correlation | 2,368/2,368 |
| Missing boundaries | 0 |
| Final source population | 1,000 |
| Post-quiescence exact snapshot | true |
| Retry queue drained | true; max transient depth 1 |
| Robot queue max / dropped | 0 / 0 |
| Churn creates / deletes / errors | 1,183 / 1,183 / 0 |

### Burst convergence

| Scenario | UP | DOWN |
|---|---:|---:|
| 100-Pod storm | 2s | 22s |
| 200 Pods at 25% | 7s | 45s |
| 200 Pods at 50% | 8s | 43s |
| 200 Pods at 75% | 18s | 44s |

Every leg proved target membership across all three planes; global count
equality alone was not accepted.

### Continuous per-instance latency

API issue to Nacos Subscribe visibility:

| Operation | Samples | P90 | P95 | P99 | Max |
|---|---:|---:|---:|---:|---:|
| Create | 1,183 | 7.840s | 12.560s | 12.675s | 12.699s |
| Delete | 1,183 | 16.923s | 18.642s | 18.879s | 20.989s |
| CrashLoopBackOff | 1 | 0.387s | 0.387s | 0.387s | 0.387s |
| Recovery | 1 | 0.686s | 0.686s | 0.686s | 0.686s |

Create/Delete percentile samples are continuous individual watch boundaries;
burst completion remains reported separately because four burst legs do not
support meaningful percentile claims.

## Verification outside the dynamic gate

The behavior changes also passed:

```text
go test ./...
go test -race -count=1 ./pkg/providers/k8s ./pkg/providers/consul ./pkg/worker ./pkg/nacos
go test -race -tags=observe -run '^TestObserveUnit' ./tests/observe/...
go vet ./...
bash scripts/observe_lifecycle_test.sh
git diff --check
```

Two independent review agents returned PASS after verifying processed-cache
authority, generation fences, fresh-process and confirmed-empty cleanup,
cross-provider/foreign-owner isolation, equal-Reversion recovery constraints,
empty-source confirmation semantics, and lock ordering.

## Artifact hashes

| Run | Artifact | SHA256 |
|---|---|---|
| 0125 | summary JSON | `726ae9404066bc46f68cc76cb4aa85858f070a35103736c0380bc2bff58c5538` |
| 0125 | summary Markdown | `8435641fde2c6121892844e143ea3f3578029f37707886df35dddf80d790037e` |
| 0125 | events JSONL | `4ec98e5022bc94abf8a28554848f621331290bd6197266dd9fa6976bba80b083` |
| 0125 | ticks JSONL | `e8c7b222fc28436e2b027bb5300cc0fa9002bd20515728322e1d1723906f5e82` |
| 0229 | summary JSON | `64c964a6802103d557585608b54b6e41085d2904e301ea2d52dea00aa83f95f6` |
| 0229 | summary Markdown | `6f9d6d6087f96c9b38eb7ba6c5b6ff76e3affb84a42c118956d7d6d84142c1e8` |
| 0229 | events JSONL | `0b173305f8f2df77f8fa14a050e294eb318026573d0b0d79e06ef8b1ab67b519` |
| 0229 | ticks JSONL | `95f67469866f4d3cff913182ad00f87edc8f9a625d37c68fa70656406d2af788` |
| 0250 | summary JSON | `2e771894b150423f8e602157e481f4cdeb0e1faec359c933c1a0fc3553ad6b55` |
| 0250 | summary Markdown | `a1153b9a7a8551c45f4d690a159a1e7f36217a575cea0c12822776505eef6847` |
| 0250 | events JSONL | `ddd2d0219c2534e32ffb4daca24908b0a9a0ed3f7500d4ae1715737d743b1ca3` |
| 0250 | ticks JSONL | `e384670f31a6965b2ca61c89076de2d997cf3b84915c81039fc6992869e0311a` |

## Next gate

The next action is a fresh 20-minute/1,000-Pod run with the same four bursts,
continuous churn, Crash/Recovery, all three watch planes, exact full-instance
comparison, final population proof, and post-quiescence drain. No previous run
may be promoted to satisfy that gate.
