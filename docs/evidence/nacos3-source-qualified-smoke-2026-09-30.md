# Nacos 3 Source-Qualified Cluster Smoke — 2026-09-30

## Verdict

**PASS.** After changing Nacos `clusterName` from the historical provider-only
value to a source-qualified projection, a final-HEAD Nacos 3 ARM64 + KWOK smoke
passed the complete three-plane equality checks.

## Run

```text
Command: OBS_DURATION=1m OBS_SCALE=10 OBS_SERVICES=2 \
         OBS_BURSTS=false OBS_CRASH_CYCLES=true OBS_TIMEOUT=10m \
         make test-observe
Source wire cluster: the configured source cluster name (`config-<stable-source-id>` in this KWOK run)
Target: nacos/nacos-server:v3.2.4-slim, linux/arm64
```

| Check | Result |
| --- | ---: |
| Exact ticks | 7/7 |
| Divergent / OBSERR ticks | 0 / 0 |
| Spotter projection exact | 7/7 |
| Mutation correlation | 2/2; missing 0; watcher errors 0 |
| Source scale | 10 on every tick |
| Queue / drops | retry depth 0; robot depth 0; dropped 0; drained true |
| Final three-plane snapshot | exact |
| Crash / recovery | passed |
| Cleanup | Pod and Nacos residuals false |

The first attempted run before updating the Observe reader was correctly
discarded: the product wrote the new source-qualified cluster while the old
reader still queried literal `k8s`, producing a harness-only false divergence.
The current reader and Subscribe watcher now derive the same `WireClusterName`
as the product.

## Latency

K8s Watch → Nacos Watch:

| Operation | P50 | P90 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: |
| Crash | 0.581s | 0.581s | 0.581s | 0.581s |
| Recovery | 0.549s | 0.549s | 0.549s | 0.549s |

## Artifacts and hashes

- [Summary Markdown](../../tests/observe/results/20260930-1858-summary.md) — SHA256 `5ddcaef4f57ab5df86635e15a471af6b733858dc1367e7bb1396624e6590d1ed`
- [Summary JSON](../../tests/observe/results/20260930-1858-summary.json) — SHA256 `3cda329e2bdc3754807594f7d0389febfd565eedbef4a7b9d9ba7b1361219803`
- [Watch events](../../tests/observe/results/20260930-1858-events.jsonl) — SHA256 `4c409736ec2ee1e837b43db1e33a04756b2b6d56639afa91482d17dde591602b`
- [Tick records](../../tests/observe/results/20260930-1858-ticks.jsonl) — SHA256 `131832e4ab171d6c500fe5f48dc9ccffbdc1672d55615f9eb1f621823170dfd3`
