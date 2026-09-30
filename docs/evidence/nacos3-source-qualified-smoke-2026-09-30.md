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
Source wire cluster: k8s-config-4b84851f95f43917-7f9843c8
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

- [Summary Markdown](../../tests/observe/results/20260930-1737-summary.md) — SHA256 `b0731e81c735626b94b69bf49d23b753f886258a8761f3f62d4f89c3bbfd734f`
- [Summary JSON](../../tests/observe/results/20260930-1737-summary.json) — SHA256 `f9fe511f058c782cb7f3cd1176428af1df6bcf443cc83afe18633a31e0b03b7e`
- [Watch events](../../tests/observe/results/20260930-1737-events.jsonl) — SHA256 `1fe83546505c823076448246d6b4f6fb6ed91dacca22fdcc5bd82ebd688f80e7`
- [Tick records](../../tests/observe/results/20260930-1737-ticks.jsonl) — SHA256 `025c299009a8d38609c398270a728281cb080008ff9c1cd19d93c6a4fcce38ad`
