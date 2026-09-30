# Nacos 3 + KWOK P1 Remediation One-Hour Qualification — 2026-09-30

## Verdict

**PASS — one-hour post-remediation qualification.** The run used the real
Nacos 3 ARM64 image, a KWOK Kubernetes API, the Spotter binary, and continuous
Kubernetes/Spotter/Nacos watchers. Every observation tick compared the full
Instance projection, including labels and Reversion, rather than relying on a
one-time API read.

This closes the local one-hour runtime gate. It does not close the separate
source-qualified Nacos wire-identity migration decision or the deployment-owned
health-policy verification boundary.

## Run configuration

```text
Command: OBS_DURATION=1h OBS_SCALE=1000 OBS_NODE_POD_CAPACITY=1200 \
         OBS_SERVICES=20 OBS_BURSTS=true OBS_CRASH_CYCLES=true \
         OBS_TIMEOUT=90m make test-observe
Target:  nacos/nacos-server:v3.2.4-slim, linux/arm64
Vehicle: owned KWOK cluster dsca-observe-38047, client-go Watch, Nacos 3 SDK/gRPC
Window:  2026-09-30 11:07:18 +08:00 through 12:07:18 +08:00
```

The test completed its own Pod, Spotter, Nacos, and KWOK cleanup. No owned
cluster or Nacos container remained after teardown.

## Acceptance summary

| Check | Result |
| --- | ---: |
| Observation window | 1h0m0s |
| Scale / services | 1000 / 20 |
| Consistency ticks | 320/320 exact; 0 divergent; 0 observation errors |
| Spotter projection checks | 320/320 exact; 0 mismatches |
| Full Instance equality | PASS, including labels and Reversion |
| Source scale coverage | 1000+ on 320/320 ticks |
| Snapshot retries | 172 unstable cuts retried; no false divergence |
| Mutation correlations | 7268/7268; missing 0; watcher errors 0 |
| Retry / robot queues | max depth 0 / max depth 0 |
| Dropped events | 0 |
| Final three-plane snapshot | exact |
| Cleanup | PASS; residual resources 0 |

## Latency

The primary latency boundary is **Kubernetes Watch event observed by the
harness → Nacos Watch observed in the catalog**. The API→Nacos values below are
auxiliary diagnostics and include KWOK API/client throttling, so they are not
the Spotter data-plane SLO.

| Operation | Samples | K8s Watch → Nacos P50/P90/P95/P99 | API → Nacos P50/P90/P95/P99 |
| --- | ---: | --- | --- |
| Crash | 1 | 0.572 / 0.572 / 0.572 / 0.572 s | 0.582 / 0.582 / 0.582 / 0.582 s |
| Create | 3633 | 0.572 / 0.622 / 0.643 / 0.700 s | 1.214 / 10.437 / 15.752 / 19.980 s |
| Delete | 3633 | 0.555 / 0.595 / 0.603 / 0.643 s | 2.775 / 9.201 / 14.573 / 18.809 s |
| Recovery | 1 | 0.586 / 0.586 / 0.586 / 0.586 s | 0.790 / 0.790 / 0.790 / 0.790 s |

## Burst coverage

The run exercised a 100-Pod storm and three 200-Pod bursts at 25%, 50%, and
75% progress. Every burst converged without a divergent tick. Per-item
Kubernetes-Watch-to-Nacos convergence stayed below 21.02 seconds; the
aggregate 200-Pod convergence windows were approximately 39–41 seconds.

## Evidence artifacts

- [Summary Markdown](../../tests/observe/results/20260930-1103-summary.md) — SHA256 `36ff45ccf9c7aa845c97830fcb7e372322085b5af329c4cf53ee4e218492cd5d`
- [Summary JSON](../../tests/observe/results/20260930-1103-summary.json) — SHA256 `7f6ab485750dde5c034dedc48a8ca903660721042569841a3626bb5183f262c0`
- [Watch events](../../tests/observe/results/20260930-1103-events.jsonl) — SHA256 `f2962f85daafb8b6a1f9895d57bdb2831f0b1575b791183dc7c725426aec13d2`
- [Tick records](../../tests/observe/results/20260930-1103-ticks.jsonl) — SHA256 `58e52e33f47dbf5ce193a19e5ed1192411fe8e02d4c4e12ea6ab35597d30f113`

