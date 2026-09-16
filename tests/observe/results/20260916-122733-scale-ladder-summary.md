# KWork scale ladder — Nacos 3 ARM64 + KWork

Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.

| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 1.450 | 15.878 | 15.878 | 1.302 | 15.436 | 15.436 | 21 |
| 1 | delete | 10 | 1.418 | 1.426 | 1.426 | 1.296 | 1.317 | 1.317 | 10 |

## Crash consistency

| Transition | Latency (s) | Polls | Mismatch polls |
|---|---:|---:|---:|
| recover | 6.618 | 6 | 5 |

## Verdict

FAIL
- crash convergence: strict equality did not converge within 2m0s (polls=93 mismatches=93): spotterEqual=true; ladder-pod-11 source=true remote=false; divergences=[{ladder-app-0 k8s ladder-pod-11  missing false 2026-09-16 12:28:22.244125 +0800 +08 m=+57.647614835}]
