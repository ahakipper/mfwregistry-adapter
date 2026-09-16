# KWork scale ladder — Nacos 3 ARM64 + KWork

Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.

| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 1.492 | 1.626 | 1.626 | 1.292 | 1.516 | 1.516 | 10 |
| 1 | delete | 10 | 1.401 | 1.404 | 1.404 | 1.304 | 1.313 | 1.313 | 10 |

## Crash consistency

| Transition | Latency (s) | Polls | Mismatch polls |
|---|---:|---:|---:|
| crash | 1.426 | 2 | 1 |
| recover | 1.392 | 2 | 1 |

## Verdict

PASS — all scale/operation samples and CrashLoopBackOff recovery transitions converged with strict equality.
