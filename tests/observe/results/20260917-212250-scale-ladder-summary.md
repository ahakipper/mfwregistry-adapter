# KWork scale ladder — Nacos 3 ARM64 + KWork

Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.

| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 1.449 | 1.611 | 1.611 | 1.297 | 1.329 | 1.329 | 10 |
| 1 | delete | 10 | 1.433 | 1.504 | 1.504 | 1.321 | 1.321 | 1.321 | 10 |

## Continuous watch stage latency

| Scale | Operation | Stage | P90 (s) | P95 (s) | P99 (s) |
|---:|---|---|---:|---:|---:|
| 1 | create | API→K8s Watch | 0.147 | 0.281 | 0.281 |
| 1 | create | API→Spotter event | 0.147 | 0.282 | 0.282 |
| 1 | create | K8s Watch→Spotter event | 0.001 | 0.001 | 0.001 |
| 1 | create | Spotter event→Nacos Subscribe | 0.591 | 0.608 | 0.608 |
| 1 | create | API→Nacos Subscribe | 0.713 | 0.848 | 0.848 |
| 1 | delete | API→K8s Watch | 0.093 | 0.111 | 0.111 |
| 1 | delete | API→Spotter event | 0.090 | 0.112 | 0.112 |
| 1 | delete | K8s Watch→Spotter event | 0.000 | 0.001 | 0.001 |
| 1 | delete | Spotter event→Nacos Subscribe | 0.593 | 0.596 | 0.596 |
| 1 | delete | API→Nacos Subscribe | 0.674 | 0.675 | 0.675 |

## Crash consistency

| Transition | Exact latency (s) | API→K8s (s) | API→Spotter (s) | Spotter→Nacos (s) | API→Nacos (s) | Polls |
|---|---:|---:|---:|---:|---:|---:|
| crash | 1.345 | 0.044 | 0.044 | 0.569 | 0.613 | 2 |
| recover | 1.390 | 0.057 | 0.054 | 0.559 | 0.614 | 2 |

## Verdict

PASS — all scale/operation samples and CrashLoopBackOff recovery transitions converged with strict equality.
