# KWork scale ladder — Nacos 3 ARM64 + KWork

Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.

| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 1.437 | 14.960 | 14.960 | 1.337 | 14.253 | 14.253 | 20 |
| 1 | delete | 10 | 1.438 | 1.439 | 1.439 | 1.309 | 1.318 | 1.318 | 10 |

## Crash consistency

| Transition | Latency (s) | Polls | Mismatch polls |
|---|---:|---:|---:|
| crash | 1.393 | 2 | 1 |
| recover | 1.408 | 2 | 1 |

## Verdict

FAIL
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
