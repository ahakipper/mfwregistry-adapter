# KWork scale ladder — Nacos 3 ARM64 + KWork

Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.

| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 1.449 | 15.987 | 15.987 | 1.306 | 15.426 | 15.426 | 21 |
| 1 | delete | 10 | 1.426 | 1.481 | 1.481 | 1.313 | 1.360 | 1.360 | 10 |

## Crash consistency

| Transition | Latency (s) | Polls | Mismatch polls |
|---|---:|---:|---:|
| recover | 6.614 | 6 | 5 |

## Verdict

FAIL
- crash convergence: strict equality did not converge within 2m0s (polls=93 mismatches=93)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
- watch coverage missing for create scale 1 (source=true nacos=false)
- watch coverage missing for delete scale 1 (source=true nacos=false)
