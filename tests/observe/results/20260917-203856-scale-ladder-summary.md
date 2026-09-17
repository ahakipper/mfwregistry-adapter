# KWork scale ladder — Nacos 3 ARM64 + KWork

Every sample independently observes the K8s source and Nacos catalog and requires strict full-Instance equality (labels, metadata, Reversion, endpoint, lifecycle, and identity). API-issued→Nacos and source-visible→Nacos latencies are reported separately.

| Scale | Operation | Samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | Source→Nacos P90 (s) | Source→Nacos P95 (s) | Source→Nacos P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 1.534 | 1.854 | 1.854 | 1.315 | 1.323 | 1.323 | 10 |
| 1 | delete | 10 | 1.514 | 1.561 | 1.561 | 1.335 | 1.411 | 1.411 | 10 |
| 10 | create | 10 | 2.015 | 2.043 | 2.043 | 1.545 | 1.619 | 1.619 | 7 |
| 10 | delete | 10 | 1.792 | 1.916 | 1.916 | 1.351 | 1.409 | 1.409 | 9 |
| 100 | create | 5 | 4.084 | 4.084 | 4.084 | 1.430 | 1.430 | 1.430 | 2 |
| 100 | delete | 5 | 18.262 | 18.262 | 18.262 | 0.000 | 0.000 | 0.000 | 0 |
| 1000 | create | 3 | 44.501 | 44.501 | 44.501 | 0.000 | 0.000 | 0.000 | 7 |
| 1000 | delete | 3 | 184.275 | 184.275 | 184.275 | 1.345 | 1.345 | 1.345 | 1 |
| 500 | create | 3 | 12.965 | 12.965 | 12.965 | 0.000 | 0.000 | 0.000 | 3 |
| 500 | delete | 3 | 92.116 | 92.116 | 92.116 | 1.342 | 1.342 | 1.342 | 1 |

## Crash consistency

| Transition | Latency (s) | Polls | Mismatch polls |
|---|---:|---:|---:|
| crash | 1.416 | 2 | 1 |
| recover | 1.543 | 2 | 1 |

## Verdict

PASS — all scale/operation samples and CrashLoopBackOff recovery transitions converged with strict equality.
