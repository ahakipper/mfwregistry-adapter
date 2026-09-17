# KWOK scale ladder — Nacos 3 ARM64 + KWOK

Create/Crash/Recovery samples use exact Reversion + Status + canonical-payload correlation across K8s Watch, Spotter provider-output/pre-worker, and Nacos Subscribe. Delete uses UID/SourceKey + offline + service-snapshot removal. Every batch also requires a stable-cut full-Instance snapshot equality check.

| Scale | Operation | Instance samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | External K8s Watch→Nacos P90 (s) | P95 (s) | P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 1 | 0.604 | 0.604 | 0.604 | 0.517 | 0.517 | 0.517 | 1 |
| 1 | delete | 1 | 0.621 | 0.621 | 0.621 | 0.577 | 0.577 | 0.577 | 1 |

## Strict batch convergence

High-scale batch repetitions are intentionally reported as min/median/max, not mislabeled as statistically meaningful P99.

| Scale | Operation | Batches | Min (s) | Median (s) | Max (s) |
|---:|---|---:|---:|---:|---:|
| 1 | create | 1 | 1.727 | 1.727 | 1.727 |
| 1 | delete | 1 | 1.668 | 1.668 | 1.668 |

## Continuous watch stage latency

| Scale | Operation | Stage | P90 (s) | P95 (s) | P99 (s) |
|---:|---|---|---:|---:|---:|
| 1 | create | API→K8s Watch | 0.087 | 0.087 | 0.087 |
| 1 | create | API→Spotter event | 0.087 | 0.087 | 0.087 |
| 1 | create | Spotter provider trigger→pre-worker | 0.000 | 0.000 | 0.000 |
| 1 | create | Spotter event→Nacos Subscribe | 0.517 | 0.517 | 0.517 |
| 1 | create | API→Nacos Subscribe | 0.604 | 0.604 | 0.604 |
| 1 | delete | API→K8s Watch | 0.044 | 0.044 | 0.044 |
| 1 | delete | API→Spotter event | 0.037 | 0.037 | 0.037 |
| 1 | delete | Spotter provider trigger→pre-worker | 0.000 | 0.000 | 0.000 |
| 1 | delete | Spotter event→Nacos Subscribe | 0.584 | 0.584 | 0.584 |
| 1 | delete | API→Nacos Subscribe | 0.621 | 0.621 | 0.621 |

## Crash consistency

| Transition | Exact latency (s) | API→K8s (s) | API→Spotter (s) | Spotter internal queue (s) | Spotter→Nacos (s) | API→Nacos (s) | Polls |
|---|---:|---:|---:|---:|---:|---:|---:|
| crash | 1.739 | 0.070 | 0.072 | 0.000 | 0.563 | 0.635 | 2 |
| recover | 1.702 | 0.048 | 0.048 | 0.000 | 0.566 | 0.614 | 2 |

## Verdict

PASS — all scale/operation samples and CrashLoopBackOff recovery transitions converged with strict equality.
