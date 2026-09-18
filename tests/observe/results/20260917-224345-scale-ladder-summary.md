# KWOK scale ladder — Nacos 3 ARM64 + KWOK

Create/Crash/Recovery samples use exact Reversion + Status + canonical-payload correlation across K8s Watch, Spotter provider-output/pre-worker, and Nacos Subscribe. Delete uses UID/SourceKey + offline + service-snapshot removal. Every batch also requires a stable-cut full-Instance snapshot equality check.

| Scale | Operation | Instance samples | API→Nacos P90 (s) | API→Nacos P95 (s) | API→Nacos P99 (s) | External K8s Watch→Nacos P90 (s) | P95 (s) | P99 (s) | Mismatch polls |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | create | 10 | 0.742 | 0.850 | 0.850 | 0.597 | 0.608 | 0.608 | 10 |
| 1 | delete | 10 | 0.653 | 0.654 | 0.654 | 0.591 | 0.597 | 0.597 | 10 |
| 10 | create | 100 | 0.775 | 0.978 | 0.978 | 0.602 | 0.608 | 0.628 | 10 |
| 10 | delete | 100 | 0.657 | 0.658 | 0.658 | 0.562 | 0.566 | 0.576 | 10 |
| 100 | create | 500 | 1.458 | 1.458 | 2.058 | 0.559 | 0.589 | 0.610 | 2 |
| 100 | delete | 500 | 16.566 | 17.411 | 18.427 | 0.591 | 0.606 | 0.705 | 4 |
| 1000 | create | 3000 | 27.285 | 29.720 | 36.995 | 2.687 | 8.126 | 13.127 | 12 |
| 1000 | delete | 3000 | 165.883 | 174.431 | 182.375 | 3.499 | 7.890 | 15.169 | 1 |
| 500 | create | 1500 | 8.888 | 9.347 | 10.397 | 0.618 | 0.714 | 1.896 | 1 |
| 500 | delete | 1500 | 81.705 | 86.639 | 90.864 | 0.614 | 0.744 | 4.170 | 0 |

## Strict batch convergence

High-scale batch repetitions are intentionally reported as min/median/max, not mislabeled as statistically meaningful P99.

| Scale | Operation | Batches | Min (s) | Median (s) | Max (s) |
|---:|---|---:|---:|---:|---:|
| 1 | create | 10 | 1.795 | 1.840 | 1.956 |
| 1 | delete | 10 | 1.777 | 1.846 | 1.948 |
| 10 | create | 10 | 1.941 | 2.071 | 2.349 |
| 10 | delete | 10 | 1.767 | 1.894 | 1.948 |
| 100 | create | 5 | 1.495 | 1.588 | 4.340 |
| 100 | delete | 5 | 18.466 | 19.878 | 20.003 |
| 1000 | create | 3 | 22.086 | 28.968 | 64.720 |
| 1000 | delete | 3 | 184.305 | 184.598 | 185.917 |
| 500 | create | 3 | 11.603 | 12.396 | 12.473 |
| 500 | delete | 3 | 91.360 | 91.589 | 92.490 |

## Continuous watch stage latency

| Scale | Operation | Stage | P90 (s) | P95 (s) | P99 (s) |
|---:|---|---|---:|---:|---:|
| 1 | create | API→K8s Watch | 0.145 | 0.242 | 0.242 |
| 1 | create | API→Spotter event | 0.145 | 0.242 | 0.242 |
| 1 | create | Spotter provider trigger→pre-worker | 0.000 | 0.000 | 0.000 |
| 1 | create | Spotter event→Nacos Subscribe | 0.597 | 0.608 | 0.608 |
| 1 | create | API→Nacos Subscribe | 0.742 | 0.850 | 0.850 |
| 1 | delete | API→K8s Watch | 0.075 | 0.089 | 0.089 |
| 1 | delete | API→Spotter event | 0.061 | 0.067 | 0.067 |
| 1 | delete | Spotter provider trigger→pre-worker | 0.000 | 0.000 | 0.000 |
| 1 | delete | Spotter event→Nacos Subscribe | 0.602 | 0.604 | 0.604 |
| 1 | delete | API→Nacos Subscribe | 0.653 | 0.654 | 0.654 |
| 10 | create | API→K8s Watch | 0.310 | 0.432 | 0.471 |
| 10 | create | API→Spotter event | 0.310 | 0.414 | 0.472 |
| 10 | create | Spotter provider trigger→pre-worker | 0.000 | 0.001 | 0.001 |
| 10 | create | Spotter event→Nacos Subscribe | 0.602 | 0.611 | 0.629 |
| 10 | create | API→Nacos Subscribe | 0.775 | 0.978 | 0.978 |
| 10 | delete | API→K8s Watch | 0.148 | 0.153 | 0.155 |
| 10 | delete | API→Spotter event | 0.135 | 0.140 | 0.148 |
| 10 | delete | Spotter provider trigger→pre-worker | 0.000 | 0.001 | 0.001 |
| 10 | delete | Spotter event→Nacos Subscribe | 0.580 | 0.591 | 0.598 |
| 10 | delete | API→Nacos Subscribe | 0.657 | 0.658 | 0.658 |
| 100 | create | API→K8s Watch | 1.132 | 1.333 | 1.477 |
| 100 | create | API→Spotter event | 1.131 | 1.333 | 1.477 |
| 100 | create | Spotter provider trigger→pre-worker | 0.000 | 0.001 | 0.006 |
| 100 | create | Spotter event→Nacos Subscribe | 0.559 | 0.589 | 0.609 |
| 100 | create | API→Nacos Subscribe | 1.458 | 1.458 | 2.058 |
| 100 | delete | API→K8s Watch | 16.088 | 17.059 | 17.934 |
| 100 | delete | API→Spotter event | 16.085 | 17.045 | 17.924 |
| 100 | delete | Spotter provider trigger→pre-worker | 0.001 | 0.002 | 0.011 |
| 100 | delete | Spotter event→Nacos Subscribe | 0.601 | 0.618 | 0.715 |
| 100 | delete | API→Nacos Subscribe | 16.566 | 17.411 | 18.427 |
| 1000 | create | API→K8s Watch | 22.734 | 29.509 | 36.705 |
| 1000 | create | API→Spotter event | 22.734 | 29.509 | 36.706 |
| 1000 | create | Spotter provider trigger→pre-worker | 0.001 | 0.001 | 0.012 |
| 1000 | create | Spotter event→Nacos Subscribe | 2.687 | 8.125 | 13.126 |
| 1000 | create | API→Nacos Subscribe | 27.285 | 29.720 | 36.995 |
| 1000 | delete | API→K8s Watch | 165.787 | 173.968 | 181.966 |
| 1000 | delete | API→Spotter event | 165.768 | 173.955 | 181.953 |
| 1000 | delete | Spotter provider trigger→pre-worker | 0.002 | 0.005 | 0.029 |
| 1000 | delete | Spotter event→Nacos Subscribe | 3.511 | 7.895 | 15.176 |
| 1000 | delete | API→Nacos Subscribe | 165.883 | 174.431 | 182.375 |
| 500 | create | API→K8s Watch | 8.509 | 9.010 | 9.902 |
| 500 | create | API→Spotter event | 8.509 | 9.010 | 9.902 |
| 500 | create | Spotter provider trigger→pre-worker | 0.000 | 0.001 | 0.003 |
| 500 | create | Spotter event→Nacos Subscribe | 0.618 | 0.713 | 1.896 |
| 500 | create | API→Nacos Subscribe | 8.888 | 9.347 | 10.397 |
| 500 | delete | API→K8s Watch | 81.356 | 86.355 | 90.356 |
| 500 | delete | API→Spotter event | 81.345 | 86.347 | 90.354 |
| 500 | delete | Spotter provider trigger→pre-worker | 0.001 | 0.003 | 0.022 |
| 500 | delete | Spotter event→Nacos Subscribe | 0.626 | 0.760 | 4.266 |
| 500 | delete | API→Nacos Subscribe | 81.705 | 86.639 | 90.864 |

## Crash consistency

| Transition | Exact latency (s) | API→K8s (s) | API→Spotter (s) | Spotter internal queue (s) | Spotter→Nacos (s) | API→Nacos (s) | Polls |
|---|---:|---:|---:|---:|---:|---:|---:|
| crash | 1.966 | 0.081 | 0.082 | 0.000 | 0.542 | 0.623 | 2 |
| recover | 1.831 | 0.045 | 0.045 | 0.000 | 0.532 | 0.577 | 2 |

## Verdict

PASS — all scale/operation samples and CrashLoopBackOff recovery transitions converged with strict equality.
