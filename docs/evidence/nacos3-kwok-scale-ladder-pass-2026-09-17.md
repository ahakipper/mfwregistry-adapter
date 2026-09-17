# Nacos 3 + KWork scale ladder PASS

## Scope

The run used the official Nacos 3 SDK, independent client-go K8s Watch,
official Nacos Subscribe callbacks, the guarded Spotter projection endpoint,
and fresh Nacos catalog reads. Every measured sample required complete
canonical Instance equality, including labels and Reversion.

- Target: Nacos 3 ARM64 + kwok + Spotter
- Scales: 1, 10, 100, 500, 1000 Pods
- Operations: Create and Delete
- Repetitions: 10/10/5/3/3 by scale
- CrashLoopBackOff and recovery: included
- Watch errors: 0
- Verdict: PASS

## API mutation to exact Nacos catalog

| Pods | Operation | Samples | P90 | P95 | P99 |
|---:|---|---:|---:|---:|---:|
| 1 | Create | 10 | 1.534s | 1.854s | 1.854s |
| 1 | Delete | 10 | 1.514s | 1.561s | 1.561s |
| 10 | Create | 10 | 2.015s | 2.043s | 2.043s |
| 10 | Delete | 10 | 1.792s | 1.916s | 1.916s |
| 100 | Create | 5 | 4.084s | 4.084s | 4.084s |
| 100 | Delete | 5 | 18.262s | 18.262s | 18.262s |
| 500 | Create | 3 | 12.965s | 12.965s | 12.965s |
| 500 | Delete | 3 | 92.116s | 92.116s | 92.116s |
| 1000 | Create | 3 | 44.501s | 44.501s | 44.501s |
| 1000 | Delete | 3 | 184.275s | 184.275s | 184.275s |

CrashLoopBackOff converged in 1.416s and recovery in 1.543s.

## Accuracy boundary and next change

This run proves complete snapshot convergence and full K8s/Nacos Watch
coverage. The Spotter middle plane was sampled through its test-only projection
endpoint; it was not yet a continuous per-instance event stream. Some
source-to-Nacos values are zero at large scale because the first polling
boundary observed both sides already exact, so those values mean "below the
polling resolution", not zero propagation time.

The next harness revision adds a guarded Spotter event stream with per-instance
timestamps and canonical payloads. The ladder will then report API→K8s Watch,
K8s Watch→Spotter event, Spotter event→Nacos Subscribe, and end-to-end
P90/P95/P99. The renewed two-hour run starts only after that event correlation
passes its short and full scale gates.
