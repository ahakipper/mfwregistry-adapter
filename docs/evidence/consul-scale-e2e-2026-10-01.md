# Consul scale-ladder E2E evidence — 2026-10-01

Command:

```bash
make test-consul-real-scale CONSUL_SCALE_LIST=100,1000,10000
```

The Makefile target used the pinned ARM64 Consul 1.22.0 and Nacos 3.2.4-slim scratch containers. Each scale used a unique application service, a Consul mutation ledger, the real Spotter provider/worker/Nacos SDK path, Nacos catalog polling, and the official Nacos SDK Subscribe callback. The scale path requires one `SyncAll` batch event and validates application-bounded batch behavior.

## Completed scales

| Instances | Catalog samples | Catalog P80 | Catalog P90 | Catalog P99 | Subscribe samples | Subscribe P80 | Subscribe P90 | Subscribe P99 | SyncAll |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 100 | 1,774.18 ms | 3,150.33 ms | 3,150.33 ms | 100 | 1,909.75 ms | 3,025.23 ms | 3,025.24 ms | 1 |
| 1,000 | 1,000 | 20,645.50 ms | 20,645.58 ms | 22,891.21 ms | 1,000 | 16,053.57 ms | 18,818.35 ms | 22,741.70 ms | 3 |

Each completed scale reported `ledger_complete=true`, `canonical_equality=wire_predicate_passed`, and cleanup passed.

## 10,000-instance result

The 10,000-instance run was attempted with a bounded test deadline. Consul accepted the registration wave, but the complete Spotter → Nacos visibility ledger did not converge before the test deadline. The run ended as a test timeout, not a PASS, and no P80/P90/P99 values were emitted for 10,000.

Artifact:

```
build/consul-real/20261001T050140Z-83039/
```

Final metadata:

```
qualification_status=failed
cleanup_status=passed
residual_unknown=false
final_exit_code=1
```

The failure is important evidence: at 10,000 instances on this local ARM64/Colima environment, the end-to-end pipeline did not complete within the gate. It must not be represented as a percentile result. The failure stack showed the scale test waiting while the Consul registration/provider/Nacos batch path was still active; further diagnosis should measure registration pressure, provider snapshot conversion, Nacos gRPC payload/throughput, and queue/full-push contention separately.

This report therefore establishes:

- 100 instances: measured P80/P90/P99 PASS.
- 1,000 instances: measured P80/P90/P99 PASS.
- 10,000 instances: attempted but NOT QUALIFIED due convergence timeout.
