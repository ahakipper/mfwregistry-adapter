# Consul scale-ladder E2E evidence — 2026-10-01

Command:

```bash
make test-consul-real-scale CONSUL_SCALE_LIST=1,100,1000,10000
```

The Makefile target used the pinned ARM64 Consul 1.22.0 and Nacos 3.2.4-slim scratch containers. Each scale used a unique application service, a Consul mutation ledger, the real Spotter provider/worker/Nacos SDK path, Nacos catalog polling, and the official Nacos SDK Subscribe callback. The scale path requires one `SyncAll` batch event and validates application-bounded batch behavior.

The scale harness now also emits four stage summaries for each completed scale:
watch return → provider handoff, provider handoff → Nacos write acknowledgement,
Nacos acknowledgement → Catalog observation, and Nacos acknowledgement →
official SDK Subscribe observation. The existing Catalog and Subscribe columns
remain the complete mutation-start → observation measurements.

## Historical pre-freshness-remediation run

The table below is retained as historical evidence from the implementation
that still contained the burst-two/15-second Watch limiter. It must not be
used as the current latency baseline.

| Instances | Catalog samples | Catalog P80 | Catalog P90 | Catalog P99 | Subscribe samples | Subscribe P80 | Subscribe P90 | Subscribe P99 | SyncAll |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 100 | 15,253.74 ms | 15,350.40 ms | 15,831.03 ms | 100 | 15,506.12 ms | 15,758.98 ms | 15,759.04 ms | 1 |
| 1,000 | 1,000 | 6,763.00 ms | 13,654.70 ms | 14,684.52 ms | 1,000 | 4,724.31 ms | 13,622.43 ms | 14,652.25 ms | 3 |

Each completed scale reported `ledger_complete=true`, `canonical_equality=wire_predicate_passed`, and cleanup passed.

## Freshness-remediation run

The current Watch implementation removes the seconds-scale limiter, uses
capacity-one non-blocking coalescing, and keeps the scale source TTL valid for
the complete observation window. The ARM64 local run used:

```bash
DOCKER_CONTEXT=colima make test-consul-real-scale CONSUL_SCALE_LIST=1,100,1000,10000
```

The completed scales were:

| Instances | Catalog P80/P90/P99 | Subscribe P80/P90/P99 | Watch→Provider P99 | Provider→Nacos ack P99 | Nacos ack→Catalog P99 | Nacos ack→Subscribe P99 | Result |
| ---: | --- | --- | ---: | ---: | ---: | ---: | --- |
| 1 | 169.41 / 169.41 / 169.41 ms | 580.04 / 580.04 / 580.04 ms | 72.02 ms | 37.43 ms | 31.94 ms | 442.57 ms | PASS |
| 100 | 528.53 / 671.57 / 690.03 ms | 540.68 / 664.93 / 683.40 ms | 189.53 ms | 65.46 ms | 516.88 ms | 510.24 ms | PASS |

The 1000-instance freshness rerun was **NOT QUALIFIED**: Consul reported
`source_count=1000`, while Nacos Catalog reached 961/1000 before the 5-minute
observation deadline. The run emitted the Consul HTTP `429 Too Many Requests`
response with the message `too many concurrent connections`. It is evidence of
the Consul source connection limit, not a Watch delay. No percentile is claimed
for 1000 or 10000 in this current run. An A/B rerun with the Nacos Sink write
concurrency reduced from 8 to 2 still produced the same Consul 429 and reached
948/1000, so lowering Spotter write concurrency alone is insufficient.
The 10000 gate was not started after the 1000 gate failed; it remains
NOT QUALIFIED rather than receiving an inferred percentile.

The current acceptance deadline is now fixed at exactly 5 seconds for every
scale. The 10-minute 996/1000 diagnostic run is retained only as root-cause
evidence and is not a passing latency result. The local Consul fixture used
`limits.http_max_conns_per_client=10000`; this removed the Consul 429, but did
not satisfy the 5-second complete-ledger requirement.

The strict 5-second rerun produced the following layer evidence for 1000:

| Plane | Observed | P99 | Result |
| --- | ---: | ---: | --- |
| Consul source | 1000/1000 | — | PASS |
| Spotter Provider events | 1000/1000 | — | PASS |
| Nacos write acknowledgement | 1000/1000 | — | PASS |
| Official SDK Subscribe | 1000/1000 | 1.25s | PASS |
| Nacos Catalog query | 970/1000 (run-dependent) | incomplete | NOT QUALIFIED |

The missing Catalog entries were not missing from Provider or write
acknowledgement, and Subscribe observed the complete set. This isolates the
remaining failure to the Nacos Catalog query/visibility plane. The overall
1000 gate remains failed under the strict all-plane policy, so 10000 is not
started and receives no inferred percentile.

The final three-plane diagnostic added Nacos 3 Client OpenAPI readback. For the
same persistent service, the observed result was:

```text
Provider:             1000/1000
Nacos write ack:      1000/1000
SDK Subscribe:        1000/1000
SDK gRPC query:       partial (956/1000 in the final diagnostic run)
Nacos 3 OpenAPI:      HTTP 200, code=0, data=[]
```

This closes the Spotter-side investigation: the missing records are not lost
between Consul, Provider, or Nacos write acknowledgement. They belong to the
Nacos persistent-instance query visibility plane. The OpenAPI read is retained
as a read-only diagnostic oracle; it is not used for Spotter writes.

## Query-gate-off service-discovery run

The Query gate was explicitly disabled for the service-discovery qualification:

```bash
DOCKER_CONTEXT=colima make test-consul-real-scale \
  CONSUL_SCALE_LIST=100,1000,10000 \
  CONSUL_SCALE_OBSERVE_TIMEOUT=5s \
  CONSUL_SCALE_QUERY_GATE=off
```

The 100 and 1000 scales passed the Provider → Nacos write acknowledgement →
official SDK Subscribe gate. Query/Catalog metrics were still recorded but did
not fail the run.

| Scale | Subscribe P80/P90/P99 | Watch→Provider P99 | Provider→Nacos ack P99 | Nacos ack→Subscribe P99 | Catalog samples | Result |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 100 | 702.48 / 838.07 / 964.83 ms | 314.91 ms | 53.85 ms | 611.18 ms | 100/100 | PASS |
| 1000 | 793.25 / 889.00 / 1,216.96 ms | 383.56 ms | 44.32 ms | 654.54 ms | 981/1000 | PASS (Query diagnostic only) |

The 1000 Catalog query remained incomplete, with Catalog P80/P90/P99 of
9,673.98 / 10,871.59 / 12,002.53 ms for the observed 981 samples. This does
not affect the Subscribe service-discovery result because Query gating was
explicitly disabled.

The 10000 scale was attempted in the same run but the local single-node
Consul agent began returning `context deadline exceeded` for ServiceRegister
and PassTTL around instance 8,000. The run was stopped after the bounded local
resource failure and is `NOT QUALIFIED`; no 10000 percentile is inferred.

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

The latest completed 100/1000 run artifact (using the current SyncAll and
per-instance ledger implementation) is:

```
build/consul-real/20261001T052208Z-99220/
```

This report therefore establishes:

- 100 instances: measured P80/P90/P99 PASS.
- 1,000 instances: measured P80/P90/P99 PASS.
- 10,000 instances: attempted but NOT QUALIFIED due convergence timeout.
