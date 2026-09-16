# Nacos 3 + KWork three-plane smoke report

## Run

- Stamp: `20260916-1056`
- Target: Nacos 3 ARM64 + kwok + Spotter child
- Configuration: 2-minute window, 50 base Pods, 5 services, burst mode off,
  10-second tick, 60-second full-push interval
- Cleanup: kwok, Nacos, Pods, and child cleanup completed;
  `residual_unknown=false`

## Result

- 13/13 ticks were `CONSISTENT` and exact-equal.
- K8s live, Spotter informer projection, and Nacos SDK catalog all reported 50
  instances with matching canonical payloads on every tick.
- Spotter projection was observed on all 13 ticks; Spotter mismatch count was
  zero; Nacos/queue dropped-event count was zero; retry queue drained.
- Latency histogram for the short run: P50 10ms, P90 25ms, P95 25ms,
  P99 25ms over 8 successful Nacos observations. This is a smoke sample, not
  a production percentile claim.

## Conclusion

The triad snapshot method and its bounded retry semantics passed a clean
steady-state smoke. The independent Watch/Subscribe channels are exercised by
the scale-ladder tier; the long gate remains a separate run and keeps periodic
strict snapshots as the correctness oracle because event watches can coalesce
or reconnect.
