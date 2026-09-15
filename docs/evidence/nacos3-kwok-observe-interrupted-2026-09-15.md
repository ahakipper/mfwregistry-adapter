# Nacos 3 + KWork observation report (interrupted)

## Scope and disposition

This is the partial result of the 2-hour `OBS_BURSTS=true` run requested for
the Spotter reliability gate. The run was intentionally stopped by the owner
after approximately 66 minutes so the observation method could be corrected
from periodic snapshots to a stronger three-plane watch design. It is not a
2-hour PASS and must not be used as the final release gate.

- Target: Nacos 3 ARM64 (`nacos/nacos-server:v3.2.4-slim`) + kwok + Spotter
- Configured window: 2h, 1000 base Pods, 20 applications, 10s observation tick,
  20s churn cadence, 5%/min churn, burst schedule enabled, node capacity 1200
- Run stamp: `20260915-2007`
- Stop reason: owner interrupt at `2026-09-15T21:13:37+08:00`
- Cleanup: kwok and Nacos teardown completed; `residual_unknown=false`

## Observed evidence before interruption

The raw per-tick JSONL is preserved at
`tests/observe/results/20260915-2007-ticks.jsonl`.

- 384 records were written (including the cold-attach warm-up record).
- 383 records were `CONSISTENT` and exact-equal; 0 were `OBSERR`.
- The single `DIVERGENT` record was the initial cold-attach snapshot with 454
  in-flight instances and an empty remote catalog; it was outside the steady
  window and the run never reached final acceptance evaluation.
- During the observed window, source and Nacos counts tracked each other from
  1000 to 1242, with no field-divergence record, no dropped event, and no
  unexplained remote extra.
- Maximum in-flight count was 454 during cold attach; maximum same-tick retry
  wait was 31.548s and maximum retry attempts was 19.

## Accuracy boundary

This run used a fresh official SDK catalog read and a live K8s `kubectl get`
snapshot on every 10s tick. It therefore proves sampled full-Instance equality
and bounded convergence, not that a transient mismatch could never appear and
disappear between ticks. The Spotter histogram starts at the informer enqueue
timestamp and ends at the Nacos sink call; it is not an independent third
watcher of Spotter's internal cache.

Conclusion: sampled consistency evidence is strong but incomplete for the
requested K8s-Watch → Spotter → Nacos-Watch latency proof. The next stage adds
independent source and sink watch channels plus a test-only Spotter projection
read, validates that observer itself, and only then restarts the long gate.
