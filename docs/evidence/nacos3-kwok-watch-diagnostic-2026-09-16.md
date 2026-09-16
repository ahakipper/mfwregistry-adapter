# KWork Watch/Subscribe diagnostic report

## Purpose

This run validated the new independent event observers before executing the
1/10/100/500/1000 ladder. It was intentionally limited to repeated single-Pod
Create/Delete transitions plus one CrashLoopBackOff/recovery transition.

## Result

The diagnostic **failed**, correctly preventing the new long run from
starting.

- Strict fresh K8s/Spotter/Nacos snapshots converged for 10 Create and 10
  Delete samples.
- K8s Watch covered every Create/Delete sample.
- Nacos SDK internal service-info cache visibly received every update, but the
  observer callback was never called. All 20 samples therefore reported
  `nacosWatchObserved=false`.
- Root cause: `nacos3GRPCVendor.Subscribe` registered its callback with an
  empty cluster cache key while Nacos updates were processed under the
  `...@@k8s` cache key. The cached state changed, but the callback lookup could
  never match.
- CrashLoopBackOff was written to Nacos as an unhealthy persistent instance,
  but the strict diagnostic did not converge within two minutes. Recovery did
  converge. This remains open for the next focused run with explicit
  per-plane mismatch diagnostics.

## Evidence

- Generated report:
  `tests/observe/results/20260916-122023-scale-ladder-summary.json`
- Single-Pod snapshot latency samples (not accepted as final percentile
  evidence because the Nacos Watch boundary was missing): steady samples were
  about 1.37-1.48s API-to-exact; the first cold sample was 15.99s.

## Fix and next gate

The callback registration/deregistration keys now include the subscribed
cluster string, with a focused ServiceInfoHolder callback regression test.
The diagnostic will be rerun. Only after Nacos callbacks, K8s Watch coverage,
Spotter projection equality, Crash/recovery, and strict catalog equality all
pass will the full scale ladder and renewed two-hour run start.
