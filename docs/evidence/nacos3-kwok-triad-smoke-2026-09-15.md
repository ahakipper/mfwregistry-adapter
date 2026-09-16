# Nacos 3 + KWork triad smoke report

## Run

- Stamp: `20260915-2243`
- Target: Nacos 3 ARM64 + kwok + Spotter child
- Configuration: 2-minute window, 50 base Pods, 5 services, burst mode on,
  10-second tick, 60-second full-push interval
- Cleanup: kwok, Nacos, Pods, and child cleanup completed with
  `residual_unknown=false`

## Evidence

The raw tick records and generated summary are preserved under
`tests/observe/results/20260915-2243-*`.

- 13 tick records were emitted; 11 were exact-equal across K8s, Spotter, and
  Nacos, with 0 OBSERR.
- At the burst transition, two records showed a short-lived non-atomic
  snapshot: the K8s live GET, Spotter informer projection, and Nacos catalog
  were read at slightly different instants while Pods were changing. The
  records retained the three counts and fingerprints; no silent drop occurred.
- The short duration ended during the scheduled 200-Pod burst rollback, so the
  acceptance correctly failed instead of treating an incomplete burst as a
  pass.

## Method correction

The smoke exposed a test-method issue rather than a Sink data-loss issue:
Spotter projection mismatch was marked immediately as product divergence even
when a source mutation was active and the next read would have linearized.
The observer is being corrected to retry that third-plane mismatch inside the
same bounded tick, exactly as it already retries mutation-attributed
source/Nacos mismatches. If the mismatch persists beyond the bound, the tick
will remain a failure. This prevents false positives without weakening the
three-plane equality requirement.
