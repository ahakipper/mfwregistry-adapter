# Task 4.1 report — Nacos 3 Observe harness boundary

Status: PASS for the harness target/readiness and live SDK view boundary.
The long kwok reliability run remains NOT VERIFIED and is a separate Task 4.2
gate.

## Changes

- The throwaway Observe container now targets `nacos/nacos-server:v3.2.4-slim`
  with the verified ARM64 digest
  `sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5`.
- Docker startup pins `--platform linux/arm64`, standalone mode, and disables
  local scratch authentication so the harness does not depend on an external
  auth policy.
- Readiness no longer calls Nacos 2 `/nacos/v1` endpoints. It waits for the
  Nacos 3 naming gRPC listener on HTTP-port+1000 (8848→9848).
- After the transport gate, `TestObserveConsistency` now runs the public
  `pkg/nacos.CheckReadinessWithConfig` SDK read/list plus persistent
  register/deregister canary before starting the child. A listener that accepts
  TCP but cannot complete naming operations therefore fails closed.
- The live Observe view constructs `pkg/nacos` with `TransportSDK` and reads
  catalog instances through the official SDK facade. Its write probe and
  cleanup probe also use `Client.RegisterInstance`/`DeregisterInstance`.
- The historical HTTP view remains only for explicitly constructed unit-test
  fixtures; `newNacosView` never initializes that compatibility client.
- Live SDK sessions are closed with the Observe test lifecycle.

## Verification

```text
go test -tags observe ./tests/observe -count=1 (PASS)
go test -tags observe ./tests/observe -run 'TestNacos|Test.*Lifecycle|TestObserveUnit' -count=1 (PASS)
go vet -tags observe ./tests/observe (PASS)
git diff --check (PASS)
```

The `TestObserveConsistency` two-hour run was not started by this task. Task
4.2 must run it with a real kwokctl cluster and record the complete JSONL
source/cache/Nacos comparison before any scale PASS is claimed.

The acceptance record distinguishes `exactEqual` ticks from transitional
ticks. A tick is exact only when the bidirectional comparison has no missing,
extra, duplicate, or disabled mismatch. A mutation-period mismatch remains
visible in JSONL and must resolve within `OBS_BOUND`; it is not silently
treated as exact.

The exact projection is intentionally explicit: membership by application,
group, and `k8s` cluster; IP; first port 7096; service name; composite instance
ID; enabled/healthy policy; `Ephemeral=false`; and the stable metadata keys
`spotterOwner`, `instanceId`, and `status`. The source side also records
`expectedCount` (online plus unhealthy representable Pods) separately from the
raw Pod count, so Pending Pods are not mistaken for missing Nacos instances.
Other Spotter metadata such as `sourceKey`, `sourceCluster`, `reversion`, and
resource fields are not asserted by this controlled Pod snapshot and remain
explicitly unchecked; this harness must not claim full domain-metadata equality.

## Independent review addendum

The first independent review found that a TCP-only gate could allow the child
to start before SDK naming operations were usable. That P1 was fixed by adding
the explicit SDK read/write canary gate described above. The follow-up review
also identified and corrected a stale Makefile comment that still called the
target image the soak stack's tag. Authentication remains intentionally
disabled for this local scratch container; production auth/TLS is outside the
Spotter harness scope.

## Compatibility / Rollback

The active Observe path now requires a Nacos 3 naming gRPC listener and the
official SDK. Unit fixture tests that directly build `nacosView{http: ...}`
retain their isolated HTTP mock behavior. Reverting this task restores the
historical Nacos 2 readiness/view behavior but must not be used as a production
target.

## Documentation

The target image, digest, readiness boundary, fixture exception, and explicit
Task 4.2 NOT VERIFIED status are recorded in this report. The final evidence
document will be updated only after the real kwok run.
