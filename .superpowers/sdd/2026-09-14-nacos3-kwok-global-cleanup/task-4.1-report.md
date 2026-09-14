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
git diff --check (PASS)
```

The `TestObserveConsistency` two-hour run was not started by this task. Task
4.2 must run it with a real kwokctl cluster and record the complete JSONL
source/cache/Nacos comparison before any scale PASS is claimed.

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
