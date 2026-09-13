# Nacos ARM64 Persistent Application-Batch Evidence — 2026-09-13

## Scope

This evidence separates the Nacos protocol-level batch capability from the
Spotter application-level persistent batch executor.

* **Protocol batch:** the pinned SDK's `BatchRegisterInstance` operation is an
  ephemeral-instance API. Against Nacos 2.1.0, the guarded SDK evaluation
  returned `RequestHandler Not Found`; the result is recorded as
  `NOT_VERIFIED/unsupported`, not as a production PASS.
* **Spotter application batch:** a full snapshot is grouped by
  namespace/group/service/cluster/operation, split into at most 100 items per
  application batch, and each persistent item uses the official SDK's
  single-instance register/deregister call with bounded concurrency. Persistent
  semantics (`Ephemeral=false`) are preserved.

## Guarded gate

```bash
go test -race -tags=nacos_real ./tests/e2e \
  -run '^TestNacosRealPersistentApplicationBatch$' -count=1
```

The test requires `NACOS_SERVER`, `NACOS_REAL_SCRATCH=1` and
`NACOS_REAL_ALLOW_WRITE=1`. Without these guards it skips explicitly as
`NOT VERIFIED: EnvError` and produces no release evidence. The gate creates 201
instances for one application, injects one cluster-admin request failure,
retries the full application batch, verifies exactly 201 Spotter-owned catalog
entries, and deregisters all entries. Cleanup must report
`residual_unknown=false`.

## Evidence record

| Field | Result |
|---|---|
| Target image/digest | Pending guarded ARM64 run; expected `nacos/nacos-server:v2.1.0-slim` (`sha256:e689b1c79ca4a391fc478b6b28eac74916bfe569f37abaa8145c156cefe45067`) |
| SDK pseudo-version | `github.com/nacos-group/nacos-sdk-go/v2 v2.3.6-0.20260902123754-002486583df5` (upstream `0024865`) |
| Namespace / group | Supplied by `NACOS_NAMESPACE` / `NACOS_GROUP`; never recorded with credentials |
| Application / cluster | Unique per run; cluster is `spotter-real-batch` |
| Desired entries | 201 persistent instances |
| Batch size | Maximum 100; expected partition `100 + 100 + 1` |
| Configured concurrency cap | Sink bounded SDK item-call limit (`DefaultPushConcurrency=8`); the gate records this configured cap in its PASS line |
| Injected error / retry | One deterministic cluster-admin request failure, followed by a successful full-batch retry |
| Final catalog hash | Recorded in the guarded test PASS log; pending until a writable target is supplied |
| Cleanup | All 201 entries deregistered through the persistent application-batch path; `residual_unknown=false` required |

## Status

The application-batch implementation and unit/race coverage are present. A
real ARM64 scratch PASS is intentionally **PENDING** until the guarded command
is run against an explicitly authorized writable target. The protocol-level
ephemeral batch rejection must not be used to fail the persistent application
batch gate.
