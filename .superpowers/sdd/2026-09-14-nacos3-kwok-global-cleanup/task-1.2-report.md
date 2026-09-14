# Task 1.2 report — Nacos 3 gRPC facade and client construction

Status: DELIVERED_WITH_LIMITS

## Problem

The pinned official `nacos-sdk-go/v3` NamingClient delegates persistent
register/deregister calls to its legacy HTTP proxy. That violated the Stage 1
contract requiring Spotter naming operations to use Nacos 3 gRPC request
objects while preserving persistent (`Ephemeral=false`) semantics.

## Changes

- Added `pkg/nacos/nacos3_grpc.go`, a Spotter-owned adapter around the official
  SDK `naming_grpc.NamingGrpcProxy`.
- Routed persistent register, deregister, complete instance reads, service
  listing, subscriptions, and close through the gRPC proxy. Select/query uses
  `healthyOnly=false`, retaining disabled and unhealthy instances for prune
  and authoritative reads.
- Added a gRPC-backed `sdkNamingClient` shim so production SDK construction no
  longer creates the SDK NamingClient HTTP delegate for business operations.
- Reused the long-lived gRPC facade for instance/catalog reads; each read is a
  direct query RPC and does not create a short-lived cached client.
- Replaced the incorrect per-service `InstanceRequest` lifecycle with the
  official SDK `PersistentInstanceRequest` protobuf over a dedicated SDK RPC
  client. This preserves persistent semantics and allows each logical <=100
  scheduler batch to issue safe per-item writes without replacement loss.
- Persistent request headers are populated through the official SDK security
  resource builder before every RPC, preserving configured authentication.
- Preserved SDK server-list failover, timeout, username/password auth, TLS CA,
  server-name, and insecure-skip-verify configuration, plus isolated cache
  ownership and bounded close behavior.
- Added a protobuf contract test that runs official codec Encode/Decode for
  `naming.PersistentInstanceRequest` and asserts `registerInstance`/
  `deregisterInstance` method names and `Ephemeral=false`.
- Kept the legacy HTTP fixture test explicitly scoped to the no-fallback
  boundary; no product operation falls back to raw HTTP in SDK mode.

## Verification

```text
go test ./pkg/nacos/... -count=1
ok   spotter/pkg/nacos

go test -race ./pkg/nacos/... -count=1
ok   spotter/pkg/nacos
```

Fresh live verification used the Nacos 3.2.4 ARM64 image digest
`sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5`:

```text
NACOS_SERVER=127.0.0.1:38848 NACOS_REAL_SCRATCH=1 NACOS_REAL_ALLOW_WRITE=1 \
  go test -tags=nacos_real ./tests/e2e/... \
  -run '^TestNacosRealPersistentApplicationBatch$' -count=1
PASS: entries=201 batch_max=100 batches=3
final_catalog_hash=ed36aa2483d2ba0feb77c912c5b45825e485753500e5d420bde2914f43763781
residual_unknown=false
```

The SDK-mode black-box fixture serves HTTP only; its lifecycle calls now return
the expected gRPC connection-unavailable error and the test confirms no
`/nacos/v1/ns/instance` request is emitted. The in-package vendor seam tests
cover persistent field mapping, read/list/subscription/close routing, and
error propagation.

## Compatibility / Rollback

`TransportHTTPCompat` and `NewHTTPCompatClient` remain available solely for
legacy fixtures and explicit migration rollback. Reverting this task's commit
restores the previous SDK delegate construction; no push or deployment was
performed.

## Documentation

This report records the implementation and verification evidence for Task 1.2.
The task brief remains the source of acceptance requirements.

## Concerns

- Live Nacos 3.2.4 validation now covers 201 unique persistent entries,
  `100+100+1` logical scheduling, exact cardinality, `Ephemeral=false`, and
  cleanup. Multi-server failover and production authenticated/TLS profiles
  remain untested in this run.
- The adapter relies on the pinned SDK's exported `NamingGrpcProxy`; upgrading
  the pseudo-version requires rerunning the protobuf contract and package race
  tests before release.
