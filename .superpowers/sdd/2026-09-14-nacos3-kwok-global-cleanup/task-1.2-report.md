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
- Preserved SDK server-list failover, timeout, username/password auth, TLS CA,
  server-name, and insecure-skip-verify configuration, plus isolated cache
  ownership and bounded close behavior.
- Added a protobuf contract test that decodes the official
  `naming.InstanceRequest` and asserts `registerInstance`/
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

- A live Nacos 3 endpoint was not available in this workspace, so wire-level
  round-trip success and server-side failover were not exercised end-to-end.
- The adapter relies on the pinned SDK's exported `NamingGrpcProxy`; upgrading
  the pseudo-version requires rerunning the protobuf contract and package race
  tests before release.
