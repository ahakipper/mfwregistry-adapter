# Task 1.1 report — Nacos 3 SDK pin and RED routing seam

Status: DELIVERED_WITH_LIMITS (RED tests captured; production gRPC vendor adapter remains a follow-up task).

## Scope completed

- Confirmed there is no released `nacos-sdk-go/v3` tag in the module proxy.
- Pinned the official `v3.x-dev` pseudo-version
  `v3.0.0-20260831100852-93a93504cc2f`, resolving to commit
  `93a93504cc2fc450c702e60c82d7f81acffe5f28`.
- Added the Spotter-owned `nacos3SDKVendor` seam with explicit persistent
  lifecycle, complete reads, service listing, subscription, and close methods.
- Routed `Client.RegisterInstance` / `DeregisterInstance` through the
  operation-specific persistent seam and forced `Ephemeral=false` at its
  boundary.
- Added focused RED coverage for persistent operation routing, gRPC request
  intent, namespace/group/service/cluster identity, and metadata preservation.

## TDD evidence

The new test was first run before the seam implementation:

```text
go test ./pkg/nacos -run TestNacos3FacadeOwnsVendorOperations -count=1
FAIL: *sdkNamingFacade does not implement nacos3Vendor (missing method Close)
```

After the minimal seam and dependency changes, focused package tests pass:

```text
go test ./pkg/nacos -count=1
ok   spotter/pkg/nacos
```

`go test ./... -count=1` compiled the repository but an unrelated existing
`spotter/pkg/distribute/election` timing test (`TestWaitTickFiresLeaderCheckAfterClockAdvance`)
failed; no Nacos package failures were observed.

## Compatibility / rollback

The explicit `http-compat` transport remains available for legacy fixtures.
Rollback is a single commit revert; no push or external deployment was done.
The vendor field is intentionally injectable so Task 1.2 can replace the
current SDK delegate path with direct Nacos gRPC request emission.

## Documentation

Target and module-resolution evidence is recorded in
`docs/evidence/nacos3-kwok-target-2026-09-14.md`.
