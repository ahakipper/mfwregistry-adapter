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
- Added focused RED coverage for persistent operation routing, vendor seam
  intent, namespace/group/service/cluster identity, and metadata preservation.

## TDD evidence

The new test was first run before the seam implementation:

```text
go test ./pkg/nacos -run TestNacos3FacadeOwnsVendorOperations -count=1
FAIL: *sdkNamingFacade does not implement nacos3Vendor (missing method Close)
```

After the seam was added, the routing assertion reaches the current vendor
delegate and fails for the intended reason (the RED gate):

```text
go test ./pkg/nacos -run TestBlackboxClientSDKModeUsesGRPCPersistentLifecycle -count=1
FAIL: SDK persistent register used legacy HTTP endpoint POST /nacos/v1/ns/instance
```

Existing facade tests remain green:

```text
go test ./pkg/nacos -run 'TestSDKFacadeRoutesPersistentLifecycleAndPreservesFields|TestSDKFacadeSelectAllIncludesDisabledAndMapsHosts' -count=1
ok   spotter/pkg/nacos
```

The complete `pkg/nacos` package is intentionally RED until Task 1.2 wires
the concrete Nacos 3 gRPC vendor adapter.

`go test ./... -count=1` is not a green gate: the intentional
`pkg/nacos` RED black-box assertion fails on the legacy `/v1/ns/instance`
route, and an unrelated existing `spotter/pkg/distribute/election` timing
test (`TestWaitTickFiresLeaderCheckAfterClockAdvance`) also failed.

Follow-up commit adding the explicit black-box RED assertion and this exact
test evidence: `6bdff6d`.

Coverage follow-up commit `46e5cff` extends seam routing assertions to
SelectAll, ListServices, Subscribe, Unsubscribe, Close, identity fields, and
error propagation, and makes the black-box test fail immediately on lifecycle
errors.

Error-coverage follow-up commit `eb65f9f` adds explicit
DeregisterPersistent error propagation and aliases the test seam to the
production contract. Exact verification:

```text
go test ./pkg/nacos -run 'TestNacos3Facade' -count=1   # PASS
go test ./pkg/nacos -run TestBlackboxClientSDKModeUsesGRPCPersistentLifecycle -count=1   # RED: POST /nacos/v1/ns/instance
```

Concrete SDK configuration, failover, and proto decoding remain scoped to
Task 1.2; this task only establishes the injectable operation seam.

## Compatibility / rollback

The explicit `http-compat` transport remains available for legacy fixtures.
Rollback is a single commit revert; no push or external deployment was done.
The vendor field is intentionally injectable so Task 1.2 can replace the
current SDK delegate path with direct Nacos gRPC request emission.

## Documentation

Target and module-resolution evidence is recorded in
`docs/evidence/nacos3-kwok-target-2026-09-14.md`.
