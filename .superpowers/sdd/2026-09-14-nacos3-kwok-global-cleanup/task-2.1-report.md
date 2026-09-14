# Task 2.1 report — Nacos deployment-owned health policy and admin opt-in

Status: PASS

## Problem

Task 2.1 requires a typed health policy that treats deployment-provisioned
cluster health checks as the default and allows naming operations (register,
query, deregister, subscribe/query) without an injected cluster-admin facade.
At the same time, an explicit admin-managed policy must still preflight cluster
health-check updates and fail construction/runtime if the preflight is not
available.

## Changes

- Added `HealthPolicy` in `pkg/nacos/client.go` with:
  - `HealthPolicyDeploymentOwned` (default/zero value),
  - `HealthPolicyAdminManaged`,
  - effective-value normalization and strict validation.
- Threaded `HealthPolicy` through `ClientConfig` and `NewClientWithConfig`, so
  SDK construction and health-policy validation are explicit and typed.
- Narrowed SDK sink gating in `pkg/nacos/nacos.go`:
  - the admin facade is only required when `HealthPolicyAdminManaged` is set;
  - default/`deployment-owned` construction no longer requires cluster-admin.
- Made SDK health check updater and cluster config update marker logic conditional on
  `HealthPolicyAdminManaged` only.
- Added/updated tests in `pkg/nacos/sdk_test.go` and `pkg/nacos/sink_test.go`:
  - `NewSinkWithConfig` constructor behavior by policy (no-admin default/deployment-owned
    succeeds, admin-managed fails fast before writes).
  - runtime naming operations under deployment-owned policy do not require admin and
    still call SDK naming methods directly.
  - blackbox registration under deployment-owned policy skips cluster health update,
    while admin-managed/legacy HTTP path behavior remains intact.
- Plumbed typed health policy into config/runtime wiring:
  - `internal/infra/config/config.go` now carries `NacosHealthPolicy` in
    `Flags`/`Config`;
  - `internal/composition/root.go` now carries `NacosHealthPolicy` in
    `Deps`/`Runtime` and defaults it from config when empty.

## Verification

```text
go test ./pkg/nacos -run 'TestSDKSinkConstructionDeploymentOwnedPolicyDefaultsToNoAdmin|TestSDKSinkConstructionNeedsAdminWhenHealthPolicyIsAdminManaged|TestSDKRuntimeNamingCallsDoNotRequireAdminUnderDeploymentPolicy|TestSDKAdminManagedPolicyStillPreflightsAdminForPush|TestBlackboxSinkFirstRegisterInDeploymentOwnedModeSkipsServerSideHealthUpdate|TestBlackboxSinkFirstRegisterDisablesServerSideHealthCheck'
ok  	spotter/pkg/nacos

go test ./internal/infra/config ./internal/composition
ok  	spotter/internal/infra/config
ok  	spotter/internal/composition

go test ./pkg/nacos -run '^TestDefaultConstructorsUseSDKAndNeverAllocateCompatHTTP$' -count=1
ok  	spotter/pkg/nacos
```

## Compatibility / Rollback

`TransportSDK` remains the default transport mode. `TransportHTTPCompat` remains
available for rollback/fixture usage and remains explicitly opt-in.

Health policy defaults to `deployment-owned` (no cluster-admin requirement for naming
operations). This is a behavior change from the previous `cluster-admin` gate during
SDK sink construction and applies only to health-check-control operations.

Rollback scope:

- Revert this task's commit to restore the previous constructor gate on SDK sinks.
- No deployment/push was performed.

## Documentation

Task file updated with the implemented behavior at:
`.superpowers/sdd/2026-09-14-nacos3-kwok-global-cleanup/task-2.1-report.md`
