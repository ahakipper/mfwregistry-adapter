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

- Added Stage-2 wiring fix in `internal/server.go`:
  - pass runtime `NacosHealthPolicy` into `nacos.ClientConfig` during sink
    construction (`HealthPolicy: effectivePolicy`);
  - compute effective policy from server config (`deployment-owned` when zero);
  - only create/invoke `nacosAdminFactory` when effective policy is
    `HealthPolicyAdminManaged`.
- Made `NewClientWithConfig` policy-gated for optional `ClusterAdminFactory`:
  - invoke factory only for `TransportSDK + HealthPolicyAdminManaged`.
- Removed ungated batch `ensureClusterHealthCheckDisabled` callsite in
  `pkg/nacos/batch.go` by guarding it with `shouldApplyHealthPolicy()`, so
  deployment-owned paths cannot regress into per-item health-check preflight calls.
- Added/updated tests:
  - `pkg/nacos/sdk_test.go`:
    - `TestSDKClientConfigAdminFactoryIsNotInvokedForDeploymentOwnedPolicy` verifies
      deployment-owned defaults do not call an injected factory;
    - `TestSDKClientConfigAdminFactoryIsInvoked` now asserts factory invocation
      is admin-managed only;
    - `TestSDKClientZeroHealthPolicyDefaultsToDeploymentOwned` verifies zero-policy
      normalization;
    - `TestSDKClientRejectsInvalidHealthPolicy` verifies validation failure;
    - `TestSDKSinkPushUnderDeploymentOwnedPolicySkipsAdminCalls` proves Push under
      deployment-owned policy performs naming register without `UpdateHealthChecker`.
  - `internal/server_test.go`:
    - updated factory-wiring test to set admin-managed policy explicitly;
    - `TestStartProvidersSkipsNacosAdminFactoryForDeploymentOwnedPolicy` proves
      default/deployment-owned wiring does not invoke factory before readiness.
  - `internal/composition/root_test.go`:
    - `TestBuildUsesRuntimeNacosHealthPolicyFromConfigAndOverrides` verifies policy
      propagation from config and explicit deps override.

## Verification

```text
go test ./pkg/nacos -run 'TestDefaultConstructorsUseSDKAndNeverAllocateCompatHTTP|TestSDKClientConfigAdminFactoryIsInvoked|TestSDKClientConfigAdminFactoryIsNotInvokedForDeploymentOwnedPolicy|TestSDKClientZeroHealthPolicyDefaultsToDeploymentOwned|TestSDKClientRejectsInvalidHealthPolicy|TestSDKRuntimeNamingCallsDoNotRequireAdminUnderDeploymentPolicy|TestSDKSinkPushUnderDeploymentOwnedPolicySkipsAdminCalls|TestSDKAdminManagedPolicyStillPreflightsAdminForPush|TestBlackboxSinkFirstRegisterInDeploymentOwnedModeSkipsServerSideHealthUpdate|TestBlackboxSinkFirstRegisterDisablesServerSideHealthCheck'
ok   	spotter/pkg/nacos

go test ./pkg/nacos -run '^TestDefaultConstructorsUseSDKAndNeverAllocateCompatHTTP$' -count=1
ok   	spotter/pkg/nacos

go test ./internal/infra/config ./internal/composition ./internal -run 'TestBuildUsesRuntimeNacosHealthPolicyFromConfigAndOverrides|TestStartProvidersInvokesFreshNacosAdminFactoryForAdminManagedPolicy|TestStartProvidersSkipsNacosAdminFactoryForDeploymentOwnedPolicy'
ok   	spotter/internal/infra/config
ok   	spotter/internal/composition
ok   	spotter/internal

go test ./...
ok   	spotter/...
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
