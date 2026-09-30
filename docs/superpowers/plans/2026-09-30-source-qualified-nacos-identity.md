# Source-Qualified Nacos Identity Plan — 2026-09-30

## Objective

Replace the historical `Provider -> Nacos clusterName` mapping with a
source-qualified mapping while preserving the domain distinction:

- `Provider` is the source type (`k8s` or the legacy Consul value `ecs`);
- `Cluster` is the workload/business cluster carried by a Pod/service;
- `SourceCluster` is the stable logical source-cluster identity;
- Nacos `clusterName` is the configured cluster name (`Cluster`) with
  `SourceCluster` as the source-identity fallback; `Provider` is not prefixed.

This is required for multiple K8s clusters and multiple logical Consul
clusters. A same-name Pod is not the source identity; a Kubernetes UID/source
cluster pair is.

## Compatibility rules

- Source-aware instances use their cluster name directly as the Nacos wire
  cluster; `SourceCluster` is the fallback when no business cluster label is
  available. Legacy instances without source metadata retain `k8s`/`ecs`.
- The raw Pod `cluster`/`cluster-name` label is retained in `Instance.Cluster`
  and canonical metadata; it is not silently repurposed as source identity.
- Consul server addresses remain HA endpoints for one logical source. A
  separate `--consul-cluster-id` identifies a different logical Consul source.
- Existing consumers and old Nacos records require an explicit migration
  review; no dual registration is performed automatically.

## Stages

1. **Contract and source data:** add deterministic source-qualified wire-name
   tests, accept `cluster-name` as a K8s business-cluster label, and attach a
   stable Consul source ID to converted instances.
2. **Sink mapping:** derive register/deregister/batch/prune/health-policy
   scopes from the source-qualified name; preserve legacy fallback for records
   without source identity.
3. **Authoritative reads:** SDK catalog reads enumerate all source-qualified
   clusters and reconstruct `Provider` from canonical metadata rather than
   confusing the wire cluster name with the provider type.
4. **Regression and migration coverage:** same Pod name, same IP/port across
   K8s clusters; two Consul source IDs; full prune isolation; legacy records;
   Reversion/labels round-trip.
5. **Qualification:** focused/race/full gates, Nacos 3 SDK smoke, and the
   corrected KWOK observation. Commit and push each stage with detailed
   English messages.

## Rollback

The legacy fallback remains available for instances without source metadata.
If a consumer cannot accept source-qualified names, deployment must stay in
that explicit compatibility mode while the migration is coordinated; silently
merging source clusters is not an acceptable rollback.

## Current execution status

- Stage 0 contract: COMPLETE in this plan.
- Stage 1 source data and source-qualified mapping: COMPLETE; focused tests
  cover K8s/Consul source identities and the `cluster-name` business label.
- Stage 2 authoritative SDK reads and source-aware prune: COMPLETE; SDK,
  provider, and legacy compatibility tests pass.
- Stage 3 migration/regression coverage: COMPLETE; tagged E2E fixtures now
  derive the expected source-qualified wire cluster rather than assuming
  `k8s`/`ecs`.
- Stage 4 final race/full gate: COMPLETE; `make test-all` and tagged E2E pass.
- Stage 5 Nacos 3 smoke: COMPLETE; source-qualified 1-minute KWOK smoke passed
  7/7 exact ticks with clean teardown. Evidence is recorded in
  `docs/evidence/nacos3-source-qualified-smoke-2026-09-30.md`.
