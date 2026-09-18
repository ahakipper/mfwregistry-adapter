# spotter Documentation

Documentation set for **spotter**, the discovery-center adapter
(`spotter`) that aggregates instance data from multiple
Kubernetes clusters and Consul/ECS and pushes standardized instance events to
the discovery center ("Atlas") over gRPC.

| Document | Description |
| --- | --- |
| [architecture.md](architecture.md) | Architecture design: system context, layering, core interfaces, data flows, leader election, consistency and reliability mechanisms, provider details, configuration, and known technical debt. |
| [data-model.md](data-model.md) | The `Instance` model pushed to the discovery center: field-by-field mapping per provider, status/state enums, `PortInfo`, the compatibility label scheme and `Reversion` semantics. |
| [operations.md](operations.md) | Operational runbook: build and run instructions, flags reference, environment presets, metrics/alerting reference, failure scenarios and deployment notes. |
| [system-readiness-consistency-audit-2026-09-12.md](system-readiness-consistency-audit-2026-09-12.md) | Current-state readiness audit: unfinished-item verification, Nacos HTTP/SDK/gRPC assessment, mandatory SDK migration defect, K8s event-path risks, Reconcile/Sink closure and prioritized optimization plan. |
| [system-readiness-consistency-remediation-plan-2026-09-12.md](system-readiness-consistency-remediation-plan-2026-09-12.md) | v6 remediation plan: mandatory official Nacos SDK/facade adoption, ordered P0/P1/P2 work packages, complete SDK test matrix, E2E gates, migration/rollback rules and release blockers. |
| [atlas-wire-compatibility.md](atlas-wire-compatibility.md) | Atlas gRPC wire-compatibility gate: current JSON mirror limitations, mock-versus-real evidence boundary, and the guarded `atlas_real` round-trip test. |
| [evidence/nacos-arm64-scratch-2026-09-13.md](evidence/nacos-arm64-scratch-2026-09-13.md) | Real ARM64 local Nacos SDK lifecycle evidence with immutable image, port mappings, cleanup status, and explicit production evidence limits. |
| [nacos-sdk-provenance.md](nacos-sdk-provenance.md) | Pinned upstream SDK race fix provenance, expiry, and lifecycle CI gate. |
| [remediation-execution-status-2026-09-13.md](remediation-execution-status-2026-09-13.md) | Current implementation, test evidence, release blockers, and external verification boundary. |
| [superpowers/plans/2026-09-14-nacos3-kwok-global-cleanup.md](superpowers/plans/2026-09-14-nacos3-kwok-global-cleanup.md) | Active Nacos 3, real kwok scale, health-policy, batching, and legacy-global removal plan. |
| [evidence/nacos3-kwok-target-2026-09-14.md](evidence/nacos3-kwok-target-2026-09-14.md) | Current Nacos 3 SDK provenance, ARM64 target, kwok acceptance schema, and evidence status. |
| [evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md](evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md) | Fresh one-hour 1000-Pod/20-application evidence for complete Instance, labels, Reversion, and exact per-tick source/Nacos equality. |
| [observation-method-validation-2026-09-17.md](observation-method-validation-2026-09-17.md) | Corrected K8s Watch → Spotter internal-cache/pre-worker → Nacos Subscribe observation model, stable-cut equality oracle, percentile semantics, and current diagnostic evidence. |
| [evidence/nacos3-kwok-continuous-watch-scale-pass-2026-09-17.md](evidence/nacos3-kwok-continuous-watch-scale-pass-2026-09-17.md) | Full 1/10/100/500/1000 KWOK continuous-watch scale evidence with 10,220 per-instance samples and strict batch min/median/max. |
| [evidence/nacos3-kwok-20m-root-cause-2026-09-19.md](evidence/nacos3-kwok-20m-root-cause-2026-09-19.md) | Failed 20-minute/1000-Pod run root cause: harness no-op deletes, K8s cache resurrection race, remediation evidence, and the mandatory revalidation gate. |
| [evidence/nacos3-kwok-reconcile-race-closure-2026-09-19.md](evidence/nacos3-kwok-reconcile-race-closure-2026-09-19.md) | Three-run fail/find/fix closure for mixed-time reconcile deletion and crash/recovery ownership, ending in a clean 10-minute/1000-Pod collision PASS. |
| [superpowers/plans/2026-09-15-remaining-closure.md](superpowers/plans/2026-09-15-remaining-closure.md) | Remaining in-scope closure plan: legacy deletion, two-hour burst gate, status reconciliation, and safe historical commit-message rewrite. |

Start with [architecture.md](architecture.md) for the big picture; see the
[project README](../README.md) for build and usage basics.
