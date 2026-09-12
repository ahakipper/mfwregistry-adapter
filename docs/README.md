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

Start with [architecture.md](architecture.md) for the big picture; see the
[project README](../README.md) for build and usage basics.
