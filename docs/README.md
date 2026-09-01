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

Start with [architecture.md](architecture.md) for the big picture; see the
[project README](../README.md) for build and usage basics.
