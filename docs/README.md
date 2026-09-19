# spotter Documentation

Documentation set for **spotter**, the discovery-center adapter
(`spotter`) that aggregates instance data from multiple
Kubernetes clusters and Consul/ECS and publishes standardized instance data to
the current Nacos Sink.

> **Current release scope (2026-09-20):** Nacos 3 is the active Sink and data
> plane. Atlas and AppCenter are explicitly excluded from this release. Atlas
> references below are historical/compatibility provenance only; AppCenter
> transport and configuration are deleted, while generic notices remain
> fail-closed. The current implementation baseline is `bb59885`.

> **Batch acceptance boundary:** a successful official SDK persistent RPC is a
> successful write attempt. Spotter does not synchronously read the same
> application batch back from Nacos, because catalog/SDK visibility may lag an
> acknowledged write under churn. `PushAll` still prunes only against the
> local desired identity set, and periodic canonical reconcile plus the
> three-watch Observe harness verify eventual full-field equality. Catalog lag
> is therefore not converted into a false retry.

| Document | Description |
| --- | --- |
| [architecture.md](architecture.md) | Architecture design: system context, layering, core interfaces, data flows, leader election, consistency and reliability mechanisms, provider details, configuration, and known technical debt. |
| [data-model.md](data-model.md) | The `Instance` model pushed to the discovery center: field-by-field mapping per provider, status/state enums, `PortInfo`, the compatibility label scheme and `Reversion` semantics. |
| [operations.md](operations.md) | Operational runbook: build and run instructions, flags reference, environment presets, metrics/alerting reference, failure scenarios and deployment notes. |
| [system-readiness-consistency-audit-2026-09-12.md](system-readiness-consistency-audit-2026-09-12.md) | Current-state readiness audit: unfinished-item verification, Nacos HTTP/SDK/gRPC assessment, mandatory SDK migration defect, K8s event-path risks, Reconcile/Sink closure and prioritized optimization plan. |
| [system-readiness-consistency-remediation-plan-2026-09-12.md](system-readiness-consistency-remediation-plan-2026-09-12.md) | v6 remediation plan: mandatory official Nacos SDK/facade adoption, ordered P0/P1/P2 work packages, complete SDK test matrix, E2E gates, migration/rollback rules and release blockers. |
| [atlas-wire-compatibility.md](atlas-wire-compatibility.md) | **Excluded-scope record:** historical Atlas JSON-mirror limitations and the guarded `atlas_real` test; no current Atlas implementation gate. |
| [evidence/nacos-arm64-scratch-2026-09-13.md](evidence/nacos-arm64-scratch-2026-09-13.md) | Real ARM64 local Nacos SDK lifecycle evidence with immutable image, port mappings, cleanup status, and explicit production evidence limits. |
| [nacos-sdk-provenance.md](nacos-sdk-provenance.md) | Pinned official SDK v3 commit/checksums, persistent gRPC boundary, expiry, and lifecycle CI gate. |
| [remediation-execution-status-2026-09-13.md](remediation-execution-status-2026-09-13.md) | Current implementation, test evidence, release blockers, and external verification boundary. |
| [superpowers/plans/2026-09-14-nacos3-kwok-global-cleanup.md](superpowers/plans/2026-09-14-nacos3-kwok-global-cleanup.md) | Active Nacos 3, real kwok scale, health-policy, batching, and legacy-global removal plan. |
| [evidence/nacos3-kwok-target-2026-09-14.md](evidence/nacos3-kwok-target-2026-09-14.md) | Current Nacos 3 SDK provenance, ARM64 target, kwok acceptance schema, and evidence status. |
| [evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md](evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md) | Fresh one-hour 1000-Pod/20-application evidence for complete Instance, labels, Reversion, and exact per-tick source/Nacos equality. |
| [observation-method-validation-2026-09-17.md](observation-method-validation-2026-09-17.md) | Corrected K8s Watch → Spotter internal-cache/pre-worker → Nacos Subscribe observation model, stable-cut equality oracle, percentile semantics, and current diagnostic evidence. |
| [evidence/nacos3-kwok-continuous-watch-scale-pass-2026-09-17.md](evidence/nacos3-kwok-continuous-watch-scale-pass-2026-09-17.md) | Full 1/10/100/500/1000 KWOK continuous-watch scale evidence with 10,220 per-instance samples and strict batch min/median/max. |
| [evidence/nacos3-kwok-20m-root-cause-2026-09-19.md](evidence/nacos3-kwok-20m-root-cause-2026-09-19.md) | Failed 20-minute/1000-Pod run root cause: harness no-op deletes, K8s cache resurrection race, remediation evidence, and the mandatory revalidation gate. |
| [evidence/nacos3-kwok-reconcile-race-closure-2026-09-19.md](evidence/nacos3-kwok-reconcile-race-closure-2026-09-19.md) | Three-run fail/find/fix closure for mixed-time reconcile deletion and crash/recovery ownership, ending in a clean 10-minute/1000-Pod collision PASS. |
| [evidence/nacos3-kwok-20m-pass-2026-09-19.md](evidence/nacos3-kwok-20m-pass-2026-09-19.md) | Fresh 20-minute/1000-Pod Nacos 3 + KWOK PASS with exact three-watch correlation, full Instance equality, burst wall/per-item timings, and P90/P95/P99 latency. |
| [superpowers/plans/2026-09-19-nacos-only-stability-e2e-plan.md](superpowers/plans/2026-09-19-nacos-only-stability-e2e-plan.md) | Authoritative Nacos-only implementation, canonical reconcile, batch/retry, E2E, Observe, and final release plan. |
| [superpowers/plans/2026-09-15-remaining-closure.md](superpowers/plans/2026-09-15-remaining-closure.md) | Superseded historical closure plan; retained for provenance only. |

Start with [architecture.md](architecture.md) for the big picture; see the
[project README](../README.md) for build and usage basics.
