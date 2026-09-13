# Commit message rewrite map

Range: `e708630^..e9ea7d2` (175 commits).

### e708630b16fd78c306636d3e7622840d8a9b4d15

docs: Mandate Nacos SDK and formalize remediation gates

Problem:
Production Nacos naming still used raw net/http, so the mandatory SDK or audited facade and its operation and lifecycle gates were not enforced.

Changes:
It updates docs/README.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/README.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 69e86b9a34f5dba4152cb23e2bdff005a897fb09

test: Strengthen deterministic provider and Nacos fixtures

Problem:
The implementation lacked operation-aware fake sink scripts for success, permanent, retryable, timeout, and prune-only failures.

Changes:
It updates internal/testkit/discoverymock/server.go, internal/testkit/discoverymock/server_test.go, internal/testkit/fakes/fakes.go, internal/testkit/fakes/fakes_test.go, internal/testkit/fakes/robot.go, internal/testkit/fakes/robot_test.go, internal/testkit/nacosmock/server.go, internal/testkit/nacosmock/server_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/testkit/discoverymock/server.go, internal/testkit/discoverymock/server_test.go, internal/testkit/fakes/fakes.go, internal/testkit/fakes/fakes_test.go, internal/testkit/fakes/robot.go, internal/testkit/fakes/robot_test.go, internal/testkit/nacosmock/server.go, internal/testkit/nacosmock/server_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### cdc433fe3f0709fcdc528e2e626943fdb9e09754

fix: Make instance identity source-aware across providers

Problem:
Instance identity did not carry stable cluster and UID identity through Kubernetes queue objects, informer lookups, cache keys, compare paths, retries, and Nacos metadata while preserving the legacy wire InstanceId.

Changes:
It updates internal/domain/instance/identity.go, internal/domain/instance/model.go, internal/domain/instance/rules.go, internal/domain/instance/rules_test.go, internal/testkit/fakes/robot.go, internal/testkit/fakes/robot_test.go, pkg/k8srobot/k8srobot.go, pkg/nacos/identity_whitebox_test.go, pkg/nacos/nacos.go, pkg/providers/cache.go, pkg/providers/common.go, pkg/providers/consul/consul.go, pkg/providers/identity.go, pkg/providers/identity_test.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/unsynced_service.go, pkg/worker/worker_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/domain/instance/identity.go, internal/domain/instance/model.go, internal/domain/instance/rules.go, internal/domain/instance/rules_test.go, internal/testkit/fakes/robot.go, internal/testkit/fakes/robot_test.go, pkg/k8srobot/k8srobot.go, pkg/nacos/identity_whitebox_test.go, pkg/nacos/nacos.go, pkg/providers/cache.go, pkg/providers/common.go, pkg/providers/consul/consul.go, pkg/providers/identity.go, pkg/providers/identity_test.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/unsynced_service.go, pkg/worker/worker_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### da83e9ebe37cebbd46f7a9770d6f17ff6ec24a5d

fix: Serialize sink writes and revalidate full snapshots

Problem:
The implementation lacked per-identity sink ordering with an exclusive full-push gate, revision tracking, tombstones, duplicate snapshot rejection, and bounded key-lock lifetimes.

Changes:
It updates internal/server.go, pkg/providers/consul/blackbox_test.go, pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/fanout.go, pkg/worker/fanout_test.go, pkg/worker/ordered_sink_test.go, pkg/worker/types.go, pkg/worker/worker.go, pkg/worker/worker_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/server.go, pkg/providers/consul/blackbox_test.go, pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/fanout.go, pkg/worker/fanout_test.go, pkg/worker/ordered_sink_test.go, pkg/worker/types.go, pkg/worker/worker.go, pkg/worker/worker_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### ea30a0c8741d82e9146f4cb4c399db8b94de56ec

fix: Preserve typed full operations through retry

Problem:
Retry handling lacked typed retry operation contracts with scope, batch identity, sequence, revision, and revalidation metadata.

Changes:
It updates internal/ports/ports.go, internal/testkit/fakes/fakes.go, internal/testkit/fakes/fakes_test.go, pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go, pkg/worker/fanout.go, pkg/worker/fanout_test.go, pkg/worker/types.go, pkg/worker/unsynced_service.go, pkg/worker/worker.go, pkg/worker/worker_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/ports/ports.go, internal/testkit/fakes/fakes.go, internal/testkit/fakes/fakes_test.go, pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go, pkg/worker/fanout.go, pkg/worker/fanout_test.go, pkg/worker/types.go, pkg/worker/unsynced_service.go, pkg/worker/worker.go, pkg/worker/worker_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### ea2156341f6bd70ec8693755d968308e2793676d

fix: Gate Nacos pruning by ownership and empty-source confirmation

Problem:
Nacos registrations had no Spotter ownership metadata, so pruning could delete foreign or unowned instances.

Changes:
It updates internal/ports/ports.go, internal/testkit/nacosmock/server.go, pkg/nacos/nacos.go, pkg/nacos/sink_test.go, pkg/providers/consul/blackbox_test.go, pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/fanout.go, pkg/worker/types.go, pkg/worker/unsynced_service.go, pkg/worker/worker.go, pkg/worker/worker_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/ports/ports.go, internal/testkit/nacosmock/server.go, pkg/nacos/nacos.go, pkg/nacos/sink_test.go, pkg/providers/consul/blackbox_test.go, pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/fanout.go, pkg/worker/types.go, pkg/worker/unsynced_service.go, pkg/worker/worker.go, pkg/worker/worker_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### b1d9e2f827644418d5c8722c764b445f0fe906e4

feat: Productionize Nacos compatibility transport

Problem:
The implementation lacked explicit Nacos server-list failover with deterministic 4xx stop semantics and transport/body-read timeout coverage.

Changes:
It updates cmd/adapter.go, cmd/adapter_test.go, internal/infra/config/config.go, internal/infra/config/config_test.go, internal/server.go, internal/server_test.go, pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/nacos.go, pkg/nacos/sink_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is cmd/adapter.go, cmd/adapter_test.go, internal/infra/config/config.go, internal/infra/config/config_test.go, internal/server.go, internal/server_test.go, pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/nacos.go, pkg/nacos/sink_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 8a8e486b7a6352aff071b90de9c9f144db3905f5

docs: Record B2 compatibility foundation status

Problem:
The remediation documents did not identify b1d9e2f as the B2 HTTP compatibility baseline or record its failover, scoped readiness, canary cleanup, and custom-scope coverage.

Changes:
It updates docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### fd1f539968363bc860b4f6c72769d9a401e9b4b7

feat: Route Nacos naming through official SDK facade

Problem:
Nacos naming lacked nacos-sdk-go/v2 v2.3.5 behind an auditable naming facade with multi-server rotation, gRPC port derivation, namespace/group, username/password, TLS, persistent lifecycle, SelectAll, service-list, subscribe, unsubscribe, and clean shutdown semantics.

Changes:
It updates cmd/adapter.go, cmd/adapter_test.go, docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, go.mod, go.sum, internal/infra/config/config.go, internal/infra/config/config_test.go, internal/server.go, internal/server_test.go, internal/testkit/nacosmock/server.go, pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/nacos.go, pkg/nacos/sdk.go, pkg/nacos/sdk_gate_test.go, pkg/nacos/sdk_test.go, pkg/nacos/sink_test.go, tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is cmd/adapter.go, cmd/adapter_test.go, docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, go.mod, go.sum, internal/infra/config/config.go, internal/infra/config/config_test.go, internal/server.go, internal/server_test.go, internal/testkit/nacosmock/server.go, pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/nacos.go, pkg/nacos/sdk.go, pkg/nacos/sdk_gate_test.go, pkg/nacos/sdk_test.go, pkg/nacos/sink_test.go, tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 5349e0cca83f2a44d19fbc6e08693f487cb562fe

docs: Publish B3 SDK seam baseline

Problem:
The documented baseline did not identify fd1f539 as the point where Nacos naming defaulted to the official SDK facade.

Changes:
It updates docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### c8e56138638cf98e366517f4c30aa69925729d22

test: Add fail-closed Atlas wire compatibility gate

Problem:
The documentation omitted the current Atlas mirror-struct and forced-JSON codec boundary, exact RPC method paths, mock-versus-real evidence distinction, and the required target-version/protobuf/TLS/auth evidence.

Changes:
It updates docs/README.md, docs/atlas-wire-compatibility.md, pkg/beehive/service/v2/wire_compat_test.go, tests/e2e/atlas_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/README.md, docs/atlas-wire-compatibility.md, pkg/beehive/service/v2/wire_compat_test.go, tests/e2e/atlas_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/README.md, docs/atlas-wire-compatibility.md

### cce983e0a16a735a9950139e049ddf8fbdbfec4d

fix: Harden observe timestamps and mutation ledger timing

Problem:
Observe timestamp handling did not parse zap JSON timestamps from the ts field, including RFC3339 offsets, numeric epoch values with exact nanosecond text handling, plain local timestamps, and wrapper prefixes.

Changes:
It updates docs/system-readiness-consistency-remediation-plan-2026-09-12.md, tests/observe/child.go, tests/observe/child_test.go, tests/observe/churn.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/system-readiness-consistency-remediation-plan-2026-09-12.md, tests/observe/child.go, tests/observe/child_test.go, tests/observe/churn.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### eb6bf0cc449169d16994e5d90d13ccb63a290696

fix: Clear baseline go vet diagnostics

Problem:
The cache expiration logger passed an argument-only Sprintf call, and the Kubernetes resource descriptor used an unkeyed literal that go vet flagged.

Changes:
It updates pkg/providers/k8s/k8s.go, tools/cache/cache.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/k8s/k8s.go, tools/cache/cache.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 49732ee19c46b329a702592ab8dc52639946ade9

docs: Synchronize readiness and release gate status

Problem:
Architecture, operations, testing, and remediation documents still omitted the eb6bf0c baseline, SDK default, audited HTTP exceptions, and clean go vet result.

Changes:
It updates docs/architecture.md, docs/operations.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/architecture.md, docs/operations.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### c3401faec38ba04f30c764112f66c132ae57c968

docs: Make DDD and notification gaps explicit

Problem:
The active graph still depended on provider and elector logger/notifier globals, dormant aggregate scaffolding, and a log-only AppCenter notice path.

Changes:
It updates docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 75a151b22ce4b8bfe2dae06942d652002d6d7538

fix: Synchronize K8s cache pointer snapshots

Problem:
Swappable K8s cache-pointer reads were not synchronized with the provider mutex, allowing flushInstances and CompareAndFlush to race cache replacement.

Changes:
It updates pkg/providers/k8s/k8s.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/k8s/k8s.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 403e0c54687d62b3a3ce8fb0e3371f46a19d3229

fix: Synchronize K8s provider stop state

Problem:
The provider stopped flag was read without the mutex, so Robot Pop and Run shutdown lacked a happens-before edge during cancellation.

Changes:
It updates pkg/providers/k8s/k8s.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/k8s/k8s.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 649ce2c568728f1b7503ca69af3e6929db615b92

docs: Advance final remediation baseline

Problem:
The readiness baseline omitted the latest K8s cache-pointer and provider-stop race fixes and their current verification state.

Changes:
It updates docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 728f1d2cb658c466f0b3fa0cb156434934d57fd5

fix: Preserve scoped full reconcile and bound provider backpressure

Problem:
Normal SyncAll events did not carry Scope, BatchID, Sequence, revalidation, and EmptyConfirmed through the typed FullOperationSink contract.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, pkg/providers/consul/consul.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/fanout.go, pkg/worker/fanout_test.go, pkg/worker/ordered_sink_test.go, pkg/worker/worker.go, pkg/worker/worker_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, pkg/providers/consul/consul.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/worker/fanout.go, pkg/worker/fanout_test.go, pkg/worker/ordered_sink_test.go, pkg/worker/worker.go, pkg/worker/worker_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 702373d5ccb45cbd34b2d3c24d64fbfc8e52172b

docs: Reconcile audit with current closure status

Problem:
The audit and Nacos plan still treated fixed R1-R7 and K2-K4 findings as current and did not identify the remaining production gates.

Changes:
It updates docs/architecture.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/architecture.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### abe822c521294d0f5e617397988f4d71476b6b37

docs: Pin final audit provenance

Problem:
The documented baseline had not advanced to the audit and remediation-plan baseline to 702373d so current status tables and historical-risk annotations match the pushed branch exactly.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 24e3a6caf7e7bb21bcfc3e149da2c6014408ae03

docs: Align testing baseline with final implementation

Problem:
The testing guide did not point at implementation baseline 702373d or distinguish offline PASS from tagged real-environment SKIP and production evidence gates.

Changes:
It updates docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/testing.md

### ff10610f9e123994f9a1ea88a5f44a8512636555

fix: Requeue provider overflow and join shutdown goroutines

Problem:
Provider saturation had no bounded identity-keyed overflow queue, nonblocking retry, capacity-drop evidence, or dispatcher join on close.

Changes:
It updates pkg/providers/consul/blackbox_test.go, pkg/providers/consul/consul.go, pkg/providers/consul/monitor.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/providers/overflow.go, pkg/providers/overflow_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/consul/blackbox_test.go, pkg/providers/consul/consul.go, pkg/providers/consul/monitor.go, pkg/providers/k8s/k8s.go, pkg/providers/k8s/k8s_whitebox_test.go, pkg/providers/overflow.go, pkg/providers/overflow_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 42ecb89ac56800597e128774bee906d3752bb9ea

refactor: Complete dependency-injected provider and notice paths

Problem:
Production Kubernetes and Consul providers, conversion, elector, metrics, and server initialization still relied on ambient globals instead of injected collaborators.

Changes:
It updates cmd/adapter.go, internal/composition/boundary_test.go, internal/composition/root.go, internal/infra/config/config.go, internal/infra/notice/notice.go, internal/infra/notice/notice_test.go, internal/server.go, pkg/metrics/proserver.go, pkg/providers/aggregate/controller.go, pkg/providers/consul/consul.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/worker/elector.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is cmd/adapter.go, internal/composition/boundary_test.go, internal/composition/root.go, internal/infra/config/config.go, internal/infra/notice/notice.go, internal/infra/notice/notice_test.go, internal/server.go, pkg/metrics/proserver.go, pkg/providers/aggregate/controller.go, pkg/providers/consul/consul.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/worker/elector.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### e025223f0c4c3e98a26d64a2504aa1710fcb5c17

docs: Record C2 dependency injection and notice gates

Problem:
The DDD and remediation documents did not record the active-graph injection, aggregate isolation, notifier lifecycle, and payload-redaction work.

Changes:
It updates docs/ddd-architecture.md, docs/operations.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/ddd-architecture.md, docs/operations.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 3a60347e2995e0bf4a0b1776bbbeb76ba8d223c1

docs: Record D1 and C2 release boundaries

Problem:
The remediation record did not identify e025223 as the implementation baseline or record bounded overflow joins, injection, aggregate isolation, and shutdown-aware notifications.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 9dcba2dab6bb81b28bc53fc516ab61a588d239f3

test: Expand real Nacos SDK operation gate

Problem:
The real Nacos gate stopped after persistent register/deregister and lacked SelectAll, service-list, subscribe/unsubscribe, and catalog coverage.

Changes:
It updates tests/e2e/nacos_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### dcd28877d1c7f114e3f37871f9f102968f3a1d2b

test: Cover appcenter notice configuration wiring

Problem:
The tests did not exercise the new appcenter endpoint, bearer token, timeout, and retry flags through Cobra adapterFlags and infra Config resolution.

Changes:
It updates cmd/adapter_test.go, internal/composition/root_test.go, internal/infra/config/config_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is cmd/adapter_test.go, internal/composition/root_test.go, internal/infra/config/config_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 0eb64368d1e888be3189bb7107e5b199fa954f82

docs: Finalize C2 and C3 verification status

Problem:
The audit and remediation documents did not identify dcd2887 as the baseline for expanded Nacos operations, provider backpressure, injection, notifier lifecycle, and vet closure.

Changes:
It updates Makefile, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is Makefile, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### b38505c7be3fb82947d5c09a5161fba610d52e91

fix: Initialize provider lifecycle stop channels

Problem:
Manually constructed K8s and Consul providers could omit a shutdown channel, allowing queue-depth and periodic goroutines to leak.

Changes:
It updates pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/consul/consul.go, pkg/providers/k8s/k8s.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 8a284b05ec935528a4cb7e5e4f6b7944933625ea

docs: Synchronize final remediation evidence

Problem:
The implementation baseline did not record b38505c or the clean full, race, coverage, and vet evidence after D1/C2 lifecycle closure.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 0d814d913491c2ca0c7c99cc5ab4e224d7f68a2b

docs: Record scratch Nacos environment evidence

Problem:
The evidence set did not record the attempted Nacos 2.1.0 scratch run, its pinned image digest, ARM-to-amd64 startup limitation, or cleanup result.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md

### 3c7243082e676587d0f797a56c53acafed120eee

docs: Align historical audit with staged closure

Problem:
The audit still presented original tables as current and did not identify the completed SDK, dependency-injection, overflow, cache, and reconciliation fixes.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 7a44d147eec62507359464410c3b9332eedec640

docs: Pin remediation provenance to current head

Problem:
The current documentation lacked the audit and remediation plan to the current 3c72430 documentation baseline while retaining b38505c as the implementation baseline.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 7e9ec06f5396a71b9e746ff0fcaae3ee2b6ca1d8

fix: Enforce SDK-only Nacos production transport

Problem:
Catalog, prune, service-list, and readiness still had paths that could allocate the raw HTTP client in SDK mode.

Changes:
It updates cmd/adapter.go, docs/architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, internal/infra/config/config.go, internal/server.go, internal/server_test.go, pkg/nacos/client.go, pkg/nacos/nacos.go, pkg/nacos/sdk.go, pkg/nacos/sdk_gate_test.go, pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is cmd/adapter.go, docs/architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, internal/infra/config/config.go, internal/server.go, internal/server_test.go, pkg/nacos/client.go, pkg/nacos/nacos.go, pkg/nacos/sdk.go, pkg/nacos/sdk_gate_test.go, pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 771dfd6cec4de76ddb589bf6e6c95c6fdad07fa1

fix: Fail closed when SDK lacks cluster admin

Problem:
SDK-mode Nacos sink registrations could succeed even when the pinned official naming SDK could not apply the required cluster health-check policy.

Changes:
It updates docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, pkg/nacos/nacos.go, pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, pkg/nacos/nacos.go, pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 8bb68237d613966b1bb55f438b727b97ee5390b8

docs: Clarify SDK cluster-admin fail-closed gate

Problem:
The readiness audit and staged plan did not yet state that strict SDK transport was active and the cluster-admin capability remained a startup gate.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 3370746825a8ed0de40091f58cbbd4d350d3d580

nacos: Fail fast when SDK sink lacks cluster admin control

Problem:
SDK sink construction could proceed even though the pinned naming SDK could not configure the required server-side health checker.

Changes:
It updates pkg/nacos/nacos.go, pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/nacos.go, pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 54768c91cec8138002916d8f7c33d2cee72dacdc

server: Validate Nacos sink capability before readiness side effects

Problem:
The server ran readiness before constructing the Nacos sink, so unsupported SDK capabilities could be discovered only after side effects.

Changes:
It updates internal/server.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/server.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 81e18f0ef7d1e96373885fc3c2b26e455438532b

docs: Record Nacos SDK startup capability blocker

Problem:
The documentation omitted that SDK-only sink construction fails before readiness side effects while the pinned Nacos Go SDK lacks the required cluster-admin health-check operation.

Changes:
It updates docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 5982e656e48b1e9312c37afbe66118d96ad38f03

refactor: Isolate remaining legacy global compatibility reads

Problem:
Residual config, logger, and notifier global reads were still exposed outside the internal infra legacycompat boundary.

Changes:
It updates docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, internal/composition/boundary_test.go, internal/infra/legacycompat/legacy.go, internal/infra/legacycompat/legacy_test.go, pkg/metrics/proserver.go, pkg/providers/consul/consul.go, pkg/providers/consul/monitor.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/worker/elector.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, internal/composition/boundary_test.go, internal/infra/legacycompat/legacy.go, internal/infra/legacycompat/legacy_test.go, pkg/metrics/proserver.go, pkg/providers/consul/consul.go, pkg/providers/consul/monitor.go, pkg/providers/k8s/conversion.go, pkg/providers/k8s/k8s.go, pkg/worker/elector.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 91cdbda5e8720358160d0a232e051084f9774737

refactor: Close election and metrics legacy boundaries

Problem:
The election package still imported ambient config for the empty campaign-key fallback instead of using the sole legacycompat adapter.

Changes:
It updates docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, internal/composition/boundary_test.go, internal/infra/legacycompat/legacy.go, pkg/distribute/election/election.go, pkg/metrics/proserver.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, internal/composition/boundary_test.go, internal/infra/legacycompat/legacy.go, pkg/distribute/election/election.go, pkg/metrics/proserver.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/ddd-architecture.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 55dfc3b1bda84d24ed3d8baaf42ce5a440571255

e2e: Harden Atlas real wire gate transport and cleanup

Problem:
The implementation lacked guarded TLS CA, server name, insecure scratch-only mode, and bearer metadata options to the opt-in Atlas gate while preserving JSON codec semantics.

Changes:
It updates tests/e2e/atlas_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/atlas_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### b3f55fc0ce1b6faf0a2c9ad231600643a067ca65

e2e: Secure Atlas canary cleanup on every exit

Problem:
The code allowed bearer authentication over plaintext unless explicitly scratch-authorized, and install deferred canary cleanup immediately after registration.

Changes:
It updates tests/e2e/atlas_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/atlas_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### a31eeca2877643558f415ad72c3ab40cc5a1cdd4

e2e: Enforce secure Atlas auth and cleanup failures

Problem:
The implementation did not derive tLS transport from the endpoint or explicit opt-in, require secure bearer credentials by default, and mark canary writes before dispatch so deferred cleanup covers transport failures.

Changes:
It updates tests/e2e/atlas_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/atlas_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 9f5e66553f72629c4de3a039e16968bdfeb6bbcc

e2e: Require full scratch guards for insecure Atlas auth

Problem:
The gate did not require scratch, write permission, and explicit insecure-auth opt-in before allowing bearer metadata over plaintext.

Changes:
It updates tests/e2e/atlas_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/atlas_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 8b563a576ea157b2d6cbf9eddca11c426302f80d

e2e: Treat insecure TLS as secure auth transport

Problem:
Atlas TLS intent detection ignored insecure-skip-verify, so explicit insecure TLS bearer-auth cases were not covered.

Changes:
It updates tests/e2e/atlas_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/atlas_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 5d9988e2016a82356592243ea5b16f05d0289f9e

test: Bound Observe harness commands and teardown

Problem:
The harness remained vulnerable to the observe stack against hung external commands and child lifecycle races.

Changes:
It updates docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, tests/observe/child.go, tests/observe/churn.go, tests/observe/lifecycle_test.go, tests/observe/stack.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, tests/observe/child.go, tests/observe/churn.go, tests/observe/lifecycle_test.go, tests/observe/stack.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 28c2280b95a463f430a16bd3c02402fb8d7c76c0

test: Classify Observe environment failures and teardown

Problem:
The full Observe consistency test treated missing host prerequisites as ordinary failures instead of explicit NOT VERIFIED environment or infrastructure outcomes.

Changes:
It updates tests/observe/child.go, tests/observe/observe_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/observe/child.go, tests/observe/observe_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 64fb79dd70926a990fc5eaecc18dc8ac8ab2a2fa

notice: Cancel in-flight deliveries during shutdown

Problem:
The notifier lacked the AppCenter notifier an owned context and cancel function so Close interrupts active HTTP requests and retry backoff waits.

Changes:
It updates internal/infra/notice/notice.go, internal/infra/notice/notice_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/infra/notice/notice.go, internal/infra/notice/notice_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### e6072546381a9576e311c6ebbda1f6a22d4f815e

test: Harden guarded Nacos real gates

Problem:
The implementation lacked a shared Nacos real-target parser for the SDK-only gates.

Changes:
It updates docs/testing.md, tests/e2e/nacos_real_config.go, tests/e2e/nacos_real_config_test.go, tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/testing.md, tests/e2e/nacos_real_config.go, tests/e2e/nacos_real_config_test.go, tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/testing.md

### 019dc618519d60e2c77b863082250b38f3a3e266

test: Make Observe Docker teardown evidence fail closed

Problem:
The implementation lost stderr from context-bounded Docker commands so the harness can distinguish an explicit not-found container from daemon, timeout, and other infrastructure failures.

Changes:
It updates docs/testing.md, tests/observe/lifecycle_test.go, tests/observe/stack.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/testing.md, tests/observe/lifecycle_test.go, tests/observe/stack.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/testing.md

### 2c7e38c9db62cf51b0a9fa7aa5e7871ddc771064

test: Classify missing Docker images explicitly

Problem:
The harness incorrectly treated docker's explicit no-such-image response as a missing local prerequisite while keeping DNS, daemon, and timeout errors fatal to teardown evidence.

Changes:
It updates tests/observe/lifecycle_test.go, tests/observe/stack.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/observe/lifecycle_test.go, tests/observe/stack.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 17a3896674743fecfbe05bbf4d1ea50e9836e48c

nacos: Clean up readiness canary after ambiguous SDK writes

Problem:
Readiness canary cleanup was installed after the first registration attempt, so a transport error could strand the canary.

Changes:
It updates pkg/nacos/client.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/client.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 2bab2563154b0b8cd161f6d338e22db77cf35797

nacos: Exclude foreign catalog hosts from reconcile reads

Problem:
The reconcile path did not filter nacos-authoritative GetAll results by the spotterOwner metadata before reconstructing domain instances.

Changes:
It updates pkg/nacos/nacos.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/nacos.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 8b5f4732426622567be06133b25686eedc39df61

test: Lock Nacos reconcile ownership boundary

Problem:
The implementation lacked regression coverage proving GetAll returns only spotter-owned catalog instances and never emits destructive deletes for foreign or legacy unowned hosts.

Changes:
It updates pkg/nacos/sink_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/sink_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 19ed899c6d66d4f72a92dbc2ca812a38c0780a5c

test: Preserve raw ownership metadata in Nacos fixtures

Problem:
The implementation lacked a test-only Nacos mock helper that preserves missing owner metadata, allowing reconcile tests to prove foreign and legacy unowned catalog instances are excluded without changing existing fixture defaults.

Changes:
It updates internal/testkit/nacosmock/server.go, pkg/nacos/sink_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is internal/testkit/nacosmock/server.go, pkg/nacos/sink_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 52535c054acdfc04dfe8daa156c5160ac9932909

fix: Preserve unfiltered discovery center GetAll results

Problem:
The harness incorrectly treated an empty provider argument as an intentional no-filter request in Client.GetAll instead of dropping every returned instance.

Changes:
It updates pkg/discoverycenter/client.go, pkg/discoverycenter/client_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/discoverycenter/client.go, pkg/discoverycenter/client_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 3a1ce465aabd4e0e5d5622a8d994fa0eae1c38e9

fix: Make cache deletion nil-safe and copy-on-return

Problem:
CacheBtree.Delete asserted on a missing identity instead of returning nil safely.

Changes:
It updates pkg/providers/cache.go, pkg/providers/cache_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/cache.go, pkg/providers/cache_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 7231049ef5d36d6f7621ea044f95226f6e73dff9

fix: Preserve unique legacy cache deletion semantics

Problem:
The compatibility layer did not mirror cacheBtree.Get's compatibility lookup in Delete for callers that still address instances by wire InstanceId.

Changes:
It updates pkg/providers/cache.go, pkg/providers/cache_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/cache.go, pkg/providers/cache_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 353801e1d2004e1b9add39f8e6812672a2b4ddb3

docs: Synchronize release status at current remediation head

Problem:
The implementation lacked authoritative current-status addenda to the audit, remediation plan, Nacos sink, Atlas wire, DDD, testing, and operations documents.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### cb16f73695fa75097c5c9b3de3db82f8481aeb97

refactor: Make Nacos constructors SDK-only by default

Problem:
The implementation did not close the exported constructor escape hatch that could silently allocate the raw HTTP compatibility client.

Changes:
It updates docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/client_whitebox_test.go, pkg/nacos/nacos.go, pkg/nacos/sink_test.go, tests/e2e/fanout_pipeline_test.go, tests/e2e/nacos_reconcile_e2e_test.go, tests/e2e/permanent_4xx_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/client_whitebox_test.go, pkg/nacos/nacos.go, pkg/nacos/sink_test.go, tests/e2e/fanout_pipeline_test.go, tests/e2e/nacos_reconcile_e2e_test.go, tests/e2e/permanent_4xx_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 0ef27b3dfe0804b6cff1f16974f72ba41973d836

docs: Refresh Nacos constructor baseline

Problem:
Release-status addenda still pointed at a pre-SDK constructor state and did not identify the explicit compatibility constructor boundary.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 1a82138a65c3ba8fd8f41c9847e08addd7865ee8

nacos: Make exported readiness SDK-default

Problem:
The implementation still retained the raw HTTP readiness escape hatch from the default CheckReadiness API.

Changes:
It updates pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/sink_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/client.go, pkg/nacos/client_test.go, pkg/nacos/sink_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 05e9d146831b55455f35f78e7eb5d5c733989a75

docs: Document SDK-default readiness APIs

Problem:
The documentation still pointed at an older or incomplete state: the authoritative remediation baseline to the strict readiness API stage.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### be321343afb40b4e0df46c85ae43fe0149625765

docs: Correct Nacos reconcile startup ordering

Problem:
The reconcile design lacked a current-status addendum and still presented readiness-before-sink ordering as current.

Changes:
It updates docs/dsca-3-reconcile.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-3-reconcile.md

### e8b978c72d31d7291d2943508e81eb354e36e21d

nacos: Inject an approved cluster admin facade

Problem:
A context-aware NacosClusterAdmin seam was absent for official or approved Admin/Maintainer implementations.

Changes:
It updates pkg/nacos/client.go, pkg/nacos/nacos.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/client.go, pkg/nacos/nacos.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 102e812d4fd118617659be034d0e6886f0d984b8

fix: Gate Nacos registration on cluster admin readiness

Problem:
The injected NacosClusterAdmin seam was incomplete for approved Admin/Maintainer implementations.

Changes:
It updates docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, pkg/nacos/client.go, pkg/nacos/nacos.go, pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, pkg/nacos/client.go, pkg/nacos/nacos.go, pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 4d0f9b83576fb044d5d87f820d5146949d41e7d8

docs: Record Nacos admin seam and registration gate

Problem:
Current-status addenda did not identify the injected NacosClusterAdmin implementation or its remaining capability boundary.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### e118882465aa0e1568fc9f747157b20c9f6da898

feat: Wire per-start Nacos admin factories

Problem:
The configuration path did not thread a per-lifecycle ClusterAdminFactory through composition runtime and server startup.

Changes:
It updates docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, internal/composition/root.go, internal/composition/root_test.go, internal/server.go, internal/server_test.go, pkg/nacos/client.go, pkg/nacos/nacos.go, pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, internal/composition/root.go, internal/composition/root_test.go, internal/server.go, internal/server_test.go, pkg/nacos/client.go, pkg/nacos/nacos.go, pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### a2b9b83e6c95b0f71e3a066c34ab4d79b53429db

docs: Refresh admin factory implementation baseline

Problem:
Current-status addenda did not identify the per-start Nacos admin factory implementation.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 9f616e3c08a3f8205ee1f4ad1ad84c2e5abf2d02

fix: Make legacy Prometheus shutdown bounded and idempotent

Problem:
Prometheus shutdown used a nil context and concurrent Stop calls were not serialized.

Changes:
It updates pkg/metrics/proserver.go, pkg/metrics/proserver_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/metrics/proserver.go, pkg/metrics/proserver_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### dc7e14c52ea66fccf81eabb6b52a5d2000c4a9d8

fix: Guard legacy metrics handler registration

Problem:
Process-wide /metrics handler registration was unguarded, so multiple legacy PrometheusService instances could panic on duplicate patterns.

Changes:
It updates pkg/metrics/proserver.go, pkg/metrics/proserver_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/metrics/proserver.go, pkg/metrics/proserver_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 5df45d5d090bb43de5abd9bd80d909aa33f0c910

fix: Make panic recovery observable and safe

Problem:
The default panic handler emitted no bounded diagnostic and could expose secret-like values without redaction.

Changes:
It updates tools/recover.go, tools/recover_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tools/recover.go, tools/recover_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 8e9ceabf70cb751a27d5a121882da1e45475cedb

docs: Reframe historical reliability audits at current head

Problem:
The reliability audit did not carry a current-status addendum for the 41 findings and DSCA scale, latency, and model-fidelity documents.

Changes:
It updates docs/dsca-1-scale.md, docs/dsca-2-latency.md, docs/dsca-5-model.md, docs/test-reliability-audit.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-1-scale.md, docs/dsca-2-latency.md, docs/dsca-5-model.md, docs/test-reliability-audit.md

### f016ae5fdb9ac4c1e2cd327f78fa88794c6b3424

docs: Normalize current audit provenance

Problem:
Current-status addenda still pointed before the panic-recovery baseline 5df45d5 and did not identify related Nacos admin/factory and metrics commits.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/dsca-3-reconcile.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/test-reliability-audit.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/dsca-3-reconcile.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/test-reliability-audit.md, docs/testing.md

### afb19ad0e341b364f1eea2d2e7e04762f749fbfb

observe: Pin ARM64 Nacos scratch image

Problem:
The scratch harness did not pin the Nacos 2.1 slim ARM64 image by digest or pass linux/arm64 to docker run.

Changes:
It updates tests/observe/stack.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/observe/stack.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 0f2b22f973ef8ef006b8bad4b34c29d69f133e66

observe: Run Nacos by verified ARM64 digest

Problem:
The ARM64 image digest passed to docker run was not validated against Docker RepoDigests, leaving an inspect-to-run mismatch window.

Changes:
It updates tests/observe/stack.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/observe/stack.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 356aa75b87211ac4a9d0db8743875c0d806eb9d5

observe: Map Nacos HTTP and gRPC scratch ports

Problem:
The scratch harness did not expose separate Nacos HTTP and gRPC ports needed to exercise the ARM64 image.

Changes:
It updates tests/observe/lifecycle_test.go, tests/observe/stack.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/observe/lifecycle_test.go, tests/observe/stack.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 35fa110d1f02feed65472fbbc68c0be90ef50510

test: Retain evidence of successful Nacos canary cleanup

Problem:
Nacos real gates mixed pending registration with cleanup-attempt and cleanup-status evidence, obscuring whether a canary might remain.

Changes:
It updates tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 896ce4d9cf358fb0b394e3d5b308125fc04f52c4

docs: Record ARM64 Nacos scratch lifecycle evidence

Problem:
The implementation lacked a formal evidence artifact for the bounded ARM64 Nacos 2.1 slim scratch run.

Changes:
It updates docs/README.md, docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/README.md, docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 00f5f104c0e7ab54545ba0dab107073b24e87f00

docs: Record authenticated Nacos scratch evidence

Problem:
The existing tests and fixtures lacked the ARM64 scratch evidence artifact with the authenticated Nacos 2.1 slim runs on the mapped HTTP/gRPC/control ports and the non-public tenant-a/blue namespace-group replay.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/dsca-1-scale.md, docs/dsca-2-latency.md, docs/dsca-3-reconcile.md, docs/dsca-5-model.md, docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/test-reliability-audit.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/dsca-1-scale.md, docs/dsca-2-latency.md, docs/dsca-3-reconcile.md, docs/dsca-5-model.md, docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/test-reliability-audit.md, docs/testing.md

### 08f07fdbbb1ed5d3201079e668eaee401e964dd5

test: Add guarded Nacos restart persistence gate

Problem:
No guarded nacos_restart gate proved persistent canary survival across a Nacos restart and SDK reconnect.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, tests/e2e/nacos_real_config.go, tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, tests/e2e/nacos_real_config.go, tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 6e8f36cff3748dfe9a527fcffc4f441591c8e061

test: Isolate vendor reconnect race in Nacos restart gate

Problem:
The restart persistence gate ran against a known SDK reconnect data race, so failures could not be attributed safely.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md, tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### aa5951c0d2f4ca3f3b4072aca32eea9d7f00451a

test: Warm Nacos SDK before guarded restart

Problem:
The implementation lacked a bounded service-list warmup before closing the pre-restart SDK client so a startup-session Close race is not confused with the known nacos-sdk-go reconnect race.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md, tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md, tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md

### 86f86e376bde1b3225975064faee1b041b6bdff9

docs: Record Nacos restart vendor race evidence

Problem:
The documentation omitted the ARM64 scratch restart persistence results separately from automatic reconnect evidence.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/nacos-sink-plan.md, docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 621a81811c765dd4a92109a2ce73a3d0eb4dae59

docs: Distinguish Observe implementation from unverified scale evidence

Problem:
The audit did not state the current live-source snapshot, divergence-age, per-tick verdict, command-bound, and kwok vehicle status.

Changes:
It updates docs/dsca-4-observation.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-4-observation.md

### b0b8589acecb28f8cb882c8c2e54de9e9beddff9

fix(nacos): Pin upstream reconnect race fix

Problem:
The dependency or image was not pinned: nacos-sdk-go to upstream commit 0024865, which atomically publishes the current RPC connection during reconnect.

Changes:
It updates docs/README.md, docs/nacos-sdk-provenance.md, go.mod, go.sum to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/README.md, docs/nacos-sdk-provenance.md, go.mod, go.sum; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/README.md, docs/nacos-sdk-provenance.md

### c0fe1746d30be2ef227bbad0419f5569606d853e

docs: Align Nacos evidence with pinned SDK revision

Problem:
The repository did not pin the reconnect race fix or distinguish new-client persistence evidence from unverified automatic reconnect behavior.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md

### 2caad47ee493408cc10f8b12135c0bc7566ecc05

test: Add guarded single-client Nacos reconnect gate

Problem:
The implementation lacked an opt-in restart gate that keeps one SDK client alive across a strictly guarded scratch container restart and verifies service visibility afterward.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### eb0c6129e573cfb9e5d2f74c32136b661d028abf

docs: Record guarded automatic reconnect gate

Problem:
The documentation omitted the new single-client Nacos restart test as an opt-in verification path while retaining NOT VERIFIED status until it runs against an approved scratch target.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md

### 32ee691168aa8ae1da6615c8af4e28dc6aec31c2

test: Harden single-client Nacos reconnect cleanup

Problem:
The reconnect gate did not retain cleanup attempts/errors or issue a post-restart SDK write before fresh visibility checks.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 346a8a5f0afdb8593c896821335357cd247e427e

test: Prove Nacos restart outage before reconnect

Problem:
The cleanup path did not stop the guarded scratch container, wait for its mapped endpoint to go down, and start it again before exercising the same SDK client's reconnect and post-restart write path.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### b0c19a7e74474bfbbf154b0420f8a30ac8726e90

test: Require observable Nacos restart outage

Problem:
The reconnect gate could pass without observing the guarded endpoint go down after stop and return after start.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 03ff24a136e8bd3b4f0574314c9fbdf2d63ab743

test: Bound Nacos restart outage and residual cleanup

Problem:
The reconnect gate did not bind endpoint transitions and cleanup absence checks to one parsed endpoint and the caller deadline.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### bbe5cca0def805728200d5df2a346bbbe9069abf

test: Fail closed when Nacos restart does not recover

Problem:
The gate did not require an observed endpoint-up transition after the guarded scratch restart and make outage polling responsive to context cancellation.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### d936fadbd92f5509a259f1cc600a684a1b4740cb

test: Verify reconnect writes with fresh Nacos client

Problem:
The implementation did not use an independent SDK client for post-restart visibility checks so local subscription or redo caches cannot produce a false positive.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### fced8244ed4abd9395cf93590413f847b96e0492

test: Verify Nacos cleanup with fresh client

Problem:
After canary deregistration, cleanup did not query a fresh verifier until the deadline or surface verifier close and query failures as cleanup errors.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 8c89fee6a272a7d09f76f9cec417bad1bf0eb404

test: Bound reconnect cleanup verification

Problem:
The implementation did not use a deadline-bound cleanup context and cancellation-aware polling for fresh-client residual checks.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### bed136599aa23599cac3240d86e0256bd5e0d490

test: Stop reconnect cleanup polling on cancellation

Problem:
The verifier did not break residual verification immediately when its cleanup context is canceled, preventing additional SDK queries after the deadline while preserving explicit failure and residual evidence.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 9fcf6227312bb34d9725c84977c03c173d012ed8

test: Verify exact disabled state after reconnect write

Problem:
The reconnect gate did not recreate the canary with unique metadata and verify the exact disabled state through a fresh SDK client.

Changes:
It updates tests/e2e/nacos_restart_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_restart_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 2c334fe47a0f49a13487643774eb9064ced3820e

nacos: Disable SDK snapshot fallback for authoritative reads

Problem:
The SDK could load startup snapshots and fall back to stale data instead of requiring live authoritative reads.

Changes:
It updates pkg/nacos/sdk.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/sdk.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### a64f543ef73bc0b2f466c68073071c1d4605ddd2

test(nacos): Reject stale SDK read fallback

Problem:
The reconcile read path could accept a stale SDK snapshot instead of proving the live Nacos response.

Changes:
It updates pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### ce64f0ad195671eb139583deea3365a48bc6fdcb

fix(nacos): Isolate authoritative SDK reads

Problem:
Authoritative SDK reads reused client state, so a cached snapshot could mask a changed Nacos catalog.

Changes:
It updates pkg/nacos/client.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/client.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### aee60a770d525292b0f5484ed5c23caea46512f3

fix(nacos): Isolate SDK cache ownership

Problem:
Independent SDK reads shared cache directories, allowing one read session to contaminate another session.

Changes:
It updates pkg/nacos/client.go, pkg/nacos/sdk.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/client.go, pkg/nacos/sdk.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### f594be10ec46c874a0716a804f9c7b478cb652ed

fix(nacos): Reuse authoritative read session

Problem:
A single authoritative snapshot opened multiple SDK sessions, multiplying cache state and resource cleanup.

Changes:
It updates pkg/nacos/client.go, pkg/nacos/nacos.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/client.go, pkg/nacos/nacos.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### c986632aebce2a950b0a47e52ff22c15011c7eb2

fix(nacos): Reuse read session during prune

Problem:
Nacos prune did not reuse one authoritative read session for all queries in a sweep.

Changes:
It updates pkg/nacos/nacos.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/nacos.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### ca2aac3c717e61ff9fb394e0f7bd8d31fbe51bf7

docs: Record ARM scratch auto reconnect evidence

Problem:
The documentation omitted the guarded ARM64 pseudo-pin 0024865 single-client restart run on the 60848/61848/61849 port tuple, including race execution, post-restart disabled-state verification, and cleanup evidence.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md

### 04f278bac8595928978d8e9ddd455cc65f751985

docs: Separate historical and current Nacos reconnect evidence

Problem:
The evidence did not mark the earlier v2.3.5 reconnect race as historical provenance and attribute the current ARM64 -race PASS to pseudo-pin 0024865.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md

### 02c0fa99791b8f0ded6f8e0ea4171b912fd43dff

docs: Label historical Nacos reconnect failure command

Problem:
The evidence did not mark the legacy restart race command as a v2.3.5 pre-pseudo-pin result so current 0024865 evidence cannot be misread as failing.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md

### b54b2b69a0268afefdfe47d24604c86392dbe297

docs: Pin Nacos capability statements to current SDK

Problem:
Nacos cluster-admin and static-token capability claims were not tied to the tested SDK pseudo-pin, allowing historical v2.3.5 evidence to be confused with current behavior.

Changes:
It updates docs/nacos-sink-plan.md, docs/operations.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/nacos-sink-plan.md, docs/operations.md

### 15ef548c9b30afe20bdd04a70343deab80732875

docs: Clarify current Nacos SDK pseudo-pin provenance

Problem:
The current documentation lacked the audit baseline and add an explicit clarification that current implementation and ARM reconnect evidence use pseudo-pin 0024865, while unqualified v2.3.5 references are historical.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md

### 107dcbe144d17a26a8320424ca1b16fa1ea60268

docs: Refresh reconcile audit provenance

Problem:
The documentation still pointed at an older or incomplete state: the dsca-3 document's current implementation HEAD while preserving historical audit evidence and explicit production verification boundaries.

Changes:
It updates docs/dsca-3-reconcile.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-3-reconcile.md

### e854752be5a019dc5f172743e52f79748da7eea1

docs: Synchronize current Nacos SDK evidence status

Problem:
The dependency or image was not pinned: current implementation and ARM64 reconnect evidence to pseudo-version 0024865, label v2.3.5 race output as historical, and preserve explicit untagged, HA, TLS, Admin, and production NOT VERIFIED boundaries across remediation and audit documents.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### f9d99ab4a280cc14bfd6be9c1df8f323b853cde5

docs: Align Nacos SDK provenance across readiness plans

Problem:
The current documentation lacked every current-status section to the pinned upstream SDK pseudo-version 0024865 and the ARM64 single-client race evidence.

Changes:
It updates docs/dsca-3-reconcile.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-3-reconcile.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 5bef4f18339a89db93abda43eda129b3e33fbe73

docs: Repair Nacos status table rendering

Problem:
The readiness matrix merged SDK status rows, producing unstable columns and ambiguous capability evidence.

Changes:
It updates docs/system-readiness-consistency-audit-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-audit-2026-09-12.md

### 89c71ab95e0828c4536990a4c70ac75f552b2be5

observe: Add owned kwok stack lifecycle scripts

Problem:
Observe had no self-contained up/down orchestration for an owned kwok cluster with dedicated API and etcd ports.

Changes:
It updates Makefile, scripts/observe-down.sh, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is Makefile, scripts/observe-down.sh, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### d110e39c259a6ae1f97a32ca25545c09c2b8132a

observe: Bind kwok lifecycle to verified owned state

Problem:
Observe cleanup did not constrain ownership to an absolute build directory and validated dsca-observe cluster names.

Changes:
It updates scripts/observe-down.sh, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-down.sh, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### b9bbb1fff4f94e1d45d84b8dabd7b2c250ee36da

observe: Preflight scratch ports and guarantee teardown

Problem:
Observe startup lacked required docker/kwokctl/kubectl preflight, scratch-port conflict checks, and an EXIT-trapped teardown path.

Changes:
It updates Makefile, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is Makefile, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### b074d77f3506c5be2037a851cdeb41a955ba08cd

observe: Fail closed on daemon ports and API readiness

Problem:
Observe startup could proceed without nc, a healthy Docker daemon, complete scratch-port checks, or a ready kwok kubeconfig and apiserver.

Changes:
It updates scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### e917c2012fe2f7bc622cb721c280215292487466

fix(observe): Provision kwok node capacity

Problem:
The Observe harness started kwok without provisioning the node pod capacity required by OBS_SCALE.

Changes:
It updates scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### a0e5e7508298ffff20db54b1c63bb1f7572e7325

observe: Enforce owned node capacity and state integrity

Problem:
The harness did not resolve the dedicated kwok node dynamically, make pod capacity patch failures fatal, verify allocatable pods meet OBS_SCALE, and validate the owned state manifest checksum before teardown.

Changes:
It updates scripts/observe-down.sh, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-down.sh, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### cfea36f4efd41c9d3d14eb1d5f75d4c2b7ecf414

observe: Harden node readiness and teardown parsing

Problem:
Observe startup did not wait for a Ready kwok node, use status-aware POSIX traps, or parse state as inert data.

Changes:
It updates Makefile, scripts/observe-down.sh, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is Makefile, scripts/observe-down.sh, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### f758ea1108d2b6677038186005d73be8d21e4f15

observe: Require CPU and memory capacity evidence

Problem:
Observe startup did not require allocatable CPU and memory in addition to the numeric pod-capacity threshold.

Changes:
It updates scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### f10b2b33db01f7af548f95f6e582d19c38cccd6b

test(observe): Cover isolated lifecycle failure paths

Problem:
Observe lifecycle failure paths had no isolated contract tests for cleanup and state retention.

Changes:
It updates scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 6b707fc8e96c886f23e2dfe1bb7b6b13253bdea0

docs: Document OBS-full and OBS-mini lifecycle gates

Problem:
The documentation lacked the owned Observe lifecycle scripts, prerequisite and port guards, node capacity checks, state-hash checks, external-kubeconfig safety, and fake-PATH contract.

Changes:
It updates docs/dsca-4-observation.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-4-observation.md, docs/testing.md

### 65f0d1160a0ef05c879f8d8c2f67e140ada7df65

observe: Guard external kubeconfig from mutations

Problem:
External OBS_KUBECONFIG mode could invoke the mutating consistency driver instead of being restricted to read-only Observe unit gates.

Changes:
It updates Makefile, docs/testing.md, scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is Makefile, docs/testing.md, scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/testing.md

### 9558d52cb746151e52a53130c9b763149b5273ff

test: Restore complete Observe lifecycle contract

Problem:
The lifecycle contract lacked checks for external-kubeconfig read-only mode and non-destructive teardown.

Changes:
It updates scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 44020083170741630994429f94db1aeee2069d7c

test(observe): Protect external kubeconfig and recipe cleanup

Problem:
The lifecycle contract did not verify external-kubeconfig read-only mode and non-destructive teardown.

Changes:
It updates Makefile, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is Makefile, docs/testing.md; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/testing.md

### 6c312906b351cfa4ae1e58a0c5232e8fea5b60ee

observe: Persist startup state before kwok mutations

Problem:
Observe startup wrote state after cluster creation, so partial failures could lack cleanup metadata and retained retry state.

Changes:
It updates scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### b7b694a9ce2df532e81ca2f8e69d524df674d94e

observe: Validate resource quantities and document owned cleanup

Problem:
Observe capacity checks lacked numeric CPU and memory quantities alongside OBS_SCALE and documented state-hash cleanup semantics.

Changes:
It updates docs/dsca-4-observation.md, docs/testing.md, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/dsca-4-observation.md, docs/testing.md, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/dsca-4-observation.md, docs/testing.md

### 8fc72614923ccdf43aa4c32b4f3ac9bd07fc96bf

observe: Fail closed on stale owned state

Problem:
Observe teardown could overwrite state or delete without a valid retained manifest after a failed kwok deletion, preventing safe retry.

Changes:
It updates scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 3caa6ba2042b582d32cf7a00a61cd160f9e897ec

test(observe): Verify state integrity cleanup gates

Problem:
Observe cleanup tests did not verify that tampered state or checksum failures block deletion.

Changes:
It updates scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 323b387cd842335b1fa69371722d22fed7d5b3dc

observe: Trap startup failures before state creation

Problem:
Observe startup installed cleanup after writing state and did not require sha256sum before up/down operations.

Changes:
It updates scripts/observe-down.sh, scripts/observe-up.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe-down.sh, scripts/observe-up.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### fb9f8bf11b84bafe69ad5911edb9cd979c2f4493

test(observe): Mock checksum lifecycle gates

Problem:
The lifecycle test harness lacked deterministic checksum mocks for state-integrity gates.

Changes:
It updates scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### c9c0537fb4351dd89154bcc5c567d63fe4c5b067

nacos: Expose explicit ephemeral batch SDK contract

Problem:
The client did not expose a clearly named official SDK batch-registration method for the real-gate evaluation.

Changes:
It updates pkg/nacos/sdk.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/sdk.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 5ff3208a877d2a4005ba34357e12ccd56b1ba4bd

test: Exercise ephemeral Nacos batch contract

Problem:
The SDK evaluation lacked an ephemeral BatchRegisterInstance exercise and explicit ephemeral cleanup evidence.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 73b5ddd7d3fcbc9ac22ad8b37f50e497128c1e6e

test(nacos): Harden SDK lifecycle matrix

Problem:
The Nacos SDK lifecycle matrix lacked coverage for persistent and ephemeral operation outcomes.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 5e31234e7e8c7331bc1fc6b54cacc5ae79242f1b

test(nacos): Audit write attempts and cleanup

Problem:
Real Nacos tests did not audit every write attempt and cleanup outcome.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 5dffdb8ddd61961f4aa9a31f5a085d6955e680ae

test(nacos): Premark real write cleanup

Problem:
Real Nacos cleanup state was recorded only after writes, so a pre-write failure could strand an unknown canary.

Changes:
It updates tests/e2e/nacos_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### e4b7d541abb401b48d428eadd49867ecfa4dfc13

test(nacos): Add bounded fresh residual verifier

Problem:
Residual verification had no bounded fresh-client verifier for proving cleanup completion.

Changes:
It updates tests/e2e/nacos_cleanup.go, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_cleanup.go, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 15beff2d03d1eb687fee6fbdc0c7a3610b5d842b

test(nacos): Verify residuals with fresh sessions

Problem:
Residual checks reused SDK sessions and could report stale visibility after cleanup.

Changes:
It updates tests/e2e/nacos_cleanup.go, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_cleanup.go, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### a93197f37cc3e4ec94c24caf6ec649f1e9dd97a8

test(nacos): Enforce ephemeral batch and residual gates

Problem:
The batch gate did not require ephemeral capability evidence and residual cleanup before passing.

Changes:
It updates pkg/nacos/sdk.go, pkg/nacos/sdk_test.go, tests/e2e/nacos_real_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/sdk.go, pkg/nacos/sdk_test.go, tests/e2e/nacos_real_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 2a5b2a0e589e8656464b7178c9b50b6f731558bc

test(nacos): Cover batch error propagation

Problem:
Batch tests did not prove that SDK batch errors propagate to the caller.

Changes:
It updates pkg/nacos/sdk_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/nacos/sdk_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 496789429ee07023487aa814928ace0bcf9dd7e2

test(nacos): Verify batch residual and cleanup status

Problem:
Batch tests did not record residual state and cleanup status separately.

Changes:
It updates tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_real_test.go, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 81c73f7fbc411e2913d77bd71610af54b66f8813

test(nacos): Premark batch cleanup before write

Problem:
Batch cleanup evidence was not pre-marked before the first write attempt.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 09979b1f1c2fe423032cc947e9ee4f84759ffe05

test(nacos): Structure batch cleanup outcomes

Problem:
Batch cleanup outcomes had no structured distinction between attempted, passed, failed, and unknown.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 7733115b249f18a89985b5c6c7dac860ada62055

test(nacos): Fail closed on batch cleanup errors

Problem:
Batch tests could pass while cleanup errors were only logged instead of failing closed.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 4c379e36078709a1704dbe70529b8135d736c292

test(nacos): Classify unsupported batch capability

Problem:
The batch gate did not classify an unsupported server capability separately from a test failure.

Changes:
It updates docs/testing.md, tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is docs/testing.md, tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
docs/testing.md

### 46b82392735bc221d011dbe30b683d1ff43fb4f8

test(nacos): Record unsupported batch as not verified

Problem:
The batch evidence did not record an unsupported target as NOT VERIFIED.

Changes:
It updates tests/e2e/nacos_sdk_eval_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is tests/e2e/nacos_sdk_eval_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 1dd14c8995a60758d26344da8ebc8284b741d3f3

docs: Qualify Nacos batch capability by target version

Problem:
The batch capability document broadly claimed 2.1.1+ support without recording the observed 2.1.0 RequestHandler Not Found result and cleanup evidence.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md

### a13ed46f3f489cd8bdca88026f7b1445ae33109b

test(observe): Verify successful cleanup cardinality

Problem:
Observe cleanup tests did not verify that successful teardown removes exactly one owned stack.

Changes:
It updates scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 47a5e025659101d094710c09301245b8816b0e67

docs: Record latest ARM64 Nacos lifecycle evidence

Problem:
The ARM64 lifecycle evidence did not record persistent PASS, batch RequestHandler Not Found, cleanup statuses, and container removal together.

Changes:
It updates docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/evidence/nacos-arm64-scratch-2026-09-13.md, docs/testing.md

### 1d759b8909b15a886b8612a0136d20f2103b6178

docs: Publish remediation execution ledger

Problem:
The implementation lacked the authoritative execution status for the serialized remediation work.

Changes:
It updates docs/README.md, docs/remediation-execution-status-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/README.md, docs/remediation-execution-status-2026-09-13.md

### 10d6334843e397f4e9f3f8a78c7570b0afaa319b

fix(consul): Add payload-free instance change handler

Problem:
Consul monitor callbacks could only receive a fabricated empty CatalogService payload.

Changes:
It updates pkg/providers/consul/consul.go, pkg/providers/consul/monitor.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/consul/consul.go, pkg/providers/consul/monitor.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 436b05f185147cb106bb5d3c9fb72cfc2d8a01de

fix(consul): Dispatch all instance handler contracts

Problem:
Consul dispatch handled only one monitor callback contract, dropping the other handler form.

Changes:
It updates pkg/providers/consul/monitor.go, pkg/providers/consul/monitor_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/consul/monitor.go, pkg/providers/consul/monitor_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 33606fa71a7fb3a12e22649ad0f16b893c4349c8

fix(consul): Notify payload-free handler failures

Problem:
Consul monitor callback failures were not sent to the notifier when no payload existed.

Changes:
It updates pkg/providers/consul/monitor.go, pkg/providers/consul/monitor_test.go to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is pkg/providers/consul/monitor.go, pkg/providers/consul/monitor_test.go; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### 67d106b6580a8e1c8f7eed2facf9c7207c936e61

docs(consul): Close fabricated monitor payload finding

Problem:
The Consul status record still lacked the closure evidence for the fabricated monitor payload finding.

Changes:
It updates docs/ddd-architecture.md, docs/remediation-execution-status-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/ddd-architecture.md, docs/remediation-execution-status-2026-09-13.md

### ece88987a0950789072bb049f386625a8db8a5c3

docs: Synchronize current status provenance

Problem:
Four DSCA documents still named an older implementation HEAD, so their current-status provenance disagreed with the Consul monitor fix.

Changes:
It updates docs/dsca-1-scale.md, docs/dsca-5-model.md, docs/operations.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-1-scale.md, docs/dsca-5-model.md, docs/operations.md, docs/testing.md

### 3f4c4193bea4b530c860fe22c865751d5d4f3037

docs: Update remediation baseline provenance

Problem:
The execution ledger still called ece8898 the latest pushed evidence instead of recording it as the latest documentation baseline.

Changes:
It updates docs/remediation-execution-status-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/remediation-execution-status-2026-09-13.md

### 03342378ba63b60f2679b5b87f3c423a2afb7ba9

test(observe): Harden node capacity failure gates

Problem:
Observe node-capacity tests lacked failure gates for missing or insufficient allocatable resources.

Changes:
It updates scripts/observe_lifecycle_test.sh to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
The affected boundary is scripts/observe_lifecycle_test.sh; reverting this commit restores the prior package/API behavior while leaving unrelated commits intact.

Documentation:
None

### e62e1cd0fe0714db3f417698be27981218071ed3

docs(observe): Update current lifecycle baseline

Problem:
The observation audit stopped at the earlier implementation HEAD and omitted the newly implemented lifecycle and capacity gates.

Changes:
It updates docs/dsca-4-observation.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-4-observation.md

### 3e9a81cf68743cbbfb84846e70a2de218e0ec5cf

docs: Advance remediation evidence baseline

Problem:
The execution ledger still pointed at e62e1cd as its latest documentation baseline after the Observe lifecycle update.

Changes:
It updates docs/remediation-execution-status-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/remediation-execution-status-2026-09-13.md

### 01cce6f6ade1c488c011b9f84be377e5a1962880

docs: Advance remediation ledger baseline

Problem:
The execution ledger still pointed at the prior documentation baseline and did not record that the remediation matrix was unchanged.

Changes:
It updates docs/remediation-execution-status-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/remediation-execution-status-2026-09-13.md

### ddbeedd26aa13c60245ae5f525806358f48982e7

docs: Clarify deferred integrations and release scope

Problem:
Release notes still treated deployment HA/TLS/auth checks and Atlas real compatibility as current evidence gates, and terminology for the optional AppCenter notifier was undocumented.

Changes:
It updates docs/operations.md, docs/remediation-execution-status-2026-09-13.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/operations.md, docs/remediation-execution-status-2026-09-13.md, docs/testing.md

### 77198645a48b9c70ea44dbe2af047dbc3d594748

docs: Defer Atlas gate and clarify operational scope

Problem:
Atlas was still labeled NOT VERIFIED/P1 and the Nacos plan still presented it as a current peer instead of a deferred optional integration.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md

### 6a2c219d67b407592e9025cea504ac7fbdbe177f

docs: Synchronize Atlas deferred scope

Problem:
The Atlas status lacked the explicit non-blocking qualifier, and reconcile/remediation documents did not carry the deferred single-Sink scope.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/dsca-3-reconcile.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/dsca-3-reconcile.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 269f42cf58fdc78c2bc4727930f6aacee3ec89b3

docs: Defer Atlas protocol validation follow-up

Problem:
The remediation plan still classified Atlas protobuf and codec validation as a current P1 section rather than a future entry gate.

Changes:
It updates docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 4ff33ccb6ba65395278ca7c67a7e459ce966ce60

docs: Align Atlas deferral across release guidance

Problem:
Atlas deferral was not reflected consistently in the wire-compatibility title, reconcile addendum, and remediation plan heading.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/dsca-3-reconcile.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/dsca-3-reconcile.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 278470d4f151ae3e0a294493318042435098b2f9

docs: Separate current blockers from deferred scope

Problem:
The release ledger mixed current Spotter blockers with deferred Atlas and deployment-level Nacos evidence, making the release boundary ambiguous.

Changes:
It updates docs/dsca-3-reconcile.md, docs/remediation-execution-status-2026-09-13.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-3-reconcile.md, docs/remediation-execution-status-2026-09-13.md

### e3776eb9c6b37b6883ee592e4c870c7c4e8002e5

docs: Clarify deferred operational gates

Problem:
Atlas documentation implied that deferral meant readiness, and DDD operations tied AppCenter contract evidence to unrelated legacy-shim retirement.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/operations.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/ddd-architecture.md, docs/operations.md

### abe4a893e1e6a37b6c5d6d26586ce7a88112ec79

docs: Clarify Atlas default runtime dependency

Problem:
The Atlas documents did not warn that the server still constructs Atlas by default, so deferring its wire gate did not make an Atlas-less deployment ready.

Changes:
It updates docs/atlas-wire-compatibility.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/atlas-wire-compatibility.md, docs/nacos-sink-plan.md, docs/system-readiness-consistency-audit-2026-09-12.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md

### 33fe2a5c8b6783839ae8e517ba8881569a31f183

docs: Label historical reconcile findings and scope

Problem:
The reconcile audit presented DS-3-1 through DS-3-7 as current gates even though they describe the historical Atlas mode.

Changes:
It updates docs/dsca-3-reconcile.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/dsca-3-reconcile.md, docs/system-readiness-consistency-remediation-plan-2026-09-12.md, docs/testing.md

### 6b1295f26d4f7f787a0de9234b295d6c8ce18dd4

docs: Clarify audit release boundaries

Problem:
Operations and the consistency audit still treated deployment HA/TLS/auth evidence and historical Atlas findings as current release blockers.

Changes:
It updates docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/operations.md, docs/system-readiness-consistency-audit-2026-09-12.md

### e9ea7d28aabec14dd6222c51fa311e68b0496e3f

docs: Publish batch and global dependency refactor plan

Problem:
The repository lacked a bounded persistent Nacos full-sync batch design, a testable package-global removal path, and a controlled detailed-message rewrite plan.

Changes:
It updates docs/superpowers/plans/2026-09-13-spotter-batch-and-global-refactor.md, docs/superpowers/specs/2026-09-13-batch-and-global-refactor-design.md to implement or document the behavior described by the commit subject.

Verification:
Not recorded at commit time; covered by the branch verification matrix.

Compatibility / Rollback:
Documentation-only wording can be reverted without changing runtime behavior.

Documentation:
docs/superpowers/plans/2026-09-13-spotter-batch-and-global-refactor.md, docs/superpowers/specs/2026-09-13-batch-and-global-refactor-design.md

## Rewrite execution result

The message-only rewrite was executed against the confirmed remediation range.

- BASE: `838ad198fd30c37f48df17a13c4e5cbfcfac22fa`
- Frozen OLD_HEAD: `e9ea7d28aabec14dd6222c51fa311e68b0496e3f`
- Rewritten range head: `2f4a1b9c907893da46154639c720ed2a96d8d429`
- Old current tip: `a768d6c46cd4e7f2507a779f803c41e3cedef3d3`
- Final `refactor/all` tip: `fefeef26d5bfd96798fce0bb5bd165a9b29127b4`

All 175 tree, parent, author/timestamp metadata, and structured-message checks
passed. The three post-range commits were replayed with
`git rebase --rebase-merges`; their patch set matched by `git range-diff`.
The remote replacement completed with the exact
`--force-with-lease` guard, and both pre-rewrite backup tags were pushed.
