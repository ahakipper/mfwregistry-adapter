# SDD ledger — plan: docs/superpowers/plans/2026-09-14-nacos3-kwok-global-cleanup.md

## Plan pre-flight

| Tasks | Shared files/interfaces | Finding | Ruling |
|---|---|---|---|
| 0.1 → 1.1 | `docs/evidence/nacos3-kwok-target-2026-09-14.md`, Nacos target wording | Stage 0 establishes the Nacos 3 target and Stage 1 consumes it. | Stage 0 documentation is authoritative; Stage 1 must not reintroduce Nacos 2 as a release target. |
| 1.1 → 1.2 | `go.mod`, `pkg/nacos/sdk.go`, vendor facade | RED tests must compile against the selected official SDK before implementation. | Pin the exact official v3 release or observed `v3.x-dev` pseudo-version before implementation; record checksum and routing evidence. |
| 1.2 → 2.1/2.2 | `pkg/nacos/client.go`, `pkg/nacos/nacos.go` | Stage 1 may retain the existing cluster-admin precondition while Stage 2 removes it. | Stage 2 owns the policy change; Stage 1 must keep the change isolated and provide a clean seam for removal. |
| 1.2 → 3.1/3.2 | SDK naming boundary and persistent operation signatures | Batch tests depend on the final persistent gRPC operation shape. | Stage 1 defines the vendor-neutral interface; Stage 3 may adapt batching but may not bypass it. |
| 3.2 → 4.1/4.2 | `pkg/nacos` readiness and `tests/observe` stack | Observe must use the exact SDK path delivered by Stage 1/3. | Stage 4 cannot use historical v2 or raw v1 data operations; all Nacos assertions use the Stage 1 facade. |
| 4.2 → 5.1/5.2 | E2E/Observe tests and legacy packages | Existing tests import globals even though production composition no longer does. | Migrate tests first; only delete packages after `go list -deps ./cmd` and repository import scans are clean. |
| 2.2 → 5.2 | optional Admin/Maintainer interface | Removing legacy packages must not reintroduce a hidden admin dependency. | The health policy remains deployment-owned; any optional preflight is injected through explicit ports only. |
| 5.2 → 6.1/6.2 | all status/evidence docs | Earlier documents contain contradictory Nacos 2 and pending/full-run claims. | Stage 6 reconciles wording only against fresh command/evidence output; historical records remain labeled historical. |

| Task | Self-consistency check | Ruling |
|---|---|---|
| 0.1 | Documentation-only target freeze has explicit files, commands, and review. | Proceed. |
| 1.1 | RED routing tests and module pin are prerequisites for implementation. | Proceed; do not call a v2 test a Nacos 3 result. |
| 1.2 | Vendor-neutral interface and gRPC construction are tested before integration. | Proceed; no raw HTTP fallback. |
| 2.1/2.2 | Tests distinguish runtime naming from optional management preflight. | Proceed; nil Admin must not block naming operations. |
| 3.1/3.2 | 201 items expose replacement-vs-additive protocol mistakes. | Proceed; logical batches remain capped at 100. |
| 4.1/4.2 | Harness lifecycle, scale, and evidence are separate but ordered. | Proceed; short smoke cannot close the two-hour gate. |
| 5.1/5.2 | Caller migration precedes deletion and preserves deployment assets. | Proceed; `config/` directory itself is not deleted. |
| 6.1/6.2 | Final docs depend on fresh verification. | Proceed only after all preceding stage reviews are clean. |

Ruling: The Nacos 3 target is a semantic change, not a documentation-only tag
swap. The current SDK v2 high-level route sends persistent instances through
legacy HTTP, so Stage 1 must establish an official SDK gRPC route before the
kwok gate is allowed to run. Cost if wrong: a false Nacos 3 PASS and lost
instances in production.

Ruling: `healthChecker=NONE` is a service/cluster management setting, not a
runtime naming prerequisite. Stage 2 will remove the hidden startup gate and
record deployment-owned preconfiguration as the explicit contract. Cost if
wrong: either Spotter remains unnecessarily blocked or Nacos overwrites the
source health state.

Ruling: The user used “kwork”; this plan interprets it as `kwokctl`, the
repository's existing real Kubernetes scale vehicle. If the user meant a
different tool, the plan would need a new scope decision; no such alternative
is assumed.

## Stage 0 review findings and rulings

- P1: Stage 0 added the new plan/spec but did not yet update the existing
  operator-facing documents that still describe Nacos 2.1 and the old
  cluster-admin startup block. Ruling: update those documents in a Stage 0
  correction commit before accepting Stage 0; historical evidence stays
  unchanged and is labeled historical.
- P1: SDK provenance and the Nacos 3 evidence artifact were not present in the
  Stage 0 commit. Ruling: add the exact current official module pseudo-version
  and checksum, plus an explicit target/evidence record, before Stage 1's
  implementation claim.
- P1: Kwok acceptance was qualitative. Ruling: pin numeric observation bounds,
  tick fields, divergence-age formula, ghost definition, and the required
  source/cache/Nacos hash comparison in the Stage 0 correction.
- P2: The spec used “applications” while the plan used “services”. Ruling:
  use “applications/services” consistently and state that `OBS_SERVICES` is
  the application count and each application maps to one Nacos service scope.
- P2: The legacy static gate and http-compat cleanup scope were ambiguous.
  Ruling: name exact import/symbol/assignment checks and explicitly state
  whether the compatibility transport is retained only for tests or removed
  with the legacy package; production remains SDK-only.
- P2: Health policy lacked a concrete deployment artifact. Ruling: add a
  deployment-preflight evidence schema and require the observer to record
  policy source/verification status without inventing an Admin API.

## Task 1.1 review findings and rulings

- P1: The first review found that the seam tests do not decode actual vendor
  proto requests. Ruling: this is a valid requirement, but it belongs to Task
  1.2 where the concrete official gRPC proxy exists; the plan now explicitly
  requires `RegisterInstanceRequest`/`DeregisterInstanceRequest` decoding in
  Task 1.2, and Task 1.1 is limited to proving the seam and RED `/v1/ns`
  failure.
- P2: The raw-HTTP static test is narrow. Ruling: strengthen the production
  path checker in Task 1.2/Stage 2 to detect URL construction and HTTP method
  calls, not only imports; the compatibility fixture remains the only named
  exception.
- P2: The RED black-box test ignored operation errors. Ruling: the test must
  fail on a non-nil operation error before inspecting requests; Task 1.2's
  concrete routing tests will also assert successful request capture.
- P2: Read/list/subscription seam routing is not yet covered. Ruling: add
  those operation-specific tests in Task 1.2 before accepting the SDK stage.

Task 1.1 review round 1: NEEDS_CHANGES. The reviewer confirmed the v3 pin and
intentional RED `/v1/ns` failure, but found missing seam coverage for reads,
service listing, subscriptions, close, and error propagation, plus a RED test
that ignored operation errors. The developer is adding those tests before a
fresh review; concrete proto decoding remains Task 1.2 as ruled above.

Task 1.1 review round 2: PASS for the amended RED-seam scope. The fresh
reviewer confirmed all seven seam methods, identity/metadata, forced
`Ephemeral=false`, and error propagation including deregistration. The
intentional black-box `/v1/ns` RED remains the expected pre-Task-1.2 signal;
the report correction was independently reviewed and passed. Task 1.1 is
complete once the plan-boundary amendment is committed and pushed.

## Task 1.2 live finding and ruling

The first direct gRPC implementation used `NamingGrpcProxy.RegisterInstance`
(`InstanceRequest`) for persistent items. A real Nacos 3.2.4 ARM64 run showed
that 201 same-service writes collapse to one entry, and a protocol
`BatchInstanceRequest` payload is exposed as ephemeral by the target. A direct
probe against the official SDK's lower-level RPC transport showed that Nacos
3's `PersistentInstanceRequest` with unique IP/port identities retains multiple
persistent entries. Ruling: Task 1.2 must replace the current path with a
thin official-SDK RPC adapter for `PersistentInstanceRequest`; logical batches
remain capped at 100, but persistent protocol batch is prohibited. Stage 1.2
is blocked until the 201-item live test passes with `Ephemeral=false` and exact
cardinality.

Task 1.2 review round 1: NOT PASS. Findings include a P0 authentication bypass
in the custom persistent RPC path (it skipped SDK security injection), a P1
read barrier that could prune before exact identity convergence, a P1 mismatch
between logical 100-item batches and deferred all-scope execution, and a P1
protobuf test that did not run the official payload codec. The developer is
fixing these issues before a fresh review. The live 201 test result is useful
evidence but cannot close the stage while auth and barrier safety are open.

Task 1.2 review round 2: NOT PASS. The fresh reviewer found that
`hasPersistentVendor()` still checks only the facade field while production
stores the vendor inside `grpcSDKClient`; therefore the exact convergence
barrier remains disabled in the real path. A developer fix must expose the
capability through the shim and add a production-shaped barrier/no-prune test.

Task 1.2 review round 3: NOT PASS. Fresh live verification reproduced the
Nacos 3 basic gRPC lifecycle PASS but the 201-item exact-barrier gate timed out.
Independent probe identified the deterministic cause: Nacos returns grouped
`ServiceName` (`DEFAULT_GROUP@@service`) while the barrier expected the raw
service name. The batch evidence was downgraded to NOT VERIFIED until this
normalization is fixed and the live gate reruns. A prior report hash came from
the barrier-disabled run and is not release evidence.

Task 1.2 review round 4: PASS. A fresh independent reviewer confirmed the
PersistentInstanceRequest gRPC/auth path, active exact barrier, logical 100-item
batching, codec round-trip, and no raw HTTP production path. Fresh local
`go test ./pkg/nacos/... -count=1`, `go test -race ./pkg/nacos/... -count=1`,
and `go vet ./pkg/nacos/...` all passed. One earlier race run showed a timing
flake in the existing HTTP-compat prune wall-clock assertion; immediate reruns
passed, so it remains a P2 test-stability follow-up rather than a release
blocker. Task 1.2 is ready for the required stage push after docs/evidence are
committed.

## Stage 2 review findings and rulings

Task 2.1/2.2 review round 1: PASS. An independent reviewer covered the health
policy type, SDK and compatibility transport construction, sink registration,
server/composition wiring, and focused black-box/white-box tests. The reviewer
confirmed that `deployment-owned` is the zero-value/default policy, naming
operations do not require a cluster-admin facade, and `admin-managed` remains an
explicit fail-closed opt-in with no raw HTTP fallback. Focused package and
internal tests passed, as did `go test -count=1 ./pkg/nacos`, `go test
-count=1 ./internal`, and the black-box sink suite.

P2 follow-up: `NacosHealthPolicy` is carried through config and composition but
is not currently exposed as a CLI flag. This does not block the deployment-owned
default or current Nacos 3 target; if operators must switch to admin-managed at
runtime, add an explicit flag, validation, documentation, and config tests in a
later bounded task.

Ruling: Stage 2 is accepted and ready to push. Stage 3 may proceed with the
deployment-owned default and must not reintroduce an unconditional cluster-admin
preflight.

## Stage 3 review findings and rulings

Task 3.1/3.2 review round 1: PASS. An independent concurrency review covered
`6656c5a..ea7fa1e` and confirmed the application-scoped partition key
(namespace/group/service/cluster/operation), the hard maximum of 100 items, the
global eight-call semaphore, stable order within a scope, overlap between
independent scopes, PersistentInstanceRequest item writes, and the
convergence-before-prune barrier. Focused package tests, the package race run,
`go vet ./pkg/nacos`, and `git diff --check` passed on the review rerun.

P2 follow-ups: the 201-item replacement regression uses a fallback fake and
therefore does not itself exercise the `hasPersistentVendor` branch or inspect
the wire-level `Ephemeral=false`; the separate SDK facade contract test and the
Stage 1.2 live Nacos 3 evidence cover those properties. The retry metric counts
failed full-sync executions eligible for replay rather than individual replay
attempts; this narrower meaning is documented and does not change the existing
typed retry queue behavior.

Ruling: Stage 3 is accepted and ready to push. The P2 test-strengthening and
metric-granularity items remain bounded follow-ups and must not weaken the
no-prune-on-error rule.

## Stage 4.1 review findings and rulings

Task 4.1 review round 1: NEEDS_CHANGES. Independent review confirmed the
Nacos 3.2.4 ARM64 image pin, gRPC-port readiness, SDK-backed live Observe view,
and fixture-only HTTP fallback, but found a P1: the harness stopped after a
TCP listener check and did not run an SDK naming read/write canary before
starting the child. A listener accepting connections alone is not proof that
Nacos naming is usable.

Fix: `TestObserveConsistency` now calls the public
`pkg/nacos.CheckReadinessWithConfig` SDK gate after the transport check and
fails closed before the child/cold attach if service-list or persistent
register/deregister cannot complete. A stale Nacos2 Makefile comment was also
corrected. Follow-up tagged tests and `go vet -tags observe ./tests/observe`
passed.

Ruling: The P1 is resolved. Stage 4.1 is accepted with the explicit local
scratch-auth deviation documented; Task 4.2 remains the unverified real
kwok scale gate.
