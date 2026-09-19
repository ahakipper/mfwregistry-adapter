# Spotter Nacos 3, Kwok Scale, and Legacy Global Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` to execute this plan task by task.
> Each task has a focused test gate, an independent review, a detailed English
> commit, and a push before the next stage begins.

**Goal:** Make Nacos 3.2.4 the supported target, prove the real K8s-to-Nacos
path with a two-hour kwok scale run, decouple health-check administration from
runtime naming writes, and remove the obsolete package-global compatibility
tree.

**Architecture:** The Nacos adapter exposes a vendor-neutral naming boundary
whose production implementation uses the official Nacos Go SDK gRPC surface
for persistent registration, deregistration, reads, and subscriptions. Full
snapshots remain logical application batches of at most 100 items, with a
global item-call limit of 8 and no unsafe assumption that protocol batch calls
are additive. Kwok drives the real client-go informer/cache/worker path while
the observer compares Kubernetes and Nacos complete views. The composition
root remains the sole owner of logger, notifier, metrics, and configuration;
legacy globals and wrappers are deleted after all repository callers migrate.

**Tech Stack:** Go 1.25, official `github.com/nacos-group/nacos-sdk-go/v3`
release when available (otherwise pinned official `v3.x-dev` commit
`93a93504cc2fc450c702e60c82d7f81acffe5f28`), Nacos
`nacos/nacos-server:v3.2.4-slim` on `linux/arm64`, client-go, kwokctl,
Docker, Cobra, zap, Prometheus, Go test/race/vet, and the existing testkit.

**Spec:** `docs/superpowers/specs/2026-09-14-nacos3-kwok-and-global-removal-design.md`

## Global Constraints

- Nacos server target is exactly `nacos/nacos-server:v3.2.4-slim` on
  `linux/arm64`; Nacos 2.1.0 is historical evidence only.
- All Nacos business operations use the official SDK; no raw HTTP business
  fallback and no compatibility-plugin dependency.
- Persistent registrations always carry `Ephemeral=false`.
- Application batches contain one namespace/group/service/cluster/operation
  scope and no more than 100 items.
- Same application scope executes in source order; independent scopes share a
  global Nacos item-call limit of 8.
- Do not use protocol `BatchRegisterInstance` for persistent Spotter data. Use
  the official SDK gRPC `PersistentInstanceRequest` per item; a 201-item
  service must retain all 201 persistent entries.
- A failed registration/update batch never runs prune for that snapshot.
- `healthChecker=NONE` is deployment configuration or an explicit
  Admin/Maintainer preflight, not a hidden requirement for naming SDK calls.
- Kwok tests must use `kwokctl` and real client-go watch/informer behavior;
  fake providers are not scale evidence.
- The definitive observe gate is at least 1,000 Pods, at least 20 services,
  at least two hours, continuous comparison, churn, burst deletion, retry,
  and verified cleanup.
- No production or test package may import deleted `pkg/log`, `pkg/notice`,
  `pkg/notice/appcenternotice`, package `spotter/config` for mutable globals, or
  `internal/infra/legacycompat` after the cleanup stage.
- Every stage commit uses detailed English subject/body sections:
  `Problem`, `Changes`, `Verification`, `Compatibility / Rollback`, and
  `Documentation`.
- After every stage: `git diff --check`, focused tests, independent review,
  commit, and `git push origin refactor/all`.

## Stage 0 — Freeze the new target and evidence contract

### Task 0.1: Update the design/status documents before code

**Files:**

- Modify: `docs/operations.md`
- Modify: `docs/testing.md`
- Modify: `docs/system-readiness-consistency-audit-2026-09-12.md`
- Modify: `docs/system-readiness-consistency-remediation-plan-2026-09-12.md`
- Modify: `docs/dsca-1-scale.md`
- Modify: `docs/dsca-4-observation.md`
- Modify: `docs/README.md`
- Create: `docs/evidence/nacos3-kwok-target-2026-09-14.md`

**Steps:**

- [ ] Record the current HEAD and clean worktree.
- [ ] Replace active Nacos 2.1 target wording with Nacos 3.2.4-slim ARM64.
- [ ] Mark old Nacos 2.1 scratch results as historical compatibility evidence.
- [ ] State that the current v2 facade routes persistent instances to legacy
      HTTP and is therefore not Nacos 3 release evidence.
- [ ] Add `docs/evidence/nacos3-kwok-target-2026-09-14.md` with the exact
      module pseudo-version/checksum, image target, gate status, tick schema,
      divergence-age formula, and ghost definition.
- [ ] Define the kwok gate, required environment, exact commands, JSONL
      evidence fields, cleanup checks, and `NOT VERIFIED` rules.
- [ ] Record the health-check decision: runtime naming does not require
      cluster-admin; deployment preconfiguration is the default contract.
- [ ] Run `git diff --check` and documentation link/path checks.
- [ ] Have a documentation reviewer inspect terminology and scope.
- [ ] Commit and push with a detailed English message.

## Stage 1 — Implement an official Nacos 3 SDK gRPC naming facade

### Task 1.1: Pin the official SDK source and add RED routing tests

**Files:**

- Modify: `go.mod`, `go.sum`
- Modify: `pkg/nacos/sdk.go`
- Create: `pkg/nacos/nacos3_sdk_test.go`
- Modify: `pkg/nacos/client_test.go`

**Interfaces:**

- `pkg/nacos` exports only Spotter-owned types and methods.
- The vendor adapter must provide `RegisterPersistent`,
  `DeregisterPersistent`, `SelectAll`, `ListServices`, `Subscribe`,
  `Unsubscribe`, and `Close` through an internal interface.

**Steps:**

- [ ] Check whether a released `nacos-sdk-go/v3` exists. If not, pin the
      official `v3.x-dev` pseudo-version resolving to commit `93a93504...` and
      record the reason in `docs/evidence/nacos3-kwok-target-2026-09-14.md`.
- [ ] Add tests that fail when persistent register/deregister are routed to
      `naming_http` or `/v1/ns`; tests must assert the Spotter-owned persistent
      operation seam and `Ephemeral=false`. Concrete vendor proto request
      decoding is a Task 1.2 responsibility because the adapter does not yet
      exist in this RED task.
- [ ] Add tests for namespace/group/service/cluster identity and metadata.
- [ ] Keep the existing facade failover/timeout/auth/TLS/complete-read tests as
      a regression baseline; concrete SDK-only routing and configuration
      assertions are a Task 1.2 responsibility because the vendor adapter is
      not constructed in this RED task.
- [ ] Add a static test that no production Nacos operation constructs a
      `net/http` request outside the explicitly removed compatibility package.
- [ ] Run the focused tests and verify they fail before implementation.

### Task 1.2: Implement the gRPC facade and Nacos 3 client construction

**Files:**

- Modify: `pkg/nacos/sdk.go`
- Modify: `pkg/nacos/client.go`
- Modify: `pkg/nacos/nacos.go`
- Create: `pkg/nacos/nacos3_grpc.go`

**Steps:**

- [ ] Construct the official SDK gRPC naming proxy behind the Spotter-owned
      interface; do not import vendor concrete types outside this adapter.
- [ ] Decode or capture the actual vendor request objects in tests and assert
      `PersistentInstanceRequest` request type, `registerInstance`/
      `deregisterInstance` method names, and `Ephemeral=false`; a seam-only
      fake is insufficient for this task.
- [ ] Add concrete SDK-only tests for failover, timeout, auth, TLS configuration,
      complete disabled/unhealthy reads, and error propagation across every
      operation-specific method.
- [ ] Route persistent register, deregister, select-all, service-list, and
      subscription operations through gRPC.
- [ ] Preserve the existing retry classification and server-list failover.
- [ ] Preserve isolated SDK cache directories and bounded close behavior.
- [ ] Make `CheckReadinessWithConfig` use the same gRPC facade and persistent
      canary semantics.
- [ ] Keep `http-compat` only as a separately named test fixture until the
      legacy cleanup stage removes it; product composition must reject it.
- [ ] Run `go test ./pkg/nacos/... -count=1` and
      `go test -race ./pkg/nacos/... -count=1`.
- [ ] Have a fresh SDK reviewer inspect all vendor routing and auth/TLS paths.
- [ ] Commit and push Stage 1.

## Stage 2 — Remove the cluster-admin health-check runtime coupling

### Task 2.1: Add the deployment-owned health policy and RED tests

**Files:**

- Modify: `pkg/nacos/client.go`
- Modify: `pkg/nacos/nacos.go`
- Modify: `pkg/nacos/sdk_test.go`
- Modify: `pkg/nacos/sink_test.go`
- Modify: `internal/infra/config/config.go`
- Modify: `internal/composition/root.go`

**Steps:**

- [ ] Add an explicit health policy value whose default means
      “Nacos cluster health checker is provisioned outside Spotter”.
- [ ] Add tests proving a naming client with no Admin/Maintainer facade can
      construct, register, deregister, and query persistent instances.
- [ ] Add tests proving registration never calls `UpdateHealthChecker` in the
      deployment-owned default policy.
- [ ] Add a separate opt-in preflight test for an injected Admin facade; its
      failure must be reported as preflight failure, not as a hidden write
      fallback.
- [ ] Run the RED tests before implementation.

### Task 2.2: Implement the decoupled health policy

**Files:**

- Modify: `pkg/nacos/client.go`
- Modify: `pkg/nacos/nacos.go`
- Modify: `internal/composition/root.go`
- Modify: `internal/server.go`
- Modify: `docs/operations.md`

**Steps:**

- [ ] Remove the unconditional SDK startup rejection for a nil cluster-admin.
- [ ] Remove per-instance `ensureClusterHealthCheckDisabled` calls from the
      default naming write path.
- [ ] Retain an explicit, injectable Admin/Maintainer preflight boundary only
      when configured by the deployment; never fall back to raw HTTP.
- [ ] Make the runtime error text distinguish naming failure from optional
      health-policy preflight failure.
- [ ] Document that `healthChecker=NONE` must be configured before Spotter
      starts, while Spotter remains responsible for `healthy`/`enabled` data.
- [ ] Run focused SDK/sink/server tests, race tests, vet, and the static raw
      HTTP checker.
- [ ] Have a fresh reviewer confirm the distinction between runtime naming and
      management APIs.
- [ ] Commit and push Stage 2.

## Stage 3 — Make application batching Nacos 3-safe

### Task 3.1: Add tests for 201-instance application batching

**Files:**

- Modify: `pkg/nacos/batch_test.go`
- Modify: `pkg/nacos/sink_test.go`
- Create: `pkg/nacos/nacos3_batch_contract_test.go`

**Steps:**

- [ ] Add a 201-instance single-application test expecting logical partitions
      `100 + 100 + 1`.
- [ ] Assert every item remains persistent and all 201 identities are sent.
- [ ] Assert no mixed register/deregister work unit and no mixed service or
      cluster scope.
- [ ] Assert same-scope ordering, independent-scope overlap, global limit 8,
      deterministic first error, retry, and “no prune after failure”.
- [ ] Add a regression test that fails if protocol batch replacement semantics
      reduce a 201-instance service to the final chunk.
- [ ] Run the new tests before changing implementation.

### Task 3.2: Implement the safe Nacos 3 application executor

**Files:**

- Modify: `pkg/nacos/batch.go`
- Modify: `pkg/nacos/nacos.go`
- Modify: `pkg/nacos/sdk.go`
- Modify: `pkg/worker/fanout.go`

**Steps:**

- [ ] Keep the application-level batch boundary at 100.
- [ ] Use the official SDK gRPC `PersistentInstanceRequest` for every
      persistent item inside each logical batch. Do not use
      `BatchInstanceRequest`: on Nacos 3 it is a complete publication and its
      high-level SDK path rejects persistent items.
- [ ] Preserve global 8-call concurrency and per-application order.
- [ ] Ensure `PushAll` registration/update completes before prune.
- [ ] Preserve retry metadata, source generation, and scope ownership.
- [ ] Add metrics for logical batch count, item count, retry count, and
      configured concurrency cap.
- [ ] Run package unit and race tests plus a synthetic 10,001-instance test.
- [ ] Have a fresh concurrency reviewer inspect ordering, cancellation, and
      partial-failure behavior.
- [ ] Commit and push Stage 3.

## Stage 4 — Execute the real kwok/Nacos 3 reliability gate

### Task 4.1: Update the Observe stack for Nacos 3 and real SDK readiness

**Files:**

- Modify: `tests/observe/stack.go`
- Modify: `tests/observe/config.go`
- Modify: `scripts/observe-up.sh`
- Modify: `scripts/observe-down.sh`
- Modify: `tests/observe/lifecycle_test.go`
- Modify: `Makefile`

**Steps:**

- [ ] Change the default image to `nacos/nacos-server:v3.2.4-slim` and force
      `--platform linux/arm64`.
- [ ] Replace v1 readiness/data probes with SDK-based naming readiness and
      gRPC-port checks; no Nacos data write uses curl or hand-written HTTP.
- [ ] Set explicit local Nacos auth identity/token values without committing
      secrets.
- [ ] Make the harness fail closed when Docker, kwokctl, kubectl, ports,
      image architecture, or image digest is not valid.
- [ ] Keep owned-state hashes and cleanup status; verify no residual container,
      cluster, Pod, temporary file, or Nacos registration.
- [ ] Add a short real smoke command and a two-hour command with exact knobs.
- [ ] Run all Observe unit/lifecycle tests and have an independent harness
      reviewer inspect cleanup and command timeouts.
- [ ] Commit and push Stage 4.1.

### Task 4.2: Run the large-scale kwok observation

**Files:**

- Create: `docs/evidence/kwok-nacos3-observe-2026-09-14.md`
- Create: `tests/observe/results/<timestamp>-nacos3-1000-2h.jsonl`
- Create: `tests/observe/results/<timestamp>-nacos3-1000-2h-summary.md`

**Steps:**

- [ ] Install or expose a verified `kwokctl` binary and record its version.
- [ ] Pull the ARM64 Nacos image and record its immutable digest.
- [ ] Run the short gate first: 200 Pods, 5 applications, 10 minutes, with
      burst deletes and one injected Nacos failure.
- [ ] Review short-gate output; do not continue if events drop, cleanup is
      unknown, or Nacos count diverges beyond the bound.
- [ ] Run the definitive gate:
      `OBS_SCALE=1000 OBS_SERVICES=20 OBS_DURATION=2h OBS_BURSTS=true make test-observe`.
- [ ] Record every 10-second tick: source count/hash, Spotter observed count,
      Nacos complete count/hash, divergence age, queue depth, batch sizes,
      retry state, and dropped-event count.
- [ ] Treat `OBS_SERVICES` as the application count; each application maps to
      one Nacos service scope. Use “application/service” consistently in
      evidence and summaries.
- [ ] Exercise create-before-delete churn, four burst shapes, deletion of all
      instances from one application, reconnect/retry, and final cleanup.
- [ ] Require 0 dropped events, 0 unexplained ghosts, bounded divergence age,
      exact final cardinality, and `residual_unknown=false`.
- [ ] Have a fresh reliability reviewer independently recompute the verdict
      from the JSONL and preserved harness logs.
- [ ] Commit and push Stage 4.2 only if the evidence meets the gate; otherwise
      commit a detailed `NOT VERIFIED` evidence record with the exact blocker
      and keep the stage open.

### Task 4.3: Close the complete Instance equality gap

**Files:**

- Modify: `internal/domain/instance/metadata.go`
- Modify: `pkg/providers/k8s/conversion.go`
- Modify: `pkg/nacos/nacos.go`
- Modify: `tests/observe/churn.go`
- Modify: `tests/observe/engine.go`
- Add: focused metadata, label, reversion, and payload-size tests

**Steps:**

- [x] Define a versioned canonical payload containing every `Instance`
      property, including `reversion`, all labels, all ports, images,
      resources, environment fields, state, and source identity.
- [x] Preserve all source labels in the production K8s conversion instead of
      dropping arbitrary labels before the sink boundary.
- [x] Store the canonical payload in compressed Nacos metadata so the full
      projection remains under Nacos's 1024-byte metadata limit; retain scalar
      keys only for legacy fallback.
- [x] Decode the payload in Nacos `GetAll` while keeping the host's actual
      wire location and enabled bit authoritative.
- [x] Build the Observe source model through the same production K8s
      conversion boundary; do not maintain a second reduced field model.
- [x] Make every tick compare the canonical payload byte-for-byte in addition
      to cardinality, identity, endpoint, scope, enabled, and lifecycle.
- [x] Use one fresh official SDK session per tick so the observer does not
      mistake the SDK's local subscription cache for the server's current
      Nacos view.
- [x] Add RED tests for label/reversion drift, complete round-trip, payload
      compression, and the Nacos metadata-size boundary.
- [x] Run package, race, vet, real Nacos 3 batch, and 100-Pod Observe smoke
      gates before starting a new definitive one-hour/scale run.
- [x] Run the definitive one-hour strict window with 1000 Pods and 20
      applications after the complete-payload and linearized-read fixes.
- [x] Treat all prior Observe runs as subset evidence until this task's fresh
      one-hour run passes.
- [x] Have an independent reviewer recompute the full-field equality scope
      and inspect Nacos metadata compatibility.
- [x] Commit and push Stage 4.3.

## Stage 5 — Delete the legacy globals and compatibility packages

### Task 5.1: Migrate all repository callers and tests

**Files:**

- Modify: `cmd/adapter_test.go`
- Modify: `tests/e2e/*.go`
- Modify: `pkg/providers/k8s/*_test.go`
- Modify: `pkg/providers/consul/*_test.go`
- Modify: `pkg/worker/*_test.go`
- Modify: `internal/infra/notice/*_test.go`

**Steps:**

- [ ] Replace legacy logger/config/notice setup with injected
      `internal/ports` fakes and `internal/infra/config.Config` values.
- [ ] Remove tests whose only purpose is preserving the explicitly abandoned
      source-compatibility API.
- [ ] Verify no test imports `spotter/config`, `spotter/pkg/log`, or
      `spotter/pkg/notice`.
- [ ] Run `go test ./... -count=1` and `go test -race ./... -count=1`.
- [ ] Have a reviewer inspect that tests still exercise behavior rather than
      merely deleting assertions.
- [ ] Commit and push Stage 5.1.

### Task 5.2: Remove the obsolete packages and wrappers

**Files:**

- Delete: `pkg/log/log.go`, `pkg/log/log_test.go`
- Delete: `pkg/notice/notice.go`, `pkg/notice/notice_test.go`
- Delete: `pkg/notice/appcenternotice/appcenternotice.go`
- Delete: `internal/infra/legacycompat/legacy.go`
- Delete: `internal/infra/legacycompat/legacy_test.go`
- Delete: `pkg/providers/k8s/legacy_provider_compat.go`
- Delete: `pkg/providers/consul/legacy_compat.go`
- Delete: `pkg/worker/legacy_compat.go`
- Delete: `pkg/distribute/election/legacy_compat.go`
- Delete: `config/config.go`
- Modify: `internal/infra/notice/notice.go`
- Modify: `scripts/check_no_legacy_globals.sh`
- Modify: `internal/composition/boundary_test.go`

**Steps:**

- [ ] Remove only the Go global/config code; retain deployment assets in
      `config/certs/` and `config/kubeconfigs/`.
- [ ] Remove the local AppCenter shim dependency; use the explicit infra
      notifier/fail-closed adapter instead.
- [ ] Delete all compatibility constructors and their documentation.
- [ ] Make the static checker fail on any import, symbol, or assignment to the
      deleted package names without exemptions.
- [ ] Run `go list -deps ./cmd` and assert none of the deleted packages appear.
- [ ] Run `go vet ./...`, `go test ./... -count=1`, and
      `go test -race ./... -count=1`.
- [ ] Have a fresh reviewer check package API breakage and deployment assets.
- [ ] Commit and push Stage 5.2.

## Stage 6 — Whole-branch verification and final documentation

### Task 6.1: Reconcile all status documents with evidence

**Files:**

- Modify: `docs/operations.md`
- Modify: `docs/architecture.md`
- Modify: `docs/ddd-architecture.md`
- Modify: `docs/testing.md`
- Modify: `docs/README.md`
- Modify: `docs/remediation-execution-status-2026-09-13.md`
- Modify: `docs/evidence/nacos3-kwok-target-2026-09-14.md`

**Steps:**

- [ ] Mark Nacos 3 SDK/gRPC only with the exact pinned version and digest.
- [ ] State whether the short and two-hour kwok gates are PASS or
      `NOT VERIFIED`; never infer a full run from unit tests.
- [ ] Remove the old claim that cluster-admin is a naming startup blocker.
- [x] Record that Atlas and AppCenter are excluded by scope; Consul scale
      remains a non-goal.
- [ ] Record the legacy package deletion and the intentional compatibility
      break.
- [ ] Run documentation link checks and `git diff --check`.

### Task 6.2: Final verification, independent whole-branch review, and push

**Steps:**

- [ ] Run `go vet ./...`.
- [ ] Run `go test ./... -count=1`.
- [ ] Run `go test -race ./... -count=1`.
- [ ] Run all tagged Observe, Nacos 3, and E2E tests with explicit skip output.
- [ ] Run the static legacy-global and raw-HTTP production-path checkers.
- [ ] Verify `git status --short`, branch HEAD, remote HEAD, and every stage
      commit body against `AGENTS.md`.
- [ ] Dispatch a fresh whole-branch reviewer on the complete diff and evidence.
- [ ] Resolve all P0-P2 findings with one scoped fix round and re-review.
- [ ] Commit the final documentation/evidence update and push
      `refactor/all`.

## Completion criteria

The plan is complete only when the Nacos 3 SDK path, the kwok scale evidence,
the health-policy boundary, and the legacy package deletion each have a clean
focused review and commit, and the whole-branch verification matrix reports
fresh PASS results. Any missing Docker/kwokctl/Nacos target or failed long run
remains explicitly `NOT VERIFIED` and is not silently promoted.
