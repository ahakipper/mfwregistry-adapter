# Spotter Remaining Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every remaining in-scope Spotter engineering item after the
Nacos3/Kwok consistency work, while leaving AppCenter delivery, Nacos
deployment operations, and real Atlas wire compatibility explicitly deferred.

**Architecture:** The active runtime keeps explicit logger/config/notifier
dependencies at the composition root and uses the official Nacos Go SDK for
naming operations. Legacy globals are removed only after repository callers and
tests migrate. The real Kwok observation remains a separate evidence gate: the
observer emits only linearized source/Nacos snapshots and records all retry
latency. Historical commit-message cleanup is performed with a verified backup
ref and a generated mapping, never by silently rewriting an unreviewed branch.

**Tech Stack:** Go, client-go, Nacos Go SDK v3, kwokctl, Docker, Cobra, zap,
Prometheus, Go test/race/vet, Git notes/rebase tooling, and the existing
Observe/Nacos test harness.

**Spec:** `docs/superpowers/specs/2026-09-14-nacos3-kwok-and-global-removal-design.md`,
`docs/superpowers/plans/2026-09-14-nacos3-kwok-global-cleanup.md`, and the
current evidence contract in
`docs/evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md`.

## Global Constraints

- Do not implement AppCenter endpoint/payload/auth/SLA delivery in this plan.
- Do not add Nacos deployment HA, TLS, production auth, non-public namespace,
  or leaderless-recovery work; these remain deployment-owned out of scope.
- Do not implement or validate the real Atlas protobuf wire contract; Atlas is
  a deferred non-blocking sink.
- Do not add same-scale Consul observation; the current project has no machine
  deployment scenario and this is an accepted non-goal.
- All Nacos business operations use the official SDK; `http-compat` remains a
  separately named test/rollback path and is rejected by product wiring.
- Complete Nacos snapshots remain application-scoped with at most 100 items,
  persistent items use `PersistentInstanceRequest`, and prune runs only after
  a successful complete publication.
- Every implementation stage has a focused test gate, an independent review,
  a detailed English commit containing `Problem`, `Changes`, `Verification`,
  `Compatibility / Rollback`, and `Documentation`, followed by an explicit
  push to `refactor/all`.
- Never use `git reset --hard`, broad recursive deletion, or an unverified
  force-push. Any history rewrite starts from a named backup ref and is
  validated before the remote update.

---

## Stage 0 — Freeze scope and inventory the remaining work

### Task 0.1: Reconcile the authoritative status matrix

**Files:**

- Modify: `docs/remediation-execution-status-2026-09-13.md`
- Modify: `docs/system-readiness-consistency-audit-2026-09-12.md`
- Modify: `docs/system-readiness-consistency-remediation-plan-2026-09-12.md`
- Modify: `docs/dsca-4-observation.md`
- Modify: `docs/nacos-sink-plan.md`
- Modify: `docs/README.md`

**Steps:**

- [ ] Mark the fresh Nacos3 ARM64 SDK lifecycle, complete Instance payload,
      linearized Observe reader, and one-hour 1000/20 evidence as completed,
      linking `docs/evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md`.
- [ ] Keep the two-hour burst Observe gate explicitly `NOT VERIFIED` until its
      own run produces complete evidence.
- [ ] Mark AppCenter delivery, deployment-owned Nacos operations, Atlas wire,
      and Consul scale with the exact out-of-scope wording from this plan.
- [ ] Record that `go vet ./...` is currently clean and that application-level
      batch is implemented even though Nacos has no persistent protocol batch.
- [ ] Run a link/path check over every changed Markdown file and
      `git diff --check`.
- [ ] Have a documentation reviewer inspect scope wording and stale historical
      claims.
- [ ] Commit and push `Stage 0: reconcile remaining in-scope status`.

## Stage 1 — Migrate callers away from legacy globals

### Task 1.1: Prove the active graph has no legacy imports

**Files:**

- Modify: `scripts/check_no_legacy_globals.sh`
- Add: `scripts/check_no_legacy_globals_test.sh`
- Modify: `internal/composition/boundary_test.go`
- Modify: `cmd/adapter_test.go`, `internal/composition/boundary_test.go`,
  `internal/composition/global_isolation_test.go`,
  `pkg/distribute/election/election_test.go`,
  `pkg/providers/consul/blackbox_test.go`,
  `pkg/providers/consul/deps_boundary_test.go`,
  `pkg/providers/k8s/deps_boundary_test.go`,
  `pkg/providers/k8s/k8s_whitebox_test.go`,
  `pkg/worker/elector_test.go`, `tests/e2e/consul_pipeline_test.go`,
  `tests/e2e/fanout_pipeline_test.go`, and
  `tests/e2e/nacos_reconcile_e2e_test.go`

**Steps:**

- [ ] Run `rg -n 'spotter/(config|pkg/log|pkg/notice)|internal/infra/legacycompat'
      --glob '*.go'` and write the exact result into the task report.
- [ ] Replace test-only imports of `spotter/config`, `pkg/log`, `pkg/notice`,
      and `legacycompat` with `internal/ports` fakes and explicit
      `internal/infra/config.Config` values.
- [ ] Add a static test that production packages under `cmd`, `internal`, and
      `pkg` cannot import any legacy path.
- [ ] Run `go list -deps ./cmd/...` and assert the deleted package paths are
      absent from the active binary dependency graph.
- [ ] Run `go test ./... -count=1`, `go test -race ./... -count=1`, and the
      static checker; verify failures are caused by real behavior regressions,
      not missing compatibility symbols.
- [ ] Have a reviewer confirm that each migrated test still asserts behavior,
      not merely constructor shape.
- [ ] Commit and push `Stage 1: migrate active callers from legacy globals`.

## Stage 2 — Delete the obsolete compatibility packages

### Task 2.1: Remove globals, shims, and obsolete tests

**Files:**

- Delete: `config/config.go`
- Delete: `pkg/log/log.go`, `pkg/log/log_test.go`
- Delete: `pkg/notice/notice.go`, `pkg/notice/notice_test.go`
- Delete: `pkg/notice/appcenternotice/appcenternotice.go`
- Delete: `internal/infra/legacycompat/legacy.go`,
  `internal/infra/legacycompat/legacy_test.go`
- Delete: `pkg/providers/k8s/legacy_provider_compat.go`,
  `pkg/providers/k8s/legacy_compat.go`
- Delete: `pkg/providers/consul/legacy_compat.go`
- Delete: `pkg/worker/legacy_compat.go`
- Delete: `pkg/distribute/election/legacy_compat.go`
- Modify: `internal/infra/notice/notice.go`
- Modify: `scripts/check_no_legacy_globals.sh`

**Steps:**

- [ ] Confirm every deletion target with `rg --files` and verify deployment
      assets under `config/certs` and `config/kubeconfigs` are not targets.
- [ ] Remove compatibility constructors and package-level mutable state after
      Task 1's import gate is green.
- [ ] Keep `internal/infra/notice` as the explicit fail-closed notifier; do not
      reintroduce AppCenter delivery or a package-global logger.
- [ ] Make the static checker fail on imports, assignments, and symbols from
      deleted package paths without exemptions.
- [ ] Run `go list -deps ./cmd/...`, `go test ./... -count=1`,
      `go test -race ./... -count=1`, and `go vet ./...`.
- [ ] Have a reviewer inspect public API breakage and confirm the composition
      root still wires logger/config/notifier explicitly.
- [ ] Commit and push `Stage 2: delete legacy global compatibility tree`.

## Stage 3 — Run the two-hour burst reliability gate

### Task 3.1: Execute and independently recompute the burst result

**Files:**

- Create: `docs/evidence/nacos3-kwok-burst-observe-2026-09-15.md`
- Generate outside Git or attach by explicit review: the run JSONL and harness
  logs for the two-hour window

**Steps:**

- [ ] Verify `kwokctl --version`, the Nacos ARM64 image architecture/digest,
      Docker ports, and a clean owned-state directory.
- [ ] Run the short burst smoke first with `OBS_SCALE=200`,
      `OBS_SERVICES=5`, `OBS_DURATION=10m`, and `OBS_BURSTS=true`; stop and
      record `NOT VERIFIED` on any dropped event, cleanup error, or bound breach.
- [ ] Run the definitive command:
      `OBS_SCALE=1000 OBS_SERVICES=20 OBS_DURATION=2h OBS_BURSTS=true make test-observe`.
- [ ] Require every emitted tick to have `source=expected=Nacos`,
      `exactEqual=true`, zero field divergence, zero dropped events, and no
      bound-expired snapshot retry. Record `snapshotAttempts` and
      `snapshotWaitMs` to preserve efficiency evidence.
- [ ] Verify create-before-delete churn, all scheduled burst shapes, one
      application emptying, retry recovery, final queue drain, and complete
      kwok/Nacos/child cleanup.
- [ ] Independently recompute the verdict from JSONL with a shell/`jq`
      verifier and compare its counts with the generated summary.
- [ ] Have a reliability reviewer inspect both the recomputation and the
      retained harness log slices.
- [ ] Commit and push a detailed PASS evidence document, or a detailed
      `NOT VERIFIED` blocker document if any criterion fails.

## Stage 3A — Three-plane watch-assisted latency evidence

- [x] Keep the periodic linearized snapshot as the correctness gate: each tick
      reads authoritative K8s state, the guarded Spotter informer projection,
      and a fresh official-SDK Nacos catalog, then compares complete Instance
      payloads.
- [x] Add an independent client-go K8s Watch with initial List/resourceVersion
      and reconnect/error reporting.
- [x] Add an official Nacos SDK Subscribe observer with empty-service updates
      enabled and safe callback shutdown; supplement callbacks with fresh
      catalog reads because callbacks can coalesce.
- [x] Add the scale ladder report for 1/10/100/500/1000 create/delete samples
      and CrashLoopBackOff/recovery, with API→Nacos and source→Nacos P90/P95/P99.
- [ ] After the interrupted sampled run is documented, execute the ladder and
      the renewed long gate; any watcher error or missing callback is a failed
      evidence condition, not silent success.

## Stage 4 — Produce the final branch verification matrix

### Task 4.1: Verify code, documentation, and repository boundaries

**Files:**

- Modify: `docs/evidence/nacos3-kwok-complete-instance-equality-2026-09-15.md`
- Modify: `docs/evidence/nacos3-kwok-target-2026-09-14.md`
- Modify: `docs/README.md`
- Add: `docs/evidence/remaining-closure-verification-2026-09-15.md`

**Steps:**

- [ ] Run `go test ./... -count=1`, `go test -race ./... -count=1`,
      `go test -tags observe ./tests/observe -count=1`,
      `go test -race -tags observe ./tests/observe -count=1`, and
      `go vet ./...`.
- [ ] Run every real Nacos3 scratch test with explicit target image/digest and
      preserve skip reasons for tests requiring deployment-owned endpoints.
- [ ] Run `scripts/check_no_legacy_globals.sh`, the Nacos SDK-only production
      tests (`go test ./pkg/nacos -run 'TestDefaultConstructorsUseSDK|Test.*SDK'`),
      `rg -n 'http\.NewRequest|http\.DefaultClient|doJSON' pkg/nacos --glob '*.go'`
      to review compatibility-only HTTP call sites, Markdown link/path checks,
      and `git diff --check`.
- [ ] Verify `git status --short` is clean, local HEAD equals the intended
      remote `refactor/all` HEAD, and every new commit has the required English
      body sections.
- [ ] Write the final matrix with PASS, NOT VERIFIED, and DEFERRED statuses;
      do not call deployment-owned or deferred work a failure of this plan.
- [ ] Have an independent whole-branch reviewer inspect the matrix and all
      scope exclusions.
- [ ] Commit and push `Stage 4: publish final in-scope verification matrix`.

## Stage 5 — Rewrite the 175 historical commit messages safely

### Task 5.1: Build a reviewable message mapping and backup

**Files:**

- Create outside the repository first: a timestamped commit mapping and
  backup-ref record
- Modify: `AGENTS.md` only if the final workflow needs a clarification

**Steps:**

- [ ] Identify the exact 175 commits from the user-approved range and verify
      their current hashes, authors, dates, and changed paths; do not infer the
      range from the current total commit count.
- [ ] Create an immutable backup ref named
      `backup/refactor-all-before-message-rewrite-20260915` and record its hash.
- [ ] Generate one detailed English message per commit from its diff and
      existing evidence, using the required sections; preserve author/date and
      parent topology.
- [ ] Have a reviewer inspect the complete mapping for factual accuracy,
      duplicate subjects, and accidental secrets before rewriting anything.
- [ ] Apply the mapping in an isolated clone/worktree and verify that the
      rewritten tree has the same file snapshots and commit count as the
      backup (`git diff --stat backup/... rewritten` must be empty at each
      matching commit checkpoint).

### Task 5.2: Validate and publish the rewritten history

**Steps:**

- [ ] Run `go test ./... -count=1`, `go vet ./...`, and the static checkers on
      the rewritten branch.
- [ ] Compare `git diff backup/refactor-all-before-message-rewrite-20260915`
      for the final tree; only commit metadata may differ.
- [ ] Obtain explicit review of the mapping and backup ref before updating the
      remote branch; use `git push --force-with-lease`, never plain force push.
- [ ] Record the backup ref, old/new HEADs, mapping checksum, and rollback
      command in `docs/evidence/remaining-closure-verification-2026-09-15.md`.
- [ ] Commit and push the final history-rewrite evidence as a normal detailed
      English commit after the rewritten branch is validated.

## Completion Criteria

This plan is complete when Tasks 0–4 have fresh PASS/DEFERRED/NOT VERIFIED
evidence with clean focused reviews, the two-hour burst gate has an explicit
result, the legacy package tree is deleted with no active imports, and Task 5
has either completed the user-approved 175-commit rewrite with a verified
backup or has a documented external blocker. AppCenter delivery, deployment
Nacos operations, Atlas wire compatibility, and Consul scale remain outside
the completion criteria by explicit user decision.
