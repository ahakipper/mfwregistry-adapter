# Spotter Batch and Global Dependency Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Every task
> ends with a focused test gate, independent review, and a detailed English
> commit.

**Goal:** Add bounded application-scoped batching for persistent Nacos full
syncs and remove package-level logger/config/notice globals from the active
Spotter graph without breaking the temporary compatibility boundary.

**Architecture:** K8s continues to emit one immutable SyncAll snapshot. The Nacos
Sink partitions it by namespace/group/application/cluster/operation, chunks each
scope at 100 items, and schedules persistent SDK calls with a global limit of
8. The DDD migration keeps internal/ports and the composition root as the only
active dependency path; deprecated public globals remain behind
internal/infra/legacycompat until external callers migrate.

**Tech Stack:** Go, official github.com/nacos-group/nacos-sdk-go/v2 pseudo-pin
v2.3.6-0.20260902123754-002486583df5, client-go, Cobra, zap, Prometheus, Go
test/race/vet, existing mock and tagged E2E harnesses.

**Spec:**
docs/superpowers/specs/2026-09-13-batch-and-global-refactor-design.md

## Global Constraints

- Keep Spotter Nacos instances persistent (Ephemeral=false).
- A persistent batch is a Spotter execution unit; the Nacos protocol batch API
  is used only when every item is temporary and the target supports it.
- Maximum persistent batch size is exactly 100; no configuration may raise it.
- Batch identity includes namespace, group, appCode/serviceName,
  provider/cluster, and operation type.
- Same application scope is ordered; independent scopes may execute concurrently
  with a global limit of 8 active Nacos item calls.
- Register/update and deregister items are never mixed in one batch.
- PushAll must not prune after a registration/update batch failure.
- Active production code must not read or write config globals, pkg/log.Logger,
  or pkg/notice.Noticer.
- Compatibility wrappers remain explicit and deprecated until external caller
  migration evidence exists; do not force-push existing history.
- Every commit has a detailed English subject and body with Problem, Changes,
  Verification, Compatibility / Rollback, and Documentation.
- Before completion run git diff --check, go vet ./..., go test ./... -count=1,
  and go test -race ./... -count=1.

---

### Task 1: Establish repository contribution and commit governance

**Files:**
- Create: AGENTS.md
- Test: repository checks only

**Interfaces:**
- Produces the commit-message contract consumed by later tasks.
- Does not change runtime behavior.

- [ ] Step 1: Record baseline and dirty state.

~~~sh
git rev-parse HEAD
git status --porcelain=v1 --untracked-files=all
~~~

Expected: the current branch name, HEAD, and an empty status. If files are
listed, record their paths and do not overwrite them.

- [ ] Step 2: Create AGENTS.md with this policy.

~~~markdown
# Repository Contribution Rules

## Commit messages

Every commit must use a detailed English subject and body. The subject must
name the area and behavior changed; generic one-line messages are forbidden.

The body must contain:

Problem:
Changes:
Verification:
Compatibility / Rollback:
Documentation:

Verification lists commands actually run and their results. Commits are atomic.
Do not rewrite pushed history or force-push without explicit user approval,
an immutable backup ref, and an independent review decision.

## Change workflow

Use test-first development for behavior changes. A task is complete only after
focused tests, race tests for concurrent code, and repository quality gates pass.
Keep historical audit evidence intact and add a current-status addendum for scope
changes.
~~~

- [ ] Step 3: Verify and commit.

~~~sh
git diff --check
rg -n "Problem:|Changes:|Verification:|Compatibility / Rollback:|Documentation:" AGENTS.md
git add AGENTS.md
git commit -m "chore: establish detailed English commit governance" \
  -m "Problem:
Recent commits do not consistently explain intent, verification, or compatibility impact.

Changes:
Add repository rules requiring detailed English subjects, structured bodies, atomic scope, and test evidence.

Verification:
- git diff --check
- rg -n 'Problem:|Changes:|Verification:|Compatibility / Rollback:|Documentation:' AGENTS.md

Compatibility / Rollback:
Policy-only change; code and pushed history are unchanged.

Documentation:
AGENTS.md is the source of truth for future commit messages."
~~~

Independent reviewer checks that the policy is enforceable and does not require
history rewriting. Push this commit only after review.

---

### Task 1A: Rewrite the recent remediation commit messages safely

**Files:**

- Create: docs/evidence/commit-message-rewrite-2026-09-13.md
- Create: scripts/validate_commit_message_map.py
- Create during the operation: backup tag
  backup/pre-detailed-commit-rewrite-20260913
- Modify: commit metadata only for the remediation range e708630^..HEAD
  (174 commits at plan authoring time)

**Interfaces:**

- Consumes the policy in AGENTS.md and the current refactor/all history.
- Produces a message-only rewrite: every commit in the selected remediation
  range keeps its original tree, parent order, author, and timestamp while its
  subject/body becomes detailed English with the five required headings.
- Original project history before e708630 is preserved. The range is a
  proposed default and must be confirmed if the user intends a different
  boundary.

- [ ] Step 1: Snapshot refs before any rewrite.

~~~sh
BASE=$(git rev-parse e708630^)
OLD_HEAD=$(git rev-parse HEAD)
BRANCH=$(git branch --show-current)
OLD_REMOTE=$(git rev-parse origin/$BRANCH)
git tag backup/pre-detailed-commit-rewrite-20260913 "$OLD_HEAD"
git log --reverse --format='%H%x09%P%x09%an%x09%ad%x09%s%x09%b' --date=iso \
  "$BASE..$OLD_HEAD" > /tmp/spotter-commit-messages-before.tsv
git rev-list --count "$BASE..$OLD_HEAD"
~~~

Expected: the command prints the exact recorded count (174 at plan authoring),
the backup tag, and the remote ref. The recorded count, not a hard-coded number,
is authoritative for the confirmed range; stop if the branch or range differs.

- [ ] Step 2: Generate and review a per-commit message map.

For every commit in BASE..OLD_HEAD, inspect its diff and write one entry to
docs/evidence/commit-message-rewrite-2026-09-13.md containing the old hash,
new message, changed paths, and evidence provenance. Each new message must use
this exact shape and must not invent a test result:

~~~text
<area>: <specific imperative behavior summary>

Problem:
<concrete defect or missing evidence shown by the commit diff>

Changes:
<files and behavior changed by this commit>

Verification:
<commands actually recorded for this commit, or "Not recorded at commit time; covered by the branch verification matrix">

Compatibility / Rollback:
<compatibility boundary, feature flag, or test/documentation-only statement>

Documentation:
<documents or evidence artifacts updated, or "None">
~~~

The map must preserve commit order and include every hash in the recorded range;
missing or duplicate hashes fail the task.

- [ ] Step 3: Validate the map without changing refs.

~~~sh
python3 scripts/validate_commit_message_map.py \
  /tmp/spotter-commit-messages-before.tsv \
  docs/evidence/commit-message-rewrite-2026-09-13.md
~~~

The validator must exit non-zero when a hash is missing, a required heading is
missing, a subject contains CJK characters, or a body claims a test result not
present in the before snapshot. If Python is unavailable, implement the same
validator as a temporary Go program under /tmp; do not weaken the checks.

- [ ] Step 4: Apply a message-only rewrite on a disposable branch ref.

Create rewrite/commit-messages-20260913 from OLD_HEAD and use a non-destructive
message callback (preferred: git filter-repo with an explicit old-hash to
message map; fallback: scripted git rebase --rebase-merges with every commit
marked reword). The callback must refuse any commit outside BASE..OLD_HEAD and
must not alter trees or parents.

- [ ] Step 5: Prove content and metadata preservation.

~~~sh
NEW_HEAD=$(git rev-parse rewrite/commit-messages-20260913)
git diff --quiet "$OLD_HEAD" "$NEW_HEAD"
git range-diff "$BASE..$OLD_HEAD" "$BASE..$NEW_HEAD"
git rev-list --reverse "$BASE..$NEW_HEAD" | while read -r c; do
  git log -1 --format='%B' "$c" | rg -q '^Problem:$'
  git log -1 --format='%B' "$c" | rg -q '^Changes:$'
  git log -1 --format='%B' "$c" | rg -q '^Verification:$'
  git log -1 --format='%B' "$c" | rg -q '^Compatibility / Rollback:$'
  git log -1 --format='%B' "$c" | rg -q '^Documentation:$'
done
~~~

Expected: final trees are identical, range-diff shows only message changes, and
every rewritten commit contains all five headings.

- [ ] Step 6: Independent review and controlled remote replacement.

The reviewer must inspect the message map and preservation output. Only after
PASS, move the rewritten ref to refactor/all and push with an exact lease:

~~~sh
git branch -f refactor/all rewrite/commit-messages-20260913
git push --force-with-lease=refs/heads/refactor/all:$OLD_REMOTE \
  git@github.com:ahakipper/mfwregistry-adapter.git \
  refactor/all:refactor/all
git push git@github.com:ahakipper/mfwregistry-adapter.git \
  backup/pre-detailed-commit-rewrite-20260913
BRANCH=$(git branch --show-current)
git fetch origin "$BRANCH"
test "$(git rev-parse "$BRANCH")" = "$(git rev-parse "origin/$BRANCH")"
~~~

If the lease fails, stop and report the remote movement; never use plain
--force. The backup tag permits recovery to the old tip.

- [ ] Step 7: Commit the evidence after the rewrite.

~~~sh
git add docs/evidence/commit-message-rewrite-2026-09-13.md
git commit -m "docs: record detailed remediation commit history rewrite" \
  -m "Problem:
Recent remediation commits lack the detailed English context required for review and rollback.

Changes:
Rewrite only the confirmed remediation range with structured English subjects and bodies while preserving commit trees, parents, authors, and timestamps.

Verification:
- git diff --quiet OLD_HEAD NEW_HEAD
- git range-diff BASE..OLD_HEAD BASE..NEW_HEAD
- per-commit five-heading validator
- remote force-with-lease parity check

Compatibility / Rollback:
Original project history is untouched and backup/pre-detailed-commit-rewrite-20260913 restores the prior tip.

Documentation:
docs/evidence/commit-message-rewrite-2026-09-13.md records the mapping and preservation evidence."
~~~

This task is a destructive ref operation. Do not execute it until the user
confirms the exact e708630^..HEAD range and the independent reviewer approves
the message map.

---

### Task 2: Pin application-scoped persistent batch boundaries with failing tests

**Files:**
- Create: pkg/nacos/batch_test.go
- Read: pkg/nacos/nacos.go, pkg/nacos/sdk.go,
  internal/domain/instance/model.go

**Interfaces:**
- Produces tests for persistentBatchKey, persistentBatch, and
  splitPersistentBatches([]*instance.Instance, int) []persistentBatch.
- Consumes the existing instance status constants and clusterOf helper.

- [ ] Step 1: Add these tests.

~~~go
func TestSplitPersistentBatchesGroupsByApplicationScopeAndOperation(t *testing.T)
func TestSplitPersistentBatchesHardCapsEachApplicationAt100(t *testing.T)
func TestSplitPersistentBatchesPreservesStableInputOrder(t *testing.T)
~~~

The first test uses pay/k8s online, pay/k8s unhealthy, pay/ecs online, and
pay/k8s offline items and requires three batches: k8s register, ecs register,
and k8s deregister. The second creates 201 pay/k8s online items and requires
sizes 100, 100, and 1. The third asserts instance IDs remain in input order.
Use a test-only batchSizes helper.

- [ ] Step 2: Verify RED.

~~~sh
go test ./pkg/nacos -run 'TestSplitPersistentBatches' -count=1
~~~

Expected: compile failure naming the missing partition types/function/constants.

- [ ] Step 3: Commit the contract tests.

~~~sh
git add pkg/nacos/batch_test.go
git commit -m "test(nacos): define application-scoped persistent batch boundaries" \
  -m "Problem:
Full snapshots have bounded per-instance concurrency but no application-scoped work unit or hard item limit.

Changes:
Pin grouping by namespace, group, service, cluster, and operation; cap each persistent batch at 100 items; preserve input order.

Verification:
- go test ./pkg/nacos -run 'TestSplitPersistentBatches' -count=1 (expected RED before implementation)

Compatibility / Rollback:
Tests only; existing Push and PushAll behavior is unchanged.

Documentation:
The batch design spec defines the persistent batch semantics."
~~~

---

### Task 3: Implement bounded persistent batch execution in the Nacos Sink

**Files:**
- Create: pkg/nacos/batch.go
- Modify: pkg/nacos/nacos.go around Push, PushAll, and pushInstances
- Modify: pkg/nacos/sink_test.go and pkg/nacos/sdk_test.go when recorder seams
  are needed

**Interfaces:**
- Produces:
  - const MaxPersistentBatchSize = 100
  - type batchOperation string with batchRegister and batchDeregister
  - type persistentBatchKey with Namespace, Group, Service, Cluster, Operation
  - type persistentBatch with Key, Items, Indexes
  - splitPersistentBatches(instances []*instance.Instance, maxItems int) []persistentBatch
  - (s *Sink) pushPersistentBatches(instances []*instance.Instance) error
- Consumes Sink.pushOne, currentPushConcurrency, the official SDK facade, and
  existing FullOperationSink retry metadata.

- [ ] Step 1: Implement the pure partitioner.

Rules:
1. Clamp maxItems to 100; values below 1 use 1.
2. Skip nil entries exactly as pushOne does.
3. Online/unhealthy map to batchRegister; offline maps to batchDeregister;
   unknown statuses are skipped.
4. Use configured namespace/group, AppCode, clusterOf(ins), and operation in the
   key.
5. Split matching items at maxItems and retain original input indexes.
6. Emit first-seen key order and input item order, never map iteration order.

- [ ] Step 2: Run the partition tests GREEN.

~~~sh
go test ./pkg/nacos -run 'TestSplitPersistentBatches' -count=1
~~~

- [ ] Step 3: Implement pushPersistentBatches.

Use one global semaphore sized by currentPushConcurrency for actual SDK item
calls; never create one full worker pool per batch. Serialize batches for one
application scope and allow independent scopes to overlap. Store item errors by
original index and return the first input-order error after every item was
attempted. Every item still calls pushOne, retaining health-check, persistent
instance, empty-IP, and status mapping behavior. Never call
BatchRegisterEphemeral from this path.

Add these tests:

~~~go
func TestPushPersistentBatchesLimitsRequestsAndPreservesApplicationOrder(t *testing.T)
func TestPushPersistentBatchesAttemptsEveryItemAndReturnsFirstInputError(t *testing.T)
func TestPushAllSkipsPruneWhenPersistentBatchFails(t *testing.T)
~~~

Use existing fakeSDKNaming, injected NacosClusterAdmin, and request recorders.
Assert actual parameters, max concurrency, deterministic first error, and zero
prune-list calls after injected registration failure.

- [ ] Step 4: Route only full sync through the batch executor.

Change Sink.PushAll to call pushPersistentBatches before prune. Keep Sink.Push on
the existing incremental pushInstances path. This means K8s initial flush and
periodic SyncAll use application batches while watch events remain single-item.

- [ ] Step 5: Run focused and race tests.

~~~sh
go test ./pkg/nacos -run 'Test(PushPersistentBatches|PushAll|SplitPersistentBatches)' -count=1
go test -race ./pkg/nacos -count=1
~~~

Expected: exit 0 with no race reports. Independent review must verify that
applications, clusters, and operations cannot mix and concurrency is not
multiplied by the number of batches.

- [ ] Step 6: Commit and push after review.

~~~sh
git add pkg/nacos/batch.go pkg/nacos/nacos.go pkg/nacos/sink_test.go pkg/nacos/sdk_test.go
git commit -m "feat(nacos): execute persistent full syncs as bounded application batches" \
  -m "Problem:
Initial and periodic snapshots can contain thousands of instances without an application-scoped execution bound.

Changes:
Partition SyncAll by scope, cap each persistent batch at 100, serialize one scope, and enforce a global SDK request limit of 8. Preserve persistent RegisterInstance/DeregisterInstance semantics and defer prune until registration batches succeed.

Verification:
- go test ./pkg/nacos -run 'Test(PushPersistentBatches|PushAll|SplitPersistentBatches)' -count=1
- go test -race ./pkg/nacos -count=1

Compatibility / Rollback:
Incremental Push remains unchanged; the previous PushAll executor can be restored without changing the persistent Nacos contract.

Documentation:
Update the Nacos sink plan and execution ledger with application-batch versus protocol-batch status."
~~~

---

### Task 4: Prove K8s startup, full-sync, and retry integration

**Files:**
- Modify: pkg/providers/k8s/k8s_whitebox_test.go
- Modify: pkg/worker/worker_test.go
- Modify: internal/server_test.go
- Create or modify: tests/e2e/nacos_batch_e2e_test.go

**Interfaces:**
- Consumes flushInstances, SyncAll, RetryOperation, and
  FanoutSink.PushAllOperation.
- Produces regression coverage for startup and periodic full snapshots.

- [ ] Step 1: Add failing tests:

~~~go
func TestK8sInitialFlushRoutesCompleteSnapshotToNacosBatchPath(t *testing.T)
func TestFullSyncRetryPreservesBatchIdentityAndScope(t *testing.T)
~~~

The first creates 201 K8s-shaped instances for one app and asserts three ordered
Nacos batches of 100, 100, and 1 before prune. The second asserts that a failed
full operation retains OperateTypeSyncAll, Scope "k8s", BatchID, and source
sequence in the retry queue.

- [ ] Step 2: Verify RED.

~~~sh
go test ./pkg/providers/k8s ./pkg/worker ./internal \
  -run 'Test(K8sInitialFlushRoutesCompleteSnapshotToNacosBatchPath|FullSyncRetryPreservesBatchIdentityAndScope)' -count=1
~~~

Expected: the startup batch assertion fails because the integration observation
does not yet exist.

- [ ] Step 3: Add only the smallest observable batch seam.

Keep flushInstances as exactly one SyncAll event. Expose batch metadata through
existing logger/metrics/test recorder seams; do not add production-only hooks.

- [ ] Step 4: Run integration tests.

~~~sh
go test ./pkg/providers/k8s ./pkg/worker ./internal \
  -run 'Test(K8sInitialFlushRoutesCompleteSnapshotToNacosBatchPath|FullSyncRetryPreservesBatchIdentityAndScope)' -count=1
go test -race ./tests/e2e -run 'Test.*Nacos.*(Batch|Reconcile)' -count=1
~~~

Expected: PASS; a failed batch must not prune and a retry must converge the
catalog.

- [ ] Step 5: Commit, review, and push.

Commit subject:
test(e2e): prove K8s initial snapshots use bounded Nacos batches

Body must state the startup/retry problem, exact tests run, compatibility behavior,
and documentation updated.

---

### Task 5: Run the guarded ARM64 Nacos batch/recovery gate

**Files:**
- Modify: tests/e2e/nacos_sdk_eval_test.go
- Modify: tests/e2e/nacos_real_test.go
- Create or modify: docs/evidence/nacos-arm64-batch-2026-09-13.md
- Modify: docs/nacos-sink-plan.md, docs/testing.md,
  docs/remediation-execution-status-2026-09-13.md

**Interfaces:**
- Consumes the existing guarded Nacos real-test environment.
- Produces protocol-batch versus Spotter-application-batch evidence.

- [ ] Step 1: Add TestNacosRealPersistentApplicationBatch under nacos_real.

The test must skip with explicit NOT VERIFIED: EnvError without NACOS_SERVER and
scratch write guards. It creates 201 persistent instances for one application
through the Spotter batch path, verifies exactly 201 owned catalog entries,
injects one request failure, retries, verifies exact convergence, deregisters
everything, and records residual_unknown=false.

- [ ] Step 2: Run the guarded test.

~~~sh
go test -race -tags=nacos_real ./tests/e2e \
  -run TestNacosRealPersistentApplicationBatch -count=1
~~~

Without guards, expected result is an explicit NOT VERIFIED skip. The Nacos 2.1.0
ephemeral protocol batch may still return RequestHandler Not Found; that result
must not be confused with the persistent application-batch fallback.

- [ ] Step 3: Record evidence.

Record image digest, SDK pseudo-version, namespace, group, application, batch
sizes, max observed concurrency, injected error, retry result, final catalog
hash, and cleanup status. Update docs with:

~~~text
Nacos protocol batch: persistent unsupported by the pinned SDK/target.
Spotter persistent application batch: PASS when the guarded test converges.
~~~

- [ ] Step 4: Run Nacos gates and commit.

~~~sh
go test ./pkg/nacos -count=1
go test -race ./pkg/nacos -count=1
go test -race -tags=nacos_sdk_eval ./tests/e2e -run 'TestNacosSDK' -count=1
go test -race -tags=nacos_real ./tests/e2e \
  -run '^TestNacosRealPersistentApplicationBatch$' -count=1
go test -race -tags=nacos_real ./tests/e2e \
  -run '^TestNacosReal$' -count=1
~~~

Commit subject:
test(nacos): verify persistent application batches on ARM64 scratch

The commit body must distinguish protocol-level unsupported behavior from the
Spotter application-level result and include exact verification commands.

---

### Task 6: Pin no-global active-graph boundaries with TDD tests

**Files:**
- Modify: internal/composition/boundary_test.go
- Create: internal/composition/global_isolation_test.go
- Create: scripts/check_no_legacy_globals.sh
- Read: cmd/adapter.go, internal/server.go, internal/composition/root.go,
  internal/infra/config/config.go, internal/infra/legacycompat/legacy.go

**Interfaces:**
- Consumes composition.Build, composition.Runtime, NewServerFromDeps,
  WithDeps provider constructors, and worker.NewElectorWithDeps.
- Produces static and behavioral gates for the global migration.

- [ ] Step 1: Add a repository-wide static checker.

Create scripts/check_no_legacy_globals.sh with `set -eu` and these rules:

1. Scan every non-test Go file under cmd, internal, pkg, config, and tools.
2. Exclude only internal/infra/legacycompat/**, config/**, pkg/log/**, and
   pkg/notice/** from the compatibility-reader allowlist.
3. Fail if any other file imports spotter/config, spotter/pkg/log, or
   spotter/pkg/notice, or assigns to an exported symbol in those packages.
4. Print each violation as `path:line:rule` and exit 1; print
   `legacy-global-boundary: PASS` and exit 0 when clean.

The composition test invokes this script, so the rule covers the complete
active graph rather than only the internal/composition package.

- [ ] Step 2: Add this isolation regression test.

~~~go
func TestCompositionBuildsIndependentRuntimesWithoutGlobalMutation(t *testing.T)
~~~

Build two immutable infraconfig.Config values with distinct Env values and
injected fake Logger/Notifier instances. Assert the Runtime configs and
collaborators are independent and that sentinel legacy global values are
unchanged.

- [ ] Step 3: Add runtime access counters to the compatibility boundary.

Add atomic read/write counters and these functions to
internal/infra/legacycompat:

~~~go
type AccessCounts struct { Reads uint64; Writes uint64 }
func ResetAccessCounts()
func AccessCountsSnapshot() AccessCounts
~~~

Every compatibility global read or write increments the corresponding counter.
The counters are test-only observability and must not change compatibility
values or the public API.

- [ ] Step 4: Run the gates before implementation.

~~~sh
scripts/check_no_legacy_globals.sh
go test ./internal/composition \
  -run 'Test(ActiveGraphDoesNotImportLegacyGlobals|CompositionBuildsIndependentRuntimesWithoutGlobalMutation)' -count=1
~~~

Expected: the checker identifies every current active import, and the
behavioral test records the current global-access baseline. Do not weaken the
allowlist to make it pass.

- [ ] Step 5: Add the runtime integration assertion.

The command-to-runtime integration test resets counters, constructs the runtime
through composition.Build and NewServerFromDeps, and asserts zero legacy
reads/writes from the active startup path. Tests that intentionally call
deprecated wrappers reset and assert their own non-zero compatibility counts.

- [ ] Step 6: Commit the tests and checker.

Commit subject:
test(ddd): pin active graph isolation from package globals

The body must state the hidden process-state problem, exact test command,
compatibility preservation, and documentation plan.

---

### Task 7: Remove global wiring from command startup

**Files:**
- Modify: cmd/adapter.go
- Modify: internal/server.go
- Modify: internal/composition/root.go
- Modify: internal/infra/config/config.go
- Modify: cmd/adapter_test.go, internal/server_test.go,
  internal/composition/root_test.go

**Interfaces:**
- Consumes infraconfig.Load, composition.Build, and
  NewServerFromDeps(*composition.Runtime).
- Produces startup that never calls assignLegacyGlobals, log.LoggerInit,
  notice.InitNoticeClient, or writes public config globals.

- [ ] Step 1: Add TestAdapterStartupDoesNotAssignLegacyGlobals.

Seed sentinel values in deprecated global packages, run the command composition
seam, and assert sentinels remain unchanged.

- [ ] Step 2: Observe RED.

~~~sh
go test ./cmd ./internal/composition \
  -run TestAdapterStartupDoesNotAssignLegacyGlobals -count=1
~~~

Expected: failure identifying the current bridge mutation.

- [ ] Step 3: Remove the bridge.

Delete the normal startup call and helper functions from cmd/adapter.go. Remove
now-unused imports. Continue creating providers through existing WithDeps
constructors. Keep deprecated wrappers available for external callers.

- [ ] Step 4: Verify and commit.

~~~sh
scripts/check_no_legacy_globals.sh
go test ./cmd ./internal ./internal/composition -count=1
go test -race ./cmd ./internal ./internal/composition -count=1
~~~

Commit subject:
refactor(composition): stop command startup from mutating legacy globals

The body must state the global coupling, explicit runtime wiring, exact tests,
rollback compatibility, and docs updated.

---

### Task 8: Migrate providers, conversion, and election off globals

**Files:**
- Modify: pkg/providers/k8s/k8s.go and conversion.go
- Modify: pkg/providers/consul/consul.go, client_factory.go, monitor.go
- Modify: pkg/distribute/election/election.go
- Modify: pkg/worker/elector.go
- Modify: internal/infra/legacycompat/legacy.go
- Modify: corresponding tests and internal/composition/boundary_test.go

**Interfaces:**
- Consumes explicit ports.Logger, ports.Notifier, ports.Clock, configuration,
  and filter values.
- Produces active constructors that never call legacycompat internally.
  Deprecated wrappers may call explicit constructors from the compatibility
  boundary.

- [ ] Step 1: Add failing direct-dependency tests.

~~~go
func TestK8SProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T)
func TestConsulProviderWithDepsDoesNotReadLegacyGlobals(t *testing.T)
func TestElectorWithDepsDoesNotReadLegacyGlobals(t *testing.T)
func TestFormatInstanceWithDepsDoesNotReadLegacyGlobals(t *testing.T)
~~~

Each supplies explicit dependencies and contradictory global sentinels, then
asserts the explicit values win.

- [ ] Step 2: Verify RED.

~~~sh
go test ./pkg/providers/k8s ./pkg/providers/consul ./pkg/distribute/election ./pkg/worker \
  -run 'Test(K8SProviderWithDeps|ConsulProviderWithDeps|ElectorWithDeps|FormatInstanceWithDeps)DoesNotReadLegacyGlobals' -count=1
~~~

- [ ] Step 3: Move compatibility outward.

Make WithDeps paths use only arguments and injected ports. Move fallback lookups
into deprecated wrapper functions. Keep internal/infra/legacycompat as the only
package allowed to import old globals.

- [ ] Step 4: Verify, review, and commit.

~~~sh
scripts/check_no_legacy_globals.sh
go test ./pkg/providers/k8s ./pkg/providers/consul ./pkg/distribute/election ./pkg/worker ./internal/composition -count=1
go test -race ./pkg/providers/k8s ./pkg/providers/consul ./pkg/distribute/election ./pkg/worker ./internal/composition -count=1
~~~

Commit subject:
refactor(ddd): route providers and election through explicit dependencies

The body must include the global-state problem, moved dependencies, exact tests,
compatibility wrapper behavior, and documentation updated.

---

### Task 9: Quarantine or retire remaining global packages

**Files:**
- Modify: config/config.go
- Modify: pkg/log/log.go
- Modify: pkg/notice/notice.go
- Modify: pkg/notice/appcenternotice/appcenternotice.go
- Modify: internal/infra/legacycompat/legacy.go
- Create: docs/migrations/legacy-callers-2026-09-13.md
- Modify: package tests and internal/composition/boundary_test.go

**Interfaces:**
- Consumes the external-caller inventory from Task 8.
- Produces explicit deprecated adapters or a documented removal gate.

- [ ] Step 1: Generate the caller inventory.

~~~sh
rg -n 'spotter/config|spotter/pkg/log|spotter/pkg/notice|LoggerInit\(|InitNoticeClient\(|config\.[A-Za-z]+' \
  --glob '*.go' --glob '!**/*_test.go' .
~~~

Classify every result as active production, test-only compatibility, or
external/unknown. Do not delete a public symbol while an active or unknown
caller remains.

- [ ] Step 2: Add deprecated API behavior-contract tests.

~~~go
func TestLegacyLoggerInitUsesConfiguredLegacyValues(t *testing.T)
func TestLegacyNoticeInitPreservesAppCodeAndEnvironment(t *testing.T)
func TestLegacyCompatibilitySnapshotsCopyValues(t *testing.T)
func TestLegacyPackagesAreNotUsedByActiveGraph(t *testing.T)
~~~

The logger test sets temporary legacy log globals, calls LoggerInit, verifies a
logger is created, and closes the temporary sink. The notice test calls
InitNoticeClient with a test environment and verifies the legacy notifier can
send through its documented local mirror. The snapshot test asserts copied
slices cannot mutate global backing arrays. The static test invokes the same
repository-wide checker. These contracts remain green until the shim is
actually retired.

- [ ] Step 3: Produce an external-caller migration inventory.

Create docs/migrations/legacy-callers-2026-09-13.md with one row for each
caller discovered outside this repository and these required columns:

~~~text
Caller repository | Owner | Entry point | Migration commit/PR | Target date |
Rollback window | Verification command | Status
~~~

If no external caller list is available, add one explicit row with
external/unknown, owner spotter-maintainers, status BLOCKED, and the reason
that repository-local rg cannot prove external migration. This is a release
gate, not an implied completion.

- [ ] Step 4: Select the safe state.

If there are no active or unknown callers, remove mutable implementation while
keeping an explicit compile-compatible adapter. If callers are unknown, leave
globals in the deprecated package, document the migration gate, and do not
claim shim retirement.

- [ ] Step 5: Verify and commit.

~~~sh
scripts/check_no_legacy_globals.sh
go test ./config ./pkg/log ./pkg/notice ./internal/infra/legacycompat ./internal/composition -count=1
go test -race ./config ./pkg/log ./pkg/notice ./internal/infra/legacycompat ./internal/composition -count=1
~~~

Commit subject:
refactor(compat): quarantine remaining package-global adapters

The body must state caller inventory, safe retirement decision, exact tests,
and rollback behavior.

---

### Task 10: Whole-branch review, documentation, and release gates

**Files:**
- Modify: docs/ddd-architecture.md
- Modify: docs/nacos-sink-plan.md
- Modify: docs/testing.md
- Modify: docs/remediation-execution-status-2026-09-13.md
- Modify: docs/system-readiness-consistency-audit-2026-09-12.md

**Interfaces:**
- Consumes all previous task commits and evidence.
- Produces the final ledger separating protocol batch, Spotter application
  batch, and legacy shim status.

- [ ] Step 1: Run the complete matrix.

~~~sh
git diff --check
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
make test-all
go test -race -vet=off -tags=observe -run '^TestObserveUnit' ./tests/observe/... -count=1
bash -n scripts/observe-up.sh scripts/observe-down.sh scripts/observe_lifecycle_test.sh
bash scripts/observe_lifecycle_test.sh
~~~

Guarded real tests without a target must report explicit NOT VERIFIED.

- [ ] Step 2: Inspect status and documentation.

~~~sh
git status --short
BRANCH=$(git branch --show-current)
git diff "origin/$BRANCH...HEAD" --stat
rg -n 'Batch|global|legacy|Nacos|Atlas|AppCenter|Current release blockers' \
  docs/nacos-sink-plan.md docs/testing.md docs/ddd-architecture.md \
  docs/remediation-execution-status-2026-09-13.md \
  docs/system-readiness-consistency-audit-2026-09-12.md
~~~

Confirm max 100, one application scope per batch, no protocol claim beyond target
support, no deployment-level Nacos HA/TLS/auth scope expansion, Atlas default
runtime caveat retained, AppCenter independent from shim retirement, and only
verified active blockers in the current list.

- [ ] Step 3: Dispatch an independent whole-branch reviewer.

The reviewer must report P0/P1/P2 findings for batch grouping, retry/prune
ordering, concurrency, global isolation, commit-policy compliance, and scope
documentation. Any P0-P2 finding starts a fix-and-review cycle.

- [ ] Step 4: Commit final documentation.

Commit subject:
docs: publish batch and dependency-boundary release status

The body must include the problem, published changes, exact verification commands,
compatibility/rollback boundary, and synchronized documentation list.

- [ ] Step 5: Push and verify remote parity.

~~~sh
BRANCH=$(git branch --show-current)
git push origin "$BRANCH"
git fetch origin "$BRANCH"
test "$(git rev-parse HEAD)" = "$(git rev-parse "origin/$BRANCH")"
git status --short
~~~

If HTTPS credentials are unavailable, use the configured GitHub SSH URL without
changing the canonical remote configuration.
