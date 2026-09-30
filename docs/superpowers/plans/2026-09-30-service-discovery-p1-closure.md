# Service Discovery P1 Closure Plan — 2026-09-30

## Objective

Close the four P1 findings from the service-discovery chain audit with
test-first, reviewable changes while preserving the current Nacos 3 SDK data
path and the existing `clusterName=k8s|ecs` wire contract:

1. never allow a source-cluster collision to overwrite an existing Nacos
   composite identity, including after a Spotter process restart;
2. reject canonical metadata that the Nacos naming contract cannot carry;
3. make the server-side `healthCheckEnabled=false` requirement explicit and
   fail closed when a deployment requests proof that the approved SDK/admin
   seam cannot provide;
4. preserve application-scoped batch retry and prune safety when only part of
   a full snapshot is accepted.

The current branch already contains the first implementation of all four
areas. This plan closes the remaining evidence and restart-boundary gaps
instead of treating those earlier commits as blanket production proof.

## Non-goals and compatibility rules

- Atlas and AppCenter remain excluded from this release.
- Nacos HA, TLS, production authentication, namespace authorization, and
  leaderless recovery remain deployment concerns, not Spotter P1 gates.
- The official Nacos Go SDK remains the product naming path. Raw HTTP is allowed
  only in the explicitly named compatibility/admin test seam.
- A source-qualified Nacos `clusterName` migration is not enabled silently. If
  consumers later require simultaneous same-address instances from multiple
  source clusters, that is a separately approved wire migration with dual-read,
  rollout, and rollback evidence. Until then, the safe behavior is fail-closed.

## Stage 0 — Baseline and decision ledger

1. Confirm the current branch, remote, clean state, existing remediation
   commits, and the one-hour Nacos 3 + KWOK evidence.
2. Record the acceptance invariants and the explicit compatibility boundary in
   this document.
3. Run `go vet ./...`, `go test ./... -count=1`, `go test -race ./...`, and
   `git diff --check` before behavior changes.

**Deliverable:** this plan, a clean baseline, and a detailed English commit.

## Stage 1 — Restart-safe source ownership (F1)

### Test-first cases

- A fresh Sink rejects a second source that reuses the same service/IP/port
  after the first process has registered the original source.
- The catalog read failure is retryable and issues no register/deregister.
- A remote Spotter-owned record with no recoverable source identity is treated
  as unknown and is not overwritten.
- Existing same-process collision and non-collision tests remain unchanged.

### Implementation

- On the first mutation of a source-aware `(service, cluster)` scope, read the
  authoritative Nacos catalog and rebuild `wireOwners` from the canonical
  `sourceKey/sourceCluster` metadata.
- Serialize the first scan per scope; do not add a catalog read to legacy
  callers that provide no source identity.
- Fail closed before any register or deregister if the read fails or an owned
  remote identity is ambiguous.
- Keep the current in-memory claim ledger for concurrent writes after the scan.

**Gate:** focused Nacos tests, `go test -race ./pkg/nacos`, full quality gates,
and a detailed English commit/push.

## Stage 2 — Metadata envelope qualification (F2)

1. Add exact-boundary tests for the 1,024 UTF-16-code-unit Nacos limit.
2. Add Unicode supplementary characters, long image/source fields, many ports,
   and high-entropy labels to deterministic/property tests.
3. Verify that rejection occurs before health policy, SDK, or HTTP mutation and
   remains permanently classified for the retry queue.
4. Keep the complete canonical payload lossless; never truncate silently.

**Gate:** focused metadata/batch tests, race tests, and full repository gates.

## Stage 3 — Health-policy contract (F3)

1. Keep `deployment-owned` as the backward-compatible default and make its
   operator attestation visible in startup logs and runbook evidence.
2. Require an injected verifier for `verified`; missing or failed verification
   must prevent naming readiness and business writes.
3. Keep `admin-managed` behind an explicitly approved SDK/admin facade. Do not
   add a hidden raw-HTTP fallback because the official Go SDK currently does
   not expose the cluster-admin switch.
4. Add a deployment contract test proving the verifier runs before readiness
   and that `healthCheckEnabled=false` is the required readback.

**Gate:** composition/server/Nacos tests and an updated operations matrix. The
deployment verifier itself is an integration dependency, not something to
fake into a production PASS.

## Stage 4 — Full-batch failure closure (F4)

1. Fault-inject one permanent and one transient item in separate application
   scopes while healthy sibling scopes succeed.
2. Assert successful scopes prune, failed scopes do not prune, and the retry
   operation retains scope, batch ID, source revision, and revalidation.
3. Verify permanent failures leave no retry spin while transient failures are
   retried at the full-scope boundary.
4. Expose/verify attempted, succeeded, transient-failed, permanent-failed,
   prune-skipped, and retry counts without widening the shared metrics port.

**Gate:** Nacos/worker fault-injection tests, race tests, `make test-all`, and
an evidence note with the failure matrix.

## Stage 5 — End-to-end qualification and independent review

1. Run the real Nacos 3 ARM64 + KWOK watch-assisted gate at 1,000 Pods and
   20 services with create/delete/crash/recovery and bounded bursts.
2. Compare complete Instance projections on every tick, including labels and
   Reversion, across K8s Watch, Spotter observation, and Nacos Watch.
3. Verify zero divergence, zero dropped events, drained retry queues, and
   clean teardown; retain raw artifacts and SHA256 hashes.
4. Review the diff against this plan and the audit line by line before marking
   a stage complete.
5. Publish the final PASS/WARN matrix, explicitly leaving source-qualified
   migration and deployment-owned health verification as bounded decisions.

## Rollback

Each stage is independently revertible. The restart-safe ownership guard is
fail-closed and does not alter valid single-source writes. Metadata rejection
is permanent by design. Health-policy changes preserve the deployment-owned
compatibility mode. Batch tests and raw evidence must remain in history even
if a later implementation is rolled back.

## Acceptance matrix

| Finding | Code acceptance | Evidence acceptance |
| --- | --- | --- |
| F1 collision | No overwrite before or after restart; same-address source collision returns typed permanent error | focused restart test plus real two-source fixture or explicit migration decision |
| F2 metadata | Exact Nacos limit enforced before mutation; no truncation | boundary/property tests and permanent-error classification |
| F3 health policy | verified mode fails closed without readback; no hidden HTTP | verifier ordering test and deployment attestation/readback record |
| F4 batch failure | failed scopes preserved; successful scopes prune; retry metadata intact | permanent/transient fault-injection matrix and `make test-all` |

## Execution status

| Stage | Status | Evidence |
| --- | --- | --- |
| Stage 0 baseline and ledger | COMPLETE | `ba4fabb`, pushed |
| Stage 1 restart-safe source ownership | CODE COMPLETE | `0ca4c83`; restart regression, focused/race/full tests passed |
| Stage 2 metadata envelope qualification | CODE COMPLETE | `a2ac038`; exact UTF-16 boundary, Unicode, high-entropy, and fuzz tests passed |
| Stage 3 health-policy contract | CODE COMPLETE / DEPLOYMENT VERIFIER OPEN | `ef31724`; explicit startup evidence labels, verified-mode fail-closed tests, and operations contract |
| Stage 4 batch failure closure | CODE COMPLETE | `deda80c`, `0ba6610`; outcome metrics, all-branch permanent classification, mixed-failure recovery tests, race/full gates passed |
| Stage 5 runtime qualification | PASS / BOUNDED DECISIONS OPEN | `nacos3-kwok-p1-closure-runtime-2026-09-30.md`; post-closure 20-minute Nacos 3 ARM64 + KWOK run passed 79/79 exact ticks at 1,000 Pods, and final HEAD smoke passed 7/7 exact ticks with clean teardown |

The remaining open decisions are intentionally bounded: a source-qualified
Nacos wire mapping requires consumer compatibility approval, and a real
deployment must provide the approved verifier if it wants `verified` health
policy rather than the default deployment attestation.
