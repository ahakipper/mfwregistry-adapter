# Service Discovery P1 Remediation Plan — 2026-09-30

## Objective

Close the four P1 findings from
[the chain audit](../../service-discovery-chain-audit-2026-09-30.md) without
silently changing the existing Nacos wire contract:

1. prevent source-cluster collisions in the Nacos composite identity;
2. make complete canonical metadata size-safe;
3. make the Nacos health-check policy an explicit, auditable startup
   precondition;
4. preserve safe full-batch retry/prune semantics when one item fails.

## Execution rules

- Work from `refactor/all`; preserve unrelated changes and historical evidence.
- Test first: add a failing negative/regression test before each behavior fix.
- Every stage ends with focused tests, `go vet ./...`, appropriate race tests,
  a detailed English commit, and `git push origin refactor/all`.
- No raw HTTP is added to the product naming path. Admin compatibility remains
  an explicitly named control-plane/test seam unless an approved SDK facade is
  supplied.
- Do not change the existing `clusterName=k8s|ecs` wire identity silently.
  Any wire-breaking identity migration requires a separate compatibility gate.

## Stage 0 — Baseline and design ledger

**Deliverables**

- this plan committed and pushed;
- current branch/remote/worktree recorded;
- a short decision ledger for source-cluster identity, metadata rejection,
  health-policy startup behavior, and partial-batch retry semantics.

**Gate**

```text
git status --short --branch       clean
go vet ./...                     PASS
go test ./... -count=1           PASS
```

## Stage 1 — F1: identity collision protection (compatibility-safe)

### 1.1 Test-first proof

Add tests for:

- two distinct source clusters with the same app/IP/port/provider;
- duplicate Nacos composite IDs in one full snapshot;
- the same collision arriving through incremental Push;
- a non-colliding multi-cluster snapshot remaining writable and prune-safe.

### 1.2 Implementation

- Introduce an explicit Nacos wire-identity collision error containing both
  source identities and the composite ID.
- Validate a full application batch before any mutation; reject the colliding
  scope and never run its prune.
- Maintain a sink-local identity ledger for incremental writes so a later
  source cannot silently overwrite a different source identity.
- Export metrics/log fields sufficient to identify service, provider,
  source-cluster, source-key, and composite ID.

### 1.3 Compatibility decision

The default remains the existing `k8s`/`ecs` cluster names. After the guard is
merged, evaluate one of these explicit deployment choices:

- prove globally unique source IP/port tuples and keep the wire contract; or
- introduce a versioned source-qualified Nacos cluster mapping with migration
  and client compatibility evidence.

No source-qualified wire change is included in the guard commit.

**Gate:** focused Nacos/worker tests, `-race`, full `go test`, and a detailed
commit/push.

## Stage 2 — F2: canonical metadata size policy

### 2.1 Test-first proof

Add deterministic and fuzz/property coverage for high-entropy labels, long
images, many ports, Unicode values, and large source fields. Assert the
serialized Nacos metadata envelope stays within the chosen limit or returns a
typed error before a network mutation.

### 2.2 Implementation

- Define one documented metadata byte limit and measure the serialized map that
  the Nacos request actually carries.
- Add `MetadataTooLargeError` with permanent classification and structured
  size/limit fields.
- Reject before health-admin or register calls; never truncate canonical data.
- Ensure the application batch records the exact offending identity and does
  not treat a rejected item as successfully applied.

**Gate:** metadata unit/fuzz tests, Nacos batch tests, permanent-error tests,
`go test -race ./pkg/nacos ./pkg/worker`, full quality gates, commit/push.

## Stage 3 — F3: explicit Nacos health-policy precondition

### 3.1 Test-first proof

Add startup tests for:

- deployment-owned policy with an explicit preflight result `verified=false`;
- verified `healthCheckEnabled=false` allowing startup;
- transient preflight failure being retryable;
- admin-managed policy without an approved facade failing closed;
- no business register occurring before a failed policy preflight.

### 3.2 Implementation

- Add a typed health-policy preflight port/result to the composition boundary.
- Make startup log and metrics distinguish `verified`, `deployment-owned`
  (operator attestation), and `unverified` states.
- Add an explicit CLI/config policy rather than an implicit zero-value choice.
- Keep the default data path on the official SDK; do not add hidden HTTP.
- Update the deployment runbook with the Nacos 3 global switch readback command
  and evidence fields. If no approved verifier is supplied, retain a clearly
  documented deployment-owned attestation rather than claiming product proof.

**Gate:** composition/server/Nacos tests, no-side-effect failure test, race
tests, docs update, commit/push.

## Stage 4 — F4: full-batch partial failure and retry closure

### 4.1 Test-first proof

Add tests for:

- one permanent item error plus successful siblings;
- one transient item error plus successful siblings;
- prune failure after all registers succeed;
- retry preserving scope, batch ID, item failure identity, and revalidation;
- no destructive prune for an application whose desired batch was not fully
  accepted;
- independent application scopes continuing when another scope fails.

### 4.2 Implementation

- Return a structured per-scope/per-item batch result instead of only the first
  error.
- Keep successful scopes eligible for prune; suppress prune only for scopes
  with incomplete desired-state application.
- Queue retry operations at the smallest safe unit: failed item or failed
  application scope, while retaining full-snapshot authority for prune replay.
- Make permanent failures observable and non-spinning, but do not let dropping
  a permanent item silently erase the remaining full-operation context.
- Add batch metrics for attempted, succeeded, transient-failed, permanent-
  failed, prune-skipped, and retried scopes.

**Gate:** Nacos/worker focused tests, full race suite, `make test-all`, commit/
push.

## Stage 5 — Runtime qualification and documentation closure

After all code stages:

1. run a guarded Nacos 3 ARM64 smoke with two logical source clusters,
   collision and non-collision cases, oversized metadata, health-policy
   preflight, and partial-batch failures;
2. rerun the KWOK Observe gate with the post-remediation binary; retain
   complete Instance equality, labels, Reversion, mutation correlation,
   zero-drop, retry-drain, and cleanup evidence;
3. update the chain audit from `P1 FOLLOW-UP` to per-finding PASS/WARN with
   raw evidence links and hashes;
4. update `docs/README.md`, operations guidance, and the remediation ledger;
5. publish a final release matrix separating code PASS, local runtime PASS,
   deployment-owned WARN, and excluded scope.

## Rollback

Each stage is independently revertible. The collision guard and metadata guard
are fail-closed and can be rolled back without changing valid registrations.
Health-policy changes must preserve the current explicit deployment-owned mode
as a documented compatibility option. Batch retry changes must never be
rolled back by deleting tests or historical evidence.

## Current status

| Stage | Status | Commit / evidence |
| --- | --- | --- |
| Stage 0 baseline and ledger | **COMPLETE** | `27e6233`, pushed. |
| Stage 1 collision protection | **CODE COMPLETE / MIGRATION DECISION OPEN** | `d1320d4`, focused unit/black-box/race tests, pushed. Existing `k8s` wire cluster is unchanged; source-qualified migration still needs consumer approval or a global address-uniqueness proof. |
| Stage 2 metadata capacity | **CODE COMPLETE** | `429fe9f`, Nacos 1024 UTF-16 code-unit guard and high-entropy negative tests, pushed. |
| Stage 3 health-policy preflight | **CODE COMPLETE / VERIFIER DEPLOYMENT OPEN** | `20ec2d2`, explicit `verified` policy and injected verifier gate, pushed. Default deployment-owned mode remains an operator attestation until a verifier is supplied. |
| Stage 4 partial batch/prune | **CODE COMPLETE** | `6a0035a`, failed-scope isolation and structured retry error, pushed. Full repository requalification remains required. |
| Stage 5 runtime qualification/docs | **NOT VERIFIED / ENVIRONMENT MISSING** | Current host is arm64 with Docker/kwokctl binaries but no local Nacos image or active KWOK cluster. `make test-all` passes; real Nacos 3 + KWOK evidence must run when the target fixture is provisioned. |
