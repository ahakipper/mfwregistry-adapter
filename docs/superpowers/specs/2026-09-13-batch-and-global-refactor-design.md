# Spotter Batch and Global Dependency Refactor Design

**Status:** Proposed for implementation review
**Date:** 2026-09-13
**Scope:** Nacos initial/full-sync batching, repository commit governance
(including the requested rewrite of the recent remediation commit messages),
and active-graph removal of package-level logger/config/notice dependencies.

## Goal

Make the initial K8s snapshot and later full reconciliations safe and bounded
for large populations, while making the active Spotter graph deterministic and
TDD-friendly by removing runtime dependence on mutable package globals.

## Decisions

### Nacos batch semantics

The word “batch” has two meanings and the implementation must name them
separately:

1. **Nacos protocol batch:** one Nacos request carrying multiple instances.
   The pinned official Go SDK rejects `Ephemeral=false` in
   `BatchRegisterInstance`; the official server/client contract supports this
   operation only for temporary instances. Spotter must not change its
   persistent-instance contract merely to use that API.
2. **Spotter persistent batch execution:** one application-scoped work unit
   containing at most 100 persistent instances. The worker schedules the work
   unit with bounded concurrency, and each persistent item is still written by
   the official SDK `RegisterInstance` or `DeregisterInstance` call. A future
   Nacos version with a verified persistent protocol batch can replace the
   executor behind the same boundary.

The batch key is:

```text
namespace + group + appCode/serviceName + provider/cluster + operation
```

Register/update items and deregister items never share a batch. The batch size
is hard-capped at 100. Batches for one application scope run in order; batches
for independent application scopes may run concurrently under a global request
limit of 8. The implementation preserves source order for deterministic error
selection and keeps `SyncAll`, `Scope`, `BatchID`, generation and retry data.

`PushAll` applies registration/update batches first and runs prune only after
the registration phase has no failures. A partial batch failure returns an
aggregated error and is retried without converting the full operation into
unrelated single-instance queue entries. Incremental watch events continue to
use the existing per-instance path.

### Global dependency removal

The active composition path owns immutable `internal/infra/config.Config`,
`ports.Logger`, `ports.Notifier`, `ports.MetricsRecorder` and clock seams.
Active production code must not read or write:

- `config` package variables;
- `pkg/log.Logger`;
- `pkg/notice.Noticer`.

The deprecated public packages remain only as an explicit compatibility
boundary until external callers are migrated. `internal/infra/legacycompat`
is the sole bridge during the migration. The active server startup must not
call `assignLegacyGlobals`, `log.LoggerInit` or `notice.InitNoticeClient`.
Compatibility behavior is protected by contract tests for the legacy logger,
notice initializer, and copied configuration snapshots; those tests remain
green until the final external-caller migration decision.

The DDD migration is staged so every step is independently testable:

1. repository-wide static import/write gates and two-runtime isolation tests;
2. command/composition wiring without global assignment;
3. provider/conversion/elector compatibility wrappers moved behind the bridge;
4. an explicit external-caller migration inventory and compatibility replay
   before deleting the deprecated shim.

### Commit governance

Every new commit uses a detailed English subject and body with the sections
`Problem`, `Changes`, `Verification`, `Compatibility / Rollback`, and
`Documentation`. Commits are atomic and record actual commands and results.
The requested history cleanup is now confirmed and frozen to the remediation
range `e708630^..e9ea7d2` (175 commits). Its parent is the preserved
pre-remediation base; original 2020 project history remains untouched. Commits
created after `e9ea7d2` are not part of this rewrite.
Before rewriting, create backup refs for both the frozen input tip and the
current branch tip, generate a per-commit message map, run a message/schema
checker, compare commit trees and patch IDs, replay post-range commits with
merge preservation, obtain independent review, and push only with
`--force-with-lease`. A different range requires an explicit scope decision
before any ref mutation.

## Non-goals

- Switching Spotter instances from persistent to temporary semantics.
- Adding raw HTTP calls to production Nacos code.
- Building or validating a multi-node Nacos deployment in this repository.
- Opportunistically batching incremental watch events before the full-sync
  contract is proven.
- Removing public compatibility packages before external caller evidence exists.

## Acceptance criteria

- A 10,001-instance snapshot is partitioned by application scope into batches
  of at most 100, with no mixed cluster or operation batch.
- Per-application order is deterministic; global Nacos request concurrency is
  bounded at 8; partial failures are observable and retryable.
- Initial K8s `flushInstances` and periodic `SyncAll` both exercise the same
  batch executor and prune only after successful registration batches.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, the tagged Nacos
  tests, and the relevant E2E suites pass.
- Active-graph tests can build two independent runtimes in one process without
  global state leakage, and a static gate reports no new global imports or
  writes.
- Every implementation commit satisfies the repository `AGENTS.md` policy.
