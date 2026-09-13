# Legacy Global Caller Inventory (2026-09-13)

Repository-local scanning found only compatibility and test callers. It cannot prove whether external repositories still import the historical packages, so the removal gate remains blocked until owners provide migration evidence.

| Caller repository | Owner | Entry point | Migration commit/PR | Target date | Rollback window | Verification command | Status |
|---|---|---|---|---|---|---|---|
| external/unknown | spotter-maintainers | `spotter/config`, `spotter/pkg/log`, `spotter/pkg/notice` public symbols | not provided | not provided | not provided | `rg -n 'spotter/(config|pkg/log|pkg/notice)'` in each consuming repository | BLOCKED: repository-local `rg` cannot prove external migration |

The safe state is therefore a compile-compatible deprecated adapter. Mutable globals remain quarantined behind `internal/infra/legacycompat`; active composition code must use injected ports and value configuration. Removal requires an owner-confirmed caller list, migration commits or PRs, replay verification, and a documented rollback window.
