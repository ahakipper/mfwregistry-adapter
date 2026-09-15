# Legacy Global Caller Inventory (2026-09-13)

Repository-local scanning found only compatibility and test callers. No
external caller inventory was provided. The owner decision recorded on
2026-09-15 is to remove the historical APIs rather than keep an unbounded
quarantine; rollback is available by reverting commit `1c9912a`.

| Caller repository | Owner | Entry point | Migration commit/PR | Target date | Rollback window | Verification command | Status |
|---|---|---|---|---|---|---|---|
| external/unknown | spotter-maintainers | `spotter/config`, `spotter/pkg/log`, `spotter/pkg/notice` public symbols | not provided | not provided | not provided | `rg -n 'spotter/(config|pkg/log|pkg/notice)'` in each consuming repository | BLOCKED: repository-local `rg` cannot prove external migration |

The previous compile-compatible adapter state is historical. The current safe
state is an explicit dependency graph with the legacy packages deleted. Any
external consumer that still imports the removed paths must migrate to
`internal/infra/config.Config` and the `WithDeps` constructors before adopting
this revision; the prior commit is the documented rollback point.
