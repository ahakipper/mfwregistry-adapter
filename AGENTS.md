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

Verification lists commands actually run and their results. Commits are
atomic. Do not rewrite pushed history or force-push without explicit user
approval, an immutable backup ref, and an independent review decision.

## Change workflow

Use test-first development for behavior changes. A task is complete only after
focused tests, race tests for concurrent code, and repository quality gates
pass. Keep historical audit evidence intact and add a current-status addendum
for scope changes.
