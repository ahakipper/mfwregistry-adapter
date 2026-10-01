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

## Consul watch latency contract

Consul watch freshness is a correctness requirement. The watch path must

- deliver a detected source change without any seconds-scale blocking limiter;
- preserve index de-duplication and coalesce bursts through a bounded,
  non-blocking signal/debounce path;
- keep load protection separate from the source-to-provider notification
  path, so request-rate control never delays the newest source state;
- record the four latency stages independently: watch return to provider sync
  completion, provider sync to Nacos write acknowledgement, Nacos write
  acknowledgement to Nacos observation, and the complete source mutation to
  Nacos observation path;
- qualify single-instance and 100/1000/10000-instance changes with P80, P90,
  and P99 evidence before claiming a latency improvement.

An implementation that waits a fixed seconds-scale interval after a changed
Consul index is a release-blocking defect, even if it reduces request volume.

Consul HTTP `429 Too Many Requests` is a source-side capacity signal. The
scale harness must classify and preserve it as a Consul source failure; it
must never be hidden by adding delay to the Watch path or reported as a Nacos
data inconsistency without identifying the responding endpoint. The local
ARM64 fixture uses `limits.http_max_conns_per_client = 10000` only to separate
the source connection-capacity failure from Spotter latency. Production
Consul limits remain deployment-owned and must be observed and tuned there.
