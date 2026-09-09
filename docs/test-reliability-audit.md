# Test Reliability Audit — Findings

**Status:** IN PROGRESS (auditors A, B, C still running; D complete)
**Date:** 2026-09-10
**Method binding:** every finding is confirmed by full code reading; verification claims were executed (test runs / live probes / scratch experiments in /tmp). Severity: P0 = real production risk + test blind spot, P1 = test cannot catch a plausible regression, P2 = weak/misleading/pinning gap.

Baseline at audit start: 33 test files, 308 test functions, 68.2% total statement coverage, full matrix green, `-race` green.

---

## Auditor D — composition wiring, config, e2e realism, election, soak

Suites executed: `go test ./internal/... ./pkg/etcd/... ./pkg/distribute/... -count=1` (ok, 14 packages) and `-race` (ok); `go test -tags=e2e ./tests/e2e/... -count=1` (ok, 4 tests) and `-race` (ok). E2E run deemed safe: all e2e deps bind dynamic/ephemeral loopback ports only (consulmock server.go:47, discoverymock server.go:40/55 bufconn, nacosmock server.go:128, etcdmock server.go:211 `127.0.0.1:0`); zero hits for forbidden ports. Soak NOT run (constraint); evidence read from results/20260909-0000-local.md.

| ID | Sev | Summary | Location (test / production) |
|---|---|---|---|
| AUDIT-D-1 | High | `NewServerFromDeps` has ZERO tests — the E3 nil-notifier regression class (EMERGENCY paging silently severed) is still unpinned; every server test builds `&Server{}` literals with its own notifier, bypassing the wiring that threads `rt.Notifier` into the elector | internal/server.go:79-121, cmd/adapter.go:84 / root_test.go:57-59, elector_e2e_test.go:211-219 |
| AUDIT-D-2 | High | e2e fanout pipeline hand-feeds the SyncAll event (`w.Handle` manual, fanout_pipeline_test.go:155) instead of the production tick path; the justifying comment is factually stale (consul.go:61 DOES assign interval since b3578b6) — the emitSyncAll→worker→FanoutSink.PushAll→prune chain is never e2e-tested | tests/e2e/fanout_pipeline_test.go:114-126,123,155 / consul.go:56-62,293-297,318; k8s.go:456 |
| AUDIT-D-3 | High | nacosmock does not model real nacos instance/list view filtering (returns all instances regardless of Enabled) → every prune test is structurally unable to catch F8; no test seeds an `Enabled: false` stale instance and asserts its deletion | nacosmock/server.go:302-310 / nacos.go:107-127; sink_test.go:309-367, fanout_pipeline_test.go:147-156 |
| AUDIT-D-4 | Med-High | soak's read side uses the same blind endpoint as the production prune (`instance/list`, assert.go:126) — a disabled zombie is invisible to BOTH the prune and the soak's divergence detection; soak would print "final convergence: PASS" while the catalog holds drift | tests/soak/assert.go:126,171-181 / nacos.go:108 |
| AUDIT-D-5 | Med-High | soak never observes the retry queue (metrics port scraped for liveness only, child.go:164; `sync_error_gauge` never read — grep zero hits) — F7's signature (non-draining queue, 9624 futile retries) was invisible; the 1h run held 130 pending entries 12 min and only manual post-hoc log parsing knows | tests/soak/child.go:157-171, soak_test.go:403-431 / unsynced_service.go:246-251, metrics.go:79-81 |
| AUDIT-D-6 | Med | cmd/adapter.go flag→config mapping + legacy-globals assignment have zero tests (no cmd/*_test.go; grep: only adapter.go references adapterFlags/applyLegacyGlobals); all 9 demo-used flags are mapped correctly TODAY, but a swapped field name passes the whole unit matrix and surfaces only in the manual 1h soak | cmd/adapter.go:144-186,197-237 / — |
| AUDIT-D-7 | Med | the permanent-4xx chain (nacosmock 400 → real Sink → real worker queue → drop) is never assembled in one test; executed experiment proves: `errors.As(FanoutError, &Permanent-iface)` → **false** (FanoutError implements no Unwrap) — benign today (drop decided on the PushTo path only) but any refactor moving classification to the Handle-time error resurrects F7 with no failing test; worker blackbox uses a test-local fake error, never wrapped/never fanned out; `SetStatus(400)` appears only in client tests | unsynced_service.go:198-203,217-218, nacos.go:230 / blackbox_test.go:296-311, client_test.go:141-219 |
| AUDIT-D-8 | Low-Med | domain rules_test does not pin the offline+empty-IP asymmetry that was F7's enabling hole (filter rejects online+empty-IP, accepts offline+empty-IP by design — owned downstream by the two guards); "harmonizing" the filter would break the three-layer defense with a green suite | rules.go:90-92 / rules_test.go:202-209 |
| AUDIT-D-9 | Low-Med | pkg/etcd TLS branch untested (dropping `cfg.TLS` assignment compiles+passes); endpoint-list rotation unasserted (prod presets carry 3 endpoints; all tests use exactly one); session-expiry covered indirectly via e2e failover | etcd.go:20-31,36-44 / etcd_test.go (2 tests) |
| AUDIT-D-10 | Low | campaign-failure paging has no unit-tier behavioral pin (only structural injection test); the real-RPC proof lives behind the manual e2e tag | election_test.go:449-471 / elector_e2e_test.go:166-278 |
| AUDIT-D-11 | Low | soak never asserts Atlas stand-in payload correctness (call counts only) — a fanout corrupting Atlas payloads while pushing Nacos correctly would pass | soak_test.go:503-511,647-653 |
| AUDIT-D-12 | Low | nacos readiness gate accepts 200 from an endpoint that answers 200 while Raft-leaderless (documented ARM pathology: every write 500s) — accepted behavior, unpinned contract; the soak had to invent a write probe the product's gate lacks | client.go:136-150, server.go:304-307 / scenarios.go:205-225 |

### Auditor D — proposed designs for the fix phase (implementable as written)

- **E2E-1** `TestE2EPermanent4xxDropsFromRetryQueue`: nacosmock + real Sink + real FanoutSink + real worker; register healthy, `SetStatus(400)`, Handle offline-sync with valid IP → assert queue depth returns to 0 AND DELETE request count ≤ 2 (the incident's anti-signature). Assembles D-7's chain.
- **E2E-2** `TestE2EServerWiresEmptyIPShellSafely`: through the server_test harness + nacosmock 400 → Handle offline instance with empty Ip → zero DELETEs reach nacosmock (proves the nacos.go skip guard through real wiring).
- **E2E-3** `TestE2EConsulFanoutPipelineRealTick`: change fanout_pipeline_test.go:123 interval arg `0`→`1`, delete the stale comment (114-126) and the manual Handle (155); let the 15s prune bound cover the real tick chain (fixes D-2 with a one-line upgrade).
- **E2E-4** `TestE2ELeaderSwitchoverMidPush`: two electors on etcdmock; leader A Stop → B promotes; assert nacos convergence across the switch + no duplicate composite ids.
- **E2E-5** `TestE2EPruneRemovesDisabledStaleInstance` (post F8-fix): nacosmock `WithInstanceListHidingDisabled()`; seed ENABLED + DISABLED ghosts → SyncAll → both pruned via catalog view. Red test against current code.
- **Soak additions**: (1) `metricsObserver` scraping `sync_error_gauge` every assert tick + hard bound "queue drains to 0 within fullPushBound after churn quiescence" (D-5); (2) failing-readinessProbe deployment 0→2→0 scenario asserting zero `answered status 4` in app.log (F7 signature); (3) finalConvergence additionally compares catalog/instances (D-4/F8); (4) Atlas payload equality vs model (D-11).
- **Wiring test for D-1**: internal package, etcdmock + fakes.FakeNotifier, revoke lease → `ElectWait` → assert the notifier received "Candidate server node election failed" — pins Build→NewServerFromDeps→NewElectorWithDeps→notify for the first time.

### Auditor D — confirmed reliable (do not churn in the fix phase)

infra/config preset equality matrix; composition root_test override/ownership semantics; server_test lifecycle group (dial cancellation, generation serialization, leader-state dedup, metrics stop, nacos sink registration through the real Handle seam, readiness unreachable); worker blackbox retry-queue group (real 5s ticker, keep-on-failure, reversion-wins, permanent drop, mid-cycle re-add from inside Push); election 19 tests + e2e elector tier (real etcd v3.6.13, measured ~3s failover, lease-revocation→EMERGENCY page); etcdmock (real embedded server, same etcd v3.6.13 as prod); nacos client_test Permanent boundaries; infra logging/metrics (real files, real HTTP exposition).

**Auditor D key takeaway:** the pattern uniting D-1/D-2/D-3/D-5/D-7 is the F7 escape pattern — each LAYER is well tested, the SEAMS are not. Cheapest high-value fixes: E2E-1, E2E-3, soak queue-depth scrape.

---

## Auditor A — worker domain

(pending)

## Auditor B — nacos + mocks domain

(pending)

## Auditor C — providers domain

(pending)

## Deduplicated fix-phase backlog

(to be assembled after all auditors report; seeded with F8: prune blind spot — list via `v1/ns/catalog/instances`, live-verified wire behavior in the plan doc)
