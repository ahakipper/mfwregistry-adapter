# etcd Upgrade & Elector Review

Date: 2026-09-03
Scope: (1) feasibility of upgrading `github.com/coreos/etcd v3.3.13+incompatible`
to the current stable line `go.etcd.io/etcd/client/v3 v3.6.13`; (2) the etcd
elector implementation (`pkg/worker/elector.go`, `pkg/distribute/election`,
`pkg/etcd`) — mechanism, robustness, testability, coverage and e2e status.

Review process: a lead-agent spike (executed first), an agent-1 comprehensive
review (including a full migration dry-run on a scratch copy and empirical
race reproduction), and an agent-2 adversarial re-verification that
independently re-ran the migration and the race reproduction. All empirical
claims below were reproduced by at least two of the three; discrepancies
between the reviewers are recorded verbatim.

---

## Part 1 — Upgrade to etcd v3.6.13: FEASIBLE-WITH-CONSTRAINTS

### 1.1 Executive summary

The mechanical code change is small and fully verified: import rewrites in
five files plus one field rename. The migration dry-run (performed twice,
independently, on scratch copies — the repository itself was never modified)
resulted in a green build, the full test matrix, the race detector and the
`-tags=e2e` compilation all passing. The two review agents independently
ENDORSE the migration and the ordered plan below.

The constraints that make this "with-constraints" rather than "trivial":

1. **A hidden compile blocker** in the dependency tree (klog v2.9.0 vs
   logr v1.4.x — see 1.4), empirically reproduced and empirically fixed.
2. **The `go 1.15` directive cannot survive** `go mod tidy`; it is rewritten
   to `go 1.25.0` (empirically reproduced). The Docker build image must
   therefore provide Go >= 1.25 (see 1.5, open items).
3. **The production etcd server version is unknown** from the repository;
   a v3.6 client is officially supported against 3.5/3.6 servers. If prod
   runs an older server, the same migration applies against
   `client/v3@v3.5.x` (identical code changes, smaller dependency jump).

### 1.2 Motivation

The current dependency is a 2019, unmaintained `+incompatible` pseudo-module.
Concretely, it is what makes offline embedded-etcd testing impossible: the
v3.3 request-statistics interceptor calls
`etcdserverpb.NewLoggablePutRequest(req).String()` on every Put, and that
fake-proto struct panics under modern protobuf reflection
(`invalid Go type int for field ...loggablePutRequest.value_size`), which
makes any embedded v3.3 server unusable for tests and pins the repository
to grpc v1.27.1. The lead agent's spike against v3.6.13 (and both agents'
dry-runs) proved the panic is gone in the new module layout and that
`concurrency` sessions, campaign, leader query and resign all work against
an embedded v3.6.13 server.

### 1.3 API inventory (old → new), all five files

| File | Old (coreos/etcd v3.3.13) | New (v3.6.13) | Status |
|---|---|---|---|
| pkg/etcd/etcd.go | `clientv3` | `go.etcd.io/etcd/client/v3` | drop-in |
| pkg/etcd/etcd.go | `pkg/transport` | `go.etcd.io/etcd/client/pkg/v3/transport` | drop-in |
| pkg/etcd/etcd.go:26 | `TLSInfo{CAFile: ...}` | `TLSInfo{TrustedCAFile: ...}` | **rename — the only non-import production code change**; both versions load the CA from the same code path |
| pkg/worker/elector.go | `clientv3.Client` | identical type | drop-in |
| pkg/worker/elector_test.go | `clientv3.LeaseID` | identical type | drop-in |
| pkg/distribute/election/election.go | `clientv3`, `clientv3/concurrency` | new paths, same package names | drop-in; `Grant(ctx,0)`, `NewSession/WithLease/WithTTL`, `NewElection`, `Campaign/Leader/Resign`, `ErrElectionNoLeader` all present |
| pkg/distribute/discovery/discovery.go (dead code) | `clientv3`, `mvcc/mvccpb` | new paths; `mvccpb` → `go.etcd.io/etcd/api/v3/mvccpb` | drop-in if not deleted first |

No production code uses `server/v3`; it would be added only as a test-only
dependency for the embedded-etcd e2e (build-tag guarded, never linked into
the binary).

### 1.4 Dependency impact

| Dependency | Before | After | Note |
|---|---|---|---|
| github.com/coreos/etcd | v3.3.13+incompatible | removed | |
| go.etcd.io/etcd/{api/v3, client/pkg/v3, client/v3} | — | v3.6.13 | new |
| google.golang.org/grpc | v1.27.1 | v1.79.3+ (1.80.0 via MVS) | deprecated-but-present APIs cover all our usage: `CustomCodec`, `ForceCodec`, `WithInsecure`, `WithBlock`, `DialContext`, legacy `Codec` (verified in the v1.79.3 source; discoverycenter + discoverymock compile and pass) |
| k8s.io/klog/v2 | v2.9.0 (indirect) | **v2.100.1 required** | the hidden blocker — see below |
| go-logr/logr | — | v1.4.3 (indirect) | pulled by grpc >= 1.79 |
| go.uber.org/zap | v1.10.0 | v1.27.x | our API surface unchanged; logging tests pass |
| prometheus/client_golang | v1.7.1 | v1.21–1.23 | our API surface unchanged; metrics tests pass |
| google.golang.org/protobuf | 1.26-era | v1.36.x | |
| github.com/google/btree, golang.org/x/sync | old | bumped | providers cache & errgroup unaffected; suites pass |
| k0kubun/pp (+colorstring) | direct | removed by tidy | was only used by dead coreos/etcd paths |

**The klog blocker (empirically reproduced by both agents):** grpc >= 1.79
requires `go-logr/logr v1.4.3`, which removed the package-level
`logr.WithCallDepth` and made `Logger` a struct; klog v2.9.0 (pinned
transitively by k8s.io/client-go v0.22.4) still calls `logr.WithCallDepth`
and compares `logr.Logger` to nil, so it stops compiling once logr v1.4.x
enters the build graph. Fix: bump klog to v2.100.1 (the combination k8s
itself ships from v1.24 on) — verified green. k8s.io v0.22.4 modules have no
grpc requirement of their own, so nothing else in the k8s stack objects.

**Go directive:** `go.etcd.io/etcd/client/v3@v3.6.13` declares
`go 1.25.0`. Under a `go 1.15` main-module directive, `go mod tidy`
(and any `-mod=mod` build, which is the default for pre-1.16 modules)
rewrites the directive to `go 1.25.0`. Correction from re-verification:
this is a `go mod tidy` rule (raise the main module's go line to the graph
maximum), not MVS proper — the outcome is identical either way. The
migration should set `go 1.25.0` deliberately; the local toolchain
(go 1.26.4) satisfies it and no source-language change is forced.

### 1.5 Runtime semantics: 3.3 → 3.6

Source-diffed (both agents): the `concurrency` election algorithm, key
layout (`prefix + lease-hex`), sentinels (`ErrElectionNoLeader`,
`ErrElectionNotLeader`), session contract (`defaultSessionTTL = 60` in
both) and the server-side TTL-0 clamping are unchanged. Noteworthy deltas,
none breaking for this codebase:

- `SessionOption` gains a `*zap.Logger` parameter (transparent to callers
  that use exported options).
- The session keepalive goroutine now also cancels its context on exit.
- v3.6 wraps lease/KV errors so that caller-context cancellation surfaces
  as `context.DeadlineExceeded`/`context.Canceled` — this makes the
  string-based check in election.go (see Part 2, F7) *more* reliable, not
  less; the robust form is `errors.Is`.
- The v3.6 client defaults its internal logger to a no-op; passing
  `Logger` in `clientv3.Config` is recommended for observability.

### 1.6 Ordered migration plan (endorsed by both agents)

1. (Optional, shrinks the surface) delete dead code first:
   `pkg/distribute/discovery` (zero importers), `NewEtcdClient`
   (zero callers), `Candidate.Resign/Tag/LeaseID` (zero callers).
2. Rewrite imports in the five live files (table 1.3).
3. Rename `CAFile` → `TrustedCAFile` (etcd.go:26).
4. go.mod surgery, in order: drop `github.com/coreos/etcd`; add the three
   `go.etcd.io/etcd/*@v3.6.13` requirements; `go mod tidy` (accept the
   `go 1.25.0` rewrite); `go mod edit -require=k8s.io/klog/v2@v2.100.1`;
   `go mod tidy` again.
5. Hardening in the same MR (files already touched):
   `errors.Is(err, context.DeadlineExceeded)` at election.go:170;
   `client.Close()` on the MemberList-failure path (etcd.go:39-41);
   keep the discarded cancel funcs (etcd.go:38, election.go:114/166).
6. Add the embedded-etcd election e2e (see 2.5) to lock behavior that
   currently has 0% coverage; verify TLS against a real cluster.
7. Before merge: confirm the Docker `golang:latest` image ships Go >= 1.25
   and confirm the production etcd server version.
8. Rollout: campaign keys are shared (`/paas-…`), and the campaign
   algorithm is identical across versions, so a v3.6 client can coexist
   with v3.3 clients during a canary. Watch notice volume during the
   window (Part 2, F7).

### 1.7 Open items that no local test can settle

1. Production etcd **server** version (client-N ↔ server-N/N-1 support).
2. Real TLS handshake with the cluster's certificates (only compiled).
3. The private Docker builder image's Go version.
4. Long-run behavior: reconnection after etcd restarts, notice volume.
5. Embedded server/v3 stability inside CI (spike covered boot, not
   CI flakiness).

---

## Part 2 — Elector implementation review

### 2.1 Mechanism (as implemented)

`pkg/worker.NewElectorWithDeps` builds an etcd client (via
`pkg/etcd.NewClientWithEndpoints`, which dials lazily and probes with a
5s MemberList) and wraps `election.NewCandidate`. `ElectWait` loops:
`Campaign` (10s timeout) → spawn `syncStoppedState` → `candidate.Wait()`,
which polls `Leader()` every 2s and, when leadership is lost, closes the
session and rebuilds it via `NewElectionSession`, then campaigns again.
Every 2s tick fires every registered callback via `go call(isLeader)`;
the elector's callback forwards to the 2048-buffered leadership channel
unless `stopped` is set. `Stop` only sets the flag and logs.

### 2.2 Robustness findings

Severity-ordered. Items F1–F4 were independently reproduced (race detector
or goroutine/stack evidence) by agent-2 in addition to agent-1's report.

| # | Severity | Location | Finding |
|---|---|---|---|
| F1 | HIGH | elector.go:86 vs :96/:114 | **Data race on `ElectWorker.stopped`**: read inside the notify callback (dispatched as `go call(isLeader)` from election.go:150), written unsynchronized by `Stop` and by `syncStoppedState`. Reproduced under `-race` (5 warnings, exact traces). Any race-enabled CI gate fails today. |
| F2 | HIGH | elector.go:110-117, spawned at :76 | **`syncStoppedState` is a post-cancel hot spin and leaks goroutines**: single-case select on `ctx.Done()`; once canceled it never exits (100% CPU; goroutine observed `[runnable]`). Spawned once per ElectWait iteration; after ctx cancel `Wait` returns immediately, so ElectWait itself becomes a tight re-campaign loop. Reproduced (5 iterations → 6 leaked goroutines). |
| F3 | HIGH | election.go:111-132 | **`NewElectionSession` fails silently**: Grant/NewSession errors return with no log, notice or error, leaving `session`/`election` pointing at the just-closed session; subsequent Campaign runs against the stale session. This is the loss-of-leadership recovery path. Nuance (from re-verification): Wait retries every 2s while not leader, so transient outages do eventually recover — the correct characterization is "zero observability + stale pointers in the interim", not permanent unrecoverability. |
| F4 | MED-HIGH | etcd.go:34-41 | **Client leak on failed probe**: `clientv3.New` succeeds (lazy); MemberList failure returns without `client.Close()` — leaks the connection and its goroutines per failed construction; a crash-looping pod accumulates them. The existing fail-fast test exercises this leak on every run. |
| F5 | MEDIUM | election.go:141-151; elector.go:88 | **Callback dispatch hazards**: fires on every 2s tick regardless of change (~30 events/min/channel forever); bare `go call(...)` with no panic isolation; consecutive ticks are separate goroutines so ordering is scheduler-dependent once contended (a stale true after a false would flip the server's provider lifecycle); the channel send blocks forever if the consumer stalls (2048 buffer ≈ 68 min at 1 msg/2s). |
| F6 | MEDIUM | elector.go:95-98, :30 | **`Stop` is advisory and the etcd client is never closed anywhere**: Stop does not cancel the context or stop polling; `etcdclient` leaks on every shutdown; only the owner canceling the ctx actually stops the candidate — which then triggers F2. |
| F7 | MEDIUM | election.go:170-173 | **Campaign errors page at EMERGENCY level**; transient etcd blips become pages. Plus brittle error discrimination (`err.Error() != "context deadline exceeded"`, sentinel comparisons without `errors.Is`) and a dead `if err == context.Canceled {}` branch. |
| F8 | MEDIUM | election.go:141, :17-18 | **Hardcoded 2s poll, no backoff/jitter/circuit breaker**; persistent errors log every 2s. `CampainTimeout` (typo) and `LeaderChangePeriod` are file constants. |
| F9 | MEDIUM | election.go:80/138/144/172/176/194 | **Global coupling in the adapter**: `config.LockCampaignKey` fallback, `log.Logger` (nil until `LoggerInit` — nil deref panics), `notice.Notice`. The production path passes the campaign key explicitly; the rest is ambient dependency the DDD refactor exists to remove. |
| F10 | LOW | election.go:60/:96/:122 | **`LeaseID()` always returns 0** — the field is never assigned, locals shadow it. Zero callers (dead). |
| F11 | LOW | etcd.go:38; election.go:114/166 | Discarded context cancel funcs (`ctx, _ :=`) — vet `lostcancel`, timers run to deadline. |
| F12 | new (re-verification) | election.go:164-174 | **`Campaign` holds `c.Lock()` across blocking I/O** (up to 10s campaign + the notice network call) — latent stall for any future concurrent use of the candidate. |
| F13 | new (re-verification) | election.go:199-203 vs :147 | **Second latent race on `callBackFuncs`**: append vs range; currently avoided only because registration precedes Wait. Same class as F1; bundle the fix. |
| F14 | checked, not a defect | election.go:187 | `resp.Kvs[0]` without a length check is safe — `Leader` returns `ErrElectionNoLeader` for empty results in both etcd versions. |

Dead code noted: `pkg/distribute/discovery` (zero importers; sole consumer
of tools/cache and tools/net), `NewEtcdClient`, `Candidate.Resign/Tag/LeaseID`.

### 2.3 Testability / TDD-readiness

What exists: the `Candidate` abstraction is the right seam, and
`NewElectorWithDeps` parameterizes endpoints/TLS/campaignKey/logger —
but it **hardwires the Candidate**; there is no exported way to construct
an `ElectWorker` with a fake candidate. Existing tests must assign three
unexported fields and call the unexported `setLeaderChangeNotifyCall`
(elector_test.go:168-173, 245-250), i.e. white-box-only, and the fake
must replace the production's async dispatch with synchronous calls
precisely because async dispatch makes black-box assertions racy.

Half-present or drifted seams:

- `internal/ports.LeaderElector` declares `ElectWait(changes chan<- bool)`;
  `pkg/worker.Elector` declares `ElectWait()` — **signature drift, so
  `ElectWorker` does not satisfy the port**; no compile-time assertion ties
  them. The purpose-built `FakeLeaderElector` is consequently unused
  outside its own package, and server tests bypass the elector entirely
  (`EnableLeaderElection: false`, server_test.go:353-366).
- `ports.Clock`/`FakeClock` exist; `candidate.Wait` uses raw
  `time.Sleep(2s)`.
- `NewElectorWithDeps` injects a logger for the elector but nothing
  (logger/notifier) for the candidate — the deeper layer is
  global-coupled (F9).

TDD posture: the elector layer is testable only through unexported-field
hacks; the candidate layer is testable only against a live etcd.
Minimal seams that fix this cleanly, in increasing value:
(1) `NewElectorWithCandidate(ctx, candidate, leaderCh, logger)`;
(2) inject a Clock into `candidate.Wait`;
(3) pass logger+notifier into `NewCandidate` instead of globals;
(4) make dispatch injectable or return transitions on a channel
    (also fixes F5's ordering hazard);
(5) unify `worker.Elector` with `ports.LeaderElector` so the existing
    fake becomes usable in server tests.

### 2.4 Coverage (measured)

| Function | Coverage |
|---|---|
| pkg/worker (package) | 60.4% |
| elector.go — ElectWait / setLeaderChangeNotifyCall / Stop | 100% |
| elector.go — NewElectorWithDeps | 33.3% (error path only) |
| elector.go — NewElector / logStop / syncStoppedState | 0% / 75% / 66.7% |
| election.go — **all ten functions** | **0.0%** |
| etcd.go — NewClientWithEndpoints | 50.0% (failure branch only) |
| etcd.go — NewEtcdClient / discovery.go — all | 0% (dead) |

The three existing elector tests cover constructor fail-fast, transition
forwarding, Stop suppression and loop re-entry — everything the *real*
candidate does (poll loop, re-election cycle, session recovery, campaign
timeouts, notices, TLS) is 0%, because a fake replaces 100% of it and
`pkg/distribute/election` has no test files of its own.

### 2.5 e2e status and proposed design

`tests/e2e` has no etcd/elector path (verified); the only test touching
the real elector stack is the fail-fast constructor test. With the v3.6
upgrade (spike-proven embedded server), a true e2e becomes feasible:

`tests/e2e/elector_e2e_test.go` (`//go:build e2e`): boot one embedded
etcd → create two `ElectWorker`s on the same campaign key with separate
channels → assert exactly one leader within ~10s (campaign 10s + poll 2s
bound the window) → close the leader's session/cancel its context →
assert failover on the other channel within ~10s (poll 2s + clamped
lease TTL) → assert mutual exclusion throughout. Optional level 2: drive
a real `Server.Run` with election enabled and a stubbed provider
initializer to observe provider stop/start on failover. The 2s poll
bounds observation latency, so per-phase timeouts stay generous.
`server/v3` lands in go.mod only (build-tag guarded; never in the
production binary). A Grant-failure scenario would also have caught F3.

---

## Part 3 — Answers

### Q1: Should we upgrade etcd?

**Yes.** Both review agents independently endorse it. The change is small
and empirically verified end-to-end twice; the semantic risk is low
(algorithm and wire behavior source-diffed as identical); staying on a
2019 unmaintained pseudo-module is what currently blocks grpc movement,
embedded-etcd testing, and every future dependency change (the klog/logr
conflict is a live demonstration of the ossification). Do it in three
independently shippable steps: dependency flip → robustness fixes
(F1-F6) → e2e. Two pre-merge gates no local test can replace: confirm the
production etcd server version and the Docker builder's Go version.

### Q2: Is `pkg/` an acceptable home for the elector under DDD?

**Yes, with one caveat that matters more than the directory.** The import
graph already satisfies the intended layering — nothing outside the
composition-adjacent server imports any elector package, and the DDD
design document itself assigns the elector to "infra adapter behind the
LeaderElector port" as later-phase hygiene. Moving it to
`internal/infra/` would convert a discipline-enforced rule into a
compile-time guarantee, but in this repository nothing is positioned to
violate the rule, so the move is churn, not correctness.

The caveat: **the elector is not actually behind the port it is supposed
to implement.** `ports.LeaderElector` and `worker.Elector` have drifted
apart (different `ElectWait` signatures), nothing enforces conformance,
the purpose-built fake is unusable in server tests, and the deeper
`election.go` layer still reads config/log/notice globals. DDD's real
rule — the application depends on a port, infra implements it — is
violated in the *wiring*, not the path. Recommendation: keep the package
in `pkg/`, spend the effort on (1) making `ElectWorker` satisfy
`ports.LeaderElector` and having the server consume the port, (2)
parameterizing the election.go globals, (3) fixing F1-F6. Fold any
directory move into the future provider/infra consolidation so the churn
happens once. The etcd upgrade is orthogonal to this decision.

---

## Reviewer agreement record

| Topic | agent-1 | agent-2 re-verification |
|---|---|---|
| Upgrade verdict | feasible-with-constraints | ENDORSE (independent full migration re-run; matrix green incl. race + e2e tags) |
| klog blocker | certain without fix | reproduced; root cause logr v1.4 API removal; klog v2.100.1 fix verified |
| go directive | auto-rewrite to 1.25.0 | reproduced; mechanism relabeled: tidy rule, not MVS |
| TLS field rename | CAFile → TrustedCAFile | verified (report's "TrustedCAField" was a typo for `TrustedCAFile`) |
| grpc deprecated APIs | all present at 1.79.3/1.80 | verified; discoverycenter/discoverymock compile and pass |
| F1 stopped race | flagged + reproduced | reproduced (-race, 5 warnings, exact traces) |
| F2 spin/leak | flagged | reproduced (runnable goroutine post-cancel; per-iteration leak) |
| F3 silent recovery failure | "recovery path broken" | verified mechanics; severity nuanced (2s retry loop gives eventual recovery; zero observability) |
| F4 client leak | flagged | verified |
| Coverage numbers | as listed | all exact |
| Port drift + dead code | flagged | all verified |
| New findings | — | F12 (lock across I/O in Campaign), F13 (callbacks race), tidy also drops k0kubun/pp, bumps prometheus/zap/grpc noted |

Both agents: read-only compliance, repository tree verified clean after
their scratch work.
