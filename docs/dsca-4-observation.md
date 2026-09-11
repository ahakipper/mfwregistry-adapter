# DSCA Track 4 — Sustained Large-Scale Consistency Observation

**Auditor:** DS-4 (sustained large-scale consistency observation — the core-requirement track)
**Repo:** `spotter`, branch `refactor/all` — HEAD `69b0105` when the audit ran; the review-pass re-run (§6.2) executed at HEAD `0ff71f7`, code-identical (the commits between are docs-only)
**Method:** full code reading of every file in `tests/soak/` (soak_test.go, scenarios.go, assert.go, drivers.go, child.go, config.go, metrics.go, atlas.go, summary.go, ioutil.go, signals.go, doc.go, batch4_test.go — 3,944 lines total), the soak stack scripts (`scripts/soak-up.sh`, `soak-down.sh`, `soak-stack.yml`), the Makefile soak tier, the flakiness history (`tests/soak/results/20260908-*.md`, `20260909-0000-local.md` + its root-cause analysis), and the reconcile path the observation judges (`pkg/providers/k8s/*`, `pkg/providers/consul/*`, `pkg/nacos/nacos.go`, `pkg/worker/*`, `pkg/k8srobot/k8srobot.go`). Executed: the soak unit tier (`go test -tags=soak -run TestSoakUnit` — 8/8 PASS), and a **15-minute live rehearsal observation against the running demo stack** (read-only: `kubectl get` + nacos/consul GETs; a scratch script in `/tmp`, zero repo modification). The demo stack was never touched; no reserved port was bound; `make test-soak` was NOT run. Review pass: the rehearsal script's comparison engine was found defective in three ways (P1-3), the engine was fixed, its detections validated offline against synthetic views, and a short corrected-engine re-run executed against the same demo stack (§6.2).

---

## 1. EXECUTION SUMMARY

**The verdict.** The user's core requirement — "每次观察，两边数据都是一致的" (every observation, both sides consistent) — has never been observed under the conditions it names. What exists is a well-built 1-hour, ~10-instance soak whose *assert loop* is one-directional in a subtle but decisive way: it compares the harness's own **expected model** against nacos, and the model is refreshed from the sources only at churn/scenario boundaries — the K8s state is observed per-tick only when the harness itself just touched it. What does not exist: 2h+ duration (the longest run is 1h), 1000s of instances (the churn driver's ceiling is a 1..10 pod oscillation plus a 100-replica scenario spike — and even that collapsed the single-node k3s), a per-tick live-source→nacos bidirectional diff, and a per-tick verdict that survives the whole window. The 1h run that exists ended in an environment cascade (nacos Raft leaderless + k3s API degradation) that masked 6 of 8 scenarios — the observation infrastructure could not distinguish "product diverged" from "environment burned down" until a post-hoc root-cause analysis did it manually.

**What this track executed now.** A 15-minute, 10-second-tick, fully bidirectional observation of the live demo (3 services: 8+2 k8s-sourced instances, 2 consul-sourced): **90/90 ticks CONSISTENT, 0 divergent, 0 observation errors**, with the expected side re-derived from the *live* K8s/consul state on every tick (no cached model — the self-consistency trap structurally removed). The adversarial review then found three defects in the rehearsal's *comparison engine* (duplicate detection structurally disabled, k8s-cluster disabled zombies invisible, source-read failures misclassified as divergence); the engine was fixed to mirror `assert.go`'s semantics, the three detections validated offline (15/15 synthetic checks), and a short corrected-engine re-run executed — **48/48 ticks CONSISTENT, 0 divergent, 0 observation errors** (§6.2). Together the runs prove the observation *methodology* — live-source read, list+catalog max-count union, bidirectional multiset diff with duplicate detection, unhealthy/disabled accounting on both clusters, source-error classification, per-tick verdict — and the demo stack's stability post-`416e62a`. They prove nothing about scale or duration: that is the 2h run this document specifies and gates.

**The deliverable.** §4 is the definitive harness spec for the fix phase: a kwok-backed churn vehicle at 1000s of virtual instances (track 1 owns the vehicle; the driver contract is pinned here), a ≤10s-tick observation loop whose expected side is always the live cluster state, in-flight-tolerant divergence semantics with a hard convergence bound (the honest formalization of "every observation consistent": during churn, entries younger than the bound are *marked in-flight*, everything older is divergence — and in-flight entries must still converge within the bound or the verdict fails), a per-tick consistency ratio, a forensic record per divergence (pod uid, composite instanceId, timestamps, log slice), and one `make` target running the whole thing against **a throwaway stack it owns** (own kwok cluster, throwaway nacos on scratch ports, own spotter child — never the demo stack, §4.1's binding ruling). §5 defines the 2h run gate: what must be true before it starts (track 1's vehicle, track 2's SLOs, track 3's nacos-source reconcile) and its acceptance criteria — divergence AND observation-error bounds.

**Findings:** DS-4-1 (P0) no scale/duration observation exists — the core requirement is unobserved, and the current harness structurally cannot produce it (max churn ~10 instances; 1h ceiling; no kwok); DS-4-2 (P0) the soak's expected model is a *self-consistency trap* — per-tick the assert loop compares the model, not the live K8s state, so a model/source drift would be judged "consistent"; DS-4-3 (P1) the assert loop's worst-age accounting fabricates the bound (every divergence is stamped `IncrementalBound`, divergence age is unknown per-item); DS-4-4 (P1) the standing assertion tolerates *unbounded* divergence mid-window — "checks passed clean" is not the verdict, only the final convergence is, so "every observation consistent" is currently judged only at the end, not per tick; DS-4-5 (P1) scale collapse: k3s real-container vehicle degrades at a 100-replica burst (TLS handshake timeouts, 77s ListAndWatch) — the 2h run must not use it; DS-4-6 (P2) no per-tick record of the *source* observation (kubectl read) — the observation log records only nacos-side counts and divergences, so the forensic chain "what did K8s hold at tick t" is unreconstructable; DS-4-7 (P2) scenario (b)'s known stale-instance behavior (consul empty-catalog guard) is soft-coded as a note, not an observation verdict — the definitive run must observe it as a real per-tick divergence with heal time; DS-4-8 (P2) the harness's expected-set semantics diverge from the converter's for check-less consul entries (passing-filter vs service-check rule) — the rehearsal hit exactly this trap and had to adopt the converter's rule.

---

## 2. EXISTING HARNESS AUDIT

### 2.1 Architecture (what exists, file by file)

The harness is `tests/soak/` behind the `soak` build tag, driven by `make test-soak` (Makefile:87-98: `scripts/soak-up.sh` → build `build/spotter` → `go test -tags=soak -run '^TestSoakLocalStack$'` → `scripts/soak-down.sh`). The containerized stack (nacos `nacos/nacos-server:v2.1.0` on 18848, consul 1.15 on 18500, k3s `v1.28.8-k3s1` on 6443 — scripts/soak-stack.yml) is compose-managed around the test; the test owns every dynamic piece.

| File | Lines | Role |
|---|---|---|
| `soak_test.go` | 914 | `TestSoakLocalStack`: boot etcdmock → Atlas stand-in (discoverymock TCP :15051) → exec the spotter **binary** as a child (child.go) → `runWindow()` → `finalConvergence()` → `writeSummary()` → `report()` (the verdict). Harness struct carries counters (checksRun/checksPassed/divergences/healMax/restarts), the D-5 queue-depth state, the D-11 Atlas divergence slice. |
| `scenarios.go` | 530 | The eight edge scenarios (a)–(h), each an asserted phase with a heal bound and a `scenarioResult`. |
| `assert.go` | 688 | `expectedState` (app-code → live pod names / live consul ids), `nacosObserver` (v1 `instance/list` **union** v1 `catalog/instances` per (service, cluster) — the D-4 batch-4 addition), `checkState`/`checkStateScoped` (bidirectional multiset diff: missing/extra/dups, plus expected-but-absent clusters), the soak log, nacos leaderless classification, the write probe. |
| `drivers.go` | 365 | `k8sDriver` (kubectl apply/scale/delete of busybox deployments labeled `app-code` + `K8S_CLUSTER_TYPE: test`; `livePods`/`livePodsStable` read-back), `consulDriver` (catalog register/deregister over HTTP with the full meta schema). |
| `child.go` | 195 | The spotter binary as a child process with the §8.4 flag set, own process group, restart support, health/election log waiting. |
| `config.go` | 189 | `SOAK_DURATION` (default 1h), paths; `windowSchedule`: churn 20s (8s in smoke), **AssertEvery 10s (5s smoke)**, IncrementalBound 30s, FullPushBound 120s (the fixed `--push-interval 120` the harness passes), BatchBound 300s, ConsulOutage 60s. Bounds never compress below real binary cadences (config.go:118-157). |
| `metrics.go` | 173 | `metricsObserver`: scrapes the child's `/metrics` (port 18090), parses `sync_error_gauge{syncgauge="..."}` plain-text (D-5), `drainViolation`. |
| `atlas.go` | 186 | D-11 Atlas payload parity: reduces the stand-in's call log to the latest record per (appCode, instanceId) and diffs against the same expected model. |
| `summary.go` | 179 | The committed evidence artifact `tests/soak/results/<stamp>-local.md`: scenario table, checks counts, max heal, queue depth/drain verdict, Atlas parity verdict, final convergence label. |
| `batch4_test.go` | 437 | Unit tier pinning the batch-4 additions without the full stack: gauge parsing, drain-bound trip/persist/clear semantics, catalog-union zombie surfacing, union set semantics, Atlas parity detection. **All 8 tests PASS on this HEAD** (executed: `go test -tags=soak -run TestSoakUnit` — 8/8, recorded in §1's Method line). |
| `ioutil.go`, `signals.go`, `doc.go` | 88 | json wrapper, append-file wrapper, INT/TERM cleanup, package doc. |

### 2.2 What each scenario covers

| # | Scenario | Mutation | Heal bound | Covered seam |
|---|---|---|---|---|
| (a) | pod restart storm | scale spike-app −50% | incremental+fullPush | mass k8s delete → nacos deregister |
| (b) | zero healthy | k8s app → 0 + consul service fully deregistered | incremental+fullPush | empty-source behavior; **documents the stale-instance finding** (consul empty-catalog guard, scenarios.go:74-83: the incremental delete path cannot fire, the prune never sees an empty pair — stale past bound, heals only on re-registration) |
| (c) | both providers, one app-code | k8s pods + consul service under the same app-code | incremental+fullPush | cluster coexistence (k8s/ecs), cross-cluster composite-id distinctness |
| (d) | nacos restart | `docker restart soak-nacos` | 2×fullPush+incremental (writable variant) | retry queue during outage, SyncAll prune restoration, **leaderless detection via write probe** (the 20260909-0000 lesson, scenarios.go:205-226) |
| (e) | consul outage 60s | `docker stop/start soak-consul` | incremental+outage / incremental+fullPush | source outage isolation: k8s leg keeps converging mid-outage |
| (f) | spotter restart | kill child, etcd lease expires, restart | incremental+fullPush | re-election, startup CompareAndFlush heals the downtime gap |
| (g) | rapid flap 20x | register/deregister one consul service 20x | incremental+fullPush | final-state convergence, keep-highest-Reversion, no duplicates |
| (h) | scale to 100 replicas | kubectl scale (20 in smoke) | BatchBound 300s | batch registration, no duplicates, convergence time |

### 2.3 Convergence bounds and observation cadence (the current shape)

- **AssertEvery 10s** (5s in smoke): each `assertOnce` (soak_test.go:439-489) scrapes queue depths, runs `nacos.checkState(expected)`, runs Atlas parity, logs one line, and increments counters. **A tick with divergences inside bounds is an observation, not a failure** (soak_test.go:432-438: "Divergences inside the current bounds are observations; the final convergence check is the hard criterion").
- **IncrementalBound 30s** — event-driven heal (informer → push → nacos).
- **FullPushBound 120s** — the `--push-interval 120` the harness passes the child (production default 21600s, pkg/providers/common.go:62); the SyncAll prune tick.
- **Drain bound (D-5)**: after churn quiescence (last churn + one full-push bound), queue depths must be zero; a breach records only on two consecutive nonzero observations (publication-lag tolerance, soak_test.go:518-558, pinned by batch4_test.go).
- **Final convergence (the §8.5 hard criterion)**: sources settled, one full pass with zero divergence + Atlas parity + drained queue → PASS (soak_test.go:602-678).

### 2.4 Flakiness history and what the audit batches changed

Four smoke runs (20260908-0839/0931/1028/1112, all 1m30s windows) plus the one full 1h run (20260909-0000):

- The smokes: 7/8, 4/8, 6/8, 7/8 scenarios. (d) failed every time — the ARM/colima nacos JVM rebuild window outlives the 270s bound (readiness returns before the instance view serves derby data). The 0931 run shows cascade flakiness: (g)/(e)/(f) all "healed in never" once (d)'s outage overlapped their windows.
- **The 2/8 full 1h run (20260909-0000)**: scenario (d)'s `docker restart` left nacos's Raft group (`naming_persistent_service_v2`) leaderless — reads served, every write 500'd (`Could not find leader`), 20,962 500s, retry queue held at 130 pending to run end. Independently, k3s's API server degraded under the sustained churn + the (h) 100-replica burst (TLS handshake timeouts, `http2: client connection lost`, one ListAndWatch taking 77,143ms). Six scenarios' "healed in never" outcomes all trace to the outage; the product verdict (Atlas never starved: 2,264 calls, zero fanout errors; queue correctly per-sink; heals within bound in every nacos-healthy window — worst 1m55.864s vs the 2m bound) required a **manual post-hoc root-cause analysis** (committed into the results file). The harness changes since: the leaderless classification + write-probe gates (assert.go:293-396, soak_test.go:772-867), the writable heal-clock variant, nacosGateBound 10m.
- **Batch 4 (327a051)** added the observation power this track inherits: D-5 the queue-depth scrape + drain bound; D-4 the catalog cross-view union (a `enabled=false` zombie the list hides becomes an extra-divergence — assert.go:531-591, `unionClusterView` per-id SET semantics so the superset doesn't double-count); D-11 the Atlas payload parity. All pinned in batch4_test.go.
- **416e62a** (post-audit, live-found P0): the remembered-pairs prune was sweeping across providers — a k8s-only full push gave the remembered ecs pair an empty desired set and DELETEd every ecs instance (the demo's demo-pay-service 0↔2 flap). Fixed sink-side: the sweep visits only remembered pairs of clusters **present in the push**; an empty push sweeps nothing. The 15-minute rehearsal below is the first continuous bidirectional observation of the demo after that fix — and its 90/90 is evidence the flap is dead (demo-pay-service held 2 instances — 1 online + 1 disabled-unhealthy — through every tick).

---

## 3. GAP ANALYSIS vs THE CORE REQUIREMENT

The requirement, taken literally: **every observation, both sides consistent** — bidirectional K8s-vs-nacos equality at each tick, at 1000s of instances, for ≥2 hours.

### 3.1 Duration — 1h observed, 2h required

`SOAK_DURATION` defaults to `time.Hour` (config.go:54); the only full run is 1h. Nothing structural prevents 2h (the schedule and scenarios scale by fraction), but two 1h-run facts say the longer window is not free: the environment cascade began ~40% in, and `nacosGateBound`/heal gates already encode multi-minute JVM pathologies that a 2h window multiplies. The 2h design (§4) must make environment-vs-product classification **per-tick and automatic** (the 20260909-0000 run needed a manual post-hoc analysis to say "environment, not product").

### 3.2 Scale — the driver tops out at ~10 instances (+1 spike to 100)

The churn: `replicas = tick%(10-1+1)+1` — a **1..10 oscillation** of `spike-app` (soak_test.go:333), plus one rotating deployment of 1 replica every 3 ticks, plus 2 stable consul app-codes rotating 4 instance ids, plus a rotating consul app-code. Steady-state instance population: **single digits**. The only scale event is scenario (h): 100 replicas (20 in smoke) — a one-shot batch, not sustained, and on the real-container k3s it is the event that helped break the API server (results/20260909-0000: `TLS handshake timeout` at 00:56:47). Track 1's kwok vehicle is the only route to 1000s (verified: zero `kwok` references in the repo; kwok v0.8.0 IS installed on this host, and a `dsca1` kwok cluster created by track 1 is running with kube-apiserver on port 34567, etcd on 34679 — I inspected it read-only; the vehicle is materializing).

### 3.3 Observation cadence and the per-tick semantics — the shape is wrong for "every observation"

**Is 10s/5s + 30s/120s bounds the right shape?** No — not because the cadence is wrong, but because the *verdict* is not per-tick. In the current harness, mid-window divergence is logged as an observation and only the final convergence is the hard criterion (soak_test.go:432-438, 165-173). A run where 332 divergent observations accumulated (20260909-0000: "134 run, 2 passed clean, 322 divergence observations") ends PASS if the last pass converges. The user's words do not allow that: every tick must match, and if a just-changed pod cannot be in nacos yet, the observation must *say so* (bounded staleness accounting), not silently pass.

**The self-consistency trap — the central gap.** The assert loop compares `expectedState` against nacos. The model is refreshed from the live sources only when: churn runs (`syncExpectedK8s` after each k8s mutation, soak_test.go:329-388), a scenario calls `refreshExpected` (soak_test.go:416-428), or the final convergence loop re-derives everything (soak_test.go:612-627). **Between those moments, the assert tick compares a cached snapshot, not the cluster.** Consequences:

1. A source change the harness did not cause (another actor, a kubelet restart of a pod, a crash-loop) drifts the model from reality; the model-vs-nacos check can pass while *both* differ from K8s — or fail while nacos is actually right (the stale-snapshot mislabel the harness itself documents at soak_test.go:612-616: "the model is the source of truth, not a stale snapshot... the final check would assert against a world that no longer exists").
2. Divergence age is genuinely unknown per item — the harness stamps every divergence's worst age as `IncrementalBound` (soak_test.go:481-483: "The divergence age is unknown per-item; bound the worst by what the remaining window still allows to heal") — a fabrication that both over- and under-counts.
3. The forensic record cannot answer "what did K8s hold at tick t": the log line records services count + divergence names only (assert.go:657-669), never the source-side observation.

The rehearsal (§6) removed the trap structurally: the expected set is re-derived from `kubectl get pods` (and the consul health endpoint) on **every tick**. The 2h design (§4) makes that the contract.

**Expected-set semantics fidelity.** The harness's consul expected set is the `passing=true` filter (`liveServiceIDs`, drivers.go:301-319). The converter's online rule is different: an entry is online only if the **`service:<appCode>` check** passes (`convertSatus`, pkg/providers/consul/convertion.go:249-270; an entry with only `serfHealth` converts to Status 2 → pushed `enabled=false`). The live demo contains exactly this case (`demo-pay-service-inst-1` has no service check): a passing-filter observer calls the nacos `enabled=false` state a **divergence** (it did, on my first rehearsal smoke run — a false positive), while the converter-rule observer calls it consistent. The 2h harness must derive the expected set with the converter's own predicate — the observation judges the pipeline's contract, and the pipeline's contract is the converter. (This is finding DS-4-8; the disabled-zombie must still be *visible* in the record — an `enabled=false` entry that the source says should be online is a divergence; DS-3-5's permanent disabled zombies stay in view.)

**Catalog union — already right.** The batch-4 union of `instance/list` with `catalog/instances` (assert.go:531-591) is the correct read side: it sees `enabled=false` (what spotter's own unhealthy pushes write), tolerates the not-found-500 steady state, and keeps duplicate detection. The 2h harness inherits it verbatim, extended with pagination beyond 100 entries per (service, cluster) — already implemented (`pageNo` loop, assert.go:184-232).

### 3.4 The verdict semantics — what "every observation consistent" must mean

Honest formalization, for a system with a convergence pipeline (informer lag + queue + HTTP writes are real):

- **Per tick t**: read the live source S(t) (kwok/k8s pod list + consul health list) and the remote R(t) (nacos list ∪ catalog). Compute the bidirectional diff per (service, cluster).
- **In-flight tolerance**: for every expected entry e whose *source state changed at time c(e)* (the churn driver knows — it issued the change; for foreign changes, the pod's own creationTimestamp/deletionTimestamp bounds it), tolerate e's absence/presence mismatch **only while `t − c(e) ≤ B`**, where B is the convergence bound — **ONE formula, stated identically here, in §4.3 step 3, and §5.1.2: `OBS_BOUND = max(track-2's SLO bound, the harness's push-interval bound P)`**. The SLO bound (track 2's measured end-to-end p99 for the event-driven pipeline: informer → queue → push → nacos-visible) judges event-driven heals; the push-interval bound P governs prune-path heals — a divergence whose only heal route is the next full push's prune legitimately waits up to P; the max covers both because the observer cannot attribute a divergence to a heal path (a bound that fits only the event-driven leg would fail healthy runs healed by the prune leg). One stated bound, three places, no per-path sums.
- **The foreign-delete fail-safe**: `deletionTimestamp` is observable only while the terminating pod object still exists. A foreign DELETE leaves no source object and therefore no observable clock — so **a remote extra with no source object and no ledger entry is DIVERGENT immediately** (fail-safe: the in-flight tolerance needs a clock; with nothing to wait for, there is nothing to tolerate). The churn profile's own deletes always carry ledger entries, so this bites only on changes the harness did not cause — exactly the changes it must not silently tolerate.
- **Divergence**: any mismatch of an entry older than B; any duplicate composite id (never tolerable — plan §8.5's "no duplicate composite instance ids per (service, cluster) — ever"); any `enabled=false` entry the source says is online; any online-expected entry that is disabled.
- **The tick verdict**: CONSISTENT (zero mismatch after tolerance), DIVERGENT (≥1 untolerated mismatch — a FAIL that accumulates; the run's acceptance requires zero), or OBSERVATION-ERROR (source/nacos read failure — retried, never counted as consistent; the §5.2 acceptance bounds it: no consecutive streak past 6 ticks, <5% of ticks overall; a persistent observation error is itself a finding, the 20260909-0000 k3s-degradation shape surfaced as read errors, not silence).
- **The run verdict**: 2h of ticks, **zero DIVERGENT ticks** is the acceptance bar; every in-flight window that did not converge within B is ALSO a failure (in-flight is tolerance, not amnesty — the entry must land within B or the tick at `c(e)+B` flips to DIVERGENT).

This is "every observation, both sides consistent" with honest transient-window semantics: the observation *distinguishes* churn-in-flight from divergence by the bound, and the bound is enforced, not just noted.

---

## 4. HARNESS DESIGN — the definitive spec (fix-phase contract)

One new tier, `make test-observe` (name distinct from `test-soak` — the existing 1h scenario soak stays as the edge-scenario tier; the observation run is its own contract). New files under `tests/observe/` (build tag `observe`), reusing the soak's proven pieces by import where the build tags allow (the soak package is tag-guarded; the shared pieces — the nacos observer, the k8s driver, the catalog union, the depth parser — should be promoted to an untagged `internal/testkit/observekit` package that both tiers import; the soak keeps its current files as thin wrappers so no behavior changes without re-verification).

### 4.1 The stack it owns — a throwaway stack, own child (the binding dsca-1 ruling)

The observe harness owns a **THROWAWAY stack**, the same own-stack/own-child pattern the soak uses and the pattern dsca-1 pinned for the scale track: **the harness never borrows the demo stack — the demo stack and its spotter process (PID 30306 at audit time) are never touched and never involved in an observe run.** Its pieces:

- **K8s source — a dedicated kwok cluster**: created by `scripts/observe-up.sh` through track 1's kwokctl machinery (the running `dsca1` cluster is track 1's *prototype*, not the run target — the observe run's cluster is its own and is torn down with the run).
- **A throwaway nacos on SCRATCH ports** (dsca-1's stack-ownership decision): never the demo's 18848, never the soak compose's — a fresh server the harness brings up, pins (512m heap), health-waits, and owns for the window. A throwaway consul on the same scratch range carries the ecs leg (the soak's own compose shape, minus k3s, minus every reserved port).
- **Its own spotter child process** — the `tests/soak/child.go` pattern verbatim: built from the repo, own process group, launched with `--providers k8s,ecs --push-interval <P>` and the kwok kubeconfig (env `KUBECONFIG`, the same contract `soak-up.sh` establishes), restart-capable, killed by the harness's teardown.
- The embedded etcd + Atlas stand-in are owned in-process (etcdmock + discoverymock), unchanged from the soak.

Two supported shapes, selected by `OBSERVE_STACK`:

- **`kwok` (the definitive one)**: the throwaway stack above. `scripts/observe-up.sh` wraps: throwaway nacos/consul up (scratch ports) → kwokctl cluster create (delegated to track 1's script) → health-waits → kubeconfig extraction; `scripts/observe-down.sh` reverses all of it (child kill → kwokctl delete cluster → throwaway servers down). busybox pre-pull NOT needed (no real containers).
- **`k3s` (rehearsal/back-compat)**: the same throwaway shape, but the K8s source is a real-container cluster — for small-scale loop-proving when the kwok vehicle is unavailable. It is NOT the existing soak stack (that stack belongs to `make test-soak`'s scenario tier): even in this shape the observe run owns its own throwaway nacos/consul and its own spotter child, pointed at the k3s kubeconfig.

**Port discipline — the demo stack is not a constraint, by construction.** The throwaway stack lives entirely on dsca-1's scratch port ranges (disjoint from the demo's 18848/18500 and from the reserved list); the child's metrics port and the Atlas stand-in's port are parameterized (`OBS_METRICS_PORT`, `OBS_ATLAS_PORT`) onto the same scratch range, because the demo's own spotter occupies the soak defaults. The observe run binds nothing reserved — which is why "demo stack and demo spotter running and untouched" is a §5.1 **sanity** gate item, not a teardown requirement: the 2h window is expected to run with the demo live the whole time.

### 4.2 The churn vehicle (contract with track 1)

The driver is `kwokDriver` — a generalization of today's `k8sDriver` (drivers.go:22-103) over the same kubectl machinery: `apply` (or direct Pod-object POST for speed — 1000 pods via `kubectl apply` of 1000-object List manifests, batched 100/object per apply), `scale` (for Node-pinned fake pods, scale = apply/delete of a batch), `delete`. All objects are bare **Pods** (no Deployments — kwok pods have no controller; the driver owns the desired count), labeled `app-code: <code>` + `K8S_CLUSTER_TYPE: test`, with a fixed podIP field (kwok `kwok.x-k8s.io/node` annotation or direct `status.podIP`) so the converter's Online path has an IP (the current `livePods` requires podIP, drivers.go:118-147 — kwok pods must set it explicitly).

Scale profile (parameterized, defaults for the 2h acceptance run):

| Parameter | Default | Meaning |
|---|---|---|
| `OBS_BASE_INSTANCES` | 1000 | steady-state pod population across `OBS_SERVICES` services (e.g. 20 services × 50) |
| `OBS_SERVICES` | 20 | distinct app-codes (nacos service cardinality) |
| `OBS_CHURN_EVERY` | 20s | churn tick (matches the soak's cadence) |
| `OBS_CHURN_RATE` | 5 (%/min) | churn as a fraction of `OBS_BASE_INSTANCES` per minute — **the dsca-1 rate, unified across the tracks** (at the 1000 default: 50 pods/min, applied per churn tick ≈ 17 create+delete pairs per 20s tick), **create-before-delete net-neutral** so the source population never drops below base |
| `OBS_BURSTS` | at 25%/50%/75% | one 100-instant-in-1s burst (the storm shape) and one 200-instance batch (the (h) shape at scale) |
| `OBS_DURATION` | 2h | hard wall-clock window |

The churn driver records, per mutation, the exact ledger entry: `{op: create/delete, podName, appCode, issuedAt}` — this is the in-flight clock source the observation tolerance consumes (`c(e) = issuedAt`). For foreign/organic changes (pods not driven by the ledger), the pod's `creationTimestamp`/`deletionTimestamp` is the fallback clock — with the §3.4 fail-safe stated once more, because it lives here operationally: **`deletionTimestamp` is observable only while the terminating pod object exists; a foreign DELETE leaves no source object and no observable timestamp, so a remote extra with no source object and no ledger entry is DIVERGENT immediately.** The churn's own deletes always carry ledger entries; the fail-safe bites only on foreign changes, which is exactly its job.

The consul leg stays at the soak's scale (2 stable + rotating) — the core requirement's scale axis is the k8s path (the plan's scale track #1), and the consul provider shares the worker/sink machinery under test.

### 4.3 The observation loop (per tick, ≤10s, the core deliverable)

```
every tick (default 10s, OBS_TICK):
  1. SOURCE    : kubectl get pods -o json (ALL observed app-codes, one call)
                 → expectedK8s = {appCode: [podName…]} for phase=Running, podIP≠""
                 consul: GET /v1/health/service/<code> per code
                 → expectedConsul with the CONVERTER predicate (service-check rule, §3.3)
  2. REMOTE    : per service: GET instance/list
                 ∪ GET catalog/instances?clusterName=k8s + clusterName=ecs (paginated)
                 (the assert.go union, max-count semantics, disabled flags kept)
  3. LEDGER    : in-flight set = entries with t − c(e) ≤ B
                 (B = OBS_BOUND = max(track-2's SLO bound, the push-interval
                 bound P) — the single §3.4 formula, no per-path sums;
                 pre-track-2 the soak's 30s/120s defaults stand in:
                 max(30s, P))
  4. DIFF      : bidirectional multiset diff per (service, cluster):
                 missing (expected-online, not remote) / extra (remote, not expected)
                 dups (within one view — never tolerable; cross-view overlap
                 collapses, §6.2's max-count union)
                 disabled mismatch (expected-online but enabled=false, and vice
                 versa) — on BOTH clusters: a remote disabled entry with no
                 source object is an immediate divergence (the §3.4 fail-safe)
  5. VERDICT   : every mismatch either (a) names an in-flight ledger entry, or (b) is DIVERGENCE
                 tick verdict = CONSISTENT | DIVERGENT | OBSERR (any source or
                 remote read failure — retried, never PASS, never divergence
                 evidence on an emptied side; §6.2's engine fix, §5.2 bounds it)
  6. RECORD    : JSONL tick record (ts, source counts, remote counts, in-flight count,
                 divergences[] with {appCode, cluster, podName, composite instanceId,
                 kind: missing/extra/dup/disabled, firstSeenTick, ageMs},
                 queue depths scraped from :18090, tick duration)
  7. CONTINUITY: a divergence seen at tick t is carried (keyed) until resolved —
                 firstSeenTick pins its true age (fixes DS-4-3's fabricated worst-age)
```

Budget discipline: the remote walk is O(#services) GETs (list + 2 catalog reads each) + pagination; at 20 services that is ~60 requests per tick — at nacos's sub-100ms local latency, comfortably inside 10s; the kubectl get is one call. If a tick's reads exceed the tick budget, the tick is recorded OBSERR (never silently skipped) and the tick cadence stretches — a stretched tick still produces exactly one record, with its true interval kept (§5.2's duration criterion counts records, not slots).

**Verdict aggregation** (the summary artifact, `tests/observe/results/<stamp>.md` + the raw JSONL committed): ticks total / consistent / divergent / obserr; the divergence ledger (every divergence with its full forensic record); max heal time (firstSeen→resolved per ledger entry); consistency ratio per 10-minute bucket (the honest windowed view); queue-depth max and drain verdict (D-5 inherited); the environment classifier's record (see 4.5).

### 4.4 The forensic record (per divergence)

Each divergence entry carries: pod uid + podName + appCode; the nacos composite instanceId (`ip#port#cluster#group@@service`) and its metadata instanceId; firstSeenTick ts and resolvedTick ts (age); the kind; the ledger link (was it in-flight? when did churn touch it?); and the log slice — the observation harness tails the child's `build/observe/log/app.log` and extracts the lines naming that podName/instanceId ±2s around firstSeen (the `nacos: registered instance …` / `retry trying to push instance failed` / `unsync service worked count` lines — the exact shapes the 20260909-0000 root-cause analysis used, but captured automatically at the moment of divergence, not post-hoc).

### 4.5 Environment classification, per tick, automatic

The 1h run's lesson: read errors and write failures must be classified live, or a 2h run drowns in manual analysis. The loop reuses the soak's classifier (assert.go:293-396): leaderless 500s (`Could not find leader`, `ConsistencyException`), nacos-read EOF/timeout (retryable), plus a periodic write probe (the `soak-env-probe` service pattern, invisible to the model). The tick record carries the environment state (healthy / leaderless / rebuilding / unreachable); a DIVERGENT tick whose window overlaps a classified nacos outage is tagged `env-overlap` — **still recorded as divergent** (the honest log), but the summary separates product-divergence (no env overlap at firstSeen, and the divergence persisted past the outage's end + one bound) from environment-divergence. The acceptance criteria (§5) count only product-divergence for PASS, but require ZERO unexplained ticks.

### 4.6 The make target

```
make test-observe                      # the definitive 2h run (throwaway kwok stack, 1000 instances)
SOAK_DURATION=30m OBS_BASE_INSTANCES=100 make test-observe   # scaled-down rehearsal
OBS_STACK=k3s make test-observe        # the real-container back-compat shape
```

Same shape as `test-soak` (Makefile:87-98): `scripts/observe-up.sh` (throwaway nacos/consul on scratch ports → kwokctl cluster create → health-waits → kubeconfig) → build → `go test -tags=observe -run '^TestObserveConsistency$' -timeout 150m` → `scripts/observe-down.sh`. The test owns the child, the etcd, the stand-in, the churn, the loop, and the summary, and tears them down in order (binary → stand-in → etcd, the existing defer discipline); observe-down then reverses the stack (kwokctl delete cluster + throwaway servers down). **The teardown is of the throwaway stack only — the demo stack and its spotter are not part of any observe run (§4.1) and are never touched, before, during, or after.**

### 4.7 What the design does NOT change

The scenario soak (a)–(h) stays `make test-soak` — its job is edge-scenario coverage with heal bounds, which is complementary, not subsumed: the observation run's churn is continuous but does not restart nacos or kill spotter (environmental scenarios at 1000 instances would mostly measure the colima JVM; a scaled scenario tier is a follow-up gated on track 1's stability findings). The Atlas parity check (D-11) is inherited into the observation loop (one extra diff per tick, near-free — the stand-in is in-process). The D-5 drain bound is inherited unchanged (with the two-consecutive-publication persistence rule).

---

## 5. THE 2H RUN GATE + ACCEPTANCE CRITERIA

### 5.1 Gate — all must be true before the definitive run starts

1. **Track 1's kwok vehicle landed**: `kwokctl` cluster script + the fake-pod manifest generator committed; the vehicle demonstrated ≥1000 virtual pods with stable watch semantics for ≥30 minutes against a spotter child (its own rehearsal), with the podIP/label contract of §4.2 verified convertible (`K8S_CLUSTER_TYPE: test` + `app-code` + podIP set).
2. **Track 2's SLOs landed**: the end-to-end latency metric (QueueObject.CreateAt → nacos-visible) exists and the per-instance SLO under burst is measured — the 2h run's `OBS_BOUND` is set from the single §3.4 formula, **`OBS_BOUND = max(track-2's SLO bound, the push-interval bound P)`**, not from any single measured percentile alone. The rationale is spelled out once: the SLO bound judges the event-driven pipeline (informer → queue → push → nacos-visible); the push-interval bound governs prune-path heals — a divergence whose only heal route is the next full push's prune legitimately waits up to P (a healthy prune-healed run waits for the next full-push tick). Judging every heal by the event-driven p99 alone would fail runs the product healed correctly. The max covers both because the observer cannot attribute a divergence to a heal path. (Setting the bound from the measured p99 ALONE is explicitly rejected — it would fail healthy prune-healed runs; this bullet and §3.4/§4.3 state the one formula identically. If track 2's fixes drive the event-driven p99 to milliseconds, the push-interval leg still floors the bound — tightening P for the 2h run is the lead's call, recorded in the run's summary header.)
3. **Track 3's nacos-source reconcile landed (or explicitly deferred by the lead)**: with the diff source still Atlas-the-fake, the 2h observation's remote-side heal coverage is the prune alone (id-set) — acceptable for the id-level consistency the core requirement names (both sides' *instance sets* equal), but field-level drift would be invisible; if track 3's `--reconcile-source nacos` lands first, the run observes it live. The gate requires the lead's explicit decision either way, recorded in the run's summary header.
4. **The rehearsal ladder (all three rungs, in order, before the 2h attempt is authorized)**: (1) the corrected-engine short rehearsal — the §6.2 shape, minutes-scale, against a stable stack, validating the comparison engine itself (duplicate detection live, k8s-disabled-zombie accounting live, source-error classification live — beyond the §6.2 offline validation); (2) the 30m/100-instance rehearsal **on the observe stack** (§4.1's throwaway stack: kwok + throwaway nacos + own spotter child) with zero product-divergence and a passing drain verdict — infra proven at small scale before burning 2h+ wall clock; (3) the 2h/1000+ definitive run. No rung is skipped. (The historical note this replaces: the original 15m §6 rehearsal ran against the demo stack read-only — methodology proof only, and with the P1-3 engine defects; the ladder's rung (1) now exists precisely because of that.)
5. **The stack floor (a SANITY gate, not a teardown constraint)**: colima 6 vCPU / ~10 GiB (the §8.2 floor), throwaway nacos pinned 512m heap — and the **demo stack and demo spotter running and untouched** through the window (sanity item: the observe run's throwaway stack binds only dsca-1 scratch ports, per §4.1, so the demo is expected to coexist with the run; the gate checks the demo is alive and unmodified, and the run's own record must show zero interaction with it). The observe run binds nothing reserved.

### 5.2 Acceptance criteria — the definitive run PASSES iff ALL hold

- **Duration**: ≥2h0m of wall-clock window completed (ticks ≥ 720 — one record per 10s slot; a tick whose reads stretched past the 10s budget still produces exactly one record, its true interval kept, the cadence stretched into the next slot), no truncation by harness error.
- **Scale**: `OBS_BASE_INSTANCES` ≥ 1000 sustained (source count ≥ 1000 on ≥95% of ticks — mid-churn dips below 1000 are expected during delete-in-flight moments even under create-before-delete net-neutral churn, and the per-tick source count is recorded so the dips are inspectable), churn active the whole window at the §4.2 rate (≥ 5% of the base population per minute, i.e. ≥ 50 instance changes per minute at the 1000 default), all three burst events executed.
- **The core requirement — per-tick consistency**: **zero product-divergent ticks** (every divergence either env-overlap-classified with the §4.5 separation, or an in-flight entry that converged within `OBS_BOUND`; an in-flight entry exceeding the bound flips its tick to divergent — no amnesty).
- **Observability of the observation — the OBSERR bound**: a 2h run with zero divergence and 700 OBSERR ticks passes every other bullet and proves nothing was actually observed — so OBSERR is itself an acceptance criterion. The run PASSES only if: (a) **zero consecutive-OBSERR streaks longer than 6 ticks** (1 minute — a streak past that is a failing observation channel, not transient blips); (b) **OBSERR < 5% of all ticks** (≤ 36 of 720); (c) every env-classified OBSERR window is separated per §4.5's classification (an nacos outage that blinds the remote side is env; a kubectl/API read failure the observer must still count, tag, and bound). Total blindness is a FAIL, not a vacuous pass.
- **Bidirectionality proven**: the record contains, per tick, both the source observation (kubectl counts) and the remote observation (list+catalog counts) — the two sides' counts equal after tolerance on every consistent tick.
- **No duplicates**: zero duplicate composite instanceIds in any (service, cluster) view, any tick — **the criterion is stated against the unioned view with the §6.2 max-count union semantics**: an id the list and the catalog both serve is NOT a duplicate (cross-view overlap collapses); an id appearing more than once within one view is. (The comparison engine must detect this — the §6.2 engine fix exists because the §6 engine could not.)
- **Retry queue**: drained (D-5 semantics) after churn quiescence at window end; max depth recorded.
- **Convergence**: every churn mutation's ledger entry converged (present in nacos if created, absent if deleted) within `OBS_BOUND` (the §3.4 formula — max of the track-2 SLO bound and the push-interval bound); the max observed convergence time recorded and ≤ the bound.
- **Environment**: any env-classified window documented with its own record (the 2h counterpart of the 20260909-0000 analysis, produced automatically); an environment failure that stops the run → the run is VOID (not FAIL) and re-attempted after the environment fix — but a run that needs >2 re-attempts is itself a finding.
- **Artifacts**: the JSONL per-tick log + the summary + the divergence forensic records committed under `tests/observe/results/`; the delivery document (plan Phase 3) quotes the per-tick consistency ratio and the verdict.

---

## 6. THE REHEARSAL (executed NOW — methodology proof on the live demo)

### 6.1 The original 15-minute run (09:59–10:14)

**Setup.** Scratch script `/tmp/ds4-rehearse.py` (python3; read-only against the demo stack: `kubectl get pods -o json` with the demo kubeconfig, nacos GETs on 127.0.0.1:18848, consul GETs on 127.0.0.1:18500; zero repo modification; no port bound). Per §4.3's loop shape, every 10s for 15 minutes: re-derive the expected sets from the **live** sources (per-tick — no cached model), read the nacos list ∪ catalog views, bidirectional diff with disabled/unhealthy accounting, per-tick verdict to a JSONL + log. The demo stack under observation: spotter PID 30306 (all fixes through 416e62a, `--push-interval 60`), 3 services — `demo-order-service` (8 pods), `demo-user-service` (2 pods), `demo-pay-service` (2 consul entries: inst-1 without a service check, inst-2 with a passing one).

**Methodology development note (honest): the first smoke run flagged a false divergence.** The naive expected set (`consul passing=true` filter, the soak driver's own rule — drivers.go:301-319) marked `demo-pay-service-inst-1` missing/unexpected-disabled, because inst-1 has no `service:demo-pay-service` check, converts to Status 2 (pkg/providers/consul/convertion.go:249-270), and is pushed `enabled=false` — which the catalog view surfaces and the list hides. The converter-predicate expected set (service-check rule) resolves it: inst-1 is *expected-unhealthy*, its `enabled=false` state is CONSISTENT, and the observation still watches it (a disappearance would be a real divergence; a flip to enabled would be too). This is finding DS-4-8 demonstrated live — the observation semantics must mirror the converter, and the catalog cross-view is what makes the disabled state visible at all.

**The per-tick log (90 ticks, first tick 09:59:14, last tick 10:14:05; the summary line was written at 10:14:15, one 10s slot later — 09:59:14 → 10:14:15 is the run envelope, the ticks themselves end at 10:14:05).** Every tick: verdict + per-service expected/remote counts (order = demo-order k8s, user = demo-user k8s, pay = demo-pay-service ecs expected/remote where remote counts online+disabled). The full JSONL is preserved at `/tmp/ds4-rehearsal-ticks.jsonl` (90 lines, one record per tick); the console log at `/tmp/ds4-rehearsal.log`. The complete tick table:

```
tick 01–90, 09:59:14 → 10:14:05 (every 10s):
  verdict: CONSISTENT (all 90)
  demo-order-service: k8s expected 8, nacos remote 8, missing 0, extra 0, dups 0
  demo-user-service:  k8s expected 2, nacos remote 2, missing 0, extra 0, dups 0
  demo-pay-service:   ecs expected-online 1 (inst-2), nacos online 1;
                      expected-unhealthy 1 (inst-1), nacos disabled 1  → consistent
  observation errors: 0;  ticks skipped: 0
SUMMARY: 90 ticks, 90 CONSISTENT, 0 DIVERGENT, 0 OBSERR
```

(The raw per-tick lines — all 90 identical in shape, `tick NN HH:MM:SS CONSISTENT {…counts…}` — are in `/tmp/ds4-rehearsal.log`; reproduced in full in this section they add 90 identical rows, so the table above states the invariant that held on every tick. Nothing was filtered: there were no other rows. The one anomaly in the whole run is in the SUMMARY line counting — see below.)

**Honest anomalies and transients recorded:**

1. **Zero transients.** No in-flight window, no divergence, no observation error occurred in the 15 minutes: the demo was quiet (the last churn before the window was the 5 order-pods recreated ~44 minutes earlier, created 01:15:37Z = 09:15:37+0800 — all healed; the demo spotter runs `--push-interval 60`, so a full-push cycle runs **every minute** — ~15 complete cycles in the 09:59:14–10:14:05 window, each cycle k8s CompareAndFlush at :47 and consul CompareAndFlush at :32–33 of the minute, `build/demo/app.log` in-window range 09:59–10:14; every cycle re-registered the same instance sets idempotently — 360 `nacos: registered instance` lines in-window, **zero deregistrations** — the steady state under a per-minute SyncAll is exactly the invariant the 90/90 records). The expected transient shape (a pod restart's heal window) did NOT materialize because nothing restarted during the window — the rehearsal proves steady-state observability and the comparison semantics, not the transient-window tolerance; that is what the fix-phase rehearsal with the churn driver proves.
2. **The demo's own known conditions, observed and classified (not hidden):** `demo-pay-service-inst-1` sits permanently `enabled=false` (probing, no consul service check — the DS-3-5 shape, a stable disabled zombie the source semantics justify); the spotter log's 31,953 historical `retry trying to push instance failed` lines are all from 2026-09-10 01:14 and earlier (the pre-416e62a empty-IP deregister 400s: `nacos: deregister #7096/k8s … Param 'ip' is required` — old instances `demo-order-service-5b7949749f-*`, i.e. a hash of the pre-restart ReplicaSet); zero such lines during the observation window, zero `Could not find leader` lines in the whole log. The catalog-vs-list F8 asymmetry is live and visible every tick (pay-service: catalog 2, list 1) — the union read is what keeps the observation honest.
3. **The `payE=1/2` rendering quirk** in my compact table above (ecs_expected 1, ecs_remote 2): the remote 2 counts the disabled instance (online 1 + disabled 1), and the diff engine reconciles expected-unhealthy=1 against disabled=1 — the JSONL's per-tick records carry the exact per-cluster breakdown (`ecs_expected` = online-expected; the disabled accounting is the `unhealthy_mismatch` check, which returned empty every tick). Consistent-by-construction was verified per tick, not by count equality alone.

**What the rehearsal proves:** the observation methodology (live-source per-tick read, list∪catalog union, bidirectional diff, converter-faithful expected semantics, per-tick verdict with a full record) executes cleanly for 15 minutes against the real stack with zero false positives and zero misses — and the demo stack is consistent on every one of the 90 observations post-416e62a. **What it does not prove:** scale (10 instances vs 1000), duration (15m vs 2h), churn tolerance (no in-flight window exercised), the kwok vehicle (not used) — and, as the review established, its comparison engine carried the three defects §6.2 fixes: the original 90/90 is evidence about the *harness's* observation loop and the stack's steady state, not about the engine's duplicate/zombie/error detections (which §6.2's offline validation and re-run supply). Those are exactly the axes §4/§5 gate.

### 6.2 The review-pass engine fix and corrected-engine re-run (P1-3, executed)

The adversarial review found that §6's script, while its loop and its verdict were sound, had a comparison engine with **three defects** — each one a §5.2 criterion or a §4.3 semantic the rehearsal *claimed* to validate but structurally did not:

1. **Duplicates were undetectable.** `union_ids` built the list∪catalog union through a ternary that emitted exactly 1 occurrence per id regardless of counts — so a genuine within-one-view duplicate collapsed to 1 and the "dups 0" column was a constant. The "No duplicates any tick" line in §6 and the §5.2 no-duplicates criterion were being *asserted* on an engine that could never have reported otherwise, while assert.go's `unionClusterView` (assert.go:552-591) deliberately keeps a within-one-view duplicate via **max-count** semantics so duplicate detection keeps its teeth.
2. **k8s-cluster disabled zombies were invisible.** Remote `#disabled` k8s entries were filtered out of the online diff and never compared against anything — a k8s-cluster instance stuck `enabled=false` with no source pod (a disabled zombie in the k8s cluster, the exact shape the soak's union surfaces as an extra) simply did not exist for the comparison. The ecs cluster had the disabled-vs-expected-unhealthy check; the k8s cluster did not.
3. **Source-read failures produced false DIVERGENT ticks.** A transient kubectl blip emptied the expected set → every remote instance became "extra" → the tick printed DIVERGENT, the opposite of §4.3's OBSERR (read failure — retried, never counted as divergence evidence). Consul read failures were silently swallowed to empty lists.

**The fix (applied to `/tmp/ds4-rehearse.py`, then validated offline before any live run).** (a) `union_ids` now implements `unionClusterView`'s max-count union verbatim: cross-view overlap collapses (list and catalog both serving an id counts once), a within-one-view duplicate survives at its max count; duplicate detection now compares real counts. (b) Every remote k8s entry is accounted for: online entries vs expected-online pods, disabled entries vs expected-unhealthy pods (Running-but-not-containers-ready, the converter's Status Unhealthy), and duplicates are computed over the whole unioned view; a disabled entry with no source pod — or a ready pod pushed disabled — is a divergence, immediate (there is no clock to wait for: no source object means no deletionTimestamp, the §3.4 fail-safe). (c) Any source or remote read failure marks the tick **OBSERR** (retried, never CONSISTENT, never DIVERGENT), exactly §4.3 step 5.

**Offline validation (15/15 checks):** the corrected engine was driven with synthetic source/remote views before touching the live stack — cross-view overlap collapses / within-view duplicates survive (list side and catalog side) / the positive control stays CONSISTENT / a k8s-cluster disabled zombie → DIVERGENT named as `unexpected_disabled` / a within-view duplicate → DIVERGENT flagged in `dups` / cross-view overlap alone is NOT a duplicate / a kubectl failure → OBSERR (not false DIVERGENT, error recorded) / a nacos read failure → OBSERR / an expected-unhealthy pod appearing disabled → CONSISTENT / a ready pod pushed disabled → DIVERGENT in both missing and unexpected-disabled. The three §5.2/§4.3 semantics the original engine never validated are now pinned by construction, and the fix phase must pin them as unit tests in the observe package — batch4_test.go's `TestSoakUnitUnionClusterViewSetSemantics` (max-count union, the exact table: overlap collapses, within-view duplicate on either side survives) and `TestSoakUnitCatalogUnionSurfacesDisabledZombie` (a catalog-surfaced disabled entry becomes a divergence) are the templates.

**The live corrected-engine re-run (8 minutes, 10s tick, 48 ticks, first tick 11:42:53, last tick 11:50:43, summary written 11:50:53): 48/48 CONSISTENT, 0 DIVERGENT, 0 OBSERR.** Same demo stack, same read-only contract (the demo spotter PID 30306 started 02:27:29 and was verified alive and untouched after the run). The per-tick counts now carry both sides' full breakdown — `k8s_expected 8 / k8s_remote 8 / k8s_expected_unhealthy 0 / k8s_disabled 0`, `ecs_expected 1 / ecs_remote 2 / ecs_expected_unhealthy 1 / ecs_disabled 1` — so the "expected 1 vs remote 2" pay-service shape of §6's quirk note is now first-class in the record (online 1 + disabled 1 vs expected-online 1 + expected-unhealthy 1). Zero duplicates on any tick under the corrected max-count union; zero disabled entries in the k8s cluster on any tick (the engine would have flagged them); zero read failures (the engine would have classified them). The stable demo exhibits none of the three defect shapes live — the re-run's value is that it proves the *engine* no longer has blind spots, not that the demo exercised them; the offline validation is what covers the shapes themselves, and the fix phase's unit tests keep them pinned.

**Artifacts (all outside the repo):** `/tmp/ds4-rehearse.py` (the corrected script, defects documented in its header), `/tmp/ds4-rehearsal2.log` (48 per-tick console lines), `/tmp/ds4-rehearsal2-ticks.jsonl` (48 per-tick JSON records), `/tmp/ds4-rehearsal2-summary.json`. The original §6 artifacts are preserved unmodified under the `/tmp/ds4-rehearsal*` names.

---

## 7. FINDINGS

### DS-4-1 (P0) — No sustained large-scale consistency observation exists; the core requirement is unobserved and the current harness cannot produce it

**Evidence.** The requirement: 1000s of instances, ≥2h, every observation consistent. The churn driver's scale ceiling: the 1..10 replica oscillation (soak_test.go:329-338: `replicas = tick%(10-1+1)+1`), one rotating 1-replica deployment, 2 stable consul app-codes (config constants soak_test.go:879-885); the single scale event is scenario (h)'s one-shot 100 replicas (scenarios.go:430-478) — which on the 1h run coincided with k3s's API degradation (results/20260909-0000: `TLS handshake timeout` from 00:56:47). Duration: `SOAK_DURATION` default 1h (config.go:54); the only full run 1h. Zero `kwok` references in the repo. **Commit coverage, corrected per git:** the four smoke results (20260908-0839/0931/1028/1112) and the 1h run were all committed 2026-09-08/09 (`b3578b6`, `417e041`); batch 4's observation additions are `327a051` (2026-09-10) and `416e62a` is 2026-09-11 — **no committed run of any kind postdates batch 4, let alone 416e62a.** The correction strengthens the finding: nothing covers the post-batch-4 code at all, and the two most recent product-behavior changes (the catalog-union read side, the prune scoping fix) are entirely unobserved at any scale. (An earlier revision of this finding claimed "only 4 smoke runs postdate batch 4" — wrong on the dates; the smokes predate it.)

**User-visible scenario.** The user asks "run 1000s of instances for 2 hours and show me every observation consistent"; the honest answer today is "the longest observation is 1 hour at ~10 instances, it ended in an environment cascade, and it predates the last two fixes." Every other track's fix will land unobserved at scale.

**Fix design.** The §4 harness (this document's core deliverable) + the §5 gate and acceptance criteria. The fix-phase execution order — the §5.1.4 rehearsal ladder, stated once and identically in both places: (1) promote the shared observation kit from the soak package; (2) build the observe test with the per-tick live-source loop and the §6.2-corrected comparison engine, pinned by unit tests for the three P1-3 semantics (max-count union, k8s-disabled accounting, source-error → OBSERR — batch4_test.go's `TestSoakUnitUnionClusterViewSetSemantics` / `TestSoakUnitCatalogUnionSurfacesDisabledZombie` are the templates), and pass the corrected-engine short rehearsal on a stable stack; (3) the 30m/100-instance rehearsal **on the observe stack** (§4.1's throwaway stack: kwok + throwaway nacos + own spotter child); (4) integrate track 1's kwok vehicle at 1000 (rehearse 30m/1000); (5) execute the 2h definitive run against the §5.2 acceptance criteria; (6) commit the artifacts. This is the P0 the whole track exists to close — its fix IS the harness plus the run.

### DS-4-2 (P0) — The assert loop compares a cached expected model, not the live K8s state (self-consistency trap)

**Evidence.** `assertOnce` (soak_test.go:439-447) runs `h.nacos.checkState(h.expected)` against the model; the model refreshes from the sources only at churn mutations (`syncExpectedK8s` after each apply/scale, soak_test.go:329-409), scenario boundaries (`refreshExpected`, soak_test.go:416-428), and the final convergence loop (soak_test.go:612-627). Between those, an assert tick judges nacos against a snapshot: a source change the harness did not cause (pod crash-loop, node restart, third-party actor) can leave model, nacos, and reality in agreement-with-model but divergence-from-cluster — and the tick prints `divergences=0`. The harness itself documents the inverse half of the trap (the stale-snapshot mislabel at soak_test.go:612-616) — the same staleness, in the passing direction, is invisible. The rehearsal script demonstrates the fix's cost profile: a full live-source re-derive is one `kubectl get pods` + 3-4 HTTP GETs per tick (~1s observed) — negligible against the 10s tick.

**User-visible scenario.** A kubelet restarts a demo pod mid-soak; spotter (correctly) re-registers it under a new pod name. The model still holds the old pod name: the old name is "expected-missing" (a false divergence), the new name is "extra" — or, worse, the model happens to match a nacos state that itself drifted. Either way the observation is about the harness's memory, not the cluster.

**Fix design.** The observation loop's step 1 (§4.3): the expected set is re-derived from the live source on every tick. In-flight tolerance replaces the model's role as the churn ledger (the ledger records the driver's own mutations; organic changes get the pod-timestamp fallback). The existing `expectedState` structure survives only as the churn ledger's index, not as the comparison source.

### DS-4-3 (P1) — Divergence age is fabricated as the incremental bound; worst-age and "heal in" measurements are unreliable

**Evidence.** assert.go's check line prints `worst_age=<bound>` for every divergent cycle — soak_test.go:469-483 stamps `worst = h.schedule.IncrementalBound` whenever divergences exist ("The divergence age is unknown per-item"), and the soak log's `worst` tracker (assert.go:657-669) therefore records the bound, not any observed age. Divergences are also not correlated across ticks: the same missing pod seen for 8 consecutive ticks is 8 unrelated observations, so "max observed heal time" is measured only inside the scenario waits (`healMax`, soak_test.go:813-819), never for standing-assertion divergences.

**User-visible scenario.** A stale instance persists for 9 minutes (as in the 20260909-0000 run: (c)'s stuck `spike-app-inst-1001` from 00:04); the soak log shows `worst_age=30s` on every line — the reported worst divergence age understates the real persistence by 18x.

**Fix design.** The divergence ledger (§4.3 step 7): every divergence is keyed (appCode, cluster, id, kind) and carried across ticks with `firstSeenTick`; age is `now − firstSeen`; resolution clears it and records the true heal time. Per-tick JSONL makes the whole ledger reconstructable.

### DS-4-4 (P1) — "Every observation consistent" is not the verdict anywhere: mid-window divergence is an observation, only the final pass is the criterion

**Evidence.** soak_test.go:430-438: "Divergences inside the current bounds are observations (the soak reports a complete picture); the final convergence check is the hard criterion"; `report()` (soak_test.go:690-734) fails only on non-convergence, a failed scenario, a drain breach, or Atlas divergence — **no code path counts divergence observations into the verdict at all.** The committed runs demonstrate the coexistence, not the verdict: the 0839 and 1112 smokes both record "4 divergence observations" under "Standing checks" *alongside* a "Final convergence: PASS" line — and both runs report overall FAILURE, because scenario (d) failed and `report()` fails the run on any failed scenario. The structural claim is the one that matters and it stands: a run whose scenarios and final convergence pass ends **PASS regardless of how many divergence observations accumulated mid-window** — 0839/1112-style "Final convergence: PASS with divergence observations recorded" is the exact shape a green-but-divergent run would present. The `checksPassed` counter is reported but has no verdict weight.

**User-visible scenario.** The user watches a 2h run where 5% of ticks show divergence that heals "eventually"; the final summary says PASS. The user's requirement said every observation must match — the harness never checked that.

**Fix design.** §3.4/§4.3's per-tick verdict with in-flight tolerance and a **hard** bound: an in-flight entry that misses the bound flips the tick to DIVERGENT; the acceptance (§5.2) requires zero product-divergent ticks. The tolerance window is bounded staleness accounting, not amnesty — this is the honest reading of "每次观察，两边数据都是一致的" the design encodes.

### DS-4-5 (P1) — The real-container k3s vehicle collapses at the 100-replica scale; the 2h run must not depend on it

**Evidence.** results/20260909-0000: scenario (h)'s scale event at 00:56:47 → `Unable to connect to the server: net/http: TLS handshake timeout`; churn scales failing from 01:00:31 (`http2: client connection lost`, `no objects passed to scale`); spotter's reflector showing the same (watch streams ending, one ListAndWatch at 77,143ms). The soak stack's k3s runs real busybox containers (drivers.go:30-57) on a single-node colima-hosted cluster; 100 pods is its demonstrated cliff, and the plan's known-fact list says the laptop cannot host 1000s of real pods.

**User-visible scenario.** Any 1000-instance attempt on k3s: the churn driver's own kubectl calls time out first, then the informer's watches break — the observation would measure colima's API server, not spotter.

**Fix design.** §4.1/§4.2: the kwok vehicle (track 1) is the definitive stack — and per §4.1's binding ruling, the observe harness owns a throwaway kwok stack + throwaway nacos + its own spotter child, never the demo stack; k3s stays as a small-scale back-compat shape only (own throwaway stack in that shape too). The design's `OBS_STACK=k3s` rehearsal path exists precisely to prove the loop before kwok, never to carry the 2h run.

### DS-4-6 (P2) — The observation record has no source-side trace: "what did K8s hold at tick t" is unreconstructable from the artifacts

**Evidence.** The soak log line (assert.go:657-669) records `services=<n> divergences=<n> worst_age=<bound>` + the divergence detail string + the queue depths — never the live pod counts per app-code, never the source read's outcome. The summary (summary.go:25-90) carries check counts and heal max only. The 20260909-0000 root-cause analysis had to reconstruct the source state from the raw `build/soak/spotter-child.log` reflector lines by hand.

**User-visible scenario.** A divergence at tick 400 of the 2h run; the investigator has the pod name and the nacos state but cannot see whether K8s still held the pod at that tick (was it a late delete? a spotter miss?) without re-deriving history from spotter's own logs.

**Fix design.** §4.3 step 6: the JSONL tick record carries both sides (source counts per service, remote counts per service/cluster, in-flight count, environment class) + §4.4's forensic log slice per divergence. The rehearsal's JSONL is the format prototype (the §6.2 re-run's records, with the per-side online/disabled breakdown, are the current shape).

### DS-4-7 (P2) — Scenario (b)'s known stale-instance behavior is a note, not an observed verdict; the definitive run must observe it as real divergence with heal time

**Evidence.** scenarios.go:74-83: when the consul empty-catalog guard blocks the heal, the note is appended ("stale instance past bound... documented finding for the full run") and the scenario still FAILs — but the *standing* observation loop only ever saw the divergence if its window happened to overlap; the condition's persistence profile (does it heal at the next re-registration? how stale exactly?) is not recorded as a first-class observation anywhere. Post-416e62a the equivalent residue is documented (a provider whose entire source is empty no longer auto-heals — the commit's "Deliberate residual").

**User-visible scenario.** A user fully decommissions a consul service; nacos keeps serving its instances indefinitely. The 2h run's churn profile should include one full-service decommission so the observation ledger records the stale window's true length (and the fix-phase decision — 416e62a's residual vs a repair — gets its measured evidence).

**Fix design.** Add a decommission phase to the churn profile (§4.2: one service's entire instance set deleted at a known tick, never re-registered); the ledger records the divergence's persistence to window end; the summary reports it as the measured residual, not a note.

### DS-4-8 (P2) — The harness's consul expected-set predicate (passing filter) diverges from the converter's (service-check rule): false divergence for check-less entries

**Evidence.** drivers.go:301-319 (`liveServiceIDs`: `/v1/health/service/<code>?passing=true` + appCode meta match) vs convertion.go:249-270 (`convertSatus`: online only if the `service:<appCode>` check passes; otherwise Status 2 → pushed `enabled=false`). The live demo contains the exact case (`demo-pay-service-inst-1`, no service check): my rehearsal's first smoke run flagged it missing + unexpected-disabled — a false positive against a stable, converter-faithful state. The soak's scenario churn always registers WITH a passing service check (drivers.go:253-277), so the soak never trips this — the blind spot is real but latent in the soak, active in the demo.

**User-visible scenario.** An operator registers a consul service without a per-service health check; spotter (by design) represents it as unhealthy/disabled in nacos; an observation built on the passing-filter predicate reports permanent divergence — the operator chases a bug that is not there.

**Fix design.** §4.3 step 1's converter-predicate expected set: an entry's expected-online status is the `service:<appCode>` check rule; expected-unhealthy entries must appear in nacos as `enabled=false` (the catalog view — already in place), and their disappearance or enablement is a real divergence. The rehearsal script's implementation is the reference (and its DS-3-5 cross-reference: the disabled-zombie shape stays visible, correctly). The k8s cluster gets the same two-sided treatment (§6.2's engine fix): a Running-but-not-containers-ready pod is expected-unhealthy (the converter pushes it disabled), and a k8s-cluster disabled entry with no source pod is a divergence.

---

## 8. BOUNDARIES AND NON-GOALS (this track)

- The kwok vehicle's internals (fake-pod stage design, node simulation, resource envelope) are track 1's; this document pins only the driver contract (§4.2) and the observation interface to it.
- Latency SLOs and the bound values are track 2's; the observation harness parameterizes `OBS_BOUND` and consumes track 2's measurements at the gate (§5.1.2).
- The reconcile-source change (Atlas → nacos) is track 3's; the observation run judges whatever is shipped at run time and the gate records the lead's decision (§5.1.3).
- The scenario soak (a)–(h) and its environment findings (ARM/colima nacos restart pathology, etc.) are inherited as-is; this track does not redesign scenario coverage, only the standing observation contract.
- The consul leg stays at soak scale (the plan's scale axis is the k8s path); consul-scale churn is a follow-up if the lead wants it.

**Artifacts produced by this audit (all outside the repo, per the read-only rule):** `/tmp/ds4-rehearse.py` (the observation script — comparison engine corrected in the review pass, the three P1-3 defects documented in its header), `/tmp/ds4-rehearsal.log` (90 per-tick console lines, original run) and `/tmp/ds4-rehearsal-ticks.jsonl` (90 per-tick JSON records, original run) + `/tmp/ds4-rehearsal-summary.json`, and the review-pass re-run's `/tmp/ds4-rehearsal2.log` (48 per-tick console lines), `/tmp/ds4-rehearsal2-ticks.jsonl` (48 per-tick JSON records), `/tmp/ds4-rehearsal2-summary.json`. The only repo file written is this document.
