# Data Stability & Consistency — Assessment and Enhancement Plan

**Status:** IN PROGRESS
**Date:** 2026-09-11
**Theme owner:** lead (orchestration + gating); auditors A1-A5 (parallel discovery); reviewers (per-document adversarial pass); fix agents (serialized agent-1/agent-2 loop).

## User requirements binding this effort

1. **Scale vehicle.** No kwok is used today (verified: zero references in the repo). The live demo's k3s runs real containers — thousands of churning instances is beyond a laptop's k3s. The assessment MUST design a virtual-instance vehicle (kwok is the natural candidate: fake pods with real watch semantics, no container runtime) to simulate 1000s of dynamic instances against the REAL informer→provider→worker→nacos chain.
2. **K8s-path latency: milliseconds, at scale.** The consul path is out of scope for this effort. The chain K8s event → instance conversion → external store (nacos) must be measured and driven to millisecond-level end-to-end even under thousands of simultaneous instance changes.
3. **Reconcile against nacos, not Atlas.** Atlas in the demo is a fake stand-in (discoverymock), not a real store. The reconcile comparison (the periodic 3-way diff) must treat nacos as the authoritative external view.
4. **Systematic process:** Phase 1 — five parallel auditors, one document each (P0..P2 findings, boundaries, existing problems), then per-document adversarial review, then author-confirmation loops until five FINAL documents. Phase 2 — serialized self-contained fix loops (agent-1 fixes, agent-2 reviews, lead relays verdicts back and forth until confirmed done), ending in one delivery document. All documents in English.

## Ground rules (carried from the test-reliability audit; they earned their keep)

- Every finding is confirmed by full code reading, with test AND production file:line evidence.
- Every claim is verified by execution (tests run, live probes, experiments) — no "should".
- Mutations prove pins; mocks must not be more forgiving than the real system.
- Fixes serialize; the lead gates every batch on the full matrix before commit.

## Phase 1 — five audit tracks (parallel, one document each)

The five tracks mirror the user's five points. Each auditor reads its complete domain, digs boundaries, and writes `docs/dsca-<n>-<topic>.md` with P0/P1/P2 findings, each with evidence and a user-visible scenario. Auditors may run tests and start throwaway mocks on dynamic ports; they must NOT touch the running demo stack, run `make test-soak`, or bind the reserved ports (18848, 18500, 6443, 12379, 19999, 19848, 19849, 18090).

| # | Track | Document | Scope (read completely) |
|---|---|---|---|
| 1 | Scale vehicle & event-path capacity | docs/dsca-1-scale.md | kwok feasibility (install/config against the existing k3s kubeconfig OR a throwaway kwok cluster), k8srobot queue (4096, drop-on-full), ants pool 100, worker Handle fanout serialization, event coalescing/cache diff at 1000s of instances; what breaks first at what instance count (reasoned + measured where feasible); the design of a scale harness (churn driver, instance counts, observation) |
| 2 | K8s-path end-to-end latency | docs/dsca-2-latency.md | the full chain: informer callback → enqueue → Pop → pod2Instance (cache diff) → pool.Submit → worker.Handle → fanout → nacos sink register (HTTP) → nacos-visible; every synchronous hop, every lock, every allocation that adds latency; NO latency metric exists today — design one (QueueObject.CreateAt → nacos-visible timestamp histogram); measure the current baseline per-instance and under burst; identify the millisecond blockers (serial HTTP posts, per-event push, 10s request timeout interactions) |
| 3 | Reconcile correctness vs nacos | docs/dsca-3-reconcile.md | CompareAndFlush's remote view comes from worker.GetAll → FanoutSink → the FIRST sink only (Atlas, the fake) — nacos is never the 3-way diff source (plan §6.3 v1 decision, documented gap); the nacos-side reconcile is the PushAll prune (catalog-based, id-set reconciliation only — no field-level diff); design the nacos-authoritative reconcile: GetAll from nacos (reconstruct round-trip fidelity — first-port-only, metadata fields), diff semantics (ADD/DELETE/UPDATE with reversion), scoping (per provider/cluster), and what "compare nacos as the real store" changes in CompareAndFlush |
| 4 | Sustained large-scale consistency observation | docs/dsca-4-observation.md | the 1h soak never covered 1000s of instances nor the post-audit fixes; design + execute the definitive observation: virtual instances at scale, ≥2h duration, continuous bidirectional comparison (K8s source vs nacos catalog+list views), divergence detection with bounded convergence, consistency verdict per observation tick; the soak harness exists (churn/assert/scenario/driver + catalog cross-view from the audit) — extend it for scale and duration; this track OWNS proving the core requirement: "every observation, both sides consistent" |
| 5 | Model fidelity & the periodic 3-diff | docs/dsca-5-model.md | instance → nacos metadata round-trip: which fields survive (instanceId/envType/envGroup/state/idc/version/reversion/cpu), which are lossy (multi-port, Image, Label, Enabled derivation); hasInstanceDiff's compared set vs what the round-trip can express (false-positive "every cycle has changes" loops the user warns about: either a bug or a model asymmetry); parseStatus fallbacks; UPDATE semantics (reversion strictly-higher wins — can a nacos-side manual edit be invisible forever?); document the exact conditions under which the periodic diff reports a change every cycle |

## Phase 1 quality loop (per document)

1. Author (auditor) writes the document (findings P0-P2, evidence, scenarios; a fix design per P0/P1).
2. A separate reviewer agent adversarially reviews: verifies evidence, attacks weak claims, checks completeness against the track's scope.
3. Lead relays the review to the author agent; the author addresses or rebuts; repeat until the reviewer signs off (target: ≤2 rounds each).
4. The five signed-off documents become the fix-phase contract. Lead commits them.

## Phase 2 — serialized fix loops

For each document, in priority order (P0s first, then P1s; P2s at the lead's discretion):
1. agent-1 implements the fix batch (tests-first where applicable).
2. agent-2 adversarially reviews (mutations, re-runs).
3. Lead relays verdicts between them until agent-2 returns PASS with no P0/P1.
4. Lead gates on the full matrix (`go test ./pkg/... ./internal/... -count=1`, `-race` on touched packages, `-tags=e2e`, soak unit tier) and commits with a detailed English message + push.
5. Cross-batch interference is prevented by the serial schedule (one batch in flight, one working tree).

## Phase 3 — delivery document

`docs/dsca-delivery.md`: the five tracks' outcomes, every fixed finding with its commit, the executed evidence (the ≥2h observation verdict, the measured latency numbers before/after, the scale harness usage), deferred items with rationale.

## Known facts seeding the tracks (from today's code reading — do not re-derive, verify and build on)

- No kwok anywhere; k3s runs real containers (nginx:alpine demo pods); the laptop cannot host 1000s of real pods.
- k8srobot: real client-go shared informers, resync=0, queue channel 4096, **drop-on-full with no log/metric** (k8srobot.go:195-199).
- Event path: Pop → pod2Instance (cache diff) → pool.Submit(100-cap ants) → worker.Handle → fanout (sequential per sink) → nacos sink (per-instance HTTP POST, RequestTimeout 10s).
- No end-to-end latency metric exists. QueueObject.CreateAt is captured but never aggregated.
- Reconcile: k8s CompareAndFlush diffs against worker.GetAll = Atlas (first sink) — nacos is reconciled only by the PushAll prune (catalog-based id-set), per plan §6.3 v1; the audit (D-3, F8) documented this seam.
- Metadata round-trip: reconstruct() restores instanceId/envType/envGroup/state/idc/version/reversion/cpu + first port only; hasInstanceDiff compares status/state/envType/envGroup/ip/instanceId/enabled/reversion — fields reconstruct can express; multi-port/Image/Label are architectural no-diff.
- The live demo stack (k3s, consul, nacos 18848, spotter PID 30306 with all fixes through 416e62a, etcd, atlas) is running and must not be disturbed by audits.
