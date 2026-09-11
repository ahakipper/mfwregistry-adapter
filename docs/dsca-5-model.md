# DSCA Track 5 — Model Fidelity & the Periodic 3-Diff

**Status:** Phase 1 audit document (author DS-5; adversarial review returned NEEDS-REVISION — revisions applied 2026-09-09: DS-5-2's activation re-scoped as conditional on widening the consul status request, since dsca-3 as written masks the pair as a false-no-diff; DS-5-4 re-scoped per leg and its remedy reconciled with dsca-3's R2; internal cross-references corrected; §5 live-wire numbers corrected; scenario fixtures persisted as Appendix A)
**Date:** 2026-09-09 (execution window 2026-09-11, see LIVE-STATE VERIFICATION)
**Repo:** module `spotter`, branch `refactor/all`, HEAD 69b0105, read-only
**HEAD note:** audited at 69b0105; current HEAD 0ff71f7 is a docs-only delta (the five DSCA docs, no code change — every cited line number stands).

---

## 1. EXECUTION SUMMARY

The user's warning — *if the periodic sync reports changes EVERY cycle, either a system bug or the instance model is not symmetric with nacos's* — decomposed into both halves, and **both halves are real, with evidence**:

1. **The every-cycle push is LIVE in the demo right now, and it is the "system" half.** Every 60 s tick re-registers every instance in nacos — 2 registers per instance per tick (one from `CompareAndFlush`, one from `emitSyncAll`'s `PushAll`) — with zero K8s changes. The root cause is **not** a field mismatch: the compare's remote view (`worker.GetAll` → `FanoutSink.GetAll` → first sink = Atlas = `discoverymock`) is **structurally empty forever** (the stand-in records pushes but never serves them back), so `k8s.go:318`'s `len(list.Instance) == 0` branch fires on every tick and pushes the whole provider list unconditionally. The user's predicted log line (`"is newer, trigger a push"`) never fires — 0 occurrences in 94,134 log lines — the every-cycle symptom flows through the *empty-remote-list* branch instead, whose signature is a full `rsyncing instance` batch + a full `nacos: registered instance` batch per tick.
2. **The model-asymmetry half is dormant today; its activation under track 3 is split — one field unconditional, one conditional.** Executed round-trip analysis (scenarios S1–S9, below; fixtures in Appendix A) found exactly **two perpetual-mismatch fields** — `Enabled` (consul leg: local `true` unconditionally vs wire `false` for status 2, `convertion.go:103` vs `nacos.go:362-365`) and `Cluster` (local `""` vs reconstructed `clusterName`, `nacos.go:507`) — of which `Enabled` is in both diff sets (`k8s.go:253`, `consul.go:453`) and `Cluster` is in the consul diff set (`consul.go:452`). **`Cluster` (DS-5-3) activates unconditionally** the moment the nacos-source compare lands: healthy status-1 reconstructions pass even the consul leg's `[online]`-only status request. **`Enabled` (DS-5-2) is conditional**: the consul compare's remote request is `[online]`-only (consul.go:417), and dsca-3's catalog-based `GetAll` rewrite KEEPS it — the status filter drops the reconstructed status-2 instance before the field diff runs — so catalog and list mask the pair alike and it degrades to a **silent false-no-diff** (case-2, no push), not a loop; the loop activates only if that request is widened to k8s's `[online, unhealthy]` shape (k8s.go:314). Everything else the diff sets consume round-trips losslessly, including `Cpu` as float32 (S3) and the 9 metadata keys exactly as the live 18848 server serves them (S8).
3. **Verdict on the compare-vs-nacos symmetry:** for the k8s provider's diff set (status/state/envType/envGroup/instanceId/ip/enabled/reversion) the round trip is lossless for every field *except* the consul-leg `Enabled`; for the consul provider's field set (envType/envGroup/status/state/ip/idc/cluster/enabled/appCode/cpu) it is lossless except `Cluster` and `Enabled`. One-shot metadata-loss blips (wiped `reversion`, missing `status`) self-heal in one cycle (the re-push rewrites the metadata); the two structural asymmetries are the only perpetual ones — `Cluster` loops unconditionally; `Enabled` loops only if the consul compare's status request is widened beyond the `[online]` shape dsca-3 preserves (under the preserved shape it is masked as a silent false-no-diff) — because in both cases re-pushing reproduces the same asymmetric wire state.

Execution anchors: the round trip was executed against the real domain types and real wire types in a scratch copy of the repo (`/tmp/ds5repo`, module `spotter`, code identical to HEAD — the scratch executor was transient and has since been deleted; the scenario fixtures and expected verdicts persist as Appendix A); the four unexported nacos functions were replicated verbatim and anchored three ways — (a) the packages' own tests run green (`go test ./pkg/providers/k8s ./pkg/nacos ./internal/domain/instance` → ok), (b) the executor's rendered register URL matches byte-for-byte the URL the live spotter built (visible inside the log's `wokderService sync failed` error lines), and (c) the executor's derived metadata matches the metadata the live nacos 18848 serves for the same instance. Read-only GETs against 127.0.0.1:18848 confirmed the on-the-wire metadata set is exactly `metadataOf`'s 9 keys.

---

## 2. ROUND-TRIP FIDELITY TABLE (executed)

### 2.1 The three legs of the round trip, precisely

**Leg A — written to nacos** (`Sink.register`, `pkg/nacos/nacos.go:361-381`): one POST per instance with top-level params `serviceName=AppCode`, `ip=Ip`, `port=firstPort(Ports)` (`nacos.go:464-469`, i.e. `Ports[0].Port` else 0), `clusterName=clusterOf(Provider)` (`nacos.go:455-460`, `Provider` or `"DEFAULT"`), `enabled` (`ins.Enabled`, **forced `false` when `Status==2`**, `nacos.go:362-365`), `ephemeral=false`, plus `metadata` = `metadataOf(ins)` (`nacos.go:480-493`) with exactly 9 keys: `instanceId, envType, envGroup, reversion, status, state, idc, cpu, version`. The composite id nacos derives is `ip#port#clusterName#DEFAULT_GROUP@@serviceName` (`compositeID`, `nacos.go:474-476`). Status 3 (offline) never reaches register — `pushOne` routes it to DELETE (`nacos.go:322-337`).

**Leg B — restored by reconstruct** (`nacos.go:498-521`): `InstanceId` from `metadata["instanceId"]`, `AppCode` from the service name, `Ip` from `host.IP`, `Ports` = **one** `PortInfo{Port: host.Port}` (no Name/Protocol/ServicePort), `Provider` and `Cluster` **both** from `host.ClusterName`, `Enabled` from `host.Enabled`, `EnvType/EnvGroup/State/Idc/Version` from metadata, `Reversion` via `parseInt64` (**parse failure → 0**, `nacos.go:552-561`), `Status` via `parseStatus` (`nacos.go:525-535`: metadata `status` when parseable, **fallback `enabled→1 / else→2`**), `Cpu` via `ParseFloat(…,32)` only on success. `GetAll` (`nacos.go:289-312`) is built on `ListInstances` — **the instance/list view that hides `enabled=false` hosts** (`client.go:221-235`, the documented F8 blind spot).

**Leg C — compared**:
- k8s periodic compare: `CompareAndFlush` → `k.hasInstanceDiff(servIns, k8sIns)` (`k8s.go:346`), diff set = `Reversion` strictly-higher gate + `EnvType, State, Status, EnvGroup, InstanceId, Ip, Enabled` (`k8s.go:245-260`; CPU/Memory deliberately excluded, comment at `k8s.go:254`). The leg's remote request is `[online, unhealthy]` (`k8s.go:314`).
- k8s cache diff (event path): same function, `pod2Instance` (`k8s.go:211-216`), old = local cache (a conversion output, never round-tripped).
- consul periodic compare: inline diff at `consul.go:439-457` = `Reversion` higher OR (equal-revision AND `EnvType, EnvGroup, Status, State, Ip, Idc, Cluster, Enabled, AppCode, Cpu`) — note the field set is reachable **only at equal revision** (the `==` guard at `consul.go:444`), unlike k8s's unguarded plain else-if (`k8s.go:251-253`); the leg's remote request is `[online]`-only (`consul.go:417`).
- consul cache diff: `extractDiff` (`consul.go:226-277`) — **reversion-only**, no field comparison.
- domain policies `DiffNewerReversion` / `DiffEqualReversion` / `CompareThreeWay` (`internal/domain/instance/rules.go:109-172`) are the intended canonical twins but are **production-dead** (used only in `rules_test.go`); see DS-5-6.

### 2.2 The field matrix

Legend: **W** = written to nacos (how), **R** = restored by reconstruct (from where), **K** = compared by k8s `hasInstanceDiff`, **C** = compared by consul `CompareAndFlush` inline diff, **D** = consul conversion's derivation (`convertion.go`). Executed verdicts refer to scenarios S1–S9 (§3; S1/S8/S8b are the live-wire pairs — S1 the healthy ecs, S8 the k8s, S8b the unhealthy ecs) — fixtures and expected verdicts in Appendix A.

| # | Field | W (nacos register) | R (reconstruct) | K (k8s diff) | C (consul diff) | D (consul conversion) | Round-trip verdict |
|---|-------|--------------------|-----------------|--------------|------------------|----------------------|--------------------|
| 1 | `InstanceId` | metadata `instanceId` | metadata `instanceId` — exact | **yes** (`k8s.go:252`) | no | `meta[instanceId]` (convertion.go:200) | **lossless**; verified live (S8: `demo-order-service-676b75dfb7-n2l6w` on the wire). Note: NOT the composite id — the composite id is ip#port#cluster#group@@service. |
| 2 | `Ports` (multi) | only `Ports[0].Port` as top-level `port` param | **one** `PortInfo{Port}` — Name, Protocol, ServicePort lost; ports 2..n lost | no | no | full list from consul `meta["ports"]` JSON (convertion.go:156-184) | **lossy (first-port-only)**. k8s conversion hard-prepends a synthetic dubbo 7096 port (conversion.go:175-179), so the k8s wire port is always 7096. Architectural no-diff: no diff set compares ports. Composite-id stability depends on `Ports[0]` never changing (a reorder changes the id → prune+register churn). |
| 3 | `Ip` | top-level `ip` | `host.IP` — exact | **yes** (`k8s.go:252`) | **yes** (consul.go:450) | `endpoint.Node.Address` | **lossless**. |
| 4 | `EnvCode` | **never** (not in metadataOf) | **never** (empty string) | no | no | composed `envType + "#" + envGroup` (convertion.go:54) | **lossy but redundant**: `EnvCode = ComposeEnvCode(envType, envGroup)` (rules.go:174-177); any EnvCode drift implies envType/envGroup drift, both compared. Executed S6: local `"test#"` vs remote `""`, k8sDiff=false. Derivable on reconstruct if ever needed. |
| 5 | `EnvType` | metadata `envType` | metadata — exact | **yes** (`k8s.go:251`) | **yes** (consul.go:446) | `meta[envType]` | **lossless**. |
| 6 | `EnvGroup` | metadata `envGroup` | metadata — exact | **yes** (`k8s.go:251`) | **yes** (consul.go:447) | `meta[envGroup]` | **lossless**. |
| 7 | `Cluster` | **never as a field** — only `clusterOf(Provider)` goes into the composite id / `clusterName` param | **synthesized** = `host.ClusterName` (nacos.go:507) | no | **yes** (consul.go:452) | hard `""` (convertion.go:101); k8s: from pod labels/env (conversion.go:432-456; demo pods: `""`) | **ASYMMETRIC (perpetual)**: local `""` vs reconstructed `"ecs"`/`"k8s"`. Invisible today (compare reads Atlas, whose JSON carries the original `Cluster`); **activates unconditionally in the nacos-source world** — the consul leg's status request is `[online]`-only, and healthy status-1 reconstructions pass it, so the consul diff set sees `Cluster` on every ecs instance every cycle. Executed S7: consulDiff=true with all else equal. |
| 8 | `Version` | metadata `version` | metadata — exact | no | no | `meta[version]` | **lossless**, uncompared. A manual edit of `version` in nacos is invisible forever (until the source reversion bumps). |
| 9 | `Enabled` | top-level `enabled` = `ins.Enabled` **forced false when Status==2** (nacos.go:362-365) | `host.Enabled` — exact wire value | **yes** (`k8s.go:253`) | **yes** (consul.go:453) | hard `true` (convertion.go:103); k8s: derived from readiness (conversion.go:295-307) | **ASYMMETRIC for the consul leg (perpetual in the field values; conditional in the loop)**: local `true` vs wire `false` for every status-2 ecs instance. For the k8s leg it is symmetric by construction (local Enabled is derived from the same status that drives the wire override). Executed S2/S8: k8sDiff=true driven by Enabled with Status equal. Invisible today (Atlas JSON carries `Enabled:true`). In the nacos-source world **both views mask the pair, at different layers**: the list view hides `enabled=false` hosts at the HTTP layer (client.go:221-235), and — even under dsca-3's catalog-based `GetAll` — the consul leg's `[online]`-only status request (consul.go:417, kept by the design) drops the reconstructed status-2 instance at the SDK layer before the field diff runs, so the pair degrades to remote-missing → case-2 → **silent false-no-diff**. The every-cycle loop activates only if the consul status request is widened to k8s's `[online, unhealthy]` shape. |
| 10 | `State` | metadata `state` | metadata — exact | **yes** (`k8s.go:251`) | **yes** (consul.go:449) | consul check status → running/probing (convertion.go:223-248) | **lossless**. |
| 11 | `HealthState` | never | never | no | no | hard `""` | never written by any conversion; dead weight in the model. |
| 12 | `AppCode` | top-level `serviceName` (the service dimension of the composite id) | the `service` argument (from `ListServices`) — exact | no | **yes** (consul.go:454) | `meta[appCode]` | **lossless**. |
| 13 | `Provider` | as `clusterName` (clusterOf) | `host.ClusterName` — exact | no (scoping key only, `GetAll` provider filter at nacos.go:301) | no (scoping key) | hard `"ecs"`; k8s hard `"k8s"` | **lossless** (and the natural symmetric replacement for the broken `Cluster` comparison, see §4). |
| 14 | `Label` | never | never | no | no | consul `Service.Meta` map (convertion.go:122-154) | **lossy**; carried in the Atlas JSON payload. Architectural no-diff. |
| 15 | `Hostname` | never | never | no | no | `endpoint.Node.Node`; k8s: `pod.Name` (== InstanceId) | **lossy**; for k8s recoverable via `instanceId`. Architectural no-diff. |
| 16 | `Cpu` | metadata `cpu` = `FormatFloat(float64(float32), 'f', -1, 32)` | `ParseFloat(…,32)` → float32 | **no — deliberately excluded** (k8s.go:254: the discovery center stores int) | **yes** (consul.go:455) | hard `0` (convertion.go:110); k8s: summed container limits | **lossless as string metadata** — executed S3 across 8 float32 values (0, 2, 0.1, 1.5, π, 123456789, 0.123456789, 16777217): every value round-trips exactly (`123456789 → "123456790" → 123456789`). The k8s exclusion is Atlas-legacy, not nacos-necessitated. |
| 17 | `Memory` | never | never | **no — excluded with Cpu** | no | hard `0`; k8s: summed limits | lossy in the nacos world; uncompared anywhere. |
| 18 | `Disk` | never | never | no | no | hard `0`; never set by k8s either | dead weight. |
| 19 | `Os` | never | never | no | no | hard `""` | dead weight. |
| 20 | `Image` | never | never | no | no | `{}` (convertion.go:114); k8s: container image map | **lossy**; carried in the Atlas JSON. Architectural no-diff. |
| 21 | `Idc` | metadata `idc` | metadata — exact | no | **yes** (consul.go:451) | derived `office`/`mix` from envType (convertion.go:81-85); k8s: pod label/env | **lossless**. |
| 22 | `Reversion` | metadata `reversion` | `parseInt64` — **0 on missing/malformed** | **yes** (strictly-higher gate, `k8s.go:249`) | **yes** (both gates, consul.go:442/444) | consul `ModifyIndex` (convertion.go:116); k8s: pod `ResourceVersion` (conversion.go:88) | **lossless when present; parse-failure → 0 → strictly-higher gate fires once** (executed S4: k8sDiff=true). Self-heals: the re-push rewrites full metadata, so next cycle parses fine. One-shot blip, not a loop. |
| 23 | `Status` | metadata `status` | `parseStatus`: metadata when parseable, else **enabled→1 / else→2** (nacos.go:525-535) | **yes** (`k8s.go:251`) | **yes** (consul.go:448) | consul check status (convertion.go:250-272); k8s: pod phase/readiness | **lossless when metadata present** (S8: `"status":"2"` reconstructs 2 with `Enabled=false`). Fallback engages only for entries without numeric status metadata — i.e. manual console registrations, not spotter-written ones. Executed S9: absent metadata + enabled=true + local status 1 → no diff (fallback agrees); absent + manual disable → status 2 vs local 1 → diff (self-healing re-push re-enables); non-numeric `"online"` → falls back to enabled-based 1. Note status-3 never round-trips by design (status 3 routes to DELETE, and `GetAll`'s status filter drops it). |
| 24 | `Level` | never | never | no | no | hard `""` | dead weight. |

### 2.3 Executed confirmation of the on-the-wire set

Three independent anchors (all executed 2026-09-11):

1. **The live server.** Read-only GET `GET /nacos/v1/ns/instance/list?serviceName=demo-order-service` against 127.0.0.1:18848: each host's `metadata` is exactly the 9 keys — `{"reversion":"38491","instanceId":"demo-order-service-676b75dfb7-n2l6w","envType":"test","cpu":"0","idc":"","state":"running","envGroup":"","version":"","status":"1"}` — byte-equal to what `metadataOf` produces for the instance the demo pushed (the same instance's field values are visible in the log's `rsyncing instance` line). The catalog view (`GET /nacos/v1/ns/catalog/instances?serviceName=demo-pay-service&clusterName=ecs`) additionally shows `127.0.0.1#8848#ecs#...` with `enabled=false, status:"2", state:"probing"` — the unhealthy push policy — **and that host is absent from the instance/list view**, confirming the F8 hiding behavior on the live server.
2. **The live log.** The `wokderService sync failed` error lines embed the complete URL the real sink built, e.g. `POST http://127.0.0.1:18848/nacos/v1/ns/instance?clusterName=k8s&enabled=true&ephemeral=false&groupName=DEFAULT_GROUP&ip=10.42.0.24&metadata=%7B%22cpu%22...%7D&namespaceId=public&port=7096&serviceName=demo-user-service` — the param set is **8 top-level + 1 metadata JSON** (`clusterName, enabled, ephemeral, groupName, ip, namespaceId, port, serviceName` + `metadata`; this line previously said "7 top-level" — the quoted URL itself shows the eight), matching `InstanceParams.values()` (client.go:318-344) and the scratch executor's rendering exactly.
3. **The scratch executor** (transient — see the note at the end of this item). A copy of the repo at `/tmp/ds5repo` (identical code) with an added `internal/ds5exec` package importing the real `spotter/internal/domain/instance` and `spotter/pkg/nacos` types; `metadataOf`/`reconstruct`/`parseStatus`/`parseInt64` replicated verbatim (they are unexported and cannot be imported cross-package) and the k8s `hasInstanceDiff` replicated verbatim from `k8s.go:245-260`. Cross-checked against the packages' own whitebox table (`k8s_whitebox_test.go:323-455`, run green: `go test ./pkg/providers/k8s ./pkg/nacos ./internal/domain/instance` → all ok). The two live-wire pairs fed back through the executor (S8) close the loop: code → log URL → live server response → reconstruct → diff. **Transience note:** `/tmp/ds5repo` was a scratch workspace and no longer exists; the executor's role (feeding the scenario fixtures of §3 through the replicated functions and recording the verdicts) is preserved in **Appendix A** — the instance-pair fixtures and expected verdicts are persisted there, so re-validation is reproducible either from Appendix A's table or by re-deriving from the quoted functions (`metadataOf`/`reconstruct`/`parseStatus`/`parseInt64` at `nacos.go:480-561`, `hasInstanceDiff` at `k8s.go:245-260`, the consul inline diff at `consul.go:439-457`). The reviewer independently re-verified the verdicts against the quoted code during adversarial review.

---

## 3. EVERY-CYCLE-DIFF RISK ANALYSIS

The user's scenario: the periodic compare reports a change every cycle **without real K8s changes**. Analyzed per world.

### 3.0 World 0 — the world that is actually running (the demo): empty remote view

The compare never gets to a field diff at all. `CompareAndFlush` (`k8s.go:314`) calls `worker.GetAll` → `FanoutSink.GetAll` (`fanout.go:216-218`, first sink only) → `DiscoveryCenter.GetAll` → gRPC `GetAllInstance` → **`discoverymock.getAllInstance`** (`discoverymock/server.go:334-353`), which filters `s.instances` — **a slice that is never seeded** in the demo (`build/atlasrun/main.go:12-22` starts `StartTCP` and nothing else; `SetInstances` exists but no demo component calls it; `synInstance`/`synAllInstance` only *record* calls, server.go:310-332). So `list.Instance` is always empty → `k8s.go:318-322` early-returns through the **push-all branch** on every tick. The consul leg is identical (`consul.go:423-428`).

This is a "system bug"-shaped cause (a stand-in that never serves a view), **not** a model asymmetry — and it is the live cause in the demo. Quantified in §5.

### 3.1 World A — today's production shape (Atlas round trip)

If the remote view were a real Atlas serving back what spotter pushed, the compare's old instance is the **Atlas model**: the JSON payload pushed by `discoverycenter.Client.Sync` marshals the **entire domain struct** (registry.go:60-77 → client.go:116-137, `json.Marshal(instances)` — the log's `rsyncing instance` lines show every field: Ports, EnvCode, Label, Image, Hostname, Memory…). A faithful Atlas round trip is **full-fidelity** for every field of the model, so:

- Perpetual mismatch candidates: **none.** For any local steady-state pair, old==new on every compared field (both are conversion outputs of the same unchanged pod).
- The user's dichotomy resolves to "system bug" in this world: the only observed every-cycle behavior comes from the empty view (World 0), and any future every-cycle report against a real Atlas must come from a real drift or an Atlas-side write-back asymmetry (outside this repo).
- The consul unhealthy `Enabled` asymmetry is invisible here: the Atlas JSON carries `Enabled:true` (the forced-false override exists **only** in the nacos sink's register, nacos.go:362-365 — it is a per-sink write policy, not a model transformation).
- Cache-diff path (b): `pod2Instance`'s `hasInstanceDiff(cache, fresh)` compares two local conversion outputs — same source, same derivation, no round trip. CPU/Memory are excluded (k8s.go:254, the Atlas-int-legacy). Executed reasoning over the conversion functions: `formatCpuSize`/`formatMemorySize`/`formatState`/`formatStatus` are deterministic functions of the pod object; a pod that did not change produces a byte-identical instance; a pod whose status flapped produces a real change (Pod status updates bump `metadata.resourceVersion` → new `Reversion` → correctly pushed). **No flap candidate found.** The one degenerate case is offline-equal (`old.Status==new.Status==3` → false, `k8s.go:247`) — deliberate.
- UPDATE semantics (c): `new.Reversion > old.Reversion` (`k8s.go:249`) against an Atlas-served old with the same reversion → gate false; field-equality path: all equal → no push. A nacos-side manual edit is **not** in this world's compare at all (nacos is never read for comparing, plan §6.3 v1) — the only nacos reconcile is the PushAll prune, which is id-set only.

**Verdict: World A carries no model-asymmetry every-cycle risk; the live symptom is World 0.**

### 3.2 World B — the nacos-source future (track 3's compare lands, `GetAll` from nacos + `reconstruct`)

Now the compare's old instance is the **reconstructed** model, and the asymmetries become the diff input. Concrete pairs and executed verdicts (scratch executor; `k8sDiff(remote, local)` is the exact `CompareAndFlush` direction, k8s.go:346):

| # | Scenario | Local (steady state, no K8s change) | Remote (reconstructed from the wire spotter itself wrote) | k8s diff verdict | Perpetual? |
|---|----------|-------------------------------------|-----------------------------------------------------------|------------------|-----------|
| S1 | consul healthy instance (the live pair `demo-pay-service-inst-2`; defined at revision — the round number was referenced but never defined in the original draft) | `Enabled=true`, `Status=1` | `Enabled=true` (no override at status 1), `Status=1` (metadata) | **false** — the steady-state consul pair is quiet | — |
| S8 | k8s online instance, full metadata (the actual live pair of `demo-order-service-676b75dfb7-n2l6w`) | the conversion output | metadata 9 keys, `enabled=true`, `status:"1"` | **false** — the steady-state k8s pair is quiet | — |
| S2/S8b | **consul unhealthy instance** (`demo-pay-service-inst-1`) | `Enabled=true` (convertion.go:103), `Status=2`, `Reversion=308` | `Enabled=false` (wire override), `Status=2` (metadata), `Reversion=308` | **true — driven by `Enabled`** (`Status` is equal) | **Conditional** (the field diff, once reached, is perpetual — but dsca-3's design as written never reaches it): the consul leg's remote request is `[online]`-only (consul.go:417) and the catalog-based `GetAll` rewrite keeps it — `statusAllowed([online], 2)` drops the reconstructed status-2 instance before the field diff runs. So **both views mask the pair**: the list view at the HTTP layer (client.go:221-235 hides `enabled=false` hosts), the catalog+status-filter at the SDK layer — and the pair degrades to remote-missing → consul's case-2 branch → its `Status==1` gate (consul.go:469) fails → **no push: a silent false-no-diff, not a loop**. The every-cycle loop activates **only if** the consul status request is widened to k8s's `[online, unhealthy]` shape (k8s.go:314) — then the field diff runs, and re-pushing writes `enabled=false` again (the override is deliberate policy, nacos.go:362-365), so the diff refires every cycle. |
| S7 | `Cluster` asymmetry (both legs) | `Cluster=""` (both conversions; demo pods carry no cluster label) | `Cluster="k8s"`/`"ecs"` (reconstruct synthesizes from clusterName, nacos.go:507) | k8s: **false** (not in the k8s set) / consul set: **true** | **YES — perpetual, consul diff set only** (consul.go:452): if the ecs leg's compare ever reads the reconstructed view, every instance diffs on `Cluster` every cycle. Re-pushing changes nothing. |
| S4 | `reversion` metadata wiped/malformed | `Reversion=38491` | `Reversion=0` (parseInt64 failure path) | **true** (strictly-higher gate) | No — **one-shot**: the triggered push rewrites full metadata; next cycle parses fine. |
| S9b | `status` metadata absent + manual console disable | `Status=1`, `Enabled=true` | `Status=2` (fallback else→2), `Enabled=false` | **true** | No — self-healing: the push re-enables the instance (desired state asserted). This is the fallback doing its job. |
| S9a/S9c | `status` metadata absent/non-numeric, enabled=true | `Status=1` | `Status=1` (fallback enabled→1) | **false** | — (fallback agrees; only manual registrations lack the metadata, and those become remoteOnly → case 3 → deregistered, which is correct reconcile behavior). |
| S5 | multi-port pod (3 ports) | `Ports=[7096,80,9090]` | `Ports=[{7096}]` | **false** — ports are not in any diff set | n/a (architectural no-diff). |
| S6 | `EnvCode` | `"test#"` | `""` | **false** — not in any diff set; derivable | n/a. |
| S3 | `Cpu` float32 | e.g. `123456789` | `123456789` (string `"123456790"` in metadata) | n/a (excluded from k8s set; **in the consul set and round-trips exactly**) | No — 8-value sweep all equal. |

**The complete perpetual-mismatch list for World B is exactly two fields:**

1. **`Enabled`, consul leg, status 2** — the sink's own write policy (forced false) makes the local `true` and the wire `false` permanently disagree; the field is in both diff sets. **Under dsca-3 as written, the mismatch is masked, not looping**: the consul leg's `[online]`-only status request (consul.go:417, kept by the catalog rewrite) drops the reconstructed status-2 pair before the field diff — the residual is a silent false-no-diff. The loop activates only if that request is widened to `[online, unhealthy]`.
2. **`Cluster`, consul diff set** — reconstruct synthesizes the field from `clusterName`; both conversions leave it empty; the field is in the consul diff set only. **This one activates unconditionally** in the nacos-source world: healthy status-1 reconstructions pass the `[online]` filter.

Everything else either round-trips exactly (InstanceId, Ip, EnvType, EnvGroup, State, Status, Version, Idc, AppCode, Provider, Cpu, Reversion-when-present) or is excluded from every diff set (Ports, EnvCode, Label, Hostname, Image, Memory, Disk, Os, Level, HealthState). The k8s provider's diff set in World B is quiet for its own steady-state traffic (S8) — the k8s-side risk concentrates entirely in the consul leg's wider field set.

**UPDATE semantics in World B (c):** with reversion equal and no field diff, no push fires (S8). A nacos-side manual edit is visible iff the edited field is in the diff set (e.g. editing `state` metadata → field diff → re-push: self-heal). A manual edit of an **uncompared** field (`version`, `idc` on the k8s side, ports, anything) is invisible forever. A manually **forged upward reversion** suppresses the k8s leg's update path only for PURE-revision updates (`new.Reversion > old.Reversion` false, k8s.go:249) — any compared-field drift still diffs and heals (the field branch at k8s.go:251-253 is a plain else-if, not gated on revision equality) — while on the consul leg it is complete suppression (the field set is reachable only through the `==` guard, consul.go:444); see DS-5-4 for the per-leg scope and the remedy reconciliation with dsca-3's R2.

### 3.3 Which world the demo is in — the direct answer to the user

The demo exhibits the every-cycle symptom **today** (§5), and the cause is the **empty remote view** (World 0), not the model. But the user's second candidate is not hypothetical: the executed analysis shows the model asymmetry is real, and one half of it — `Cluster` — would produce the same symptom (for the consul leg, on every instance, every cycle) the moment the nacos-source compare lands, without §4's fixes; the other half — `Enabled` for status-2 ecs — is masked by dsca-3's preserved `[online]`-only status request as a **silent false-no-diff**, and converts into the every-cycle symptom only if that request is widened. The causes are distinguishable by their log signatures:

- **Empty-view cause:** full `rsyncing instance` batch per tick, **zero** `is newer, trigger a push` lines, **zero** `match much id` lines, the `discovery center online ... instances size` line (k8s.go:337) never printed.
- **Model-asymmetry cause (World B):** the `is newer, trigger a push` case-1 line per instance per tick (k8s.go:348) — the exact line the user predicted — for a fixed field: `Cluster` for all ecs (unconditional), `Enabled` for ecs-status-2 (only under a widened status request).
- **Enabled-as-false-no-diff (World B under dsca-3 as written):** no log line at all for the status-2 pair — it falls into the consul case-2 branch whose `Status==1` gate (consul.go:469) rejects it silently. Zero-log silence for a known-asymmetric pair is itself a monitoring signature (DS-4's territory: alert on "local unhealthy ecs instance absent from the compare's remote view" — the catalog view the prune already walks exposes it).

---

## 4. MODEL SYMMETRY DESIGN

Goal: the periodic 3-diff must be a function of (real drift) only — zero false diffs from the round trip, and deliberate, bounded false-no-diffs.

### 4.1 Options

**Option A — extend nacos metadata to carry every diff-compared field losslessly.**
Add metadata keys for everything the model holds (`envCode`, full `ports` JSON, `label` JSON, `image` JSON, `hostname`, `memory`, …).
- *False-diff:* eliminated for every carried field.
- *False-no-diff:* eliminated for carried fields — but no diff set consumes ports/label/image/hostname today, so the added fidelity buys nothing the diff uses, while the wire grows per instance per push (the metadata JSON rides every register URL; the URL is already ~400 bytes, and a JSON blob of ports+label+image roughly doubles it — per instance, per event, per full push, at 1000s of instances this is track 2's latency budget).
- *Evolution risk:* a mixed fleet (old spotter writes 9 keys, new writes 20) makes reconstruct's output depend on the writer's version — undetectable without a schema marker.

**Option B — restrict the diff set against reconstructed views to fields the round trip preserves.**
Drop `Cluster` from the consul field set (or compare `Provider`, which is symmetric: local `"ecs"` vs reconstructed `clusterName` `"ecs"`); drop `Enabled` from the set when the old side is reconstructed (or compare the *push-side derived* value).
- *False-diff:* eliminated for the two perpetual fields.
- *False-no-diff:* real drift in `Cluster`/`Enabled` at the source becomes invisible — but `Cluster` is not meaningfully sourced today (both conversions hardcode/derive empty), and `Enabled` is not a source field at all (it is the push policy's output). The sacrifice is a field that carries no source signal.
- *Cost:* a provider-local diff-set change; no wire change; no versioning needed.

**Option C — version the metadata schema (`schemaVersion` metadata key).**
Write `"schemaVersion":"1"` alongside the 9 keys; `reconstruct` reads it and treats missing/unknown as the oldest known shape. On a future extension, the compare can assert "entry at schema < required → re-push once to upgrade" — a bounded, idempotent self-migration.
- *False-diff/false-no-diff:* C alone fixes nothing today; it is the *vehicle* that makes A (or any extension) safely evolvable.
- *Cost:* one metadata key, one reconstruct branch, one upgrade rule.

### 4.2 Recommendation — C now + targeted B, no bulk A

1. **Add `schemaVersion` metadata key (value `"1"` = today's 9 keys).** `reconstruct` treats a missing key as v1 (backward compatible with the live entries on 18848 and any older binary's writes). This is the enabling fix for every later extension and costs one key.
2. **Fix `Enabled` symmetry by comparing the derived wire value — hardening + conditional-activation co-fix (not a required co-fix of dsca-3 as written).** The compare against a nacos-reconstructed old must evaluate `wireEnabled(local) := local.Enabled && local.Status != InstanceStatusUnhealthy` — exactly the derivation `Sink.register` applies (nacos.go:362-365). Then local-vs-remote is symmetric for both legs by construction, and the *semantic* the diff asserts ("would the next push write a different enabled than the remote holds?") is precisely the reconcile question. Keep raw `Enabled` in the model (the Atlas payload and future consumers still want the source value). **Positioning (revision):** under dsca-3 as written — the consul leg's status request preserved at `[online]`-only (consul.go:417) — the status-2 pair never reaches the field diff, so this fix is *not* required to prevent a loop there; the residual is a silent false-no-diff (masked at the SDK layer by `statusAllowed`, exactly as the list view masks at the HTTP layer). It becomes a **required co-fix the moment anyone widens the consul status request** to k8s's `[online, unhealthy]` shape (a widening dsca-3 explicitly documents as unnecessary today — its §3.3: "`[online]` is *sufficient* … no change required; documented so the next reader does not 'fix' it into a loop" — this track's verdict seconding that: widening without `wireEnabled` materializes DS-5-2's loop). As immediate hardening, adopt `wireEnabled` regardless of the filter shape: it is cheap, makes the diff's semantics match the sink's write policy, and removes the booby trap for every future widening.
3. **Fix `Cluster` asymmetry by comparing `Provider`, not `Cluster`, when the old side is reconstructed** (or, equivalently, stop synthesizing `Cluster` in `reconstruct` — leave it empty and carry the original under a v2 metadata key only if a consumer appears). `Provider` round-trips exactly (clusterOf ↔ clusterName) and is what the code actually scopes on (`GetAll`'s provider filter, nacos.go:301).
4. **Derive `EnvCode` on reconstruct** (`ComposeEnvCode(envType, envGroup)`, rules.go:174-177) — free, restores a field consumers may read, changes no diff.
5. **Do not bulk-extend metadata (no Option A).** Ports/Label/Image/Hostname/Memory stay out until a diff set consumes them; if one ever does, ship them as schemaVersion 2 with the upgrade rule from (1).
6. **The nacos-source compare must read the catalog view, not the instance list** — already dsca-3's design (§3.2 of that track), and still necessary (F8: the list view hides exactly the `enabled=false` entries the unhealthy policy writes, so a list-based compare silently degrades every status-2 pair to "remote missing"). **But note the catalog alone does not expose the status-2 pair to the consul leg**: the leg's `[online]`-only status request masks it at the SDK layer even over the catalog — necessary but, for the `Enabled` residual, not sufficient. Widening the filter is a separate decision (see item 2).

### 4.3 Decision table (field-by-field, the contract for the fix phase)

| Field | Keep in compared set? | Symmetry mechanism | If left as-is |
|-------|----------------------|--------------------|---------------|
| `InstanceId`, `Ip`, `EnvType`, `EnvGroup`, `State`, `Status`, `Reversion`, `AppCode`, `Idc`, `Version`, `Provider`, `Cpu` | yes (as today per provider) | already lossless; `Cpu` may stay excluded on the k8s set for Atlas-int compat, safe to include for nacos-only compares (S3) | no false diff |
| `Enabled` | yes, but compare **derived** `wireEnabled(local)` vs remote | §4.2-2 | under dsca-3's preserved `[online]` request: **silent false-no-diff for ecs status-2** (masked by the status filter); if the request is widened without the fix: **perpetual every-cycle diff** — DS-5-2 |
| `Cluster` | replace with `Provider` in the consul set (or drop) | §4.2-3 | **perpetual every-cycle diff for all ecs (World B)** — DS-5-3 |
| `EnvCode` | never (derived) | derive on reconstruct | no diff either way |
| `Ports`, `Label`, `Image`, `Hostname`, `Memory`, `Disk`, `Os`, `Level`, `HealthState` | never (no consumer) | do not carry; schemaVersion 2 if a consumer appears | false-no-diff only, currently meaningless |
| `Status` fallback path | n/a | keep `parseStatus` fallback (self-heals, S9) | one-shot blips at worst |
| `Reversion` parse failure | n/a | keep `parseInt64`→0 (self-heals in one cycle, S4) | one-shot blip |

---

## 5. LIVE-STATE VERIFICATION (the demo's steady state)

Stack (untouched, read-only observation): spotter PID 30306 (`adapter --env test --providers k8s,ecs --nacos-addr 127.0.0.1:18848 --push-interval 60 …`), started 2026-09-11 02:27:29; k3s with 10 demo pods (5 aged ~47 h, 5 aged ~42 min at observation time); consul 18500 with 2 ecs instances; nacos 18848; Atlas stand-in = `build/atlasrun` (discoverymock, 127.0.0.1:19999); etcd 12379. Log: `build/demo/app.log`, 94,134 lines, 2026-09-09 09:44 → 2026-09-11 09:55.

**The user's scenario is LIVE.** Steady-state numbers for 2026-09-11 (494 k8s ticks + 494 consul ticks at 60 s). **Window note:** the 494-tick count is the calendar-day count of tick log lines; it includes ~45 ticks logged by an earlier spotter run before the current binary (PID 30306) started at 02:27:29 — i.e. ~449 ticks from the current run plus ~45 residual from the run at 01:42:35 that preceded it (the pre-02:27:29 lines come from a process whose log lines are indistinguishable by content). The per-tick rates are unaffected (registers-per-tick ratios are computed within matched windows, e.g. the 09:53:47 window analysis below).

| Signal | Count | Meaning |
|---|---|---|
| `nacos: registered instance` | 5,385 k8s + 1,980 ecs | per instance per k8s tick: 990/494 = **2.00** (old pods), 87/42 = 2.07 (young pods); per ecs instance: 1,980/2/494 = **2.00** |
| `rsyncing instance` (Atlas per-instance Sync) | 507 per old instance | ~1.03/tick — the CompareAndFlush push-all branch |
| `synced instance to the discovery center successfully` | 508/508 (ecs), 507 (old k8s), 55 (young k8s) | confirms the Atlas leg of the same pushes |
| `is newer, trigger a push` (case-1 line) | **0** | the field-diff path never fires |
| `k8s match much id` / `consul match much id` (case-2) | **0** | |
| `atlas server pre delete` / `process discovery center instance deleting` (case-3) | **0** | |
| `Instance data inconsistency` notices | 0 | |
| `discovery center online k8s/ecs instances size` (k8s.go:337, consul.go:432) | **0 occurrences in the whole log** | the size log prints only past the early return — its total absence is direct proof the `len(list.Instance)==0` branch (k8s.go:318-322, consul.go:423-428) is taken on **every** tick |
| `retry trying to push instance` | 0 today (all 31,953 retry lines are 09-09/09-10, pre-fix binaries — the empty-ip DELETE 400 storm, already fixed) | the current binary is healthy |
| `wokderService sync failed` (nacos timeouts) | 0 today | |
| WARN noise: `invalid instance … nil env type` for coredns / local-path-provisioner | 1,014 each (~2 per tick) | system pods rejected by the filter on every `GetAll` pass — pure log noise (DS-5-7) |

**The 2-registers-per-tick-per-instance decomposition** (both executed-verified by window analysis at 09:53:47):
1. `CompareAndFlush` → empty remote list → `buildAndSendEvent` per instance → `worker.Handle(Sync)` → fanout `Push` → Atlas `SynInstance` (one `rsyncing instance` + one `synced … successfully` line) **and** nacos register (1st register).
2. `emitSyncAll` → `worker.Handle(SyncAll)` → fanout `PushAll` → Atlas `SynAllInstance` (no per-instance log) **and** nacos `PushAll` = `Push` (2nd register) + catalog prune (steady state: lists, no deletes).

**Live wire check (read-only GETs):** 3 services in DEFAULT_GROUP; `demo-order-service` k8s cluster: **8 hosts** in the catalog view (the 8 `demo-order-service` pods of the 10-pod k8s total — the remaining 2 pods are `demo-user-service`, the split §1 states correctly as order+user; the earlier draft of this line miscounted 10 by conflating the cluster with the provider total), every host's metadata = the 9 keys, `reversion`/`status` matching the pods' current values; `demo-pay-service` ecs cluster: catalog shows 2 hosts, one `enabled=true` (inst-2, status 1), one `enabled=false` (inst-1, status `"2"`, state `"probing"`) — and the instance/list view returns only inst-2. The nacos-side state is exactly what the push policy wrote; **the drift is not on the nacos side — it is the Atlas-side empty view driving the loop.**

**Conclusion of the live check:** the every-cycle symptom the user described is present and is caused by the remote view being structurally empty (World 0), not by the round trip. The round-trip asymmetries are real but dormant (they live in the never-taken case-1 path).

---

## 6. FINDINGS

### DS-5-1 (P0) — The periodic compare's remote view is structurally empty in every discoverymock-backed deployment; the compare degenerates to an unconditional full re-push twice per tick

**Evidence:**
- `internal/testkit/discoverymock/server.go:310-332` — `synInstance`/`synAllInstance` only record calls; `server.go:334-353` — `getAllInstance` filters `s.instances`, which only `SetInstances` (server.go:159-164) can populate.
- `build/atlasrun/main.go:12-22` — starts the TCP server, never calls `SetInstances`: the demo's Atlas serves an empty GetAll forever.
- `pkg/worker/fanout.go:216-218` — `GetAll` returns the first sink's view only (Atlas); `internal/server.go:301-318` — sink order Atlas-then-nacos; `pkg/worker/worker.go:99-101` — the provider's `worker.GetAll` delegates to it.
- `pkg/providers/k8s/k8s.go:318-322` and `pkg/providers/consul/consul.go:423-428` — the empty-list early-return push-all branch.
- Live: §5 — 2 registers per instance per tick, 0 case-1/2/3 lines, 0 occurrences of the `k8s.go:337` size log in 94k lines.

**Scenario:** the user watches nacos (or the spotter log) and sees every instance re-registered every 60 s with zero K8s changes — "the periodic sync reports changes EVERY cycle." With nacos upserts being idempotent the visible damage is bounded (log volume ~7.4k lines/day at 12 instances; register QPS; the `UpdateCluster` path is guarded by `healthCheckDone`), but the compare itself provides **zero protection** in this shape: no field drift, no delete, nothing is detected, because the diff input is an empty set. Any real bug that only the periodic compare would catch (the exact purpose of the periodic 3-diff) is invisible in the demo world.

**Fix design:** (a) make `atlasrun` an echo stand-in: after each `SynInstance`/`SynAllInstance`, seed the mock's `SetInstances` from the received payloads (with the mock's own upsert semantics, ~30 lines), so the demo's compare actually compares; (b) the structural fix is track 3's nacos-authoritative compare — this finding is its urgency proof, with the §4.2 items as co-fixes (of which §4.2-3, the `Cluster`/`Provider` fix, is the required one for the consul leg; §4.2-2, `wireEnabled`, is hardening today and required only if the consul status request is widened — see §4.2-2's positioning note); (c) near-term hardening: log a counter when the compare takes the empty-remote branch (a branch that fires every tick for a provider with N>0 local instances is a broken remote source, not a steady state).

### DS-5-2 (P1, conditional activation) — `Enabled` is asymmetric for the consul leg (local unconditional `true` vs wire forced `false` for status 2): under dsca-3 as written the residual is a **silent false-no-diff**; it becomes the every-cycle loop only if the consul compare's status request is widened

**Evidence:**
- `pkg/providers/consul/convertion.go:103` — `Enabled: true` hardcoded; `pkg/nacos/nacos.go:362-365` — `register` forces `enabled=false` when `Status==2`; `nacos.go:508` — `reconstruct` restores `Enabled = host.Enabled`.
- `pkg/providers/k8s/k8s.go:253` — `Enabled` is in the k8s diff set; `consul.go:453` — in the consul set.
- Executed S2/S8b: local `demo-pay-service-inst-1` (`Enabled=true, Status=2`) vs its own wire reconstruction (`Enabled=false, Status=2, reversion equal`) → `hasInstanceDiff = true`, driven solely by `Enabled` — **if the field diff is reached**.
- **The gating condition (revision of the original activation claim):** the consul compare's remote request is `[online]`-only (`consul.go:417`), and dsca-3's catalog-based `GetAll` rewrite **keeps** that request (its §3.3: "`[online]` is *sufficient* … No change required") — the rewrite's `statusAllowed([online], 2)` drops the reconstructed status-2 instance **before the field diff runs**. So catalog and list mask the pair alike for the consul leg (list at the HTTP layer, client.go:221-235; catalog+status-filter at the SDK layer), the pair degrades to remote-missing, and consul's case-2 branch — whose `Status==1` gate is `consul.go:469` (the k8s twin's gate is `k8s.go:358`) — rejects it: **no push, no log line: a SILENT FALSE-NO-DIFF, not a loop.**
- Live wire: the 18848 catalog holds inst-1 with `enabled=false`; the instance/list view hides it (client.go:221-235).
- Why it is invisible today: the compare reads the Atlas JSON, which carries `Enabled:true` (the override lives only in the nacos sink).

**Scenario (two branches):**
1. **dsca-3 as written (status request preserved `[online]`-only):** the status-2 ecs pair is silently dropped from the compare's remote view every cycle — the local truth (`Enabled=true` by policy, status 2) vs the wire state (`enabled=false`) is never asserted, but nothing loops. The cost is bounded: the pair is *already* what the push policy wrote, so the false-no-diff masks no real drift (the wire state is the intended one); the residual harm is observability (the asymmetry is undetectable from the compare) — flagged for DS-4 rather than fixed here.
2. **If the request is widened to `[online, unhealthy]` (k8s's shape, k8s.go:314) without the `wireEnabled` fix:** every status-2 ecs instance logs `the instance … is newer, trigger a push` every 60 s and re-registers — the user's every-cycle warning, materialized by the model asymmetry, indistinguishable from real churn in the logs.

**Fix design:** compare the derived wire value (§4.2-2): `wireEnabled(local) = local.Enabled && local.Status != 2` against `remote.Enabled`. Symmetric for both legs by construction; asserts exactly "the next push would write a different enabled than the remote holds." Positioned as hardening + conditional-activation co-fix (§4.2-2): not required to prevent a loop under dsca-3 as written; required the day the status filter widens. Contrast DS-5-3, whose activation has no such condition.

### DS-5-3 (P1) — `Cluster` is asymmetric for both legs (local `""`/label-derived vs reconstructed `clusterName`): the consul diff set compares it, so a nacos-source compare reports a diff every cycle for every ecs instance

**Evidence:**
- `pkg/nacos/nacos.go:507` — `reconstruct` sets `Cluster: cluster` from `host.ClusterName`; `pkg/providers/consul/convertion.go:101` — consul conversion sets `Cluster: ""`; k8s conversion derives it from pod labels/env (`conversion.go:432-456`, empty for the demo pods).
- `pkg/providers/consul/consul.go:452` — `Cluster` is in the consul compare's field set.
- Executed S7: local `Cluster=""` vs remote `Cluster="ecs"` with every other compared field equal → consul diff = true. The k8s set does not compare `Cluster` → false.

**Scenario:** same shape as DS-5-2 but affecting **every** ecs instance (healthy or not) the moment the ecs leg's compare reads the reconstructed view — a 100% false-diff rate for the provider. **Unlike DS-5-2, this activation is unconditional**: the status-1 reconstructions of healthy instances pass even the consul leg's `[online]`-only status request (consul.go:417, the shape dsca-3 preserves), so no filter shape masks it — this half of the headline stands as stated.

**Fix design:** §4.2-3 — compare `Provider` (round-trips exactly: `clusterOf` ↔ `clusterName`) instead of `Cluster`, or stop synthesizing `Cluster` in `reconstruct`.

### DS-5-4 (P2) — A forged or upward-edited `reversion` in nacos suppresses the update path: completely on the consul leg, only for pure-revision updates on the k8s leg

**Evidence (per leg, revision):** the update gates are `new.Reversion > old.Reversion` (k8s.go:249; consul.go:442) — but the *field-diff reachability* differs by leg:
- **k8s leg — suppression is partial, not complete.** `hasInstanceDiff`'s field branch (`k8s.go:251-253`) is a plain `else if` with **no revision-equality guard**: a forged-upward remote reversion falsifies only the strictly-higher gate; if *any* compared field drifts (the actual reason the local instance would want to push), the field diff still fires and the healing push rewrites the reversion (register is an unconditional upsert). Permanent suppression on the k8s leg is therefore confined to **PURE-revision updates** — a local instance whose only change is the reversion itself (the common case in practice, since `ResourceVersion` bumps on every object touch, often with no compared-field change).
- **consul leg — suppression is complete.** The field set is reachable only through the equal-revision guard (`consul.go:444`): forge the remote reversion upward and *both* branches are dead (strictly-higher false; `==` false) — no field diff, no push, forever.
- `parseInt64` failure (→0) is the opposite, benign case: it fires the gate once and the re-push rewrites the metadata (executed S4: one-shot).

**Scenario:** an operator "fixes" an instance's reversion in the nacos console to a large value. On the consul leg the instance can never be re-asserted until the source reversion surpasses the forged number (consul `ModifyIndex` — effectively forever). On the k8s leg the instance is suppressed only while its compared fields stay frozen — the first real field drift (label/env/ip change) heals it in one cycle; a pure `ResourceVersion` bump does not.

**Fix design (reconciled with dsca-3's R2 — LEAD RULING):** dsca-3 §3.3's rule **R2** ("Reversion is provider-owned monotonic state, not an authority token … *any* reversion mismatch — either direction — counts as drift, and the local value wins"; concretely: move `Reversion` into the compared set, `diff = fieldsDiffer(local, remote) || local.Reversion != remote.Reversion`) **is the remedy** — the first push after the forge rewrites the wire reversion to the local value, healing the suppression in one cycle, and the redesign dissolves this finding's gates entirely. **This track's original proposal (log/notice, do not push) is retracted as the remedy and retained as a companion anomaly signal:** a remote reversion *above* the provider's monotonic ceiling is reachable only by out-of-band means (manual console edit, or a second writer with a different source view — under leader election spotter is the single writer), so even as R2 heals the value, the event deserves a one-time log/notice per instance precisely because it witnesses something R2's push silently normalizes. Net position: R2 per dsca-3 (cross-reference: docs/dsca-3-reconcile.md §3.3, rule R2); this finding contributes the anomaly signal, not the remedy.

### DS-5-5 (P2) — Uncompared carried fields (`version`, `idc` on the k8s side) drift invisibly; `parseStatus`'s enabled→1 fallback classifies metadata-less entries as online

**Evidence:** the k8s diff set (`k8s.go:251-253`) excludes `Version`/`Idc` although both round-trip through metadata; the consul set includes `Idc` but not `Version`. `nacos.go:525-535` — fallback maps any metadata-less entry (manual console registration) to status 1 (executed S9a/S9c).

**Scenario:** a manual edit of `version`/`idc` metadata in nacos persists until the next real source change; a manually registered instance (no metadata) appears to the compare as an online remote instance and is correctly deregistered as remoteOnly (case 3) — acceptable, but the deletion of a manual registration deserves a distinct log line so operators can tell reconcile from mistake.

**Fix design:** document the intentional false-no-diff set (§4.3) in the plan; no code change required unless a consumer of `version`/`idc` drift detection appears.

### DS-5-6 (P2) — The domain diff policies are production-dead and already divergent from the provider copies

**Evidence:** `internal/domain/instance/rules.go:109-142` defines `DiffNewerReversion`/`DiffEqualReversion`; the only usages are `rules_test.go` (grep across the repo: no production call site). The provider copies diverged: k8s's `hasInstanceDiff` (`k8s.go:245-260`) = `DiffNewerReversion` **plus `Enabled`** (added by the hardening comment at `k8s.go:255`, never backported to the domain twin); consul's inline diff (`consul.go:439-457`) = `DiffEqualReversion`'s gates and fields verbatim. `CompareThreeWay` (rules.go:146-172) reimplements `CompareAndFlush`'s map dance with cleaner semantics and is unused.

**Scenario:** track 3 lands the nacos-source compare and — needing a diff policy — either imports the domain twin (silently dropping the `Enabled` hardening) or writes a third copy; three "k8s-compatible" policies now exist and drift independently.

**Fix design:** wire the providers' compares to the domain policies (one canonical `DiffPolicy` per provider flavor, the k8s one gaining `Enabled` officially), and make the domain policy the single place the §4.3 decision table lives.

### DS-5-7 (P2) — Filter rejection of system pods floods the log every tick (observability noise, not correctness)

**Evidence:** 1,014 WARN lines each for `coredns-6799fbcd5-fzqdp` and `local-path-provisioner-…` on 2026-09-11 (`invalid instance … nil env type`), ~2 per tick per pod from every `GetAll` pass (`k8s.go:429`, `k8s.go:186`).

**Scenario:** at 1000s of instances (track 1's scale world) the steady-state log becomes majority filter-rejection noise, masking real warnings.

**Fix design:** demote the per-instance filter rejection to debug in the full-list path (keep WARN on the event path), or aggregate (count + first-instance sample per tick).

### DS-5-8 (P2) — The every-cycle full re-push multiplies any per-instance sink error into a per-minute retry storm

**Evidence:** 09-09's log holds 29,325 `retry trying to push instance failed again, sink: nacos … Param 'ip' is required` lines — the pre-fix empty-ip DELETE loop amplified by the unconditional per-tick full push (DS-5-1's branch feeding the retry queue 12 entries per minute). The current binary shows 0 (the `pushOne` empty-ip skip and the permanent-drop classification fixed it), but the amplifier mechanism — full re-push every tick — is still live.

**Scenario:** any future per-instance nacos error (a 500-ingressing server, a metadata-size rejection) becomes 12×/minute queue churn per failing instance, at exactly the cadence DS-5-1 fixes.

**Fix design:** resolved structurally by DS-5-1's fix (the compare stops pushing everything when nothing differs); additionally the retry queue already drops Permanent() 4xx — keep asserting that property when touching the sink.

---

## 7. Boundaries of this audit

- The consul→nacos direction (ecs leg register policy) was analyzed for the compare's read side only; the consul path is out of scope per plan §User-requirements-2.
- The scratch executor (`/tmp/ds5repo/internal/ds5exec`) replicated the four unexported nacos functions verbatim rather than calling them (Go's internal-package rule); the replication was anchored three ways (§2.3) and the packages' own tests were run green. **The scratch workspace was transient and no longer exists** (it lived in `/tmp`); the executed verdicts persist as the fixture table in **Appendix A** (instance-pair fixtures + expected verdicts, independently re-verified against the quoted code during adversarial review). Re-validation procedure after any change to `metadataOf`/`reconstruct`/`parseStatus`/`parseInt64`/`hasInstanceDiff`/the consul inline diff: re-derive the verdicts from the quoted functions (`nacos.go:480-561`, `k8s.go:245-260`, `consul.go:439-457`) against Appendix A's fixtures, and re-run the packages' own tests (`go test ./pkg/providers/k8s ./pkg/nacos ./internal/domain/instance`) — no scratch executor is required.
- The demo stack was never touched: only read-only GETs to 127.0.0.1:18848 (instance/list, catalog/instances, service/list) and log greps; no reserved ports were bound; no `make test-soak`.
- `HealthState`, `Disk`, `Os`, `Level` are confirmed dead weight in the model (never written by any conversion, never compared, never carried) — flagged for the eventual model slimming, not a stability finding.

---

## Appendix A — Scenario fixtures and expected verdicts (re-validation without the scratch executor)

The scratch executor (`/tmp/ds5repo`) was transient and no longer exists; this table persists what it established: the instance-pair fixtures and the expected diff verdicts. Diff functions: `k8sDiff(remote, local)` = `hasInstanceDiff` (`k8s.go:245-260`); `consulDiff(remote, local)` = the inline diff (`consul.go:439-457`), both in the `CompareAndFlush` direction (k8s.go:346). Shared fixture base (fields not listed are equal on both sides): `InstanceId`, `Ip`, `EnvType="test"`, `EnvGroup=""`, `State="running"`, `Status=1`, `Enabled=true`, `Reversion=38491` (S2/S8b use 308), `AppCode`, `Cpu=0`, `Idc`, `Provider`, `Cluster=""` on local. Wire-side base (what register writes for the shared base): `enabled=true`, metadata 9 keys (`instanceId, envType, envGroup, reversion, status, state, idc, cpu, version`), `clusterName=Provider`, `port=Ports[0]`, `serviceName=AppCode`. Verdicts were executed by the scratch executor (§2.3 anchors) and independently re-verified against the quoted functions during adversarial review.

| # | Fixture delta vs shared base (local / wire) | Reconstructed remote delta | k8sDiff | consulDiff | Perpetual? / mechanism |
|---|---|---|---|---|---|
| S1 | consul healthy live pair (`demo-pay-service-inst-2`): no delta | no delta (wire `enabled=true`, metadata `status:"1"`) | false | false | — steady state quiet |
| S8 | k8s live pair `demo-order-service-676b75dfb7-n2l6w` (`Ip=10.42.0.25`, `Port=7096`, `Reversion=38491`, metadata `cpu:"0"`, `idc:""`, `version:""`) | exact round trip (metadata 9 keys) | **false** | false (k8s pair, consul set not consulted) | — steady state quiet |
| S2/S8b | consul unhealthy live pair `demo-pay-service-inst-1` (`Ip=127.0.0.1`, port 8848, `Reversion=308`): local `Enabled=true` (convertion.go:103), `Status=2`; wire `enabled=false` (register override, nacos.go:362-365), metadata `status:"2"`, `state:"probing"` | `Enabled=false`, `Status=2`, `Reversion=308`, `Cluster="ecs"` (synthesized, nacos.go:507) | **true** — driven solely by `Enabled` | **true** if reached (`Cluster` + `Enabled`) — **unreachable under dsca-3 as written**: consul's `[online]`-only request (consul.go:417) drops the status-2 reconstruction before the diff | conditional (see DS-5-2): loop only under a widened status request; otherwise silent false-no-diff |
| S3 | `Cpu` float32 sweep: 0, 2, 0.1, 1.5, π, 123456789, 0.123456789, 16777217; wire metadata `cpu` = shortest float32 string | parse → exact float32 equality for all 8 (e.g. `123456789 → "123456790" → 123456789`) | n/a (k8s set excludes Cpu) | false | — lossless as metadata |
| S4 | local `Reversion=38491`; wire metadata `reversion` wiped/malformed | `Reversion=0` (parseInt64 failure) | **true** (strictly-higher) | **true** (strictly-higher) | no — one-shot: re-push rewrites metadata |
| S5 | local `Ports=[7096,80,9090]`; wire `port=7096` only | `Ports=[{7096}]` | false (ports not in any diff set) | false | — architectural no-diff |
| S6 | local `EnvCode="test#"`; never written to wire | `EnvCode=""` | false (not in any diff set) | false | — derivable: `ComposeEnvCode` |
| S7 | local `Cluster=""`; wire `clusterName=k8s`/`ecs` | `Cluster="k8s"`/`"ecs"` (nacos.go:507) | false (not in the k8s set) | **true** (`Cluster`, consul.go:452) | **YES — unconditional** (status-1 passes `[online]`): DS-5-3 |
| S9a | local `Status=1`; wire metadata `status` absent, `enabled=true` | `Status=1` (fallback enabled→1, nacos.go:525-535) | false | false | — fallback agrees |
| S9b | local `Status=1, Enabled=true`; wire metadata `status` absent, manual console disable (`enabled=false`) | `Status=2` (fallback else→2), `Enabled=false` | **true** | **true** | no — self-healing re-push re-enables |
| S9c | local `Status=1`; wire metadata `status="online"` (non-numeric), `enabled=true` | `Status=1` (fallback) | false | false | — falls back to enabled-based |

Re-validation after any change to `metadataOf`/`reconstruct`/`parseStatus`/`parseInt64` (`nacos.go:480-561`), `hasInstanceDiff` (`k8s.go:245-260`), or the consul inline diff (`consul.go:439-457`): re-derive this table's verdicts from the quoted functions against these fixtures and re-run `go test ./pkg/providers/k8s ./pkg/nacos ./internal/domain/instance`.
