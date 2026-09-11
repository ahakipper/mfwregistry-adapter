# DSCA Track 3 — Reconcile Correctness vs Nacos

**Auditor:** DS-3 (reconcile correctness vs nacos)
**Repo:** `spotter`, branch `refactor/all`, HEAD `69b0105`
**HEAD note:** audited at `69b0105`; the current HEAD `0ff71f7` is docs-only (it adds the five DSCA phase-1 documents and nothing else) — the code under audit is identical between the two.
**Method:** full code reading of the three reconcile surfaces + read-only live probes against the demo nacos at `127.0.0.1:18848` (GETs only: `service/list`, `instance/list`, `catalog/instances`, `catalog/services`) + read-only `kubectl get pods` against the demo k3s + analysis of the running spotter's own log (`build/demo/app.log`, PID 30306, `--push-interval 60`). No repo files were modified; the demo stack was not touched.

---

## 1. EXECUTION SUMMARY

**The verdict, in one paragraph.** The periodic 3-way diff (both providers' `CompareAndFlush`) reads its "remote" view through `worker.GetAll` → `FanoutSink.GetAll` → **the first sink only = Atlas** (`pkg/worker/fanout.go:216-218`, plan §6.3 v1). In the live demo Atlas is `build/atlasrun` — the `discoverymock` stand-in whose `GetAllInstance` returns instances from an in-memory list that **no code ever populates** (`internal/testkit/discoverymock/server.go:334-353`; `build/atlasrun/main.go:12-22` never calls `SetInstances`). So in the demo the diff's remote view is **always empty**, `CompareAndFlush` always takes its "remote empty → push everything" branch, and the 3-way diff is effectively dead code: measured live, **12 Atlas `SynInstance` RPCs and 24 nacos registers per 60-second tick** (10 k8s + 2 ecs instances, each registered twice: once by the degenerate push-all, once by `PushAll`'s upsert pass) — 20,888 nacos registers in the current log. The absolute numbers are window-dependent snapshots: the register counter grows monotonically while the demo runs (20,888 at audit time, 22,096 at review time) and the per-tick count scales with pod count; the stable invariant is **2.00 registers per instance per tick** (24 = 12 instances × 2 paths). Nacos is reconciled today only by the `PushAll` prune: id-set level, catalog-based, no field diff. The user's requirement — the reconcile comparison must treat nacos as the authoritative external view — is therefore unmet on both axes: the diff consults a fake store, and the only nacos-side mechanism can neither see nor heal field-level drift.

**The design, in one paragraph.** Make the diff source a *designated* reconcile sink instead of the hardcoded first sink: `FanoutSink` gains a `reconcile` designation (default nil = primary = Atlas, production unchanged), selected by a new `--reconcile-source` flag; `internal/server.go` wires `--reconcile-source nacos` to the nacos sink. **Zero changes to the `ports.InstanceSink` signature, the `Worker` interface, or any provider call site** — the provider tag already rides the existing `GetAll(statuses, provider)` signature, and nacos-side scoping is `clusterName == provider` (k8s compares `(svc, k8s)` pairs, consul `(svc, ecs)`). Two prerequisites inside the nacos sink: (1) `GetAll` must switch from `instance/list` (which **hides `enabled=false` instances — proven live in this audit**) to the catalog view the prune already uses; (2) `reconstruct` must stop writing `Cluster` from `clusterName` (both converters leave `Cluster` empty; consul's diff compares `Cluster`, so the current reconstruct would false-positive every cycle). The entire k8s `hasInstanceDiff` compared set (status/state/envType/envGroup/ip/instanceId/enabled/reversion) **round-trips losslessly** — demonstrated below — so the k8s diff needs no model changes at all. The consul leg has exactly one model-level asymmetry — `Enabled`: the converter hardcodes local `true` (convertion.go:103) while the wire forces `false` for every status-2 instance (nacos.go:362-364), and `Enabled` is in the consul compared set (consul.go:453) — so the consul compare must evaluate the wire projection `wireEnabled(local) = local.Enabled && local.Status != 2` instead of the raw field (DS-5-2; the rule, its guards, and its pins are §3.3). The prune stays: the field diff heals same-id drift, the prune heals changed-id drift; neither subsumes the other.

**Round-trip demonstration (executed, live).** For `demo-order-service-676b75dfb7-n2l6w` I took the actual local view (the gRPC push payload logged by the running spotter at 09:55:47.881) and the actual remote view (the live nacos catalog host `10.42.0.25#7096#k8s#DEFAULT_GROUP@@demo-order-service`), and ran `reconstruct()`'s mapping by hand:

| Instance field | Local (K8s source, from the live push payload) | `reconstruct()` from live nacos | Round-trips? | In k8s `hasInstanceDiff` compared set? |
|---|---|---|---|---|
| `InstanceId` | `demo-order-service-676b75dfb7-n2l6w` | metadata `instanceId` = same | yes | yes |
| `Ip` | `10.42.0.25` | host `ip` = `10.42.0.25` | yes | yes |
| `Status` | `1` | metadata `status` = `"1"` (parseStatus) | yes | yes |
| `State` | `running` | metadata `state` = `"running"` | yes | yes |
| `EnvType` | `test` | metadata `envType` = `"test"` | yes | yes |
| `EnvGroup` | `""` | metadata `envGroup` = `""` | yes | yes |
| `Enabled` | `true` | host `enabled` = `true` | yes — and the k8s leg is symmetric *by construction*: k8s's local `Enabled` derives from the same pod readiness that drives status (`formatContainerEnabled`, conversion.go:295-307 — phase Running + containers ready + no DeletionTimestamp → enabled), so a local status-2 pod is locally `enabled=false` and matches the wire's forced `false` (nacos.go:362-364). The consul leg is asymmetric (local hardcoded `true`, convertion.go:103) — DS-5-2, handled as a compare rule in §3.3 | yes |
| `Reversion` | `38491` (= pod resourceVersion) | metadata `reversion` = `"38491"` | yes | yes |
| `Cpu` | `0` (float32) | metadata `cpu` = `"0"` → 0 | yes (shortest-float encoding is exact) | no (deliberately excluded, k8s.go:254 comment) |
| `Idc` / `Version` | `""` / `""` | metadata `idc` / `version` = `""` | yes | no / no |
| `AppCode` | `demo-order-service` | the `service` param of reconstruct | yes | no (keyed, not compared) |
| `Provider` | `k8s` | host `clusterName` → `Provider` | yes | no |
| `Cluster` | `""` (no `cluster` label, no `K8S_CLUSTER_NAME` env) | host `clusterName` → **`"k8s"`** | **NO — mismatch** | no (k8s) — **but yes (consul, consul.go:452)** |
| `Ports` | `[{dubbo-7096, dubbo, 7096}]` | first port only: `7096` | first-port only | no |
| `Image` | `{"nginx":"nginx:alpine"}` | — | **lost** | no |
| `Label` | `{compatibility:aos_*…}` | — | **lost** | no |
| `Memory` | `0` | — | **lost** | no (excluded: int-typed storage) |
| `Hostname` | pod name | — | **lost** (== InstanceId, recoverable in principle) | no |
| `EnvCode` | `test#` (derived) | — | lost (derived) | no |

Conclusion: **every field the k8s diff compares survives the round trip; the losses (multi-port, Image, Label, Memory, Hostname) are exactly the fields the k8s diff already does not compare.** The one trap is `Cluster` (consul-side, see finding DS-3-4). Two supporting live facts: the demo pod declares no container ports, so the single port is the synthetic `dubbo-7096` that `formatAppPort` always prepends (pkg/providers/k8s/conversion.go:174-179) — meaning **the k8s composite id's port component is the constant 7096** (confirmed: all 8 live instanceIds end `#7096#k8s#`), and k8s identity drift can only come from an ip change; and the unhealthy shape round-trips too (live `demo-pay-service-inst-1`: metadata `status "2"`, `enabled:false`, `state "probing"` → reconstruct gives Status 2 / Enabled false / state probing, matching the push policy of nacos.go:362-364).

**Live proof of the F8 asymmetry (why the diff must read the catalog, not the instance list).** Against the live demo nacos, read-only:

- `GET /nacos/v1/ns/catalog/instances?serviceName=demo-pay-service&clusterName=ecs` → **2 hosts** (`count: 2`), including `127.0.0.1#8848#ecs` with `enabled: false`, metadata `status: "2"`.
- `GET /nacos/v1/ns/instance/list?serviceName=demo-pay-service` → **1 host** — the `enabled=false` instance is hidden.

The existing nacos `Sink.GetAll` uses `ListInstances` (pkg/nacos/nacos.go:296) — the hiding view. Any nacos-source diff built on today's `GetAll` would misread every spotter-pushed-unhealthy instance as "absent from nacos".

**Catalog endpoint behaviors (live, read-only), for the design's walk discipline:** `catalog/instances` **requires** `clusterName` (HTTP 500 without it); a `(service, cluster)` pair with no entry answers `500` with body `caused: cluster bogus is not found!;` / `caused: service DEFAULT_GROUP@@no-such-svc is not found!;` — both match the existing `isCatalogNotFound` helper (pkg/nacos/nacos.go:272-278), so the diff's walk tolerates absent pairs exactly like the prune does (nacos.go:244-247).

**Findings:** DS-3-1 (P1) Atlas-as-diff-source — the diff verifies a fake store and is dead code in the demo; DS-3-2 (P1) field-level nacos drift (console edit / enabled flip / API delete) has no heal path in production; DS-3-3 (P1) the would-be nacos diff source reads the view that hides disabled instances; DS-3-4 (P2) `reconstruct` `Cluster` mismatch would make the consul compare false-positive every cycle; DS-3-5 (P2) consul case-3 `Status=2` heals ecs ghosts into permanent disabled zombies instead of deregistering; DS-3-6 (P2) k8s `CompareAndFlush` treats a `GetAll` error as "push everything" (burst hazard with a nacos source at scale); DS-3-7 (P2) the boot-time total-empty-source residual (the 416e62a residue, shrunk but not eliminated by the design).

---

## 2. CURRENT-STATE AUDIT

### 2.1 The three reconcile surfaces

#### (a) k8s `CompareAndFlush` (pkg/providers/k8s/k8s.go:300-391)

**When it runs.** Once at boot — `monitor()` blocks until `k.robot.HasSynced()` (k8s.go:88-97), then starts the interval goroutine and calls `CompareAndFlush()` synchronously (k8s.go:99-101), *before* the watch goroutine starts (k8s.go:104-130) — and then on every push-interval tick via `ProcessIntervalFullPush` (k8s.go:443-464, `--push-interval 60` in the demo; default 21600s), which runs `CompareAndFlush()` then `emitSyncAll()` per tick.

**The diff source.** `k.worker.GetAll([]int32{InstanceStatusOnline, InstanceStatusUnhealthy}, ProviderK8s)` (k8s.go:314). `worker.GetAll` delegates to `pusher.GetAll` (pkg/worker/worker.go:99-101); the pusher is the `FanoutSink`; `FanoutSink.GetAll` returns `f.sinks[0].Sink.GetAll(...)` — **the first sink, Atlas, by construction** (pkg/worker/fanout.go:216-218, "the v1 semantics of plan §6.3"). The nacos sink's `GetAll` (nacos.go:289-312) is reachable only through that fanout, and is therefore **never called in any deployed configuration** (its only callers are tests). The comment on the sink says it plainly: "v1 comparisons never run against the Nacos view (plan §3.3)" (nacos.go:287-288).

**What it does with the Atlas view — the three cases** (after refilling the local cache, k8s.go:302-311):

- **Empty/error remote** (k8s.go:318-322): if the remote list is nil/empty — *including the error path, which only logs* (k8s.go:315-317) — push every local instance (`buildAndSendEvent` per instance, an `OperateTypeSync` event through the whole fanout: Atlas `SynInstance` + nacos register). **This is the branch the live demo takes every single tick**, because the demo's Atlas is the `discoverymock` whose instance set is never populated.
- **Case 1 — both exist** (k8s.go:345-353): `hasInstanceDiff(servIns /* Atlas view */, k8sIns /* local */)` (k8s.go:245-260): equal-and-offline → no diff; `new.Reversion > old.Reversion` → diff; else field diff on {EnvType, State, Status, EnvGroup, InstanceId, Ip, Enabled} → diff. Diff → push the **k8s** instance (k8s data wins), set the `bothExist` notice flag. On diff, also `delete()` from both maps so the leftovers are the set-differences.
- **Case 2 — local only** (k8s.go:355-363): push the local instance **only if `Status == 1`** (online); unhealthy local instances are silently not pushed here.
- **Case 3 — remote only** (k8s.go:375-389): for every Atlas-view instance with no local counterpart: force `Enabled=false, Status=3, State=terminated` on the *Atlas-reported* instance and push it → the nacos sink maps Status 3 to `deregister` (nacos.go:324-337). Because the case-3 instance comes from the remote view, it carries the remote's ip — against the Atlas view that is Atlas's stored ip; against nacos it would be the live host ip (see the design: strictly better-formed).

**The `PushAppCodes` filter** (k8s.go:323-333, on the remote list; the local list is filtered at conversion, conversion.go:68-75): when `--push-appcodes` is set, the remote list is filtered to those appcodes *before* the set difference — so remote instances of other appcodes are never marked offline (pinned by `TestCompareAndFlushPushAppCodesFiltersRemoteList`, k8s_whitebox_test.go:1057-1099). This scoping is what keeps a spotter that only owns some appcodes from deleting everyone else's instances; it transfers unchanged to a nacos view because it operates on whatever list the seam returns.

**The notice paths** (k8s.go:365-372, 385-388): `notice.Notice("Instance data inconsistency", …)` per case class — case 1 "same instances, fields differ", case 2 "instances exist in K8s but not in the discovery center", case 3 the reverse. In the live demo **none of these ever fire** (the empty branch returns before them; the log line `discovery center online k8s instances size :%d` at k8s.go:337 never appears in `build/demo/app.log` — grep-verified, which is direct evidence the non-empty diff branch never executes).

#### (b) consul `CompareAndFlush` (pkg/providers/consul/consul.go:400-500)

Same shape, three differences that matter:

- **Boot path**: `Run()` fires `go c.CompareAndFlush()` asynchronously (consul.go:93) alongside the monitor; it holds the provider lock across the whole compare, including the `worker.GetAll` RPC (consul.go:401-402, 417).
- **Error semantics**: a `GetAll` error **early-returns** (consul.go:418-421) — conservative, unlike k8s's push-everything. The empty-remote branch (consul.go:423-428) pushes all local instances like k8s.
- **The diff set and the case-3 semantics**: consul asks `GetAll([InstanceStatusOnline], ProviderEcs)` (consul.go:417) — status filter is **[online] only** (k8s asks [online, unhealthy]). Case 1 compares `Reversion` first, then — only when reversions are **equal** — a wide field set {EnvType, EnvGroup, Status, State, Ip, **Idc**, **Cluster**, Enabled, AppCode, **Cpu**} (consul.go:444-457). Case 2 pushes local online instances (consul.go:469-472). Case 3 sets **`Status = 2`** on the remote-only instance and pushes it (consul.go:488-493) — the "old instance, mark not-ready" Atlas semantics. Against the nacos sink, a Status-2 push is an *upsert with `enabled=false`* (nacos.go:362-364), **not a deregister**: an ecs instance deleted while spotter was down heals to a permanent disabled zombie (finding DS-3-5).

#### (c) the nacos `PushAll` prune (pkg/nacos/nacos.go:161-266)

The only nacos-authoritative reconcile that exists. Per provider full push (`emitSyncAll` → `OperateTypeSyncAll` → worker's SyncAll handler → `FanoutSink.PushAll` → `nacos.Sink.PushAll`):

- **The catalog listing**: for every `(serviceName, clusterName)` pair, `ListCatalogInstances(service, cluster)` — the ADMIN catalog view, paginated, with the count-based stop (pkg/nacos/client.go:250-286). The catalog is used precisely because `instance/list` hides `enabled=false` (the F8 lesson; live-proven again in §1). A pair with no catalog entry answers 500 "… is not found" and is treated as empty (nacos.go:244-247).
- **The desired-set semantics**: desired = pushed instances with `Status != offline`, keyed by **composite id** `ip#port#cluster#group@@service` (nacos.go:191-205, 474-476). Offline (Status 3) pushed instances get an *empty* desired entry — their pair is still pruned even if the per-instance deregister failed. Remote hosts of the pair whose composite id is not in desired → `deregister` (nacos.go:253-263), with a `host.ClusterName != key.cluster` guard so one provider never prunes another's cluster (nacos.go:254-256).
- **The remembered-pairs scoping (the 416e62a fix)**: `remembered` records every pair ever pushed non-offline (nacos.go:214-217); the sweep union = desired pairs ∪ remembered pairs **of clusters present in this push** (nacos.go:218-231) — a remembered pair of the pushed cluster whose desired set went empty (a service whose every instance vanished upstream) is swept to empty-desired = prune everything remote. Pairs of clusters not in the push belong to another provider and are never touched (the live delete/re-register flapping incident that 416e62a fixed). The memory is process-local: it dies with the process and rebuilds from the next register.
- **The empty-push no-op**: an empty pushed list carries no cluster, therefore no provider identity (`worker.Event` has no provider field), and sweeps nothing (nacos.go:58-76, 218-231) — the conservative door that keeps one provider's empty SyncAll from deleting every other provider's instances. The deliberate residual (nacos.go:70-83): a provider whose *entire* source list is empty does not auto-heal its remote registrations.

**What the prune can and cannot do.** It is an **id-set** reconcile: it deletes remote instances whose composite id is not in the desired set. It never compares fields — a remote instance with the right id and wrong metadata/enabled/status is *kept as-is*. It also cannot heal a remote instance that is *missing* (that is an ADD, which only a push does), and its delete coverage for changed identity is exactly its strength: when an instance's ip changes, the push registers the *new* composite id while the *old* registration remains (upsert by id); the prune is the only thing that deletes the old id.

### 2.2 The gap map: "reconcile against nacos as the authoritative store" vs what exists

The three diff kinds, and where the nacos view participates today:

| Diff kind | Nacos view's participation today | Heal status |
|---|---|---|
| **ADD** (local has, remote lacks) | None directly — but covered *by accident of architecture*: any missing instance is pushed by case-2 / the empty-remote branch, and the upsert lands in nacos too. | Covered in both worlds (it never mattered which store the view came from for ADDs). |
| **DELETE** (remote has, local lacks) | Indirect only: case-3 consults the **Atlas** view's ghosts; the nacos ghost set is reconciled by the **prune** (id-set, remembered-scoped) — **only pairs in the desired ∪ remembered-remembered-of-pushed-clusters set**. | Prod (real Atlas): healed by case-3 (Atlas still has the ghost) + prune. **Demo (fake Atlas): case-3 never fires (view always empty); only the prune runs, and its remembered map is empty at boot** — a service that vanished while spotter was down is not healed until any instance of the pair re-registers (the 416e62a residual). |
| **UPDATE** (both have, field-level) | **Never.** `hasInstanceDiff` is fed the Atlas view exclusively. | **Prod: no heal at all.** Demo: healed only as a side effect of the Atlas-empty degeneracy (push-all re-registers overwrite nacos metadata each tick). |

Per drift class, with the current heal status (demo vs production-with-real-Atlas):

| Drift class | Today, demo (fake Atlas) | Today, production (real Atlas) | With the nacos-source design |
|---|---|---|---|
| Manual nacos **metadata edit** (console/API) | Heals ≤1 interval, *by accident* (degenerate push-all overwrites metadata on re-register) | **Never heals** — Atlas view unchanged → no diff; prune only deletes; no re-register without a local pod event | Heals ≤1 interval: field diff → push local (re-register rewrites metadata) |
| Manual nacos **enabled flip** (console disable) | Heals ≤1 interval by accident | **Never heals** — and is silently *permanent*: spotter never re-asserts `enabled` | Heals ≤1 interval: `Enabled` is both pushed and reconstructed → diff → push |
| Manual nacos **instance delete** (API) | Heals ≤1 interval by accident (push-all re-registers) | **Never heals** until a local pod event produces a diff push | Heals ≤1 interval: ADD (local has, nacos lacks) → push |
| Manual nacos **instance add** (extra id in a spotter-owned pair) | Pruned ≤1 interval (id not in desired → DELETE) | Same (prune) | Same (prune) **plus** case-3 deregister via the nacos view |
| **Mid-flight DELETE lost** (k8srobot queue drop-on-full) | Healed next tick: informer `List` lacks the pod, remote view has it → case-3 | Same | Same, via the nacos view |
| **Downtime drift: pod deleted while spotter down** | After restart: case-3 dead (Atlas empty); prune sweeps it only if the pair is remembered — it is not, at boot → **not healed until re-register** | Case-3 fires (Atlas has the ghost) → Status-3 push → deregister; prune as backstop | **Healed at boot**: nacos view has the ghost, local lacks it → case-3 → deregister (see §4) |
| **Downtime drift: whole service vanished while spotter down** | Not healed (416e62a residual: remembered empty at boot, Atlas empty) | Healed by case-3 (real Atlas has the ghosts) | **Healed at boot** via case-3 from the nacos view |
| **Downtime drift: new pods while spotter down** | Healed at boot (case-2 / empty branch pushes all) | Same | Same |
| **Provider source totally empty at boot** | No heal (len>0 guard, deliberate) | Same | Same — the accepted residual (DS-3-7) |

The Atlas dependency, stated as the user put it: the comparisons are against a stand-in, so **the demo's reconcile verifies nothing** — its only "verification" is the accident that an empty view means "push everything", which both hides drift classes (UPDATE/DELETE never evaluated) and burns a full re-push every tick (24 nacos registers/minute measured). In production the same code verifies the *wrong* store: Atlas may be perfectly consistent while nacos — the store the system is being verified against — has drifted invisibly.

---

## 3. THE DESIGN — Nacos-Authoritative Reconcile

### 3.1 Requirement (e) first: the seam, because everything else hangs off it

**The minimal port change is: no port change.** `ports.InstanceSink.GetAll(statuses []int32, provider string)` (internal/ports/ports.go:55-59) already carries the provider tag, and both providers already pass it (`ProviderK8s` / `ProviderEcs`). Per-provider scoping of the nacos view is `clusterName == provider` inside the sink — no routing state needs to live in the port. A `GetAllView`-per-provider construction would duplicate routing information the signature already carries; it is rejected.

**The change is one routing decision inside `FanoutSink`:**

```go
// pkg/worker/fanout.go
type FanoutSink struct {
    sinks    []NamedSink
    reconcile *NamedSink   // nil = primary (first sink) — the v1 default
    logger   ports.Logger
}

// SetReconcileSource designates the sink whose view GetAll returns.
// Empty/nil resets to the primary. Unknown name → error.
func (f *FanoutSink) SetReconcileSource(name string) error

// GetAll returns the reconcile-designated sink's view; nil designation
// keeps the v1 primary semantics (production unchanged).
func (f *FanoutSink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
    if f.reconcile != nil {
        return f.reconcile.Sink.GetAll(statuses, provider)
    }
    return f.sinks[0].Sink.GetAll(statuses, provider)
}
```

**Config and wiring:**

- `internal/infra/config`: `Flags.ReconcileSource string` + `Config.ReconcileSource string` (plain string; empty = default = primary; mirrors `NacosAddr`'s "the empty default IS the legal value" pattern, config.go:174-177 / 250-253).
- `cmd/adapter.go`: `--reconcile-source` flag next to `--nacos-addr` (cmd/adapter.go:123-126), `ReconcileSource: flagString(cmd, "reconcile-source")` at the Flags construction (cmd/adapter.go:159).
- `internal/server.go startProviders` (server.go:301-326): after the fanout is built, `if s.cfg.ReconcileSource == nacos.SinkName { require nacosSink != nil; fanout.SetReconcileSource(nacos.SinkName) }`; any other non-empty value errors at startup (fail fast, same discipline as `NewFanoutSink`'s name validation).

**Migration path — the design must work for both worlds, and it does by default:**

- **Demo** (Atlas = fake): add `--reconcile-source nacos`. Atlas remains registered and still receives every push (harmless; it is the recording mock); only the *read* view designation flips to nacos. The degenerate push-all disappears — the nacos view is non-empty, so the diff branch finally runs; steady-state churn drops from 24 registers/minute to ~zero; the three "Instance data inconsistency" notices become live signals.
- **Production** (Atlas = the real discovery center, still the primary *push* target): no flag → `reconcile == nil` → `GetAll` returns the primary's view → behavior byte-identical to today (the §6.6 degeneration guarantee extended to the read path). When production's authoritative external store becomes nacos — the direction the user's requirement points — the same flag flips it with no code change; Atlas remains a push-only sink until it is retired.

### 3.2 Requirement (b) + (d): the nacos view — catalog-based, provider-scoped, reconstruct fidelity

**`Sink.GetAll` must be rewritten to the catalog view.** Today it walks `ListServices` then `ListInstances` per service (nacos.go:289-312) — the view that **hides `enabled=false`** (client.go:219-221; live-proven in §1: pay-service shows 1 host in `instance/list`, 2 in the catalog). A diff built on it would misread every spotter-pushed-unhealthy instance as "absent" → spurious ADD pushes every cycle (and would never see console-disabled instances at all — the exact heal the design exists to provide). The rewrite:

```go
// pkg/nacos/nacos.go
func (s *Sink) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
    services, err := s.client.ListServices(100)          // paginated, unchanged
    if err != nil { return nil, err }
    instances := []*instance.Instance{}
    for _, service := range services {
        // clusterName == provider: the per-provider/cluster scoping.
        // Absent pair → 500 "is not found" → isCatalogNotFound → skip (empty),
        // exactly like the prune (nacos.go:244-247).
        hosts, err := s.client.ListCatalogInstances(service, provider)
        if err != nil {
            if isCatalogNotFound(err) { continue }
            return nil, err                               // any other error aborts the view
        }
        for _, host := range hosts {
            ins := reconstruct(service, host)
            if !statusAllowed(statuses, ins.Status) { continue }
            instances = append(instances, ins)
        }
    }
    return &instance.InstanceList{Instance: instances}, nil
}
```

Why this satisfies the requirements:

- **(d) Comparison set = the catalog view**: `enabled=false` instances are visible. The status classification still works because `parseStatus` prefers the metadata `status` string and only falls back to `enabled` (nacos.go:525-535) — so a console-disabled healthy instance (metadata `status "1"`, `enabled:false`) reconstructs as `Status=1, Enabled=false`: included in the k8s `[online, unhealthy]` request, and *diffable on `Enabled`* → the console-disable heal. A spotter-pushed-unhealthy instance (metadata `status "2"`, enabled false) reconstructs Status 2 — included, and steady (no diff) against a local status-2 instance.
- **(a) Per-provider/cluster scoping**: the walk passes `clusterName=provider`, so k8s's compare sees exactly the `(svc, k8s)` pairs and consul's the `(svc, ecs)` pairs. This is *stricter* than the Atlas view, whose provider scoping depended on the remote's `Provider` field being what spotter pushed; here the scope is nacos's own cluster identity. A k8s compare can therefore never case-3 an ecs instance (the diff-level twin of the 416e62a prune scoping). `catalog/instances` requires `clusterName` (live-verified: 500 without it), which the walk always supplies.
- **Cost**: one `ListServices` page walk + one catalog page walk per service per tick — batched per pair, not per instance (the prune already does the identical walk for its pairs; the diff roughly doubles per-tick GET volume — bounded by #services, acceptable at v1; a shared per-tick catalog snapshot is the scale-phase follow-up, noted for tracks 1/2).

**`reconstruct` fidelity — what must be fixed, what is already sound:**

- **Fix (required): `Cluster` must not be set from `clusterName`** (nacos.go:507). Both converters leave `Cluster` empty (k8s: `formatCluster` reads a `cluster` label / `K8S_CLUSTER_NAME` env, absent in the demo — conversion.go:432-456; consul: hardcodes `Cluster: ""`, convertion.go:101), while `reconstruct` writes `Cluster = clusterName`. The k8s diff does not compare `Cluster`, but **consul's does** (consul.go:452) — as-is, a consul-vs-nacos compare would report a diff *every cycle forever* (the exact false-positive loop the user warns about; DS-5's territory, DS-3-4 here). The fix is a fidelity *correction*: the metadata never carried `Cluster`; `clusterName` already lands in `Provider` (which reconstruct sets at nacos.go:506); leaving `Cluster` "" makes the round-trip honest.
- **Sound, verified field by field** (the §1 table): the entire k8s `hasInstanceDiff` set — `InstanceId` (metadata), `Ip` (host), `Status` (metadata+`parseStatus`, fallback enabled→1/else→2, never 3 — correct, since spotter deregisters offline instances instead of registering them status-3), `State`/`EnvType`/`EnvGroup` (metadata), `Enabled` (host), `Reversion` (metadata, `parseInt64` with a 0 default — garbage metadata degrades to 0, which loses the comparison to any real local reversion and heals by push). `Idc` and `Cpu` round-trip too (consul's equal-revision set compares both; `metadataOf`'s shortest-float `cpu` encoding is exact for float32). `AppCode` reconstructs as the service name, which is what spotter registered (`serviceName = ins.AppCode`).
- **Structurally uncompared, structurally lost**: multi-port (reconstruct builds a single `PortInfo` from `host.Port`, nacos.go:505), `Image`, `Label`, `Memory`, `Hostname`, `EnvCode`. These are outside both providers' compared sets today; the design does not widen the compared set to include them (it cannot — the round trip cannot express them). Not a regression; documented as the model's boundary (DS-5 owns the deep dive).
- **A k8s-specific identity note from the live data**: `formatAppPort` always prepends the synthetic `dubbo-7096` port (conversion.go:174-179), so k8s `firstPort` is the constant 7096 — the k8s composite id can only change via ip. Identity drift for k8s = ip change; the prune owns deleting the old ip's registration (the push registers the new id only).

### 3.3 Requirement (c): diff semantics and precedence rules

The compare loop itself is unchanged in shape (both providers keep their `ListToMap` set difference); what changes is the *meaning* of the remote operand and four explicit rules. The domain package already has the scaffolding (`CompareThreeWay` + `DiffPolicy`, internal/domain/instance/rules.go:146-172) that the providers shadow inline — the fix phase should collapse the inline copies onto it (plan line: "Inline diff policies shadow the domain rules").

**R1 — Local is the source of truth.** For provider-owned pairs (the `(svc, cluster)` scope the provider pushes), the informer/consul-catalog view is authoritative; nacos is a cache to converge. Every diff outcome pushes the *local* instance. Nothing in the nacos-source compare ever adopts remote values.

**R2 — Reversion is provider-owned monotonic state, not an authority token.** The "strictly-higher reversion wins" rule was Atlas's database guard (the conversion comment at conversion.go:84-87 documents it as a receiver-side protection against out-of-order pushes). Nacos's v1 register is an **unconditional upsert** — there is no server-side rejection to respect. Therefore in the nacos-source compare, *any* reversion mismatch — **either direction** — counts as drift: a nacos-side reversion *higher* than local (the case the task asks about: reachable only by out-of-band means — a manual register with hand-written metadata reversion, since spotter is the single writer under leader election and k8s `ResourceVersion` / consul `ModifyIndex` are monotonic for a given identity) is drift like any other, and the local value wins. Concretely: move `Reversion` from a guard into the compared set — `diff = fieldsDiffer(local, remote) || local.Reversion != remote.Reversion`. Porting Atlas's "remote higher wins" into this compare would let one out-of-band reversion bump permanently blind the reconcile (remote-higher + field-equal → never push → the drifted reversion persists forever, and every future local update would also be "older" and invisible). **Precedence ruling vs DS-5-4 (lead ruling): R2's drift→push-local is THE unified remedy — any reversion mismatch in either direction heals by overwriting with the local value.** DS-5-4 proposes treating remote-higher as alert-only (log/notice, no push, because the source is the reversion authority); this track overrules that *as a remedy* — alert-only leaves the forged value in place and the instance unhealed — but adopts it as an **optional companion signal**: the notice on remote-higher is worth keeping (it distinguishes a forged/edited reversion from ordinary drift and deserves a human look), layered on top of the push, never instead of it.

**R3 — Mid-flight safety.** "Nacos newer than local" cannot arise from spotter's own in-flight pushes: `CompareAndFlush`'s `GetAll()` reads the same informer store that `pod2Instance` converts from (k8s.go:411-416 vs k8s.go:177), and k8s `ResourceVersion` is monotonic per object — if a newer instance was enqueued, the store already holds it and the compare sees it. The in-flight windows (queue drop-on-full, pool submit pending) produce *local-newer-than-nacos*, which is the normal ADD/UPDATE case. So remote-newer ⇒ out-of-band ⇒ R1 applies. (A dropped delete event heals as case-3: the informer `List` no longer has the pod; the nacos view does.)

**R4 — The status-3 equality guard is obsolete in the remote compare.** `hasInstanceDiff`'s "both offline → no diff" (k8s.go:247-248) exists for the *local cache* diff, where an offline instance can linger in cache. The nacos view never holds a status-3 instance (offline instances are deregistered, never registered), so the guard is dead weight in the remote compare — harmless, but the rule set should say so.

**The three cases under the rules:**

- **ADD** (local has, nacos lacks — keyed by `metadata.instanceId` via `ListToMap`, which matches the local keying by pod name / consul instanceId): push local (case 2 as-is; the empty-nacos branch "push all" is just ADD for everything, and remains the first-boot behavior).
- **DELETE** (nacos has, local lacks): case 3 — mutate the *reconstructed* instance to `Status=3, State=terminated, Enabled=false` and push; the nacos sink's `pushOne` maps it to `deregister` by composite id. Because the reconstructed instance carries the host's own `ip`/`port`/cluster (nacos.go:337, `firstPort`/`clusterOf` read the same fields reconstruct wrote), **the deregister is well-formed by construction** — no empty-ip 400 risk (the event-path offline-shell guard at k8s.go:198-210 exists because *events* can produce empty-ip shells; the nacos view cannot).
- **UPDATE** (both have, diff per R1/R2): push local. This is the case that does not exist today in any meaningful form (the Atlas view never reflected nacos-side edits), and the one that heals manual metadata edits and enabled flips.

**Consul-specific adjustments in nacos-reconcile mode:**

- **`Enabled` must be compared as its wire projection, not the raw field (DS-5-2, adopted).** The asymmetry is real and consul-leg-only: the consul converter hardcodes local `Enabled: true` (convertion.go:103) regardless of status, the nacos register forces wire `enabled=false` for every status-2 instance (nacos.go:362-364), and `Enabled` is in the consul compared set (consul.go:453) — a raw compare on a status-2 pair is local `true` vs remote `false`, a diff every cycle forever (the push re-registers with wire `false`; the diff re-fires; the user's every-cycle loop, materialized by the model). The k8s leg needs nothing: its local `Enabled` derives from the same pod readiness that drives status (`formatContainerEnabled`, conversion.go:295-307 — every local status-2 k8s pod is locally `enabled=false`), and the k8s compare's `[online, unhealthy]` request *does* meet status-2 pairs in case 1, where it compares false-vs-false and is stable. **The consul fix: compare `wireEnabled(local) = local.Enabled && local.Status != InstanceStatusUnhealthy` against `remote.Enabled`** — exactly the derivation `Sink.register` applies before writing (nacos.go:362-364) — so the compare asserts precisely "would the next push write a different enabled than the remote holds", symmetric for both legs by construction. Raw `Enabled` stays in the model (the Atlas payload still carries the source value).
- **The same asymmetry is already neutralized today — but only accidentally; the two guards involved are REQUIRED invariants.** As the code stands (raw-`Enabled` compare, consul.go:453), no loop fires, for two reasons neither of which was written for this purpose: (a) the `[online]` status request (consul.go:417) excludes reconstructed status-2 instances from the remote map, so a local status-2 ecs instance never meets its own reconstruction in the case-1 field compare; (b) case-2's `Status == 1` gate (consul.go:469) then blocks the push that the resulting "local-only" classification would otherwise emit every tick. Remove either guard without the wire projection — widen `[online]` to k8s's `[online, unhealthy]` shape, or drop the case-2 gate — and the every-cycle loop re-arms. **Do both fixes (lead ruling): the wire-derived compare above is the robust one (correct regardless of the guards); the guards are the current accidental safety and stay pinned as invariants in their own right** — the case-2 gate is also the local-unhealthy-no-push policy, and the `[online]` request is load-bearing for the recovery heal below.
- The compared set must drop `Cluster` (reconstruct fix §3.2 makes this moot, but the rule stands: the compared set is the round-trippable intersection). `Idc`/`Cpu` stay — both round-trip. `Enabled` is the one field that satisfies the intersection rule *only through the projection above* — raw local `Enabled` does not round-trip on the consul leg (it is never what the wire holds for status-2); `wireEnabled(local)` does, by construction.
- The status request `[online]` (consul.go:417) is *sufficient* with the catalog view (parseStatus prefers metadata, so console-disabled healthy instances reconstruct as status 1 and are visible), and it preserves the case-3 semantics: a remote status-2 instance is invisible to the `[online]` view, so a recovered consul instance whose nacos metadata still says "2" presents as remote-lacking → case-2 push (local status 1) → heals. No change required; documented so the next reader does not "fix" it into a loop — note that this is the same property that serves as guard (a) for DS-5-2 above (one filter, two jobs; both are why the wire projection, not a widened request, is the right fix).
- **Case 3's `Status = 2`** (consul.go:489) must become `Status = 3` when the reconcile source is nacos — otherwise remote-only ecs instances heal into permanent disabled zombies (nacos upsert-enabled-false, hidden from consumers, visible in the catalog) instead of being deregistered. This is finding DS-3-5; gating it on the reconcile-source mode (rather than unconditionally) keeps the Atlas-primary world's "old instance" semantics untouched.

**Error path alignment (findings DS-3-6):** k8s's current "GetAll error → log → push everything" (k8s.go:314-322) must become skip-the-tick (consul's shape, consul.go:418-421) *when the source is a remote store that can blip*. With nacos as the source and thousands of instances, a transient 5xx per tick would otherwise trigger a full re-push burst — the exact thundering-herd the scale/latency tracks are driving out. The pin to change: `TestCompareAndFlushGetAllErrorPushesAll` (k8s_whitebox_test.go:938-950).

### 3.4 Requirement (f): interaction with the prune — keep both, define the roles

The nacos-source `CompareAndFlush` does **not** make the prune redundant. The two mechanisms are orthogonal on the axis that matters — *identity*:

| | Field-level diff (nacos-source CompareAndFlush) | PushAll prune |
|---|---|---|
| Heals | same-composite-id drift: metadata edits, enabled flips, status/state/env drift, reversion drift (either direction), missing instances (ADD) | changed-composite-id drift: ip change (k8s), port change, instances added out-of-band into owned pairs, offline instances whose per-instance deregister failed, vanished services (remembered sweep) |
| Cannot heal | an old-id registration left behind by an identity change (the push upserts the *new* id; the old id is untouched — only the prune's catalog listing sees it) | same-id field drift (it only deletes ids not in the desired set; a wrong-metadata right-id instance is *kept*) |
| Cadence | boot + every push-interval tick (`CompareAndFlush`) | every push-interval tick (`emitSyncAll` → SyncAll → PushAll; **not** at boot — the first SyncAll lands on the first tick) |

Keep both, exactly as scheduled today. The accepted duplications — there are two, both idempotent: **ADD-side**, an instance found missing by the diff is pushed via `Sync`, then `PushAll`'s upsert pass re-registers it again in the same tick; **DELETE-side**, a remote-only instance is acted on twice per tick — case-3 pushes `Status=3` → the nacos sink deregisters it by composite id, *and* the same tick's `emitSyncAll` → `PushAll` prune deletes the id again (it is not in the desired set) — two idempotent deletes of the same registration, the second a no-op. Neither duplication is waste worth removing: the diff-path action is what makes each heal independent of the SyncAll path (partial-failure safety — one path failing does not strand the heal). A shared per-tick catalog snapshot between the two walks is a scale-phase optimization (tracks 1/2), not a correctness need.

### 3.5 Test contract for the fix phase

- `pkg/worker` fanout: designated-source routing (set sink's view returned; nil designation = primary, secondary `GetAll` still never called; unknown name errors; `--reconcile-source nacos` without a nacos sink fails at wiring).
- `pkg/nacos`: catalog-based `GetAll` — the nacosmock already serves both views with the correct visibility split (`instance/list` skips `Enabled==false`, server.go:443-444; `catalog/instances` includes them — documented at server.go:458-461, the loop has no Enabled filter at server.go:495-503), so no mock work is needed for the split — the contract is: `GetAll` must read the catalog view, provider scoping via clusterName, `isCatalogNotFound` tolerance per service, error propagation. The real work here is re-pinning `TestBlackboxSinkGetAllReconstructsInstances` (sink_test.go:885) for the catalog source — including flipping its `Cluster` assertion (currently expects `"k8s"` from the reconstruct; the §3.2 fix makes it `""`).
- `pkg/nacos` reconstruct: `Cluster` stays empty; all round-trip pins from the §1 table (each compared field; `parseInt64` garbage → 0; `parseStatus` metadata preference over enabled).
- `pkg/providers/k8s`: reconcile-mode diff against a nacos-shaped remote — ADD / DELETE (deregister by composite id) / UPDATE (field diff) / reversion-mismatch-either-direction pushes local; skip-on-error repin (DS-3-6); and the k8s status-2 steady pin — a local unhealthy pod vs its own reconstruction (status 2, enabled false) compares `Enabled` false-vs-false and produces no diff, no push (the §3.3 k8s-leg symmetry, pinned so a future conversion change cannot silently re-arm DS-5-2 on this leg).
- `pkg/providers/consul`: case-3 status-3 in nacos mode; `Cluster` no longer in the effective compared set; **the status-2-ecs-pair no-diff/no-push pin (the DS-5-2 neutralization contract)** — a local status-2 ecs instance that exists in the nacos catalog as `enabled=false`/`metadata.status "2"` must produce no diff and no push at steady state, tick after tick. The pin must hold through the wire projection (`wireEnabled(local)=false` vs `remote.Enabled=false` — no diff even if the pair meets in case 1) and through the guards (the `[online]` request excludes the reconstruction; the case-2 gate blocks the push) — assert the observable (no diff, no push), so the pin stays green whichever mechanism carries it, and goes red the moment either is broken.
- Metrics (feeds DS-4): per-tick diff outcome counters (adds/deletes/updates by provider) — the "reconcile ran and found N" signal the observation track needs to assert convergence.

---

## 4. RESTART/DOWNTIME RECONCILE

### 4.1 The boot path audit (what actually happens today)

**k8s** (`monitor`, k8s.go:86-140):

1. Block until `HasSynced()` (15s-sleep warning loop, k8s.go:88-97).
2. `go ProcessIntervalFullPush()` — the ticker's **first** fire is a full interval after boot (60s demo / 21600s default); nothing interval-driven happens at boot.
3. `k.CompareAndFlush()` — **synchronous, at boot, before the watch goroutine exists** (k8s.go:101 vs 104). This is the startup reconcile the task asked to verify: yes, it runs once after `HasSynced`.
4. The watch loop starts; events flow.

With today's Atlas source, the boot `CompareAndFlush` in the demo is the degenerate push-all (Atlas view empty). In production-with-real-Atlas it is a genuine 3-way compare — against Atlas.

Two corrections to the folklore that the task description asked me to verify:

- **`flushInstances` is dead code** (k8s.go:274-297): grep-verified — no callers anywhere in the repo (only its own doc comment references it). The "startup `flushInstances` len>0 guard" no longer describes a live path; the *live* startup guard is `CompareAndFlush`'s own `if all := k.GetAll(); all != nil && len(all) > 0` (k8s.go:302) — same semantics, same deliberate rationale (an empty source at startup means "not synced / broken source" far more often than "everything died"; flushing or pushing on it would either poison the cache or assert a false vanished state).
- **`emitSyncAll` does not run at boot** (only from the tick, k8s.go:454; consul.go:338) — so **the prune does not run at boot either**, and the nacos `remembered` map is empty at boot (nacos.go:84, in-memory only). The boot-time nacos reconcile today is therefore: nothing except the side effects of the boot push-all.

**consul** (`Run`, consul.go:88-104): `go c.CompareAndFlush()` at boot (async, under the provider lock) + `go ProcessIntervalFullPush()` + the blocking monitor start. The same empty-remote degeneracy applies in the demo; consul's boot compare early-returns on `GetAll` error and on an empty local source (consul.go:406).

### 4.2 The boot reconcile under the design, and the residuals

With the nacos view as the diff source, the **existing** boot `CompareAndFlush` (unchanged schedule, k8s.go:101 / consul.go:93) becomes a real startup heal:

- **Drift accumulated while spotter was down is healed at boot**: pods deleted while down → the nacos view has them, local lacks them → case-3 → deregister (the reconstructed host supplies a well-formed composite id). New pods while down → ADD → push. Field drift (labels/env changed while down) → UPDATE → push. Services that vanished entirely while down → their nacos ghosts are remote-only → case-3 → deregistered — **this closes the 416e62a residual for the vanished-service case**, which today (demo) is unhealed because the remembered map died with the process and the Atlas view is empty. The nacos *catalog itself* replaces the in-memory `remembered` memory as the boot-time record of what spotter used to own — a strictly more durable source of the same information.
- The readiness gate already orders the dependencies: nacos is probed before the sink is even constructed (`CheckReadiness`, server.go:304-307), so at boot the nacos view is reachable; if nacos dies between startup and the first tick, the new skip-on-error semantics defer the heal rather than assert a false state.

**Residuals that remain (honest accounting):**

1. **Total-empty provider source at boot** (DS-3-7): a provider whose entire local list is empty (k8s: zero valid pods across all kubeconfigs — the k8s.go:302 guard; consul: nil/empty `GetAll` — consul.go:406) performs no case-3, and its `emitSyncAll` empty event is the conservative no-op at the sink (nacos.go:58-76). Its nacos ghosts persist until any instance of any of its pairs re-registers (re-populating `remembered`) or a manual clean. This is the deliberate blast-radius bound: a total-empty source is indistinguishable from a broken source, and its blast radius must not be every spotter-managed registration. The design shrinks the 416e62a residual to exactly this case; DS-4's observation should alert on the signature (provider source empty while its nacos cluster still holds instances — directly observable from the two views).
2. **The remembered map stays in-memory**: the prune's vanished-service sweep still cannot see a pair that vanished while spotter was down *if* the provider's source later comes back non-empty but without any instance of that pair — wait, no: with the nacos-source compare, that pair's ghosts are remote-only in the *very first* compare → case-3 → deregistered. The remembered-memory weakness is fully subsumed by case-3 as long as the source is non-empty. The only surviving role of `remembered` is intra-uptime vanished-service sweeps between compares (same cadence anyway). No change needed; the dependency on process memory is broken.
3. **Ecs ghosts heal to deregister only if DS-3-5 is fixed** (case-3 status 3 in nacos mode); until then they heal to disabled zombies.

---

## 5. FINDINGS

### DS-3-1 (P1) — The periodic 3-way diff compares against the first sink (Atlas), never nacos; in the demo the diff is dead code

**Evidence:**
- `pkg/worker/fanout.go:216-218` — `GetAll` returns `f.sinks[0].Sink.GetAll(...)`, "the v1 semantics of plan §6.3"; the doc comment (fanout.go:200-215) states the failure window: "secondary-sink drift that involves no local instance change (e.g. an out-of-band Nacos deletion) is invisible here".
- `pkg/providers/k8s/k8s.go:313-317` and `pkg/providers/consul/consul.go:417-421` — both compares source `worker.GetAll` (→ the fanout → Atlas).
- `build/atlasrun/main.go:12-22` starts `discoverymock.StartTCP` and never calls `SetInstances`; `internal/testkit/discoverymock/server.go:334-353` — `getAllInstance` filters `s.instances`, which stays empty forever. The demo's Atlas `GetAll` is **always empty**.
- Live log evidence (`build/demo/app.log`, spotter PID 30306, `--push-interval 60`): the non-empty-diff log line `discovery center online k8s instances size :%d` (k8s.go:337) **never appears**; measured **12 Atlas `SynInstance` RPCs and 24 nacos registers per 60s tick** (10 k8s + 2 ecs instances × 2 paths); **20,888 total nacos registers** in the current log; none of the three case notices (k8s.go:367/371/387, consul.go:478/482/495) ever fires.

**Why P1, not P0:** nacos still converges for the classes the prune covers (id-set), so this is not active data loss; but the user has explicitly declared the comparison target wrong for this system's verification, the mechanism that would verify consistency never executes in the demo, and the degenerate push-all it collapses into is a permanent 2×-per-interval full re-push (the churn the scale/latency tracks are separately fighting). Fix design: §3.1 (reconcile-source designation) + §3.2 (catalog-based GetAll).

**User-visible scenario:** an engineer demonstrates the system's self-healing by deleting an instance in the nacos console and waiting for spotter to restore it. In the demo it *appears* to heal within 60s — but only because the broken (always-empty) Atlas view makes spotter re-push everything every tick. The same demonstration against a production-shaped deployment (real Atlas) never heals at all (see DS-3-2): the "verification" the demo appears to provide is an artifact of the fake.

### DS-3-2 (P1) — Field-level nacos drift (manual metadata edit, enabled flip, API-side delete) has no heal path when the Atlas view is populated

**Evidence:** `hasInstanceDiff`'s remote operand is the Atlas view only (k8s.go:335-346); the nacos prune removes only ids absent from the desired set (nacos.go:253-263) and never inspects fields; no code path re-registers an instance without either a local source event or a diff push, and the diff that would fire reads Atlas, which was not edited.

**User-visible scenario (the emergency drain that never undoes):** an operator disables one instance in the nacos console (`enabled=false`) to drain it during an incident. Atlas is untouched, so the compare sees no diff; the prune sees the id in the desired set and keeps it. When the incident ends, the operator expects spotter to re-assert the K8s truth — nothing does. The instance stays disabled in nacos indefinitely while K8s reports it healthy: a silent per-instance discovery outage. The same holds for a metadata typo fixed by hand in the console, or an instance deleted via API to work around a problem: no heal until the pod itself changes. (In the demo all three heal by the DS-3-1 accident; the operator's mental model formed on the demo is wrong for production.)

**Fix:** the UPDATE case of §3.3 — the nacos-source field diff heals all three classes within one push interval.

### DS-3-3 (P1) — The nacos `Sink.GetAll` (the would-be diff source) reads the instance-list view that hides `enabled=false`

**Evidence:** `pkg/nacos/nacos.go:296` uses `ListInstances`; `pkg/nacos/client.go:219-221` documents the hiding ("this view is unsuitable for the prune"); live-proven in this audit — `instance/list` for `demo-pay-service` returns 1 host, `catalog/instances` returns 2 (the `enabled=false` `127.0.0.1#8848#ecs` host is hidden).

**User-visible scenario:** if the reconcile-source switch (§3.1) were flipped without the GetAll rewrite, every spotter-pushed-unhealthy instance (and every console-disabled one) would be read as "absent from nacos" → a spurious ADD push every cycle → the "every cycle has changes" loop the user explicitly warns about, plus 2× register churn per affected instance per tick. The prerequisite is §3.2's catalog-based GetAll. (This finding is why the design's prerequisite order matters: view first, routing second.)

### DS-3-4 (P2) — `reconstruct` writes `Cluster` from `clusterName`; both converters write `Cluster: ""` — a consul-vs-nacos compare would false-positive every cycle

**Evidence:** `pkg/nacos/nacos.go:507` (`Cluster: cluster`); k8s `formatCluster` returns "" without a `cluster` label / `K8S_CLUSTER_NAME` env (pkg/providers/k8s/conversion.go:432-456, absent in the live demo pods); consul hardcodes `Cluster: ""` (pkg/providers/consul/convertion.go:101); consul's equal-revision diff compares `Cluster` (pkg/providers/consul/consul.go:452).

**User-visible scenario:** with the reconcile source flipped but reconstruct unfixed, the ecs provider reports "instance data inconsistency" notices and re-pushes every ecs instance on every tick — the false-positive loop; alerts train operators to ignore them, and the real drift signal drowns. **Fix:** reconstruct leaves `Cluster` empty (the metadata never carried it; `Provider` already holds the cluster name) — a fidelity correction, not a loss.

### DS-3-5 (P2) — consul case-3 pushes `Status=2`; against nacos this heals remote-only ecs instances into permanent disabled zombies instead of deregistering

**Evidence:** `pkg/providers/consul/consul.go:488-493` (`servIns.Status = 2`); nacos `pushOne` maps status 2 to upsert-with-`enabled=false` (pkg/nacos/nacos.go:338-350, 362-364), not deregister; live demo state shows the shape: `demo-pay-service-inst-1` sits in the catalog as `enabled:false`, `metadata.status "2"` — invisible to `instance/list` consumers, permanently present in the catalog.

**User-visible scenario:** an ecs instance is removed from consul while spotter is down (or its delete event is lost). After restart, the nacos-source compare correctly finds it remote-only — and then *keeps* it forever as a disabled zombie: consumers using `instance/list` never see it (silent), operators auditing the catalog see a ghost that no reconcile ever removes. **Fix:** in nacos-reconcile mode, consul case-3 emits `Status=3` (the k8s shape), which the nacos sink maps to deregister; the composite id is derivable from the reconstructed host. Atlas-primary semantics stay untouched (mode-gated).

### DS-3-6 (P2) — k8s `CompareAndFlush` treats a `GetAll` error as "remote empty → push everything"

**Evidence:** `pkg/providers/k8s/k8s.go:314-322` — the error is logged and execution falls into the nil/empty-list branch that pushes all local instances; consul early-returns instead (`consul.go:418-421`); pinned by `TestCompareAndFlushGetAllErrorPushesAll` (pkg/providers/k8s/k8s_whitebox_test.go:938-950).

**User-visible scenario:** with nacos as the diff source at 1000s of instances (the scale track's target), one transient nacos 5xx per tick — or a slow page during the catalog walk (RequestTimeout is 10s per request, client.go:31) — triggers a full re-push burst to both sinks each interval: register-amplification precisely when the system is degraded. **Fix:** skip the tick on source error (align with consul), repin the test. (With the Atlas source this behavior was arguably defensible — an unreachable Atlas was a "resync everything" trigger; with a remote store that blips, it is a burst amplifier.)

### DS-3-7 (P2, residual by design) — Boot with a totally-empty provider source performs no nacos heal (the 416e62a residue, shrunk)

**Evidence:** the k8s guard `k8s.go:302` (`len(all) > 0`); consul's `consul.go:406`; the empty `emitSyncAll` conservative no-op at the sink (`nacos.go:58-76`); the remembered map is process-local and empty at boot (`nacos.go:84`); `flushInstances` — the function whose guard this rationale was written for — is dead code (`k8s.go:274-297`, no callers).

**User-visible scenario:** a k3s cluster's every spotter-managed pod is deleted during a maintenance window in which spotter also restarts. On boot, the provider's source is legitimately empty; no case-3 runs; the nacos cluster's registrations for those services persist as ghosts until any instance re-registers or a manual clean. The design keeps this bound deliberately (a total-empty source cannot be distinguished from a broken one; the blast radius must not be every registration) but requires the observation layer to alert on the signature: provider source empty while its nacos cluster still holds instances — a two-view check DS-4's harness can assert every tick.

---

## Appendix — evidence index (all paths absolute; live probes executed 2026-09-09..11 against the running demo)

- Diff-source seam: `/Users/donghongshuai/go/src/gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/worker/fanout.go:216-218`; `pkg/worker/worker.go:99-101`; `pkg/providers/k8s/k8s.go:313-317`; `pkg/providers/consul/consul.go:417-421`.
- The fake Atlas: `build/atlasrun/main.go`; `internal/testkit/discoverymock/server.go:334-353` (GetAll filter over never-populated state).
- k8s compare: `pkg/providers/k8s/k8s.go:245-260` (hasInstanceDiff), `300-391` (CompareAndFlush), `393-409` (buildAndSendEvent), `411-435` (GetAll), `443-486` (tick + emitSyncAll), `274-297` (dead flushInstances).
- consul compare: `pkg/providers/consul/consul.go:400-500`; `pkg/providers/consul/convertion.go:93-120` (Cluster "").
- nacos prune + reconstruct: `pkg/nacos/nacos.go:161-266` (PushAll/prune), `289-312` (GetAll, instance-list), `498-535` (reconstruct/parseStatus), `455-476` (clusterOf/firstPort/compositeID), `480-493` (metadataOf).
- nacos client views: `pkg/nacos/client.go:221-235` (ListInstances, hiding), `250-286` (ListCatalogInstances), `291-313` (ListServices).
- Wiring: `internal/server.go:301-326` (sink construction order, readiness gate), `internal/infra/config/config.go:250-253` (NacosAddr precedent), `cmd/adapter.go:123-126, 159`.
- Domain scaffolding: `internal/domain/instance/rules.go:109-172`.
- Live probes (GET-only): `service/list` (3 services); `catalog/instances` for demo-order-service/k8s (8 hosts, ids and revisions matching the 8 running pods and their resourceVersions — e.g. pod `demo-order-service-676b75dfb7-n2l6w`, RV 38491 = metadata reversion "38491", ip 10.42.0.25); `catalog/instances` demo-pay-service/ecs (2 hosts incl. enabled=false); `instance/list` demo-pay-service (1 host — the F8 live proof); `catalog/services` (3 services, per-service counts); catalog 500 behaviors (no clusterName / bogus cluster / missing service).
- Live kubectl (read-only): 10 demo pods + 2 kube-system pods; field extraction for the round-trip table (§1).
- Demo log analysis: `/Users/donghongshuai/go/src/gitlab.mfwdev.com/paas/mfwregistry-adapter/build/demo/app.log` — per-minute counters (12 Atlas rsyncs, 24 nacos registers), absence of the non-empty-diff log line and all case notices, presence of the actual push payloads used as the round-trip's local operand.
