# DSCA Track 2 — K8s-Path End-to-End Latency

**Auditor:** DS-2 (K8s-path end-to-end latency)
**Repo:** `spotter`, branch `refactor/all`, HEAD `69b0105`
**Method:** full code reading of the event chain (informer callback → enqueue → Pop → pod2Instance → ants pool → worker.Handle → FanoutSink → nacos Sink → HTTP register) + an executed scratch harness (built as a sibling module at `/Users/donghongshuai/go/src/gitlab.mfwdev.com/paas/ds2-latency-audit/`, importing `spotter`'s real packages: `pkg/nacos`, `pkg/worker`, `pkg/providers`, `pkg/k8srobot`, `pkg/beehive/service/v2` — no repo file was modified) against a throwaway loopback HTTP mock on a dynamic port + read-only GET probes against the live demo nacos at `127.0.0.1:18848` + analysis of the running spotter's own log (`build/demo/app.log`, PID 30306, `--push-interval 60`). No repo files were modified; the demo stack was not touched; no reserved port was bound.

**Track scope restated (plan §1, track 2).** The chain K8s event → instance conversion → external store (nacos) must be measured and driven to millisecond-level end-to-end even under thousands of simultaneous instance changes. The consul path is out of scope. No latency metric exists today — one is designed here and the baseline measured.

---

## 1. EXECUTION SUMMARY

**The verdict, in one paragraph.** The in-process hops (informer callback, channel send, Pop, `formatInstance`, cache diff, pool submit) are all microseconds — **not** the problem; the whole local prefix costs ~10 µs per event (measured, §4). The latency is concentrated in three places: **(1) one HTTP round trip per instance to nacos with no batching** (Nacos v1/v2 OpenAPI has *no batch register endpoint* — verified against the official docs, §3 hop 7; the only "batch" endpoints touch metadata, Beta), so N instances = N serial POSTs on one keep-alive connection; **(2) the sequential fan-out** — `FanoutSink.Push` pushes Atlas first and nacos second on the same goroutine (`pkg/worker/fanout.go:170-182`), so every millisecond of Atlas latency is added to the nacos leg (measured: atlas=50ms → nacos leg +50ms, exactly linear, §4 table c); **(3) there is exactly one nacos-bound HTTP client with a default `http.Transport` whose `MaxIdleConnsPerHost` effective value is 2** (`pkg/nacos/client.go:145` — `http.Client{Timeout: 10s}`, zero Transport ⇒ `http.DefaultTransport`): serial pushes reuse one connection fine, but the moment pushes become concurrent (100 ants workers all reaching the nacos leg), each worker opens its *own* TCP connection (measured: 1000 concurrent registers opened **624 TCP connections**, §4), and only 2 are kept idle afterwards — connection churn on every burst. Live log evidence on the real demo (12 pods, burst of 6): **watch-event → nacos-visible = 45–79 ms per instance, end to end**, of which the actual nacos POST is ~2–8 ms (live log inter-register gaps; live GET reads ~4 ms) — i.e. **the nacos leg is milliseconds, but the chain around it currently delivers tens of milliseconds single-event, and will deliver seconds under a 3000-instance burst** (derived in §5: 3000 × one serial POST at real-server ~5 ms ≈ 15 s wall on the single per-event goroutine chain, bounded only by the 100-worker pool and the single TCP connection per worker). No latency metric exists anywhere in the path (only `sync_once_durations_histogram` — the *Atlas* gRPC call duration, recorded at `pkg/discoverycenter/client.go:128`; nothing observes the nacos leg, and nothing observes end-to-end).

**The design, in one paragraph.** One new histogram, `event_to_store_e2e_duration_seconds` (or ms — see §6), observed at the sink's per-instance completion point (`pkg/nacos/nacos.go` `pushOne`/`register`/`deregister`), labeled by `sink` and `event`, measuring `time.Since(triggerOrigin)` where the origin timestamp is carried losslessly from `QueueObject.CreateAt` (already captured at `k8srobot.go:193`, `time.Now()` with full nanosecond precision — but **truncated to seconds** at `k8s.go:118` (`obj.CreateAt.Unix()`) and shipped as `Event.Trigger int64`; the fix is to widen the origin carry, three small seams, no interface break needed — §6). "Nacos-visible" is proxied by the register's 2xx response time (the same `doForm` call that made it visible; a catalog read would add a cache and its own latency — the choice is documented in §6). Buckets must span 1 ms → 10 s (the existing `LinearBuckets(0,1000,10)` — 1-second granularity — cannot see a millisecond-level chain; §6).

**Baseline numbers (all measured this session; mock-based numbers are optimistic floors, calibrated against the live server where marked).**

| Experiment | p50 | p95 | p99 | max | note |
|---|---|---|---|---|---|
| (a) raw `client.RegisterInstance` serial ×1000, loopback mock | 54 µs | 94 µs | 148 µs | 1.07 ms | throughput 16.8 k/s; optimistic floor |
| (a-live) real nacos per-POST (from live log inter-register gaps, 2 bursts) | ~2 ms | ~8 ms | — | 20 ms | live calibration, loopback demo nacos |
| (a-live) real nacos GET read ×10 | 3.4 ms min | — | — | 11 ms | p50 3.7 ms |
| (a-live) real nacos GET read ×50 parallel | 23 ms min | 132 ms p95 | — | 180 ms | **the real server degrades hard under parallel load** |
| (b) `Sink.Push` 1 instance, mock | 43 µs | 68 µs | 150 µs | 304 µs | includes metadata marshal + ensureCluster map lookup |
| (b) `Sink.Push` 100 instances (serial), mock | 4.70 ms | 4.90 ms | 4.90 ms | 5.06 ms | one event, 100 serial POSTs ≈ 47 µs each — linear |
| (c) worker.Handle→fanout→both sinks, 1 event | 88 µs | — | — | — | warm |
| (c) same, 100 concurrent events | 2.74 ms | 3.54 ms | 3.56 ms | 3.57 ms | wall 4.25 ms |
| (c) same, 1000 concurrent events | 19.0 ms | 26.3 ms | 28.8 ms | 29.2 ms | wall 30.2 ms |
| (c) 100 events, fake atlas 5 ms | 8.44 ms | 8.85 ms | 8.86 ms | 8.87 ms | **atlas latency added 1:1 to every nacos leg** |
| (c) 100 events, fake atlas 50 ms | 54.5 ms | 55.0 ms | 55.1 ms | 55.1 ms | linear, no isolation |
| (c2) ants(100) pool submit → nacos 2xx, 3000 events | 2.10 ms | 6.18 ms | 10.6 ms | 15.7 ms | wall 80 ms, 37 k inst/s — **mock floor** |
| (d) `formatInstance` (replica, synthetic pod) | 0.67 µs | 2.3 µs | 4.1 µs | 179 µs | pure CPU |
| (d) pod2Instance full shape (Get+diff+insert, cache@30000) | 7.4 µs | 13.0 µs | 15.7 µs | 185 µs | per event, 30 k cached instances |
| (d2) `cache.Get` @30 000 entries | 3.0 µs | 4.7 µs | 5.8 µs | 39 µs | deep copy included |
| (d2) `cache.ReplaceOrInsert` @30 000 | 4.2 µs | 7.8 µs | 10.0 µs | 163 µs | deep copy included |
| (d2) `cache.List` @30 000 | 108 ms | — | — | — | `GetAll`/`emitSyncAll` only, not per-event |
| (reuse) 200 serial registers | 1 TCP connection | — | — | — | keep-alive ON |
| (reuse) 1000 concurrent registers | **624 TCP connections** | — | — | — | `MaxIdleConnsPerHost`=2 effective |
| (live-log) watch event → nacos register 2xx (real demo, real nacos) | 60 ms (n=35) | — | — | 1.6 s* | *the 1.6 s tail pairs are pending-pod artifacts; true pairs 45–79 ms |
| (drop-sim) queue 4096, consumer 100 µs/ev, input 20 k ev/s | — | — | — | — | **37.7 % of events dropped silently** (k8srobot.go:195-199) |

**What must be true for the user's requirement (§7 SLO proposal):** steady-state single change p99 < 100 ms (today: ~60–80 ms real, marginal); 3000-instance burst: convergence — last instance visible — ≤ 10 s with zero drops (today: est. ~15 s serial-POST floor on real server, with drops starting above ~10 k events/s sustained input — the drop threshold is track 1's domain, cross-referenced in §5 DS-2-6).

---

## 2. CHAIN AUDIT — hop by hop

The chain under audit (file:line at HEAD `69b0105`):

```
informer callback (k8srobot.go:165-179)
  → enqueue: build QueueObject + non-blocking chan send (k8srobot.go:184-200, chan 4096)
  → Pop (k8s.go:107, k8srobot.go:234-252)
  → pod2Instance (k8s.go:175-243): robot.GetByKey → formatInstance (conversion.go:17-127)
      → VerifyInstance filters (common.go:82-117) → cache.Get (cache.go:41-57)
      → hasInstanceDiff (k8s.go:245-260) → ProcessCache/ReplaceOrInsert (cache.go:93-108)
  → pool.Submit(eventSync) (k8s.go:125-127; ants pool 100, k8s.go:65)
  → worker.Handle (k8s.go:167-173 → worker.go:85-92 → handler worker.go:52-65)
  → FanoutSink.Push — SEQUENTIAL per sink (fanout.go:170-182): atlas first, nacos second
  → nacos Sink.Push → pushOne → register (nacos.go:131-141, 322-357, 361-381)
  → ensureClusterHealthCheckDisabled (nacos.go:379, 416-433) [first push of a pair only]
  → client.RegisterInstance → doForm → one HTTP POST (client.go:171-173, 376-401)
  → 2xx "ok" ⇒ instance visible to nacos readers
```

**Hop 1 — informer callback → enqueue (`k8srobot.go:184-200`).** Cost sources: one type assertion, one 3-field struct allocation (`QueueObject`, `k8srobot.go:189-194`), a `time.Now()` call, and a **non-blocking** channel send (`select ... default`, `k8srobot.go:195-199`). Measured equivalent in every Go program: sub-microsecond. The informer's own callback dispatch (client-go `ResourceEventHandlerFuncs`, `k8srobot.go:165-179`) runs on the shared-informer processor goroutine — one goroutine per informer, so a slow consumer would back up *all* events of a cluster; the non-blocking send guarantees it never blocks (at the price of silent drops, DS-2-6). `CreateAt` is captured here with full precision (`time.Now()`, `k8srobot.go:193`) — **the only place in the chain where a true origin timestamp exists**, and it survives only as `Event.Trigger` **truncated to whole seconds** (`k8s.go:118` `obj.CreateAt.Unix()`), which is why no usable metric can be derived today without a small widening change (§6).

**Hop 2 — Pop (`k8s.go:104-130`, single goroutine).** `robot.Pop` blocks on the channel (`k8srobot.go:234-252`); wake-up is scheduler-level (µs). The loop then does, **serially on one goroutine**: a log line (`k8s.go:116` — measured in the live log to cost ~1–2 ms when 49 events churn? No: the live log shows same-millisecond processing; the INFO log is ~µs when logging is file+no-std, though at 3000-event bursts the volume itself is a cost, DS-2-8), `obj.CreateAt.Unix()` (ns), `pod2Instance` (measured: 4–8 µs typical, up to ~185 µs with GC noise, §4-d), `pool.Submit` (ants lock + cond signal, µs), `robot.Finish` (no-op, `k8srobot.go:257-259`). **This one goroutine is the throughput ceiling of the entire prefix**: ~5 µs/event pure, so ~200 k events/s — comfortably above thousands — *provided* `pod2Instance` stays µs-scale, which the measurements confirm even at 30 k cached instances (the b-tree `Get` is O(log n) with degree 2 ≈ 17 node compares at 30 k, and the deep copy dominates at ~3 µs).

**Hop 3 — pod2Instance internals (`k8s.go:175-243`).**
- `robot.GetByKey` (`k8srobot.go:263-277`): indexer map lookup + slice append; µs. Note `items[0]` is taken — only the first cluster's pod for a key; multi-cluster same-namespace/name collisions are out of scope here.
- `formatInstance` (`conversion.go:17-127`): measured **0.67 µs p50 / 4.1 µs p99** on a realistic one-container pod (replicated, since unexported — §4-d). Cost sources: container loop with two `resource.Quantity` parses (`formatCpuSize`/`formatMemorySize` call `r.String()` and `AsInt64` — note `formatCpuSize` compiles a regexp **per call** when `AsInt64` fails, `conversion.go:310-323`; the happy path `AsInt64` ok does not hit it, and the demo pods' `2`-cpu/`2048Mi` limits parse via `AsInt64` — but a pod with a fractional non-canonical cpu limit would recompile the regex every event; measured cost when triggered: a `regexp.MustCompile` is ~10–20 µs, a P2). Label/env maps rebuilt per event (allocation, µs).
- `VerifyInstance` (`k8s.go:154-164` → `common.go:85-114`): 7 field checks, ns. The live log shows this filter hard-dropping pending pods (`invalid instance ... pending state`, `build/demo/app.log` 09:15:38.401-432) — **by design** (they carry no IP yet), which is *good* for latency (fewer pushes) and *correct* for consistency.
- `cache.Get` (`cache.go:41-57`): RLock + b-tree search + **deep copy of the whole Instance** (`deepcopy.Copy` via reflection-free recursive copy, `cache.go:116-121`). Measured 2.9–3.0 µs p50 *at every size from 100 to 30 000 entries* (size-insensitive: the copy dominates). **Copy semantics confirmed: every `Get` and every `ReplaceOrInsert` deep-copies the instance** — two full copies per changed event. At 3 000 events/s that is ~6 000 copies/s of a ~1 KB struct ≈ 6 MB/s of short-lived allocation: irrelevant for latency (GC noise only, p99 max samples ~180 µs), but it is the *only* per-event allocation hotspot of the prefix.
- `hasInstanceDiff` (`k8s.go:245-260`): 8 field compares, ns. **The dedup/coalescing point**: 10 updates for one pod arriving back-to-back each find the previous one in the cache; only reversion-increasing or field-changing updates pass (verified: repeat add ⇒ nil, `k8s_whitebox_test.go:489-491`). **But this dedup is serial-consumer-only: it cannot see updates that arrive while an earlier push is still in the pool** — the cache is updated *before* the push is submitted (`k8s.go:211-216`), so push #1 (in the ants queue) and push #2 (cache-newer) both exist; nacos receives both, in submission order but possibly interleaved with other events. The second register is a harmless upsert (same composite id), so the cost is duplicate work, not corruption. 10 updates → up to 10 POSTs (no cross-event coalescing; DS-2-7 evaluates the option).
- `ProcessCache`→`ReplaceOrInsert` (`k8s.go:142-151` → `cache.go:93-108`): write lock + deep copy + b-tree replace; measured 3–4 µs.

**Hop 4 — pool.Submit (`k8s.go:125-127`, ants v2.4.3, capacity 100).** The pool is created blocking-mode default (`k8s.go:65`, `ants.NewPool(100, withExpiryDuration(100s))`): when all 100 workers are busy, `Submit` **blocks the Pop-loop goroutine on the pool's cond** (`ants/pool.go retrieveWorker`, cond.Wait loop) — this is the mechanism that makes the queue fill and then drop (DS-2-6): the 4096-event channel absorbs the burst only while the Pop loop is free; once Submit blocks, the channel fills in 4096 events' worth of input, then `enqueue` silently drops. Workers expire after 100 s idle and are respawned on demand (µs). The ants lock itself is uncontended-µs.

**Hop 5 — worker.Handle (`k8s.go:167-173` → `worker.go:85-92`).** A map lookup on `OperateType` and a synchronous call into the handler (`worker.go:52-65`): **the entire sink fan-out runs inside the pool worker's goroutine**. On error it calls `unsyncedService.Add` (`worker.go:60`) which takes the retry-store lock (fast, no I/O under it — `unsynced_service.go:80-115`).

**Hop 6 — FanoutSink.Push (`fanout.go:170-182`).** **Sequential over sinks in declaration order** — Atlas first (primary), nacos second (`internal/server.go:301,318`). One sink's error never short-circuits the others (errors collected into `FanoutError`), but **latency adds**: the nacos leg starts only after the Atlas `Push` returns. The Atlas `Push` is a gRPC `SynInstance` with a 10 s context (`discoverycenter/client.go:123-127`) **plus** an INFO log that marshals the whole instance list to JSON (`client.go:117-121`, "rsyncing instance: {...}" — visible in the live log). Measured with a fake sink: atlas 5 ms ⇒ every nacos leg +5 ms; atlas 50 ms ⇒ +50 ms (§4-c, exactly linear, no isolation). In the live demo, the Atlas leg (discoverymock, in-process) is fast (same-millisecond, live log 09:15:39.936→.938), so today's real added latency is small — but the *architecture* couples the two stores 1:1, and a slow Atlas (production discovery center, network RTT, 10 s timeout) **directly delays nacos visibility by up to 10 s per event** while nacos itself is healthy. This is the fan-out's documented design trade-off ("Sequential, not parallel: ... deterministic ordering", `fanout.go:163-169`) — correct for ordering guarantees, wrong for the millisecond requirement.

**Hop 7 — nacos Sink.Push → pushOne → register (`nacos.go:131-141, 322-357, 361-381`).** Per instance: policy switch (ns), `Ip == ""` guard (ns), `register` → `client.RegisterInstance` with `InstanceParams.values()` (`client.go:318-344`): builds `url.Values` (8–10 `Set` calls) + **`json.Marshal` of the 10-field metadata map** (~1–2 µs) + `values.Encode()` (query-string encoding, µs). Then `doForm` (`client.go:376-401`): `http.NewRequest` + `context.WithTimeout` + **one HTTP POST on the shared `http.Client`** + read ≤1 MB body + status check. The POST is the whole ballgame: **loopback mock 43–54 µs; real demo nacos ~2–8 ms** (live log inter-register gaps 0–20 ms, GET reads 3.4–11 ms). On success, `ensureClusterHealthCheckDisabled` (`nacos.go:416-433`): two `rememberedMu` lock/unlock pairs around two map lookups (ns) — **no HTTP** after the first push of each (service, cluster) pair (the first push of a pair adds one extra `PUT /nacos/v1/ns/cluster`, measured in the live log at ~9–14 ms once per pair per process, `02:27:48.511→.525`). `RequestTimeout = 10 s` (`client.go:31`) bounds every call — a wedged nacos holds one pool worker for up to 10 s per instance (100 workers ⇒ 100 concurrent 10 s timeouts then pool exhaustion ⇒ Submit blocks ⇒ queue fills ⇒ drops; the full cascade is DS-2-5).

**Locks on the path, held durations:** k8s cache RWMutex (`cache.go:38`) — RLock ~µs (Get), Lock ~µs (ReplaceOrInsert), never held across I/O; `rememberedMu` (`nacos.go:93`) — held for map ops only, explicitly not across HTTP (`nacos.go:411-415` comment); ants pool lock — µs, but `Submit` can *wait* (cond) for an unbounded time under saturation (the real risk, DS-2-6); retry-store RWMutex — not on the happy path. **No lock on the path is a latency problem.**

**Honest scope note (informer leg).** The real client-go watch → informer-store → callback dispatch latency (kube API server to this process) was NOT measured (would require driving the live k3s, out of bounds). It is upstream of `CreateAt` and is correctly *excluded* from the designed metric (the metric measures from `CreateAt`, the adapter's own ingestion point). k3s watch propagation is typically tens of ms; the SLO in §7 is stated from `CreateAt` on purpose.

---

## 3. THE LATENCY METRIC — design (patch description, NOT implemented)

**Definition.** `event_to_store_e2e_duration` = time from `QueueObject.CreateAt` (the moment the informer callback received the object, `k8srobot.go:193`) to the moment the instance's register/deregister HTTP call returns 2xx from nacos (the "store-visible" proxy).

**Why the register's 2xx is the "nacos-visible" proxy (documented choice).** The alternative — reading the catalog back (`ListCatalogInstances`) — adds a server-side cache layer to the observation (nacos's catalog read path serves from its in-memory datamodel, but read-your-write is not contractually instantaneous across the cluster's consistency protocol), adds one more GET per observation (perturbing the very latency being measured under burst), and cannot attribute *which* push made the instance visible. The 2xx of the POST is the same event that makes the instance visible to every subsequent reader of the same datamodel; it is the tightest observable proxy that adds zero extra load. The residual skew (a reader served by a stale replica) is a nacos-internal property, out of this system's control, and is bounded by nacos's own consistency guarantees. This choice is recorded here so the reviewer can attack it.

**Where the origin timestamp comes from — the one real gap.** `CreateAt` exists (ns precision) but the chain carries only `obj.CreateAt.Unix()` (`k8s.go:118`) → `Event.Trigger int64` (`pkg/worker/types.go:24`, `internal/ports/ports.go:76`) → `Push(triggerTime int64, ...)` (`internal/ports/ports.go:56`). Seconds truncation makes a p50-of-60 ms metric read as uniformly 0 ms or 1000 ms. Three minimal seam changes (no interface semantics broken — `Trigger` stays, a parallel field is added):

1. `pkg/worker/types.go` — `Event` gains `TriggerTime time.Time` (or `TriggerNS int64`; `time.Time` preferred for monotonic-clock correctness — `time.Since` then uses the monotonic reading and is immune to wall-clock steps). Set at `k8s.go:125-127` from `obj.CreateAt` (pass `obj` into the closure — it is already in scope; `triggerTime := obj.CreateAt.Unix()` line 118 stays for compatibility).
2. `internal/ports/ports.go` — `Event` (line 75) gains the same field (the `ports.Event` and `worker.Event` types are parallel structs today; both get the field; alternatively unify, but that is a bigger refactor than this track needs).
3. `pkg/nacos/nacos.go` — `Sink` gains a `metrics ports.MetricsRecorder` field (constructor param, `NewSink(addr, logger, metrics)` — **one new optional arg**; `internal/server.go:309` passes `s.metrics`; a nil default keeps every existing test compiling), and `pushOne` (or `register`/`deregister`, which know the outcome) observes after the HTTP call returns:
   `s.metrics.ObserveEventToStoreDuration(since(e.TriggerTime), ins.Status == offline ? "delete" : "upsert", err == nil)`.

   The fan-out is in the way: `FanoutSink.Push` (`fanout.go:170-182`) receives `[]*instance.Instance` and the trigger — the per-sink observation needs the trigger *time*, which rides the `Event` only down to `worker.Handle`. Since `FanoutSink.Push`'s signature already carries `triggerTime int64`, the clean spot is: **the worker's Sync handler** (`worker.go:52-65`) passes `e.TriggerTime` into a small per-sink observation wrapper — concretely, give `FanoutSink` the recorder + trigger-time at `Push` (signature change) OR observe at the *worker* level per sink by wrapping each `NamedSink.Sink` with a recording decorator constructed in `NewFanoutSink` (no signature change; the decorator implements `ports.InstanceSink`, measures `time.Since(triggerTime)` around `Push`, labels itself with the sink's name). **Recommended: the decorator** — it also covers the retry path (`unsynced_service.go:221-242` calls `PushTo` per sink) with zero extra changes, and it is where the per-sink label lives naturally.

4. `pkg/metrics/stat.go` — the collector:
   ```go
   EventToStoreDurationHistogram = prometheus.NewHistogramVec(
       prometheus.HistogramOpts{
           Name: "event_to_store_duration_histogram",
           Help: "end-to-end: informer-callback CreateAt -> sink store-visible (2xx), per instance",
           Buckets: append(append([]float64{},
               0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}),
       },
       []string{"sink", "outcome"}, // e.g. sink="nacos"|"atlas", outcome="ok"|"error"
   )
   ```
   registered in the existing `init()` (`stat.go:58-60`). **Buckets are the load-bearing choice:** the existing series use `LinearBuckets(0, 1000, 10)` (`stat.go:16`) — 1-second granularity, blind to everything this track cares about (a 60 ms p50 lands in bucket 0 either way). The ms-scale chain needs exponential-ish buckets from 1 ms to 10 s as above.
5. `internal/infra/metrics/metrics.go` + `internal/ports/ports.go` — the port extension: `MetricsRecorder` (ports.go:90-100) gains `ObserveEventToStoreDuration(sink string, outcome string, d time.Duration)`; `Recorder` (metrics.go:38) implements it via the `HistogramVec`; the two `nopMetricsRecorder` implementations (`worker.go:103-112`, `discoverycenter/client.go:190-199`) gain no-op methods. This is the same accepted-breaking pattern `SetSyncErrorQueueDepth(sink, depth)` already set (`metrics.go:70-81`).
6. `ports.MetricsRecorder` also gains nothing else — queue drop and pool saturation counters belong to track 1's design but the *latency* series needs one companion **counter** to be interpretable under burst: `events_dropped_total` observed at `k8srobot.go:197-199` (the `default:` branch — today the drop is perfectly silent, plan §1 known fact). Without it, a burst that drops 37 % of events shows *beautiful* percentiles on the survivors (survivorship bias, demonstrated by the drop simulation in §4). The counter is one `metrics.EventsDroppedTotal.Inc()` + the port method; the actual drop *log/metric* fix itself is track 1's finding — the metric-design dependency is declared here so the latency series is not deployed without its blind-spot detector.

**Label set.** `sink` ("atlas"/"nacos") — per-sink end-to-end is the whole point (the sequential fan-out makes the two legs' latencies differ by the other's duration; per-sink series make DS-2-2 visible in production). `outcome` ("ok"/"error") — error retries take the 5 s ticker path and their latencies (5 s + n) would otherwise poison the percentiles; operators alert on the ok-series and on the error count separately. NOT labels: provider (single-digit cardinality but the k8s path is this track's only scope; the consul provider can adopt the same series later — the decorator is sink-side, provider-agnostic), appcode/service (cardinality thousands — per-service percentiles belong in a dashboard aggregate, not Prometheus labels), event type add/update/delete (low value: delete and upsert cost the same POST shape).

**What the percentiles must prove (the user's requirement, restated as alertable statements).** p50 and p99 of `event_to_store_duration_histogram{sink="nacos",outcome="ok"}`: steady state, p50 < 50 ms / p99 < 100 ms (single change, no burst — proves the chain is not accumulating junk); under a 3000-instance burst, p99 < 1 s and — crucially — the **convergence time** (last-instance e2e, i.e. the max, or a companion "burst convergence" gauge = wall time from burst start to the 3000th observation) ≤ 10 s, with `events_dropped_total` = 0 across the burst. Percentiles alone cannot prove convergence (p99 of 3000 events says nothing about the last one); hence the max/convergence companion.

**Tests (with the patch).** Unit: a nacosmock-backed sink push where the mock's `SetDelay` injects 50 ms — assert one observation lands in the right bucket with `sink=nacos,outcome=ok` (the histogram vec's `Collect` or the testkit fake recorder pattern, `internal/testkit/fakes.go:592` precedent); error path with `SetStatus(500)` — assert `outcome=error` and that the retry path (drain via `UnsyncedService`) produces a *second* observation ≥ 5 s later (pins that retries are counted, labeled, and not silently merged). Whitebox (k8s): submit an event whose `QueueObject.CreateAt` is `time.Now().Add(-1*time.Second)`; assert the observed duration ≥ 1 s (pins the origin carry through the pool + fan-out, the exact seam this design adds). Mutation pins: delete the `TriggerTime` field copy at `k8s.go:125` → the whitebox test fails (0 instead of ≥1 s); delete the decorator wrap in `NewFanoutSink` → no observations at all, the nacosmock test fails.

---

## 4. BASELINE — the measured numbers

**Environment.** macOS 15 (darwin 24.6.0 arm64), Apple Silicon, GOMAXPROCS=8. Harness: sibling module `ds2harness` importing spotter's real packages; the nacos stand-in is a local loopback `httptest` server (dynamic port) answering "ok" to every v1 endpoint — **an optimistic floor** (no real datamodel, no persistence, no raft); every real-server number below is marked `live` and comes from the untouched demo stack (GET probes only + the running spotter's own log timestamps). Real-server per-call numbers from this audit: GET ~4 ms, register ~2–8 ms (loopback nacos 2.1.0 standalone).

**(a) Per-POST register latency, serial, 1000 iterations (mock).**
```
(a) client.RegisterInstance serial   n=1000  p50=53.9µs  p95=93.6µs  p99=148.2µs  max=1.07ms  throughput=16758 req/s
```
One warm keep-alive connection; includes URL building, metadata JSON marshal, form encode, POST, body read. **Live calibration:** the running spotter's log bursts show inter-register gaps of 0–20 ms (09:15:40 burst: [6.0, 0.0, 0.0, 2.0] ms; 10:11:47 burst: [2, 4, 19, 20, 1, 2, 2, 8] ms) — the real nacos POST is ~2–8 ms with occasional ~20 ms (GC or the pair's one-time cluster PUT). Live GET reads: 10 samples min 3.4 ms / p50 3.7 ms / max 11 ms; **50 parallel GETs: p50 65 ms, p95 132 ms, max 180 ms — the real server degrades an order of magnitude under parallelism** (single-node demo nacos; this bounds how much concurrency the sink leg can usefully add, DS-2-4).

**(b) The same through the real `Sink.Push` (mock).**
```
(b) Sink.Push 1 instance            n=1000  p50=42.5µs   p95=68.4µs   p99=149.9µs  max=304.3µs   throughput=21045 inst/s
(b) Sink.Push 100 instances          n=20   p50=4.70ms   p95=4.90ms   p99=4.90ms   max=5.06ms
```
The 1-instance path adds ~0 over the raw client (metadata marshal + ensureCluster map lookup are µs — confirmed: after the warm-up push, the pair's `healthCheckDone` map hit short-circuits and no extra HTTP happens, `nacos.go:416-423`). The 100-instance push is **exactly linear** (~47 µs each): `Sink.Push` is a serial per-instance loop (`nacos.go:133-139`) — one event carrying N instances costs N round trips on one connection. Live translation: 100 instances × real ~5 ms ≈ 0.5 s; 3000 instances ≈ 15 s (single event or single worker's queue).

**(c) Full chain worker.Handle → fanout → sinks (mock nacos + fake atlas).**

| atlas delay | burst | p50 | p95 | p99 | wall | note |
|---|---|---|---|---|---|---|
| 0 | 1 | 88 µs | — | — | 89 µs | single event, warm |
| 0 | 100 | 2.74 ms | 3.54 ms | 3.56 ms | 4.25 ms | 23.5 k ev/s |
| 0 | 1000 | 19.0 ms | 26.3 ms | 28.8 ms | 30.2 ms | 33 k ev/s, mock floor |
| 5 ms | 100 | 8.44 ms | 8.85 ms | 8.86 ms | 8.9 ms | **atlas adds 1:1 to nacos leg** |
| 50 ms | 100 | 54.5 ms | 55.0 ms | 55.1 ms | 55.1 ms | linear, no isolation |

The sequential-fan-out overhead is exactly the atlas cost (compare rows: p50 goes 2.74 → 8.44 → 54.5 ms as atlas goes 0 → 5 → 50 ms; the delta is the atlas latency, ±0.2 ms). **This is the cleanest experimental proof of DS-2-2.**

**(c2) 3000-instance burst through the real ants pool (100) — the production shape (`k8s.go:125` Submit → Handle → fanout → nacos).**
```
(c2) ants(100) submit -> nacos 2xx   n=3000  p50=2.10ms  p95=6.18ms  p99=10.6ms  max=15.7ms  wall=80.4ms  37291 inst/s
```
Mock floor: with a µs-fast nacos stand-in, the pool absorbs 3000 concurrent events in 80 ms. **Real-server translation (honest arithmetic, not measurement):** the same 3000 events hit the real nacos as 3000 serial-ish POSTs distributed over ≤ 100 workers' own connections; per-POST 2–8 ms serial, but the 50-parallel GET experiment showed the real server at p50 65 ms under 50-way parallelism — the wall lands in the **5–20 s** region, not 80 ms. The mock proves the *spotter-side* machinery is not the bottleneck (pool, worker, fan-out, client all clear 3000 events in < 100 ms when the store answers in µs); the bottleneck is the store round trip × count, plus the connection churn below.

**Connection reuse (measured).**
```
(reuse) 200 serial registers        → 1 TCP connection (keep-alive ON)
(reuse) 1000 concurrent registers   → 624 TCP connections opened, wall 32ms
```
`http.Client{Timeout: 10s}` with zero Transport ⇒ `http.DefaultTransport`: `MaxIdleConns=100`, **`MaxIdleConnsPerHost` field reads 0 but the effective runtime default is 2** (`net/http` `DefaultMaxIdleConnsPerHost`). Serial pushes ride one warm connection (good — the live demo's steady state). Concurrent bursts: every worker that finds no idle connection dials a fresh one (624 conns for 1000 requests), and after the burst only 2 are kept — the next burst dials hundreds again. On loopback this costs µs; on a real network (and on the nacos server's accept queue) it is tens of ms per dial and a server-side connection storm — the exact shape that made the live 50-parallel GET degrade to 180 ms.

**(d) pod2Instance per-event cost (formatInstance replica + real `providers.Cache`).**
```
(d) formatInstance (replica)                       n=5000  p50=0.67µs   p95=2.3µs   p99=4.1µs    max=179µs
(d) pod2Instance full (cache@30000, all diffs)     n=5000  p50=7.4µs    p95=13.0µs  p99=15.7µs   max=185µs
(d) pod2Instance no-diff path (dedup)              n=5000  p50=4.25µs   p95=8.6µs   p99=10.8µs   max=157µs
```
(`formatInstance` is unexported; the harness replicates it line-for-line from `conversion.go` — same k8s.io types, same sub-functions — and the cache dance uses the REAL `providers.NewCache(2)`.) **The whole local prefix is single-digit microseconds per event, even with 30 000 cached instances.** The dedup (no-diff) path is *cheaper* than the diff path (no insert copy) — coalescing via the cache diff is already effective at the event level.

**(d2) `providers.Cache` costs (real code).**
```
Get            @100/1000/5000/30000:  p50 2.9–3.2µs  (size-insensitive; deep copy dominates)
ReplaceOrInsert@100/1000/5000/30000:  p50 2.7–4.2µs
List           @100: 254µs | @1000: 2.78ms | @5000: 15.3ms | @30000: 107.6ms
```
`Get`/`ReplaceOrInsert` are flat in size (deep copy is the cost, not the tree walk). `List` is linear-and-copying — it is the `GetAll`/`emitSyncAll` path (every push-interval tick, and `CompareAndFlush`), **not** per-event; at 3000 instances `emitSyncAll`'s `cache`-free `GetAll` + event build is the same shape (a 3000-instance `List` ≈ 11 ms of copying per tick — fine at 60 s cadence, a problem only if the tick shrinks).

**Live-log end-to-end (real chain, real nacos, 12-pod demo).** Pairing each `get changes from k8s robot client` line (`k8s.go:116`) with the first `nacos: registered instance` of the same pod (`nacos.go:378`) within 5 s: **n=35 pairs, p50 60 ms, true pairs 45–79 ms** (the 6 events pairing at ~1.5 s are an artifact: they were *pending-pod* events correctly dropped by the filter — the immediately-following WARN line proves it — and the pairing matched them to the *next* update's register). Decomposition of a true pair (09:15:39.936 → 40.015, 79 ms): Pop+convert+submit ≈ 2 ms (atlas "synced" logged at .938), **atlas gRPC ≈ 2 ms**, then the nacos leg ≈ 77 ms of which the POST itself is ~2–8 ms — the remainder is the fan-out's atlas-side JSON-marshal INFO log + gRPC + scheduling (the live chain runs with full INFO logging: one ~700-byte JSON `rsyncing instance:` line per event plus one register line per event — measurable, DS-2-8). **Today's real, steady-state, single-event end-to-end: ~60 ms.** This is the number the metric of §3 would have produced continuously.

**Queue-drop simulation (channel semantics of `k8srobot.go:184-200`, not the real informer).** 4096-buffer channel, consumer at 100 µs/event (optimistic full prefix cost), input rate ramped:

| input rate | sent | dropped | % |
|---|---|---|---|
| 10 000 ev/s | 9 997 | 0 | 0 % |
| 20 000 ev/s | 19 281 | 7 262 | **37.7 %** |
| 50 000 ev/s | 49 595 | 40 012 | 80.7 % |

With the real server in the loop the consumer's *downstream* (pool → sinks) saturates far earlier than the Pop loop itself: the Pop loop stays free (Submit only blocks when 100 workers are all busy), the channel absorbs 4096 events, and everything past that is **silently dropped with no log, no metric, no count** (`k8srobot.go:197-199`). At 3000 simultaneous instance changes each producing add+several updates+delete (a realistic rolling churn produces 5–10 events per instance), a 3000-instance burst is 15 000–30 000 events — above the 10 k/s sustainable rate of even the mock consumer, and far above the real-server-backed throughput (~200–2000 inst/s serial; §5 arithmetic). **Drops during the user's target burst are not a risk; they are the default outcome** (mitigating factor: the 60 s `CompareAndFlush`/`emitSyncAll` tick re-pushes everything dropped — the burst self-heals within one interval, which is why the *consistency* observation tracks see a clean system; but *latency* for a dropped event is 60 s, not milliseconds). Full capacity analysis is track 1's; the latency-relevant statement is DS-2-6's.

---

## 5. FINDINGS

### DS-2-1 (P0) — One HTTP round trip per instance, no batching, on a serial loop

**Evidence.** `pkg/nacos/nacos.go:131-141` (`Push` iterates instances, `pushOne` per instance), `nacos.go:366-374` (one `RegisterInstance` per instance), `pkg/nacos/client.go:171-173` + `376-401` (one POST per call). Nacos v1 OpenAPI has **no batch register**: `/nacos/v1/ns/instance` is single-instance (POST/PUT/DELETE); the only multi-instance endpoints are `PUT/DELETE /nacos/v1/ns/instance/metadata/batch` — **metadata-only, Beta, cannot register or deregister** (official v1 and v2 OpenAPI docs, checked this session; v2's `/nacos/v2/ns/instance` is equally single-instance). Measured: `Sink.Push` 100 instances = 4.7 ms mock (linear), ⇒ 100 × real ~5 ms ≈ 0.5 s; 3000 ≈ **15 s** for one full-push event; per-event single pushes are the same cost paid 3000 times across the pool.

**User-visible scenario.** A 3000-instance deployment scale-out: the first instance becomes visible in nacos in tens of ms (fine), the last one after ~15 s of serial POSTing on the real server — and every `CompareAndFlush` tick that finds drift re-pays the same 15 s. "Millisecond-level at thousands of simultaneous changes" is impossible while N changes = N round trips.

**Fix design (options, with trade-offs).**
- **(A) Bounded concurrent per-instance pushes inside `Sink.Push`** (e.g. an errgroup with a semaphore of 16–32). No API change; nacos-side upserts are idempotent and order-independent for distinct composite ids (the reversion guard is field-level, not arrival-order-level — the strictly-higher-reversion rule at the receiving side, `nacos.go`'s documented contract). Trade-off: the live 50-parallel GET experiment showed the demo nacos degrading to p50 65 ms under 50-way parallelism — concurrency must be capped well below the server's knee (start 8–16, tune by the §3 metric); a wedged server now holds N workers × semaphore slots for 10 s each.
- **(B) Pipelining across instances on one connection** (HTTP/1.1 has no true pipelining in Go's client; this effectively means (A) with `MaxIdleConnsPerHost` raised so concurrent requests reuse a small connection pool instead of dialing — combine with DS-2-4).
- **(C) v2 API migration** — same single-instance shape, no gain; not a fix. **(D) metadata/batch abuse** — cannot register; not a fix. **Recommendation: A+B together, per-sink concurrency knob (default 8), measured by the §3 histogram's p99 and burst-convergence gauge.** The earlier-audit note "real nacos HTTP ~1–10 ms per call" matches this audit's live numbers; at 5 ms/POST, 8-way concurrency clears 3000 registers in ~1.9 s wall — inside the §7 SLO.

### DS-2-2 (P0) — Sequential fan-out: Atlas's latency is added 1:1 to every nacos push

**Evidence.** `pkg/worker/fanout.go:170-182` — `Push` loops sinks in declaration order, Atlas first (`internal/server.go:301`), nacos second (`server.go:318`); the comment at `fanout.go:163-169` states the trade-off ("Sequential, not parallel ... a slow secondary must not reorder the primary's pushes"). Measured: fake atlas 5 ms ⇒ nacos-leg p50 +5.0 ms (2.74→8.44 ms); atlas 50 ms ⇒ +51.8 ms (2.74→54.5 ms) — perfectly linear, zero isolation. Worst case is structural: the Atlas leg is a gRPC call with a **10 s** timeout (`discoverycenter/client.go:18,123`) — a slow-but-not-dead discovery center delays every nacos registration by up to 10 s while nacos itself is healthy.

**User-visible scenario.** Discovery centerGC pause / network blip of 2 s: every instance change in that window appears in nacos 2 s late — service consumers see pods "not registered" for the blip's duration even though nacos was ready to accept them in 5 ms. The millisecond requirement fails on the *healthy* store because of the *sick* sibling.

**Fix design (options).**
- **(A) Parallel sinks with a WaitGroup** inside `FanoutSink.Push` (each sink on its own goroutine, failures still aggregated into `FanoutError`, `Push` returns after all complete). Preserves "every sink attempted, error isolation" exactly; loses the deterministic *completion* order (Atlas may finish after nacos) — which nothing depends on (the ordering argument in the comment is about the *primary's pushes not being reordered*, i.e. per-sink ordering, which parallel execution preserves: each sink still receives pushes in Trigger order as emitted by its own serial caller chain... **caveat: with per-sink goroutines, two rapid events for the same instance can now interleave across sinks** — event 1's atlas leg and event 2's atlas leg stay ordered (same goroutine), same for nacos; cross-sink ordering is already unordered today. Safe.)
- **(B) Reorder — nacos first, atlas second.** Zero-code option at the wiring site (`internal/server.go:301,318` swap) but it changes `GetAll`'s primary (the first sink is the reconcile view, `fanout.go:216-218`) — **rejected: that is DS-3's domain and would flip the reconcile source to nacos implicitly.** (Worth stating so nobody "fixes" latency by breaking the reconcile design.)
- **(C) Fan-out with per-sink bounded concurrency** (A + a small pool per sink so a slow sink also doesn't consume ants workers).
- **Recommendation: A.** Trade-off accepted: error aggregation order becomes nondeterministic (tests must assert set-wise, not sequence-wise — `FanoutError.FailedSinks()` order is the one observable that changes; it is consumed by `failedSinkNames` as a set of names into the retry queue, order-insensitive, `unsynced_service.go:68-77`). Also fix the *second*-order effect while there: the Atlas leg's per-event `json.Marshal` INFO log (`discoverycenter/client.go:117-121`) is on the critical path (DS-2-8).

### DS-2-3 (P1) — The trigger origin is truncated to whole seconds; no end-to-end latency is observable at all

**Evidence.** `QueueObject.CreateAt` is captured at ns precision (`k8srobot.go:193`) and immediately truncated: `triggerTime := obj.CreateAt.Unix()` (`k8s.go:118`) → `Event.Trigger int64` (`pkg/worker/types.go:24`, `internal/ports/ports.go:76`) → `Push(triggerTime int64, ...)`. The only per-push metric that exists anywhere is `sync_once_durations_histogram` — the *Atlas gRPC call duration* (`pkg/discoverycenter/client.go:128`, `ObserveSyncOnceDuration(time.Since(before))`) — which measures one leg's call time, not end-to-end, and **nothing at all observes the nacos leg** (the sink never sees a MetricsRecorder; `NewSink(addr, logger)` at `nacos.go:109`). Buckets of the existing series are `LinearBuckets(0,1000,10)` (`pkg/metrics/stat.go:16,25,34,40`) — 1 s granularity, blind below 1 s.

**User-visible scenario.** "Is the K8s→nacos chain millisecond-level?" is today answerable only by reading `app.log` timestamps by hand (this audit's §4 live-log method) — impossible to alert on, invisible in the metrics endpoint, and the 60 ms real baseline would read as a flat zero in every existing histogram.

**Fix design.** §3 in full (the decorator at `NewFanoutSink`, the ms-scale buckets, `sink`/`outcome` labels, the `events_dropped_total` companion, the whitebox origin-carry test with the mutation pin). The metric is the *precondition* for every other fix in this document: without it, DS-2-1/2/4 fixes cannot be shown to work.

### DS-2-4 (P1) — One shared `http.Client` with `MaxIdleConnsPerHost`=2 (effective): burst connection churn, no transport tuning

**Evidence.** `pkg/nacos/client.go:145` — `http: &http.Client{Timeout: RequestTimeout}` (zero Transport ⇒ `http.DefaultTransport`). Measured: 200 serial registers ⇒ **1 TCP connection** (keep-alive works); 1000 concurrent registers ⇒ **624 TCP connections**, then only 2 kept idle (`DefaultMaxIdleConnsPerHost`); live: 50-parallel GET against the demo nacos ⇒ p50 65 ms / p95 132 ms vs 4 ms serial (server-side degradation under connection pressure).

**User-visible scenario.** A 3000-instance burst with the DS-2-1 concurrency fix opens hundreds of fresh connections to nacos within milliseconds; on a real network each dial adds RTT + TLS (n/a here) to that instance's end-to-end, and the nacos server's accept/backlog becomes the shared bottleneck — the burst's p99 is dominated by connection setup, and the *next* burst pays it again (only 2 conns survive).

**Fix design.** Construct the Transport explicitly in `NewClient` (`client.go:143-147`): `&http.Transport{ MaxIdleConns: 100, MaxIdleConnsPerHost: <concurrency>, MaxConnsPerHost: <cap>, IdleConnTimeout: 90s, DialContext: net.Dialer{Timeout: 2s}.DialContext, TLSHandshakeTimeout: 5s }` with `<concurrency>`/`<cap>` tied to the DS-2-1 semaphore (a transport that allows more connections than the push concurrency wastes conns; fewer starves it). Trade-off: `MaxConnsPerHost` also becomes the circuit breaker — a wedged nacos can hold at most that many workers instead of 100 (interacts with the 10 s `RequestTimeout`; consider lowering the timeout for register/deregister to ~2 s — a register that takes 2 s has already failed the SLO and retrying via the 5 s ticker is cheaper than blocking a worker for 10 s). Keep `http.Client{Timeout}` as the outer bound (it covers the body read, which the per-request context does not).

### DS-2-5 (P1) — 10 s `RequestTimeout` × serial per-instance loop × 100 ants workers = pool exhaustion cascade into queue drops

**Evidence.** `RequestTimeout = 10 s` applies to **every** v1 call (`client.go:31,384,415`); `Sink.Push` is serial per instance (`nacos.go:133-139`); the worker handler runs inside the 100-capacity ants pool (`k8s.go:65,125`); `Submit` blocks when the pool is full (ants v2.4.3 `retrieveWorker` cond-wait — no `Nonblocking` option set, `k8s.go:65`); the Pop loop then stalls, the 4096 channel fills, and `enqueue` drops silently (`k8srobot.go:195-199`). The drop simulation (§4) shows 37.7 % loss at 20 k ev/s input against a *fast* consumer; with a wedged nacos the effective throughput per worker is 1 instance / 10 s — 100 workers ⇒ 10 inst/s ⇒ the channel fills in 4096/10 ≈ **410 s of wedged-store events**, then drops. (The retry side is well-designed: per-sink keyed retry with permanent-drop on 4xx, 5 s ticker — `unsynced_service.go:137-148,247-280`; the 5 s ticker is failure-path-only and does not touch the happy path. Queue-drop mechanics are track 1's DS-1 domain — cross-referenced, not duplicated: this finding's latency-specific claim is the *timeout sizing*, not the drop.)

**User-visible scenario.** Nacos hangs (GC, disk, network): for ~7 minutes every instance change is swallowed into 10 s stalls, the queue fills, drops begin — then nacos recovers and the 60 s tick's full push heals everything (consistency survives, latency does not: minutes, not milliseconds). During the wedge, the millisecond SLO is unmeetable by definition — the finding is that the system's *recovery posture* (drop, heal on tick) is actually the right latency trade, but the 10 s timeout makes the wedge's blast radius 100 workers × 10 s.

**Fix design.** Split the timeout budget per call class: register/deregister/cluster PUT get ~2 s (they are single-row writes; a 2 s answer is a failed SLO already — fail fast into the 5 s retry ticker, which is the designed recovery path); catalog/service list reads keep 5–10 s (they are the full-push path, off the hot chain). Concretely: a `timeoutFor(path, method)` helper in `client.go` replacing the single `RequestTimeout` const at the two `context.WithTimeout` sites (`client.go:384,415`), plus the DS-2-4 `MaxConnsPerHost` cap. Trade-off: a genuinely slow-but-working nacos (cold start, 3 s writes) starts queueing retries — acceptable, the retry ticker converges and the full-push tick covers; better than a 10 s worker hostage per instance.

### DS-2-6 (P1, cross-reference to track 1) — Drop-on-full queue: silent, unmetered, and the survivorship bias that would fake a passing latency SLO

**Evidence.** `k8srobot.go:195-199` — `select { case queue <- item: default: /* drop, no log, no metric */ }`; the plan's known-facts list records it ("drop-on-full with no log/metric"). Simulated thresholds (§4): 0 drops at 10 k ev/s input, 37.7 % at 20 k, 80.7 % at 50 k against a 100 µs consumer. The latency-specific hazard: **once the §3 metric exists, a burst that drops 40 % of events reports excellent percentiles on the 60 % that survived** — the SLO would pass while two-fifths of the changes were never pushed (and only healed by the 60 s tick). This is why §3 refuses to ship the histogram without `events_dropped_total`.

**User-visible scenario.** 3000-instance churn (each instance ~5–10 events): 15–30 k events arrive in seconds; the real-server-backed throughput clears ~1–4 k events/s; thousands of events drop; nacos converges only at the next full-push tick (up to 60 s late) — "consistent" per the observation tracks, "not millisecond" per the requirement.

**Fix design (belongs to track 1; latency-side requirements stated here).** The drop must be *counted* (`events_dropped_total` at `k8srobot.go:197`) and the SLO stated as a conjunction: p99 latency AND zero drops AND convergence ≤ bound. Capacity-side options (bigger channel, Nonblocking pool with overflow counter, batch Pop) are DS-1's to design; this track's constraint: any fix must keep `CreateAt` semantics (the metric's origin) — e.g. a bigger channel preserves it for free; a coalescing dequeue (DS-2-7) must keep the *earliest* CreateAt of the coalesced set, not the latest, or the metric under-reports queue wait.

### DS-2-7 (P2) — No cross-event coalescing: 10 updates for one pod while its first push is in flight ⇒ up to 10 registers

**Evidence.** The cache-diff dedup (`k8s.go:211-216`) is *serial-consumer-only*: it sees updates only when the Pop loop processes them; the cache is written **before** the push is submitted, so mid-flight pushes are never re-diffed. Verified by reading (the whitebox test pins the serial case only, `k8s_whitebox_test.go:489-491`): 10 updates arriving while push #1 sits in the ants queue produce 10 `worker.Event`s — 10 POSTs to nacos, each an upsert of the same composite id (harmless, order-free thanks to the reversion guard). Under a rolling update (the canonical 3000-instance burst), the *event* count is 5–10× the instance count, so the POST count is too.

**User-visible scenario.** A pod churning through Pending→Running→Ready in 2 s generates ~4 events; nacos receives up to 4 registers for it; the last one is correct; the wasted 3 POSTs steal burst capacity from other instances (latency, not correctness).

**Fix design (options).** (A) *Per-instance in-flight coalescing* at the sink: a small map keyed by composite id holding the latest pending instance; the POST loop drains latest-only (keep-highest-Reversion, same rule as the retry queue's `Add`, `unsynced_service.go:103-107`). Trade-off: adds a lock + map on the hot path (µs, fine) and changes push *count* semantics — the §3 metric's per-instance observations become per-*final-state* observations (arguably the right thing to measure for "visible"). (B) *Delay-batch* (Linger 20–50 ms): the sink collects instances for one short window and pushes concurrently (with DS-2-1's semaphore). Trade-off: adds a fixed latency floor (the linger) to every single event — **breaks the single-change p99<100 ms SLO's headroom if set too high; a 20 ms linger is affordable but must be a knob, default off, enabled only when bursts are the workload.** (C) Do nothing: the dedup already handles the steady state; the burst case is DS-2-1's concurrency to absorb. **Recommendation: A for the burst workload, C as default; B rejected as a default.** (A) preserves single-event latency exactly (no linger) and only collapses *back-to-back same-instance* pushes.

### DS-2-8 (P2) — Per-event INFO logging on the critical path (JSON marshal of every pushed instance, twice)

**Evidence.** `pkg/discoverycenter/client.go:117-121` — every `Sync` marshals the entire instance list to JSON **just to log it** ("rsyncing instance: [{...700B...}]"), on the Atlas leg = on the nacos leg's critical path (sequential fan-out, DS-2-2); `pkg/nacos/nacos.go:378` — one INFO line per register; `k8s.go:116` — one INFO line per event. The live log shows all three per event (measured contribution: the live 79 ms pair decomposes to ~2 ms prefix + ~77 ms atlas+nacos leg, of which the POST is 2–8 ms — the rest is gRPC + these logs + scheduling; the JSON-marshal log is ~10–20 µs of CPU per event but forces a synchronous write into the lumberjack file on every event under burst — at 20 k events/s the log I/O becomes the serial prefix's bottleneck).

**User-visible scenario.** At burst scale the adapter's own log volume (3 lines × 700 B per event × 20 k events/s ≈ 40 MB/s) saturates the log file and *becomes* the latency; the INFO level also floods ops dashboards.

**Fix design.** Demote the three lines to Debug (or sample 1/N) — a one-line level change per site; the `rsyncing instance` marshal should be guarded by the level check so the JSON is never built when not logged (today it marshals unconditionally, `client.go:118`). Trade-off: the live-log latency-measurement method this audit used (§4) depends on those lines — keep them behind a config flag (`--log-push-payload`) rather than deleting, so DS-4's observation harness can still measure end-to-end from logs when the §3 metric is not yet deployed.

---

## 6. METRIC DESIGN SUMMARY (the patch description, consolidated)

> Full rationale in §3; this is the actionable form. **Not implemented — design only.**

| # | File | Change |
|---|---|---|
| 1 | `pkg/worker/types.go:23-27` | `Event` gains `TriggerTime time.Time` (monotonic-safe origin) |
| 2 | `internal/ports/ports.go:75-79` | `ports.Event` gains the same field |
| 3 | `pkg/providers/k8s/k8s.go:125-127` | pass `obj.CreateAt` into the submit closure; set `TriggerTime` (keep line 118's `triggerTime` for compat) |
| 4 | `pkg/metrics/stat.go` | new `EventToStoreDurationHistogram *prometheus.HistogramVec` (labels `sink`, `outcome`; buckets 1 ms→10 s exponential-ish) + `EventsDroppedTotal prometheus.Counter`; register in `init()` (stat.go:58-60) |
| 5 | `internal/ports/ports.go:90-100` | `MetricsRecorder` gains `ObserveEventToStoreDuration(sink, outcome string, d time.Duration)` and `IncEventsDropped()` |
| 6 | `internal/infra/metrics/metrics.go` | `Recorder` implements both via the vec/counter (mirror `SetSyncErrorQueueDepth`, metrics.go:79-81) |
| 7 | `pkg/worker/worker.go:103-112`, `pkg/discoverycenter/client.go:190-199` | the two `nopMetricsRecorder`s gain no-op methods |
| 8 | `pkg/worker/fanout.go` (`NewFanoutSink`, 134-160) | wrap each `NamedSink.Sink` in a recording decorator (implements `ports.InstanceSink`; observes `time.Since(triggerTime)` per `Push`/`PushAll`, labels = the sink's name, outcome by error) — covers the retry path (`PushTo`, fanout.go:222-230) automatically |
| 9 | `pkg/k8srobot/k8srobot.go:197-199` | `default:` branch counts the drop (`metrics.EventsDroppedTotal.Inc()`) — requires a recorder injection or a package-level hook; smallest form: an exported `k8srobot.SetDropObserver(func())` called from the provider wiring |
| 10 | tests | nacosmock delay-injection → bucket+label pin; 500-injection → `outcome=error` + retry re-observation ≥ 5 s; whitebox `CreateAt=-1s` → duration ≥ 1 s; mutations: drop field-copy (3), drop decorator (8), drop drop-counter (9) each fail their test |

**Interpretation contract (what dashboards/alerts may claim).** The series measures *from the informer callback's `CreateAt`* — upstream (kube-apiserver → watch → informer store) is excluded BY DESIGN (out of this system's control and unobservable here); it measures *to the store's 2xx* — nacos-internal replication skew is excluded (documented proxy choice, §3). `outcome=error` observations measure the failed attempt only; the successful retry's duration is a separate (later) observation — an alert on p99 must select `outcome="ok"` and pair with the error-rate and drop counters or it lies by survivorship (§4's drop simulation is the proof).

---

## 7. PROPOSED SLOs (for the user to ratify)

Derived from the measurements: real per-POST 2–8 ms (loopback demo nacos; production network adds RTT), live steady-state end-to-end 60 ms, prefix 10 µs, sequential-fan-out coupling 1:1, 3000-instance burst = 3000 round trips.

| SLO | Steady state (single instance change, no burst) | Burst (3000 simultaneous instance changes) |
|---|---|---|
| **End-to-end latency** (CreateAt → nacos 2xx, `outcome=ok`) | p50 < 50 ms, **p99 < 100 ms** | p50 < 500 ms, **p99 < 1 s** |
| **Convergence** (burst start → LAST instance visible in nacos) | — | **≤ 10 s** (with DS-2-1 concurrency ~8: 3000 × 5 ms / 8 ≈ 1.9 s floor on the demo server; 10 s leaves 5× headroom for a real network) |
| **Completeness** | `events_dropped_total` = 0 over any 5-min window | **0 drops per burst** (SLO is the conjunction — latency percentiles without the drop count are survivorship-biased, §4) |
| **Isolation** | nacos p99 unaffected by Atlas latency ≤ 10× nominal | same (the DS-2-2 fix makes this automatic; without it, Atlas at 2 s ⇒ nacos at 2 s = SLO violated on a healthy nacos) |

**"Millisecond-level" operationally, restated.** The *per-store-write* latency is already milliseconds (2–8 ms). What the requirement can operationally mean — and what these SLOs encode — is: (1) the adapter's own machinery adds ≤ 10 ms to any change's path to a healthy store (today: ~10 µs prefix + fan-out coupling; after DS-2-2: ~10 µs + 1 store round trip); (2) a single change is end-to-end visible in ≤ 100 ms p99 (today ~60 ms real — passes, barely, and invisible without §3's metric); (3) thousands of simultaneous changes converge in seconds, not minutes (today: est. ~15 s serial floor + drop-then-heal-in-60 s; after DS-2-1/2/4: ~2–4 s), with zero silent loss. Ratification notes for the user: the steady-state p99<100 ms budget assumes the store's own write RTT ≤ 50 ms (a cross-DC nacos would consume it all — the SLO should then be re-stated per network class); the burst convergence ≤ 10 s assumes the nacos cluster absorbs 8-way concurrent writes without the 50-parallel degradation the demo showed at 50-way (the knob exists to be tuned down).

---

## Appendix — reproduction

- Harness: `/Users/donghongshuai/go/src/gitlab.mfwdev.com/paas/ds2-latency-audit/` (sibling module, `replace spotter =>` the repo; `go build -o ds2harness .`; modes: `raw|sink|chain|burst|conv|btree|reuse|drop`; the local mock is loopback-only on a dynamic `httptest` port; no reserved port touched, no repo file modified). Delete at will — it is throwaway.
- Live probes: `curl http://127.0.0.1:18848/nacos/v1/ns/service/list?...` (GET only) and the running spotter's `build/demo/app.log` timestamp pairing (the python one-liners are in the audit transcript; the method is: pair `k8s.go:116` lines with the first same-pod `nacos.go:378` line within 5 s, discarding pairs whose event line is immediately followed by the pending-pod WARN).
- Nacos batch-register verification: nacos.io v1 and v2 OpenAPI docs (this session): `/nacos/v1/ns/instance` and `/nacos/v2/ns/instance` single-instance; `instance/metadata/batch` metadata-only Beta.
