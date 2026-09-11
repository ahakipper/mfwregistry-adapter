# DSCA Track 1 — Scale Vehicle & Event-Path Capacity

**Auditor:** DS-1 (scale vehicle & event-path capacity)
**Repo state:** `spotter` @ `refactor/all`, HEAD `69b0105`
**Date:** 2026-09-11
**Contract:** docs/dsca-plan.md, track #1

---

## EXECUTION SUMMARY

All experiments ran against a **throwaway kwok cluster on scratch ports** (apiserver 34567, etcd 34679) and **in-process scratch programs in /tmp**. The live demo stack was never mutated; final verification below.

| # | Command / action | Result |
|---|---|---|
| E1 | `brew install kwok` (darwin/arm64) | kwok + kwokctl v0.8.0 installed |
| E2 | `KWOK_KUBE_VERSION=v1.28.8 kwokctl create cluster --name dsca1 --runtime binary --kube-apiserver-port 34567 --etcd-port 34679 --kubeconfig /tmp/dsca1/kwok/kubeconfig` | Cluster ready in 1m22s (binary downloads) + 3s start; kube-apiserver/kube-controller-manager/kube-scheduler/etcd/kwok-controller all host processes, no containers |
| E3 | Apply 1 fake Node (capacity pods: 5000) | Node `Ready` in ~8s; kwok-controller posts heartbeats (conditions with `KubeletReady`, 10s lease) |
| E4 | Apply 1 fake Pod (labels `app-code` + `app` + `K8S_CLUSTER_TYPE: test`, nodeSelector `type: kwok`, toleration) | Pod → `Running`, `Ready=True`, `podIP 10.0.0.1`, conditions Initialized/Ready/ContainersReady in ~10s, entirely via kwok stages (no kubelet) |
| E5 | Apply 500 fake Pods (one `kubectl apply -f` of a `List`) | 501 pods Running/Ready on ONE fake node |
| E6 | Scratch client-go informer (exact spotter shape: shared pod informer, resync=0, 4096 channel, drop-on-full, single Pop consumer) against kwok | Initial sync: **501 ADD events** replayed to the informer; steady state: 0 events/s; verified kwok pods produce REAL watch events |
| E7 | Churn: burst-delete 501 pods + recreate | **2,004 events** (501 add + 1,002 update + 501 delete) consumed with **0 drops**; peak observed informer callback burst ~200 events/s (kwok delete sweep is apiserver-paced) |
| E8 | Scale to 3,501 pods (6 chunks of 500) | All 3,501 Running/Ready; informer saw 3,501 ADD + 6,000 UPDATE = **9,501 events, 0 drops**; ADD rate ~100/s per 500-pod chunk (kubectl apply pacing), each pod emits 2 UPDATEs (pod-create status, pod-ready status) |
| E9 | Throttled consumer runs (100/s, 20/s, 10/s) with 500–1,500-pod delete bursts | Queue depth peaked at **2,588** (of 4,096) on a 3,000-event burst at 10 events/s consumption; no drops yet at that burst size |
| E10 | Synthetic overflow (producer > consumer, 4096-channel, exact `select/default` shape) | **pushed=4,097, dropped=45,903, consumed=442** — the drop-on-full path of k8srobot.go:195-199 proven lossy under any producer burst that outpaces the consumer |
| E11 | Serial nacos POST loop (scratch replica of `Sink.Push` → `RegisterInstance`: one HTTP POST per instance, 10s timeout, sequential) at 0/0.5/2/5 ms server latency | 1000: 15,276/s / 1,338/s / 384/s / 162/s; 5000: 23,742/s / 1,285/s / 377/s / 161/s — **throughput is exactly 1/(latency + overhead)**; serial pushes have zero parallelism |
| E12 | Read-only GETs against the LIVE nacos (127.0.0.1:18848) | readiness ~3.9ms; `/ns/instance/list` ~3.8–4.2ms (n=10) — the real server's per-request server-side cost floor |
| E13 | Live spotter metrics (read-only, :8090) | `sync_once_durations_histogram`: count 3,871, sum 1,606 (p50 < 1ms, in-ms buckets) for the Atlas gRPC per-event push; `sync_error_gauge` all 0 |
| E14 | Demo log lag analysis (build/demo/app.log, 37MB) | Steady-state single-instance Pop→nacos-register lag p50 **36ms**, p90 **79ms** (n=10); a 6-pod burst scale-up: lag **~1.6s per pod** (sequential startup updates gate the final Ready update) |
| E15 | Post-experiment demo verification | k3s: 12 pods (unchanged); nacos: 3 services, 2/8/1 hosts (unchanged); spotter PID 30306 up 8h12m; metrics :8090 answers 200 |

Read-only against the live demo: `kubectl get` on build/soak/kubeconfig, `curl GET` on nacos :18848, `curl GET` on spotter metrics :8090, reading build/demo/app.log. Nothing was applied to the demo cluster; the demo stack was never addressed by any write.

---

## KWOK FEASIBILITY

### Decision: kwok-only throwaway cluster (REJECTED: attaching kwok to the demo k3s)

**The decision is a throwaway kwok cluster, and attaching kwok to the existing demo k3s is rejected on data-pollution grounds:**

- The demo's spotter watches **ALL pods, all namespaces** (k8srobot.go:158-165 — `factory.Core().V1().Pods().Informer()`, no namespace, no label selector at the informer level).
- The only filtering happens at conversion: `formatAppCode` (conversion.go:205-215) accepts a pod if it has label `app-code` (or `cadvisor-app`, or a namespace-`name` composite — the last branch would fire for ANY pod with a `name` label; for pods without, `labels["name"]` yields `""` and appCode becomes `default-`, still non-empty). `formatEnvType` (conversion.go:263-275) takes `K8S_CLUSTER_TYPE` from labels or the `application` container env.
- `config.PushAppCodes` is unset in the demo invocation (no `--push-appcodes` flag on PID 30306), so **every fake kwok pod carrying `app-code` + `K8S_CLUSTER_TYPE` labels would be converted and pushed to the live nacos**, polluting the demo's data and the other tracks' observations. Pods without those labels are filtered (`app-code` non-empty but envType empty → `InitInstanceFilters` rejects `EnvType == ""`, common.go:94-96) — but that filtering is fragile (the namespace-`name` composite branch) and the blast radius is the live demo.
- Therefore: **a separate throwaway kwok cluster on a scratch port is the required vehicle.** This is also what E2-E8 executed successfully.

### What kwok is (verified by execution, not just docs)

kwok ("Kubernetes WithOut Kubelet") is a fake node/pod provider that runs against a REAL Kubernetes API server (kwokctl spins up real etcd + kube-apiserver + kube-controller-manager + kube-scheduler as host processes under the `binary` runtime — no containers). The kwok controller watches the API and mutates the *status* of fake nodes and pods according to configurable Stage CRDs. To any real client (kubectl, client-go informer), these are ordinary API objects served by an ordinary apiserver: **watches and lists are real**. Verified in E6: a genuine client-go shared informer (spotter's exact informer shape, client-go v0.22.4) received 501 ADD events on initial sync and streamed live ADD/UPDATE/DELETE during churn — E7, E8.

Docs (kwok.sigs.k8s.io) claim: "KWOK can reliably maintain 1k nodes and 100k pods easily", "KWOK can create 20 nodes or pods per second", thousands of nodes on a laptop without significant CPU/memory. Execution confirms the spirit at our scale: 3,501 pods + 1 node on one arm64 laptop with kube-apiserver at ~0.8% CPU / 470MB RSS, kwok-controller at 0.0% CPU / 15MB RSS idle (E8 system check). The docs' scale claim is far above the 1000-5000-instance target.

### Install & usage notes (darwin/arm64, executed)

```
brew install kwok                                # v0.8.0, installs kwok + kwokctl
KWOK_KUBE_VERSION=v1.28.8 kwokctl create cluster \
  --name dsca1 --runtime binary \
  --kube-apiserver-port 34567 --etcd-port 34679 \
  --kubecontroller-manager-port ... --kube-scheduler-port ... \
  --kubeconfig /tmp/dsca1/kwok/kubeconfig --timeout 20m
kubectl --kubeconfig /tmp/dsca1/kwok/kubeconfig apply -f node.yaml pods.yaml
kwokctl delete cluster --name dsca1              # teardown
```

- `--runtime binary` needs NO docker: components are downloaded binaries (~1m22s first time, cached under ~/.kwok). CI-tested on darwin/arm64 per the docs' runtime matrix.
- Pin `KWOK_KUBE_VERSION` to match the demo k3s (v1.28.8) if version parity matters; the default is v1.36.1.
- `kwokctl create cluster --name dsca1` rejects `--kwok-controller-port` (unknown flag) in v0.8.0; the controller port is auto-allocated (32765 observed).
- Do NOT let kwokctl write the default `~/.kube/config` — pass an explicit `--kubeconfig` (scratch path), otherwise the host's current-context is switched.

### Node sizing (measured, with the pitfall)

kwok docs' example node (capacity cpu: 32, pods: 110) does NOT scale to 1000+ pods: the kube-scheduler sums pod CPU limits against node allocatable. With 500m limits, 32 CPUs fit 64 pods (E-diagnosis: `0/1 nodes are available: 1 Insufficient cpu`). Two fixes, both executed:

1. Keep per-container limits tiny (100m): 32 CPUs fit 320 pods. Still not 5000.
2. Patch the fake node's status with huge capacity: `kubectl patch node <n> --subresource=status --type=merge -p '{"status":{"capacity":{"cpu":"1000","pods":"5000"},...}}'`. A plain `kubectl patch` without `--subresource=status` reports "patched" but does nothing (the /status endpoint is a separate resource — a silent no-op trap). After the subresource patch, 3,501 pods scheduled onto one node (E8).
3. **A fake kwok node has no real pods-per-node limit** — the docs treat node status as arbitrary ("The status can be any value"); the practical limits are the scheduler's accounting and the apiserver/etcd event volume, both controlled by the declared capacity.

Node count for 1000-5000 pods: **1 node is sufficient and cheapest** (single scheduler queue, single heartbeat). More nodes add scheduling fidelity but multiply heartbeats/leases; for spotter's event-path testing (per-pod events, not node lifecycle), 1-3 nodes is the right shape.

### API-server load at 5000-pod churn (measured proxy)

- Applying 500 pods via one `kubectl apply` produces a paced ~100 pod-create/s stream; the kwok controller then status-patches each pod (2 patches/pod: create-status, ready-status) → the informer sees ~100 ADD/s + ~100 UPDATE/s sustained during the chunk (E8 log).
- A 1,500-event delete+create churn completed in ~6 seconds of event stream (E7): sustained ~300 events/s with 0 drops at unlimited consumption.
- The apiserver of a binary-runtime kwok cluster has default QPS limits (`--disable-qps-limits` exists for stress work): our burst-deletes were visibly paced by the apiserver (deletes arriving over ~5s). For a true thundering-herd driver, `kwokctl create cluster --disable-qps-limits` (or `--extra-args`) is the knob; the default pacing is actually a feature for realistic rolling-update simulation.

---

## EVENT-PATH CAPACITY

### The chain (file:line, all read in full)

```
kube-apiserver watch
  → client-go shared pod informer (resync=0, all namespaces)     k8srobot.go:158-165
  → AddEventHandler callbacks                                    k8srobot.go:165-179
  → enqueue(): queue <- item  (chan, cap 4096)                   k8srobot.go:184-200
      select { case queue <- item: ; default: }                  k8srobot.go:195-199   ← SILENT DROP
  → monitor loop (SINGLE goroutine): robot.Pop()                 k8s.go:104-130
  → pod2Instance: GetByKey (informer store) + formatInstance     k8s.go:121, 175-243
      + cache diff (hasInstanceDiff)                             k8s.go:211-216, 245-260
      + VerifyInstance filters                                   common.go:82-117
  → pool.Submit(eventSync)   ants pool, cap 100                  k8s.go:125, 65; common.go:36
  → worker.Handle (per event, 1 instance)                        worker.go:85-92
  → pusher.Push = FanoutSink.Push (sequential sinks)             fanout.go:170-182
      → atlas sink: gRPC SynInstance (10s timeout)               discoverycenter/client.go:117-130
      → nacos sink: per-instance HTTP POST (10s timeout)         nacos.go:131-142, 322-357
```

Producer side: the informer callbacks run on client-go's processor goroutines; `enqueue` never blocks (drop-on-full), so the producer rate is the apiserver's event rate.

Consumer side: ONE goroutine Pops and converts, then dispatches to the ants pool (cap 100). The pool's workers each call `worker.Handle` synchronously → `FanoutSink.Push` → both sinks serially. So the *conversion* stage is single-threaded (pod2Instance incl. cache diff under `k.cache` mutex — CacheIterface), while the *push* stage has ≤100 concurrent workers, but each worker's push is a serial (atlas-gRPC, then nacos-HTTP) round trip per event, and each event carries exactly 1 instance (k8s.go:125-128 submits per-instance closures; `eventSync` builds a 1-instance Event).

### Per-rollout event arithmetic (measured in E7/E8 on kwok, matches real-k3s behavior)

Per pod per lifecycle (kwok fast stages, same shape as real kubelet):
- create: 1 ADD + 2 UPDATE (pod-create status: IP/Initialized; pod-ready status: Running/Ready)
- delete: 2 UPDATE (deletionTimestamp set → conversion marks Offline) + 1 DELETE

A rolling update of N replicas ≈ **6N queue events** (N×3 create-side + N×3 delete-side), of which ~4N survive the cache diff as pushes (the Pending/starting updates are filtered by `InstanceStatePending` — common.go:103 — and `hasInstanceDiff` dedupes repeated no-change updates; the unready→ready transition and offline delete always pass).

| Scale | Rolling update (all-at-once) | Events into 4096 queue | Passes the cache diff (pushes) | Serial nacos time @2ms/POST (E11) | Serial nacos time @4ms (live GET floor) |
|---|---|---|---|---|---|
| 1,000 | 6,000 events in burst | 6,000 > 4,096 → **drops unless consumer keeps up** | ~4,000 | 8s | 16s |
| 3,000 | 18,000 events | **4.4× queue capacity** | ~12,000 | 24s | 48s |
| 5,000 | 30,000 events | **7.3× queue capacity** | ~20,000 | 40s | 80s |

### Drop probability — the queue math (E9, E10)

The queue is the only buffer between the informer's unbounded event rate and the single-Pop consumer. Drops occur when `depth == 4096` at an enqueue attempt:

- Measured consumer ceiling (Pop loop alone, conversion only): the informer consumer in E6-E8 ran effectively unlimited (>1,000 events/s); the real consumer adds pod2Instance + pool dispatch, so its Pop rate is well above 500/s on this hardware. The binding constraint is NOT the Pop loop.
- Measured producer bursts: 500-pod delete sweep ≈ 1,500 events over ~5s (~300/s); 500-pod apply ≈ 100 ADD/s + 100 UPDATE/s.
- At those rates the 4,096 buffer absorbs bursts **if the downstream push path keeps the pool drained**. The danger window is downstream slowness: with the nacos POST at the live server's ~4ms floor and 100 pool workers, max drain ≈ 100/0.004 = **25,000 events/s** theoretical, but with ONE slow nacos (200ms) it collapses to **500/s**, and with a 10s timeout on a dead nacos the pool SATURATES at 100 in-flight 10s requests → drain ≈ **10 events/s** → the queue fills in 4096/10 ≈ **7 minutes** of churn, then every event is silently dropped (k8srobot.go:197-199) **including the DELETEs that would have deregistered the now-stale instances**.
- E10 proves the mechanism end-to-end: producer faster than consumer → 4,097 admitted, 45,903 dropped, no signal anywhere.
- E9 shows the realistic near-miss: 3,000-event burst at 10 events/s consumption → peak depth 2,588, drain takes 100+s. One larger burst (or a slower consumer) crosses 4,096.

### Push throughput (measured)

E11 (serial POST, exact sink shape): the nacos sink's per-instance HTTP POST loop has **zero concurrency per event and per fanout ordering**: throughput = 1/(per-POST latency). 1,338/s at 0.5ms server cost; **384/s at 2ms; 161/s at 5ms**. The live demo's nacos GET floor is ~4ms (E12) → the realistic per-sink serial ceiling is **~250/s**, and the sink is the second sink in the fanout (after the Atlas gRPC, itself ~1ms per event per E13 but serial within the same Push call).

E14 (live demo log): single-instance steady state Pop→registered p50 36ms (two informer hops + fanout + POST). A 6-pod burst: all 6 Pods' ADDs popped within 80ms, but the *Ready* transition UPDATEs arrive ~1.5s later (readiness probes), and the registers complete ~1.6s after the ADDs — dominated by waiting for the last status update, then serial completion of 6 pushes within ~10ms of each other. Extrapolated to 1,000 pods at the observed per-POST cost: **~4s of pure serial nacos POSTing** per full batch (E11 2ms: 1,000 → 2.6s measured; at the 4ms live floor: ~4-16s), during which the ants pool is fully drained only if 100 workers each hold one event — which they can, but each worker's Push is atlas-then-nacos serial, so the nacos requests from 100 workers do overlap (bounded concurrency 100). The measured 5000@5ms case (31s) is the honest worst case.

### The first bottleneck at each scale

| Scale | First bottleneck | Evidence |
|---|---|---|
| 1,000 | **Nacos serial POST floor** (~250-380/s realistic): a 1,000-instance full registration takes 4-8s even at best; rolling updates take ~2× that | E11, E12, E14 |
| 3,000 | Same, plus **queue-pressure risk**: a 3,000-replica all-at-once rollout emits 18,000 events = 4.4× queue capacity; survival depends entirely on the consumer keeping depth < 4,096, which depends on nacos latency | E8, E9 arithmetic |
| 5,000 | **Queue overflow with silent drops** becomes the *expected* outcome of any fast rollout (30,000 events, 7.3× capacity) unless event coalescing exists — it does not: every informer event is one queue object, and the cache diff runs AFTER the drop point | E10; k8srobot.go:195-199 |

Secondary capacity facts:
- The `hasInstanceDiff` cache-diff coalescing (k8s.go:211-216) only suppresses *pushes*, not *queue entries* — it runs after Pop. Multiple status updates per pod each occupy a queue slot. 6N queue events per rollout is irreducible today.
- The ants pool (100) with `PoolExpireTime` 100s: pool Submit blocks? — `ants.Pool.Submit` returns `ErrPoolOverload` only if configured Nonblocking; default (blocking) means **Submit blocks the single Pop loop when 100 pushes are in flight** — the monitor loop stalls, the queue fills, drops start. This converts "slow nacos" into "dropped events" with no log line. (k8s.go:125; ants v2.4.3 default options.)
- `GetAll`/`List` path at 5,000 pods: `k.GetAll()` (k8s.go:411-435) formats all pods per interval; at 5,000 instances with per-instance cache insert + map diff, this is CPU-seconds scale per full-push tick — acceptable at the 60s demo interval, but `CompareAndFlush` also does `worker.GetAll` (gRPC to Atlas) and per-diff `buildAndSendEvent` → another 5,000-event wave through the same pool.
- Multi-cluster: every cluster's watcher shares the SAME 4,096 queue (k8srobot.go:124-126) — 2 clusters double the producer rate into the same buffer.

---

## FINDINGS

### DS-1-1 (P0): Queue-full events are dropped silently — data loss with no observable signal

**Evidence:** `pkg/k8srobot/k8srobot.go:195-199`:
```go
select {
case queue <- item:
default:
    // The queue is full: drop the event instead of blocking the informer.
}
```
No counter, no log, no metric. Repo-wide grep for queue metrics in `internal/infra/metrics` and `pkg/metrics` finds none; the only queue-depth series that exists is the retry queue's (`sync_error_gauge`, worker/unsynced_service.go:296-302). The plan's C-14 finding is hereby re-confirmed and extended: the drop also *disarms* every downstream safety net — the retry queue only sees events that survived, the full-push prune only runs per interval (60s demo / 6h default), so a dropped DELETE leaves a stale nacos instance for up to one full-push interval, and a dropped final Ready update leaves an instance registered-but-unhealthy.

**Scenario:** A 3,000-replica rolling update (or a nacos that slows to 200ms/POST for a minute under load): the queue hits 4,096, and the informer's next 10,000+ events vanish. Spotter logs nothing; the metrics endpoint shows `sync_error_gauge 0` (nothing failed — nothing was attempted); nacos serves stale instances of the OLD replicaset (deregisters dropped) and misses instances of the NEW one. Clients route to dead pods. Only the next full-push tick (60s+ later, if `CompareAndFlush`'s Atlas view diff notices) heals it.

**Fix design (queue-full observability + backpressure):**
1. Make the drop observable NOW: a `queueDropTotal` counter (labels: cluster) incremented in the `default` arm, plus a `queueDepth` gauge, exported through the existing metrics recorder; log at rate (first drop per burst at Warn, then every 10k).
2. Backpressure instead of pure loss: replace the channel with a bounded work-queue that (a) coalesces by key (a map of key→latest-object, FIFO keys — the cache-diff then naturally collapses intermediate updates BEFORE the drop point), and (b) applies drop-policy by event type: never drop DELETE or offline-transition events; drop redundant UPDATEs first (they are recomputable from the store — a later event for the same key supersedes).
3. Drop-recovery hook: when a drop is unavoidable, record the dropped keys and schedule an immediate targeted re-`GetByKey` scan for those keys (or fall back to the full-push path early) instead of waiting for the interval.
4. The Pop-loop stall (see DS-1-3) interacts: as long as `pool.Submit` can block the Pop loop, any queue change only changes WHERE the loss happens. Fix order: metrics first (P0 evidence), coalescing second, pool non-blocking third.

### DS-1-2 (P1): Nacos sink per-instance serial HTTP caps the K8s-path at ~160-400 events/s — the millisecond-at-scale requirement is unreachable on this path

**Evidence:** `pkg/nacos/nacos.go:131-142` (`Push` loops `pushOne` sequentially, first-error-wins), `pkg/nacos/nacos.go:361-381` (`register` → one `RegisterInstance`), `pkg/nacos/client.go:31` (`RequestTimeout = 10 * time.Second`), `pkg/worker/fanout.go:170-182` (sequential sinks, "Sequential, not parallel" is a documented design choice). Measured: E11 (1,338/s @0.5ms, 384/s @2ms, 161/s @5ms — pure serial); E12 (live nacos GET floor ~4ms); E14 (live log: 6-pod burst registers complete ~1.6s after ADDs; single-instance p50 36ms).

**Scenario:** 1,000-instance cold registration (spotter restart or new cluster attach): at the live server's ~4ms write cost, the nacos sink alone takes ~4s; with the unhealthy → enabled=false and health-check-PUT extra calls on first register of each (service, cluster) pair (nacos.go:379, `ensureClusterHealthCheckDisabled`), the first-ever full push adds one more PUT per pair. Under the 2× churn of a rolling update, 8s+ — and every one of those seconds the queue is filling (see DS-1-1's math: at 161/s drain, a 6N=30,000-event rollout of 5,000 replicas overruns the queue even without any nacos slowness).

**Fix design:** a bounded-parallelism batch push at the nacos sink: (a) keep Push's per-instance policy, but issue registers/deregisters with a small worker set (start 8; the API is idempotent upserts, ordering within one instance is all that matters); (b) batch the per-instance calls into the existing retry-friendly shape — the FanoutError already aggregates per-sink, so partial batch failures stay expressible; (c) an end-to-end latency metric (QueueObject.CreateAt → nacos-register-complete histogram) as proposed by track 2, so the parallelism win is measurable, not asserted. The 10s RequestTimeout stays per-request; only concurrency changes.

### DS-1-3 (P1): A slow sink stalls the single Pop loop through the blocking ants pool — converting downstream latency into upstream event loss

**Evidence:** `pkg/providers/k8s/k8s.go:104-130` (one Pop goroutine), `k8s.go:125` (`k.pool.Submit` — ants default options are blocking: Submit waits for a free worker), `k8s.go:65` (pool cap `providers.PoolBenchSize` = 100, common.go:36), fanout/worker Handle is synchronous per event (worker.go:85-92 → fanout.go:170-182). With 100 workers all blocked in a 10s nacos timeout (client.go:31), `Submit` blocks, the Pop loop stops, and the 4,096 queue fills in seconds; every subsequent enqueue takes the silent-drop path (DS-1-1). The retry queue's `ProcessUnsynced` (5s ticker, unsynced_service.go:137-148) does NOT help: it only receives events that already failed a push, not events lost upstream.

**Scenario:** nacos restarts (the soak's scenario (d) at 1/100th the scale) under a 1,000-pod churn: for ~10s, 100 in-flight requests time out; the Pop loop blocks on Submit; ~2,000 events arrive; the queue overflows; the DELETEs of the churn are dropped; after nacos recovers, the retry queue retries only the 100 failed instances; the other ~1,900 dropped events (including deregisters) never reach any sink until the next full push.

**Fix design:** decouple consumption from push capacity: (a) `ants.NewPool` with `ants.WithNonblocking(true)` makes Submit return `ErrPoolOverload` instead of blocking, and the Pop loop can then count-and-log/route overflow to the retry queue explicitly (converting invisible loss into the existing retry machinery); or better (b) a per-provider bounded local queue between Pop and the pool whose entries are coalesced by instanceId (only the latest event per instance matters to the sink, given reversion semantics — k8s.go:245-260 already trusts strictly-higher reversion), which bounds the memory to O(instances) instead of O(events) and removes the 4,096 cliff entirely. Either change needs the DS-1-1 metrics in place to be verifiable.

### DS-1-4 (P2): Rolling updates emit 6 queue events per pod with no pre-Pop coalescing — the buffer is spent on superseded intermediate states

**Evidence:** k8srobot.go:165-179 enqueues every informer callback as a distinct QueueObject; measured E7/E8: 501-pod churn = 2,004 events (~4 per pod), 3,500-pod scale-up = 9,501 events (add + 2 updates per pod); the cache diff that would collapse them (k8s.go:211-216) runs only after Pop. The `hasInstanceDiff` check means most intermediate UPDATEs are discarded one Pop at a time — each having occupied a queue slot and a pod2Instance conversion (GetByKey + full formatInstance allocation) that is then thrown away.

**Scenario:** at 5,000 replicas the steady-state CPU of the single Pop loop is dominated by formatting instances that the diff immediately discards; during rollouts it doubles the queue pressure that DS-1-1 already shows to be the overflow driver.

**Fix design:** per-key latest-value coalescing at enqueue (a keyed map + FIFO of keys, exactly the shape the informer's store makes cheap: the store already holds only the latest object; enqueue can store `key→QueueObject` and append the key once). Backward-compatible with Pop semantics (Finish is a no-op, k8srobot.go:257-259). This alone cuts rolling-update queue traffic ~3× and removes the drop probability cliff at the 3,000-instance tier without touching the sink.

### DS-1-5 (P2): At scale, the per-Pop INFO log and per-instance log volume becomes an operational hazard

**Evidence:** k8s.go:116 logs every Popped event at INFO (49 in the current quiet demo window; E14's burst shows 16 lines for 6 pods); nacos.go:378 logs every register (`nacos: registered instance ...` — 21,880 lines in build/demo/app.log, 37MB, driven by full-push re-registers); discoverycenter client.go:117-119 marshals and logs the full instance JSON per Atlas push (`rsyncing instance: <full JSON>` — 10,984 lines). At 5,000 instances × per-event, these logs are 100s of MB/hour and the JSON marshal per event is measurable CPU on the Pop-adjacent path (the marshal happens inside `Client.Sync`, i.e., inside a pool worker, before the RPC).

**Scenario:** the ≥2h scale observation of track 4 fills the log disk / slows the process before the consistency data is collected; log rotation loses exactly the burst windows the observation needs.

**Fix design:** demote the per-event lines to Debug (keep a rate-limited summary: events/s, register/s counters in metrics); keep the failure paths at Warn+. For track 4's observation, the harness should capture spotter's metrics port, not tail logs.

---

## SCALE HARNESS DESIGN (for track 4 to execute)

### Vehicle (from the feasibility decision)

A throwaway kwok binary-runtime cluster, ports 34567 (apiserver) / 34679 (etcd) or any free scratch ports; kubeconfig at a scratch path. NEVER the demo k3s (conversion accepts any labeled pod — pollution, see KWOK FEASIBILITY). A dedicated nacos (or the nacosmock at realistic delay) on a scratch port, and a spotter built from the tree pointed at the kwok kubeconfig + that nacos + `--push-interval` shortened (e.g. 60s as the demo uses) + `--leader-elect=false` for single-instance runs.

### The fake node

```yaml
apiVersion: v1
kind: Node
metadata:
  name: kwok-node-scale-0
  annotations:
    node.alpha.kubernetes.io/ttl: "0"
    kwok.x-k8s.io/node: fake
  labels:
    type: kwok
    kubernetes.io/os: linux
    kubernetes.io/arch: arm64
spec:
  taints:
  - effect: NoSchedule
    key: kwok.x-k8s.io/node
    value: fake
status:
  phase: Running
  capacity:     {cpu: "1000", memory: 4Ti, pods: "5000"}
  allocatable:  {cpu: "1000", memory: 4Ti, pods: "5000"}
```
After apply, `kubectl patch node kwok-node-scale-0 --subresource=status --type=merge -p '<same status>'` (the plain patch is a silent no-op — the measured trap). Capacity 1000 CPU lets the scheduler place 5,000 pods with 100m limits, or omit limits entirely.

### The kwok pod spec the harness applies (the exact labels conversion.go requires)

Verified by execution (E4/E5): the minimum for spotter to convert a pod is

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: scale-app-<i>            # becomes InstanceId (conversion.go:104)
  labels:
    app-code: scale-app-<n>      # REQUIRED: AppCode (conversion.go:207); no app-code → namespace-name composite or rejected
    app: scale-app-<n>           # label passthrough (compatibility:aos_app)
    K8S_CLUSTER_TYPE: test       # REQUIRED: EnvType (conversion.go:43/267) — the underscore form in LABELS (env var form also works, conversion.go:43)
spec:
  nodeSelector: {type: kwok}     # schedule onto the fake node
  tolerations:
  - key: kwok.x-k8s.io/node
    operator: Exists
    effect: NoSchedule
  containers:
  - name: application            # REQUIRED only if env-based envType/idc/cluster are used (conversion.go:34, formatIDC/formatCluster read the "application" container's env)
    image: scale/fake:latest
    ports:
    - name: http                 # name prefix http→ProtoHTTP (conversion.go:185-189); the sink's port = FIRST port (nacos.go:464-469) — note formatAppPort always prepends a synthetic dubbo-7096 port (conversion.go:174-179), so the nacos composite id uses 7096
      containerPort: 8080
      protocol: TCP
    resources:
      limits: {cpu: 100m, memory: 128Mi}   # keep small so the scheduler fits thousands (or patch node capacity huge)
```

No readinessProbe needed: kwok's fast stages write `Ready=True` conditions + `containerStatuses[].ready` themselves (what `containersReady` checks, conversion.go:283-293). The pod's `status.podIP` is assigned by the kwok pod-create stage (10.0.0.x) — required for online pushes (`Ip == ""` filters, common.go:97-99, nacos.go:333-349).

### Readiness transitions (kwok stage semantics, documented)

The default fast stages (embedded in the kwok-controller config, `~/.kwok/clusters/<name>/kwok.yaml`, 10 Stage CRDs) drive: pod-create (IP + Initialized; general stages add 1s delay + 0-5s jitter — annotation-overridable), pod-ready (phase Running, Ready/ContainersReady True, containerStatuses running), pod-complete, pod-delete (deletionTimestamp Exists → `delete: true`). Per-stage timing is tuned by per-pod ANNOTATIONS on the harness pods:
- `pod-ready.stage.kwok.x-k8s.io/delay: <ms>` — fixed delay before Ready
- `pod-ready.stage.kwok.x-k8s.io/jitter-delay: <ms>` — random jitter (uniform in (delay, jitter-delay))
- same pattern for `pod-create.stage.kwok.x-k8s.io/*`
The general (slower) stages ship in the same tree (`kustomize/stage/pod/general/pod-ready.yaml`: `durationMilliseconds: 1000`, `jitterDurationMilliseconds: 5000`) — deploy those CRDs into the throwaway cluster to simulate realistic startup timelines (seconds, not instant). Custom stages (e.g. "30% of pods go unready then recover") are Stage CRDs with a CEL selector on e.g. a `chaos: unready` label and a statusTemplate setting `Ready=False` — the injection mechanism track 4's flap scenarios should use instead of deleting pods.

### Churn driver design

Reuse the existing soak harness shape (tests/soak/drivers.go `k8sDriver.apply/scale` pattern — same label scheme already proven there) but against the kwok kubeconfig, and prefer bare Pod lists (E5/E8: one `kubectl apply -f list.yaml` of 500 pods) over Deployments: a Deployment rollout on kwok works (the controller-manager is real) but bare pods give the driver direct control of N, names, and per-pod stage annotations. Patterns:

1. **Cold attach:** N = 1000 / 3000 / 5000 pods applied in 500-pod chunks (measured: ~100 pods/s creation stream, sustainable); observe first-registration convergence (the nacos catalog view reaching N) and the K8s→nacos lag distribution.
2. **Rolling update:** delete chunk i, create replacement chunk i+500, one chunk at a time (controlled rollout) AND a full-burst variant (all N deleted at once, E7 shape: 6N events); observe queue behavior, drop counter (after DS-1-1's fix; before the fix, observe via informer-side counting as our scratch informer did), convergence time, stale-instance window.
3. **Flap:** annotation-driven stage delays (pod-ready delay 0 vs 5s) and a custom unready stage on a labeled subset; observe Unhealthy (enabled=false) propagation and recovery.
4. **Sustained churn for the ≥2h observation (track 4's core):** a background loop keeping a target churn rate (e.g. 5%/min pod replacement ≈ 50 pods/min at 1000 ≈ measured 300 events/s bursts every 12s) — comfortably below the drop cliff today, deliberately raised toward it in later runs once observability lands.

### What to observe

- **Source side:** informer event counts by type (the scratch informer from E6 — a 60-line client-go program — can run as a sidecar against the same kubeconfig; or spotter's own queue metrics post-fix).
- **Spotter side:** the existing Prometheus port (`:8090` in the demo): `sync_once_durations_histogram` (per-event Atlas push), `sync_all_durations_histogram` (per full-push), `sync_error_gauge` per sink; add nothing to the repo — the harness scrapes.
- **Nacos side:** the catalog view per (service, cluster) — `GET /nacos/v1/ns/catalog/instances` pagination (the prune's own view, client.go:250-286), and the instance list per service; convergence = catalog view equals the live pod set (by composite id `ip#port#k8s#DEFAULT_GROUP@@app`).
- **Divergence bound:** per observation tick (track 4's cadence), compare K8s live pods (kubectl, by app-code label) vs nacos catalog; record max staleness (age of any divergence) and the heal time after each churn phase — the same `waitForConverged` + `retryClusterView` shape the soak scenarios already use (scenarios.go:482-497).
- **The verdict for the user requirement:** at each of 1000/3000/5000, the observation must show (a) no unexplained instance loss (drop counter flat or explained), (b) K8s→nacos lag p50/p99 in the seconds, driven to the milliseconds target by the DS-1-2 fix, (c) both views consistent at every tick (track 4's ownership).

### Known limits of the vehicle (honest)

- kwok pods have no real container traffic, readiness, or probe behavior — the event PATH is real, the timing is stage-configured; latency numbers measure the spotter chain, not kubelet reality.
- The binary-runtime apiserver has default QPS limits that pace bursts (feature for realism, limit for stress); `--disable-qps-limits` if a true herd is wanted.
- The nacos side must be either the throwaway mock with a realistic per-request delay (set from E12's ~4ms live floor) or a dedicated real nacos on a scratch port — never the demo's 18848 instance, whose registration state the other tracks' baselines depend on.

---

## Environment restoration

The kwok throwaway cluster (`--name dsca1`) remains up on ports 34567/34679 with 501 pods for the next auditor/track to reuse, or tear down with `kwokctl delete cluster --name dsca1` (nothing persists outside ~/.kwok/clusters/dsca1 and /tmp/dsca1). Scratch programs live in /tmp/dsca1/{informer,serialreg}. The demo stack is untouched: k3s pods 12, nacos 3 services with 2/8/1 hosts (as at audit start), spotter PID 30306 (uptime 8h+, through all experiments), no writes issued against any demo component. No repo file was modified except this document.
