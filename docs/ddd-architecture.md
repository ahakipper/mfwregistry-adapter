# spotter — Target DDD Architecture Design

Status: design document for the `refactor/all` branch. This is the target
architecture that the later implementation phases will follow. It audits the
current codebase against Domain-Driven Design principles and defines an
explicitly layered design with dependency inversion at every boundary.
Companion documents: [architecture.md](architecture.md) (current-state
behavior), [data-model.md](data-model.md) (Instance field mappings),
[operations.md](operations.md) (runbook). This document defines where the
code is *going*.

---

## 1. Executive Summary

### 1.1 Why refactor

The refactor is driven by observable engineering failures, not aesthetics.
Three classes of evidence, all reproducible on the current tree:

1. **Nil-logger test crashes.** `go test ./pkg/providers/consul/watch/`
   crashes with SIGSEGV. The `watch` package calls `log.Logger.Error(...)`
   (watcher.go:81, watcher.go:97) on a package-global `*zap.SugaredLogger`
   (`pkg/log/log.go:11`) that is `nil` unless `log.LoggerInit()` ran first.
   The watch tests never initialize it, and when the consul dial fails the
   error branch dereferences the nil pointer. The same latent crash exists
   in 19 non-test files that reference `log.Logger` (17 with live calls;
   cmd/adapter.go and pkg/providers/aggregate/controller.go contain only
   commented references).
2. **Untestable external dependencies.** Tests in `pkg/providers/consul`
   (monitor_test.go:19, 55, 68, 86) and `pkg/providers/consul/watch`
   (watcher_test.go:11-12) dial hardcoded consul endpoints
   (`10.72.73.172:8520`, `172.16.129.37:8520`); `pkg/discoverycenter`
   tests (client_test.go:13) dial a hardcoded Atlas gRPC endpoint
   (`172.18.27.63:50051`) by mutating the ambient global
   `config.GrpcAddr` (client_test.go:16). These are not "bad tests": the
   *production* constructors force them to be this way —
   `NewDiscoveryCenter()` (registry.go:27-35) panics unless a live gRPC
   connection can be established, `ClientFactorySimple.ConsulClientFactory()`
   (client_factory.go:45, 59) probes `Status().Leader()` over the network
   inside what is effectively a constructor call, and `NewEtcdClient()`
   (etcd.go:33-36) dials etcd and calls `MemberList`. There is no seam to
   inject a fake.
3. **`go vet` build failures.** `go vet ./tools/cache/` fails on
   cache.go:52 (`fmt.Sprintf call has arguments but no formatting
   directives`), and the full `go vet ./...` reports 20 further findings:
   discarded context cancel functions in pkg/etcd, pkg/distribute,
   pkg/discoverycenter and internal/server.go; an unkeyed struct literal
   in k8s.go:42; self-assignments in client_test.go.
These are all symptoms of the same architectural causes, catalogued in
Section 2: package-level mutable globals (`log.Logger`, `notice.Noticer`,
21 exported vars in config/config.go:3-34), `panic()` in constructors
(cmd/adapter.go:44, 47, 99, 118; registry.go:29), hidden network clients
constructed deep inside constructors, `time.Sleep`/ticker loops with
hardcoded durations, and configuration read as ambient global state from
six different packages.

### 1.2 Goal

An explicitly layered DDD design in which:

- every unit is testable in isolation (pure domain functions need no
  infrastructure; everything else is tested through narrow ports with
  fakes),
- the dependency rule is enforced by package layout: `interfaces ->
  application -> domain`, with infrastructure implementing
  application-owned ports and wired only in a single composition root,
- runtime behavior is **byte-for-byte compatible**: the same CLI flags, the
  same etcd keys, the same env presets, the same metrics names, the same
  Instance wire shape (see 4.h and the single accepted exception in 4.i.4).

---

## 2. Current-State Audit (module-by-module inventory)

Legend for "Disposition": **keep** (move unchanged), **refactor** (same
responsibility, new shape), **move** (relocate), **delete** (dead code),
**wrap** (keep the implementation, hide it behind a port).

### 2.1 Entry points and configuration

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| main.go:19-21 | `cmd.Execute()` | none | Interfaces | keep |
| cmd/root.go:29-45 | cobra root command | `cfgFile` declared (root.go:26) but the `--config` flag that would set it is commented out (root.go:53) — dead var; viper file discovery (root.go:64-87) is dead in practice because no config file is ever read by adapter | Interfaces | keep, delete dead var |
| cmd/root.go:54-60 | logging flags | flag values are copied into config globals later (adapter.go:110-116) | Interfaces | keep |
| cmd/adapter.go:28-70 | `adapter` command: flag parsing, env preset, notice init, server start | panics on bad env (adapter.go:44, 47); `err` from `GetString` checked *after* `env` is used (adapter.go:42-48); stale `err` re-checked after `InitNoticeClient` (adapter.go:50-52); server error only printed, process exits 0 (adapter.go:54-58); commented-out signal handling (adapter.go:62-68) — no graceful shutdown at all | Interfaces | refactor |
| cmd/adapter.go:95-130 | `initAdapterFlags` writes 14 config package vars directly (the preset functions write 8 more), then `log.LoggerInit()` | the composition happens by mutating package globals — there is no object to hand to `NewServer()` | Interfaces (composition root) | refactor |
| config/config.go:3-34 | 21 exported mutable package vars | ambient global state read from 6 packages (GrpcAddr x5 sites, PushAppCodes x6, Providers x5, PushAllInterval x3 — see grep counts); presets mutate the same globals | Infrastructure (config loader) | refactor into immutable struct |

### 2.2 Orchestration

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| internal/server.go:21-44 | `Server`: leader state machine, provider lifecycle, metrics server | embeds `sync.Mutex` (server.go:43); `Providers` field is write-mostly (server.go:162, 182, 185) | Application (leader lifecycle use case) + composition root | refactor |
| internal/server.go:47-77 | `NewServer` | reads `config.Providers`/`config.MetricsAddr` ambient globals; `ecancel` leaked on the error path (server.go:62-65, vet); elector construction hardwires etcd (see 2.6) | Composition root | refactor |
| internal/server.go:80-146 | `Run` loop over `leaderChCh` | mixed layers in one loop: leadership policy, provider start/stop, notice, metrics; `s.stopProviderFunc()` called without nil check in the stop case (server.go:138) — nil dereference if the server is stopped before it ever became leader | Application | refactor |
| internal/server.go:222-258 | `InitializeProviders` | reads `config.*` ambient globals; constructs concrete providers — hidden dependencies | Composition root | move |

### 2.3 Provider layer

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/providers/iface.go:8-15 | `Provider` interface | `CompareAndFlush()` takes no arguments and returns nothing — the remote side comes from the injected worker, making the contract untestable without a live worker | Application port (`InstanceSource` + use case) | refactor |
| pkg/providers/iface.go:18-27 | `KVMProvider` interface | **dead**: declared, never implemented, never referenced anywhere else in the tree (verified by grep) | — | delete |
| pkg/providers/common.go:11-59 | state/status/env/label constants | `InstanceStateStarting` (common.go:12) and `InstanceStateOOM` (common.go:14) have no references (verified by grep) | Domain | move (drop 2 dead consts) |
| pkg/providers/common.go:61-63 | `FullPushInterval = 21600 * time.Second` | duplicates the `--push-interval` default in cmd/adapter.go:88 — two sources of truth | Domain (policy constant) | move |
| pkg/providers/common.go:74-80 | `ListToMap` | pure; fine | Domain | move |
| pkg/providers/common.go:82-117 | `InitInstanceFilters` | pure validation policy, but lives beside provider plumbing; error strings double as control flow | Domain | move |
| pkg/providers/cache.go:11-17 | `CacheIterface` (typo in name) | fine as a contract; name typo | Domain (only the interface) | move + rename |
| pkg/providers/cache.go:27-133 | b-tree cache with deep copy | depends on google/btree + mohae/deepcopy — cannot live in a stdlib-only domain; `Delete` returns `item.(*InstanceCacheItem).Instance` after a `btree.Delete` that can return nil (cache.go:88-90) — latent nil type-assertion panic, and `Delete` has no production callers anyway | Infra-internal implementation of the domain interface | move (interface -> domain, btree impl -> infra-side package); drop `Delete` |
| pkg/providers/aggregate/controller.go | unfinished aggregate controller | **dead**: every method body is commented out (controller.go:32-134); package-global `Providers` slice + `RegisterAggregateProvider` (controller.go:9-15) has no callers (verified by grep) | — | delete |

### 2.4 Kubernetes provider

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/providers/k8s/k8s.go:36-72 | `NewK8SProvider` | constructs `k8srobot.NewRobot` directly (k8s.go:50) — no seam; ants pool error silently discarded (`p, _ := ants.NewPool(...)`, k8s.go:65) | Infrastructure adapter | refactor |
| pkg/providers/k8s/k8s.go:86-140 | `monitor`: sync gate + event loop | busy-wait gate `for { if HasSynced() break; ... time.Sleep(15s) }` (k8s.go:88-97) — sleeps are not cancellable; `time.Sleep(1s)` in the pop loop (k8s.go:113) | Application (incremental sync loop) | refactor |
| pkg/providers/k8s/k8s.go:226-239 | `hasInstanceDiff` — Reversion-based diff rule | **domain logic living as a private method of an infra object**; duplicated (consul.go:348-364, unsynced_service.go:43) | Domain | move |
| pkg/providers/k8s/k8s.go:272-363 | `CompareAndFlush` three-way diff | business policy (which side wins, mark-offline rules, inconsistency notices) entangled with cache + worker + metrics + config (`config.PushAppCodes` read at k8s.go:295-305); duplicated in consul.go:306-406 with diverging rules (see 4.j) | Domain (diff) + Application (two thin use cases) | refactor |
| pkg/providers/k8s/k8s.go:253-269 | `flushInstances` | **dead**: no callers (verified by grep) | — | delete |
| pkg/providers/k8s/k8s.go:408-428 | `ProcessIntervalFullPush` | ticker with hardcoded fallback `providers.FullPushInterval`; directly touches `metrics.SyncAllK8sDurationsHistogram` (k8s.go:421) | Application | refactor |
| pkg/providers/k8s/conversion.go:17-127 | `formatInstance` pod -> Instance | reads ambient `config.PushAppCodes` deep inside conversion (conversion.go:68-75) — a pure function made impure; logs via global logger (conversion.go:71) | Infra k8s converter (pure parts -> Domain) | refactor |
| pkg/providers/k8s/conversion.go:130-168, 171-203, 205-215 | labels, ports, appcode | pure except logger at :71; hardcoded dubbo port 7096 injected into every instance (conversion.go:174-179) | Infra k8s converter | refactor |
| pkg/providers/k8s/conversion.go:218-249, 251-263, 313-365 | `formatStatus`, `formatEnvType`, `formatState` | pure state machines — currently unexported and untestable outside the package | Domain (state/status mapping) | move |
| pkg/providers/k8s/conversion.go:285-310 | cpu/memory formatting | `formatCpuSize` logs via global logger (conversion.go:291); memory divisor heuristic (i-suffix -> 1024, else 1000) is policy | Infra k8s converter | refactor |

### 2.5 Consul provider

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/providers/consul/consul.go:37-70 | `NewConsulProvider` | builds the client factory + monitor (consul.go:43-49); constructor name typos (`NeweClientFacotorySimple`, client_factory.go:21) | Infrastructure adapter | refactor |
| pkg/providers/consul/consul.go:94-122 | `syncInstance` | creates **two** new caches and throws one away (consul.go:98-100); locking discipline manual | Application (incremental sync) | refactor |
| pkg/providers/consul/consul.go:185-236 | `extractDiff` — Reversion-based cache diff | **domain logic as a private method**; mutates inputs (sets Status/Enabled/State on add, consul.go:213-215; on delete, consul.go:227-229) | Domain | move |
| pkg/providers/consul/consul.go:306-406 | `CompareAndFlush` | near-duplicate of the k8s one, with different diff fields and different offline semantics (`Status = 2` at consul.go:395 vs `Enabled=false, Status=3, State=terminated` at k8s.go:350-352) | Domain + Application | refactor (deduplicate) |
| pkg/providers/consul/convertion.go:38-120 | `convertInstance` endpoint -> Instance | pure mapping (no globals); file name typo ("convertion") | Infra consul converter | refactor |
| pkg/providers/consul/convertion.go:223-272 | `convertState`/`convertSatus` (typo) | health-check -> state/status policy, pure | Domain (mapping rules) | move |
| pkg/providers/consul/monitor.go:16-22 | `Monitor` interface | reasonable seam already — but handlers appended without a lock (monitor.go:178-184) | Infra consul adapter port | wrap |
| pkg/providers/consul/monitor.go:85-121 | `watchConsul` blocking watch | **latent bug**: `time.Sleep(blockQueryWaitTime * time.Second)` (monitor.go:103) multiplies a `time.Duration` by `time.Second` — 5s x 1s = 5e18 ns ≈ 158 years, on the client-factory failure path; notices on every failed poll (monitor.go:101, 113) | Infra consul adapter | refactor |
| pkg/providers/consul/monitor.go:123-143 | `updateRecord` debounce | `int64(refreshIdleTime.Seconds())` is `int64(0.05) == 0` (monitor.go:133), so the "50 ms debounce" is really "any change seen since the last 50 ms tick" — untestable time logic | Infra consul adapter | refactor |
| pkg/providers/consul/monitor.go:161-176 | `updateInstanceRecord` | invokes handlers with a fabricated empty `&api.CatalogService{}` (monitor.go:167, own TODO) — the handler contract is a lie | Infra consul adapter | refactor |
| pkg/providers/consul/monitor.go:35 | `watcher *watch.Watcher` field | **constructed-but-unused**: imported and typed, never assigned (verified by grep for assignments) | — | delete field |
| pkg/providers/consul/client_factory.go:9-13 | `ConsulClientFactory` + `DefaultConsulClientFactory` | the package-level `DefaultConsulClientFactory` (client_factory.go:13) is declared and never assigned or used (verified by grep) | Infra consul adapter | keep iface, delete global |
| pkg/providers/consul/client_factory.go:34-75 | client probing | `Status().Leader()` network probe inside the getter (client_factory.go:45, 59) — makes every caller integration-bound | Infra consul adapter | wrap behind port |
| pkg/providers/consul/watch/* | consul watch plans, nodes snapshot, race watcher | **entire subpackage is unused by production code** (only reference: the dead field monitor.go:35); its tests crash (nil logger) or dial real consul; `watcher.go:31` package var `watchPlans` is shadowed by the struct field and dead; `close(planStopCh)` (watcher.go:99) is executed by every plan goroutine — double-close panic with >1 plan; `Watch` returns the outer `err` instead of `perr` (watcher.go:100); hardcoded watch of service "redis" (watcher.go:135) | — | delete |

### 2.6 Push layer and worker

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/worker/types.go:5-10 | `Worker` interface | mixes event routing, retry orchestration and remote reads in one contract | Application port (`InstanceSink` + `EventQueue`) | refactor |
| pkg/worker/types.go:27-30 | `EventResource` struct | **dead**: never referenced (verified by grep) | — | delete |
| pkg/worker/worker.go:17-31 | `NewResourceWorker` | calls `discoverycenter.NewDiscoveryCenter()` (worker.go:24) which panics on connection failure (registry.go:29); starts a goroutine in the constructor (worker.go:28) — untestable | Application service (composed in root) | refactor |
| pkg/worker/worker.go:39-57 | handlers | `e.Data[0].InstanceId` indexed without a length check (worker.go:44) — latent panic | Application | refactor |
| pkg/worker/unsynced_service.go:31-55 | `Add` keeps highest Reversion per instanceId | correct semantics, private | Application (retry queue) | refactor (expose test seams) |
| pkg/worker/unsynced_service.go:57-84 | `Sync` loop | hardcoded `5000 * time.Millisecond` ticker (unsynced_service.go:58); touches global metrics collector (unsynced_service.go:72); infinite loop, no way to observe/drain for tests | Application | refactor |
| pkg/worker/elector.go:34-50 | `NewElector` | constructs the etcd client + candidate internally — no seam | Infra etcd adapter behind `LeaderElector` port | wrap |
| pkg/worker/elector.go:83-90 | `syncStoppedState` | `for { select { case <-ctx.Done(): w.stopped = true } }` — after the channel closes the loop spins forever on zero-value receives (never returns) | Infra etcd adapter | fix in place |
| pkg/worker/worker_fack.go | `FackWorker`/`FackPusher` | **test-only code shipped in the production package**; its only caller is consul_test.go:15 (verified by grep); duplicates worker.go wholesale | — | delete (replaced by a fake `InstanceSink` in tests) |

### 2.7 Discovery center client and data model

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/discoverycenter/registry.go:13-17 | `Pusher` interface | good contract, wrong owner (infra package owns the port the application needs) | Application port (`InstanceSink`) | move |
| pkg/discoverycenter/registry.go:27-35 | `NewDiscoveryCenter` | **panics** on connection failure (registry.go:29) | Infra gRPC adapter | refactor (return error) |
| pkg/discoverycenter/registry.go:37-58 | `Push` | reads ambient `config.DisablePushWorker` (registry.go:38); mixes "should we push" policy with transport | Infra gRPC adapter | refactor |
| pkg/discoverycenter/client.go:105-123 | `getConnect` | reads ambient `config.GrpcAddr` (client.go:109); `time.Sleep(5s)` retry loop (client.go:112); discarded cancel funcs (client.go:43, 64, 80, 106 — vet) | Infra gRPC adapter | refactor |
| pkg/discoverycenter/client.go:79-103 | `GetAll` | one RPC per requested status plus a client-side provider filter loop (client.go:93-99) — transport detail leaking policy | Infra gRPC adapter | keep semantics, move filter decision |
| pkg/beehive/service/v2/v2.go:27-111 | Instance/PortInfo/InstanceList/CommonResponse + request types | the de-facto domain model wearing an infra costume (it exists to mirror protobuf wire types) | Domain (`internal/domain/instance`) | move |
| pkg/beehive/service/v2/v2.go:115-159 | hand-rolled `InstanceServiceClient` | infra client; wire-compatibility limitation documented at v2.go:1-16 (plain structs, not generated proto messages — needs a JSON codec on the server side) | Infra gRPC adapter | move |

### 2.8 Distributed coordination

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/distribute/election/election.go:21-43 | `Candidate` interface | exposes 7 methods; `Resign()` (election.go:30), `LeaseID()` (election.go:42) and `Tag()` (election.go:36) have no external callers (verified by grep) | Infra etcd adapter | shrink port |
| pkg/distribute/election/election.go:67-97 | `NewCandidate` | reads ambient `config.LockCampaignKey` (election.go:91); discarded cancel (election.go:102, 154 — vet) | Infra etcd adapter | refactor |
| pkg/distribute/election/election.go:122-149 | `Wait` | `time.Sleep(2s)` polling loop (election.go:129); error compared by `!=` sentinel (election.go:131) and by `err.Error() != "context deadline exceeded"` string match (election.go:158) | Infra etcd adapter | refactor |
| pkg/distribute/discovery/discovery.go | `Node` register/keepalive/watch | **entire package is dead**: no file in the tree imports `spotter/pkg/distribute/discovery` (verified by grep); it is the sole consumer of `tools/cache` and `tools/net` | — | delete |
| pkg/etcd/etcd.go:12-40 | `NewEtcdClient` | reads 4 ambient config vars; dials + `MemberList` probe in the constructor (etcd.go:33-36); discarded cancel (etcd.go:33 — vet) | Infra etcd adapter | refactor |

### 2.9 Cross-cutting support

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| pkg/log/log.go:11 | `var Logger *zap.SugaredLogger` | **the** root cause of the SIGSEGV class: 19 non-test files reference this global (17 with live calls; cmd/adapter.go and aggregate/controller.go only in comments); nil until `LoggerInit()` runs | Infra logging adapter behind `ports.Logger` | refactor |
| pkg/log/log.go:13-73 | `LoggerInit` | reads 7 ambient config vars; level built by `int8` cast of an int flag (log.go:19) | Infra logging adapter | refactor (constructor takes a struct) |
| pkg/metrics/stat.go:8-60 | six collectors + `init()` `MustRegister` | `SyncAllDurationsHistogram` (stat.go:9-17) is only referenced from the commented-out aggregate controller — unused; `init()` side effect makes import order observable | Infra metrics adapter | refactor (explicit registration) |
| pkg/metrics/proserver.go:16-45 | metrics/pprof HTTP server | package-global `srv` (proserver.go:16) — `Stop()` nil-derefs if `Start()` never ran; `log.Logger.Fatal` in a library (proserver.go:29); `mock()` dead (proserver.go:36-45); `http.Handle` on the DefaultServeMux (proserver.go:28) | Interfaces (entrypoint) | refactor |
| pkg/notice/notice.go:9-26 | `Noticer` global + `Notice()` | global mutated at startup; fire-and-forget goroutine per notice (notice.go:20-25) | Infra notice adapter behind `ports.Notifier` | refactor |
| pkg/notice/notice.go:29-48 | `GetLocalIP` | a network utility living in the notice package | Infra (util) | move |
| pkg/notice/appcenternotice/appcenternotice.go | local mirror of the notice client | logs through the global `log.Logger` (nil-guarded at :77) — the guard is a symptom, not a fix | Infra notice adapter | keep as implementation |
| pkg/k8srobot/k8srobot.go:78-93 | `Robot` interface | already a decent seam — but only the concrete constructor exists (k8srobot.go:120) | Infra k8s adapter | wrap behind `InstanceSource` |

### 2.10 Tools

| File | Current role | Problems (evidence) | Target layer | Disposition |
| --- | --- | --- | --- | --- |
| tools/cache/cache.go | TTL table cache | **vet build failure** at cache.go:52; only importer is the dead `distribute/discovery` package (verified by grep) | — | delete (with its consumer) |
| tools/gpool/pool.go | goroutine pool | **never imported** anywhere (verified by grep); the app uses `ants` instead | — | delete |
| tools/net/ip.go | local IP lookup | only importer is the dead `distribute/discovery` package (verified by grep); duplicates `notice.GetLocalIP` | — | delete (or merge into infra util) |
| tools/unit/humantime.go | relative time formatting | pure; used by k8s.go:261, 422 and consul.go:297 | Infra util | keep |
| tools/recover.go | `WithRecover` | pure; used by server.go:159, 189 and unsynced_service.go:65 — but the default handler swallows panics silently (recover.go:3-5) | Infra util | keep (log through injected logger) |

### 2.11 Test files (current state)

| File | Status | Root cause |
| --- | --- | --- |
| pkg/providers/k8s/k8s_test.go | passes offline (605 lines of pure conversion goldens) | none — this is the regression suite the refactor must preserve |
| pkg/providers/consul/watch/watcher_test.go:11-12 | SIGSEGV (nil `log.Logger`, watcher.go:81/97) | global logger |
| pkg/providers/consul/watch/race_watcher_test.go:10-11 | SIGSEGV + real consul dials | global logger + hardcoded addresses |
| pkg/providers/consul/consul_test.go:17 | dials `10.72.73.172:8520`; hangs or fails | constructor performs network probes; deleted in Phase C (5) and rewritten in Phase D |
| pkg/providers/consul/monitor_test.go:12-14, 19, 55, 68, 86 | dials real consul; `init()` runs `LoggerInit()` which writes `app.log` into the source tree (the tracked file `pkg/providers/consul/app.log` is dirty in git after a test run) | global logger + ambient config defaults |
| pkg/discoverycenter/client_test.go:13-16 | dials `172.18.27.63:50051` by mutating `config.GrpcAddr` | ambient config + constructor dials |

---

## 3. Target DDD Layering

### 3.1 The four layers

1. **Domain** (`internal/domain/instance`) — the Instance aggregate, value
   objects, and the pure business rules: Reversion-based diff/merge,
   validation policy, state/status enums and their mapping rules, env-code
   composition. Zero dependencies: no interfaces to infrastructure, no
   logging, no clock, no `time.Sleep`, no I/O. It compiles with only the
   standard library. Only the *interface* of the instance cache lives here;
   the b-tree/deep-copy implementation (which pulls in google/btree and
   mohae/deepcopy) lives in an infra-side package and implements it.
2. **Application** (`internal/app/...`) — use cases that orchestrate domain
   types over ports. Two primary use cases (`IncrementalSync`,
   `FullSync`) plus the leader lifecycle use case, the retry queue, and the
   application-owned ports in `internal/ports`.
3. **Infrastructure** (`internal/infra/...`) — adapters that implement the
   ports by wrapping concrete technology: client-go informers (k8srobot),
   consul api + monitor, the gRPC discovery-center client, etcd election,
   zap logging, the notice client, Prometheus metrics, the config loader.
   Each adapter owns its external client construction and returns errors —
   never panics.
4. **Interfaces / entrypoints** (`cmd/`, `internal/composition`, the
   metrics/pprof HTTP server) — cobra CLI parsing and the **composition
   root**: the single place where concrete adapters are constructed and
   injected into application services.

### 3.2 Dependency-rule diagram

```
                    +-----------------------------------+
                    |        INTERFACES (entrypoints)   |
                    |  cmd/ (cobra CLI)                 |
                    |  internal/composition (DI root)   |
                    |  internal/infra/metrics/http      |
                    +-----------------+-----------------+
                                      | constructs & injects
                                      v
+-----------------------+     +-----------------------------------+
|   INFRASTRUCTURE      |     |          APPLICATION              |
| internal/infra/...    |<----|  internal/app/sync                |
|  k8s (robot+convert)  | impl|  internal/app/fullsync            |
|  consul (mon+convert) |     |  internal/app/leader              |
|  grpcdiscovery        |     |  internal/app/retry               |
|  etcd (election)      |     |  consumes only ports from         |
|  logging (zap)        |     |  internal/ports                   |
|  notice               |     +-----------------+-----------------+
|  metrics              |                       | uses
|  config               |                       v
+-----------------------+     +-----------------------------------+
        ^                     |            DOMAIN                |
        | may import domain   |  internal/domain/instance        |
        | types, never the    |  Instance, PortInfo, EnvCode     |
        | reverse             |  diff / merge / validation /     |
                              |  state & status mapping          |
                              |  (pure functions, stdlib only)   |
                              +-----------------------------------+

RULES
  1. interfaces  -> application -> domain          (compile-time arrows)
  2. infra -> implements application ports, may import domain types
  3. NOTHING imports infra except internal/composition and cmd
  4. domain imports nothing from this repository and nothing external
```

### 3.3 Package mapping

| Current package/file | Target package | Notes |
| --- | --- | --- |
| `pkg/beehive/service/v2` (types, v2.go:27-111) | `internal/domain/instance` | package name `v2` dies; wire-format structs stay |
| `pkg/beehive/service/v2` (client, v2.go:115-159) | `internal/infra/grpcdiscovery` | gRPC client surface |
| `pkg/providers/common.go` (constants, filters, ListToMap) | `internal/domain/instance` | drop 2 dead state consts; `InitInstanceFilters` swaps github.com/pkg/errors for stdlib errors during the move |
| `pkg/providers/common.go:65-70` (`RuntimeConfig`) | `internal/domain/instance` | value object, moves alongside the model |
| `pkg/providers/cache.go:11-17` (interface) | `internal/domain/instance` | rename `CacheIterface` -> `InstanceCache`; drop `Delete` (zero callers + nil-assertion bug) |
| `pkg/providers/cache.go:27-133` (b-tree impl) | infra-side cache package (e.g. `internal/infra/cache`) | implements the domain interface; keeps google/btree + mohae/deepcopy out of the domain |
| k8s `hasInstanceDiff` (k8s.go:226-239) | `internal/domain/instance/diff.go` | named policy `DiffNewerReversion` |
| consul `extractDiff` (consul.go:185-236) | `internal/domain/instance/diff.go` | named policy `DiffEqualRevision`-family rules |
| k8s+consul `CompareAndFlush` diff policy | `internal/domain/instance/compare.go` | shared primitives; TWO thin provider-specific use cases wrap them (4.j) — no forced unification |
| `formatState`/`formatStatus`/`formatEnvType` rules | `internal/domain/instance/state.go` | pure functions on `PodObservation` (see 4.h) |
| consul `convertState`/`convertSatus` rules | `internal/domain/instance/state.go` | health-check -> state/status policy |
| `pkg/providers/k8s/conversion.go` (pod plumbing) | `internal/infra/k8s/converter.go` | builds `PodObservation` from `*v1.Pod` + `QueueObject`, no policy |
| `pkg/providers/consul/convertion.go` (endpoint plumbing) | `internal/infra/consul/converter.go` | |
| `pkg/k8srobot` | `internal/infra/k8s/robot` | unchanged implementation |
| `pkg/providers/consul/monitor.go` + `client_factory.go` | `internal/infra/consul` | behind `InstanceSource` |
| `pkg/discoverycenter` | `internal/infra/grpcdiscovery` | implements `InstanceSink` |
| `pkg/distribute/election` + `pkg/etcd` | `internal/infra/etcd` | implements `LeaderElector` |
| `pkg/log` | `internal/infra/logging` | implements `ports.Logger` |
| `pkg/notice` + `pkg/notice/appcenternotice` | `internal/infra/notice` | implements `ports.Notifier`; `GetLocalIP` becomes a `LocalIPProvider` helper used only for the leader-change notice text |
| `pkg/metrics/stat.go` | `internal/infra/metrics` | implements `ports.MetricsRecorder`; preserves LinearBuckets(0, 1000, 10) and the label-value-as-name convention |
| `pkg/metrics/proserver.go` | `internal/infra/metrics/http` (entrypoint) | owned by composition root |
| `pkg/worker` (worker.go, unsynced_service.go) | `internal/app/sync` + `internal/app/retry` | the use cases |
| `pkg/worker/elector.go` | `internal/app/leader` (use case) + `internal/infra/etcd` (adapter) | split |
| `internal/server.go` | `internal/app/leader` + `internal/composition` | split |
| `cmd/root.go`, `cmd/adapter.go` | `cmd/` + `internal/composition/root.go` | flags stay, wiring moves |
| `config/config.go` | `internal/infra/config` | immutable struct builder |
| `tools/unit`, `tools/recover` | `internal/infra/util/...` or stay `tools/` | keep |
| `tools/cache`, `tools/gpool`, `tools/net` | — | deleted (dead, see 2.10) |
| pkg/providers/aggregate, pkg/distribute/discovery, pkg/providers/consul/watch, pkg/worker/worker_fack.go, `KVMProvider`, `flushInstances`, `EventResource`, `DefaultConsulClientFactory` | — | deleted (dead, see 2.3-2.7, 2.10) |

---

## 4. Key Design Decisions

### (a) Logger and Notifier: kill the package globals

**Problem.** `log.Logger` (log.go:11) is nil until `LoggerInit()` runs; 21
files call methods on it; watch tests crash; `notice.Noticer` (notice.go:9)
is the same pattern one level up.

**Decision.** Define a narrow `Logger` port (see 4.d) in `internal/ports`.
Every application service and infra adapter receives a `Logger` as a
constructor parameter. The zap adapter (`internal/infra/logging`) implements
it. A nil-safe no-op implementation (`ports.NopLogger`) is the default in
tests and the zero-value fallback. Same treatment for `Notifier`
(replacing `notice.Notice`, 17 call sites in 6 files).

**Rationale.** This is the direct, mechanical fix for the nil-logger SIGSEGV
class: no code path can observe a nil logger because the dependency is a
value that exists from construction. It also removes the "import order
matters" property (metrics/stat.go:58-60 `init()` registration is treated
the same way).

### (b) Config: immutable struct, built once

**Problem.** 21 exported mutable vars (config/config.go:3-34) written by
cmd/adapter.go:95-130 and read ambiently from 6 packages; tests mutate
`config.GrpcAddr` (client_test.go:16) to aim at real endpoints.

**Decision.** `internal/infra/config` exposes
`Load(flags) (Config, error)` returning an immutable struct (all fields
unexported with getters, or exported-but-never-mutated by convention
enforced by review). The struct is constructed exactly once in the
composition root and passed into constructors. The env presets
(config.go:36-78) become pure functions `PresetFor(env) (Endpoints, error)`
returning values instead of mutating globals. Field provenance is fixed:

- from `PresetFor(env)` — the 8 preset values (config.go:36-78):
  `EtcdEndpoints`, `CertFile`, `KeyFile`, `CAFile`, `KubeConfigPath`,
  `ConsulAddress`, `LockCampaignKey`, `Providers`;
- from flags — the 14 written by `initAdapterFlags` (adapter.go:95-130):
  `LogFilePath`, `LogSize`, `LogLevel`, `LogBackups`, `LogAge`,
  `LogEncoding`, `LogToStd`, `PushAllInterval`, `GrpcAddr`,
  `DisablePushWorker`, `Providers`, `PushAppCodes`, `EnableLeaderElection`,
  `MetricsAddr`.

`Providers` appears in both lists (preset default, flag override); the flag
wins, preserving current behavior.

**Rationale.** Ambient configuration makes every function's behavior depend
on hidden global state — that is precisely why the discoverycenter tests
had to mutate a global. A value type makes dependencies visible in
signatures. The flag names and defaults do not change.

### (c) Constructors: errors, never panics

**Problem.** `NewDiscoveryCenter()` panics (registry.go:29);
`cmd/adapter.go` panics four times (adapter.go:44, 47, 99, 118).

**Decision.** All `NewX(...)` in application and infrastructure return
`(T, error)`. Only `main`/composition decides between fatal exit and
degraded start. Connection establishment moves out of constructors where
practical: the gRPC adapter exposes `Dial() error` so the composition root
can retry or fail fast explicitly; the consul factory keeps its probing but
returns errors instead of leaving a half-initialized client.

**Rationale.** A constructor that dials a network cannot be used in a unit
test; a constructor that panics cannot even be called to see the error.

### (d) Ports (exact signatures)

All ports live in `internal/ports`. `instance` is
`internal/domain/instance`. Signatures are copied from the current code so
the implementation phase is mechanical.

```go
package ports

import (
    "context"
    "time"

    "spotter/internal/domain/instance"
)

// Logger is the narrow logging port. It covers every method currently used
// on the global log.Logger (grep: Info x24, Infof x36, Warn x1, Warnf x13,
// Error x7, Errorf x27); Fatal is intentionally absent — the metrics HTTP
// adapter returns errors instead of exiting.
type Logger interface {
    Info(args ...interface{})
    Infof(format string, args ...interface{})
    Warn(args ...interface{})
    Warnf(format string, args ...interface{})
    Error(args ...interface{})
    Errorf(format string, args ...interface{})
}

// Notifier replaces notice.Notice (notice.go:17-26).
type Notifier interface {
    Notify(title, content string)
}

// Clock replaces direct time.Now / time.Sleep / time.NewTicker usage.
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
}

// InstanceSource is one provider's view of the world. It generalizes the
// current providers.Provider (iface.go:8-15): CompareAndFlush moves to the
// application use case, Run takes the context it currently stores as a field.
type InstanceSource interface {
    // Name is the provider name pushed as Instance.Provider ("k8s" | "ecs").
    Name() string
    // Run starts watching the source and blocks until ctx is cancelled
    // (current: Provider.Run, k8s.go:75-81 / consul.go:72-88).
    Run(ctx context.Context) error
    // Watch returns the incremental change stream. The channel idiom is the
    // taken decision. Current implementations: the robot pop loop
    // (k8s.go:104-130) and the consul monitor instance handlers
    // (monitor.go:161-176 -> InstanceChanged, consul.go:175-178); both become
    // channel feeds inside the adapters. Without this method IncrementalSync
    // has no input.
    Watch(ctx context.Context) <-chan []*instance.Instance
    // GetAll returns every currently valid instance
    // (current: Provider.GetAll, k8s.go:383-403 / consul.go:141-173).
    GetAll() []*instance.Instance
}

// InstanceSink pushes instances to the discovery center. It is the current
// discoverycenter.Pusher (registry.go:13-17) verbatim, plus error returns
// that already exist there.
type InstanceSink interface {
    Push(triggerTime int64, instances []*instance.Instance) error
    PushAll(triggerTime int64, instances []*instance.Instance) error
    GetAll(statuses []int32, provider string) (*instance.InstanceList, error)
}

// LeaderElector is the current worker.Elector (elector.go:13-20) with the
// notification channel made explicit instead of captured in NewElector.
// The etcd adapter receives its context via its constructor (current:
// NewElector's ctx, elector.go:34-50). Stop() is retained for future
// graceful shutdown even though the current Server.Run never calls it
// (Stop is declared at elector.go:78-81 but un-called in internal/server.go).
type LeaderElector interface {
    // ElectWait blocks campaigning and reports every leadership transition
    // on changes (current: leaderChCh wiring, elector.go:34-50, 66-75).
    ElectWait(changes chan<- bool)
    Stop()
}

// EventQueue is the retry queue for failed pushes (current:
// UnsyncedService.Add, unsynced_service.go:31-55) with the test seams the
// current code lacks. Drain() is snapshot-and-iterate: it returns the
// queued events without removing them; the production retry loop drains,
// attempts one push per entry, and re-adds failures (Add applies the same
// keep-highest-Reversion rule, unsynced_service.go:41-47).
type EventQueue interface {
    Add(triggerTime int64, instances []*instance.Instance)
    Len() int
    Drain() []*Event
}

// Event mirrors worker.Event (types.go:21-25) — owned by the application.
type Event struct {
    Trigger int64
    Data    []*instance.Instance
    Operate OperateType // "Sync" | "SyncAll" (types.go:16-19)
}

type OperateType string

const (
    OperateTypeSync    OperateType = "Sync"
    OperateTypeSyncAll OperateType = "SyncAll"
)

// MetricsRecorder replaces direct references to the package-global
// prometheus collectors (stat.go:8-56). The adapter must preserve
// LinearBuckets(0, 1000, 10) (stat.go:16, 25, 34, 40) and the
// WithLabelValues("sync_error_gauge") label-value-as-name convention
// (unsynced_service.go:72).
type MetricsRecorder interface {
    ObserveSyncOnceDuration(d time.Duration)                    // sync_once_durations_histogram
    ObserveSyncAllDuration(provider string, d time.Duration)    // sync_all_durations_histogram{provider}
    SetSyncErrorQueueDepth(n int)                               // sync_error_gauge
    MarkSyncOnce()                                              // sync_once_gauge — set to 1 on every Sync RPC (client.go:51)
}
```

The domain's own contract stays internal and tiny:

```go
// internal/domain/instance

// InstanceCache is the current CacheIterface (providers/cache.go:11-17),
// renamed. Delete is dropped: it has zero production callers and carries
// the flagged nil type-assertion (cache.go:88-90). The b-tree/deep-copy
// implementation moves to an infra-side package so the domain stays
// stdlib-only.
type InstanceCache interface {
    Get(id string) *Instance
    ReplaceOrInsert(ins *Instance) *Instance
    List() []*Instance
    Clear()
}

// InstanceFilter is the current filter type (providers/common.go:72).
type InstanceFilter func(ins *Instance) error

// PodObservation is the neutral observation struct built by the k8s
// converter (conversion.go) from *v1.Pod + QueueObject; the domain rule
// functions map PodObservation -> state/status (conversion.go:218-249,
// 251-263, 313-365 become pure functions over this struct).
type PodObservation struct {
    // Fields mirror exactly what formatStatus/formatState/formatEnvType
    // read from a pod today: phase, container statuses, deletion timestamp,
    // labels, env vars. Introduced in Phase A.
}
```

Two further taken decisions that shape the application layer:

- **ants goroutine pools stay infra-adapter-internal.** Each adapter owns
  its own pool (k8s.go:65, consul.go:59); there is no pool port — the
  bounded-concurrency property is an implementation detail of the adapters.
- **The dormant SyncAll path is kept.** `PushAll` / `OperateTypeSyncAll`
  and its handler (worker.go:49-56, client.go:63-77) stay in the port and
  the gRPC adapter for wire-surface compatibility, marked dormant (the
  periodic path only ever emits `OperateTypeSync`).

### (e) Time control: one Clock, everywhere

**Problem.** `time.Sleep` at k8s.go:95 (15 s), k8s.go:113 (1 s),
monitor.go:103 (buggy 158-year sleep), monitor.go:118, race_watcher.go:38,
client.go:112 (5 s), election.go:129 (2 s); tickers at k8s.go:413,
consul.go:288, monitor.go:125, unsynced_service.go:58 — all hardcoded and
none cancellable via context.

**Decision.** One mechanism: the `Clock` interface above (`Now()` +
`After(d)`). Every loop becomes
`select { case <-clock.After(d): ... case <-ctx.Done(): return }`. Ticker
semantics (fire every d regardless of processing time) are consciously
replaced by after-each-iteration semantics: at the scales involved (50 ms
debounce, 5 s retry, 6 h full push) the drift is irrelevant, and the new
form is cancellable — today `Server.Stop()` cannot interrupt any sleep. A
single abstraction keeps tests uniform: a fake clock advances time instead
of waiting; a real clock is one trivial adapter.

### (f) Retry / unsynced service

**Problem.** `UnsyncedService` is correct but untestable: 5 s hardcoded
ticker (unsynced_service.go:58), global metrics (unsynced_service.go:72),
infinite loop, private store.

**Decision.** Keep the semantics exactly — 5 s cadence (via injected
`Clock`), keep only the highest-`Reversion` event per `InstanceId`
(unsynced_service.go:43), delete-on-success — but implement `EventQueue`
with `Len()`/`Drain()` so tests can assert queue contents after simulated
failures. `Drain()` is snapshot-and-iterate; the production retry loop
drains the queue, attempts one push per entry, and re-adds failures
(the re-add goes through `Add`, which applies the same
keep-highest-Reversion rule, so retries never push stale data over newer
data). The 5 s value becomes a field on the application service,
defaulting to 5 s.

### (g) Dead code: what gets deleted

All verified by grep (no callers outside their own definition):

| Item | Evidence | Action |
| --- | --- | --- |
| `pkg/worker/worker_fack.go` (entire file) | only caller consul_test.go:15 | delete; tests get a fake `InstanceSink` instead |
| `KVMProvider` interface | iface.go:18-27, zero references | delete |
| `pkg/providers/aggregate` (entire package) | controller.go:9-15 registration never called; all methods commented (controller.go:32-134) | delete |
| `pkg/distribute/discovery` (entire package) | zero importers of the package | delete |
| `pkg/providers/consul/watch` (entire subpackage) | only production reference is the unused field monitor.go:35 | delete |
| `DefaultConsulClientFactory` global | client_factory.go:13, never assigned/used | delete |
| `flushInstances` | k8s.go:253-269, no callers | delete |
| `EventResource` type | types.go:27-30, no references | delete |
| `InstanceStateStarting`, `InstanceStateOOM` | common.go:12, 14, no references | delete |
| `SyncAllDurationsHistogram` collector | stat.go:9-17, referenced only in commented aggregate code | delete |
| `PrometheusService.mock` | proserver.go:36-45, no callers | delete |
| `Candidate.Resign`, `Candidate.LeaseID`, `Candidate.Tag` | election.go:30, 42, 36 — no callers | drop from the port |
| `cmd` `cfgFile` var | root.go:26, setting flag commented out | delete |
| `tools/cache`, `tools/gpool`, `tools/net` | gpool never imported; cache+net only by the dead discovery package | delete |

Preserved despite looking odd (they are live behavior): the hardcoded
dubbo port 7096 (conversion.go:174-179), the "all clusters synced" gate
(k8s.go:88-97), the 4096-slot drop-on-full robot queue
(k8srobot.go:195-199), `--appcodes` filtering, `DisablePushWorker`.

### (h) The Instance model and the former mirror package

`pkg/beehive/service/v2` becomes `internal/domain/instance`. The struct
fields (v2.go:27-52), `PortInfo` (v2.go:55-60), `InstanceList` +
`GetInstance` (v2.go:63-73) and `CommonResponse` + getters (v2.go:76-95)
move verbatim — they are the domain model. `RuntimeConfig`
(common.go:65-70) moves alongside them. The gRPC client surface
(v2.go:115-159) and the request messages move to the infra adapter. The
documented wire-format limitation (plain structs are not generated proto
messages; the server must accept a JSON codec — v2.go:1-16) is an accepted
constraint of this refactor and is restated in the package doc comment of
the new location. Renaming the package from `v2` to `instance` is internal
only; the JSON field layout used by the codec is unchanged.

State/status mapping rules are decoupled from the pod type through
**`PodObservation`** (declared in the domain, see 4.d): the k8s converter
builds a `PodObservation` from `*v1.Pod` + `QueueObject`
(conversion.go:17-127), the domain rule functions map `PodObservation` ->
state/status (pure versions of conversion.go:218-249 and 313-365), and the
adapter-level `formatInstance` composes the two. The k8s golden tests
(k8s_test.go) keep testing the adapter-level `formatInstance`, so they move
to `internal/infra/k8s` together with the converter.

**Known non-goal: graceful shutdown.** The commented-out signal handling
(adapter.go:62-68) stays out of scope for this refactor; `Stop` surfaces
exist on the ports but are wired only when a future change adds signal
handling.

### (i) Backward-compatibility invariants (binding on the implementation)

The refactor is internal-only. The following MUST NOT change:

1. **CLI flags** — every flag in cmd/root.go:54-60 and cmd/adapter.go:83-92,
   including shorthands (`-r -t -e -i -g -w -p -m -n -l -a -s -c`), defaults
   and help strings.
2. **Env presets** — the exact endpoint lists, cert paths, kubeconfig lists
   and campaign keys in config/config.go:36-78.
3. **Runtime etcd keys** — campaign key `/paas/spotter`
   (`/paas/spotter-test` in test).
4. **Metrics names/labels** — `sync_all_durations_histogram{provider}`,
   `sync_once_durations_histogram`, `sync_once_gauge`, `sync_error_gauge`
   (stat.go:9-56), and the default `--metrics-addr :8090` with
   `/metrics` and pprof endpoints. The metrics adapter must preserve
   `LinearBuckets(0, 1000, 10)` (stat.go:16, 25, 34, 40) and the
   `WithLabelValues("sync_error_gauge")` label-value-as-name convention.
   **Accepted exception:** the never-incremented
   `sync_all_durations_histogram{provider="all"}` series
   (`SyncAllDurationsHistogram`) is deleted per 4(g); its zero-series
   disappears from `/metrics` and this is the one accepted change to the
   metrics surface.
5. **Push semantics** — Reversion-last-writer-wins, filter rules
   (common.go:82-117), three-way CompareAndFlush outcomes, 5 s retry
   cadence, notice texts.
6. **README behavior** — `./spotter adapter` invocation and the documented
   10-second failover expectation.

### (j) Divergent diff rules (found during the audit)

k8s `hasInstanceDiff` (k8s.go:226-239) and the consul inline diff
(consul.go:348-364) disagree: k8s treats `new.Reversion > old.Reversion`
alone as a diff and excludes Cpu/Memory from field comparison (comment at
k8s.go:234); consul only diffs fields when revisions are *equal* and does
compare Cpu (consul.go:361). Unifying them is a product decision explicitly
out of scope.

**Taken decision:** keep TWO thin provider-specific use cases that wrap
SHARED domain primitives. The domain provides the named policies
`DiffNewerReversion` (k8s) and `DiffEqualReversion` (consul), the offline
markers, and the three-way compare skeleton; each use case holds its own
parameters — the remote-status query set (k8s asks for
`{online, unhealthy}` at k8s.go:286, consul for `{online}` at
consul.go:323), the `PushAppCodes` filter (k8s.go:295-305), the offline
marker shape (`Enabled=false, Status=3, State=terminated` at k8s.go:350-352
vs `Status=2` at consul.go:395), and the locking discipline (consul locks,
k8s does not). The duplication that 4.j removes is the *plumbing* (pool
submit, event construction, metrics, logging), not the policy.

---

## 5. Migration Plan

Each phase ends with a green build and a green `pkg/providers/k8s` test run
(the existing 605-line conversion suite is the golden regression net).

### Phase A — domain package + ports (pure extraction)

- **Entry criteria:** clean build on `refactor/all`.
- **Do:** create `internal/domain/instance` (types from v2.go:27-111;
  `RuntimeConfig` from common.go:65-70; constants/filters/ListToMap from
  common.go — swapping `InitInstanceFilters`' github.com/pkg/errors for
  stdlib errors during the move; the `InstanceCache` interface from
  cache.go:11-17, with `Delete` dropped; `PodObservation`;
  `DiffNewerReversion`/`DiffEqualReversion`/`Compare` extracted
  from k8s.go:226-239, consul.go:185-236 and both CompareAndFlush bodies;
  state/status mapping functions over `PodObservation`). The b-tree/deep-copy
  cache implementation moves to an infra-side package implementing the
  domain interface. Create `internal/ports`. Unit-test the extracted rules
  with table tests (no infra).
- **Files touched:** new packages only, plus re-export shims
  (`pkg/beehive/service/v2` aliases `internal/domain/instance`) so old
  code keeps compiling.
- **Exit criteria:** domain package has `go test` coverage of diff, filters,
  state mapping; whole repo still builds; k8s tests still pass.
- **Risk & mitigation:** behavior drift during extraction — mitigation:
  copy bodies verbatim first, refactor only after tests pin them.

### Phase B — dependency injection (logger, notifier, config, constructors)

- **Entry criteria:** Phase A merged.
- **Do:** zap adapter implements `ports.Logger` (noop default); notice
  adapter implements `ports.Notifier`; `internal/infra/config.Load`
  produces the immutable `Config`; every `NewX` returns `(T, error)` and
  takes `(Logger, Config, ...)`; `NewDiscoveryCenter` stops panicking
  (registry.go:29); composition root assembled in `internal/composition`,
  called from `cmd/adapter.go`. Old globals keep working during the phase
  (they are written by the composition root, read by not-yet-migrated
  code), then disappear package by package.
- **Files touched:** all of 2.1-2.9 constructors; no behavioral change.
- **Exit criteria:** no production code references `log.Logger`,
  `notice.Noticer` or `config.*` except the infra adapters and the
  composition root; k8s conversion tests unchanged and passing.
- **Risk & mitigation:** large mechanical diff — mitigation: one package
  per commit, each buildable.

### Phase C — application services consume ports; dead code deleted

- **Entry criteria:** Phase B merged; globals gone.
- **Do:** `internal/app/sync` (IncrementalSync, fed by
  `InstanceSource.Watch`), `internal/app/fullsync` (TWO thin
  provider-specific CompareAndFlush use cases over the shared domain
  primitives, per 4.j), `internal/app/retry` (EventQueue), `internal/app/leader`
  (Server state machine, fixed server.go:138 nil deref). Providers shrink
  to infra adapters implementing `InstanceSource` — each keeps its ants
  pool internal. Delete everything in the 4(g) table.
  `pkg/providers/consul/consul_test.go` must be deleted **in the same
  change**: it calls `worker.NewResourceFackWorker` (consul_test.go:15), so
  deleting worker_fack.go alone leaves the consul package's tests
  non-compiling and Phase C's `go vet ./...` exit criterion unachievable
  (the test rewrite is otherwise Phase D work). `k8s_test.go` moves to
  `internal/infra/k8s` together with the converter so the golden suite
  keeps testing the unexported `formatInstance`. Fix the tools/cache vet
  bug by deleting the package (it is dead); fix remaining vet findings
  that the refactor touches anyway (discarded cancels in the moved code).
- **Exit criteria:** `go vet ./...` clean on the surviving tree; dependency
  rule holds (grep: nothing outside `internal/composition` and `cmd`
  imports `internal/infra/...`); k8s golden tests pass in their new home.
- **Risk & mitigation:** accidental behavior change in the duplicated
  CompareAndFlush dedup — mitigation: keep the two field-set policies
  separate (4.j) and port the notice texts verbatim.

### Phase D — test build-out (executed by a later team; seams designed here)

- Fakes for every port (`FakeInstanceSink` with push/error scripting,
  `FakeLeaderElector`, `FakeClock`, `FakeNotifier` capturing titles).
- Rewrite the currently-failing tests: watch tests become domain/adapter
  tests over fakes (the watch package itself is deleted in Phase C);
  a new consul provider test replaces the deleted consul_test.go; consul
  provider tests get a fake `InstanceSource`-internal monitor;
  discoverycenter tests get a `grpc.Server` on a local listener with the
  JSON codec (the mirror structs' documented constraint).
- Test matrix per Section 6.

---

## 6. Testability Map

| Target module | Test mode | How |
| --- | --- | --- |
| `internal/domain/instance` (all of it: diff, compare, filters, state mapping) | **pure unit tests** | table tests; no mocks at all |
| `internal/app/sync` (IncrementalSync) | unit + fake ports | `FakeInstanceSink` records pushes; failure script drives the retry path |
| `internal/app/fullsync` | unit + fake ports | local+remote lists in, expected pushes out; all three inconsistency cases |
| `internal/app/retry` (EventQueue) | unit + fake clock + fake sink | `Drain()`/`Len()` assertions; highest-Reversion-wins |
| `internal/app/leader` | unit + fake elector | transitions on a channel; stop-before-start case (currently a nil-deref at server.go:138) becomes a test |
| `internal/infra/k8s` converter | unit | v1.Pod literals in, Instance out — the existing k8s_test.go goldens move here with the converter (5, Phase C) |
| `internal/infra/k8s` robot | integration, offline | client-go ships `kubernetes.NewFakeClientSet` + `informers` — drive the informer factory against the fake clientset; recommended over any live cluster |
| `internal/infra/consul` converter | unit | `api.ServiceEntry` literals in, Instance out |
| `internal/infra/consul` monitor | integration | `consul` test container or an `httptest` server faking the three consul endpoints used (`Health().State`, `Catalog().Services`, `Health().Service` — monitor.go:109, 192, 209); no hardcoded IPs |
| `internal/infra/grpcdiscovery` | integration, in-process | local `grpc.Server` with the JSON codec; asserts SynInstance/SynAll/GetAll behavior incl. the non-zero-code error path (client.go:56-58, 72-74) |
| `internal/infra/etcd` election | integration | embedded etcd (the etcd client used here supports `embed`); two electors, one campaign key, assert exactly one leader within the 2 s poll period |
| `internal/infra/logging`, `notice`, `metrics`, `config` | unit | buffer sink; captured notices; `config.Load` on flag maps |
| `internal/composition` | smoke | construct everything with noop/fake adapters; assert the object graph is non-nil and Start/Stop terminates |
| Full binary | e2e | spotter + a fake discovery center + fake consul; assert pushed instances |

---

## 7. Traceability: Failure -> Root Cause -> Fix

| Current failure | Root cause (audit ref) | Design element that eliminates it |
| --- | --- | --- |
| SIGSEGV in `pkg/providers/consul/watch` tests (nil `log.Logger` at watcher.go:81/97) | package-global logger (2.9, log.go:11) + the watch subpackage being unused production code (2.5) | §4(a) injected `ports.Logger` with noop default; §4(g) the watch subpackage is deleted outright |
| consul tests dial hardcoded consul endpoints — 10.72.73.x:8520 (consul_test.go:17, monitor_test.go:86) and 172.16.129.x:8520 (monitor_test.go:19, 55, 68) | constructors perform network probes (`ClientFactorySimple.ConsulClientFactory`, client_factory.go:45/59); no seam (2.5) | §3 `InstanceSource` port + §6 fake consul monitor; addresses come from `Config`, never literals in tests |
| discoverycenter tests dial `172.18.27.63:50051` by mutating `config.GrpcAddr` (client_test.go:13-16); `NewDiscoveryCenter` panics (registry.go:29) | ambient config global (2.1/2.7) + panicking constructor that dials (2.7) | §4(b) immutable `Config` passed to the adapter; §4(c) `NewX` returns error, `Dial` explicit; §6 in-process gRPC test server |
| `go vet ./tools/cache` fails: `fmt.Sprintf` args without directives (cache.go:52) | dead package with no tests ever compiled against it (2.10) | §4(g) delete `tools/cache` (sole consumer `distribute/discovery` is also deleted) |
| Test runs dirty the source tree (`pkg/providers/consul/app.log` modified) | `LoggerInit` writes to `config.LogFilePath + "app.log"` with a zero-value path (monitor_test.go:12-14 -> log.go:50) | §4(a)/§4(b): tests never call a global `LoggerInit`; the zap adapter takes an explicit output path, tests use the noop logger |
| (Latent, same class) server stop nil-deref (server.go:138); 158-year sleep on consul failure (monitor.go:103); `syncStoppedState` infinite spin (elector.go:83-90); `close(planStopCh)` double-close (watcher.go:99) | untestable orchestration with hidden time and lifecycle state (2.2, 2.5, 2.6) | §4(e) cancellable `Clock.After` loops; §5 Phase C fixes each while extracting the use cases; §6 leader tests cover stop-before-start |

*End of document. Every file:line citation above was verified against the
working tree at commit `945ce51` on branch `refactor/all`.*
