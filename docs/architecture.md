# spotter — Architecture Design

This document describes the architecture of **spotter**, the discovery-center
adapter, reflecting the current state of the code after the module was renamed
to `github.com/ahakipper/spotter` and made self-contained (the former private
dependencies are now local mirror packages, see
[Layered Architecture](#layered-architecture)).

Companion documents: [data-model.md](data-model.md) (the `Instance` model,
status/state enums, ports, labels), [operations.md](operations.md) (build,
configuration, metrics and failure runbook) and [../README.md](../README.md)
(project README).

## Overview

spotter is a discovery-center adapter that aggregates instance data from
multiple sources — several Kubernetes clusters plus Consul-backed ECS
(machine-deployed) services — converts the observed pods and Consul service
endpoints into a single standardized `Instance` model, and pushes the resulting
instance events (incremental and full) to the discovery center ("Atlas") over
gRPC. Multiple spotter replicas run as one election group backed by etcd: only
the elected master connects to the providers and pushes; the other replicas stay
in the election as hot backups and take over within about ten seconds if the
master dies. Downstream, the gateway and the Java SDK consume the instance data
from the discovery center, so spotter is effectively the authority for "which
container/machine instances exist and what state they are in".

## System Context

```
 +----------------------+   +----------------------+
 | Kubernetes cluster   |   | Consul servers       |
 |  A / B / C / ...     |   | (health + catalog)   |
 +----------+-----------+   +----------+-----------+
            | pod informer per cluster   | blocking health-state watch
            +-------------+--------------+
                          v
          +--------------------------------------+
 etcd --->|              spotter                 |---> Prometheus + pprof
 (leader  |  master replica: connects providers |     :8090/metrics (default),
  election|  and pushes; backup replicas stay   |     /debug/pprof/*
  key     |  in the election as hot standby)    |
 /paas/   +------------------+-------------------+
 spotter)                    |        notice / alerting
                             |        (leader lost, robot not synced,
                             +-------> push failures, data inconsistency)
                             v
                  +---------------------+
                  |  Discovery center   |
                  |     ("Atlas")       |
                  |     gRPC :50051     |
                  +----------+----------+
                             |
                  +---------------------+
                  | Gateway, Java SDK   | (consume instance data
                  +---------------------+  from the discovery center)
```

Key external touch points:

| Touch point | Direction | Notes |
| --- | --- | --- |
| Kubernetes API servers | read (watch) | One pod `SharedInformer` per cluster, all namespaces; kubeconfigs in `config/kubeconfigs/`. |
| Consul API | read (blocking queries) | Health state + catalog services + service entries. |
| etcd | read/write | Leader election (`concurrency.Election`), node registration/watch. |
| Discovery center (Atlas) | write + read (gRPC) | `SynInstance`, `SynAllInstance`, `GetAllInstance`. |
| Prometheus | expose | `:8090/metrics` by default, plus `net/http/pprof` endpoints. |
| Notice channel | write | Emergency-level notices on failures and inconsistencies. |

## Layered Architecture

| Layer | Package paths | Responsibility |
| --- | --- | --- |
| CLI entry | `cmd/` (`root.go`, `adapter.go`), `main.go` | Cobra command tree + Viper config discovery; the `adapter` subcommand parses flags, applies an env preset, initializes logging and notices, then starts the server. |
| Orchestration | `internal/server.go` | `Server`: creates the elector, subscribes to leader-change notifications on `leaderChCh`, and starts/stops the provider set on leadership transitions; also owns the Prometheus HTTP server. |
| Provider layer | `pkg/providers/` (`iface.go`, `common.go`, `cache.go`), `pkg/providers/k8s/`, `pkg/providers/consul/`, `pkg/providers/aggregate/` | The `Provider` interface, shared filters/constants and the instance cache; the Kubernetes provider and the Consul provider implementations; an (unfinished) aggregate controller. |
| Push layer | `pkg/worker/` (`worker.go`, `types.go`, `elector.go`, `unsynced_service.go`, `worker_fack.go`), `pkg/discoverycenter/` | `DefaultWorker` routes events to handlers; `UnsyncedService` retries failed pushes; `pkg/discoverycenter` holds the gRPC client and the `Pusher` contract. |
| Distributed coordination | `pkg/distribute/election/`, `pkg/distribute/discovery/`, `pkg/etcd/` | etcd client construction, the `Candidate` leader-election implementation, and the `Node` register/keepalive/watch mechanism. |
| Multi-cluster K8s access | `pkg/k8srobot/` | The multi-cluster informer "robot" consumed by the K8s provider. |
| Support | `pkg/log/`, `pkg/metrics/`, `pkg/notice/`, `tools/` | zap + lumberjack logging, Prometheus metrics and the metrics HTTP server, notice/alert facade, plus small helpers (`tools/recover.go`, `tools/cache`, `tools/gpool`, `tools/net`, `tools/unit`). |
| Self-contained mirror packages | `pkg/beehive/service/v2`, `pkg/notice/appcenternotice`, `pkg/k8srobot` | Local replacements for former private dependencies (see below). |

### Self-contained mirror packages

When the project was made self-contained, three imports of private modules were
replaced by local packages that mirror the original API surface:

- **`pkg/beehive/service/v2`** — replaces the private `beehive-proto
  api/service/v2` module. It mirrors the generated protobuf types (`Instance`,
  `PortInfo`, `InstanceList`, `CommonResponse`, the request messages) and the
  `InstanceServiceClient` gRPC client surface used by this repository, including
  the original RPC paths (`/service.v2.InstanceService/SynInstance`,
  `.../SynAllInstance`, `.../GetAllInstance`). Because the structs are plain Go
  structs and not generated proto messages, the package is compile-compatible
  only — see [Known Limitations](#known-limitations-and-technical-debt).
- **`pkg/notice/appcenternotice`** — replaces the private `appcenter-notice/client`
  module. It keeps the chainable `Noticer` API (`WithAppCode`/`WithKey`/`WithEnv`
  and `SendNotice`) but delivers notices through the application logger instead
  of the internal HTTP notice service, so alerts are log lines only.
- **`pkg/k8srobot`** — replaces the former private `servicemesh/robot`
  multi-cluster Kubernetes client module. It is a real implementation built on
  `k8s.io/client-go` informers and preserves the consumed surface
  (`NewRobot`, `Run`, `Stop`, `HasSynced`, `Pop`, `Finish`, `GetByKey`, `List`).

## Core Interfaces

The following signatures are copied verbatim from the source.

**Provider** — `pkg/providers/iface.go`:

```go
// Provider is a interface
type Provider interface {
    // Run starts to monitor s subject
    // The Run action should hang utils the monitor stopped.
    Run() error
    CompareAndFlush()
    // GetAll get all the instances from current provider
    GetAll() []*sv.Instance
}
```

**Worker / Elector** — `pkg/worker/types.go` and `pkg/worker/elector.go`:

```go
type Worker interface {
	AddEventHandler(opt OperateType, handler EventResourceHandler)
	Handle(d *Event)
	ProcessUnsynced() // ProcessUnsynced process instances that have not been successfully pushed before
	GetAll(enable []int32, provider string) (r *v2.InstanceList, err error)
}
```

```go
// Elector is used for master-slave election
type Elector interface {
    // Start the distribute node
    // init distributed related work
    ElectWait()

    // Stop represent node exit
    Stop()
}
```

**Pusher** — `pkg/discoverycenter/registry.go`:

```go
// Pusher is the contract used by the workers to talk to the discovery center.
type Pusher interface {
    Push(triggerTime int64, instance []*v2.Instance) (err error)
    PushAll(triggerTime int64, instance []*v2.Instance) (err error)
    GetAll(enable []int32, provider string) (list *v2.InstanceList, err error)
}
```

**CacheIterface** — `pkg/providers/cache.go`:

```go
type CacheIterface interface {
    Get(id string) *sv.Instance
    Delete(id string) *sv.Instance
    ReplaceOrInsert(ins *sv.Instance) *sv.Instance
    List() []*sv.Instance
    Clear()
}
```

**Robot** — `pkg/k8srobot/k8srobot.go`:

```go
// Robot is the multi-cluster watcher contract consumed by the providers.
type Robot interface {
	// Run starts every cluster informer. It blocks until Stop() is called.
	Run() error
	// Stop shuts the robot down. After Stop, Pop returns an error.
	Stop()
	// HasSynced reports whether every cluster store has been synced.
	HasSynced() bool
	// Pop blocks until an event is available or the robot is stopped.
	Pop() (QueueObject, error)
	// Finish acknowledges a previously popped object.
	Finish(obj QueueObject)
	// GetByKey returns the objects stored under "<namespace>/<name>".
	GetByKey(resource ResourceType, key string) ([]interface{}, bool)
	// List returns all objects of the given resource across all clusters.
	List(resource ResourceType) []interface{}
}
```

**Candidate / Node** — `pkg/distribute/election/election.go` and
`pkg/distribute/discovery/discovery.go`:

```go
type Candidate interface {
    // Campaign puts a value as eligible for the election. It blocks until
    // it is elected, an error occurs, or the context is cancelled.
    Campaign(timeout time.Duration) error

    // IsLeader judge this candidate whether it is a leader
    IsLeader() (bool, error)

    // Resign lets a leader start a new election.
    Resign() error

    // AddObserveCallFunc add a callback func for leader changes
    AddObserveCallFunc(f LeaderChangeFunc)

    // Tag represent a tag for this node participate in the election campaign
    Tag() string

    // Wait will
    Wait()

    // LeaseID return the lease id for this candidate need to keepalive
    LeaseID() clientv3.LeaseID
}
```

```go
type Node interface {
    // PushChannel return the channel of this node subscription
    PushChannel() string

    // KeepAlive send the heart beat package to etcd
    // Make sure this node is active
    Keepalive() error

    // watch the status of other nodes in this cluster
    Watch() error

    // GetClusterInfo get all node info of this cluster
    GetClusterInfo() map[string]interface{}

    // Quit represent node exit
    Quit()
}
```

Other supporting contracts: `Monitor` (`pkg/providers/consul/monitor.go`),
`ConsulClientFactory` (`pkg/providers/consul/client_factory.go`) and
`InstanceFilter` (`pkg/providers/common.go`, `type InstanceFilter func(ins *sv.Instance) error`).

## Data Flow

### (a) Incremental flow (Kubernetes)

1. Each cluster informer fires an `add`/`update`/`delete` callback; `pkg/k8srobot`
   converts it into a `QueueObject{RType: Pods, Key: "<namespace>/<name>", Event, CreateAt}`
   and enqueues it on a shared 4096-slot buffered channel (non-blocking send,
   see [Kubernetes Provider Details](#kubernetes-provider-details)).
2. `(*k8s).monitor` pops objects in a dedicated goroutine via `robot.Pop()`.
3. `pod2Instance(obj)` fetches the pod with `robot.GetByKey(Pods, obj.Key)` and
   converts it with `formatInstance` (rules in [data-model.md](data-model.md));
   on a DELETE event the pod is already gone, so the cached instance is taken
   from the local cache and marked offline.
4. `VerifyInstance` applies the instance filters (nil appcode, nil env type,
   nil IP while online, `pending` state suppression, zero `Reversion`, unknown status).
5. `hasInstanceDiff(cacheInstance, instance)` compares against the local cache;
   unchanged instances are dropped (no event, no push).
6. Changed instances are submitted to the ants goroutine pool (100 workers,
   100 s expiry) so push concurrency is bounded.
7. `eventSync` wraps the instance in `worker.Event{Operate: OperateTypeSync}` and
   calls `worker.Handle`.
8. `DefaultWorker.Handle` dispatches to the `OperateTypeSync` handler, which calls
   `discoverycenter.Pusher.Push` → `Client.Sync` → gRPC `SynInstance` (10 s
   per-call read timeout, observed in `sync_once_durations_histogram`).
9. If the push fails (transport error or non-zero response code), the event is
   added to the `UnsyncedService` retry queue, which retries every 5 s and keeps
   only the highest-`Reversion` event per instance id.

### (b) Full flow (periodic reconcile)

1. `ProcessIntervalFullPush` runs a ticker with `providers.FullPushInterval`
   (default 21600 s = 6 h; overridable per invocation with `--push-interval`, in seconds).
2. On each tick, `CompareAndFlush()` runs `GetAll()` locally (list every pod of
   every cluster through the informer stores, convert, filter) and refreshes the
   local cache.
3. It fetches the remote view with `worker.GetAll([]int32{online, unhealthy}, provider)`
   → gRPC `GetAllInstance` (one call per requested status; the K8s provider also
   supports `--appcodes` filtering of the remote view).
4. Both sides are keyed by `InstanceId` (`providers.ListToMap`) and a three-way
   diff is computed:
   - **both exist but fields differ** → push the provider-side instance (provider wins);
   - **provider-only** → push it if it is online (status 1);
   - **discovery-center-only** → mark it offline: the K8s provider sets
     `Enabled = false`, `Status = 3` and `State = terminated`; the Consul
     provider sets `Status = 2`.
5. Each diff result is emitted as an `OperateTypeSync` event through the same
   ants pool, so the full push is a batch of incremental pushes
   (`OperateTypeSyncAll` / `SynAllInstance` exists but is not used by the
   periodic path).
6. Whenever any of the three diff cases is non-empty, an
   *"Instance data inconsistency"* notice is emitted (one variant per case).

## Leader Election Design

- Election is implemented with `github.com/coreos/etcd/clientv3/concurrency.Election`
  under the campaign key `config.LockCampaignKey` (`/paas/spotter` in dev and
  product, `/paas/spotter-test` in test).
- `NewElector` (`pkg/worker/elector.go`) builds the etcd client
  (`pkg/etcd.NewEtcdClient`, optional TLS from the env preset) and a `Candidate`
  whose campaign value is a fresh UUID tag.
- The candidate grants an etcd lease (TTL 10 s in `NewElectionSession`) and
  creates a concurrency session on it; if the process dies, the lease expires
  and the leadership key disappears, forcing a re-election. The README states
  re-election completes within about 10 seconds.
- `ElectWait()` loops: `Campaign(CampainTimeout = 10 s)`, then `Wait()`. `Wait()`
  polls `IsLeader()` every `LeaderChangePeriod = 2 s` and invokes the registered
  leader-change callbacks asynchronously (in goroutines, so a slow callback
  cannot block the election). When the node observes it is no longer the leader
  it closes the current session, creates a new one and campaigns again — so
  non-master nodes remain in the election as hot backups.
- `internal.Server` receives leader changes on `leaderChCh` (a `chan bool` with
  a 2048 buffer; the buffer is required, otherwise the elector's asynchronous
  notifications would block). Its `Run()` main loop is a small state machine:
  - duplicate notifications (same value as current state) are ignored;
  - losing the leader (`isLeader == false`) → log + notice ("Leader role lost"
    with the local IP) and `stopProviders()` — providers are stopped, but the
    election continues;
  - gaining the leader (`isLeader == true`) → `stopAndStartProviders()` runs in
    a goroutine, which stops any previous providers and starts a fresh set with
    a new cancel context; if provider startup fails, a "Failed to start the
    provider" notice is emitted.
- With `--leader-elect=false` the election is bypassed: the server writes `true`
  into `leaderChCh` once and the process acts as a forever-fake leader.
- Only the master connects to the Kubernetes clusters and pushes to the
  discovery center; the backups run nothing but the election loop.

## Consistency and Ordering Design

- **`Reversion` as a monotonic version.** The K8s provider sets
  `Reversion = pod.ObjectMeta.ResourceVersion`; the Consul provider sets
  `Reversion = endpoint.Service.ModifyIndex`. Pushes may be reordered or
  duplicated under a complex network, so the receiving side must compare the
  incoming `Reversion` with the stored one and accept the write only when it is
  larger — last-writer-wins by provider version, not by arrival order. The same
  rule is applied locally by `hasInstanceDiff` (`new.Reversion > old.Reversion`
  ⇒ diff) and by the `UnsyncedService` (a retried event only replaces a queued
  one if its `Reversion` is larger).
- **Why CPU/memory are excluded from the diff.** `hasInstanceDiff` explicitly
  does not compare `Cpu`/`Memory` because the discovery center stores them as
  integer types, so every comparison would report a difference and produce a
  push storm. (The Consul `CompareAndFlush` still compares `Cpu` — the two
  providers are not fully symmetric here.)
- **The "all clusters synced" gate.** Before consuming any watch event, the K8s
  provider spins until `robot.HasSynced()` is true for every configured cluster
  (logging a "robot failed to sync the K8s clusters" notice every 15 s while
  waiting). This is a deliberate availability-for-correctness trade-off: spotter
  is the aggregation of *all* instances, and a full push compares the local view
  with the discovery center. If a cluster were unreachable and spotter proceeded
  anyway, the local view would be incomplete and the missing cluster's instances
  would be marked offline in the discovery center — every instance of that
  cluster would become inaccessible through the gateway and the Java SDK, i.e. a
  full outage of that cluster's services. The consequence is that startup blocks
  until every configured cluster is reachable (see [operations.md](operations.md),
  "Cluster decommission").

## Reliability Mechanisms

- **`UnsyncedService`** (`pkg/worker/unsynced_service.go`): every failed push
  (`OperateTypeSync` and `OperateTypeSyncAll`) is queued in a map keyed by
  `InstanceId`; a 5 s ticker retries the queued events and removes them on
  success. For each instance id only the event with the highest `Reversion` is
  kept, so retries never push stale data over newer data. The queue depth is
  exported as the `sync_error_gauge` metric.
- **ants goroutine pool**: both providers create `ants.NewPool(100)` with a
  100 s expiry duration (`providers.PoolBenchSize = 100`,
  `providers.PoolExpireTime = 100`). All pushes go through `pool.Submit`,
  bounding push concurrency; the comment in the filter code notes the historical
  motivation — unbounded pushes could cause database deadlocks in the consumer
  (referred to in code as "Finder").
- **Instance filters** (`providers.InitInstanceFilters`): drop nil instances,
  empty appcode, empty env type, online instances without an IP, instances with
  zero `Reversion`, status `unknown`, and — notably — pods in the `pending`
  state (suppressing the noisy early phase of pod creation and the associated
  push pressure).
- **Notice/alert integration points**: leader role lost, elector init failure,
  provider start failure, robot not synced, consul client/watch failures,
  incremental/full push failures, and the three "Instance data inconsistency"
  variants of `CompareAndFlush`. All notices are currently delivered through
  `pkg/notice` → the local `appcenternotice` logger.
- **Prometheus metrics** (default `--metrics-addr :8090`, path `/metrics`, pprof
  endpoints included via `net/http/pprof`):

| Metric | Type | Meaning |
| --- | --- | --- |
| `sync_all_durations_histogram{provider="k8s"}` | histogram | Duration of the K8s provider's periodic `CompareAndFlush`. |
| `sync_all_durations_histogram{provider="ecs"}` | histogram | Duration of the Consul provider's periodic `CompareAndFlush`. |
| `sync_all_durations_histogram{provider="all"}` | histogram | Registered aggregate variant (currently unused by the active code paths). |
| `sync_once_durations_histogram` | histogram | Duration of a single `SynInstance` gRPC call. |
| `sync_once_gauge{syncgauge="sync_once_gauge"}` | gauge | Set to 1 after every single sync observation. |
| `sync_error_gauge{syncgauge="sync_error_gauge"}` | gauge | Number of instances waiting in the unsynced retry queue. |

## Kubernetes Provider Details

**Multi-cluster informer robot (`pkg/k8srobot`).** `NewRobot` takes one
`Cluster{ConfigPath, Resources: []RN{{Pods, ""}}}` per kubeconfig, loads each
kubeconfig, builds a clientset and a `SharedInformerFactory` (resync period 0)
with a pod informer over all namespaces. Per-cluster event handlers convert
callbacks into `QueueObject`s and send them on one shared
`queue chan QueueObject` with `queueSize = 4096`; the send is **non-blocking** —
when the channel is full the event is dropped rather than blocking the informer.
The drop window is self-healing: because informer stores keep the current state
and every periodic `CompareAndFlush` recomputes the diff against the discovery
center, a dropped event is corrected by the next full push (default every 6 h,
plus the `CompareAndFlush` run at provider start). `HasSynced()` only returns
true once every cluster store has synced; `NewRobot` fails fast if any
kubeconfig is missing/unreadable. `Pop()` blocks until an event is available
(draining already-queued events before reporting the robot stopped) and
`Finish()` is a no-op kept for API compatibility.

**Provider wiring (`pkg/providers/k8s`).** `NewK8SProvider(ctx, worker,
pushInterval, configPath)` builds the robot, a degree-2 b-tree instance cache
(`providers.NewCache(2)`), the ants pool and the filters. `Run()` → `monitor()`:
start the robot, wait for the all-clusters gate, run `CompareAndFlush()` once
synchronously, start the periodic full-push ticker, then consume `robot.Pop()`
in a goroutine.

**Pod → `Instance` conversion (`pkg/providers/k8s/conversion.go`):**
`InstanceId`/`Hostname` = pod name, `Provider` = `"k8s"`; `AppCode` = label
`app-code`, else label `cadvisor-app`, else `namespace + "-" + labels["name"]`
(pods without an appcode are dropped); `EnvType` = the container env
`K8S_CLUSTER_TYPE` (any container) overridden by the label `env-type` when both
exist, lower-cased, with `"online"` mapped to `"product"`; `Status` from the
pod phase + container readiness and `State` from the full pod state machine
(mapping tables in [data-model.md](data-model.md)); `Ports` = a fixed dubbo
port first (`Name: "dubbo-7096"`, `Protocol: "dubbo"`, `Port: 7096`), then every
`container.ports` entry with the protocol inferred from the port-name prefix
(`http*` → `http`, `grpc*` → `grpc`, else empty); `Reversion` =
`pod.ObjectMeta.ResourceVersion` parsed as int64; `Cpu`/`Memory` = sums of the
container resource limits (`1000m` → 1 CPU; memory in MB, `1024`- or
`1000`-divisor depending on the quantity format); `Image` = container name →
image for every container; `EnvCode` = `EnvType + "#" + EnvGroup` with
`EnvGroup` from the `env-group` label; `Version` from the `version` label;
`Idc` from the `idc` label or the `APP_IDC` env of the `application` container;
`Cluster` from the `cluster` label or the `K8S_CLUSTER_NAME` env of the
`application` container; `Label` = the compatibility label scheme
([data-model.md](data-model.md)); and `Enabled` = true only when the pod phase
is `Running`, every container status is ready and running, and there is no
deletion timestamp.

## Consul Provider Details

- **Client factory** (`client_factory.go`): `ClientFactorySimple` keeps one
  `api.Client` per configured address and probes them with `Status().Leader()` —
  a cached client that reports a leader is reused; otherwise each configured
  address is tried in order and the first that reports a leader is cached. This
  works around the consul client's lack of load balancing / leader awareness.
- **Blocking watch** (`monitor.go`): `watchConsul` issues
  `client.Health().State(api.HealthAny, &api.QueryOptions{WaitIndex, WaitTime})`
  with `blockQueryWaitTime = 5 s` (also used as the sleep after client-factory
  failures to prevent an infinite hot loop). When `LastIndex` changes, a token
  is pushed on a 64-slot channel. `updateRecord` debounces with a 50 ms ticker
  (`periodicCheckTime`; `refreshIdleTime` is also 50 ms): once no new change has
  been seen for that idle window, the registered instance handlers are invoked
  asynchronously.
- **Tag filter**: service entries are fetched with
  `client.Health().Service(name, "microservice", true, q)` — only endpoints
  tagged `microservice` (and passing health, `passingOnly=true`) are considered.
- **Endpoint meta → `Instance` mapping** (`convertion.go`): the `Service.Meta`
  map carries `ports` (JSON array of `{name, protocol/scheme, port}`),
  `envType`, `envGroup`, `appCode`, `version`, `instanceId` and `namespace`.
  Missing mandatory meta (ports, appcode, version) makes the conversion fail and
  the endpoint is skipped. `Idc` is `"office"` when `envType == "dev"`, else
  `"mix"`. `Provider` is `"ecs"`. The full field mapping is in
  [data-model.md](data-model.md).
- **Health-check-derived status/state**: the provider looks for the check whose
  `CheckID` equals `"service:" + appcode`; if that check is `passing`, the
  instance is `Status = 1` / `State = "running"`, otherwise `Status = 2` /
  `State = "probing"` (`"unknown"` when the appcode cannot be resolved or the
  endpoint is nil).
- **Reversion** = `endpoint.Service.ModifyIndex`.
- **Event generation**: every watch callback triggers `syncInstance`, which
  rebuilds the full endpoint list, diffs it against the previous cache (add when
  new, update when `Reversion` grew, delete when the id disappeared) and emits
  `OperateTypeSync` events for each change. As with the K8s provider, the
  periodic `CompareAndFlush` reconciles against the discovery center.

## Configuration and Deployment

**Environment presets** (selected with `--env`; `test` is the default and also
the fallback for any other value). Each preset also selects the etcd TLS
material from `config/certs/` (`etcdtest` vs `etcdprod`):

| Preset | etcd endpoints | Kubeconfigs (`config/kubeconfigs/`) | Consul addresses | Campaign key |
| --- | --- | --- | --- | --- |
| `test` | `172.18.12.181:2379`, `172.18.12.182:2379`, `172.18.12.183:2379` | `k8s-sailor` | `10.72.73.172:8520`, `10.72.73.173:8520`, `10.72.73.174:8520` | `/paas/spotter-test` |
| `dev` | same as test | `k8s-sailor`, `k8s-vipper` | same as test | `/paas/spotter` |
| `product` | `192.168.11.100:2479`, `192.168.11.101:2479`, `192.168.11.102:2479` | `k8s-eel`, `k8s-otter`, `k8s-slug`, `k8s-bernuda` | `10.132.2.40:8520`, `10.132.2.42:8520`, `10.132.2.43:8520` | `/paas/spotter` |

**CLI flags.** The root command (`spotter`) defines the logging flags; the
`adapter` subcommand defines the runtime flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-r, --providers` | `[]` | Comma-separated provider list: `k8s`, `ecs`. |
| `-t, --leader-elect` | `true` | Enable etcd leader election. |
| `-e, --env` | `test` | Environment preset: `test`, `dev`, `product`. |
| `-i, --push-interval` | `21600` | Full-push interval in seconds. |
| `-g, --grpc-addr` | `172.16.130.71:50051` | Discovery center (Atlas) gRPC address. |
| `-w, --disable-worker` | `false` | Disable the real push (log only; for testing). |
| `--appcodes` | `[]` | Only push instances of these appcodes (for testing). |
| `--metrics-addr` | `:8090` | Prometheus metrics address. |
| `-p, --log-file-path` | `./logfiles/` | Log directory. |
| `-m, --log-maxsize` | `100` | Max log size (MB). |
| `-n, --log-backup-number` | `10` | Number of log backups. |
| `-l, --log-level` | `-1` | `-1` debug, `0` info, `1` warning. |
| `-a, --log-age` | `7` | Max log age (days). |
| `-s, --log-to-std` | `true` | Also write logs to stdout. |
| `-c, --log-encoding` | `json` | Log format: `log` (console) or `json`. |

**Build and deployment.** `make OS=darwin` / `make OS=linux` produce the
`spotter` binary (`go build -o spotter`). The container image is built by the
internal CI pipeline (drone) on tag events and published as
`hub.mfwdev.com/paas/spotter:<tag>`; the Dockerfile copies `./config` and the
binary into `/usr/bin` and uses `/usr/bin/spotter` as the entrypoint.

## Known Limitations and Technical Debt

1. **`pkg/beehive/service/v2` is compile-compatible only.** The structs are
   plain Go structs, not generated proto messages, and `grpc.ClientConn.Invoke`
   uses the default proto codec, so calls will fail at runtime
   ("proto: not a proto message") unless the server is configured with a codec
   that accepts these structs (e.g. a JSON codec registered via
   `grpc.WithDefaultCallOption(grpc.ForceCodec(...))`). This is stated in the
   package header of `v2.go`.
2. **`pkg/notice/appcenternotice` is a local no-op logger.** Notices are log
   lines only; no alert is actually delivered to the internal appcenter-notice
   service.
3. **`pkg/providers/aggregate/controller.go` is an unfinished refactor.** All
   methods are commented out; the intent was to deduplicate `CompareAndFlush`
   across providers, but each provider still carries its own copy.
4. **etcd key rename.** The campaign key is now `/paas/spotter`
   (and `/paas/spotter-test`), and the node registration prefix is
   `/paas/spotter/register/`. An old deployment still campaigning under the
   former key would not compete with a new one, so old and new deployments must
   not run concurrently — perform a cold start / full cutover instead.
5. **Duplicated provider logic.** `CompareAndFlush`, `ProcessIntervalFullPush`,
   `VerifyInstance`, `buildAndSendEvent` and the ants-pool setup are effectively
   duplicated between `pkg/providers/k8s` and `pkg/providers/consul` (a `TODO`
   in the consul provider acknowledges this).
6. **Tests require live infrastructure.** Several tests (consul monitor/watch,
   discovery center client) dial real endpoints and *fail* (not skip) when the
   infrastructure is unreachable.
7. **Pre-existing `go vet` findings.** `go vet ./...` currently reports context
   leaks (discarded cancel functions in `pkg/etcd`, `pkg/distribute`,
   `pkg/discoverycenter`, `internal/server.go`), an unkeyed struct literal in
   `pkg/providers/k8s/k8s.go`, self-assignments in
   `pkg/discoverycenter/client_test.go` and a formatting issue in
   `tools/cache/cache.go`.
8. **Go 1.15 module, mixed formatting.** `go.mod` declares `go 1.15`; parts of
   the tree use 4-space indentation while the rest is not `gofmt`-formatted.

## References

- [../README.md](../README.md) — project README (build, usage, special notes).
- [data-model.md](data-model.md) — the `Instance` model and state machines.
- [operations.md](operations.md) — operational runbook.
