# spotter — Instance Data Model

This document specifies the `Instance` model that spotter pushes to the
discovery center, together with the status/state enums, the port model, the
compatibility label scheme and the `Reversion` semantics.

Sources of truth:

- `pkg/beehive/service/v2/v2.go` — the `Instance` / `PortInfo` structs.
- `pkg/providers/k8s/conversion.go` — the Kubernetes (pod) mapping.
- `pkg/providers/consul/convertion.go` — the Consul (endpoint) mapping.
- `pkg/providers/common.go` — the status/state/protocol constants.

See [architecture.md](architecture.md) for how instances flow to the discovery
center.

## The `Instance` struct

```go
type Instance struct {
	InstanceId  string
	Level       string
	Ports       []*PortInfo
	Ip          string
	EnvCode     string
	EnvType     string
	EnvGroup    string
	Cluster     string
	Version     string
	Enabled     bool
	State       string
	HealthState string
	AppCode     string
	Provider    string
	Label       map[string]string
	Hostname    string
	Cpu         float32
	Memory      int32
	Disk        int32
	Os          string
	Image       map[string]string
	Idc         string
	Reversion   int64
	Status      int32
}
```

### Field-by-field mapping

| Field | Type | K8s provider source (`formatInstance`) | Consul provider source (`convertInstance`) |
| --- | --- | --- | --- |
| `InstanceId` | `string` | `pod.Name` (the pod name is the instance id) | `Service.Meta["instanceId"]`; empty meta aborts conversion |
| `Level` | `string` | not set (zero value) | set to `""` explicitly |
| `Ports` | `[]*PortInfo` | fixed dubbo port 7096 + every `container.ports` entry (see below) | JSON array in `Service.Meta["ports"]` (see below) |
| `Ip` | `string` | `pod.Status.PodIP` | `endpoint.Node.Address` |
| `EnvCode` | `string` | `envType + "#" + envGroup` | `envType + "#" + envGroup` |
| `EnvType` | `string` | env `K8S_CLUSTER_TYPE` of any container, overridden by label `env-type`; lower-cased; `"online"` → `"product"` | `Service.Meta["envType"]` |
| `EnvGroup` | `string` | label `env-group` (empty when absent) | `Service.Meta["envGroup"]` |
| `Cluster` | `string` | label `cluster`, else env `K8S_CLUSTER_NAME` of the `application` container | set to `""` explicitly |
| `Version` | `string` | label `version` | `Service.Meta["version"]`; empty meta aborts conversion |
| `Enabled` | `bool` | phase `Running` AND every container ready+running AND no deletion timestamp | always `true` (only passing endpoints are converted); set to `false` when the provider marks an instance deleted |
| `State` | `string` | pod phase + container status state machine (table below) | consul check state machine (table below) |
| `HealthState` | `string` | not set (zero value) | set to `""` explicitly |
| `AppCode` | `string` | label `app-code`, else label `cadvisor-app`, else `namespace + "-" + labels["name"]`; empty appcode drops the pod | `Service.Meta["appCode"]`; empty meta aborts conversion |
| `Provider` | `string` | `"k8s"` | `"ecs"` |
| `Label` | `map[string]string` | compatibility label scheme (below) | `Service.Meta` plus the compatibility label scheme (below) |
| `Hostname` | `string` | `pod.Name` | `endpoint.Node.Node` |
| `Cpu` | `float32` | sum of container CPU limits (`1000m` → `1`) | `0` |
| `Memory` | `int32` | sum of container memory limits, in MB (`1024`- or `1000`-divisor by quantity format) | `0` |
| `Disk` | `int32` | not set (zero value) | `0` |
| `Os` | `string` | not set (zero value) | `""` |
| `Image` | `map[string]string` | container name → image for every container | empty map |
| `Idc` | `string` | label `idc`, else env `APP_IDC` of the `application` container | `"office"` when `envType == "dev"`, else `"mix"` |
| `Reversion` | `int64` | `pod.ObjectMeta.ResourceVersion` (parsed as int64) | `endpoint.Service.ModifyIndex` |
| `Status` | `int32` | pod phase + container readiness (table below) | consul health check (table below) |

Notes:

- Pods are dropped entirely (no `Instance` is produced) when the appcode is
  empty, or when `--appcodes` is set and the appcode is not in the list.
- On a K8s DELETE event the pod is already gone from the informer store, so the
  provider takes the cached instance, sets `Status = 3` and re-emits it with
  the full field set.

## Status enum

Constants from `pkg/providers/common.go`:

| Value | Constant | Meaning |
| --- | --- | --- |
| `0` | `InstanceStatusUnknown` | Unknown / not yet formatted. |
| `1` | `InstanceStatusOnline` | Instance is online. |
| `2` | `InstanceStatusUnhealthy` | Instance exists but is not healthy. |
| `3` | `InstanceStatusOffline` | Instance is deleted. |

### K8s mapping rules (`formatStatus`)

| Condition | Status |
| --- | --- |
| `pod == nil`, or event is DELETE, or `DeletionTimestamp` is set | `3` (offline) |
| Phase `Running` and every container status is `Ready` and `State.Running != nil` | `1` (online) |
| Phase `Running` but some container is not ready / not running | `2` (unhealthy) |
| Phase `Failed` and `Reason == "Evicted"` | `2` (unhealthy) |
| Phase `Pending` | `2` (unhealthy) |
| Other phases (`Succeeded`, `Unknown`) | `3` (offline, the initial default) |

### Consul mapping rules (`convertSatus`)

| Condition | Status |
| --- | --- |
| Check `CheckID == "service:" + appCode` exists and is `passing` | `1` (online) |
| Otherwise (check missing, not passing, appcode unresolvable) | `2` (unhealthy, the initial default) |

## State enum

Constants from `pkg/providers/common.go`:

| Value | Comment in source |
| --- | --- |
| `pending` | Instance not scheduled. |
| `starting` | Instance is starting. |
| `probing` | (no comment) |
| `oom` | The instance is OOM Killed. This state may exist for a very short time. |
| `crash` | Instance exited without code 0. |
| `running` | Instance is running. |
| `error` | Instance can not be started, e.g. the start command path is incorrect. |
| `failed` | Instance can not be created due to a system error, e.g. invalid kubelet CNI configuration. |
| `terminated` | Instance is deleted. |
| `evicted` | Instance has been evicted. |
| `unknown` | (no comment) |

### Pod phase / condition → state mapping (`formatState`)

| Pod condition | State |
| --- | --- |
| `DeletionTimestamp` set | `terminated` |
| Phase `Pending` | `pending` |
| Phase `Unknown` | `unknown` |
| Phase `Failed`, `Reason == "Evicted"` | `evicted` |
| Phase `Failed`, other reason | `failed` |
| Phase `Succeeded` | `terminated` |
| Phase `Running`, all containers ready | `running` |
| Phase `Running`, a container is waiting with `CrashLoopBackOff` and the last termination reason is `Error` | `error` |
| Phase `Running`, a container is waiting with `CrashLoopBackOff` (no error termination) | `crash` |
| Phase `Running`, not all ready, none of the above | `probing` |
| Phase `Running`, a non-ready container matched above but another was also non-ready | `unknown` |
| Anything else / empty result | `unknown` |

(`starting` and `oom` are defined but not produced by the current K8s
conversion code.)

### Consul check → state mapping (`convertState`)

| Condition | State |
| --- | --- |
| Check `CheckID == "service:" + appCode` exists and is `passing` | `running` |
| Appcode cannot be resolved | `unknown` |
| Nil endpoint | `unknown` |
| Otherwise (check missing or not passing) | `probing` (the initial default) |

## `PortInfo` model

```go
type PortInfo struct {
	Name        string
	Protocol    string
	Port        int32
	ServicePort int32
}
```

**K8s provider (`formatAppPort`):** a fixed dubbo entry is always emitted
first — `Name: "dubbo-7096"`, `Protocol: "dubbo"`, `Port: 7096`
(`ServicePort` unset). Then, for every container port: `Name` = the container
port name, `Port` = `containerPort`, and `Protocol` inferred from the port
name prefix when the protocol is TCP — `http*` → `http`, `grpc*` → `grpc`,
otherwise empty. Known protocol constants: `http`, `grpc`, `websocket`,
`dubbo`.

**Consul provider (`convertPort`):** `Service.Meta["ports"]` holds a JSON
array of `{name, protocol, port, scheme}` objects. `Protocol` falls back to
`Scheme` when empty (the ECS deployment service registers `scheme` instead of
`protocol`); `Name` falls back to `"<protocol><index>"` when empty. Both
`Port` and `ServicePort` are set to the port value.

## Env code composition

`EnvCode` is the concatenation `envType + "#" + envGroup`, built identically by
both providers. `EnvType` is one of `dev`, `test`, `staging`, `product`
(constants in `pkg/providers/common.go`); the K8s mapping additionally folds
`online` into `product`.

## Reversion semantics

`Reversion` is the provider-side version of the instance and is expected to be
strictly increasing. The K8s provider uses the pod's `ResourceVersion`; the
Consul provider uses the service registration's `ModifyIndex`. Because pushes
may not arrive in order under a complex network, the receiving side must
compare the incoming `Reversion` with the stored one and only accept the write
when it is larger — a last-writer-wins rule keyed on the provider version.
spotter applies the same rule locally: `hasInstanceDiff` treats
`new.Reversion > old.Reversion` as a difference, and the unsynced retry queue
only replaces a queued event when the new event's `Reversion` is larger. Note
that a zero `Reversion` is rejected by the default instance filter.

## Compatibility label scheme

Both providers add an "AOS compatibility" label set to `Instance.Label`, used
by the gateway and the Java SDK:

| Label key | Source | Purpose |
| --- | --- | --- |
| `compatibility:aos_namespace` | K8s: `pod.Namespace`; Consul: `Service.Meta["namespace"]` | Namespace compatibility for AOS. |
| `compatibility:aos_app` | K8s: pod label `app`; Consul: `Service.Meta["appCode"]` | AOS/FengXiao had an incompatibility with the `app` field of generated pods. |
| `compatibility:aos_dr_host` | constructed (rule below) | Used by gateways to generate DestinationRule rules adapted to AOS microservices. |
| `compatibility:aos_mark` | K8s: pod label `mark` | Used by the AOS WebIDE. |
| `env:san` | K8s: env `spring.application.name` of the `application` container; Consul: `Meta["spring.application.name"]` | Carries `spring.application.name` for the Java SDK. |

**drHost construction rule.** On the Kubernetes side: when the pod carries a
`deploy-id` label (the FengXiao deployment style) the host is
`app + "." + namespace`; otherwise (the AOS microservice style) it is just
`app`. On the Consul side (ECS deploy) the host is always
`appCode + "." + namespace`.

## Related messages

`pkg/beehive/service/v2` also mirrors the transport messages used around the
model: `InstanceList{Instance []*Instance}`, `CommonResponse{Code int32, Msg string}`,
`SynInstancesRequest`, `SynAllInstancesRequest` and
`GetAllInstancesRequest{Status int32, Provider string}`. See
[architecture.md](architecture.md#core-interfaces) for the RPC surface.
