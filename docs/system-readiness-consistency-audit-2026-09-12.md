# spotter 当前工程就绪度与一致性闭环审计

**审计日期：** 2026-09-12  
**仓库：** `/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter`  
**分支/提交：** `refactor/all` / `838ad19`  
**文档状态：** FINAL（已完成第二轮独立 reviewer 复核）

本文是对当前工程的一次总盘点，作为后续全面优化的主要参考。它把已有设计文档、当前代码、测试结果和已提交的观察产物放在同一份证据链中；结论优先以当前工作树和实际命令输出为准，不以旧文档中的历史基线为准。

## 1. 审计范围与方法

本次审计覆盖四个问题域：

1. 文档列出的未完成项，逐项核对当前实现，判断是“仍未完成”“部分完成”还是“文档过时”。
2. Nacos Sink 的 HTTP 功能、错误与重试、全量清理、鉴权/namespace 边界，以及官方 Go SDK 的 gRPC 能力。
3. `K8s Watch → k8srobot 队列 → provider cache/diff → worker/Fanout → Sink` 的数据路径和并发/丢失风险。
4. `CompareAndFlush/PushAll/GetAll` 与 Sink 的闭环，包括新增、更新、删除、漂移、错误和空源场景。

执行过的本地核验：

- `make test-all`：通过。包括 race 单测、blackbox、5 个 smoke 用例和 `-tags=e2e` 测试。
- `go test ./... -count=1`：业务包通过，但整体退出 1；`tools/cache` 触发 vet printf 诊断。
- `go vet ./...`：当前剩余两条诊断，见第 2 节。
- `go test -tags=observe -run '^TestObserveUnit' ./tests/observe/... -count=1`：通过。
- 只读检查 `git status --short --branch`：工作树干净；测试生成的 `build/` 已被忽略。
- 官方资料和 SDK 源码核验：下载并检查 `github.com/nacos-group/nacos-sdk-go/v2@v2.3.5`，同时查阅 Nacos 官方 Go SDK、Open API 和 SDK proto 资料。

## 2. 先给结论：当前状态矩阵

| 项目 | 当前判定 | 结论依据 | 文档是否已过时 |
|---|---|---|---|
| Burst delete race | **NOT DONE / P1** | 25 分钟 burst 结果仍有 18/151 个 DIVERGENT tick，4/8 收敛 leg 超过 60s；生产代码仍没有 sink tombstone 或 push 执行前重读校验 | 否；`dsca-delivery.md` 已明确列为 deferred |
| Observe `logSlice` + ledger/apply 竞态 | **PARTIAL / P2** | ledger、tracker、每 tick 双向比对已存在；但 zap JSON 时间戳解析仍只尝试行首时间，ledger 在 kubectl apply 返回后写入，tick 可能读到“已落地但无 ledger”的窗口 | 否；文档仍准确记录缺口 |
| Consul 同等规模观察 | **ACCEPTED NON-GOAL** | 2h/1000 观察只启动 `--providers k8s`；当前没有机器部署场景，按本次范围暂不展开 | 否；范围边界已明确 |
| Nacos HTTP Sink | **FUNCTIONAL PASS / ARCHITECTURE NON-CONFORMANT / PRODUCTION PARTIAL** | HTTP register/deregister/catalog/list、PushAll prune、4xx/5xx 分类、连接池和本地/e2e 测试均存在；但当前 `pkg/nacos` 是裸 `net/http` 客户端，没有通过官方 SDK，且鉴权、可配置 namespace、生产 HA/TLS/写入就绪、PushAll 失败重试语义仍有边界 | 否；功能结论仍成立，但“直接裸 HTTP”现在明确列为必须整改的架构缺陷 |
| Nacos 官方 SDK gRPC 能力 | **SUPPORTED BY SDK, NOT IN THIS REPO** | 官方 Go SDK v2 有 `naming_grpc`、`RegisterInstance`、`DeregisterInstance`、`BatchRegisterInstance`；但 persistent instance 在 v2.3.5 的 delegate 中走 HTTP，ephemeral/batch 才走 gRPC | 部分过时：旧计划把“官方 SDK/gRPC”统一写成非目标，当前应改成“能力存在，且必须纳入生产路径” |
| Nacos SDK 统一接入约束 | **NOT DONE / P1（Nacos 启用时为 release blocker）** | 生产代码直接调用 Nacos v1 HTTP OpenAPI；没有 SDK adapter、SDK 认证/重连/版本兼容层，也没有禁止新增裸 HTTP 的静态门禁和完整 SDK 回放测试 | 否；这是本次新增的强制整改项，不能继续作为“可选 POC” |
| Atlas 真实 protobuf wire | **NOT VERIFIED / P1** | 本仓库模型是普通 Go struct；生产 `Dial` 强制 JSON codec，只有本地 discoverymock/e2e 证明 JSON 链路；未证明真实 Atlas 接受该 codec | 否；限制说明准确 |
| Notice / appcenter 告警 | **PARTIAL / P1** | composition 已有注入式 `Notifier`，但 providers/election 等仍直接调用 `pkg/notice.Notice`；`appcenternotice` 实现只写本地日志，不是已验证的真实告警投递 | 否 |
| DDD 目标架构 | **NOT DONE / P1** | `pkg/log.Logger`、`pkg/notice.Noticer`、`config.*` 仍被生产包读取；`cmd/adapter.go` 仍执行 legacy globals bridge；`pkg/providers/aggregate/controller.go` 仍是注释 scaffolding | 否 |
| `go vet ./...` | **NOT DONE / P2** | 当前实测只剩 `tools/cache/cache.go:52` 和 `pkg/providers/k8s/k8s.go:68` 两条；旧文档中的“20+ 条”是历史基线 | 是：数量已明显收敛，应更新为当前两条 |

**总体判定：** 业务主路径已经达到“可构建、可测试、可在本地 Nacos 2.1 形状运行”的阶段，但还不是“生产一致性闭环已证明”。最重要的未闭环问题是事件乱序/全量快照竞态、全量失败重试降级、空源保护语义、所有权边界，以及 Nacos 生产路径绕过官方 SDK 的架构缺陷。Consul 大规模观察按当前没有机器部署场景处理为 accepted non-goal，不影响本次 K8s 主路径结论；一旦重新启用 ECS/机器部署，必须单独打开该验证项。

## 3. Nacos Sink 审计

### 3.1 已经具备的能力

当前 `pkg/nacos` 是自研的 Nacos v1 HTTP OpenAPI 客户端，而不是 SDK。这一点不再只是实现选择，而是本次审计确认的强制整改缺陷：

- 生产 Nacos 操作必须统一经官方 Nacos SDK（或官方 SDK 暴露的等价 facade）；不得在业务路径继续新增或保留散落的裸 `net/http` 调用。
- 当前 HTTP 实现可以在迁移期间作为隔离的回滚/对照 adapter 保留，但不能据此宣称 Nacos 生产接入已完成。
- naming 的 register/deregister/list/query/subscribe 必须先完成 SDK adapter；catalog、prune 等 Admin 能力如果官方 Go naming SDK 没有等价接口，必须使用受支持的官方 Admin/Maintainer SDK 或单独版本化、可审计的 SDK facade。临时裸 HTTP 只能作为明确标记的迁移过渡，不能成为最终生产路径。

现有实现证据：

- `NewClient` 构造显式 `http.Transport`，`MaxIdleConnsPerHost=8`、`MaxConnsPerHost=8`，外层超时 10s；证据：`pkg/nacos/client.go:123-197`。
- `Sink.Push` 支持：online 注册、unhealthy 注册为 `enabled=false`、offline 删除、unknown 跳过；证据：`pkg/nacos/nacos.go:227-245,480-516`。
- `PushAll = Push + prune`；prune 读取 `catalog/instances`，能看见 `enabled=false` 实例，并按 `(service, cluster)` 作用域清理；证据：`pkg/nacos/nacos.go:248-420`。
- `GetAll` 读取 service list，再按 provider 对应的 Nacos cluster 读取 catalog，重建可参与 compare 的实例；证据：`pkg/nacos/nacos.go:422-472`。
- `APIError.Permanent()` 将 4xx 判为永久错误、5xx 判为可重试；证据：`pkg/nacos/client.go:397-420`。
- retry queue 会删除永久错误，避免 F7 类 4xx 无限重试；证据：`pkg/worker/unsynced_service.go:244-280`。
- 本地 sink、mock、blackbox、e2e 均有覆盖；`make test-all` 和 `go test -race ./pkg/nacos ./pkg/worker` 已通过。

因此，“HTTP 是否完全就绪”要拆成两个答案：

- **功能就绪：是。** 基本注册、删除、分页、catalog 清理、状态映射、错误分类和本地验证已具备。
- **生产就绪：否，当前只能判 PARTIAL。** 仍有第 3.2 节中的闭环、身份和运维边界，且生产真实 Nacos/Atlas/TLS/HA 尚未作为本仓库证据。

### 3.2 当前 Sink 的一致性风险

#### R1：`PushAll` 失败后重试会丢失“全量清理”语义（P1）

`DefaultWorker` 的 `SyncAll` handler 调用 `pusher.PushAll`；失败后只把实例列表放入 `UnsyncedService`，而 retry loop 通过 `FanoutSink.PushTo` 调用的是单实例 `Push`，不是 `PushAll`：

- `pkg/worker/worker.go:66-73`：SyncAll 失败后 `unsyncedService.Add(...)`。
- `pkg/worker/unsynced_service.go:237-241`：retry 统一调用 `PushTo(... pending.Instance ...)`。
- `pkg/worker/fanout.go:395-400`：`PushTo` 始终转成 named sink 的 `Push`。

影响：如果 Nacos `PushAll` 的失败发生在 catalog list 或 prune deregister，而不是某个 register，那么 5 秒重试只会重新 register 这些实例，不会重试 prune；远端 ghost 可能要等下一次 full-push interval（默认 6 小时）才被清理。这个问题是 Sink 通信闭环中优先级最高的新增发现。

#### R2：同一实例更新可能乱序到达 Nacos（P1）

`FanoutSink.Push` 对每个 Sink 启动 goroutine，K8s provider 又通过 ants pool 并发提交多个事件：

- `pkg/worker/fanout.go:288-320`：每次 Push/PushAll 都并行调用各 named sink。
- `pkg/providers/k8s/k8s.go:191-220`：同一 Pop loop 把多个状态变化提交给 pool。
- `pkg/nacos/nacos.go:81-117`：单次 Push 内部也按 bounded parallelism 执行。

当前 Nacos v1 register 是无条件 upsert；Nacos Sink 没有按 `Reversion` 做 CAS。于是同一 `InstanceId` 的 revision 10 和 revision 11 可以同时在途，revision 10 晚到并覆盖 revision 11。retry queue 的“最高 Reversion”规则只约束失败队列，不约束已成功提交但乱序完成的 happy path。`FanoutSink` 的注释声称每个 Sink 的 push 保持序列化（`fanout.go:279-283`），但当前代码没有按实例 key 建立序列化队列，这个注释与实现不一致。

#### R3：全量快照与删除事件存在 register-after-deregister race（已实测）

25 分钟 burst 观察已经复现：full-push tick 在删除事件到达前读取旧快照，旧快照的 register wave 在事件路径的 deregister 之后完成；下一次 tick 的 case-3 才清掉 ghost。证据：

- `tests/observe/results/20260911-2212-summary.md:31-77`。
- 当前 full-push 顺序：`CompareAndFlush → emitSyncAll`，证据 `pkg/providers/k8s/k8s.go:579-624`。
- `emitSyncAll` 将快照直接送进 `SyncAll`，没有在每个执行闭包中重读 provider cache，证据 `pkg/providers/k8s/k8s.go:615-624`。

这不是文档误报，代码中没有 tombstone、generation 校验或执行前 revalidation。建议至少在 Sink 入口引入短 TTL tombstone，或在 provider 提交的执行闭包中按 `InstanceId + Reversion` 重读当前 cache，拒绝旧快照注册。

#### R4：空源是保守安全策略，但不是完整闭环（P1）

为了避免一个 provider 的空列表误删另一个 provider，Nacos Sink 对空 `PushAll` 不扫 remembered pairs；provider 的 `CompareAndFlush` 也只在 `len(all)>0` 时进入对账：

- `pkg/providers/k8s/k8s.go:396-431`、`pkg/providers/consul/consul.go:458-479`。
- `pkg/nacos/nacos.go:158-182`、`602-624`。

结果是 provider 全部实例在 spotter 停机/源端空窗期间消失时，Nacos ghost 可能永久保留，除非后续出现同 pair 的非空推送或人工清理。这个取舍可以是产品决策，但不能称为“完整闭环”；需要显式的 provider ownership lease/epoch 或两阶段“确认源确实为空”机制。

#### R5：所有权只靠 clusterName，缺少 writer identity（P1/P2）

当前 `Provider -> clusterName` 只有 `k8s`/`ecs` 两个值，prune 和 Nacos authoritative GetAll 以此作为作用域：`pkg/nacos/nacos.go:630-654`、`422-472`。如果同一个 Nacos namespace/cluster 被其他 writer 使用，spotter 会把其 host 当成自己的远端集合参与 case-3/prune。`schemaVersion` 只说明 metadata 形状，不说明 owner，见 `pkg/nacos/nacos.go:656-713`。

生产启用 Nacos authoritative reconcile 前，必须确认 `k8s`/`ecs` cluster 在该 namespace 内是 spotter 独占，或增加 owner 标识并在 prune/compare 中强制过滤。

#### R6：ready 200 不等价于“可写”

`CheckReadiness` 只访问 `/nacos/v1/console/health/readiness` 并检查 HTTP 200：`pkg/nacos/client.go:207-230`。已有 soak 结果证明 Nacos 可能读可用但 Raft leaderless、所有写请求 500；soak 只能靠额外 write probe 识别：`tests/soak/assert.go:293-396`。生产启动 gate 仍可能误判“已就绪”。

#### R7：Nacos 生产路径绕过官方 SDK（P1，Nacos 启用时为 release blocker）

当前 `pkg/nacos/client.go` 和 `pkg/nacos/nacos.go` 直接拼接 Nacos v1 HTTP 请求。这样会绕过官方 SDK 的认证、token 刷新、gRPC/HTTP 路由、重连、redo/cache 和版本兼容处理；同时，后续很容易出现一部分操作走 SDK、另一部分操作继续裸 HTTP 的隐式分叉。该问题与“HTTP 基本 CRUD 是否能跑通”是两件事：当前 HTTP 功能可以判为 functional pass，但架构合规和生产发布必须判为 **NOT DONE**。

整改硬门禁：

1. `pkg/nacos` 只依赖 `NacosTransport`/SDK facade，不直接依赖 `net/http` 实现业务操作。
2. 静态检查禁止生产包新增 Nacos URL、HTTP method 和 query 拼接；HTTP 只能集中在受审计的 adapter 内。
3. register、deregister、list、query、subscribe、batch、catalog/prune、readiness probe 均必须在测试矩阵中声明“SDK 支持、官方 Admin SDK 支持、或临时例外”，不得出现未分类调用。
4. SDK 迁移完成前，Nacos Sink 不能从 PARTIAL 晋级为 production PASS；仅有 mock 或 SDK 编译成功也不能关闭该缺陷。

### 3.3 官方 Go SDK 与 gRPC 结论

**结论：官方 Go SDK 支持 Nacos 2.x gRPC，但当前仓库没有使用它。**

本地核验 `github.com/nacos-group/nacos-sdk-go/v2@v2.3.5`：

- `clients/naming_client/naming_grpc/naming_grpc_proxy.go` 提供 `RegisterInstance`、`BatchRegisterInstance`、`DeregisterInstance`、`GetServiceList` 等 gRPC proxy。
- `clients/naming_client/naming_proxy_delegate.go` 的 `getExecuteClientProxy` 按 `instance.Ephemeral` 选择：persistent (`false`) 走 `naming_http`，ephemeral (`true`) 走 `naming_grpc`。
- `BatchRegisterInstance` 直接委托 gRPC proxy，因此 SDK 有批量注册能力；这和当前仓库“手工 HTTP 单实例请求”的能力边界不同。
- SDK `constant.ServerConfig` 有 `GrpcPort`，默认由主端口加 RPC offset 推导。

官方资料：

- [Nacos Go SDK Usage](https://nacos.io/en/docs/latest/manual/user/go-sdk/usage/)：说明 Go SDK v2、Nacos > 2.x、`GrpcPort`、namespace、username/password 等配置。
- [Nacos Open API Overview](https://nacos.io/en/docs/latest/manual/user/overview/api-overview/)：说明 Nacos 3.x 的 Client Open API 主要走 gRPC，Admin/Console API 仍走 HTTP；这意味着 Nacos 2.1 的 v1 HTTP 方案不能直接假定等同于 Nacos 3.x gRPC API。
- [nacos-sdk-go v2.3.5 release](https://github.com/nacos-group/nacos-sdk-go/releases)：当前核验到的 v2 线版本。
- [nacos-sdk-proto](https://github.com/nacos-group/nacos-sdk-proto)：统一 gRPC protobuf 定义。

SDK 并不意味着可以直接替换当前 Sink：

1. spotter 目前使用 persistent instance（`ephemeral=false`），而 SDK v2 对 persistent 单实例调用会走 HTTP；切换 SDK 不自动获得 gRPC。
2. SDK 的 batch register 是 gRPC，但需要验证 Nacos 2.1.0 服务端对该 request type 的兼容性、enabled/status metadata、cluster 语义和错误码。
3. 当前 prune 依赖 catalog/admin 视图（包括 disabled host），而 SDK naming client 的常规 `GetService`/订阅视图不等价于 catalog admin view；prune 可能仍需 HTTP Admin API。
4. SDK 自带 cache/reconnect/redo 机制，可能与 spotter 自己的 retry、persistent lifecycle、leader election 叠加，必须先做语义和故障注入验证。

建议支持计划（SDK 接入从可选 POC 提升为强制整改）：

| 阶段 | 工作 | 退出条件 |
|---|---|---|
| G0 | 固定目标服务端版本，确认 2.1/2.2/3.x 的 naming gRPC、TLS、auth、batch API 和 catalog API 矩阵 | 版本兼容表和 protobuf/request 证据齐全 |
| G1 | 抽象 `NacosTransport`；接入官方 SDK facade；现有 HTTP 仅作为隔离回滚/对照 adapter | 生产路径不再散落裸 HTTP；状态/metadata/错误分类测试全绿 |
| G2 | 影子读/对照写：HTTP 与 SDK 在动态 scratch Nacos 上比较 register/deregister/list、enabled、metadata、reversion | 连续观察成功率、延迟、错误和最终 catalog 集合一致；所有差异可解释 |
| G3 | 验证 SDK batch、persistent 生命周期、catalog/prune/Admin SDK 覆盖；不能覆盖的接口必须形成带期限的例外决策 | 目标版本兼容、部分失败可定位、重试不丢 operation type；无未分类裸 HTTP |
| G4 | SDK 默认路径灰度；HTTP 只保留受控回滚开关；按 Sink 指标观察 | 连续运行和故障注入达标后，才允许删除 HTTP fallback |

不建议未经验证直接把 `nacos-sdk-go/v2` 替换进主路径：它会同时改变传输协议、连接生命周期、缓存/redo 机制和可能的 persistent 行为。但“必须通过 SDK”是发布约束，不能再将 SDK 迁移长期放在可选 POC 状态；必须先完成 adapter、兼容性回放和完整测试，再切换默认路径。

## 4. K8s Watch → Cache → Diff → Sink 审计

### 4.1 当前实际链路

```text
client-go informer callbacks
  → k8srobot coalescing queue (4096 distinct keys)
  → single Pop loop
  → GetByKey + formatInstance + VerifyInstance
  → providers.Cache diff / ReplaceOrInsert
  → ants pool (100; Submit is blocking by default)
  → worker.Handle(Sync)
  → FanoutSink.Push (Atlas and optional Nacos)
  → Nacos register/deregister or Atlas gRPC
```

主要证据：`pkg/k8srobot/k8srobot.go:184-280`、`pkg/providers/k8s/k8s.go:191-220,267-334`、`pkg/worker/worker.go:50-73`、`pkg/worker/fanout.go:288-320`。

### 4.2 已确认正确的部分

- key-based coalescing 已替代旧的纯 channel drop；drop observer、`events_dropped_total`、`k8s_queue_depth` 已存在。
- recovery buffer 会在 Pop 释放容量后回灌；事件触发时间保留最早值，满足延迟观测语义。
- K8s delete 有 cache fallback；有 IP 的已注册实例会生成 offline shell 并由 Sink 删除；无 IP 且缓存也无 IP 时会丢弃，不污染 retry queue。
- 空 `ContainerStatuses` 不再错误判为 online；CrashLoopBackOff/error、Succeeded/Failed、unhealthy empty-IP 等边界已加入测试。
- `FanoutError.Unwrap`、per-sink retry key、4xx permanent drop、catalog prune 和 Nacos authoritative routing 已加入测试。
- `make test-all` 与 e2e race 均通过；当前没有证据表明普通单实例 happy path 必然失败。

### 4.3 当前设计缺陷/风险

#### K1：事件身份缺少 cluster 维度（P0/P1）

`QueueObject.Key` 只有 `<namespace>/<name>`，`GetByKey` 会在所有 cluster store 中查找；provider 直接取 `items[0]`：

- `pkg/k8srobot/k8srobot.go:70-75,263-280`。
- `pkg/providers/k8s/k8s.go:267-272`。

同时 `InstanceId` 只使用 `pod.Name`：`pkg/providers/k8s/conversion.go:103-105`。因此：

- 两个 cluster 中相同 namespace/name 的事件会在 queue 中合并；
- B cluster 的删除事件可能取到 A cluster 的对象；
- 同名 Pod 会在 cache 和 `ListToMap` 中互相覆盖；
- Nacos 的 composite id 含 IP/cluster，但 compare 主键却只有 `InstanceId`。

这是多集群架构下的身份设计问题，不是测试未覆盖的小分支。应让事件携带 cluster identity，并让 cache/diff 主键至少为 `sourceCluster + namespace + pod UID`；如果外部协议必须保持现有 `InstanceId`，应另设不可碰撞的内部 identity key。

#### K2：Pop loop 仍可能因下游慢而阻塞（P1）

`ants.NewPool` 没有启用 nonblocking；当 100 个 worker 被 Nacos/Atlas 慢请求占满时，`Submit` 会阻塞唯一 Pop loop：`pkg/providers/k8s/k8s.go:217-220`。队列虽能记录 drop，但 recovery buffer 也只保存最多 4096 条：`pkg/k8srobot/k8srobot.go:445-466`。超过该容量的 dropped keys 只能等 full-push，不能保证毫秒级或短窗口恢复。

这是“可观测的降级”，不是已经证明的必然数据丢失；但对高峰故障仍是 P1 风险。应采用非阻塞提交 + 按实例 key 的 overflow queue，或把 provider 事件流完全交给有界、可观测的应用队列。

#### K3：all-clusters sync gate 和错误重试不可取消（P1/P2）

`monitor` 在 `HasSynced()` 未完成时使用 `time.Sleep(15s)`，Pop 错误时使用 `time.Sleep(1s)`：`pkg/providers/k8s/k8s.go:170-177,195-202`。leader 丢失时 server 取消 provider context，但这些 sleep/gate 不会立即响应；一个永久失联 cluster 会使 provider 生命周期拖尾甚至旧 provider 与新 provider 重叠。

#### K4：`--appcodes` 过滤实现只允许列表首项（P1）

`formatInstance` 中的循环在第一个不相等项就 return：`pkg/providers/k8s/conversion.go:68-75`。当允许列表为 `[a,b]` 且 appcode 为 `b` 时，会在比较 `a` 时提前丢弃。该逻辑应改为“遍历直到命中，遍历结束仍未命中才丢弃”。这是当前可直接修复的确定性 bug。

#### K5：同一 Pod 的多事件在 cache diff 后仍可并行 push（P1）

coalescing 只发生在 Pop 前；一旦第一个事件已 Pop 并提交 pool，后续 update 仍可能形成新的 worker event。`hasInstanceDiff` 只保护本地 cache，不保护下游顺序：`pkg/providers/k8s/k8s.go:303-345`。结合 R2，会把 revision-order 问题传递到 Nacos。

#### K6：Pod 重建/同名复用可能留下旧 composite id（P1/P2）

同一个内部实例名在 delete+recreate 被 coalesced 时，若只看到最后的 Add，provider 会注册新 IP，但没有旧 IP 的 per-instance deregister；旧 composite id 依赖下一次 `PushAll` prune 才清理。这与 R3 是同一类快照/身份竞态。

## 5. Reconcile 与 Sink 通信闭环

### 5.1 当前闭环形态

默认配置下，`FanoutSink.GetAll` 从首个 Sink（Atlas）读取；只有设置 `--reconcile-source nacos` 才读取 Nacos catalog：`pkg/worker/fanout.go:341-368`，`internal/server.go:338-356`。写路径始终 fanout 到所有 Sink；读取路径只有一个 designated source。

因此，启用 Nacos Sink 并不等于启用 Nacos authoritative reconcile。生产若未同时设置 `--reconcile-source nacos`，Nacos 的字段漂移主要依赖周期性的 `PushAll` 全量 upsert/prune；启动时 Atlas 没有漂移时，Nacos 可能不会立刻被修复。

### 5.2 操作闭环矩阵

| 场景 | 当前行为 | 闭环判定 |
|---|---|---|
| 本地 online，远端缺失 | case-2 推送本地；PushAll 也会 upsert | 基本闭环 |
| 本地字段变化 | Nacos authoritative 模式可由 compare 推送；默认 Atlas 模式依赖事件或下一次 full push | **配置依赖** |
| 远端同 ID metadata/enabled 漂移 | Nacos authoritative 模式可修复；默认模式不保证及时修复 | **PARTIAL** |
| 远端 composite id 变化/IP 变化 | 新 id register，旧 id 由 PushAll catalog prune 清理 | 基本闭环，但受 R3 竞态影响 |
| 远端有、本地无 | K8s case-3 生成 offline；Consul 在 Nacos 模式生成 offline，Atlas 模式仍是 status-2 | Nacos 模式基本闭环；空源除外 |
| Nacos catalog/list 读取失败 | Nacos 模式 K8s compare 跳过 tick；Consul compare 返回 | 安全但会延迟收敛 |
| `PushAll` register 失败 | 整个 SyncAll 进入 per-instance retry | **不完整：丢失 prune operation** |
| `PushAll` prune 失败 | 同上，retry 只做单实例 Push | **不完整：远端 ghost 可能等下一 full tick** |
| provider 全量为空 | compare 不进入，Nacos empty PushAll 保守 no-op | **明确残余，不是完整闭环** |
| Nacos ready 200 但 leaderless | 启动 gate 可能放行；写入全失败，soak 需额外 probe | **生产 gate 不完整** |
| 非 spotter writer 共用 namespace/cluster | 可能被 compare/prune 当作 spotter-owned | **所有权未证明** |

### 5.3 要称为“完整闭环”必须满足的条件

1. 每个事件有不可碰撞的 source identity，且同一实例的写入按 revision 串行或由 Sink/CAS 防旧值覆盖。
2. Retry entry 保留 `OperateType`、provider/scope 和必要的 full-push operation；prune 失败必须可独立重试。
3. Nacos authoritative 模式在生产配置中显式启用，或系统在 Nacos Sink 存在时强制启用并记录配置。
4. 空源必须经过 ownership lease/连续确认后才允许删除；当前的“空即 no-op”应作为安全模式，而不是成功闭环。
5. Nacos 的 owner、namespace、cluster、schema 和版本兼容必须可验证；读取失败不能被误当成空集。
6. 写就绪检查必须覆盖真实写入路径或可证明等价的 write probe，而不只是 console readiness 200。

## 6. 优化优先级与后续计划

### P0：先解决会造成错误数据的并发和身份问题

1. **统一事件身份**：`clusterID + namespace + pod UID` 作为内部 key；`QueueObject`、cache、diff、retry 和 forensic record 全部携带它。兼容外部 `InstanceId` 时，不再用 pod name 单独做内部主键。
2. **同实例串行/版本保护**：在 provider-to-sink 之间增加 per-key ordered executor，或在 Nacos Sink 建立 `latest revision`/tombstone 状态；旧 revision 的 register 不得覆盖新 revision。对 full-push closure 增加执行前 cache/generation revalidation。
3. **保留 PushAll 操作类型**：`pendingPush` 至少携带 `OperateType`、provider scope 和 batch identity；单实例失败可重试 `Push`，prune/list 失败必须重试 `PushAll` 或等价 prune task。

### P1：补齐生产一致性边界

4. 修复 `--appcodes` 首项提前 return 的确定性 bug，并增加多元素回归测试。
5. 将 Nacos authoritative reconcile 与 `--nacos-addr` 的配置关系显式化：默认启用、或启动时强制要求 operator 明确选择；不能让“写了 Nacos 但仍读 Atlas”成为隐含行为。
6. 设计 provider ownership lease/epoch；空源删除需要连续观测和 owner 证明，避免源故障被解释为“所有实例已删除”。
7. `HasSynced`/Pop/Consul watch 全部改成 context-cancellable wait；ants `Submit` 非阻塞化并把 overflow 进入有界、可观测、按 key 合并的队列。
8. 把 Nacos readiness 改为 read + write probe；补齐 TLS CA、username/password、namespace、group、server list 的配置和测试。
9. Atlas 生产 codec 做真实环境验证：确认服务端 proto/JSON codec、RPC method path、TLS 和返回码；在未验证前保留“本地 JSON only”警示。

### P2：收尾工程质量和观察能力

10. 修复 observe `logLineTime`：先解析 zap JSON 的 `ts` 字段，再解析行首时间；ledger 在 apply 请求发出前写入，并在失败时回滚/标失败。
11. **Accepted non-goal（当前范围）**：不把 K8s 的 2h/1000 结果外推为 Consul 规模证明。只有重新启用 ECS/机器部署、或 Consul 成为本项目的规模目标时，才启动独立的 Consul 1000+ 观察；届时由平台/服务发现负责人补充车辆、指标和验收窗口。
12. 清理 legacy globals：providers、election、notice、logging、config 全部走 ports；删除或隔离 `aggregate` scaffolding。
13. 清理两个 vet 错误：`tools/cache/cache.go:52` 的无格式占位符调用，以及 `pkg/providers/k8s/k8s.go:68` 的 unkeyed `RN` literal；随后重新运行 `go vet ./...`。
14. 将 `operations.md`、`architecture.md`、`testing.md` 的历史状态更新到当前：Go 1.25/etcd v3.6、Nacos flags、当前 test matrix 和两条实际 vet 诊断。

### 6.1 建议的验证门禁

每个 P0/P1 修复至少需要以下证据：

- 单元测试 + `-race`；
- Nacos mock 与真实 Nacos 2.1 scratch 环境各一组；
- 同一实例 revision 乱序、delete/register 交叉、PushAll prune 失败、leaderless/readiness 失败注入；
- Nacos catalog 与 instance/list 双视图核对；
- 1000+ K8s 观察至少 30 分钟 rehearsal，再做 2h；
- 修改 retry、fanout、provider identity 后重新跑 `make test-all`，并保存关键指标、drop count、retry depth、max heal 和最终集合 hash。

## 7. 证据索引

核心当前代码：

- K8s provider：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/pkg/providers/k8s/k8s.go`
- K8s conversion：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/pkg/providers/k8s/conversion.go`
- Robot：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/pkg/k8srobot/k8srobot.go`
- Worker/Fanout/Retry：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/pkg/worker/{worker.go,fanout.go,unsynced_service.go}`
- Nacos Sink/Client：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/pkg/nacos/{nacos.go,client.go}`
- Server wiring：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/internal/server.go`
- Current metrics：`/Users/d-robotics/go/src/github.com/ahakipper/mfwregistry-adapter/pkg/metrics/stat.go`、`internal/infra/metrics/metrics.go`

主要历史/设计文档：

- 当前架构：[architecture.md](architecture.md)
- 数据模型：[data-model.md](data-model.md)
- 运维：[operations.md](operations.md)
- DDD 目标：[ddd-architecture.md](ddd-architecture.md)
- Nacos 计划：[nacos-sink-plan.md](nacos-sink-plan.md)
- DSCA 交付：[dsca-delivery.md](dsca-delivery.md)
- Burst 结果：[20260911-2212-summary.md](../tests/observe/results/20260911-2212-summary.md)
- 2h 持续观察：[20260911-1914-summary.md](../tests/observe/results/20260911-1914-summary.md)

## 8. 独立 reviewer 复核记录

第二轮 reviewer 已完成只读反向核验，结论为 **PASS（技术风险成立，文档仅需一处范围语义调整）**：

- R1 的 `SyncAll → Push` 重试语义是否确实丢失 prune；
- R2 的同实例乱序是否被现有 Nacos/Atlas 语义或测试覆盖抵消；
- K1 的 cluster 维度身份风险是否是可达生产场景；
- Nacos SDK persistent/ephemeral 与 gRPC 的判断；
- “HTTP functional pass / production partial”的边界；
- 当前两条 vet 诊断和文档 stale 判定。

Reviewer 确认：

1. R1 成立：`SyncAll` 失败进入 retry 后经 `PushTo` 降级为单实例 `Push`，不会重试 Nacos `PushAll` 的 catalog prune；该结论由 `worker.go:66-70`、`unsynced_service.go:221-241`、`fanout.go:397-404` 共同证明。
2. R2 成立：Fanout 和 provider ants pool 允许同一实例的多个 revision 并行进入 Nacos；Nacos register 是无条件 upsert，retry queue 的最高 revision 规则不能保护已成功但乱序完成的 happy path。`fanout.go:288-320`、`k8s.go:217-219`、`nacos.go:81-117` 的注释/实现不一致已被确认。
3. K1 成立：`QueueObject.Key`/`GetByKey`/`InstanceId` 缺少 cluster 维度，`items[0]` 选择可跨集群取错对象；证据为 `k8srobot.go:366-377`、`k8s.go:269-273`、`conversion.go:103-105`。
4. Nacos SDK 判断成立：v2.3.5 的 `getExecuteClientProxy` 对 persistent instance 走 HTTP、ephemeral 走 gRPC，`BatchRegisterInstance` 走 gRPC；“SDK 有能力”不等于“本仓库已启用”。
5. HTTP Sink 保持“功能 PASS / 架构不合规 / 生产 PARTIAL”是正确边界；ready 200、auth/namespace、真实 HA/TLS、所有权和 SDK 统一接入仍不能被本地 mock 证明。
6. 空源/ownership/readiness 结论应统一理解为“安全保护优先但闭环不完整”，而不是已完成闭环。
7. Consul 同规模观察应改为 **accepted non-goal**，而不是当前范围内的缺陷；重新启用 ECS/机器部署时再打开该门禁。

本次定稿已据此完成：Consul 项目状态改为 accepted non-goal，新增触发条件；本次用户补充约束进一步将 Nacos SDK 统一接入列为 P1 强制整改和 Nacos 启用时的 release blocker；其余技术风险和整改优先级保持不变。

**Reviewer 状态：** PASS
