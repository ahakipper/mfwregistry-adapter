# spotter 全面修复与验证计划

> **执行说明：** 本计划以 [system-readiness-consistency-audit-2026-09-12.md](system-readiness-consistency-audit-2026-09-12.md) 为输入，按工作包和提交批次执行。每个批次必须先补测试、再改实现、再跑门禁；任何 P0/P1/P2 未关闭时不得宣布批次完成。

**目标：** 修复 Spotter 的实例身份、事件顺序、全量对账、Nacos Sink 通信闭环和生产配置边界，并形成可重复的测试与 E2E 证据链。

**计划版本：** v6 FINAL（补充 Nacos SDK 强制接入约束，已完成独立 reviewer 复核）

**版本变更：** v6 将“禁止 Nacos 生产路径裸 HTTP、统一经官方 Nacos SDK/facade”从可选 POC 提升为 P1 强制整改和 Nacos 启用时的发布门禁，并补充 SDK 迁移、例外管理和完整测试矩阵。

**当前基线：** `refactor/all` / `fd1f539`。A0→A3、B1、B2 HTTP 过渡层和 B3 SDK seam 已按阶段提交并通过 focused/full/race 测试；`go vet ./...` 仍有两条已知诊断（`tools/cache/cache.go:52`、`pkg/providers/k8s/k8s.go:70`）。Nacos naming 已默认经官方 SDK，catalog/prune、cluster Admin、readiness 仍是有期限的 audited HTTP 例外；真实 Nacos/Atlas 证据、DDD/通知与 vet 收口仍未闭环。

**范围边界：** K8s 是本阶段规模主路径；Consul 1000+ 规模观察是 accepted non-goal，只有重新启用 ECS/机器部署时才开启独立里程碑。Nacos SDK 统一接入是生产必做项；兼容验证完成前可保留 HTTP 回滚/对照通道，但不能把裸 HTTP 作为最终生产路径。

**执行状态（2026-09-13）：** `b1d9e2f` 已完成 B2 的 HTTP compatibility foundation：多地址 failover（5xx/transport 可切换、4xx 停止）、显式 namespace/group/auth/TLS/timeout、CLI→Config wiring、read+write readiness canary（成功地址固定 register/deregister，清理失败告警）、custom scope PushAll/prune/GetAll 回归测试。`fd1f539` 完成 B3 SDK seam：生产默认 `sdk`，naming lifecycle/query/subscribe 走官方 SDK，HTTP 仅集中在已登记的 catalog/prune、cluster Admin、readiness 例外。真实 Nacos 版本验证仍未提供，因此 Nacos 发布状态仍为 NOT VERIFIED。

## 1. 不可变的验收原则

1. **身份不混淆。** 内部主键必须区分 source cluster、namespace、Pod UID；外部 `InstanceId` 是否保持旧格式必须由兼容测试证明，不能靠 pod name 唯一性假设。
2. **旧状态不可覆盖新状态。** 同一 source identity 的 register/update/delete 必须按 source revision/sequence 有序执行；Nacos 不能出现旧 revision 晚到覆盖新 revision。
3. **全量操作可重放。** `PushAll` 的 register、catalog list、prune、deregister 每个失败阶段都必须可重试；retry 不得把 prune 错误静默降级成单实例 Push。
4. **空源不等于已删除。** 只有经过 ownership proof 和连续空窗确认，才允许批量删除远端；默认保护策略不能被描述成完整闭环。
5. **读取失败不等于空集合。** Nacos/Atlas/Consul/K8s 任意读取错误必须进入显式 error 状态，不能触发全量误删或全量重推。
6. **双视图可解释。** Nacos `instance/list`（可能隐藏 disabled）与 `catalog/instances`（包含 disabled）必须分别定义用途；任何比较都记录使用的视图和 namespace/group/cluster scope。
7. **生产证据和 mock 证据分离。** mock/e2e 只证明代码契约；真实 Nacos/Atlas 的 TLS、auth、leaderless、版本兼容必须有单独 scratch/pre-prod 证据。
8. **每个 P0/P1 必须有负向测试。** 只证明 happy path 不算关闭；必须包含超时、乱序、重复、空列表、部分失败、重启和恢复。
9. **可回滚。** 每个批次保留旧配置/旧行为开关，默认行为不发生未记录的破坏性变化；变更只能在后续批次移除开关。
10. **Nacos 访问必须经 SDK。** Nacos naming、查询、订阅、注册、注销和 Admin/Catalog 操作必须通过官方 SDK 或受审计的 SDK facade；生产代码不得散落裸 `net/http`。HTTP fallback 只能在迁移窗口内作为显式、可回滚的兼容通道，不能作为最终完成标准。

## 2. 当前测试与 E2E 起始评估

### 2.1 已有且可以立即运行

| 命令 | 当前证据 | 证明范围 |
|---|---|---|
| `make test-all` | 当前已通过 | allowlist 单测/race、blackbox、smoke、loopback e2e |
| `go test -tags=observe -run '^TestObserveUnit' ./tests/observe/... -count=1` | 当前已通过 | observe 比对引擎、ledger/记录的单测层 |
| `go test -race -count=1 -tags=e2e ./tests/e2e/...` | 当前已通过 | consul→worker→Atlas/Nacos mock、elector、Nacos reconcile、4xx 负向路径 |
| `go vet ./...` | 当前失败 | `tools/cache/cache.go:52`、`pkg/providers/k8s/k8s.go:68` 两条静态问题 |

### 2.2 当前测试不能证明的内容

- 没有真实 K8s informer→多集群同名对象的端到端测试；当前 K8s provider 测试主要是 white-box/fake robot。
- 没有测试同一实例 revision 乱序完成后 Nacos 最终状态仍为最新 revision。
- 没有测试 `PushAll` 的 catalog/prune 失败在 retry 中仍保留全量 operation type。
- Nacos mock 不证明真实服务的 auth、TLS、namespace、Raft leaderless、版本差异和 write readiness。
- Atlas discoverymock 使用 JSON codec，不证明生产 Atlas 接受 JSON 或接受当前 method path/字段布局。
- `make test-observe` 默认依赖已存在的 kwok cluster/kubeconfig 和本地 Nacos image；当前 `tests/observe/stack.go` 只负责 Nacos 容器，不负责创建/删除 kwok cluster，不能称为完全自包含栈。
- `tests/observe` 的 log slice 曾可能解析不到 zap JSON 内的 `ts`；当前修复已支持 zap 字符串/数值和行首时间戳，并把 apply/delete ledger clock 提前到 API 调用前。完整 2h OBS 仍需在自包含环境重新执行，不能由本地单测替代。

### 2.3 E2E 是否可以开始

**可以，但分三层：**

1. **离线 loopback E2E：立即可开始。** `make test-e2e` 不依赖企业 etcd、Consul、Atlas 或 Nacos；所有 mock 应绑定 `127.0.0.1` 动态端口。
2. **真实 Nacos scratch E2E：条件可开始。** 必须有 Nacos 2.1.x image、Docker/Colima、scratch 端口、512MB JVM 预算、健康与写探针；不得使用 demo 的 18848。
3. **observe 2h：修复 harness 自包含性后开始。** 必须由脚本创建临时 kwok cluster、生成独立 kubeconfig、启动独立 Nacos/Atlas/etcd/spotter child，并在失败时区分 EnvError、ConfigError、InfraError、TestError。

## 3. 工作包总览与依赖

```text
WP-0A 身份模型 ─┬─> WP-0B 有序写入/快照校验 ─┬─> WP-0C PushAll 可重放
                │                            │
                └────────────────────────────┴─> WP-1A Nacos ownership/空源

WP-1B Nacos 配置/HTTP 过渡 ──> WP-1C 官方 SDK 强制迁移 ──> WP-1D 灰度与 HTTP fallback 下线决策
WP-1E Atlas 真实 codec 验证（独立，可并行）
WP-2A 测试补齐 ──> WP-2B E2E/observe 门禁 ──> WP-2C 全量验收与文档更新
WP-2V vet 清零（独立，尽早完成）
```

提交顺序建议：

1. `A0` 基线与测试夹具；
2. `A1` 身份模型；
3. `A2` 有序写入与 burst race；
4. `A3` PushAll retry；
5. `B1` 空源/ownership；
6. `B2` Nacos HTTP 配置与 write readiness；
7. `B3` 官方 SDK 迁移与协议兼容性验证（强制）；
8. `B4` Atlas codec；
9. `C1` observe/E2E 门禁；
10. `C2` DDD globals/通知；
11. `C3` vet、文档和最终回归。

任何批次之间都运行完整回归，不允许只运行修改包。

## 4. A0：建立可回归的基线夹具

**目标：** 先把当前行为、测试数量和运行环境固定下来，后续每个 P0/P1 都能证明“修复了什么、没有破坏什么”。

**文件：**

- 修改 `Makefile`、`docs/testing.md`；
- 新增或完善 `internal/testkit/fakes`、`internal/testkit/nacosmock`、`internal/testkit/discoverymock` 的故障注入接口；
- 新增测试结果清单，不提交大体积原始日志。

**步骤：**

1. 固定 `make test-all`、`go vet ./...`、`go test -tags=observe -run '^TestObserveUnit'` 的基线输出和失败原因。
2. 为 fake Sink 增加可脚本化序列：success、4xx permanent、5xx retry、timeout、prune-only error，并记录 Push/PushAll/PushTo 的 operation type。
3. 为 Nacos mock 增加 catalog/list 双视图、按 cluster/namespace/group 过滤、独立的 list/prune 错误注入和请求计数。
4. 为 fake Robot 增加多 cluster、同 namespace/name、不同 UID、delete/recreate 事件序列。
5. 为每个夹具添加 `var _ interface = (*fake)(nil)` 编译期断言。

**验收：** 所有新负向夹具测试先在当前实现上按预期失败；`make test-all` 原有测试保持通过。提交 `A0`。

## 5. A1：统一 K8s source identity（P0）

### 5.1 目标与设计

解决 `QueueObject.Key=<namespace>/<name>`、`GetByKey` 跨所有 cluster 返回集合、provider 取 `items[0]`、`InstanceId=pod.Name` 导致的串集群/串实例问题。

采用兼容设计：

- `k8srobot.Cluster` 新增稳定 `ID`；旧配置没有 ID 时由规范化 kubeconfig 路径生成稳定 hash，并在启动日志中记录映射。
- `QueueObject` 新增 `ClusterID` 和 `UID`，保留原 `Key` 供旧调用方使用。
- 队列内部 key 使用 `ClusterID + "/" + namespace + "/" + UID`；UID 缺失时才回退到 `ClusterID + "/" + namespace + "/" + name`。
- `Instance` 增加内部 `SourceKey`（JSON 不出线），保持旧 `InstanceId=pod.Name` 的外部兼容；Nacos metadata 增加 `sourceKey` 和 `sourceCluster`，旧条目没有时按 legacy fallback 处理。
- 新增 `instance.IdentityKey(*Instance) string`：优先 `SourceKey`，其次 `Provider + ":" + InstanceId`，禁止业务代码直接以 `InstanceId` 作为内部 map key。
- cache、retry key、provider compare、observe ledger 全部改用 `IdentityKey`；Nacos composite id 是否需要按 source cluster 拆分，在 A1 的 compatibility gate 中验证 IP/port 重叠风险。若任一 source cluster 存在相同 `(service, IP, port)`，或无法证明 generic `k8s`/`ecs` clusterName 独占，必须启用 source-scoped cluster 命名（例如 `k8s-<8hex clusterID>`），并新增 `NacosScopeMap` 让 `GetAll`/prune 遍历每个已拥有 scope；legacy `k8s`/`ecs` 条目在迁移完成前只读、不 destructive prune。
- Robot 保留 `GetByKey` 兼容方法，增加 `GetByClusterKey(resource, clusterID, key)`；K8s provider 只使用带 cluster 的方法。

这是双通道迁移：`QueueObject.ClusterID/UID` 和 `IdentityKey` 先只改变内部寻址；现有 wire `InstanceId`、旧 Nacos metadata 和旧 `k8s` clusterName 均保留。只有当兼容回放证明同名 Pod 在不同 cluster 中不会互相覆盖、旧条目能被 legacy fallback 正确读取、且 Nacos composite id 在目标集群中无 IP/port 碰撞时，才允许讨论改变外部/Nacos cluster identity。兼容回放必须输出“旧 InstanceId → 新 SourceKey → Nacos composite id”的 1:1 映射表；任一对象出现 0 个或多个目标映射，A1 不能合并。

### 5.2 文件与测试顺序

**文件：** `pkg/k8srobot/k8srobot.go`、`pkg/providers/k8s/k8s.go`、`pkg/providers/k8s/conversion.go`、`pkg/providers/cache.go`、`internal/domain/instance/model.go`、`internal/domain/instance/rules.go`、`pkg/worker/unsynced_service.go`、Nacos `metadataOf/reconstruct`。

- [ ] 先写 `TestRobotSameKeyDifferentClustersRemainDistinct`。
- [ ] 先写 `TestK8SPod2InstanceUsesEventClusterNotItemsZero`。
- [ ] 先写 `TestIdentityKeyUsesUIDAndCluster`。
- [ ] 先写 Nacos round-trip 的 `sourceKey` 保留测试以及 legacy 无 sourceKey 回退测试。
- [ ] 先写 `TestRetryKeysDoNotMergeSamePodNameAcrossClusters`。
- [ ] 执行 `go test -race ./pkg/k8srobot ./pkg/providers/k8s ./pkg/providers ./pkg/worker ./pkg/nacos -count=1`。
- [ ] 执行 `make test-all`。

### 5.3 兼容与回滚

先保持 wire `InstanceId` 和默认 Nacos clusterName 不变；只增加内部 SourceKey/metadata。若发现真实下游依赖 metadata 白名单，先关闭 sourceKey 写入并保留内部 key；在未证明 Nacos cluster/IP 重叠前，不切换 composite cluster 命名。回滚时只关闭 sourceKey 写入和新寻址读取，保留旧 metadata/InstanceId；不得通过删除 Nacos 数据回滚。

**完成标准：** 多 cluster 同 key、同名 Pod delete/recreate、legacy remote entry 三组测试在 `-race` 下通过，且没有 `InstanceId` 直接作为内部主键的生产引用（`rg` 门禁）。提交 `A1`。

## 6. A2：同实例有序写入与 full-push 快照竞态（P0）

### 6.1 目标

修复两类问题：

1. 同一实例的多个事件已经 Pop 后仍可并行进入多个 Sink，Nacos 无 CAS，旧 revision 可能覆盖新 revision。
2. `CompareAndFlush/emitSyncAll` 使用旧快照，register 可能在 delete deregister 之后到达，复现已观察到的 burst delete race。

### 6.2 推荐实现

增加一个应用内 `OrderedPushGate`（放在 `pkg/worker`，后续迁移到 application layer）：

```go
type PushIdentity struct {
    SourceKey string
    Provider  string
}

type PushEnvelope struct {
    Identity PushIdentity
    Scope    string
    Revision int64
    Sequence uint64
    Operate  OperateType
    Trigger  int64
    Data     []*instance.Instance
}

type OrderedPushGate interface {
    Submit(env PushEnvelope, fn func() error) error
    Close() error
}
```

落点必须固定：`DefaultWorker.Handle` 是所有 provider 出站事件的唯一入口，新增可选的 `orderedDispatcher` 依赖，并给 `worker.Event` 增加 `IdentityKey string`、`Revision int64`、`Sequence uint64`、`Scope string`、`BatchID string` 字段；旧调用方零值保持兼容。`OperateTypeSync` 按 `IdentityKey` 串行，`OperateTypeSyncAll` 按 `Scope` 作为 barrier。K8s provider 负责填充这些字段，worker 不从实例名称猜身份；FanoutSink 只负责 sink 并行和错误聚合，不再声称“每个 Sink 的调用天然串行”。`PushAllTo(name, trigger, scope, batchID, instances)` 作为 Fanout 的显式重试接口，与 `PushTo(name, trigger, instances)` 分开，避免 full operation 在 retry 中丢失。

字段注入责任和缺省策略必须固定：

| 字段 | 写入者/时机 | 缺省行为 |
|---|---|---|
| `IdentityKey` | K8s provider 在 `pod2Instance` 成功转换后写入；Consul provider 使用 `provider + ":" + service instanceId` | `Sync` 缺失时拒绝进入 ordered gate，记录 `invalid_event{reason=missing_identity}`；兼容 legacy 事件只允许走显式 legacy fallback 并告警 |
| `Scope` | provider 在 Compare/SyncAll 读源时写入，格式为 `provider/namespace/group/cluster-scope` | `SyncAll` 缺失时拒绝 PushAll，不能默认为全局 scope |
| `Revision` | K8s=`ResourceVersion`，Consul=`ModifyIndex` | 0 只允许测试/legacy fallback；生产事件记录 `invalid_event` 并不写入 Nacos |
| `Sequence` | provider 出站 dispatcher 单调递增；重启后由新 epoch 前缀隔离 | 缺失按 0 仅用于旧事件；新代码缺失必须被测试捕获 |
| `BatchID` | SyncAll 构造时生成 UUID/epoch+tick | Sync 使用空值；SyncAll 缺失拒绝进入 retry batch |

新增 `TestEventMissingIdentityRefusedOrFallbackWithReason`，同时覆盖拒绝路径和 legacy fallback 路径；`UnsyncedService` 只透传字段，不重新猜测 identity/scope。

实现规则：

- 每个 identity 只有一个执行链；不同 identity 仍可并行。
- `Sequence` 在 provider 事件进入 gate 前递增；`Revision` 用于跨重启/远端回放判断。
- 新 envelope 若比同 identity 的待执行 envelope 旧，则丢弃并记录 `events_superseded_total`；delete/offline 不得被普通 update 淘汰。
- Nacos Sink 增加按 identity 的 in-flight lock 和 latest revision/sequence guard：网络调用期间持有该 identity 的锁；完成前检查是否已有更新的 desired state；旧 full-push register 不得覆盖新 delete。
- `emitSyncAll` 不在构造 Event 前读取一次并直接提交旧 slice；改为提交带 `Scope + SnapshotGeneration` 的 envelope，执行时重新读取当前 cache/source；执行前 generation 不匹配则重新构造该 scope 的动作。full-push 的每个实例操作都必须经过同一 identity gate，不能在一个绕过 gate 的 PushAll goroutine 中直接写 Nacos。
- 如果执行前 revalidation 仍无法覆盖跨进程旧写，增加短 TTL tombstone；tombstone 的 TTL 必须由 `max(request timeout, retry window, observed full-push overlap)` 计算，不能固定成未验证的常数。

### 6.3 测试先行与验收

- [ ] `TestOrderedGateSameIdentityRevision10Then11AlwaysEndsAt11`。
- [ ] `TestOrderedGateDeleteCannotBeSupersededByOldFullPush`。
- [ ] `TestNacosLateOldRegisterIsSkippedOrCannotOverwriteTombstone`。
- [ ] `TestFullPushRevalidatesSnapshotBeforeRegister`。
- [ ] `TestPushAllBurstDeleteConvergesWithinBound`，用事件时序精确重现 `20260911-2212` 的 race。
- [ ] 运行 `go test -race -tags=e2e ./pkg/worker ./pkg/providers/k8s ./pkg/nacos ./tests/e2e -count=1`；不带 `-tags=e2e` 的命令不能作为 E2E 门禁。
- [ ] 运行至少一次真实 Nacos scratch 的 register/delete/register 回放，读取 catalog 确认最终 revision/state。

**验收：** 同一 identity 的最终 Nacos state 只能是最新 sequence；burst delete 不得再出现 register-after-deregister ghost；若不能把收敛压到事件路径尾部，必须明确记录 remaining bound 和原因。提交 `A2`。

### 6.4 最终接口收敛清单（A2/A3 的强制契约）

为避免实现分叉，先在 `internal/ports` 定义数据结构，再在 `pkg/worker/types.go` 用 type alias 保持旧包名兼容：

```go
type Event struct {
    Trigger     int64
    Data        []*instance.Instance
    Operate     OperateType
    IdentityKey string
    Revision    int64
    Sequence    uint64
    Scope       string
    BatchID     string
}

type RetryOperation struct {
    Sink      string
    Operate   OperateType
    Provider  string
    Scope     string
    BatchID   string
    Identity  string
    Revision  int64
    Sequence  uint64
    Trigger   int64
    Instances []*instance.Instance
}

type EventQueue interface {
    Add(op RetryOperation)
    Len() int
    Drain() []RetryOperation
}
```

`sinkFanout` 的最终签名固定为：`PushTo(name string, trigger int64, instances []*instance.Instance) error`、`PushAllTo(name string, trigger int64, scope string, batchID string, instances []*instance.Instance) error`、`Sinks() []string`。旧 `Event` 调用方只填 `Trigger/Data/Operate` 时，`Sync` 走 legacy fallback 并产生 `invalid_event`/兼容指标；`SyncAll` 缺少 `Scope` 或 `BatchID` 必须拒绝入队。`UnsyncedService` 仅读取和透传这些字段，不重新构造 identity。该清单对应的 compile-time assertions、fake 实现和迁移 adapter 必须在 A2/A3 同一批次提交。

## 7. A3：PushAll/Prune 失败可重放（P1）

### 7.1 目标与数据结构

当前 retry 只保存 instance，并通过 `PushTo` 调 `Push`；必须改用第 6.4 节定义的 `ports.RetryOperation`，保留 operation type、provider、scope、batchID、identity、revision、sequence、trigger 和 instances。

`EventQueue` 改为按 `(sink, operate, scope, identity/batchID)` 存储；`internal/ports.EventQueue` 必须从当前仅有的 `Add(triggerTime, instances)` 扩展为接收 `RetryOperation`（或等价的明确结构），否则 operation type 仍会在接口边界丢失。同一 identity 的 incremental 失败不能覆盖 full-push batch，full-push batch 也不能覆盖更新的 incremental delete。新增 `FanoutSink.PushAllTo(name, trigger, scope, batchID, instances)`，并在 `sinkFanout` 接口中加入同名方法；普通 `PushTo(name, trigger, instances)` 永远不承载 full operation。所有 fake sink、recording sink 和 e2e harness 必须同时实现并断言 `PushTo` 与 `PushAllTo`。

### 7.1.1 迁移与回退顺序

为避免一次性改写 `internal/ports.EventQueue` 造成编译和行为切换窗口，按三步执行：

1. **兼容双写阶段：** 保留旧 `Add(trigger, instances)` 作为 adapter；每次失败同时生成 legacy instance/sink 记录和 `RetryOperation` 记录，但只消费 legacy 记录，新增 metrics 比较两者的 key、scope、operation 和 revision，不执行 destructive delete。
2. **新消费阶段：** A3 测试和 E2E 通过后，打开 `retry_operation_v2=true`；只消费 `RetryOperation`，legacy 记录保留只读影子用于核对一个完整 retry window，不再重复推送。
3. **清理阶段：** 连续一个完整 full-push interval 无 key 漂移、无重复消费、无 batch 丢失后，删除 legacy adapter；此步骤单独提交。

回退步骤是可执行的：将 `retry_operation_v2=false`，停止新队列消费者，保留新队列快照并把尚未成功的 `RetryOperation` 映射回 legacy `(instance,sink)` 条目；映射必须保留 instance、sink、最高 revision 和原 trigger，`SyncAll`/prune 项不得映射成普通 `Push`，无法映射的项进入 `rollback_blocked` 告警而不是丢弃。

双写切换的硬门禁：切换前必须完成至少一个完整 `full-push interval` 加一个 retry interval 的 shadow window；期间 `legacy_pending_count == v2_pending_count`，`operation_missing=0`、`operation_duplicate=0`、`operation_retyped=0`、`invalid_event=0`，每个 `BatchID/Scope/Identity` 的 replay hash 逐项相等，且没有任何未分类的 `rollback_blocked`。切换必须在 queue mutex 的 quiescent point 原子完成；任一条件不满足则保持 legacy 消费、记录 `retry_migration_blocked`，不得部分切换。回退后再跑一个完整 retry window，确认新旧快照和最终 Nacos catalog hash 一致，才允许重新尝试。

### 7.2 行为规则

- `Sync` 失败：按 instance/sink 保留最高 revision，调用 `PushTo`。
- `SyncAll` 的 register 失败：记录失败实例；可重试的 5xx 继续以 `SyncAll` operation 重放，或拆为明确的 register subtask，但不能无标记降级。拆分时必须保留原 batch 的 `scope/batchID`，否则同一批次的 prune 可能被提前判成功。
- `SyncAll` 的 catalog list/prune/deregister 失败：必须重试等价 `PushAll`/prune task；成功前不能删除 batch queue key。
- 4xx permanent 错误只能删除对应不可修复 task，并记录 sink、scope、instance/composite id；不能把整个 batch 静默 drop。若 API 只返回 batch 级 4xx，必须把 batch 标成 `permanent_unknown_scope` 并告警，由人工确认后才能清理，不能自动清空所有实例。
- 重试成功后使用 compare-and-delete，不能删除重试期间新插入的更高 revision/sequence。
- metrics 至少记录 `retry_operations_total{sink,operate,outcome}`、`retry_queue_depth{sink,operate}`、`prune_failures_total`。

### 7.3 测试与验收

- [ ] `TestSyncAllPruneFailureRetainsSyncAllOperation`。
- [ ] `TestSyncAllRetryCallsPushAllNotPush`。
- [ ] `TestIncrementalAndFullPushQueuesDoNotOverwriteEachOther`。
- [ ] `TestPrune5xxRetriesAndDeletesGhostAfterRecovery`。
- [ ] `TestPrune4xxDropsOnlyPermanentTask`。
- [ ] 在 Nacos mock 中只让 catalog list 失败，确认下一次重试确实再次访问 catalog；再让 register 失败，确认错误可拆分。
- [ ] 运行 `go test -race -tags=e2e ./pkg/worker ./pkg/nacos ./tests/e2e -count=1`；不带 `-tags=e2e` 的命令不能作为 A3 重放门禁。

**完成标准：** Nacos ghost 在 prune 失败后不需要等待下一个 6h interval 才有机会清理；operation type、scope、batch identity 在日志/metrics/测试中可见。提交 `A3`。

## 8. B1：Nacos ownership、空源与 Reconcile 语义（P1）

### 8.1 目标

将“clusterName 相等”升级为可验证的 writer ownership，并把空源从隐式 no-op 变成显式状态机：`ObservedNonEmpty → EmptyCandidate → EmptyConfirmed → DeleteAllowed`。本工作包分两步，不假设当前 etcd 已有可复用的 ownership lease。

### 8.2 设计

- Nacos metadata 增加 `spotterOwner`、`sourceClusterID`、`schemaVersion`；旧条目没有 owner 时默认只读，不参加 prune/delete。
- **B1a（先落地，零新 etcd 协议）：** 对每个 scope 记录连续成功空读次数；至少连续 `N=3` 次完整 source read 成功、期间无 read error，才从 `EmptyCandidate` 进入 `EmptyConfirmed`。未确认前永远不 destructive prune；一次 error 立即回到 `ObservedNonEmpty`/`Unknown`。N 是配置项，生产默认不缩短安全窗口。
- **B1b（可选 owner 持久化）：** 在 B1a 通过后，才引入 `(namespace, group, provider, sourceClusterID)` 的 owner record/lease；实现必须复用现有 etcd client abstraction，先做 design+integration test，不把“新增 lease”作为 A 阶段前置。
- `GetAll` 读取 catalog 时只接受 owner marker 匹配的 host；不匹配/缺 marker 的 host 进入 `foreign` 计数和告警，不被 case-3 删除。
- 同一 Nacos namespace/group 中若仍使用 generic `k8s`/`ecs` clusterName，启动时必须输出 ownership warning；若无法证明独占，禁止启用 destructive prune。

旧条目没有 owner marker 时，兼容模式只允许读取和非破坏性对账；不得把“缺 owner”当作 spotter-owned。迁移工具必须先统计旧条目、写入 owner marker、再打开 destructive prune，并可按 scope 回滚到 read-only。

回滚闭包：任意 `foreign`、owner 冲突、read error、scope hash 变化或误删计数非零，立即按 scope 关闭 destructive prune、保留当前远端状态、停止该 scope 的批量删除，并输出 `owner_scope_hash`、`foreign_count`、`empty_confirm_count` 和 `rollback_reason`。回滚只能通过配置/状态开关完成，不通过删除 Nacos 数据完成。

作用域回滚触发和恢复规则：

| 触发条件 | 必须记录的三元组 | 立即动作 | 恢复条件 |
|---|---|---|---|
| `foreign_count>0` 或 owner mismatch | `scope,sink,owner_id` | destructive prune=off，保留队列和远端 | `foreign_count=0` 连续 3 次采样且 owner hash 稳定 |
| 任意 read error/HTTP 401/403/429/5xx | `scope,method,status/root_cause` | 视图标 `safe-fail`，不转空集合、不清队列 | read+write probe 连续 3 次成功 |
| `scope_hash` 变化 | `scope,old_hash,new_hash` | 暂停该 scope 的删除与 full retry | 新 hash 与 source snapshot 连续 3 次一致 |
| 误删计数或 unexpected delete >0 | `scope,composite_id,request_id` | 立即停用 destructive prune，保留现场 | 人工确认原因并完成 catalog 回放 |

`N=3` 是门禁常量，必须在 metrics 和 summary 中可见；恢复前不得自动重新打开 destructive prune。`ID-K8S-IDENTITY` 未完成并签字前，B1 的 owner/destructive 路径只能 dry-run。

### 8.3 Reconcile 场景表

| 场景 | 期望动作 | 必须验证 |
|---|---|---|
| local online / remote missing | register local | catalog 出现 owner marker |
| same ID field drift | local wins | metadata/status/reversion 被覆盖 |
| remote-only owned host | offline/deregister | composite id 正确，删除可重试 |
| remote-only foreign host | no delete | foreign metric + warning |
| provider read error | skip and retain state | 不误删、不全量重推 |
| one empty read | EmptyCandidate | 不删除 |
| N 次 confirmed empty | scope delete | 仅 owner scope 被清理 |

### 8.4 测试与验收

- [ ] owner mismatch 不删除测试。
- [ ] 1 次/2 次空源不删除，3 次成功空源才删除测试。
- [ ] 空源与 read error 交错时状态回退测试。
- [ ] K8s 和 Consul provider scope 互不删除测试。
- [ ] `--reconcile-source nacos` 与 `--nacos-addr` 缺失时 fail-fast 测试。
- [ ] 真实 Nacos scratch catalog/list 双视图验证。
- [ ] Nacos list/catalog 任一非“catalog not found”错误都产生 `OBSERR/safe-fail`，不能转换为空集合；记录 scope、HTTP method、status、latency。
- [ ] A1、A2、B1 每个提交都输出同一 `migration_scope_hash`；hash 不一致时 B1 只能 dry-run，不能开启 owner/destructive prune。

**完成标准：** Reconcile 对每种输入都能给出确定动作；无 owner 的远端数据不会被 Spotter 误删。提交 `B1`。

## 9. B2：Nacos HTTP 生产化（P1）

### 9.1 配置扩展

新增配置（默认值保持兼容）：

- `--nacos-namespace`，默认 `public`；
- `--nacos-group`，默认 `DEFAULT_GROUP`；
- `--nacos-username`、`--nacos-password`，默认空；密码只能来自受控 secret/env，不写日志；
- `--nacos-ca-file`、`--nacos-insecure-skip-verify=false`；
- `--nacos-readiness-probe=true|false`，生产默认 true；
- `--nacos-server-list` 可选多地址，和原 `--nacos-addr` 保持兼容；
- `--nacos-transport=sdk|http-compat`，默认 `sdk`；`http-compat` 仅限迁移回滚或显式测试；
- `--reconcile-source nacos` 的配置校验和 owner scope 配置。

### 9.2 Client 行为

- 把 endpoint、namespace、group、TLS、credential、timeout 放进不可变 `NacosClientConfig`。
- 实现 token provider：登录、过期前刷新、401 重试一次；401/403/404/409/429/5xx 分别分类。
- 所有 register/deregister/list/catalog/cluster 请求统一注入 namespace/group/auth；禁止保留散落的 `DefaultNamespaceID` 常量作为唯一路径。
- readiness 由 read probe + dedicated write probe 构成。write probe 使用 owner-scoped canary instance，成功后立即 deregister；清理失败必须告警，不得污染业务 scope。
- 迁移完成前仅保留当前 HTTP bounded transport 作为隔离的回滚/对照 adapter；禁止在业务包新增裸 `net/http` 调用。并发/连接/超时仍需配置化并暴露 metrics，但 B2 不能据此宣称 SDK 接入完成。
- `UpdateCluster` 失败需要可重试、可观测；不能只写 warning 后无限等下一次偶然 register。

错误分类决策树：

| 错误 | register/deregister | service/list/catalog read | readiness/write probe |
|---|---|---|---|
| 401/403 | permanent，停止该 task 并告警认证失败 | safe-fail，保留上一份视图 | gate FAIL |
| 404/409 | 记录 scope/参数；只有明确资源不存在时 permanent | catalog not found 仅在带完整 service+cluster selector 时按空 pair 处理，其余 safe-fail | gate FAIL |
| 429 | retry with backoff，不得 busy-loop | retry with backoff | gate FAIL |
| 500/503/timeout/EOF | retry，保留 operation type | `OBSERR/safe-fail`，禁止转空集合 | gate FAIL，直到 write probe 恢复 |
| 2xx + 非预期 body | protocol error，不能当成功 | protocol error，不能当空集合 | gate FAIL |

该决策树必须由 `pkg/nacos` unit/mock 测试逐格覆盖；leaderless 的 `500 Could not find leader` 不能被 readiness 的 200 结果掩盖。

Nacos 运行状态机与队列动作：

| 状态 | 进入条件 | queue 动作 | readiness/gate | 退出条件 |
|---|---|---|---|---|
| `ReadyWritable` | read probe 和 write probe 均成功 | 正常消费 | allow | 任意连续 write/read error |
| `ReadOnlySafeFail` | read 失败、401/403、非完整 catalog selector 404/409 | 保留未完成 operation，不产生 destructive prune | deny new destructive work | read+write probe 连续 3 次成功 |
| `Retrying` | 429、500/503、timeout、EOF | 保留原 operation，指数退避并限制最大并发 | deny readiness for new leader | 同一 operation 成功或达到人工升级阈值 |
| `PermanentRejected` | 明确可归因的 4xx task | 只删除该 task；batch 级未知 4xx 不自动删除 | deny affected scope | 人工确认或配置修复后重新入队 |
| `ProtocolError` | 2xx 但 body/codec 不符合契约 | 保留 operation，停止该 endpoint | deny | 协议版本/codec 修复并重新探针 |

每次状态变化必须输出 `scope,sink,state,root_cause,queue_depth,next_retry_at`；`ReadOnlySafeFail` 和 `Retrying` 都不能被计为 CONSISTENT 或 PASS。

### 9.3 测试与真实验收

- [ ] unit：namespace/group/auth/TLS/timeout/错误分类。
- [ ] mock：401 token refresh、403 permanent、429 retry、leaderless write 500。
- [ ] e2e：readiness 200 但 write 500 必须失败启动 gate。
- [ ] scratch Nacos 2.1：persistent register、unhealthy disabled、catalog prune、重启恢复、TLS/auth（若环境支持）。
- [ ] 新增 `tests/e2e/nacos_real_test.go`（`//go:build nacos_real`）；真实环境命令固定为 `go test -tags=nacos_real ./tests/e2e/... -run TestNacosReal -count=1`，在该文件落地前不得声称真实 Nacos 已验证。
- [ ] 记录 Nacos 版本、镜像 digest、配置 hash、请求成功率、p50/p99、retry depth、最终 catalog hash。

**完成标准：** HTTP 过渡 adapter 的目标版本和目标配置具备生产证据，但 Nacos 生产路径仍保持 PARTIAL，直到 B3 的官方 SDK 迁移、完整回放和静态门禁全部通过。提交 `B2`。

## 10. B3：官方 Nacos Go SDK 迁移与协议兼容性验证（P1；Nacos 启用时为 release blocker）

### 10.1 已确认事实

官方 Go SDK v2 支持 naming gRPC proxy；`BatchRegisterInstance` 走 gRPC；SDK 根据 `Ephemeral` 选择 persistent HTTP 或 ephemeral gRPC。当前仓库使用 persistent instance，因此直接引入 SDK 不会自动把现有单实例 register 变成 gRPC。SDK 能力存在不等于本仓库已经合规：当前 `pkg/nacos` 仍直接调用裸 HTTP，必须完成统一 SDK facade 和迁移门禁。

**执行状态（2026-09-13，B3 SDK seam）：** 工作树已接入 `github.com/nacos-group/nacos-sdk-go/v2 v2.3.5`，新增 `TransportMode` 和 `sdkNamingFacade`。生产 server wiring 默认选择 `sdk`；`http-compat` 只允许显式测试/回滚。persistent register/deregister、SelectAll（含 disabled）、service list、subscribe/unsubscribe 通过官方 naming SDK；SDK 自动配置 gRPC 端口（server port + 1000）、namespace/group、username/password、TLS 和多 server list。由于该 SDK 未提供 catalog/prune、cluster Admin 和 console readiness 等价接口，这些能力暂时集中在显式、可审计的 HTTP compatibility adapter，不再散落在业务层；该例外仍需真实目标版本 Admin/Maintainer SDK 评估和 B3-G 到期决策。静态 access token 在 SDK 模式下 fail-closed（SDK v2.3.5 没有等价静态 token 配置），避免“配置看似生效但实际未认证”。

**B3 发布判定：** SDK seam 与离线测试已通过，但生产门禁仍为 **NOT VERIFIED / REMAINING**。`go test -tags=nacos_sdk_eval ...` 与 `go test -tags=nacos_real ...` 在未提供 `NACOS_SERVER` 时只会 SKIP；必须在 scratch/pre-production Nacos 2.x 上补齐 query/list、subscribe、batch（persistent 明确不支持时保留 per-instance 证据）、catalog/prune、namespace/group、TLS/auth、重连/重启、错误恢复和最终集合 hash，才能关闭 `ID-NACOS-SDK-MANDATE`。

本工作包的不可变约束：

1. 生产 Nacos naming 操作（register、deregister、list、query、subscribe、batch）必须通过官方 `nacos-sdk-go/v2` 或其薄 facade。
2. Admin/Catalog/prune/readiness 等非 naming 操作必须优先使用官方 Admin/Maintainer SDK；若目标版本没有等价 SDK 接口，必须建立单独、版本化、可审计的 adapter，并登记 `ID-NACOS-SDK-MANDATE` 例外、负责人、到期时间和删除条件。散落在业务代码中的裸 HTTP 永久禁止。
3. HTTP adapter 仅允许作为迁移期回滚/对照通道；在没有 SDK 兼容证据和完整测试前，不得把 Nacos Sink 标记为 production PASS。

### 10.2 实施顺序

1. 新增 `NacosTransport` 内部接口和 `NacosSDKTransport` facade；接口按 operation type 区分 naming、Admin/Catalog、readiness，禁止业务层自行拼接 URL。
2. 新增 `tests/e2e/nacos_sdk_eval_test.go`（`//go:build nacos_sdk_eval`），引入 `nacos-sdk-go/v2`，实现 SDK adapter 及可注入 fake，先不改变默认 wiring。
3. 对同一个 scratch Nacos 做 HTTP vs SDK 对照：register/deregister、service list、query instances、subscribe、batch register、metadata、enabled、namespace/group、TLS/auth、错误/重连和最终 catalog 集合。
4. 单独验证 SDK batch gRPC 与目标 Nacos 版本的 request type、persistent/ephemeral 生命周期、心跳、leaderless、重启恢复和错误码兼容；不能以 SDK 编译成功作为协议通过。
5. 验证 catalog/prune/readiness 是否有官方 Admin/Maintainer SDK 等价能力；没有覆盖的接口必须形成带期限的例外，不得偷偷保留散落 HTTP。
6. 新增 `--nacos-transport=sdk|http-compat`，默认 `sdk`；`http-compat` 只允许显式开启并输出 `NON_PRODUCTION_COMPAT`，支持即时回退但不能成为发布默认。
7. 增加静态门禁：生产 `pkg/nacos` 不得新增 `net/http`、Nacos URL、HTTP method/query 拼接；所有调用必须通过已登记 adapter。

### 10.3 退出条件

- `make test-all` 和 HTTP compatibility 回归保持通过；SDK adapter 有独立单测和真实 scratch 证据。
- persistent 生命周期、redo/cache、leaderless、auth、TLS、namespace/group、catalog ownership、错误重试和重连全部通过；
- 每一个 Nacos operation type 都有 SDK/facade 覆盖证明；任何未分类裸 HTTP 调用都阻断发布。
- 目标 Nacos 版本的 SDK gRPC 与 Admin/Catalog 兼容性矩阵完整；例外项有 owner、expiry 和 rollback。
- SDK 默认路径灰度指标达标后，才允许删除 `http-compat`；未达标则保留回滚但状态仍为 NOT DONE。

提交 `B3`（SDK adapter/测试）和 `B3-G`（SDK 默认灰度与 HTTP fallback 下线决策）分开。SDK 评估命令固定为 `go test -tags=nacos_sdk_eval ./tests/e2e/... -run TestNacosSDK -count=1`；该 tag 未创建前不得声称 SDK 测试已执行。

### 10.4 SDK 强制接入测试集（完整清单）

以下测试集是 B3 的必要组成，不允许只验证“能连上 gRPC”或“SDK 能编译”：

- **Facade/静态门禁：** fake SDK 记录每个 operation type；register、deregister、service list、query instances、subscribe、batch、catalog、prune、readiness 均必须命中登记的 facade；`rg` 门禁阻断生产包新增 `net/http`、Nacos URL、HTTP method/query 拼接。
- **生命周期：** `ephemeral=false` 的 persistent register/update/delete；`ephemeral=true` 的 heartbeat、过期和进程重启；persistent 与 ephemeral 不能混淆；delete/recreate 和旧 revision 乱序必须保持最终状态正确。
- **协议与字段：** namespace、group、service、cluster、ip、port、weight、enabled、healthy、metadata、instanceId、sourceKey、reversion 的 round-trip；空值、默认值和未知字段均要有断言。
- **批量语义：** batch register 成功、部分失败、重复、超限、SDK 对 persistent batch 的拒绝路径；不能把 batch 失败降级成丢失 operation type 的单实例 Push。
- **发现语义：** service list、query instances、disabled 实例可见性、subscribe 初始快照/增量事件、catalog 与 naming view 双视图差异；read error 不能转换为空集合。
- **可靠性：** gRPC 端口错误、连接断开、server 重启、leaderless、EOF、timeout、429/5xx、token 过期、401/403、TLS CA/hostname 错误；验证 SDK redo/cache 与 Spotter retry 不重复、不丢失、不乱序。
- **一致性闭环：** K8s Watch → cache → diff → SDK facade → Nacos 的 add/update/delete/foreign/empty/error；`PushAll` 的 register、catalog list、prune、deregister 各阶段失败后仍保留原 operation type 并可重放；burst delete 不得出现 register-after-deregister ghost。
- **真实环境：** 目标 Nacos 版本 scratch/pre-prod 回放 persistent register、unhealthy disabled、catalog prune、TLS/auth/namespace/group、重启恢复和最终 catalog hash；记录版本、镜像 digest、SDK 版本、gRPC port、请求成功率、p50/p99、retry depth 和 cleanup 结果。

固定命令：

```bash
go test -race ./pkg/nacos ./pkg/worker -count=1
go test -tags=nacos_sdk_eval -race ./tests/e2e/... -run TestNacosSDK -count=1
go test -tags=nacos_real -race ./tests/e2e/... -run 'TestNacosReal|TestNacosSDKReal' -count=1
```

上述 tag 对应测试文件未创建、环境不可达或只运行 mock 时，SDK 接入状态必须保持 `NOT VERIFIED`，不得标记为完成。

## 11. B4：Atlas 真实 codec/protobuf 验证（P1）

**目标：** 证明生产 Atlas 接受当前 wire，或明确切换到真实 protobuf 生成代码。

**步骤：**

1. 取得目标 Atlas 服务版本、proto 文件、TLS/认证和 RPC method contract。
2. 新增 `tests/e2e/atlas_real_test.go`（`//go:build atlas_real`），用真实或预发 Atlas 做 `Dial → SynInstance → SynAllInstance → GetAllInstance` 报文 round-trip；记录 JSON codec 是否被接受、返回 code/msg、字段缺失和大小限制。
3. 若服务端只接受 protobuf，生成真实 proto client，并在 `pkg/discoverycenter` 中保留兼容 adapter；默认切换必须有 feature flag。
4. discoverymock 继续保留 JSON 测试，但不得把它标成生产兼容证明。
5. 将 codec、method path、TLS、返回码、最大 payload 写入一份可版本化兼容矩阵。

**验收：** 真实 Atlas 证据通过才可把该项标 PASS；真实验证命令固定为 `go test -tags=atlas_real ./tests/e2e/... -run TestAtlasReal -count=1`；该文件/标签未创建或 endpoint 不可用时，文档和启动日志都保持 NOT VERIFIED。提交 `B4`。

## 12. C1：补齐测试、边界和 E2E 门禁（P0/P1）

**执行状态（2026-09-13）：** Observe harness 已完成本地 deterministic 修复：`logSlice` 解析 zap JSON `ts`（字符串、数值纳秒精度）及带前缀/普通行首时间戳；churn ledger 在 apply/delete API 调用前发布 mutation clock，避免 tick 先看到 source/remote 变化而没有 ledger。对应 `tests/observe` parser/engine tests 与 `-tags=observe` 编译门禁通过。完整 OBS-mini/OBS-full、kwok/Nacos/Atlas/etcd 自包含运行尚未执行，Consul 规模观察继续为 accepted non-goal。

### 12.1 新增负向测试矩阵

| 组件 | 必测边界 |
|---|---|
| Robot | 多 cluster 同 key、UID 缺失、delete tombstone、队列满、recovery 超限、Stop 期间 Pop、HasSynced 永不完成 |
| K8s conversion | appcodes 多值命中第二项、空 IP online/unhealthy/offline、空 ContainerStatuses、CrashLoop、Succeeded/Failed、ResourceVersion 异常 |
| Cache/diff | same identity revision 乱序、同名不同 cluster、snapshot stale、offline equality、duplicate identity |
| Worker/Fanout | 同实例并发顺序、Push/PushAll 混合失败、FanoutError nested permanent、PushTo/PushAllTo、unknown sink |
| Retry | full operation retention、mid-cycle re-add、ghost sink、permanent 4xx、prune-only 5xx、queue depth per operation |
| Nacos | list vs catalog disabled split、namespace/group/owner scope、catalog not found、pagination clamp、readiness write failure、SDK facade operation coverage、裸 HTTP 静态门禁 |
| Nacos SDK | register/deregister/query/subscribe/batch 的 SDK 路由、persistent/ephemeral 生命周期、gRPC reconnect/redo/cache、auth/TLS/namespace/group、Admin/Catalog facade、HTTP compatibility 回滚和最终集合一致性 |
| Reconcile | add/update/delete/foreign/empty/error、Nacos authoritative 与 Atlas default 两种模式 |
| Observe | zap JSON ts、行首 timestamp、apply failure rollback、ledger-before-apply、source read OBSERR、duplicate max-count union |

### 12.2 E2E 分层

**E2E-L（离线，必须每个 PR）：**

- `make test-e2e`（Makefile 的 canonical wrapper，必须等价执行 `go test -race -count=1 -tags=e2e ./tests/e2e/...`；CI 需检查 recipe 未丢失 `-tags=e2e`）；
- Nacos mock + real Sink + real Fanout + real Worker；
- 新增 full-prune-failure retry、same-instance out-of-order、multi-cluster identity 回放。

**E2E-R（真实 scratch，nightly/manual）：**

- `tests/e2e/nacos_real_test.go`（`nacos_real` tag；由 B2 创建）；
- 动态 Nacos 端口、TLS/auth/namespace/group、leaderless/write probe、catalog/list；
- 保存 summary 和 hash，不提交原始全量日志。

mock 与真实差异必须显式登记：

| 能力 | mock 证据 | 真实证据要求 | 不能互相替代的原因 |
|---|---|---|---|
| K8s informer/watch | fake Robot、unit/e2e 回放 | 临时 kube-apiserver/kwok watch | fake 不证明 resync、ListAndWatch、队列节流和跨 cluster store |
| Nacos list/catalog | nacosmock 双视图和错误注入 | Nacos 2.1 scratch 的实际 HTTP response/disabled visibility | mock 不证明 Raft、权限、分页实现和 leaderless 行为 |
| Nacos auth/TLS/namespace | mock 参数断言 | 真实证书、token、namespace 的 register/list/prune | mock 只能证明参数被发送 |
| Atlas codec | discoverymock JSON codec | 真实 Atlas method path、protobuf/JSON codec、TLS、返回码 | JSON bufconn 不证明生产服务端 codec |
| SDK gRPC | SDK adapter 编译/unit、operation coverage 和裸 HTTP 静态门禁 | Nacos 2.1 目标版本的 grpc port、batch/register/deregister/query/subscribe 回放 | SDK 编译成功不证明 server request type、persistent 生命周期或 reconnect 兼容 |
| SDK Admin/Catalog | facade contract、例外清单和回滚测试 | 目标版本 Admin/Maintainer SDK 的 catalog/prune/readiness 回放；若无官方覆盖，必须有带期限的例外证据 | naming SDK 的 service view 不能自动替代 catalog/admin 视图 |

机器可读判定规则：

- `mock-pass`：仅 mock/unit/e2e-L 通过；只能放行代码回归，不得宣称协议、TLS、鉴权或生产可写。
- `real-pass`：对应能力的真实 scratch/pre-prod 测试通过，且记录版本、digest、endpoint、codec、TLS/auth、status、latency 和最终 catalog hash；必须同时满足 mock-pass。
- `env-error`：依赖缺失、端口冲突、Nacos/kwok/Atlas 不可达；运行判定为 VOID/EnvError，不计产品 PASS/FAIL，但必须保存 cleanup 结果。
- `test-fail`：依赖正常且断言失败；计入对应 P0/P1/P2，不得由重跑自动降级为 mock-pass。

K8s fake Robot、Nacos mock、discoverymock JSON codec 和 SDK 编译结果默认只能产生 `mock-pass`；没有真实证据时，计划状态保持 `NOT VERIFIED`。

**OBS（observe 观察）：**

- **OBS-mini（当前可运行形态）：** 明确要求 `OBS_KUBECONFIG` 指向已存在且专用的 kwok cluster；只跑短窗口 smoke，不宣称自包含；
- **OBS-full（上线门禁形态）：** 新增 `scripts/observe-up.sh` 创建临时 kwok cluster、Nacos、kubeconfig 和 scratch ports；新增 `scripts/observe-down.sh` 无条件清理 child、pods、kwok、Nacos；在这两个脚本落地前，`make test-observe` 不能作为自包含验收；
- 启动前做 docker/kubectl/kwok/image/CPU/memory/port preflight；
- 每 tick 写 source counts、remote list/catalog counts、retry/drop、environment class、verdict；
- EnvError/InfraError 不能伪装成产品 FAIL；但所有 OBSERR 必须计数，不能被当作 PASS。

Make target 契约：`make test-observe` 必须等价执行 `go test -tags=observe -run '^TestObserveConsistency$$|^TestObserveUnit' ...`，但 OBS-mini/OBS-full 的外部前置条件仍按本节区分；直接 `go test` 命令若未带对应 build tag，一律不计入 E2E/OBS 门禁。

### 12.3 E2E 启动硬门禁

启动前必须全部通过：

1. 二进制构建和版本 hash；
2. 只使用 loopback/scratch ports；
3. Nacos image、Docker、kwokctl、kubectl 可用；
4. kubeconfig 指向临时 cluster，不修改默认 `~/.kube/config`；
5. Atlas stand-in 可 dial，Nacos read+write probe 通过；
6. `--nacos-addr` 与 `--reconcile-source` 配置一致；
7. 日志目录、结果目录可写且不在源码脏区；
8. timeout 足以覆盖 etcd/Nacos 启动，但测试结束必须回收所有子进程/容器。

失败终结与清理要求：每个外部步骤使用唯一 `FAIL-<step>` code；任一前置失败立即停止后续 destructive 操作，进入 `EnvError`/`InfraError`，并在 defer 中清理 child、容器、pods、临时 kubeconfig、listener 和端口文件。summary 必须包含 `cleanup_status`、`residual_pods`、`residual_containers`、`residual_listeners`；任一残留未知时 OBS-full 不能 PASS。恢复重跑前必须证明上一次 cleanup 完整。

**完成标准：** E2E-L 可在干净机器上立即运行；E2E-R/OBS 的每个外部前置失败有明确分类、证据和 cleanup。提交 `C1`。C1 的 DoD 必须分别标出 OBS-mini 和 OBS-full，不能用已有 `dsca1` 共享 cluster 的结果替代 full 自包含证据。E2E-R 的 mock 通过只能标记 `mock-pass`，不能提升为 `real-pass`。

## 13. C2：通知、DDD globals 与生命周期收口（P1/P2）

### 13.1 通知

- 把 providers/election/metrics 的 `notice.Notice` 全部改成 `ports.Notifier` 注入；保留 `pkg/notice` 兼容 shim 但生产路径不读取全局。
- appcenter notifier 增加真实 HTTP/API adapter、超时、重试、认证和失败计数；日志只作为 fallback，不能宣称已告警。
- 每个严重事件至少有通知单测、发送失败测试和真实 scratch endpoint 测试。

### 13.2 DDD

- `pkg/log.Logger`、`pkg/notice.Noticer`、`config.*` 只允许出现在 composition/infra compatibility layer；用 `rg` 做 CI 门禁。
- 增加 `compatibility_mode`（默认 ON）和 rollback checklist；每次移除 shim 前，先通过 `rg` 引用门禁、CLI/env preset 回归和启动/停止回放。
- providers 使用 `ports.Logger/Notifier/Clock/InstanceSource`；所有 sleep/ticker 通过可取消 context/clock。
- 删除或隔离 `pkg/providers/aggregate/controller.go`，明确不再作为生产 composition point。
- 保持 CLI flags、env presets、metrics names 的兼容测试。

### 13.3 验收

- nil logger/notifier 不 panic；
- leader loss/provider stop 在 gate/sleep 中可取消；
- `make test-all`、`go test -race ./...`（或明确 allowlist）通过；
- legacy global 引用只剩兼容层，并在文档中列出计划删除版本。

提交 `C2`。

## 14. C3：vet 清零、文档和最终发布门禁

### 14.1 低风险修复

- `tools/cache/cache.go:52`：改为带格式占位符的日志，或按 dead-code 决策删除该包及唯一 dead consumer；不得只是从 allowlist 排除。
- `pkg/providers/k8s/k8s.go:68`：改为 keyed `k8srobot.RN{Resource: k8srobot.Pods, Namespace: ""}`。

### 14.2 最终命令矩阵

```bash
go vet ./...
go test ./... -count=1
go test -race ./... -count=1
make test-all
go test -tags=observe -run '^TestObserveUnit' ./tests/observe/... -count=1
go test -race -tags=e2e ./tests/e2e/... -count=1
```

真实 scratch 门禁（不放入每个 PR）：

```bash
go test -tags=nacos_real ./tests/e2e/... -count=1
OBS_DURATION=30m OBS_SCALE=100 make test-observe
```

### 14.3 文档同步

更新 `docs/README.md`、`architecture.md`、`operations.md`、`testing.md`：

- Go 1.25 / etcd v3.6.13；
- 当前 Nacos flags、SDK/HTTP compatibility 状态；明确“生产必须经 SDK，HTTP 仅为迁移回滚通道”；
- 当前两条 vet 诊断（修复后应为 0）；
- `make test-e2e`、`make test-observe` 的真实前置条件；
- accepted non-goal：Consul 规模观察；
- 生产 Atlas codec、appcenter 通知和 Nacos auth/namespace 的证据边界。

## 15. 每个批次的通用 Definition of Done

批次只有在以下项目全部满足时才可合并：

1. 生产代码、测试、文档和迁移/回滚说明在同一提交中可理解；
2. 新增测试在旧实现上能证明缺陷，修复后通过；
3. `-race` 通过，未引入 goroutine/ticker/context 泄漏；
4. 相关 mock 不比真实系统更宽松；
5. 至少一个真实 scratch 或兼容性证据（P2 文档项可明确标记未验证）；
6. 指标、日志和错误分类可回答“发生了什么、影响哪个 sink/scope、是否恢复”；
7. 运行树、容器、临时 kubeconfig、日志目录清理完成；
8. reviewer 明确回复 **PASS，且没有 P0/P1/P2 remaining**；
9. 记录 commit、测试命令、关键输出、artifact hash 和回滚方法。

## 16. 风险与决策记录模板

每个需要产品或生产环境决策的项必须写下：

```text
Decision ID:
Owner:
Scope:
Chosen option:
Compatibility impact:
Evidence required:
Rollback:
Expiry/revisit trigger:
```

至少需要建立以下 Decision ID：

- `ID-K8S-IDENTITY`：是否只新增内部 SourceKey，或允许改变外部/Nacos cluster identity；
- `ID-NACOS-OWNER`：`k8s/ecs` cluster 是否由 Spotter 独占；
- `ID-NACOS-GRPC`：SDK batch gRPC 是否在目标 Nacos 版本可用；
- `ID-NACOS-SDK-MANDATE`：官方 SDK/facade 对每个 Nacos operation type 的覆盖、HTTP compatibility 例外 owner/expiry/删除条件；
- `ID-ATLAS-CODEC`：真实 Atlas 接受 JSON 还是必须 protobuf；
- `ID-NOTICE-DELIVERY`：appcenter 真实告警 endpoint、认证和 SLA；
- `ID-CONSUL-SCALE`：重新启用 ECS/机器部署时打开 Consul 规模观察。

**计划结论：** 先执行 A0→A1→A2→A3，关闭身份、乱序和全量重试三项一致性风险；再执行 B1/B2 明确 Nacos 生产边界；B3 官方 SDK 迁移是 Nacos 启用时的强制发布门禁，B4 负责 Atlas 协议能力验证；最后用 C1/C2/C3 把测试、E2E、DDD、通知和静态质量收口。任何阶段都不能用当前 2h K8s 观察结果替代未验证的 Nacos/Atlas/Consul 生产结论。

## 17. 最终 reviewer 结论

**Reviewer：** `final_plan_reviewer_v3`  
**结论：** **PASS**  
**复核范围：** v6 全文、A2/A3 接口与迁移/回滚、B1/B2/B3 状态机、C1 mock/real 与 E2E/OBS 门禁、版本追踪。  
**复核结果：** 未发现遗留 P0、P1 或 P2 阻塞项。Reviewer 特别确认：

- `make test-e2e` 与 `make test-observe` 的 build-tag 契约已明确，未带 tag 的直接命令不计入门禁；
- A3 的双写 shadow window、`operation_*` 计数、replay hash、quiescent-point 切换和 `retry_migration_blocked` 回滚条件可执行；
- A2/A3 的 `Event`、`RetryOperation`、`EventQueue`、`PushTo/PushAllTo` 责任和兼容行为已在计划内闭合；
- B1 的 owner/空源回滚触发与恢复条件、B2 的 Nacos 错误状态机、C1 的 mock-pass/real-pass/cleanup 判定已具备机器可判定的验收规则。
- B3 已将官方 Nacos SDK/facade 设为强制生产路径，`http-compat` 仅限迁移期回滚；operation coverage、persistent/ephemeral、gRPC/Admin/Catalog、重连和最终一致性测试均列为发布门禁。

该文档现在是后续实现批次的主计划；实现过程中若新增风险，必须先更新本计划和对应 Decision ID，再进入代码变更。
