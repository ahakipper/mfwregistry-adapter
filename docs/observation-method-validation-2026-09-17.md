# K8s → Spotter → Nacos 观察方法修正与验证报告

日期：2026-09-17

## 结论

旧的两小时观察已经停止，且不得作为 PASS。旧方法能够周期性读取 K8s、Spotter 和 Nacos 的完整快照，但没有在整个窗口持续关联三路事件；原 Spotter event 还位于 informer 入队之前，本质上只是第二个 Kubernetes 观察者，不代表 Spotter 内部 cache 已处理数据。

独立开发审计和评审均判定旧阶段延迟证据为 **FAIL（方法不成立）**，但没有据此判定产品数据已经不一致。完整 Instance 快照比较本身仍然有效，因为 canonical payload 覆盖 labels、Reversion、SourceKey、SourceCluster、端点、生命周期及其他 domain 字段。

当前已完成测试方法修正，并通过单 Pod 的真实 Nacos 3 ARM64 + KWOK 诊断。全规模阶梯和新的两小时观察仍需使用修正后的方法重新执行，因此本文当前状态为：

**METHOD VALIDATED / FULL GATES PENDING**

## 为什么旧结果不能继续使用

| 问题 | 旧行为 | 风险 | 修正 |
| --- | --- | --- | --- |
| Spotter 中间面 | informer callback 在 queue admission 之前记录 | queue 丢弃、coalescing、provider 未输出也可能被误报为 Spotter 已处理 | 在统一 `worker.Handle` 出口前记录，并用 origin 区分增量 cache、SyncAll 与 reconcile |
| Spotter snapshot | `provider.GetAll()` 重读 informer store | 没有证明 internal cache 状态 | 读取 active provider cache，并返回 cache generation |
| 事件关联 | 只按 pod name + present/delete | Crash、Recovery 或重复 callback 可能误配 | Create/Crash/Recovery 按 Reversion + Status + canonical payload；Delete 按 UID/SourceKey + offline + Nacos snapshot removal |
| Nacos 删除 | 只有服务完全为空才算删除 | 服务仍有其他实例时漏掉单实例删除 | 对每个 service 的 Subscribe 完整快照做前后差分 |
| Nacos callback 时间 | clone 大 hosts 列表后取时间 | 大规模时系统性放大/扰动时间点 | callback 入口立即取时间 |
| strict convergence 时间 | Nacos fresh read 前取 `now` | 系统性低估可见时间 | read 完成并通过 stable-cut 复核后取完成时间 |
| 快照一致性 | K8s、Spotter、Nacos 串行读一次 | 读窗口发生变化时可能比较不同逻辑状态 | K8s fingerprint 与 Spotter cache generation/event sequence 前后不变才接受 |
| 高规模百分位 | 500/1000 Pod 仅 3 个 batch 样本仍计算 P90/P95/P99 | nearest-rank 三个百分位全部退化成 max | 每实例事件样本计算 P90/P95/P99；少量 batch 只报告 min/median/max |
| Watch 健康 | channel 关闭可能静默 | Watch 已停止但报告仍可能 PASS | sequence hole/reset、ring gap、错误和提前关闭全部 fail-closed |

## 修正后的三个时间边界

1. **K8s Watch**：独立 client-go List+Watch，记录外部观察者看到目标 Pod revision/status 的时间。
2. **Spotter provider-output/pre-worker**：所有 K8s provider 的 worker 出口统一记录，覆盖增量、启动/周期 SyncAll 和 reconcile；`origin=event-cache-applied` 才明确表示增量 cache 路径，其他 origin 不冒充相同语义。
3. **Nacos Subscribe**：官方 Nacos Go SDK callback；Create/Crash/Recovery 必须能解出与 Spotter 完全相同的 canonical payload，Delete 必须出现在该 service 的完整快照差分中。

Spotter event 同时携带 provider trigger time、pre-worker observed time、operation 和 origin，因此 `Spotter provider trigger → pre-worker` 是同一进程内的因果耗时；报告同时保留实际 origin。独立 K8s Watch 与 Spotter informer 是并行观察者，二者的到达时间差不再被描述成因果处理时延。

## 一致性裁判

Watch 用于事件完整性和延迟，周期性 snapshot 用于数据正确性；两者同时通过才算 PASS。

每次 strict snapshot 使用以下稳定窗口：

```text
K8s snapshot A
  → Spotter internal-cache snapshot A
  → fresh Nacos SDK catalog
  → K8s snapshot B
  → Spotter internal-cache snapshot B
```

只有 K8s canonical fingerprint、Spotter cache generation、Spotter event sequence 和 cache fingerprint 前后均未变化，才接受这次比较。接受后继续要求：

- K8s → Spotter：完整 canonical Instance 相等；
- K8s → Nacos：端点、scope、identity、lifecycle、owned metadata 和压缩 canonical Instance 相等；
- labels 与 Reversion 必须相等；
- retry queue 最终 drain；
- `events_dropped_total = 0`；
- 无 Watch gap/reset/提前关闭/缺失 mutation。

Nacos 服务端自行维护的 wire `Healthy` 位只记录、不作为 Spotter domain equality；Status=2 的实例仍使用已定义的 Nacos 3 映射（transport enabled/healthy=false，canonical 保留 domain Enabled=false/Status=2）。Nacos 额外添加的非 Spotter metadata 也不属于 domain Instance 字节级相等范围。

## 测试工具自证

单元与 race 测试覆盖：

- 错误 Reversion 或 canonical payload 不能满足事件关联；
- 非空 service 删除一个实例能够从 Nacos snapshot 差分识别；
- Delete 的 K8s 新 ResourceVersion 与 Spotter cached tombstone Reversion 不同，但 UID/SourceKey 相同，能够准确关联；
- 三路 channel 提前关闭会失败；
- 100 个 per-instance 样本的 P90/P95/P99 与 4 个偶数 batch 的 min/median/max 分开计算；
- 增量 observer 以 `event-cache-applied` origin 在 worker 尚未调用时执行；其他输出使用独立 origin；
- SyncAll 与增量路径使用相同 worker-boundary observer。

执行结果：

- `go test -tags=observe ./tests/observe -run TestObserveUnit -count=1`：PASS
- `go test -race -tags=observe ./tests/observe -run 'TestObserveUnitWatchTimeline|TestObserveUnitScaleLadder' -count=1`：PASS
- `go test ./pkg/providers/k8s -run TestInstanceEventObserver -count=1`：PASS

## 真实单 Pod 诊断

原始报告：`tests/observe/results/20260917-223949-scale-ladder-summary.json`

目标：Nacos 3 ARM64 + KWOK；Create/Delete 各 1 次，并覆盖 CrashLoopBackOff 和 Recovery。

| 场景 | API→K8s Watch | Spotter provider trigger→pre-worker | Spotter→Nacos Subscribe | API→Nacos Subscribe | Stable-cut strict equality |
| --- | ---: | ---: | ---: | ---: | ---: |
| Create | 87.381 ms | 0.072 ms | 516.638 ms | 604.094 ms | 1.727 s |
| Delete | 44.210 ms | 0.099 ms | 584.416 ms | 621.237 ms | 1.668 s |
| Crash | 69.544 ms | 0.119 ms | 562.673 ms | 634.797 ms | 1.739 s |
| Recovery | 47.519 ms | 0.143 ms | 565.594 ms | 613.882 ms | 1.702 s |

诊断结论：**PASS**。零 watcher error；Create/Crash/Recovery 使用 revision-status-canonical，Delete 使用 uid-sourcekey-offline-removal；四类场景均完成 stable-cut 全字段一致性。实际 Spotter origin 均为 `event-cache-applied`。由于每个场景只有 1 个样本，这些数值只是测试方法诊断，不能解释为正式 P90/P95/P99。

随后完成了最终源码版本的短 sustained-observe 工具验证：`tests/observe/results/20260918-2202-summary.json`。窗口为 20 Pods / 2 应用 / 1 分钟，13/13 stable snapshots exact，2/2 Nacos subscriptions ready，66 个 K8s Watch 事件、62 个 Spotter 事件、5 个 Nacos 事件，2/2 mutation 关联，0 missing、0 watcher error、0 divergence、0 dropped event，retry queue drained。runner 只运行一次并以 `EXIT_CODE=0` 结束，launchd label 已移除。该窗口验证的是两小时测试工具，不是两小时可靠性结论。summary SHA256 为 `8da23a2931b8c875c24b30c2251395da6b004a46353138f174fd48f9db0961e6`，events SHA256 为 `c3c9da5fa8a4c5a5ca7fdcae9cc184e0c8c6646dbca084eaa49bd99bc7485948`，ticks SHA256 为 `ac1c7d3c6ba148fa4299a1949d7b7c053ebe1e8e703ee880f369486c6a7c5bdf`。

## 下一步正式门禁

1. 使用修正后的方法重跑 1/10/100/500/1000 Pod Create/Delete 全规模阶梯。
2. 用 per-instance 样本输出各规模 P90/P95/P99，同时单列 batch min/median/max。
3. 全规模 PASS 后，将同一三路 Watch 和 mutation journal 接入两小时、1000 Pod、20 应用、持续 churn 观察。
4. 两小时内周期性执行 stable-cut 全字段 snapshot；结束后要求零缺失 mutation、零稳定快照不一致、零 drop、队列 drain。
5. 独立评审 Agent 复核代码、原始 JSON、清理状态和最终报告后，才给出正式总体结论。

## 当前不在本轮范围

- AppCenter 真实告警接入；
- Nacos 部署级 TLS、认证、非 public namespace、HA、leaderless；
- Atlas 真实 protobuf wire compatibility。
