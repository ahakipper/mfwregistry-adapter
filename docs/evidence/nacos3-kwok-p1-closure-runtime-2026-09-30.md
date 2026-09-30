# Nacos 3 + KWOK P1 Closure Runtime Evidence — 2026-09-30

## Verdict

**PASS for the local data-plane qualification.** The post-remediation run used
Nacos 3.2.4 ARM64, KWOK, the Spotter child, and continuous K8s/Spotter/Nacos
watchers. Every tick compared the complete Instance projection, including
labels and Reversion. The final HEAD also passed a fresh one-minute smoke after
the mixed-failure retry fix.

## 20-minute / 1,000-Pod run

```text
Command: OBS_DURATION=20m OBS_SCALE=1000 OBS_NODE_POD_CAPACITY=1200 \
         OBS_SERVICES=20 OBS_BURSTS=true OBS_CRASH_CYCLES=true \
         OBS_TIMEOUT=45m make test-observe
Target:  nacos/nacos-server:v3.2.4-slim, linux/arm64
Vehicle: owned KWOK cluster dsca-observe-8031, client-go Watch, Nacos 3 SDK/gRPC
```

| Check | Result |
| --- | ---: |
| Window / scale | 20m / 1,000 Pods / 20 services |
| Exact consistency | 79/79 ticks; 0 divergent; 0 OBSERR |
| Spotter projection | 79/79 exact; 0 mismatches |
| Mutation correlation | 3,268/3,268; missing 0; watcher errors 0 |
| Queue / drops | retry depth 0; robot depth 0; dropped 0; drained true |
| Final three-plane snapshot | exact |
| Burst / crash / recovery | 100-Pod storm, three 200-Pod bursts, crash and recovery passed |
| Cleanup | PASS; KWOK and Nacos resources removed |

Latency from **K8s Watch → Nacos Watch**:

| Operation | P50 | P90 | P95 | P99 | Samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| Create | 0.572s | 0.606s | 0.613s | 0.639s | 1,633 |
| Delete | 0.549s | 0.593s | 0.599s | 0.617s | 1,633 |
| Crash | 0.529s | 0.529s | 0.529s | 0.529s | 1 |
| Recovery | 0.570s | 0.570s | 0.570s | 0.570s | 1 |

The API→Nacos numbers are auxiliary only and include KWOK client throttling;
they are not the Spotter data-plane boundary.

## Final-HEAD smoke

The final HEAD (`0ba6610`) was rebuilt and passed a fresh one-minute Nacos 3
ARM64 + KWOK smoke at scale 10: 7/7 exact ticks, 2/2 mutation correlations,
zero divergence/errors/drops, exact final snapshot, and `residual_unknown=false`
for both Pod and Nacos cleanup.

## Artifacts and hashes

### 20-minute qualification

- [Summary Markdown](../../tests/observe/results/20260930-1241-summary.md) — SHA256 `6fe986166090ad9153d4b4b508b7c1c260e8fbb4290f0c6418c86bcccb19bfc5`
- [Summary JSON](../../tests/observe/results/20260930-1241-summary.json) — SHA256 `0ff429370aaf8cea9f2f285661d5ff7c3d24e106bccdffbdb4f9202581ad6338`
- [Watch events](../../tests/observe/results/20260930-1241-events.jsonl) — SHA256 `69ec9f911284d80394bb1e6daebcf0db5000c54ea710481678cf6ecd35f273e2`
- [Tick records](../../tests/observe/results/20260930-1241-ticks.jsonl) — SHA256 `a5431d9a51c6ad28974a965e040e3109ae8b5b4b7126d34398d76aee78b33f50`

### Final-HEAD smoke

- [Summary Markdown](../../tests/observe/results/20260930-1313-summary.md) — SHA256 `ff3240e74bfa4a4582154af5a268c0605b089dc5c3f83a2214fb755a832de391`
- [Summary JSON](../../tests/observe/results/20260930-1313-summary.json) — SHA256 `1529f03425bf7fb97d1c22bab3993111628f6495001be979cb80a45db656aac4`
- [Watch events](../../tests/observe/results/20260930-1313-events.jsonl) — SHA256 `b27e1df7a43e845189719013f087d45987f4495551f10691a439659ebe095b35`
- [Tick records](../../tests/observe/results/20260930-1313-ticks.jsonl) — SHA256 `0d828662def882fb9aa7d66fbd8109180625f9ba0345c7cbfe67014d929044de`

## Remaining bounded decisions

- A source-qualified Nacos `clusterName` migration still requires consumer
  compatibility approval; the current contract fails closed on same-address
  collisions rather than silently changing the wire identity.
- A deployment that wants `health-policy=verified` must inject a real approved
  control-plane verifier. The default deployment-owned mode is an attestation,
  not a product claim that the server switch was read back.
