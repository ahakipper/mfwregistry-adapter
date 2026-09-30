# Nacos 3 + KWOK P1 Remediation Smoke — 2026-09-30

## Verdict

**PASS — post-remediation smoke only.** This run validates that the P1 code
changes can start the real Nacos 3 ARM64/official gRPC fixture, reconcile a
small K8s source, and cleanly tear down. It is not a replacement for the
1,000-Pod long-duration qualification.

## Run

```text
Command: OBS_DURATION=1m OBS_SCALE=10 OBS_SERVICES=2 OBS_BURSTS=false OBS_CRASH_CYCLES=true OBS_TIMEOUT=10m make test-observe
Target:  nacos/nacos-server:v3.2.4-slim, linux/arm64
Vehicle: owned KWOK cluster dsca-observe-14251, client-go Watch, Nacos 3 SDK/gRPC
Code:    post-remediation working tree (P1 remediation commits included)
```

The runner created the KWOK cluster and throwaway Nacos container, provisioned
the health-check switch, ran Spotter, and deleted the owned cluster/container
after the test. No observe state file or container remained.

## Acceptance summary

| Check | Result |
| --- | ---: |
| Observation duration | 1m0s |
| Scale / services | 10 / 2 |
| Exact ticks | 7/7 |
| Product-divergent ticks | 0 |
| Observation errors | 0 |
| K8s/Spotter/Nacos mutation correlations | 2/2 |
| Missing mutations | 0 |
| Dropped events | 0 |
| Retry queue final drain | true |
| Final three-plane snapshot | exact |
| Crash / recovery samples | 1 / 1 |
| Overall smoke verdict | **PASS** |

## Latency samples

The smoke has one Crash and one Recovery sample per operation, so these are
diagnostic values rather than statistically meaningful percentiles:

| Operation | K8s Watch → Nacos P50/P90/P95/P99 | API → Nacos P50/P90/P95/P99 |
| --- | --- | --- |
| Crash | 0.534s / 0.534s / 0.534s / 0.534s | 0.561s / 0.561s / 0.561s / 0.561s |
| Recovery | 0.585s / 0.585s / 0.585s / 0.585s | 0.603s / 0.603s / 0.603s / 0.603s |

## Latest rerun after SDK log-level hardening

The first smoke passed, but exposed unbounded SDK debug output in the child
log. The SDK facade now initializes the official logger at `warn`; the rerun
used owned KWOK cluster `dsca-observe-21989`, passed the same acceptance gates,
and produced a 138 KiB child log instead of the prior multi-gigabyte growth.

## Artifacts and hashes

- [Summary Markdown](../../tests/observe/results/20260930-1031-summary.md) —
  SHA256 `54eeb317ec56cd1257d1af3417be7a869748dfa5b9ac9cefb015153a60ff0759`
- [Summary JSON](../../tests/observe/results/20260930-1031-summary.json) —
  SHA256 `e37257e657a04cf16ec7bda8768eb9ede737150a41eefef8cc1b0a77065ebae7`
- [Watch events](../../tests/observe/results/20260930-1031-events.jsonl) —
  SHA256 `57e1d30249e6d3a31d98a7fd9ed5cb6b9961a386854d870b2648234efa5e9979`
- [Tick records](../../tests/observe/results/20260930-1031-ticks.jsonl) —
  SHA256 `295dad19eb22cb23cb85d64fc4821558c49691ad9c931eccd7f9a03904688f75`

Latest rerun artifacts:

- [Summary Markdown](../../tests/observe/results/20260930-1041-summary.md) —
  SHA256 `11fb455385c7084b206995fbcd46923d580015bfa3f4cb42f7f469165c02c78c`
- [Summary JSON](../../tests/observe/results/20260930-1041-summary.json) —
  SHA256 `de2ed08a3f9359f6331472e9bb765437ec3bde35b4bd77596d433ee928d064ca`
- [Watch events](../../tests/observe/results/20260930-1041-events.jsonl) —
  SHA256 `41ebd340e90909432ae2ee6a80114b3a0f1c395da85df6d53a93276d841863dd`
- [Tick records](../../tests/observe/results/20260930-1041-ticks.jsonl) —
  SHA256 `cc3c46e0e4e44909cecc1f7f6dd39cb6c613b84b403af0467b678f512c02c696`

## Boundary

This smoke confirms the post-remediation runtime path and cleanup. It does not
close the source-qualified Nacos wire migration decision, provide a production
health-policy verifier, or qualify 1,000-Pod/20-service sustained churn. Those
remain the next Stage 5 gates.
