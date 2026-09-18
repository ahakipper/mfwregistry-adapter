# Nacos 3 + KWOK continuous-watch scale evidence

Date: 2026-09-17/18

This is the first full scale run using the corrected observation method. It is
not the two-hour soak; it validates the three-plane measurement and
per-instance percentile implementation before the soak.

Raw artifacts:

- [scale-ladder JSON](../../tests/observe/results/20260917-224345-scale-ladder-summary.json)
- [scale-ladder Markdown](../../tests/observe/results/20260917-224345-scale-ladder-summary.md)
- SHA256 JSON: `d821b92e11ddfab50929340fe3aa2eba646d0542f7f83f29869c01f2a4f8ef37`
- SHA256 Markdown: `2a78df2ae7ff2d23f386b921cf89b73d5bc142aea1158e4cb47fc7d8a476d7a7`

Short sustained-observe validation (20 Pods / 2 services / 1 minute) is also
tracked: summary SHA256 `a103d883a4d76536c6da7204a50d7400935ecb8445460b4641f80f5feef61460`,
events SHA256 `c969b6618fc4b1c499afd6b7db3df41cc08c5de52b862515ab94f1254f893ab9`,
and ticks SHA256 `dcf45d4f62e119aea30e0a768a6a15911d34dfd14f8c00d124c761c86249dfa9`.

## Coverage and verdict

- Target: Nacos 3 ARM64 + KWOK.
- Scales: 1, 10, 100, 500, 1000.
- Operations: Create and Delete at every scale; CrashLoopBackOff and Recovery.
- 10,220 per-instance samples across the ladder.
- Batch repetitions: 10 at scales 1/10, 5 at 100, 3 at 500/1000. Batch values are reported as min/median/max, not as formal P99.
- Zero watch errors, zero failures, zero dropped-event evidence, and cleanup completed.
- Verdict: **PASS for the scale-ladder method and product behavior under this run; the 2-hour soak remains pending.**

## Per-instance API → Nacos Subscribe latency

| Scale | Operation | Instance samples | P90 (s) | P95 (s) | P99 (s) |
|---:|---|---:|---:|---:|---:|
| 1 | Create | 10 | 0.742 | 0.850 | 0.850 |
| 1 | Delete | 10 | 0.653 | 0.654 | 0.654 |
| 10 | Create | 100 | 0.775 | 0.978 | 0.978 |
| 10 | Delete | 100 | 0.657 | 0.658 | 0.658 |
| 100 | Create | 500 | 1.458 | 1.458 | 2.058 |
| 100 | Delete | 500 | 16.566 | 17.411 | 18.427 |
| 500 | Create | 1500 | 8.888 | 9.347 | 10.397 |
| 500 | Delete | 1500 | 81.705 | 86.639 | 90.864 |
| 1000 | Create | 3000 | 27.285 | 29.720 | 36.995 |
| 1000 | Delete | 3000 | 165.883 | 174.431 | 182.375 |

The large delete values are real end-to-end visibility times, not test timeouts:
the K8s Watch itself reached P99 181.966s at 1000 deletes, while the additional
Spotter provider-trigger→pre-worker P99 was 29ms and Spotter→Nacos Subscribe
P99 was 15.176s. This separates source/API pressure from downstream Nacos
visibility.

## Batch completion

Batch completion is separately reported because three repetitions cannot support
a meaningful batch P99:

- 1000 Create: min 22.086s, median 28.968s, max 64.720s.
- 1000 Delete: min 184.305s, median 184.598s, max 185.917s.
- 500 Create: min 11.603s, median 12.396s, max 12.473s.
- 500 Delete: min 91.360s, median 91.589s, max 92.490s.

## Strict correctness gates

Each stable-cut snapshot compared the full domain projection, including labels,
Reversion, SourceKey/SourceCluster, endpoint, status, lifecycle, and canonical
payload. Create/Crash/Recovery event correlation used Reversion + Status +
canonical payload. Delete used UID/SourceKey + offline + per-service Nacos
snapshot removal. All samples passed.

## Limitation

This is a scale-ladder qualification, not the requested two-hour reliability
claim. The formal two-hour run must still complete with continuous K8s Watch,
Spotter provider-output stream, 20 Nacos SDK subscriptions, append-only mutation
journal, stable-cut attempts, zero missing mutation correlation, and queue drain.
