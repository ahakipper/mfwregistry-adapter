# KWork three-watch diagnostic PASS

## Observed boundaries

The harness now continuously observes three independent boundaries:

1. client-go Watch of the authoritative K8s API;
2. Spotter's own informer callback after conversion/filtering, exposed through
   a guarded sequence/ring long-poll endpoint;
3. official Nacos SDK Subscribe callback, followed by a fresh catalog read for
   complete-field equality.

Every sample also requires the periodic K8s/Spotter/Nacos canonical snapshots
to be exactly equal. A watch error, sequence gap, missing callback, or field
drift fails the report.

## Single-Pod timing

| Operation | Boundary | P90 | P95 | P99 |
|---|---|---:|---:|---:|
| Create | API→K8s Watch | 147ms | 281ms | 281ms |
| Create | API→Spotter event | 147ms | 282ms | 282ms |
| Create | K8s Watch→Spotter event | 0.705ms | 0.804ms | 0.804ms |
| Create | Spotter event→Nacos Subscribe | 591ms | 608ms | 608ms |
| Create | API→Nacos Subscribe | 713ms | 848ms | 848ms |
| Delete | API→K8s Watch | 93ms | 111ms | 111ms |
| Delete | API→Spotter event | 90ms | 112ms | 112ms |
| Delete | K8s Watch→Spotter event | 0.166ms | 0.513ms | 0.513ms |
| Delete | Spotter event→Nacos Subscribe | 593ms | 596ms | 596ms |
| Delete | API→Nacos Subscribe | 674ms | 675ms | 675ms |

CrashLoopBackOff reached K8s Watch in 43.5ms, Spotter in 43.5ms, Nacos
Subscribe in 612.8ms, and strict catalog equality in 1.345s. Recovery reached
Nacos Subscribe in 613.5ms and strict equality in 1.390s.

The recovery K8s-Watch→Spotter delta was -2.8ms because two independent
watchers race from the same API event; this signed delta describes observer
arrival order, not a causal negative latency. API-origin timings remain
positive and are the stable cross-watcher comparison.

## Verdict

PASS: ten Create and ten Delete samples, Crash, and recovery all had complete
K8s/Spotter/Nacos event coverage, zero watcher errors/gaps, and exact canonical
snapshot convergence.
