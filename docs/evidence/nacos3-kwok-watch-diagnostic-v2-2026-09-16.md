# KWork Watch/Subscribe diagnostic v2

## Result

The second single-Pod diagnostic fixed and verified the Nacos Subscribe event
boundary: all ten Create and ten Delete samples were observed by both the
independent K8s Watch and the official Nacos SDK Subscribe callback.

Strict snapshots converged for all twenty Create/Delete samples. Steady
single-Pod API-to-Nacos-exact observations were approximately 1.36-1.45s; the
first cold Create was 15.88s. These remain diagnostic measurements and are not
the final percentile report.

The CrashLoopBackOff transition failed because the official Nacos 3 naming
query/Subscribe view omitted the persistent instance when it was written with
`enabled=false`, even though the gRPC register request succeeded. Spotter then
re-registered it on every reconcile interval. Recovery to ready/online became
visible and converged in 6.62s.

## Product fix

SDK mode now writes an unhealthy instance as transport `enabled=true` and
`healthy=false`. The complete canonical metadata remains the authoritative
Spotter domain payload and preserves `Enabled=false`, `Status=2`, state,
labels, and Reversion. Reconstruction and equality compare the canonical
domain value while accepting this exact query-visibility wire projection.
Online and manually disabled drift remain strict.

The next diagnostic must prove Crash and recovery through K8s Watch, Spotter
projection, Nacos Subscribe, and fresh catalog equality before the full scale
ladder starts.
