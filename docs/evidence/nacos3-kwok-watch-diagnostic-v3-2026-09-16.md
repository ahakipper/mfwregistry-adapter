# KWork Watch/Subscribe diagnostic v3

## Result

The third single-Pod diagnostic proved the Crash/Unhealthy product fix:

- CrashLoopBackOff converged across K8s, Spotter, and the fresh Nacos SDK view
  in 1.393s.
- Recovery converged in 1.408s.
- All ten Create and ten Delete operations reached strict full-field snapshot
  equality.
- K8s Watch covered all twenty measured operations.
- Nacos Subscribe covered the last nine Create/Delete pairs, but missed the
  first cold pair because the mutation started immediately after Subscribe
  returned, before the push stream delivered its first changed snapshot.

The run correctly failed its watch-coverage gate. The first cold Create took
14.96s; steady Create P90 was 1.437s and Delete P99 was 1.439s. These values
remain diagnostic evidence because the first callback boundary was incomplete.

## Method correction

The ladder now performs an unmeasured Create/Delete warm-up and requires both
independent K8s Watch and Nacos Subscribe coverage before recording percentile
samples. This removes observer-startup time from application propagation
latency while retaining the cold-start measurement as a separate future
scenario.
