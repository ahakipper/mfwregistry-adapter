# KWork Watch/Subscribe diagnostic PASS

## Result

The warmed single-Pod diagnostic passed every observer and consistency gate:

- 10 Create and 10 Delete samples reached strict full-field equality across
  K8s live state, Spotter informer projection, and fresh Nacos SDK catalog.
- All 20 samples were observed by both independent client-go K8s Watch and
  official Nacos SDK Subscribe callbacks.
- Watch errors: 0.
- CrashLoopBackOff converged in 1.426s; recovery converged in 1.392s.
- Create API-to-Nacos exact: P90 1.492s, P95/P99 1.626s.
- Delete API-to-Nacos exact: P90 1.401s, P95/P99 1.404s.
- Create source-visible-to-Nacos exact: P90 1.292s, P95/P99 1.516s.
- Delete source-visible-to-Nacos exact: P90 1.304s, P95/P99 1.313s.

The unmeasured warm-up completed before percentile collection, so these
samples exclude observer startup while retaining strict snapshot and event
coverage. The full 1/10/100/500/1000 ladder is the next gate.
