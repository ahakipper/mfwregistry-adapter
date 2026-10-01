# Consul → Nacos 3 ARM64 local qualification evidence

Date: 2026-10-01
Repository: `mfwregistry-adapter`
Branch: `refactor/all`
Qualification code baseline: `663341b`; the Makefile lifecycle and real-gate hardening used for this run are included in the follow-up commit that adds this evidence.

## Command and lifecycle

The reusable command was:

```bash
make test-consul-real-local
```

The target created only two uniquely named, disposable containers, bound to loopback ports, and removed them in its EXIT cleanup trap. It refused preconfigured `CONSUL_SERVER`/`NACOS_SERVER` values, occupied ports, non-ARM64 Docker engines, image-architecture mismatches, and existing target names.

| Component | Image and digest | Architecture | Host mapping |
| --- | --- | --- | --- |
| Consul | `hashicorp/consul:1.22.0@sha256:117f1fdd7cd6d84069fa0f69a0c0804b5b3bf9a30450d2143d5817b78ce757b6` | `linux/arm64` | `127.0.0.1:18500 → 8500` |
| Nacos | `nacos/nacos-server:v3.2.4-slim@sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5` | `linux/arm64` | `127.0.0.1:28848 → 8848`, `29848 → 9848`, `29849 → 9849` |

Nacos 3 required a base64 JWT key of at least 256 bits even with authentication disabled; the lifecycle fixture supplies a non-production local token. Before any business registration, the guarded test used the explicit HTTP compatibility Admin seam to set and read back `healthCheckEnabled=false`. All naming registrations, updates, TTL transitions, queries, and cleanup used the official Go SDK.

## Observed result

Configured samples: 2. The report covered create, update, delete, recovery, TTL health-down, and TTL health-recovery. The observation oracle polled the Nacos catalog every 100 ms and required complete count/identity plus `Enabled=true` and `Healthy=true` for positive states. Update additionally required the old port to disappear.

| Operation | P50 | P90 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: |
| Create | 147.32 ms | 437.15 ms | 437.15 ms | 437.15 ms |
| Update | 1,588.28 ms | 1,672.23 ms | 1,672.23 ms | 1,672.23 ms |
| Delete | 1,787.52 ms | 3,050.53 ms | 3,050.53 ms | 3,050.53 ms |
| Recovery | 955.30 ms | 1,271.00 ms | 1,271.00 ms | 1,271.00 ms |
| Health down | 2,973.48 ms | 2,977.26 ms | 2,977.26 ms | 2,977.26 ms |
| Health recovery | 953.81 ms | 1,039.83 ms | 1,039.83 ms | 1,039.83 ms |

The same mutations were observed through the official Nacos SDK Subscribe
callback with `UpdateCacheWhenEmpty=true`:

| Subscribe transition | P50 | P90 | P95 | P99 |
| --- | ---: | ---: | ---: | ---: |
| Create | 533.33 ms | 600.70 ms | 600.70 ms | 600.70 ms |
| Update | 1,215.85 ms | 1,312.82 ms | 1,312.82 ms | 1,312.82 ms |
| Delete | 1,729.96 ms | 2,954.46 ms | 2,954.46 ms | 2,954.46 ms |
| Recovery | 1,011.93 ms | 1,162.38 ms | 1,162.38 ms | 1,162.38 ms |
| Health down | 2,988.19 ms | 3,009.60 ms | 3,009.60 ms | 3,009.60 ms |
| Health recovery | 839.88 ms | 943.45 ms | 943.45 ms | 943.45 ms |

All six Subscribe summaries had 2 samples and zero callback errors.

The real Nacos catalog returned the observed host IP `172.17.0.5`, confirming that the Consul provider projection uses the Consul node address rather than the registration's `Service.Address=127.0.0.1`.

The final lifecycle metadata was:

```
qualification_status=passed
cleanup_status=passed
residual_unknown=false
final_exit_code=0
```

The complete artifact is under `build/consul-real/20261001T021656Z-54838/` in the local workspace. The directory is ignored by Git and contains the JSON report, test log, image architecture records, startup logs, container logs, cleanup log, and final exit metadata. The Nacos health switch was restored to its original value before the container was removed.

## Interpretation and limits

This is a real local ARM64 Consul 1.22 → Spotter provider/worker → Nacos 3.2.4 official-SDK qualification, not a production SLO. The latency boundary is explicitly:

```
Consul Agent mutation start → Nacos catalog observed by SDK-backed polling
and official SDK Subscribe callback
```

The test separately records the provider watch-to-sync metric and carries the
same origin through Event.Trigger to the per-sink Nacos SDK acknowledgement
metric; the JSON report above contains the catalog and Subscribe observations.
The two samples per operation are too small for a statistically meaningful
production percentile claim. TLS/ACL, multi-node HA, non-public namespace
authorization, and Consul Enterprise partition behavior remain deployment-
scope evidence gaps.
