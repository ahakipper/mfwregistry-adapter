# Nacos health-checker evidence (2026-09-21)

## Conclusion

Registering a persistent instance does not, by itself, disable Nacos server-side health checks. The registration request carries instance state (`ip`, `port`, `healthy`, `enabled`, `ephemeral`, and metadata); the active checker is a property of the `(namespace, group, service, cluster)` object.

This distinction explains the `limactl -> 10.0.x.x:7096` warnings. `limactl usernet` is the Colima forwarding process. Nacos is the component that initiates TCP checks; the forwarding process only carries those connections into the host's network/TUN path.

## Reproduction against historical Nacos 2.1.0

The local `nacos/nacos-server:v2.1.0-slim` image was started under the existing Colima/amd64 emulation path. A persistent instance was registered through the v1 naming API:

```text
serviceName=health-v2
clusterName=k8s
ip=10.0.0.1
port=7096
ephemeral=false
```

The service metadata endpoint returned:

```json
{"healthChecker":{"type":"TCP"}}
```

While the instance remained registered, the Nacos container's `/proc/net/tcp6` contained many `SYN_SENT` connections to `10.0.0.1:7096`. This is the same mechanism as the screenshot's repeated `dial tcp 10.0.x.x:7096` timeouts. The instance may still be reported as `healthy=true` while connection attempts are in flight; that does not mean that probing is disabled.

Nacos's current health documentation describes persistent instances as being maintained by server-side active health checks and lists `TCP`, `HTTP`, `MySQL`, and `NONE` as the checker choices: [Health, Weight, And Metadata](https://www.nacos.io/en/docs/next/manual/user/naming/health-and-metadata/).

## Observe harness protection

The Nacos 3 Observe harness now performs an explicit test/deployment-control-plane preflight before any synthetic instance is registered:

1. Set Nacos 3 naming `healthCheckEnabled=false` through `/nacos/v3/admin/ns/ops/switches`.
2. Read the same switch back and fail closed unless the value is actually `false`.
3. Start Spotter and send all business register, deregister, query, and subscribe operations through the official SDK.

The switch is the Nacos 3 server-wide health-check guard; a per-cluster Admin update returned success while leaving the gRPC runtime cluster unchanged, so it is not used as the acceptance guard. The Admin compatibility call is not the Spotter data path. If this control-plane preflight cannot be completed or read back as false, the harness fails closed instead of running with an unknown probe policy.

The Nacos 3 container also sets `NACOS_AUTH_ADMIN_ENABLE=false` for this isolated local fixture. This is a test-only startup setting; production authentication and deployment policy remain outside the Spotter scope.

## Verification run

Run: `20260921-0138` (Nacos 3.2.4 ARM64, KWOK, 20 Pods, 2 services, 2-minute window, SDK data path).

Evidence:

- `Nacos naming healthCheckEnabled=false provisioned for 2 k8s service clusters` was emitted after a successful readback before Spotter startup.
- 13/13 observation ticks were exactly consistent; 0 divergent and 0 observation-error ticks.
- 2/2 K8s mutations were correlated to Nacos observations; missing=0.
- K8s Watch -> Nacos latency: create P90/P95/P99 `0.574s`; delete P90/P95/P99 `0.584s`.
- Final post-quiescence three-plane snapshot was exact.
- Nacos and KWOK teardown completed with no residual resources.

The detailed report is [20260921-0138-summary.md](../../tests/observe/results/20260921-0138-summary.md); the machine-readable record is [20260921-0138-summary.json](../../tests/observe/results/20260921-0138-summary.json).

## Scope boundary

This evidence closes the local probe-storm cause and the Observe-fixture guard. It does not claim that a production Nacos deployment has the same health policy: production Admin authentication, TLS, namespace/group authorization, and deployment ownership still need to be configured by the deployment owner. Those deployment gates are intentionally outside the current Spotter scope.
