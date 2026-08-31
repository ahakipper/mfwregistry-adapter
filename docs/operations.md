# spotter — Operations Runbook

Operational reference for building, running, monitoring and troubleshooting
spotter. See [architecture.md](architecture.md) for design background and
[data-model.md](data-model.md) for the pushed data model.

## Build

```bash
# macOS binary
make OS=darwin

# Linux binary (used by the container image)
make OS=linux
```

The Makefile target is `go build -o spotter`; the binary lands in the
repository root. The official build is performed by the internal CI pipeline
(drone) on tag events and produces `hub.mfwdev.com/paas/spotter:<tag>`.

## Run

```bash
# show all flags
./spotter -h
./spotter adapter -h

# run the adapter (defaults: env=test, leader election on, metrics on :8090)
./spotter adapter
```

Typical production invocation:

```bash
./spotter adapter -e product -r k8s,ecs -g <atlas-grpc-addr> -i 21600
```

The `adapter` subcommand requires `--env` to be one of `test`, `dev` or
`product` (anything other than those three panics at startup; `test` is the
default). kubeconfigs are read relative to the working directory, so the
process is normally started from the directory that contains `config/`.

## Flags reference

| Flag | Default | Meaning |
| --- | --- | --- |
| `-r, --providers` | `[]` | Providers to run: `k8s`, `ecs` (comma separated). |
| `-t, --leader-elect` | `true` | Enable etcd leader election. `false` makes the process a forever-fake leader. |
| `-e, --env` | `test` | Environment preset (etcd, kubeconfigs, consul, campaign key). |
| `-i, --push-interval` | `21600` | Full-push interval, in seconds (21600 = 6 h). |
| `-g, --grpc-addr` | `172.16.130.71:50051` | Discovery center (Atlas) gRPC address. |
| `-w, --disable-worker` | `false` | Disable the real push; pushes are logged only. Testing flag. |
| `--appcodes` | `[]` | Restrict pushes to these appcodes. Testing flag. |
| `--metrics-addr` | `:8090` | Prometheus metrics listen address. |
| `-p, --log-file-path` | `./logfiles/` | Log directory. |
| `-m, --log-maxsize` | `100` | Max log size (MB) before rotation. |
| `-n, --log-backup-number` | `10` | Number of rotated log files kept. |
| `-l, --log-level` | `-1` | `-1` debug, `0` info, `1` warning. |
| `-a, --log-age` | `7` | Max log age in days. |
| `-s, --log-to-std` | `true` | Mirror logs to stdout (dual writing: file + console). |
| `-c, --log-encoding` | `json` | Log format: `log` (console) or `json`. |

## Environments

| Preset | etcd endpoints | Kubeconfigs (`config/kubeconfigs/`) | Consul addresses | Campaign key |
| --- | --- | --- | --- | --- |
| `test` | `172.18.12.181:2379`, `172.18.12.182:2379`, `172.18.12.183:2379` | `k8s-sailor` | `10.72.73.172:8520`, `10.72.73.173:8520`, `10.72.73.174:8520` | `/paas/spotter-test` |
| `dev` | same as `test` | `k8s-sailor`, `k8s-vipper` | same as `test` | `/paas/spotter` |
| `product` | `192.168.11.100:2479`, `192.168.11.101:2479`, `192.168.11.102:2479` | `k8s-eel`, `k8s-otter`, `k8s-slug`, `k8s-bernuda` | `10.132.2.40:8520`, `10.132.2.42:8520`, `10.132.2.43:8520` | `/paas/spotter` |

etcd TLS material is selected by the same preset (`config/certs/etcdtest` for
test/dev, `config/certs/etcdprod` for product). The etcd client verifies
connectivity with a `Cluster.MemberList` call (5 s deadline) at startup and
fails fast when etcd is unreachable.

## Metrics and alerting

Endpoint: `http://<metrics-addr>/metrics` (default `:8090`). The same HTTP
server also exposes the standard `net/http/pprof` endpoints (`/debug/pprof/...`).

| Metric | Type | Meaning | Suggested alert |
| --- | --- | --- | --- |
| `sync_all_durations_histogram{provider="k8s"}` | histogram | Duration (ms) of the K8s provider's periodic `CompareAndFlush`. | Alert on sudden growth (full pushes taking minutes) or absence for longer than ~2 full-push intervals. |
| `sync_all_durations_histogram{provider="ecs"}` | histogram | Duration (ms) of the Consul provider's periodic `CompareAndFlush`. | Same as above. |
| `sync_all_durations_histogram{provider="all"}` | histogram | Aggregate variant registered by the metrics package (not currently observed by the active code paths). | None. |
| `sync_once_durations_histogram` | histogram | Duration (ms) of a single `SynInstance` gRPC call. | Alert on latency growth / bucket saturation (10 s per-call timeout). |
| `sync_once_gauge{syncgauge="sync_once_gauge"}` | gauge | Set to 1 after every single-sync observation (heartbeat of the push path). | Alert when absent for several minutes while the node is master. |
| `sync_error_gauge{syncgauge="sync_error_gauge"}` | gauge | Number of instances waiting in the unsynced retry queue. | Alert when it stays > 0 for more than a few minutes (persistent push failures). |

Notice-based alerts (currently log lines — see the note in
[architecture.md](architecture.md#known-limitations-and-technical-debt)):
leader role lost, elector initialization failure, provider start failure,
robot not synced, consul client/watch failures, incremental/full push failure,
and three "Instance data inconsistency" variants emitted during full pushes.

## Failure scenarios and expected behavior

| Scenario | Expected behavior | Observable signal |
| --- | --- | --- |
| Master dies (process or network) | The etcd lease (TTL 10 s) expires; the campaign key is released and a backup is elected. Backups campaign continuously, so re-election completes within about 10 s. The new master starts its providers. | "Leader role lost" notice on the old master (if it is still alive to notice), provider start logs, and a gap in `sync_once_gauge` observations during the takeover. |
| Robot not synced (a K8s cluster unreachable at startup) | The K8s provider loops on `robot.HasSynced()`, emitting a "robot failed to sync the K8s clusters" notice every 15 s. No watch events are consumed and no push happens until *all* configured clusters are synced. | Repeated "robot failed to sync" notices; startup does not progress. |
| Consul watch failure | The monitor logs a warning and emits a "Failed to fetch data from consul..." notice, then retries. A client-factory failure sleeps for the 5 s block-query window to avoid a hot loop. | Consul-related notices in the logs. |
| Push failure to the discovery center | The failed event is queued in the unsynced service; a 5 s ticker retries it and removes it on success. Only the highest-`Reversion` event per instance id is kept, so retries never regress data. | "Failed to sync data incrementally" notice and a growing `sync_error_gauge`. |
| Data inconsistency (full push) | When `CompareAndFlush` finds instances that differ, are provider-only, or are discovery-center-only, the provider pushes the corrections and emits the corresponding "Instance data inconsistency" notice (three variants: same instances with differing fields; instances missing from the discovery center; instances present in the discovery center but not in the provider). | The inconsistency notice and the per-case log lines. |
| Non-master replica | Runs only the election loop; no K8s/Consul connections, no pushes. Its metrics stay idle. | `sync_once_gauge` absent on backup replicas. |

## Deployment notes

- **Cold start required after the etcd key rename.** The campaign key is
  `/paas/spotter` (`/paas/spotter-test` in test) and node registration uses
  `/paas/spotter/register/`. A replica of a pre-rename deployment campaigns
  under a different key, so it would not compete with the new deployment and
  both could act as masters simultaneously. Do not run old and new
  deployments concurrently: stop the old deployment, then start the new one.
- **Cluster decommission procedure.** spotter must know about *every* cluster:
  the K8s provider blocks until all configured clusters are synced, and a full
  push assumes the local view is complete. When a cluster is decommissioned
  (or otherwise becomes permanently unreachable), remove its kubeconfig from
  the environment preset (or from the `config/kubeconfigs` set used by the
  deployment) and restart spotter. If this is not done, the next restart of
  the service blocks on the unreachable cluster and receives no incremental
  events at all.
- Conversely, adding a cluster requires adding its kubeconfig and restarting.
- Run an odd number of replicas (at least two, one master plus one hot
  backup). Backups add/remove flexibly because membership is only expressed
  through the etcd campaign.
- The container image sets `TZ='Asia/Shanghai'` and expects the `config/`
  directory (certs + kubeconfigs) next to the binary under `/usr/bin`.

## See also

- [architecture.md](architecture.md) — architecture design.
- [data-model.md](data-model.md) — the `Instance` model and state machines.
- [../README.md](../README.md) — project README.
