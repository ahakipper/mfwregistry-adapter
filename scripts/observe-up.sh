#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/build/observe"
mkdir -p "$out"
kc="${OBS_KUBECONFIG:-$out/kubeconfig}"
if [[ -n "${OBS_KUBECONFIG:-}" ]]; then
  [[ -r "$kc" ]] || { echo "EnvError: OBS_KUBECONFIG is not readable: $kc" >&2; exit 2; }
  echo "external kubeconfig mode: $kc (read-only; observe-down will not delete it)"
  exit 0
fi
for bin in docker kwokctl kubectl nc; do command -v "$bin" >/dev/null || { echo "EnvError: missing $bin" >&2; exit 2; }; done
docker info >/dev/null 2>&1 || { echo "InfraError: docker daemon unavailable" >&2; exit 3; }
cluster="${OBS_KWOK_CLUSTER:-dsca-observe-$$}"
[[ "$cluster" =~ ^dsca-observe-[a-zA-Z0-9_-]+$ ]] || { echo "EnvError: invalid cluster name" >&2; exit 2; }
api="${OBS_KWOK_API_PORT:-34567}"
etcd="${OBS_KWOK_ETCD_PORT:-34679}"
for port in "$api" "$etcd" "${OBS_NACOS_PORT:-28848}" "${OBS_NACOS_GRPC_PORT:-29848}" "${OBS_NACOS_CONTROL_PORT:-29849}" "${OBS_ATLAS_PORT:-19997}" "${OBS_METRICS_PORT:-19998}"; do
  if nc -z 127.0.0.1 "$port" >/dev/null 2>&1; then echo "EnvError: scratch port already in use: $port" >&2; exit 2; fi
done
umask 077
printf 'cluster=%s\nkubeconfig=%s\napi=%s\netcd=%s\n' "$cluster" "$kc" "$api" "$etcd" > "$out/observe-state.tmp"
mv "$out/observe-state.tmp" "$out/observe-state"
sha256sum "$out/observe-state" > "$out/observe-state.sha256"
cleanup_enabled=1
cleanup() { rc=$?; if [ "$cleanup_enabled" -eq 1 ]; then if ! kwokctl delete cluster --name "$cluster" --kubeconfig "$kc" >/dev/null 2>"$out/cleanup-error"; then echo "residual_unknown=true" > "$out/cleanup-status"; else echo "residual_unknown=false" > "$out/cleanup-status"; fi; fi; exit $rc; }
trap cleanup EXIT
kwokctl create cluster --name "$cluster" --kubeconfig "$kc" --kube-apiserver-port "$api" --etcd-port "$etcd"
for i in {1..30}; do [[ -s "$kc" ]] && kubectl --kubeconfig "$kc" get --raw=/readyz >/dev/null 2>&1 && break; sleep 1; done
[[ -s "$kc" ]] || { echo "InfraError: kwok kubeconfig not created" >&2; exit 3; }
kubectl --kubeconfig "$kc" get --raw=/readyz >/dev/null || { echo "InfraError: kwok apiserver not ready" >&2; exit 3; }
node="${OBS_KWOK_NODE:-$(kubectl --kubeconfig "$kc" get nodes -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)}"
kubectl --kubeconfig "$kc" label node "$node" kwok.x-k8s.io/node=fake --overwrite >/dev/null 2>&1 || { echo "InfraError: kwok node $node unavailable" >&2; exit 3; }
# Pre-seed a generous allocatable capacity so large observe runs do not depend
# on a user's pre-existing node patch. The status subresource is best-effort
# across kwok versions; verify the node and fail closed when capacity is absent.
scale="${OBS_SCALE:-1000}"
kubectl --kubeconfig "$kc" patch node "$node" --subresource=status --type=merge -p "{\"status\":{\"capacity\":{\"pods\":\"$scale\"},\"allocatable\":{\"pods\":\"$scale\"}}}" >/dev/null || { echo "InfraError: kwok node capacity patch failed" >&2; exit 3; }
capacity=$(kubectl --kubeconfig "$kc" get node "$node" -o jsonpath='{.status.allocatable.pods}')
[[ "$capacity" =~ ^[0-9]+$ && "$capacity" -ge "$scale" ]] || { echo "InfraError: node pod capacity $capacity below $scale" >&2; exit 3; }
cpu=$(kubectl --kubeconfig "$kc" get node "$node" -o jsonpath='{.status.allocatable.cpu}')
memory=$(kubectl --kubeconfig "$kc" get node "$node" -o jsonpath='{.status.allocatable.memory}')
[[ -n "$cpu" && -n "$memory" ]] || { echo "InfraError: node CPU/memory capacity unavailable" >&2; exit 3; }
kubectl --kubeconfig "$kc" wait --for=condition=Ready "node/$node" --timeout=30s >/dev/null || { echo "InfraError: kwok node not Ready" >&2; exit 3; }
echo "state=ready" >> "$out/observe-state"
sha256sum "$out/observe-state" > "$out/observe-state.sha256"
cleanup_enabled=0
