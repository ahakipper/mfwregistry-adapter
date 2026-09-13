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
cleanup() { kwokctl delete cluster --name "$cluster" --kubeconfig "$kc" >/dev/null 2>&1 || true; }
trap cleanup ERR
kwokctl create cluster --name "$cluster" --kubeconfig "$kc" --kube-apiserver-port "$api" --etcd-port "$etcd"
for i in {1..30}; do [[ -s "$kc" ]] && kubectl --kubeconfig "$kc" get --raw=/readyz >/dev/null 2>&1 && break; sleep 1; done
[[ -s "$kc" ]] || { echo "InfraError: kwok kubeconfig not created" >&2; exit 3; }
kubectl --kubeconfig "$kc" get --raw=/readyz >/dev/null || { echo "InfraError: kwok apiserver not ready" >&2; exit 3; }
kubectl --kubeconfig "$kc" label node "${OBS_KWOK_NODE:-kwok-node}" kwok.x-k8s.io/node=fake --overwrite >/dev/null 2>&1 || true
printf 'cluster=%s\nkubeconfig=%s\napi=%s\netcd=%s\n' "$cluster" "$kc" "$api" "$etcd" > "$out/observe-state"
sha256sum "$out/observe-state" > "$out/observe-state.sha256"
trap - ERR
