#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
out="${OBS_WORKDIR:-$root/build/observe}"
mkdir -p "$out"
kc="${OBS_KUBECONFIG:-$out/kubeconfig}"
if [[ -n "${OBS_KUBECONFIG:-}" ]]; then
  [[ -r "$kc" ]] || { echo "EnvError: OBS_KUBECONFIG is not readable: $kc" >&2; exit 2; }
  echo "external kubeconfig mode: $kc (read-only; observe-down will not delete it)"
  exit 0
fi
for bin in kwokctl kubectl; do command -v "$bin" >/dev/null || { echo "EnvError: missing $bin" >&2; exit 2; }; done
cluster="${OBS_KWOK_CLUSTER:-dsca-observe}"
api="${OBS_KWOK_API_PORT:-34567}"
etcd="${OBS_KWOK_ETCD_PORT:-34679}"
kwokctl create cluster --name "$cluster" --kubeconfig "$kc" --kube-apiserver-port "$api" --etcd-port "$etcd"
kubectl --kubeconfig "$kc" label node "${OBS_KWOK_NODE:-kwok-node}" kwok.x-k8s.io/node=fake --overwrite >/dev/null 2>&1 || true
echo "$kc" > "$out/owned-kubeconfig"
