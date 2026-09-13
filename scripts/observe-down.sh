#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
if [[ -n "${OBS_KUBECONFIG:-}" ]]; then echo "external kubeconfig mode: no deletion"; exit 0; fi
kc="${OBS_WORKDIR:-$root/build/observe}/owned-kubeconfig"
[[ -r "$kc" ]] || exit 0
command -v kwokctl >/dev/null || { echo "InfraError: missing kwokctl for teardown" >&2; exit 2; }
path=$(<"$kc")
kwokctl delete cluster --name "${OBS_KWOK_CLUSTER:-dsca-observe}" --kubeconfig "$path" || { echo "InfraError: kwok teardown failed" >&2; exit 1; }
rm -f "$kc"
