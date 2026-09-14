#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
if [[ -n "${OBS_KUBECONFIG:-}" ]]; then echo "external kubeconfig mode: no deletion"; exit 0; fi
state="$root/build/observe/observe-state"
[[ -r "$state" ]] || exit 0
sha256sum -c "$root/build/observe/observe-state.sha256" >/dev/null || { echo "EnvError: owned state hash mismatch" >&2; exit 2; }
cluster=$(awk -F= '$1=="cluster"{print $2}' "$state")
kubeconfig=$(awk -F= '$1=="kubeconfig"{print $2}' "$state")
[[ "$cluster" =~ ^dsca-observe-[a-zA-Z0-9_-]+$ ]] || { echo "EnvError: invalid owned state" >&2; exit 2; }
expected_kwok_kubeconfig="${HOME}/.kwok/clusters/${cluster}/kubeconfig.yaml"
if [[ -n "${OBS_KWOK_KUBECONFIG:-}" ]]; then
  configured_kwok_kubeconfig="$OBS_KWOK_KUBECONFIG"
  if [[ "$configured_kwok_kubeconfig" != /* ]]; then
    configured_kwok_kubeconfig="$root/$configured_kwok_kubeconfig"
  fi
  [[ "$kubeconfig" == "$configured_kwok_kubeconfig" ]] || { echo "EnvError: foreign kubeconfig state" >&2; exit 2; }
else
  [[ "$kubeconfig" == "$root/build/observe/"* || "$kubeconfig" == "$expected_kwok_kubeconfig" ]] || { echo "EnvError: foreign kubeconfig state" >&2; exit 2; }
fi
command -v kwokctl >/dev/null || { echo "InfraError: missing kwokctl for teardown" >&2; exit 2; }
command -v sha256sum >/dev/null || { echo "InfraError: missing sha256sum for state verification" >&2; exit 2; }
kwokctl delete cluster --name "$cluster" --kubeconfig "$kubeconfig" || { echo "InfraError: kwok teardown failed" >&2; exit 1; }
rm -f "$state" "$root/build/observe/observe-state.sha256"
