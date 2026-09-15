#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
if [[ -n "${OBS_KUBECONFIG:-}" ]]; then echo "external kubeconfig mode: no deletion"; exit 0; fi
state="$root/build/observe/observe-state"
remove_owned_nacos() {
  # The test process normally removes this container in t.Cleanup. If the
  # process is interrupted, however, only this shell trap remains. The fixed
  # name is reserved by observe-up.sh, so it is safe to remove when present.
  command -v docker >/dev/null 2>&1 || return 0
  if docker inspect dsca-observe-nacos >/dev/null 2>&1; then
    docker rm -f dsca-observe-nacos >/dev/null 2>&1 || echo "InfraError: Nacos container cleanup failed (residual_unknown=true)" >&2
  fi
}
remove_owned_child() {
  pid_file="$root/build/observe/spotter-child.pid"
  [[ -r "$pid_file" ]] || return 0
  pid=$(tr -dc '0-9' < "$pid_file" || true)
  if [[ "$pid" =~ ^[0-9]+$ ]]; then
    command_line=$(ps -p "$pid" -o command= 2>/dev/null || true)
    if [[ "$command_line" == *"build/observe/spotter adapter"* ]]; then
      kill -TERM "$pid" 2>/dev/null || true
      sleep 1
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$pid_file"
}
if [[ ! -r "$state" ]]; then
  remove_owned_child
  remove_owned_nacos
  exit 0
fi
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
remove_owned_child
remove_owned_nacos
rm -f "$state" "$root/build/observe/observe-state.sha256"
