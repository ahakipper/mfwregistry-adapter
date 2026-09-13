#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
if [[ -n "${OBS_KUBECONFIG:-}" ]]; then echo "external kubeconfig mode: no deletion"; exit 0; fi
state="$root/build/observe/observe-state"
[[ -r "$state" ]] || exit 0
sha256sum -c "$root/build/observe/observe-state.sha256" >/dev/null || { echo "EnvError: owned state hash mismatch" >&2; exit 2; }
source "$state"
[[ "$cluster" =~ ^dsca-observe-[a-zA-Z0-9_-]+$ ]] || { echo "EnvError: invalid owned state" >&2; exit 2; }
[[ "$kubeconfig" == "$root/build/observe/"* ]] || { echo "EnvError: foreign kubeconfig state" >&2; exit 2; }
command -v kwokctl >/dev/null || { echo "InfraError: missing kwokctl for teardown" >&2; exit 2; }
kwokctl delete cluster --name "$cluster" --kubeconfig "$kubeconfig" || { echo "InfraError: kwok teardown failed" >&2; exit 1; }
rm -f "$state" "$root/build/observe/observe-state.sha256"
