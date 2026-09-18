#!/usr/bin/env bash
set -euo pipefail

# Durable local runner for hour-scale Observe gates. Launch this script through
# nohup so a Codex/terminal session disconnect does not orphan the owned stack.
# It writes an explicit terminal status file on every normal shell exit; the
# Make targets retain ownership of kwok/Nacos/Spotter cleanup.
root=$(cd "$(dirname "$0")/.." && pwd)
mode=${1:-}
run_id=${2:-}
[[ "$mode" == "ladder" || "$mode" == "observe" ]] || { echo "usage: $0 ladder|observe RUN_ID" >&2; exit 2; }
[[ "$run_id" =~ ^[0-9]{8}-[0-9]{6}$ ]] || { echo "invalid RUN_ID: $run_id" >&2; exit 2; }

out="$root/build/observe"
mkdir -p "$out"
status="$out/${mode}-${run_id}.status"
log="$out/${mode}-${run_id}.log"
pid_file="$out/${mode}-${run_id}.pid"
launch_label="${OBS_LAUNCH_LABEL:-${XPC_SERVICE_NAME:-}}"

# launchctl submit may schedule one replacement process in the small interval
# between this service exiting and the asynchronous self-remove completing.
# A terminal status makes RUN_ID immutable: a replacement removes the label
# and exits without overwriting artifacts or starting a second owned stack.
if [[ -f "$status" ]] && grep -Eq '^EXIT_CODE=[0-9]+$' "$status"; then
  if [[ -n "$launch_label" ]]; then
    (/bin/launchctl remove "$launch_label" >/dev/null 2>&1 || true) &
  fi
  exit 0
fi

printf 'RUNNING\n' > "$status"
printf '%d\n' "$$" > "$pid_file"
finish() {
  rc=$?
  printf 'EXIT_CODE=%d\n' "$rc" > "$status"
  rm -f "$pid_file"
  # launchctl submit may respawn a completed submitted service. Remove our own
  # label after the terminal status is durable so a successful gate runs once.
  if [[ -n "$launch_label" ]]; then
	(/bin/launchctl remove "$launch_label" >/dev/null 2>&1 || true) &
  fi
}
trap finish EXIT

cd "$root"
case "$mode" in
  ladder)
    OBS_LADDER_TIMEOUT=${OBS_LADDER_TIMEOUT:-120m} make test-observe-ladder > "$log" 2>&1
    ;;
  observe)
    OBS_SCALE=${OBS_SCALE:-1000} \
    OBS_NODE_POD_CAPACITY=${OBS_NODE_POD_CAPACITY:-1200} \
    OBS_SERVICES=${OBS_SERVICES:-20} \
    OBS_DURATION=${OBS_DURATION:-2h} \
    OBS_BURSTS=${OBS_BURSTS:-true} \
    OBS_TIMEOUT=${OBS_TIMEOUT:-180m} \
      make test-observe > "$log" 2>&1
    ;;
esac
