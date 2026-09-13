#!/usr/bin/env bash
set -euo pipefail

grep -q 'OBS_KUBECONFIG external mode' Makefile
grep -q 'external kubeconfig mode: no deletion' scripts/observe-down.sh
root=$(cd "$(dirname "$0")/.." && pwd)
rm -f "$root/build/observe/observe-state" "$root/build/observe/observe-state.sha256"
tmp=$(mktemp -d "${TMPDIR:-/tmp}/observe-life.XXXXXX")
cleanup() { rm -f "$tmp"/bin/* "$tmp"/* 2>/dev/null || true; rmdir "$tmp"/bin "$tmp" 2>/dev/null || true; }
trap cleanup EXIT
fake="$tmp/bin"; mkdir "$fake"
cat >"$fake/nc" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
cat >"$fake/docker" <<'EOF'
#!/usr/bin/env bash
[[ "${1:-}" == info ]] && exit "${FAKE_DOCKER_RC:-0}"
exit 0
EOF
cat >"$fake/kubectl" <<'EOF'
#!/usr/bin/env bash
case "${FAKE_KUBECTL_MODE:-ok}" in
  fail) exit 1;;
  ready) [[ "$*" == *"--raw=/readyz"* ]] && exit 0; exit 1;;
  capacity) [[ "$*" == *"jsonpath"* ]] && { printf '1000'; exit 0; };;
esac
[[ "$*" == *"--raw=/readyz"* ]] && exit 0
[[ "$*" == *"jsonpath"* ]] && { printf '1000'; exit 0; }
exit 0
EOF
cat >"$fake/kwokctl" <<'EOF'
#!/usr/bin/env bash
if [[ "${1:-}" == create ]]; then
  [[ "${FAKE_KWOK_CREATE_RC:-0}" == 0 ]] || exit "$FAKE_KWOK_CREATE_RC"
  kc=""; while (($#)); do [[ "$1" == --kubeconfig ]] && kc="$2"; shift; done
  mkdir -p "$(dirname "$kc")"; printf 'fake-kubeconfig\n' >"$kc"; exit 0
fi
if [[ "${1:-}" == delete ]]; then
  [[ -n "${FAKE_DELETE_LOG:-}" ]] && printf '%s\n' delete >>"$FAKE_DELETE_LOG"
  exit "${FAKE_KWOK_DELETE_RC:-0}"
fi
exit 0
EOF
chmod +x "$fake"/*

assert_fail() { set +e; "$@" >/dev/null 2>&1; rc=$?; set -e; [[ $rc -ne 0 ]] || { echo "expected failure: $*" >&2; exit 1; }; }

# Missing prerequisite must be classified as EnvError and leave no state.
assert_fail env PATH="/usr/bin:/bin" OBS_KWOK_CLUSTER=dsca-observe-test /bin/bash "$root/scripts/observe-up.sh"

# External kubeconfig is read-only and observe-down must not delete it.
external="$tmp/external-kubeconfig"; printf 'external\n' >"$external"
PATH="$fake:/usr/bin:/bin" OBS_KUBECONFIG="$external" "$root/scripts/observe-up.sh"
PATH="$fake:/usr/bin:/bin" OBS_KUBECONFIG="$external" "$root/scripts/observe-down.sh"
[[ -f "$external" ]] || { echo "external kubeconfig was removed" >&2; exit 1; }

# Docker daemon and port conflicts fail before cluster creation.
assert_fail env PATH="$fake:/usr/bin:/bin" FAKE_DOCKER_RC=1 "$root/scripts/observe-up.sh"
cat >"$fake/nc" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
assert_fail env PATH="$fake:/usr/bin:/bin" "$root/scripts/observe-up.sh"

# Create failure invokes the cleanup path and leaves no owned state.
rm -f "$root/build/observe/observe-state" "$root/build/observe/observe-state.sha256"
cat >"$fake/nc" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
assert_fail env PATH="$fake:/usr/bin:/bin" FAKE_KWOK_CREATE_RC=1 "$root/scripts/observe-up.sh"
[[ ! -e "$root/build/observe/observe-state" ]] || { echo "state leaked after create failure" >&2; exit 1; }

# A successful create followed by readiness failure must invoke delete cleanup.
delete_log="$tmp/delete.log"
assert_fail env PATH="$fake:/usr/bin:/bin" FAKE_KUBECTL_MODE=fail FAKE_DELETE_LOG="$delete_log" OBS_KWOK_CLUSTER=dsca-observe-readyfail "$root/scripts/observe-up.sh"
grep -q '^delete$' "$delete_log" || { echo "readiness failure did not invoke kwok delete" >&2; exit 1; }

# Teardown failure is surfaced and preserves owned state for investigation.
mkdir -p "$root/build/observe"
printf 'cluster=dsca-observe-residual\nkubeconfig=%s/build/observe/kubeconfig\n' "$root" >"$root/build/observe/observe-state"
shasum -a 256 "$root/build/observe/observe-state" >"$root/build/observe/observe-state.sha256"
assert_fail env PATH="$fake:/usr/bin:/bin" FAKE_KWOK_DELETE_RC=1 "$root/scripts/observe-down.sh"
[[ -e "$root/build/observe/observe-state" ]] || { echo "state removed after teardown failure" >&2; exit 1; }
rm -f "$root/build/observe/observe-state" "$root/build/observe/observe-state.sha256"

# Tampered state must be rejected without invoking deletion.
printf 'cluster=dsca-observe-tampered\nkubeconfig=%s/build/observe/kubeconfig\n' "$root" >"$root/build/observe/observe-state"
printf 'bad-hash\n' >"$root/build/observe/observe-state.sha256"
delete_log="$tmp/tampered-delete.log"
assert_fail env PATH="$fake:/usr/bin:/bin" FAKE_DELETE_LOG="$delete_log" "$root/scripts/observe-down.sh"
[[ ! -e "$delete_log" ]] || { echo "tampered state invoked delete" >&2; exit 1; }
rm -f "$root/build/observe/observe-state" "$root/build/observe/observe-state.sha256"

echo "observe lifecycle shell tests: PASS"
