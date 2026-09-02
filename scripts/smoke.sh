#!/usr/bin/env bash
# smoke.sh runs the offline binary-level smoke checks documented in
# docs/testing.md section 4.3. Each case runs the spotter binary inside a
# fresh temporary directory (the legacy logger writes app.log and logfiles/
# into the working directory) under a timeout, and asserts both the exit
# code and the required output patterns. The script exits nonzero when any
# case fails and never leaves artifacts in the repository.
#
# Usage: ./scripts/smoke.sh /path/to/spotter

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "usage: $0 /path/to/spotter" >&2
    exit 2
fi

# Resolve the binary to an absolute path: the cases below run inside a
# temporary directory, so a relative path would stop resolving.
SPOTTER_BIN=$1
if [ ! -f "$SPOTTER_BIN" ]; then
    echo "smoke: binary not found: $SPOTTER_BIN" >&2
    exit 2
fi
SPOTTER_BIN=$(cd "$(dirname "$SPOTTER_BIN")" && pwd)/$(basename "$SPOTTER_BIN")

# Repository root, used to copy the etcd TLS fixtures into the sandbox and
# for the final artifact check.
REPO_ROOT=$(cd "$(dirname "$0")/.." && pwd)

# Per-case timeout in seconds (case 5 needs the 5s etcd member-list deadline).
CASE_TIMEOUT=30

WORKDIR=$(mktemp -d "${TMPDIR:-/tmp}/spotter-smoke.XXXXXX")
cleanup() {
    rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

# Case 5 runs the adapter against the preset etcd endpoints with TLS. The
# TLS file paths are relative (./config/certs/etcdtest/...), so the fixtures
# are copied into the sandbox working directory; the etcd endpoints
# themselves stay unreachable offline and the case asserts the resulting
# dial-failure message (docs/testing.md section 7, smoke case 5 exception).
if [ -d "$REPO_ROOT/config/certs" ]; then
    mkdir -p "$WORKDIR/config"
    cp -R "$REPO_ROOT/config/certs" "$WORKDIR/config/certs"
fi

# Output of the case executed last; asserted by assert_output.
LAST_OUTPUT=""

# Total failure counter; the script exits nonzero when any case fails.
FAILED=0

# run_case NUMBER EXPECTED_EXIT DESCRIPTION args...
# Executes the binary inside WORKDIR under a deadline, captures the combined
# output in LAST_OUTPUT, and records a failure when the exit code does not
# match EXPECTED_EXIT.
run_case() {
    local number=$1
    local expected_exit=$2
    local description=$3
    shift 3

    local exit_code
    LAST_OUTPUT=$(cd "$WORKDIR" && run_with_timeout "$CASE_TIMEOUT" "$SPOTTER_BIN" "$@" 2>&1) && exit_code=0 || exit_code=$?

    if [ "$exit_code" -ne "$expected_exit" ]; then
        echo "FAIL case $number: $description"
        echo "     command:    $SPOTTER_BIN $*"
        echo "     exit code:  $exit_code (expected $expected_exit)"
        echo "     output:     $LAST_OUTPUT"
        FAILED=$((FAILED + 1))
        return 1
    fi
    return 0
}

# assert_output NUMBER DESCRIPTION PATTERN
# Records a failure when LAST_OUTPUT does not contain PATTERN. -e keeps
# patterns that start with dashes (for example --providers) safe.
assert_output() {
    local number=$1
    local description=$2
    local pattern=$3

    if ! grep -q -e "$pattern" <<<"$LAST_OUTPUT"; then
        echo "FAIL case $number: $description"
        echo "     missing pattern: $pattern"
        echo "     output:          $LAST_OUTPUT"
        FAILED=$((FAILED + 1))
        return 1
    fi
    return 0
}

# run_with_timeout SECONDS COMMAND args...
# Runs COMMAND under a deadline and returns its exit status, or 124 when the
# deadline expires. Uses the timeout binary when available (Linux, macOS
# with coreutils); otherwise a bash kill -0 polling watchdog, which reaps
# the child promptly and escalates TERM -> KILL on overrun.
run_with_timeout() {
    local timeout_secs=$1
    shift

    if command -v timeout >/dev/null 2>&1; then
        timeout "$timeout_secs" "$@"
        return
    fi

    "$@" &
    local pid=$!

    local deadline=$((SECONDS + timeout_secs))
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            kill -TERM "$pid" 2>/dev/null || true
            sleep 1
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null
            return 124
        fi
        sleep 0.05
    done

    local status=0
    wait "$pid" || status=$?
    return "$status"
}

# Ensure no artifacts leak into the repository during the smoke run. The
# legacy artifacts are app.log files and logfiles/ directories (written by
# the legacy logger into its working directory); both names are gitignored,
# so a plain `git status --porcelain` never reports them. The guard below
# snapshots the artifact state BEFORE case 1 and runs ONE final check AFTER
# the last case: any artifact that appears in between fails the run.
#
# Two complementary listings:
#   - smoke_git_artifacts: `git status --porcelain --ignored` with a
#     pathspec covering app.log anywhere ("**/app.log"), a top-level
#     logfiles/ directory ("logfiles") and the contents of any nested
#     logfiles/ directory ("**/logfiles/**"). The plain "logfiles" spec
#     and the "/**" glob are both required: a bare ":(glob)**/logfiles"
#     matches nothing, because git collapses ignored directories and
#     reports the collapsed entry only when its contents are matched.
#   - smoke_find_artifacts: a find over the tree (excluding .git/ and
#     build/) for app.log files and logfiles/ directories. git cannot see
#     EMPTY ignored directories at all, so the find listing is what makes
#     an empty leaked logfiles/ detectable.
smoke_git_artifacts() {
    git -C "$REPO_ROOT" status --porcelain --ignored -- \
        ':(glob)**/app.log' 'logfiles' ':(glob)**/logfiles/**' 2>/dev/null || true
}

smoke_find_artifacts() {
    find "$REPO_ROOT" \
        \( -path "$REPO_ROOT/.git" -o -path "$REPO_ROOT/build" \) -prune -o \
        \( -name app.log -o -name logfiles -type d \) -print 2>/dev/null | sort || true
}

# check_new_artifacts diffs the current artifact listings against the
# baselines captured before case 1; it reports only artifacts created by
# this smoke run (pre-existing artifacts are in the baseline).
check_new_artifacts() {
    local new_git new_find
    new_git=$(comm -13 <(printf '%s\n' "$BASELINE_GIT_ARTIFACTS" | sort) \
                        <(smoke_git_artifacts | sort))
    new_find=$(comm -13 <(printf '%s\n' "$BASELINE_FIND_ARTIFACTS" | sort) \
                        <(smoke_find_artifacts))
    if [ -n "$new_git$new_find" ]; then
        echo "FAIL: smoke run left new artifacts in the repository:" >&2
        [ -n "$new_git" ] && printf '     %s\n' $new_git >&2
        [ -n "$new_find" ] && printf '     %s\n' $new_find >&2
        FAILED=$((FAILED + 1))
    fi
}

# Baseline: captured once, BEFORE any case runs.
BASELINE_GIT_ARTIFACTS=$(smoke_git_artifacts)
BASELINE_FIND_ARTIFACTS=$(smoke_find_artifacts)

echo "== smoke: $SPOTTER_BIN =="

# Case 1: root help shows usage, the command list and the adapter command
# (the command list prints the adapter short description).
run_case 1 0 "spotter -h shows usage, commands and the adapter command" -h &&
    assert_output 1 "spotter -h shows usage" "Usage:" &&
    assert_output 1 "spotter -h shows the command list" "Available Commands" &&
    assert_output 1 "spotter -h lists the adapter command" "Run the instance adapter" &&
    echo "PASS case 1: spotter -h shows usage, available commands and the adapter command"

# Case 2: adapter help shows the command description and its flags.
run_case 2 0 "spotter adapter -h describes the adapter and its flags" adapter -h &&
    assert_output 2 "spotter adapter -h describes the command" "The adapter command is the main entry point" &&
    assert_output 2 "spotter adapter -h documents --providers" "--providers" &&
    assert_output 2 "spotter adapter -h documents --leader-elect" "--leader-elect" &&
    echo "PASS case 2: spotter adapter -h describes the command, --providers and --leader-elect"

# Case 3: invalid env fails fast with exit 1.
run_case 3 1 "spotter adapter -e badenv exits 1 on invalid env" adapter -e badenv &&
    assert_output 3 "spotter adapter -e badenv announces startup" "starting adapter" &&
    assert_output 3 "spotter adapter -e badenv reports the invalid env" "invalid env param" &&
    echo "PASS case 3: spotter adapter -e badenv prints starting adapter and invalid env param, exit 1"

# Case 4: no providers configured.
run_case 4 1 "spotter adapter -e test exits 1 when no providers are set" adapter -e test &&
    assert_output 4 "spotter adapter without --providers reports the missing providers" "no providers configured" &&
    echo "PASS case 4: spotter adapter -e test without --providers fails with no providers configured"

# Case 5: unknown provider name; offline etcd dial fails inside the elector.
# This case deliberately hits the 5s etcd member-list deadline (the one
# offline exception documented in section 7) and asserts the message; the
# command prints the error and returns, so the exit code is 0.
run_case 5 0 "spotter adapter with a bad provider reports the etcd dial failure" adapter -e test -r badprovider -t=false &&
    assert_output 5 "spotter adapter with a bad provider reports the etcd dial failure" "connect to etcd server failed" &&
    echo "PASS case 5: spotter adapter with a bad provider reports connect to etcd server failed, exit 0"

# Final artifact check: one check after the last case, diffed against the
# baseline captured before case 1 (see smoke_git_artifacts /
# smoke_find_artifacts above).
check_new_artifacts

if [ "$FAILED" -ne 0 ]; then
    echo "== smoke: $FAILED case(s) failed ==" >&2
    exit 1
fi

echo "== smoke: all 5 cases passed =="
