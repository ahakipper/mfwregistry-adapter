#!/usr/bin/env bash
set -eu

# check_no_legacy_globals.sh enforces the post-migration dependency boundary.
# It scans production code and tests alike: no source file may import the
# removed package-global config/logger/notice APIs or the deleted compatibility
# bridge. This keeps a future test helper from silently reintroducing the
# process-wide state that the composition root eliminated.
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

violations=""
append_violation() {
	if [ -n "$violations" ]; then
		violations="$violations\n"
	fi
	violations="${violations}$1"
}

while IFS= read -r path; do
	[ -n "$path" ] || continue

	while IFS=: read -r line _; do
		[ -n "${line:-}" ] || continue
		append_violation "$path:$line:imports removed legacy package-global API"
	done < <(rg -n '^[[:space:]]*([A-Za-z_.][A-Za-z0-9_.]*[[:space:]]+)?"spotter/(config|pkg/log|pkg/notice|internal/infra/legacycompat)(/|")' "$path" || true)

	# These identifiers were the old process-wide entry points. Rejecting them
	# in source prevents stale compatibility examples from becoming regressions.
	while IFS=: read -r line _; do
		[ -n "${line:-}" ] || continue
		append_violation "$path:$line:references removed legacy entry point"
	done < <(rg -n '\b(LoggerInit|InitNoticeClient|NewK8SProvider|NewConsulProvider|NewElector|NewCandidateWithClock)\b' "$path" || true)
done < <(rg --files cmd internal pkg tests tools | rg '\.go$' || true)

if [ -n "$violations" ]; then
	printf '%b\n' "$violations" | sort -u
	exit 1
fi

printf '%s\n' 'legacy-global-boundary: PASS'
