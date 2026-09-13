#!/usr/bin/env bash
set -eu

# check_no_legacy_globals.sh enforces the migration boundary around the
# pre-composition package globals. It intentionally scans production Go files
# only; tests may import compatibility packages to exercise the old contract.
repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

violations=""

is_exempt() {
	case "$1" in
		internal/infra/legacycompat/*|config/*|pkg/log/*|pkg/notice/*|*_test.go|pkg/providers/k8s/legacy_compat.go|pkg/providers/k8s/legacy_provider_compat.go|pkg/providers/consul/legacy_compat.go|pkg/distribute/election/legacy_compat.go|pkg/worker/legacy_compat.go)
			return 0
			;;
		*)
			return 1
			;;
	esac
}

	append_violation() {
	if [ -n "$violations" ]; then
		violations="$violations\n"
	fi
	violations="${violations}$1"
}

while IFS= read -r path; do
	[ -n "$path" ] || continue
	is_exempt "$path" && continue

	# Only dedicated compatibility wrappers and tests may import legacycompat.
	while IFS=: read -r line _; do
		[ -n "${line:-}" ] || continue
		append_violation "$path:$line:import legacycompat outside compatibility boundary"
	done < <(rg -n '"spotter/internal/infra/legacycompat"' "$path" || true)

	# Imports are reported from the import declaration itself. This covers
	# both ordinary package imports and explicit aliases in grouped imports.
	while IFS=: read -r line _; do
		[ -n "${line:-}" ] || continue
		append_violation "$path:$line:import legacy global package"
	done < <(rg -n '^[[:space:]]*([A-Za-z_.][A-Za-z0-9_.]*[[:space:]]+)?"spotter/(config|pkg/log|pkg/notice)"' "$path" || true)

	# Resolve aliases from this file's legacy imports, then reject assignment
	# through those aliases. Restricting the search to imported aliases avoids
	# mistaking ordinary local Config fields for writes to package globals.
	while IFS=: read -r line import_text; do
		[ -n "${line:-}" ] || continue
		package_path=$(printf '%s\n' "$import_text" | sed -nE 's/.*"(spotter\/(config|pkg\/log|pkg\/notice))".*/\1/p')
		[ -n "$package_path" ] || continue
		alias=$(printf '%s\n' "$import_text" | sed -nE 's/^[[:space:]]*([A-Za-z_.][A-Za-z0-9_.]*)[[:space:]]+".*/\1/p')
		if [ -z "$alias" ]; then
			case "$package_path" in
				spotter/config) alias=config ;;
				spotter/pkg/log) alias=log ;;
				spotter/pkg/notice) alias=notice ;;
			esac
		fi
		[ -n "${alias:-}" ] || continue
		while IFS=: read -r assign_line _; do
			[ -n "${assign_line:-}" ] || continue
			append_violation "$path:$assign_line:assign legacy global package symbol"
		done < <(rg -n "(^|[^[:alnum:]_])${alias}\\.[A-Z][A-Za-z0-9_]*([[:space:]]*(\\+=|-=|\\+\\+|--|=([^=]|$)))" "$path" || true)
	done < <(rg -n '^[[:space:]]*([A-Za-z_.][A-Za-z0-9_.]*[[:space:]]+)?"spotter/(config|pkg/log|pkg/notice)"' "$path" || true)
done < <(rg --files cmd internal pkg config tools | rg '\.go$' | rg -v '_test\.go$' || true)

if [ -n "$violations" ]; then
	printf '%b\n' "$violations" | sort -u
	exit 1
fi

printf '%s\n' 'legacy-global-boundary: PASS'
