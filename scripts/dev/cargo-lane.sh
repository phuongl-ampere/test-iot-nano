#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s <lane> -- <cargo command and arguments>\n' "${0##*/}" >&2
  exit 2
}

[[ "$#" -ge 3 ]] || usage

lane="$1"
shift
[[ "$lane" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || usage
[[ "$1" == "--" ]] || usage
shift
[[ "$#" -gt 0 ]] || usage

root="$(git -C "$(dirname "${BASH_SOURCE[0]}")/../.." rev-parse --show-toplevel)"
root="$(cd "$root" && pwd)"
root_name="$(basename "$root" | tr -cs 'A-Za-z0-9._-' '_')"
root_hash="$(printf '%s' "$root" | git -C "$root" hash-object --stdin)"
worktree_key="${root_name}-${root_hash:0:12}"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}"
target_root="${IOT_NANO_LANE_TARGET_ROOT:-$cache_root/rush-iot-nano/cargo-lanes}"
target_dir="$target_root/$worktree_key/$lane"
log_path="${IOT_NANO_LANE_LOG:-$root/.cargo-lane/timing.log}"

mkdir -p "$target_dir" "$(dirname "$log_path")"

if [[ "${IOT_NANO_USE_SCCACHE:-0}" == "1" && -z "${RUSTC_WRAPPER:-}" ]] && command -v sccache >/dev/null 2>&1; then
  export RUSTC_WRAPPER="$(command -v sccache)"
fi

SECONDS=0
if CARGO_TARGET_DIR="$target_dir" cargo "$@"; then
  status=0
else
  status=$?
fi
elapsed_seconds="$SECONDS"

printf '%s lane=%q worktree=%q target=%q elapsed_seconds=%s exit=%s\n' \
  "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$lane" "$worktree_key" "$target_dir" \
  "$elapsed_seconds" "$status" >>"$log_path"

exit "$status"
