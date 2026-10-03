#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
installer="$root/scripts/install-linux-monolith.sh"

fail() {
  printf 'test-install-linux-monolith: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local text="$1"
  local expected="$2"

  [[ "$text" == *"$expected"* ]] || fail "expected output to contain $expected"
}

[[ -x "$installer" ]] || fail 'installer must be executable'

help_output="$(bash "$installer" --help)"
assert_contains "$help_output" '--seed-mode MODE       starter (default), demo, or none.'
assert_contains "$(<"$installer")" 'demo_seed()'
assert_contains "$(<"$installer")" 'demo) demo_seed ;;'
assert_contains "$(<"$installer")" 'http://127.0.0.1:${public_address##*:}'
assert_contains "$(<"$installer")" '/api/v1/management/applications'
assert_contains "$(<"$installer")" '/api/v1/management/profiles/device-profiles'
assert_contains "$(<"$installer")" '/api/v1/management/alert-rules'

printf 'test-install-linux-monolith: ok\n'
