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
assert_contains "$help_output" 'seed-demo     Seed a clean existing platform with the Power Monitor demo; does not reset data.'
assert_contains "$help_output" '--system-username NAME'
assert_contains "$help_output" '--public-address ADDR  Default: 0.0.0.0:17180.'
assert_contains "$help_output" '--mqtt-address ADDR    Default: 0.0.0.0:17183.'
assert_contains "$help_output" '--mqtt-tls-address ADDR Default: 0.0.0.0:17184.'
assert_contains "$(<"$installer")" 'public_address="0.0.0.0:17180"'
assert_contains "$(<"$installer")" 'mqtt_address="0.0.0.0:17183"'
assert_contains "$(<"$installer")" 'mqtt_tls_address="0.0.0.0:17184"'
assert_contains "$(<"$installer")" '"name":"Power Meter"'
assert_contains "$(<"$installer")" 'demo_seed()'
assert_contains "$(<"$installer")" 'demo) demo_seed ;;'
assert_contains "$(<"$installer")" 'seed-demo) seed_mode="demo"; seed_platform ;;'
assert_contains "$(<"$installer")" 'http://127.0.0.1:${public_address##*:}'
assert_contains "$(<"$installer")" '/api/v1/management/applications'
assert_contains "$(<"$installer")" '/api/v1/management/profiles/device-profiles'
assert_contains "$(<"$installer")" '/api/v1/management/alert-rules'

printf 'test-install-linux-monolith: ok\n'
