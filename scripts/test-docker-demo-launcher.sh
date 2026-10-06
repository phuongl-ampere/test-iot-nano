#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
launcher="$root/install/docker/start-demo.sh"
compose_file="$root/install/docker/compose.yaml"

fail() {
  printf 'test-docker-demo-launcher: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local text="$1" expected="$2"
  [[ "$text" == *"$expected"* ]] || fail "expected launcher to contain: $expected"
}

[[ -x "$launcher" ]] || fail "launcher must be executable: $launcher"
bash -n "$launcher"

contents="$(<"$launcher")"
compose_contents="$(<"$compose_file")"
assert_contains "$contents" 'docker compose build'
assert_contains "$contents" 'IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME=systemadmin'
assert_contains "$contents" 'IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD=systemadmin'
assert_contains "$contents" 'IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS=true'
assert_contains "$contents" 'iot-nano --bootstrap-system'
assert_contains "$contents" 'docker compose up -d'
assert_contains "$contents" 'http://127.0.0.1:17180/readyz'
assert_contains "$contents" 'seed-demo'
assert_contains "$compose_contents" 'IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS: "${IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS:-false}"'

printf 'test-docker-demo-launcher: ok\n'
