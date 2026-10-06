#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$root/install/docker/compose.yaml"
env_example="$root/install/docker/.env.example"
dockerfile="$root/install/docker/Dockerfile"

fail() {
  printf 'test-docker-install: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  local text="$1"
  local expected="$2"

  [[ "$text" == *"$expected"* ]] || fail "expected output to contain $expected"
}

[[ -f "$compose_file" ]] || fail 'compose file must exist'
[[ -f "$env_example" ]] || fail '.env.example must exist'
[[ -f "$dockerfile" ]] || fail 'Dockerfile must exist'

compose="$(<"$compose_file")"
env="$(<"$env_example")"
dockerfile_contents="$(<"$dockerfile")"
assert_contains "$compose" '"${HTTP_PORT:-17180}:18080"'
assert_contains "$compose" '"${MQTT_PORT:-17183}:1883"'
assert_contains "$compose" '"${MQTT_TLS_PORT:-17184}:8883"'
assert_contains "$env" 'HTTP_PORT=17180'
assert_contains "$env" 'MQTT_PORT=17183'
assert_contains "$env" 'MQTT_TLS_PORT=17184'
[[ "$dockerfile_contents" != *' + &&'* ]] || fail 'Dockerfile must use valid shell command chains'

printf 'test-docker-install: ok\n'
