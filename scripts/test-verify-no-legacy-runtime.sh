#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="$root/scripts/verify-no-legacy-runtime.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

expected_files=(
  infra/compose.yaml
  infra/compose.timescale.yaml
  infra/docker/Dockerfile
  infra/monolith/monolith.env.example
  infra/monolith/migrate.sh
  infra/monolith/rollback.sh
  infra/systemd/iot-nano-monolith.service
  scripts/e2e-local.sh
  scripts/e2e-monolith.sh
  scripts/install-raspberry-pi.sh
  services/iot-nano-monolith/Cargo.toml
  services/iot-nano-monolith/src/adapters.rs
  services/iot-nano-monolith/src/cache.rs
  services/iot-nano-monolith/src/config.rs
  services/iot-nano-monolith/src/lib.rs
  services/iot-nano-monolith/src/main.rs
  services/iot-nano-monolith/src/management.rs
  services/iot-nano-monolith/src/readiness.rs
  services/iot-nano-monolith/src/runtime.rs
)

retired_paths=(
  infra/systemd/iot-nano-api.service
  infra/systemd/iot-nano-core.service
  infra/systemd/iot-nano-mqttd.service
  infra/systemd/iot-nano-stream.service
  scripts/rpc-e2e.py
)

copy_file() {
  local relative_path="$1"
  mkdir -p "$(dirname "$fixture/$relative_path")"
  cp "$root/$relative_path" "$fixture/$relative_path"
}

populate_fixture() {
  local relative_path

  for relative_path in "${expected_files[@]}"; do
    copy_file "$relative_path"
  done
  mkdir -p "$fixture/services/iot-nano-mqttd" "$fixture/infra/systemd"
  cp -R "$root/services/iot-nano-mqttd/." "$fixture/services/iot-nano-mqttd/"
  copy_file infra/systemd/iot-nano-mqttd-standalone.service
}

assert_failure_contains() {
  local expected_reason="$1"
  local output

  if output="$(IOT_NANO_VERIFY_ROOT="$fixture" "$verifier" 2>&1)"; then
    printf 'verifier accepted an invalid fixture (%s)\n' "$expected_reason" >&2
    exit 1
  fi
  if [[ "$output" != *"$expected_reason"* ]]; then
    printf 'verifier failed for the wrong reason (%s):\n%s\n' \
      "$expected_reason" "$output" >&2
    exit 1
  fi
}

populate_fixture
if ! IOT_NANO_VERIFY_ROOT="$fixture" "$verifier"; then
  printf 'verifier rejected a clean monolith fixture with retained standalone assets\n' >&2
  exit 1
fi

for retired_path in "${retired_paths[@]}"; do
  mkdir -p "$(dirname "$fixture/$retired_path")"
  printf '%s\n' 'retired deployment asset' >"$fixture/$retired_path"
  assert_failure_contains 'retired legacy deployment asset remains'
  rm "$fixture/$retired_path"
done

printf '%s\n' 'exec "$root/scripts/install-mqttd-standalone.sh"' \
  >>"$fixture/scripts/e2e-local.sh"
assert_failure_contains 'legacy runtime references found'
populate_fixture

printf '%s\n' 'const INJECTED_LEGACY_REFERENCE: &str = "x-iot-nano-legacy";' \
  >>"$fixture/services/iot-nano-monolith/src/main.rs"
assert_failure_contains 'legacy runtime references found'
populate_fixture

for relative_path in "${expected_files[@]}"; do
  rm "$fixture/$relative_path"
  assert_failure_contains \
    "expected production/deployment path is missing or unreadable: $fixture/$relative_path"
  populate_fixture

  chmod 000 "$fixture/$relative_path"
  assert_failure_contains \
    "expected production/deployment path is missing or unreadable: $fixture/$relative_path"
  chmod "$(stat -f '%Lp' "$root/$relative_path")" "$fixture/$relative_path"
done
