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
  infra/dev/api.env
  infra/dev/iot-nano-core.env
  infra/dev/iot-nano-mqttd.env
  infra/dev/iot-nano-stream.env
  infra/systemd/iot-nano-api.service
  infra/systemd/iot-nano-core.service
  infra/systemd/iot-nano-mqttd.service
  infra/systemd/iot-nano-stream.service
  scripts/rpc-e2e.py
)

standalone_installer="install-mqttd-standalone.sh"
standalone_unit="iot-nano-mqttd-standalone.service"
legacy_binary="iot-nano-api"

copy_file() {
  local relative_path="$1"

  mkdir -p "$(dirname "$fixture/$relative_path")"
  cp -p "$root/$relative_path" "$fixture/$relative_path"
}

populate_fixture() {
  local relative_path

  rm -rf "$fixture"
  mkdir -p "$fixture"
  for relative_path in "${expected_files[@]}"; do
    copy_file "$relative_path"
  done
  copy_file "infra/systemd/$standalone_unit"
  copy_file "scripts/$standalone_installer"
  copy_file scripts/test-verify-no-legacy-runtime.sh
  copy_file scripts/verify-no-legacy-runtime.sh
  copy_file scripts/verify-failures.sh
  copy_file scripts/verify-monolith-topology.sh
  mkdir -p "$fixture/scripts/fixtures"
}

assert_success() {
  local case_name="$1"
  local output

  if ! output="$(IOT_NANO_VERIFY_ROOT="$fixture" "$verifier" 2>&1)"; then
    printf 'verifier rejected a valid fixture (%s):\n%s\n' "$case_name" "$output" >&2
    exit 1
  fi
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

write_fixture_file() {
  local relative_path="$1"
  local content="$2"

  mkdir -p "$(dirname "$fixture/$relative_path")"
  printf '%s\n' "$content" >"$fixture/$relative_path"
}

permission_mode() {
  local path="$1"

  if stat -c '%a' "$path" >/dev/null 2>&1; then
    stat -c '%a' "$path"
  else
    stat -f '%Lp' "$path"
  fi
}

populate_fixture
assert_success 'clean monolith fixture with retained standalone assets'

write_fixture_file infra/monolith/injected-raw.sh \
  "# retired binary literal\n$legacy_binary --serve"
assert_failure_contains 'retired binary literal found in monolith deployment paths'
populate_fixture

write_fixture_file infra/monolith/injected-quoted.sh \
  "exec \"$legacy_binary\" --serve"
assert_failure_contains 'retired binary literal found in monolith deployment paths'
populate_fixture

write_fixture_file infra/monolith/injected-escaped.sh \
  'exec iot\-nano\-api --serve'
assert_failure_contains 'retired binary literal found in monolith deployment paths'
populate_fixture

write_fixture_file infra/monolith/injected-continued.sh \
  $'exec iot-nano-\\\napi --serve'
assert_failure_contains 'retired binary literal found in monolith deployment paths'
populate_fixture

write_fixture_file scripts/arbitrary-deployment-tool.sh \
  "exec $legacy_binary --serve"
assert_failure_contains 'retired binary literal found in monolith deployment paths'
populate_fixture

write_fixture_file infra/systemd/arbitrary-deployment-unit.service \
  "ExecStart=/opt/rush-iot-nano/$legacy_binary"
assert_failure_contains 'retired binary literal found in monolith deployment paths'
populate_fixture

write_fixture_file services/iot-nano-monolith/src/injected-route.rs \
  'const RETIRED: &str = "/internal/v1";'
assert_failure_contains 'legacy runtime references found in monolith source paths'
populate_fixture

write_fixture_file services/iot-nano-monolith/src/injected-header.rs \
  'const RETIRED: &str = "x-iot-nano-legacy";'
assert_failure_contains 'legacy runtime references found in monolith source paths'
populate_fixture

write_fixture_file services/iot-nano-monolith/src/injected-env.rs \
  'IOT_NANO_CORE_URL=http://127.0.0.1:8081'
assert_failure_contains 'legacy runtime references found in monolith source paths'
populate_fixture

printf '%s\n' 'retired command' >"$fixture/retired-command"
ln -s ../retired-command "$fixture/scripts/legacy-link"
assert_failure_contains 'symlinked production/deployment path found'
populate_fixture

mv "$fixture/services/iot-nano-monolith" \
  "$fixture/services/iot-nano-monolith-target"
ln -s iot-nano-monolith-target "$fixture/services/iot-nano-monolith"
assert_failure_contains 'symlinked production/deployment path found'
populate_fixture

chmod 000 "$fixture/infra/compose.yaml"
assert_failure_contains 'expected production/deployment path is missing or unreadable'
chmod "$(permission_mode "$root/infra/compose.yaml")" "$fixture/infra/compose.yaml"
populate_fixture

mkdir "$fixture/infra/monolith/unreadable"
write_fixture_file infra/monolith/unreadable/injected.sh 'true'
chmod 000 "$fixture/infra/monolith/unreadable"
assert_failure_contains 'unreadable production/deployment path found'
chmod 700 "$fixture/infra/monolith/unreadable"
populate_fixture

rm "$fixture/infra/compose.yaml"
assert_failure_contains 'expected production/deployment path is missing or unreadable'
populate_fixture

for retired_path in "${retired_paths[@]}"; do
  write_fixture_file "$retired_path" 'retired deployment asset'
  assert_failure_contains 'retired legacy deployment asset remains'
  populate_fixture
done

printf 'exec "$root/scripts/%s"\n' "$standalone_installer" \
  >>"$fixture/scripts/install-raspberry-pi.sh"
assert_failure_contains 'standalone MQTTD package referenced by monolith deployment'
populate_fixture

printf '\n[dev-dependencies]\n%s = { path = "../../services/%s" }\n' \
  "$legacy_binary" "$legacy_binary" \
  >>"$fixture/services/iot-nano-monolith/Cargo.toml"
assert_success 'Cargo dependency allowance'
populate_fixture

installer_config_block="$(awk '
  /if \[ ! -e "\$config_path" \]; then/ { capture = 1 }
  capture { print }
  capture && /^fi$/ { exit }
' "$root/scripts/$standalone_installer")"
if [[ "$installer_config_block" != *'--owner root --group iot --mode 0640'* ]]; then
  printf 'standalone installer must create config as root:iot mode 0640\n' >&2
  exit 1
fi
if ! rg -q 'sudo chown root:iot "\$config_path"' "$root/scripts/$standalone_installer" ||
  ! rg -q 'sudo chmod 0640 "\$config_path"' "$root/scripts/$standalone_installer"; then
  printf 'standalone installer must normalize existing config ownership and mode\n' >&2
  exit 1
fi
