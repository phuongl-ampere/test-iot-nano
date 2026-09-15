#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="$root/scripts/verify-no-legacy-runtime.sh"
fixture="$(mktemp -d)"
installer_fixture=""
trap 'rm -rf "$fixture" "$installer_fixture"' EXIT

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

assert_log_contains() {
  local expected_line="$1"
  local log_path="$2"

  if ! rg -Fqx -- "$expected_line" "$log_path"; then
    printf 'installer did not request expected privilege command: %s\n' \
      "$expected_line" >&2
    exit 1
  fi
}

assert_log_excludes() {
  local unexpected_line="$1"
  local log_path="$2"

  if rg -Fqx -- "$unexpected_line" "$log_path"; then
    printf 'installer requested an unexpected privilege command: %s\n' \
      "$unexpected_line" >&2
    exit 1
  fi
}

create_fake_privilege_command() {
  local command_name="$1"
  local command_path="$2/$command_name"

  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'printf "%s %s\n" "$(basename "$0")" "$*" >>"$FAKE_COMMAND_LOG"' \
    '[[ "${FAKE_INSTALL_DRY_RUN:-0}" != "1" ]] || exit 0' \
    'case "$(basename "$0")" in' \
    '  sudo|fake-sudo)' \
    '    [[ "${1:-}" == "-v" ]] || "$@"' \
    '    ;;' \
    '  getent|id|fake-getent|fake-id)' \
    '    exit 1' \
    '    ;;' \
    '  install|fake-install)' \
    '    if [[ "${1:-}" == "-d" ]]; then' \
    '      mkdir -p "${!#}"' \
    '    else' \
    '      destination="${!#}"' \
    '      source_path="${@: -2:1}"' \
    '      mkdir -p "$(dirname "$destination")"' \
    '      cp "$source_path" "$destination"' \
    '    fi' \
    '    ;;' \
    '  chmod|fake-chmod)' \
    '    /bin/chmod "$@"' \
    '    ;;' \
    'esac' >"$command_path"
  chmod 0755 "$command_path"
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

for e2e_script in scripts/e2e-local.sh scripts/e2e-monolith.sh; do
  printf '\nexec %s --serve\n' "$legacy_binary" >>"$fixture/$e2e_script"
  assert_failure_contains 'retired binary literal found in monolith deployment paths'
  populate_fixture
done

for e2e_script in scripts/e2e-local.sh scripts/e2e-monolith.sh; do
  printf '\n# %s\n' "$standalone_installer" >>"$fixture/$e2e_script"
  assert_failure_contains 'standalone MQTTD package referenced by monolith deployment'
  populate_fixture
done

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

installer_fixture="$(mktemp -d)"
fake_bin="$installer_fixture/bin"
fake_command_log="$installer_fixture/privilege.log"
installer_root="$installer_fixture/root"
template_path="$installer_root/services/iot-nano-mqttd/config/standalone.toml"
config_path="$installer_fixture/etc/rush-iot-nano/iot-nano-mqttd.toml"
config_dir="$(dirname "$config_path")"
state_path="$installer_fixture/var/lib/iot-nano-mqttd"
service_path="$installer_fixture/etc/systemd/system/iot-nano-mqttd-standalone.service"
install_path="$installer_fixture/opt/rush-iot-nano"
mkdir -p \
  "$fake_bin" \
  "$(dirname "$template_path")" \
  "$(dirname "$service_path")" \
  "$installer_root/infra/systemd" \
  "$installer_root/target/release"
cp "$root/services/iot-nano-mqttd/config/standalone.toml" "$template_path"
cp "$root/infra/systemd/$standalone_unit" \
  "$installer_root/infra/systemd/$standalone_unit"
printf '%s\n' 'fake mqttd binary' >"$installer_root/target/release/iot-nano-mqttd"
for fake_command in \
  fake-sudo fake-cargo fake-getent fake-id fake-groupadd fake-useradd \
  fake-install fake-chown fake-chmod fake-systemctl; do
  create_fake_privilege_command "$fake_command" "$fake_bin"
done
for command_name in \
  sudo cargo getent id groupadd useradd install chown chmod systemctl; do
  ln -s "fake-$command_name" "$fake_bin/$command_name"
done

PATH="$fake_bin:$PATH" \
  FAKE_COMMAND_LOG="$fake_command_log" \
  FAKE_INSTALL_DRY_RUN=1 \
  bash -c 'source "$1"' -- "$root/scripts/$standalone_installer"
if [[ -s "$fake_command_log" ]]; then
  printf 'sourcing the standalone installer invoked production commands\n' >&2
  exit 1
fi

run_standalone_installer() {
  PATH="$fake_bin:$PATH" \
    FAKE_COMMAND_LOG="$fake_command_log" \
    IOT_NANO_MQTTD_ROOT="$installer_root" \
    IOT_NANO_MQTTD_CONFIG_DIR="$config_dir" \
    IOT_NANO_MQTTD_CONFIG_PATH="$config_path" \
    IOT_NANO_MQTTD_STATE_PATH="$state_path" \
    IOT_NANO_MQTTD_SERVICE_PATH="$service_path" \
    IOT_NANO_MQTTD_INSTALL_PATH="$install_path" \
    IOT_NANO_MQTTD_SUDO_BIN=fake-sudo \
    IOT_NANO_MQTTD_CARGO_BIN=fake-cargo \
    IOT_NANO_MQTTD_GETENT_BIN=fake-getent \
    IOT_NANO_MQTTD_ID_BIN=fake-id \
    IOT_NANO_MQTTD_GROUPADD_BIN=fake-groupadd \
    IOT_NANO_MQTTD_USERADD_BIN=fake-useradd \
    IOT_NANO_MQTTD_INSTALL_BIN=fake-install \
    IOT_NANO_MQTTD_CHOWN_BIN=fake-chown \
    IOT_NANO_MQTTD_CHMOD_BIN=fake-chmod \
    IOT_NANO_MQTTD_SYSTEMCTL_BIN=fake-systemctl \
    bash -c 'source "$1"; install_mqttd_standalone' -- \
    "$root/scripts/$standalone_installer" >/dev/null
}

: >"$fake_command_log"
run_standalone_installer

if ! cmp -s "$template_path" "$config_path"; then
  printf 'standalone installer did not create config from the template\n' >&2
  exit 1
fi
assert_log_contains \
  "fake-sudo fake-install --owner root --group iot --mode 0640 $template_path $config_path" \
  "$fake_command_log"
assert_log_contains \
  "fake-install --owner root --group iot --mode 0640 $template_path $config_path" \
  "$fake_command_log"
assert_log_contains "fake-sudo fake-chown root:iot $config_path" "$fake_command_log"
assert_log_contains "fake-chown root:iot $config_path" "$fake_command_log"
assert_log_contains "fake-sudo fake-chmod 0640 $config_path" "$fake_command_log"
assert_log_contains "fake-chmod 0640 $config_path" "$fake_command_log"

printf '%s\n' 'existing standalone configuration' >"$config_path"
chmod 0600 "$config_path"
cp "$config_path" "$installer_fixture/original.toml"
: >"$fake_command_log"
run_standalone_installer

if ! cmp -s "$installer_fixture/original.toml" "$config_path"; then
  printf 'standalone installer changed existing config content\n' >&2
  exit 1
fi
if [[ "$(permission_mode "$config_path")" != "640" ]]; then
  printf 'standalone installer did not normalize existing config mode\n' >&2
  exit 1
fi
assert_log_excludes \
  "fake-sudo fake-install --owner root --group iot --mode 0640 $template_path $config_path" \
  "$fake_command_log"
assert_log_contains "fake-sudo fake-chown root:iot $config_path" "$fake_command_log"
assert_log_contains "fake-chown root:iot $config_path" "$fake_command_log"
assert_log_contains "fake-sudo fake-chmod 0640 $config_path" "$fake_command_log"
assert_log_contains "fake-chmod 0640 $config_path" "$fake_command_log"
