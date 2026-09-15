#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker_bin="${DOCKER_BIN:-docker}"
base_compose="$root/infra/compose.yaml"
timescale_compose="$root/infra/compose.timescale.yaml"
dockerfile="$root/infra/docker/Dockerfile"
migrate_script="$root/infra/monolith/migrate.sh"
rollback_script="$root/infra/monolith/rollback.sh"
rollback_copy_failure_fixture="$root/scripts/fixtures/verify-rollback-timescale-copy-failure.sh"
rollback_coherence_fixture="$root/scripts/fixtures/verify-rollback-coherence.sh"
monolith_main="$root/services/iot-nano-monolith/src/main.rs"
monolith_runtime="$root/services/iot-nano-monolith/src/runtime.rs"

run_script_library() {
  local script="$1"
  local function="$2"
  local path="$3"
  IOT_NANO_MONOLITH_TEST_LIB=1 bash -c 'source "$1"; "$2" "$3"' -- \
    "$script" "$function" "$path"
}

expect_unsafe_path_rejected() {
  local script="$1"
  local function="$2"
  local path="$3"
  if run_script_library "$script" "$function" "$path"; then
    printf '%s accepted unsafe Docker volume path: %s\n' "$script" "$path" >&2
    exit 1
  fi
}

path_mode() {
  stat -f '%Lp' "$1" 2>/dev/null || stat -c '%a' "$1"
}

base_services="$(IOT_DEVICE_TOKEN_VAULT_KEY=topology-test-key-material-at-least-32-bytes \
  "$docker_bin" compose --file "$base_compose" config --services | LC_ALL=C sort)"
if [[ "$base_services" != "iot-nano-monolith" ]]; then
  printf 'base compose must contain only iot-nano-monolith, got:\n%s\n' "$base_services" >&2
  exit 1
fi

timescale_services="$(IOT_DEVICE_TOKEN_VAULT_KEY=topology-test-key-material-at-least-32-bytes \
  TIMESCALE_POSTGRES_PASSWORD=topology-test-password \
  DATABASE_URL=postgres://iot:topology@timescaledb:5432/iot \
  "$docker_bin" compose --file "$base_compose" --file "$timescale_compose" config --services | LC_ALL=C sort)"
expected_timescale_services=$'iot-nano-monolith\ntimescaledb'
if [[ "$timescale_services" != "$expected_timescale_services" ]]; then
  printf 'Timescale compose must contain monolith and timescaledb, got:\n%s\n' "$timescale_services" >&2
  exit 1
fi

rg -n 'iot-nano-(api|core|stream|mqttd)|IOT_NANO_.*_(URL|SECRET)|/internal/' \
  "$base_compose" "$timescale_compose" "$root/infra/monolith" "$dockerfile" && {
  printf 'retired service topology or internal transport configuration found\n' >&2
  exit 1
}

rg -q 'cargo build --release --package iot-nano-monolith' "$dockerfile"
rg -q 'target/release/iot-nano-monolith' "$dockerfile"
rg -q 'IOT_NANO_STORAGE: sqlite' "$base_compose"
rg -q 'IOT_NANO_INTERNAL_DIR: /var/lib/iot-nano/internal' "$base_compose"
rg -q 'IOT_NANO_STORAGE: timescale' "$timescale_compose"
rg -q 'DATABASE_URL:' "$timescale_compose"
test -x "$migrate_script"
test -x "$rollback_script"
test -x "$rollback_copy_failure_fixture"
test -x "$rollback_coherence_fixture"
bash -n \
  "$migrate_script" \
  "$rollback_script" \
  "$rollback_copy_failure_fixture" \
  "$rollback_coherence_fixture"

migrate_only_block="$(awk '
  /if arguments\.migrate_only/ { capture = 1 }
  /if arguments\.bootstrap_admin/ { capture = 0 }
  capture { print }
' "$monolith_main")"
printf '%s\n' "$migrate_only_block" | rg -q 'MonolithConfig::from_env\(\)\?'
printf '%s\n' "$migrate_only_block" | rg -q 'MonolithRuntime::migrate\(&configuration\)'
if printf '%s\n' "$migrate_only_block" | rg -q 'storage_from_env|var_os'; then
  printf 'migrate-only must not bypass complete MonolithConfig validation\n' >&2
  exit 1
fi

runtime_migrate_block="$(awk '
  /pub async fn migrate/ { capture = 1 }
  /pub async fn start/ { capture = 0 }
  capture { print }
' "$monolith_runtime")"
prepare_line="$(printf '%s\n' "$runtime_migrate_block" | nl -ba | rg 'prepare_internal_directory\(&config\.internal_dir\)' | awk '{ print $1 }')"
lock_line="$(printf '%s\n' "$runtime_migrate_block" | nl -ba | rg 'InstanceLock::acquire_blocking' | awk '{ print $1 }')"
platform_line="$(printf '%s\n' "$runtime_migrate_block" | nl -ba | rg 'PlatformStore::open\(&config\.storage\)' | awk '{ print $1 }')"
[[ -n "$prepare_line" && -n "$lock_line" && -n "$platform_line" &&
  "$prepare_line" -lt "$platform_line" && "$lock_line" -lt "$platform_line" ]] || {
  printf 'migrate-only must prepare and lock internal state before platform migration\n' >&2
  exit 1
}

path_test_root="$(mktemp -d "$root/topology-path-test.XXXXXX")"
trap 'rm -rf "$path_test_root"' EXIT
safe_backup_root="$path_test_root/backup-root"
run_script_library "$migrate_script" prepare_backup_root "$safe_backup_root"
[[ -d "$safe_backup_root" && ! -L "$safe_backup_root" && -O "$safe_backup_root" &&
  "$(path_mode "$safe_backup_root")" == 700 ]] || {
  printf 'migration backup root is not owner-only\n' >&2
  exit 1
}
expect_unsafe_path_rejected "$migrate_script" prepare_backup_root "$path_test_root/backup:root"
expect_unsafe_path_rejected "$migrate_script" prepare_backup_root "relative/backup-root"
ln -s "$safe_backup_root" "$path_test_root/symlink-root"
expect_unsafe_path_rejected "$migrate_script" prepare_backup_root "$path_test_root/symlink-root/child"
expect_unsafe_path_rejected "$rollback_script" verify_backup_directory "$path_test_root/backup:root"
expect_unsafe_path_rejected "$rollback_script" verify_backup_directory "relative/backup-root"
expect_unsafe_path_rejected "$rollback_script" verify_backup_directory "$path_test_root/symlink-root"

"$rollback_copy_failure_fixture" "$rollback_script"
"$rollback_coherence_fixture" "$rollback_script"
