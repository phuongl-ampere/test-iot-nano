#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
base_compose="$root/infra/compose.yaml"
timescale_compose="$root/infra/compose.timescale.yaml"
dockerfile="$root/infra/docker/Dockerfile"
migrate_script="$root/infra/monolith/migrate.sh"
rollback_script="$root/infra/monolith/rollback.sh"

base_services="$(IOT_DEVICE_TOKEN_VAULT_KEY=topology-test-key-material-at-least-32-bytes \
  docker compose --file "$base_compose" config --services | LC_ALL=C sort)"
if [[ "$base_services" != "iot-nano-monolith" ]]; then
  printf 'base compose must contain only iot-nano-monolith, got:\n%s\n' "$base_services" >&2
  exit 1
fi

timescale_services="$(IOT_DEVICE_TOKEN_VAULT_KEY=topology-test-key-material-at-least-32-bytes \
  TIMESCALE_POSTGRES_PASSWORD=topology-test-password \
  DATABASE_URL=postgres://iot:topology@timescaledb:5432/iot \
  docker compose --file "$base_compose" --file "$timescale_compose" config --services | LC_ALL=C sort)"
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
rg -q -- '--migrate-only' "$migrate_script"
rg -q 'ROLLBACK_SQLITE_BACKUP|TIMESCALE_RESTORE_POINT' "$rollback_script"
