#!/usr/bin/env bash
set -euo pipefail

# Fresh Linux installer for the IoT Nano monolith. It is intended for a
# Debian/Ubuntu host with systemd (including Raspberry Pi OS 64-bit).

script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_root="/opt/rush-iot-nano"
config_root="/etc/iot-nano"
data_root="/var/lib/iot-nano"
service_name="iot-nano-monolith.service"
service_user="iotnano"
seed_mode="starter"
public_address="0.0.0.0:17180"
mqtt_address="0.0.0.0:17183"
mqtt_tls_address="0.0.0.0:17184"
web_https_enabled="false"
powermonitor_url="http://localhost:3002"
tls_common_name="iot-nano"
system_username="systemadmin"
system_password="systemadmin"
tenant_slug="tenant"
tenant_username="tenant"
tenant_password="tenant"
user1_username="user1"
user1_password="user1"
user2_username="user2"
user2_password="user2"

usage() {
  cat <<'EOF'
Usage: sudo ./scripts/install-linux-monolith.sh [command] [options]

Commands:
  install       Install, bootstrap, start, smoke-test, and optionally seed (default).
  seed-demo     Seed a clean existing platform with the Power Monitor demo; does not reset data.
  status        Show service status and health.
  smoke-test    Check public health/ready endpoints.
  backup        Stop, archive platform plus internal state, then restart.
  uninstall     Disable service and remove binary/config; preserves data by default.

Options:
  --seed-mode MODE       starter (default), demo, or none.
  --public-address ADDR  Default: 0.0.0.0:17180.
  --mqtt-address ADDR    Default: 0.0.0.0:17183.
  --mqtt-tls-address ADDR Default: 0.0.0.0:17184.
  --system-username NAME System Account username used by seed-demo.
  --system-password VALUE System Account password used by seed-demo.
  --tenant-slug SLUG     Tenant slug used by seed-demo.
  --tenant-username NAME Tenant Account username used by seed-demo.
  --tenant-password VALUE Tenant Account password used by seed-demo.
  --user1-username NAME  Demo owner username used by seed-demo.
  --user1-password VALUE Demo owner password used by seed-demo.
  --user2-username NAME  Demo viewer username used by seed-demo.
  --user2-password VALUE Demo viewer password used by seed-demo.
  --powermonitor-url URL Launch URL seeded for Power Monitor demo mode.
  --yes                  Skip destructive uninstall confirmation.
  --purge-data           With uninstall, remove /var/lib/iot-nano.

Default lab credentials: systemadmin/systemadmin, tenant/tenant,
user1/user1, user2/user2. Override interactively during install.
EOF
}

require_root() {
  if [[ "$(id -u)" != "0" ]]; then
    printf '%s\n' 'Run this command with sudo.' >&2
    exit 1
  fi
}

prompt() {
  local label="$1" variable="$2" value
  read -r -p "$label [$variable]: " value || true
  [[ -n "$value" ]] && printf '%s' "$value" || printf '%s' "$variable"
}

prompt_install_values() {
  printf '%s\n' 'Press Enter to use each displayed default.'
  public_address="$(prompt 'Public HTTP address' "$public_address")"
  mqtt_address="$(prompt 'MQTT plaintext address' "$mqtt_address")"
  mqtt_tls_address="$(prompt 'MQTT TLS address' "$mqtt_tls_address")"
  web_https_enabled="$(prompt 'HTTPS enabled for browser cookies: true or false' "$web_https_enabled")"
  seed_mode="$(prompt 'Seed mode: starter, demo, or none' "$seed_mode")"
  powermonitor_url="$(prompt 'Power Monitor launch URL' "$powermonitor_url")"
  system_username="$(prompt 'System username' "$system_username")"
  system_password="$(prompt 'System password' "$system_password")"
  tenant_slug="$(prompt 'Tenant slug' "$tenant_slug")"
  tenant_username="$(prompt 'Tenant username' "$tenant_username")"
  tenant_password="$(prompt 'Tenant password' "$tenant_password")"
  user1_username="$(prompt 'First user username' "$user1_username")"
  user1_password="$(prompt 'First user password' "$user1_password")"
  user2_username="$(prompt 'Second user username' "$user2_username")"
  user2_password="$(prompt 'Second user password' "$user2_password")"
  case "$seed_mode" in starter|demo|none) ;; *) printf '%s\n' 'Seed mode must be starter, demo, or none.' >&2; exit 2;; esac
  case "$web_https_enabled" in true|false|1|0|on|off) ;; *) printf '%s\n' 'HTTPS mode must be true/false, 1/0, or on/off.' >&2; exit 2;; esac
}

install_dependencies() {
  command -v apt-get >/dev/null || {
    printf '%s\n' 'Only Debian/Ubuntu/Raspberry Pi OS is supported by this installer.' >&2
    exit 2
  }
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential ca-certificates curl jq openssl pkg-config libssl-dev
  if ! command -v cargo >/dev/null; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y cargo rustc
  fi
}

install_paths() {
  id "$service_user" >/dev/null 2>&1 || useradd --system --user-group \
    --home-dir "$data_root" --shell /usr/sbin/nologin "$service_user"
  install -d -o root -g root -m 0755 "$install_root" "$config_root"
  install -d -o "$service_user" -g "$service_user" -m 0700 \
    "$data_root" "$data_root/platform" "$data_root/internal"
  install -d -o "$service_user" -g "$service_user" -m 0700 /run/tls
}

build_and_install_binary() {
  cargo build --manifest-path "$script_root/Cargo.toml" --release --package iot-nano-monolith
  install -o root -g root -m 0755 \
    "$script_root/target/release/iot-nano-monolith" "$install_root/iot-nano-monolith"
  install -o root -g root -m 0644 \
    "$script_root/infra/systemd/iot-nano-monolith.service" "/etc/systemd/system/$service_name"
}

write_runtime_material() {
  if [[ ! -f /run/tls/mqtt-key.pem || ! -f /run/tls/mqtt-cert.pem ]]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=$tls_common_name" \
      -keyout /run/tls/mqtt-key.pem -out /run/tls/mqtt-cert.pem
    chown "$service_user:$service_user" /run/tls/mqtt-key.pem /run/tls/mqtt-cert.pem
    chmod 0600 /run/tls/mqtt-key.pem
    chmod 0644 /run/tls/mqtt-cert.pem
  fi
  local vault_key
  vault_key="$(openssl rand -base64 48 | tr -d '\n')"
  umask 077
  cat >"$config_root/monolith.env" <<EOF
IOT_NANO_STORAGE=sqlite
IOT_NANO_SQLITE_PATH=$data_root/platform/platform.sqlite
IOT_NANO_INTERNAL_DIR=$data_root/internal
IOT_NANO_TLS_CERT_PATH=/run/tls/mqtt-cert.pem
IOT_NANO_TLS_KEY_PATH=/run/tls/mqtt-key.pem
IOT_NANO_HTTP_ADDRESS=$public_address
IOT_NANO_MQTT_TCP_ADDRESS=$mqtt_address
IOT_NANO_MQTT_TLS_ADDRESS=$mqtt_tls_address
IOT_NANO_HTTPS_ENABLED=$web_https_enabled
IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS=true
IOT_DEVICE_TOKEN_VAULT_KEY=$vault_key
EOF
  chown root:"$service_user" "$config_root/monolith.env"
  chmod 0640 "$config_root/monolith.env"
}

bootstrap_system() {
  systemctl stop "$service_name" 2>/dev/null || true
  runuser -u "$service_user" -- env \
    IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME="$system_username" \
    IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD="$system_password" \
    bash -c "set -a; . '$config_root/monolith.env'; set +a; '$install_root/iot-nano-monolith' --bootstrap-system"
}

wait_ready() {
  local attempt local_health_url="http://127.0.0.1:${public_address##*:}"
  for attempt in $(seq 1 60); do
    if curl --fail --silent --show-error "$local_health_url/readyz" >/dev/null; then return 0; fi
    sleep 1
  done
  journalctl -u "$service_name" --since '5 minutes ago' --no-pager >&2 || true
  return 1
}

seed_management_url() {
  printf 'http://127.0.0.1:%s\n' "${public_address##*:}"
}

seed_api_json() {
  local method="$1" path="$2" payload="$3"

  curl --fail --silent --show-error --cookie "$seed_tenant_cookie" \
    --request "$method" --header 'Content-Type: application/json' --data "$payload" \
    "$seed_management_url$path"
}

seed_api_get() {
  curl --fail --silent --show-error --cookie "$seed_tenant_cookie" "$seed_management_url$1"
}

prepare_seed_session() {
  local create_tenant_status

  seed_management_url="$(seed_management_url)"
  seed_state_dir="$(mktemp -d)"
  trap 'rm -rf "$seed_state_dir"' EXIT
  seed_system_cookie="$seed_state_dir/system.cookie"
  seed_tenant_cookie="$seed_state_dir/tenant.cookie"
  curl --fail --silent --show-error --cookie-jar "$seed_system_cookie" -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg username "$system_username" --arg password "$system_password" '{username:$username,password:$password}')" \
    "$seed_management_url/api/v1/system/auth/login" >/dev/null
  create_tenant_status="$(curl --silent --show-error --output "$seed_state_dir/tenant.json" --write-out '%{http_code}' \
    --cookie "$seed_system_cookie" -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg slug "$tenant_slug" --arg username "$tenant_username" --arg password "$tenant_password" '{slug:$slug,metadata:{},tenant_account_username:$username,tenant_account_password:$password}')" \
    "$seed_management_url/api/v1/system/tenants")"
  case "$create_tenant_status" in
    201|409) ;;
    *) printf 'Tenant seed failed with HTTP %s\n' "$create_tenant_status" >&2; return 1 ;;
  esac
  curl --fail --silent --show-error --cookie-jar "$seed_tenant_cookie" -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg tenant_slug "$tenant_slug" --arg password "$tenant_password" '{tenant_slug:$tenant_slug,password:$password}')" \
    "$seed_management_url/api/v1/tenant/auth/login" >/dev/null
}

seed_user() {
  local username="$1" password="$2"

  seed_api_json POST /api/v1/management/users \
    "$(jq -nc --arg username "$username" --arg password "$password" '{username:$username,password:$password}')" >/dev/null
}

starter_seed() {
  prepare_seed_session
  seed_user "$user1_username" "$user1_password"
  seed_user "$user2_username" "$user2_password"
}

demo_create_profile() {
  local path="$1" payload="$2" response

  response="$(seed_api_json POST "$path" "$payload")"
  jq -er '.id' <<<"$response"
}

demo_create_asset() {
  local name="$1" parent_asset_id="$2" asset_profile_id="$3" response

  response="$(seed_api_json POST /api/v1/management/assets \
    "$(jq -nc --arg name "$name" --arg parent_asset_id "$parent_asset_id" --arg asset_profile_id "$asset_profile_id" \
      '{name:$name,asset_profile_id:$asset_profile_id,parent_asset_id:(if $parent_asset_id == "" then null else $parent_asset_id end),metadata:{local_seed:"installer-demo"},attributes:null}')")"
  jq -er '.id' <<<"$response"
}

demo_create_device() {
  local name="$1" asset_id="$2" device_profile_id="$3" serial_number response

  serial_number="DEMO-$(printf '%s' "$name" | tr '[:lower:]' '[:upper:]' | tr -cs 'A-Z0-9' '-')"
  serial_number="${serial_number%-}"
  response="$(seed_api_json POST /api/v1/management/devices \
    "$(jq -nc --arg serial_number "$serial_number" --arg name "$name" --arg asset_id "$asset_id" --arg device_profile_id "$device_profile_id" \
      '{serial_number:$serial_number,display_name:$name,asset_id:$asset_id,device_profile_id:$device_profile_id,attributes:{local_seed:"installer-demo"}}')")"
  jq -er '.device_id' <<<"$response"
}

demo_assign_owner() {
  local kind="$1" resource_id="$2" user_id="$3"

  seed_api_json PUT "/api/v1/management/${kind}/${resource_id}/owner" \
    "$(jq -nc --arg user_id "$user_id" '{user_id:$user_id}')" >/dev/null
}

demo_seed_alert_rule() {
  local device_id="$1"

  seed_api_json POST /api/v1/management/alert-rules \
    "$(jq -nc --arg device_id "$device_id" '{
      name:"High active power",enabled:true,device_id:$device_id,metric_key:"power_w",
      rule_type:"event_threshold",comparison:"gt",threshold:500,window_seconds:null,
      for_seconds:0,resolve_after_seconds:300,reopen_grace_seconds:3600,
      hysteresis:null,severity:"warning",reminder_interval_seconds:86400
    }')" >/dev/null
}

demo_seed() {
  local owner_user_id power_meter_profile_id power_farm_profile_id power_zone_profile_id
  local farm_1_id farm_1_zone_1_id farm_1_zone_2_id farm_2_id farm_2_zone_1_id farm_2_zone_2_id
  local farm_1_zone_1_device_1_id farm_1_zone_1_device_2_id farm_1_zone_2_device_1_id farm_1_zone_2_device_2_id
  local farm_2_zone_1_device_1_id farm_2_zone_1_device_2_id farm_2_zone_2_device_1_id farm_2_zone_2_device_2_id
  local resource_id

  starter_seed
  seed_api_json POST /api/v1/management/applications \
    "$(jq -nc --arg launch_url "$powermonitor_url" '{
      app_id:"powermonitor",kind:"full_stack",launch_url:$launch_url,
      client_id:"powermonitor-client",redirect_uris:[($launch_url + "/api/v1/auth/callback")],enabled:true
    }')" >/dev/null
  owner_user_id="$(seed_api_get /api/v1/management/users | jq -er --arg username "$user1_username" '[.[] | select(.username == $username)] | if length == 1 then .[0].id else error("demo owner is missing or ambiguous") end')"
  power_meter_profile_id="$(demo_create_profile /api/v1/management/profiles/device-profiles '{"name":"Power Meter","telemetry_schema":{"power_w":{"type":"number","unit":"W"},"voltage_v":{"type":"number","unit":"V"},"current_a":{"type":"number","unit":"A"},"energy_kwh":{"type":"number","unit":"kWh"}},"metric_mapping":{"power_w":"Active power","voltage_v":"Voltage","current_a":"Current","energy_kwh":"Energy"}}')"
  power_farm_profile_id="$(demo_create_profile /api/v1/management/profiles/asset-profiles '{"name":"Power Farm","fields":{"location":{"type":"string"},"capacity_kw":{"type":"number"}},"dashboard_defaults":{"primary_metric":"power_w","aggregation":"sum"}}')"
  power_zone_profile_id="$(demo_create_profile /api/v1/management/profiles/asset-profiles '{"name":"Power Zone","fields":{"location":{"type":"string"}},"dashboard_defaults":{"primary_metric":"power_w","aggregation":"sum"}}')"
  farm_1_id="$(demo_create_asset 'Power Farm 1' '' "$power_farm_profile_id")"
  farm_1_zone_1_id="$(demo_create_asset 'Farm 1 / Zone 1' "$farm_1_id" "$power_zone_profile_id")"
  farm_1_zone_2_id="$(demo_create_asset 'Farm 1 / Zone 2' "$farm_1_id" "$power_zone_profile_id")"
  farm_2_id="$(demo_create_asset 'Power Farm 2' '' "$power_farm_profile_id")"
  farm_2_zone_1_id="$(demo_create_asset 'Farm 2 / Zone 1' "$farm_2_id" "$power_zone_profile_id")"
  farm_2_zone_2_id="$(demo_create_asset 'Farm 2 / Zone 2' "$farm_2_id" "$power_zone_profile_id")"
  farm_1_zone_1_device_1_id="$(demo_create_device 'Farm 1 / Zone 1 / Device 1' "$farm_1_zone_1_id" "$power_meter_profile_id")"
  farm_1_zone_1_device_2_id="$(demo_create_device 'Farm 1 / Zone 1 / Device 2' "$farm_1_zone_1_id" "$power_meter_profile_id")"
  farm_1_zone_2_device_1_id="$(demo_create_device 'Farm 1 / Zone 2 / Device 1' "$farm_1_zone_2_id" "$power_meter_profile_id")"
  farm_1_zone_2_device_2_id="$(demo_create_device 'Farm 1 / Zone 2 / Device 2' "$farm_1_zone_2_id" "$power_meter_profile_id")"
  farm_2_zone_1_device_1_id="$(demo_create_device 'Farm 2 / Zone 1 / Device 1' "$farm_2_zone_1_id" "$power_meter_profile_id")"
  farm_2_zone_1_device_2_id="$(demo_create_device 'Farm 2 / Zone 1 / Device 2' "$farm_2_zone_1_id" "$power_meter_profile_id")"
  farm_2_zone_2_device_1_id="$(demo_create_device 'Farm 2 / Zone 2 / Device 1' "$farm_2_zone_2_id" "$power_meter_profile_id")"
  farm_2_zone_2_device_2_id="$(demo_create_device 'Farm 2 / Zone 2 / Device 2' "$farm_2_zone_2_id" "$power_meter_profile_id")"
  for resource_id in "$farm_1_id" "$farm_1_zone_1_id" "$farm_1_zone_2_id" "$farm_2_id" "$farm_2_zone_1_id" "$farm_2_zone_2_id"; do
    demo_assign_owner assets "$resource_id" "$owner_user_id"
  done
  for resource_id in "$farm_1_zone_1_device_1_id" "$farm_1_zone_1_device_2_id" "$farm_1_zone_2_device_1_id" "$farm_1_zone_2_device_2_id" "$farm_2_zone_1_device_1_id" "$farm_2_zone_1_device_2_id" "$farm_2_zone_2_device_1_id" "$farm_2_zone_2_device_2_id"; do
    demo_assign_owner devices "$resource_id" "$owner_user_id"
    demo_seed_alert_rule "$resource_id"
  done
  printf '%s\n' 'Power Monitor demo seed completed.'
}

seed_platform() {
  case "$seed_mode" in
    starter) starter_seed ;;
    demo) demo_seed ;;
    none) ;;
    *) printf 'Seed mode must be starter, demo, or none.\n' >&2; return 2 ;;
  esac
}

smoke_test() {
  local local_health_url="http://127.0.0.1:${public_address##*:}"
  curl --fail --show-error "$local_health_url/healthz"
  curl --fail --show-error "$local_health_url/readyz"
}

backup() {
  local backup_root="/var/backups/iot-nano" timestamp backup_dir
  timestamp="$(date -u +%Y%m%dT%H%M%SZ)"; backup_dir="$backup_root/$timestamp"
  install -d -o root -g root -m 0700 "$backup_dir"
  systemctl stop "$service_name"
  if ! tar -C "$data_root" -czf "$backup_dir/platform-and-internal.tar.gz" platform internal; then
    systemctl start "$service_name" || true
    return 1
  fi
  systemctl start "$service_name"
  printf 'Backup created: %s\n' "$backup_dir/platform-and-internal.tar.gz"
}

uninstall() {
  local confirmation=""
  if [[ "$uninstall_yes" != 1 ]]; then
    read -r -p 'Type uninstall to continue: ' confirmation
    [[ "$confirmation" == uninstall ]] || { printf '%s\n' 'Cancelled.'; return; }
  fi
  backup
  systemctl disable --now "$service_name" || true
  rm -f "/etc/systemd/system/$service_name" "$install_root/iot-nano-monolith" "$config_root/monolith.env"
  [[ "$uninstall_purge_data" == 1 ]] && rm -rf "$data_root"
  systemctl daemon-reload
}

command="${1:-install}"; shift || true
uninstall_yes=0
uninstall_purge_data=0
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --seed-mode) seed_mode="$2"; shift 2 ;;
    --public-address) public_address="$2"; shift 2 ;;
    --mqtt-address) mqtt_address="$2"; shift 2 ;;
    --mqtt-tls-address) mqtt_tls_address="$2"; shift 2 ;;
    --system-username) system_username="$2"; shift 2 ;;
    --system-password) system_password="$2"; shift 2 ;;
    --tenant-slug) tenant_slug="$2"; shift 2 ;;
    --tenant-username) tenant_username="$2"; shift 2 ;;
    --tenant-password) tenant_password="$2"; shift 2 ;;
    --user1-username) user1_username="$2"; shift 2 ;;
    --user1-password) user1_password="$2"; shift 2 ;;
    --user2-username) user2_username="$2"; shift 2 ;;
    --user2-password) user2_password="$2"; shift 2 ;;
    --powermonitor-url) powermonitor_url="$2"; shift 2 ;;
    --yes) uninstall_yes=1; shift ;;
    --purge-data) uninstall_purge_data=1; shift ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done
case "$command" in
  install) require_root; prompt_install_values; install_dependencies; install_paths; build_and_install_binary; write_runtime_material; bootstrap_system; systemctl daemon-reload; systemctl enable --now "$service_name"; wait_ready; seed_platform; smoke_test ;;
  seed-demo) seed_mode="demo"; seed_platform ;;
  status) systemctl status "$service_name" --no-pager; smoke_test ;;
  smoke-test) smoke_test ;;
  backup) require_root; backup ;;
  uninstall) require_root; uninstall ;;
  --help|-h|help) usage ;;
  *) usage >&2; exit 2 ;;
esac
