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
public_address="0.0.0.0:8080"
management_address="127.0.0.1:8081"
mqtt_address="0.0.0.0:1883"
mqtt_tls_address="0.0.0.0:8883"
web_https_enabled="false"
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
  status        Show service status and health.
  smoke-test    Check public health/ready endpoints.
  backup        Stop, archive platform plus internal state, then restart.
  uninstall     Disable service and remove binary/config; preserves data by default.

Options:
  --seed-mode MODE       starter (default) or none.
  --public-address ADDR  Default: 0.0.0.0:8080.
  --management-address ADDR  Default: 127.0.0.1:8081.
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
  management_address="$(prompt 'Management address (keep private)' "$management_address")"
  web_https_enabled="$(prompt 'HTTPS enabled for browser cookies: true or false' "$web_https_enabled")"
  seed_mode="$(prompt 'Seed mode: starter or none' "$seed_mode")"
  system_username="$(prompt 'System username' "$system_username")"
  system_password="$(prompt 'System password' "$system_password")"
  tenant_slug="$(prompt 'Tenant slug' "$tenant_slug")"
  tenant_username="$(prompt 'Tenant username' "$tenant_username")"
  tenant_password="$(prompt 'Tenant password' "$tenant_password")"
  user1_username="$(prompt 'First user username' "$user1_username")"
  user1_password="$(prompt 'First user password' "$user1_password")"
  user2_username="$(prompt 'Second user username' "$user2_username")"
  user2_password="$(prompt 'Second user password' "$user2_password")"
  case "$seed_mode" in starter|none) ;; *) printf '%s\n' 'Seed mode must be starter or none.' >&2; exit 2;; esac
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

starter_seed() {
  [[ "$seed_mode" == starter ]] || return 0
  local management_url="http://$management_address" state_dir system_cookie tenant_cookie
  state_dir="$(mktemp -d)"; trap 'rm -rf "$state_dir"' EXIT
  system_cookie="$state_dir/system.cookie"; tenant_cookie="$state_dir/tenant.cookie"
  curl --fail --silent --show-error --cookie-jar "$system_cookie" -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg username "$system_username" --arg password "$system_password" '{username:$username,password:$password}')" \
    "$management_url/api/v1/system/auth/login" >/dev/null
  curl --fail --silent --show-error --cookie "$system_cookie" -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg slug "$tenant_slug" --arg username "$tenant_username" --arg password "$tenant_password" '{slug:$slug,metadata:{},tenant_account_username:$username,tenant_account_password:$password}')" \
    "$management_url/api/v1/system/tenants" >/dev/null
  curl --fail --silent --show-error --cookie-jar "$tenant_cookie" -H 'Content-Type: application/json' \
    --data "$(jq -nc --arg tenant_slug "$tenant_slug" --arg password "$tenant_password" '{tenant_slug:$tenant_slug,password:$password}')" \
    "$management_url/api/v1/tenant/auth/login" >/dev/null
  for user in "$user1_username:$user1_password" "$user2_username:$user2_password"; do
    curl --fail --silent --show-error --cookie "$tenant_cookie" -H 'Content-Type: application/json' \
      --data "$(jq -nc --arg username "${user%%:*}" --arg password "${user#*:}" '{username:$username,password:$password}')" \
      "$management_url/api/v1/management/users" >/dev/null
  done
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
    --management-address) management_address="$2"; shift 2 ;;
    --yes) uninstall_yes=1; shift ;;
    --purge-data) uninstall_purge_data=1; shift ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 2 ;;
  esac
done
case "$command" in
  install) require_root; prompt_install_values; install_dependencies; install_paths; build_and_install_binary; write_runtime_material; bootstrap_system; systemctl daemon-reload; systemctl enable --now "$service_name"; wait_ready; starter_seed; smoke_test ;;
  status) systemctl status "$service_name" --no-pager; smoke_test ;;
  smoke-test) smoke_test ;;
  backup) require_root; backup ;;
  uninstall) require_root; uninstall ;;
  --help|-h|help) usage ;;
  *) usage >&2; exit 2 ;;
esac
