#!/usr/bin/env bash

# This file is sourced by seed-local-platform.sh. It intentionally has no
# top-level side effects so its lifecycle functions can be tested in isolation.
local_platform_helper_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

local_platform_configure() {
  local root

  root="${IOT_NANO_LOCAL_PLATFORM_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/rush-iot-nano/local-platform}"
  IOT_NANO_LOCAL_PLATFORM_ROOT="$root"
  IOT_NANO_LOCAL_PLATFORM_PATH="$root/platform.sqlite"
  IOT_NANO_LOCAL_INTERNAL_DIR="$root/internal"
  IOT_NANO_LOCAL_VAULT_PATH="$root/vault.key"
  IOT_NANO_LOCAL_TLS_CERT_PATH="$root/mqtt-cert.pem"
  IOT_NANO_LOCAL_TLS_KEY_PATH="$root/mqtt-key.pem"
  IOT_NANO_LOCAL_PID_FILE="${IOT_NANO_LOCAL_PID_FILE:-$root/monolith.pid}"
  IOT_NANO_LOCAL_LOG_FILE="${IOT_NANO_LOCAL_LOG_FILE:-$root/monolith.log}"
  IOT_NANO_PUBLIC_HTTP_ADDRESS="${IOT_NANO_PUBLIC_HTTP_ADDRESS:-127.0.0.1:18080}"
  IOT_NANO_MANAGEMENT_ADDRESS="${IOT_NANO_MANAGEMENT_ADDRESS:-127.0.0.1:18081}"
  IOT_NANO_MQTT_TCP_ADDRESS="${IOT_NANO_MQTT_TCP_ADDRESS:-127.0.0.1:18883}"
  IOT_NANO_MQTT_TLS_ADDRESS="${IOT_NANO_MQTT_TLS_ADDRESS:-127.0.0.1:18884}"
  IOT_NANO_MANAGEMENT_URL="${IOT_NANO_MANAGEMENT_URL:-http://$IOT_NANO_MANAGEMENT_ADDRESS}"
  IOT_NANO_LOCAL_CARGO_LANE="${IOT_NANO_LOCAL_CARGO_LANE:-$local_platform_helper_root/scripts/dev/cargo-lane.sh}"
  IOT_NANO_LOCAL_KILL_BIN="${IOT_NANO_LOCAL_KILL_BIN:-/bin/kill}"
}

local_platform_fail() {
  printf 'local-platform: %s\n' "$*" >&2
  return 1
}

local_platform_require_reset() {
  if [[ "$#" != 1 || "$1" != '--reset' ]]; then
    printf 'usage: %s --reset\n' "${0##*/}" >&2
    return 2
  fi
}

local_platform_public_port() {
  local address="$IOT_NANO_PUBLIC_HTTP_ADDRESS"
  printf '%s\n' "${address##*:}"
}

local_platform_listener_pid() {
  local port

  port="$(local_platform_public_port)"
  lsof -nP -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true
}

local_platform_pid_is_expected() {
  local pid="$1"
  local open_files

  open_files="$(lsof -nP -p "$pid" 2>/dev/null || true)"
  [[ "$open_files" == *'iot-nano-monolith'* ]] || return 1
  [[ "$open_files" == *"$IOT_NANO_LOCAL_PLATFORM_PATH"* ]] || return 1
}

local_platform_wait_for_listener_exit() {
  local attempts=0

  while [[ -n "$(local_platform_listener_pid)" ]]; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -gt 50 ]]; then
      local_platform_fail 'timed out waiting for the local monolith listener to stop'
      return 1
    fi
    sleep 0.2
  done
}

local_platform_stop() {
  local pid

  local_platform_configure
  pid="$(local_platform_listener_pid)"
  if [[ -z "$pid" ]]; then
    rm -f "$IOT_NANO_LOCAL_PID_FILE"
    return 0
  fi
  if ! local_platform_pid_is_expected "$pid"; then
    local_platform_fail "refusing to stop unverified listener PID $pid"
    return 1
  fi

  "$IOT_NANO_LOCAL_KILL_BIN" -TERM "$pid"
  local_platform_wait_for_listener_exit
  rm -f "$IOT_NANO_LOCAL_PID_FILE"
}

local_platform_clear_state() {
  local_platform_configure

  rm -f "$IOT_NANO_LOCAL_PLATFORM_PATH" \
    "$IOT_NANO_LOCAL_PLATFORM_PATH-wal" \
    "$IOT_NANO_LOCAL_PLATFORM_PATH-shm" \
    "$IOT_NANO_LOCAL_PID_FILE"
  rm -rf "$IOT_NANO_LOCAL_INTERNAL_DIR"
}

local_platform_require_material() {
  local path

  for path in "$IOT_NANO_LOCAL_VAULT_PATH" "$IOT_NANO_LOCAL_TLS_CERT_PATH" "$IOT_NANO_LOCAL_TLS_KEY_PATH"; do
    [[ -f "$path" ]] || {
      local_platform_fail "required local runtime material is missing: $path"
      return 1
    }
  done
}

local_platform_export_environment() {
  local_platform_configure
  local_platform_require_material

  export IOT_NANO_STORAGE=sqlite
  export IOT_NANO_SQLITE_PATH="$IOT_NANO_LOCAL_PLATFORM_PATH"
  export IOT_NANO_INTERNAL_DIR="$IOT_NANO_LOCAL_INTERNAL_DIR"
  export IOT_NANO_TLS_CERT_PATH="$IOT_NANO_LOCAL_TLS_CERT_PATH"
  export IOT_NANO_TLS_KEY_PATH="$IOT_NANO_LOCAL_TLS_KEY_PATH"
  export IOT_NANO_PUBLIC_HTTP_ADDRESS
  export IOT_NANO_MANAGEMENT_ADDRESS
  export IOT_NANO_MQTT_TCP_ADDRESS
  export IOT_NANO_MQTT_TLS_ADDRESS
  export IOT_DEVICE_TOKEN_VAULT_KEY
  IOT_DEVICE_TOKEN_VAULT_KEY="$(tr -d '\r\n' <"$IOT_NANO_LOCAL_VAULT_PATH")"
}

local_platform_bootstrap() {
  local username="$1"
  local password="$2"

  local_platform_export_environment
  IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME="$username" \
    IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD="$password" \
    "$IOT_NANO_LOCAL_CARGO_LANE" local-platform -- \
      run -p iot-nano-monolith -- --bootstrap-system
}

local_platform_wait_for_http() {
  local path="$1"
  local attempts=0

  while ! curl --fail --silent --show-error --max-time 2 \
    "http://$IOT_NANO_PUBLIC_HTTP_ADDRESS$path" >/dev/null; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -gt 50 ]]; then
      local_platform_fail "timed out waiting for $path"
      return 1
    fi
    sleep 0.2
  done
}

local_platform_start() {
  local pid

  local_platform_export_environment
  mkdir -p "$IOT_NANO_LOCAL_PLATFORM_ROOT"
  nohup "$IOT_NANO_LOCAL_CARGO_LANE" local-platform -- \
    run -p iot-nano-monolith >"$IOT_NANO_LOCAL_LOG_FILE" 2>&1 < /dev/null &

  local_platform_wait_for_http /healthz
  local_platform_wait_for_http /readyz
  pid="$(local_platform_listener_pid)"
  if [[ -z "$pid" ]] || ! local_platform_pid_is_expected "$pid"; then
    local_platform_fail 'started process could not be verified as the local monolith'
    return 1
  fi
  printf '%s\n' "$pid" >"$IOT_NANO_LOCAL_PID_FILE"
}
