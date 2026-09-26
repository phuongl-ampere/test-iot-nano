#!/usr/bin/env bash

# This file is sourced by seed-local-platform.sh. It intentionally has no
# top-level side effects so its lifecycle functions can be tested in isolation.
local_platform_helper_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

local_platform_workspace_key() {
  local root_name
  local root_hash

  root_name="$(basename "$local_platform_helper_root" | tr -cs 'A-Za-z0-9._-' '_')"
  root_hash="$(printf '%s' "$local_platform_helper_root" | git -C "$local_platform_helper_root" hash-object --stdin)"
  printf '%s-%s\n' "$root_name" "${root_hash:0:12}"
}

local_platform_configure() {
  local root
  local cargo_bin
  local cache_root

  root="${IOT_NANO_LOCAL_PLATFORM_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/rush-iot-nano/local-platform}"
  cache_root="${XDG_CACHE_HOME:-$HOME/.cache}"
  IOT_NANO_LOCAL_PLATFORM_ROOT="$root"
  IOT_NANO_LOCAL_PLATFORM_PATH="$root/platform.sqlite"
  IOT_NANO_LOCAL_INTERNAL_DIR="$root/internal"
  IOT_NANO_LOCAL_VAULT_PATH="$root/vault.key"
  IOT_NANO_LOCAL_TLS_CERT_PATH="$root/mqtt-cert.pem"
  IOT_NANO_LOCAL_TLS_KEY_PATH="$root/mqtt-key.pem"
  IOT_NANO_LOCAL_PID_FILE="${IOT_NANO_LOCAL_PID_FILE:-$root/monolith.pid}"
  IOT_NANO_LOCAL_LOG_FILE="${IOT_NANO_LOCAL_LOG_FILE:-$root/monolith.log}"
  IOT_NANO_LOCAL_RUNNER_FILE="${IOT_NANO_LOCAL_RUNNER_FILE:-$root/monolith-runner.sh}"
  IOT_NANO_LOCAL_OWNER_FILE="${IOT_NANO_LOCAL_OWNER_FILE:-$root/workspace-root}"
  IOT_NANO_LOCAL_STARTUP_ATTEMPTS="${IOT_NANO_LOCAL_STARTUP_ATTEMPTS:-300}"
  IOT_NANO_PUBLIC_HTTP_ADDRESS="${IOT_NANO_PUBLIC_HTTP_ADDRESS:-127.0.0.1:18080}"
  IOT_NANO_MANAGEMENT_ADDRESS="${IOT_NANO_MANAGEMENT_ADDRESS:-127.0.0.1:18081}"
  IOT_NANO_MQTT_TCP_ADDRESS="${IOT_NANO_MQTT_TCP_ADDRESS:-127.0.0.1:18883}"
  IOT_NANO_MQTT_TLS_ADDRESS="${IOT_NANO_MQTT_TLS_ADDRESS:-127.0.0.1:18884}"
  IOT_NANO_MANAGEMENT_URL="${IOT_NANO_MANAGEMENT_URL:-http://$IOT_NANO_MANAGEMENT_ADDRESS}"
  IOT_NANO_LOCAL_CARGO_LANE="${IOT_NANO_LOCAL_CARGO_LANE:-$local_platform_helper_root/scripts/dev/cargo-lane.sh}"
  IOT_NANO_LOCAL_LANE_TARGET_ROOT="${IOT_NANO_LOCAL_LANE_TARGET_ROOT:-${IOT_NANO_LANE_TARGET_ROOT:-$cache_root/rush-iot-nano/cargo-lanes}}"
  IOT_NANO_LOCAL_WORKSPACE_KEY="$(local_platform_workspace_key)"
  IOT_NANO_LOCAL_BINARY_PATH="$IOT_NANO_LOCAL_LANE_TARGET_ROOT/$IOT_NANO_LOCAL_WORKSPACE_KEY/local-platform/debug/iot-nano-monolith"
  cargo_bin="${IOT_NANO_LOCAL_CARGO_BIN:-$(command -v cargo || true)}"
  IOT_NANO_LOCAL_CARGO_BIN_DIR="${IOT_NANO_LOCAL_CARGO_BIN_DIR:-$(dirname "$cargo_bin")}"
  IOT_NANO_LOCAL_KILL_BIN="${IOT_NANO_LOCAL_KILL_BIN:-/bin/kill}"
  IOT_NANO_LOCAL_LAUNCHCTL_BIN="${IOT_NANO_LOCAL_LAUNCHCTL_BIN:-/bin/launchctl}"
  IOT_NANO_LOCAL_SERVICE_LABEL="${IOT_NANO_LOCAL_SERVICE_LABEL:-io.rush-iot-nano.local-platform.$IOT_NANO_LOCAL_WORKSPACE_KEY}"
}

local_platform_fail() {
  printf 'local-platform: %s\n' "$*" >&2
  return 1
}

local_platform_write_owner() {
  mkdir -p "$IOT_NANO_LOCAL_PLATFORM_ROOT"
  chmod 700 "$IOT_NANO_LOCAL_PLATFORM_ROOT"
  umask 077
  printf '%s\n' "$local_platform_helper_root" >"$IOT_NANO_LOCAL_OWNER_FILE"
  chmod 600 "$IOT_NANO_LOCAL_OWNER_FILE"
}

local_platform_assert_ownership() {
  local owner
  local pid

  if [[ -f "$IOT_NANO_LOCAL_OWNER_FILE" ]]; then
    owner="$(<"$IOT_NANO_LOCAL_OWNER_FILE")"
    [[ "$owner" == "$local_platform_helper_root" ]] || {
      local_platform_fail "local runtime belongs to a different workspace: $owner"
      return 1
    }
    return 0
  fi

  pid="$(local_platform_listener_pid)"
  if [[ -n "$pid" ]]; then
    local_platform_pid_is_expected "$pid" || {
      local_platform_fail "refusing to claim unverified listener PID $pid"
      return 1
    }
    local_platform_write_owner
    return 0
  fi

  if [[ -e "$IOT_NANO_LOCAL_PLATFORM_PATH" || -e "$IOT_NANO_LOCAL_INTERNAL_DIR" ]]; then
    local_platform_fail 'local runtime state exists without an ownership marker or verified listener'
    return 1
  fi
  local_platform_write_owner
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
  [[ "$open_files" == *"$IOT_NANO_LOCAL_BINARY_PATH"* ]] || return 1
  [[ "$open_files" == *"$IOT_NANO_LOCAL_PLATFORM_PATH"* ]] || return 1
}

local_platform_has_listeners() {
  local address
  local port

  for address in \
    "$IOT_NANO_PUBLIC_HTTP_ADDRESS" \
    "$IOT_NANO_MANAGEMENT_ADDRESS" \
    "$IOT_NANO_MQTT_TCP_ADDRESS" \
    "$IOT_NANO_MQTT_TLS_ADDRESS"; do
    port="${address##*:}"
    if lsof -nP -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | grep -q .; then
      return 0
    fi
  done
  return 1
}

local_platform_pid_file_pid() {
  local pid

  [[ -f "$IOT_NANO_LOCAL_PID_FILE" ]] || return 0
  pid="$(<"$IOT_NANO_LOCAL_PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] && printf '%s\n' "$pid"
}

local_platform_wait_for_shutdown() {
  local pid="$1"
  local attempts=0

  while "$IOT_NANO_LOCAL_KILL_BIN" -0 "$pid" 2>/dev/null || local_platform_has_listeners; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -gt 50 ]]; then
      local_platform_fail 'timed out waiting for the local monolith process and listeners to stop'
      return 1
    fi
    sleep 0.2
  done
}

local_platform_stop() {
  local pid
  local listener_pid
  local tracked_pid

  local_platform_configure
  local_platform_assert_ownership
  listener_pid="$(local_platform_listener_pid)"
  tracked_pid="$(local_platform_pid_file_pid)"

  if [[ -n "$tracked_pid" ]] && "$IOT_NANO_LOCAL_KILL_BIN" -0 "$tracked_pid" 2>/dev/null; then
    local_platform_pid_is_expected "$tracked_pid" || {
      local_platform_fail "refusing to stop unverified PID file process $tracked_pid"
      return 1
    }
    pid="$tracked_pid"
  else
    pid="$listener_pid"
  fi
  if [[ -z "$pid" ]]; then
    rm -f "$IOT_NANO_LOCAL_PID_FILE"
    local_platform_remove_launch_agent
    return 0
  fi
  if ! local_platform_pid_is_expected "$pid"; then
    local_platform_fail "refusing to stop unverified listener PID $pid"
    return 1
  fi

  "$IOT_NANO_LOCAL_KILL_BIN" -TERM "$pid"
  local_platform_wait_for_shutdown "$pid"
  local_platform_remove_launch_agent
  rm -f "$IOT_NANO_LOCAL_PID_FILE"
}

local_platform_clear_state() {
  local_platform_configure
  local_platform_assert_ownership

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
  [[ -x "$IOT_NANO_LOCAL_CARGO_BIN_DIR/cargo" ]] || {
    local_platform_fail "cargo is unavailable from $IOT_NANO_LOCAL_CARGO_BIN_DIR"
    return 1
  }
}

local_platform_validate_loopback_address() {
  local name="$1"
  local address="$2"

  case "$address" in
    127.0.0.1:*|localhost:*|'[::1]':*) ;;
    *)
      local_platform_fail "$name must bind to loopback, received $address"
      return 1
      ;;
  esac
}

local_platform_preflight() {
  local command

  local_platform_configure
  local_platform_require_material || return 1
  for command in curl jq grep mktemp lsof; do
    command -v "$command" >/dev/null || {
      local_platform_fail "required command is unavailable: $command"
      return 1
    }
  done
  local_platform_validate_loopback_address IOT_NANO_PUBLIC_HTTP_ADDRESS "$IOT_NANO_PUBLIC_HTTP_ADDRESS" || return 1
  local_platform_validate_loopback_address IOT_NANO_MANAGEMENT_ADDRESS "$IOT_NANO_MANAGEMENT_ADDRESS" || return 1
  local_platform_validate_loopback_address IOT_NANO_MQTT_TCP_ADDRESS "$IOT_NANO_MQTT_TCP_ADDRESS" || return 1
  local_platform_validate_loopback_address IOT_NANO_MQTT_TLS_ADDRESS "$IOT_NANO_MQTT_TLS_ADDRESS" || return 1
}

local_platform_remove_launch_agent() {
  if [[ "$(uname -s)" == Darwin && -x "$IOT_NANO_LOCAL_LAUNCHCTL_BIN" ]]; then
    "$IOT_NANO_LOCAL_LAUNCHCTL_BIN" remove "$IOT_NANO_LOCAL_SERVICE_LABEL" 2>/dev/null || true
  fi
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
    if [[ "$attempts" -gt "$IOT_NANO_LOCAL_STARTUP_ATTEMPTS" ]]; then
      local_platform_fail "timed out waiting for $path"
      return 1
    fi
    sleep 0.2
  done
}

local_platform_wait_for_management() {
  local attempts=0

  while ! curl --silent --show-error --output /dev/null --max-time 2 \
    "http://$IOT_NANO_MANAGEMENT_ADDRESS/api/system/auth/login"; do
    attempts=$((attempts + 1))
    if [[ "$attempts" -gt "$IOT_NANO_LOCAL_STARTUP_ATTEMPTS" ]]; then
      local_platform_fail 'timed out waiting for the management listener'
      return 1
    fi
    sleep 0.2
  done
}

local_platform_write_runner() {
  mkdir -p "$IOT_NANO_LOCAL_PLATFORM_ROOT"
  chmod 700 "$IOT_NANO_LOCAL_PLATFORM_ROOT"
  umask 077
  {
    printf '%s\n' '#!/usr/bin/env bash'
    printf 'export IOT_NANO_STORAGE=%q\n' "$IOT_NANO_STORAGE"
    printf 'export IOT_NANO_SQLITE_PATH=%q\n' "$IOT_NANO_SQLITE_PATH"
    printf 'export IOT_NANO_INTERNAL_DIR=%q\n' "$IOT_NANO_INTERNAL_DIR"
    printf 'export IOT_NANO_TLS_CERT_PATH=%q\n' "$IOT_NANO_TLS_CERT_PATH"
    printf 'export IOT_NANO_TLS_KEY_PATH=%q\n' "$IOT_NANO_TLS_KEY_PATH"
    printf 'export IOT_NANO_PUBLIC_HTTP_ADDRESS=%q\n' "$IOT_NANO_PUBLIC_HTTP_ADDRESS"
    printf 'export IOT_NANO_MANAGEMENT_ADDRESS=%q\n' "$IOT_NANO_MANAGEMENT_ADDRESS"
    printf 'export IOT_NANO_MQTT_TCP_ADDRESS=%q\n' "$IOT_NANO_MQTT_TCP_ADDRESS"
    printf 'export IOT_NANO_MQTT_TLS_ADDRESS=%q\n' "$IOT_NANO_MQTT_TLS_ADDRESS"
    printf 'export IOT_DEVICE_TOKEN_VAULT_KEY=%q\n' "$IOT_DEVICE_TOKEN_VAULT_KEY"
    printf 'export IOT_NANO_LANE_TARGET_ROOT=%q\n' "$IOT_NANO_LOCAL_LANE_TARGET_ROOT"
    printf 'export PATH=%q\n' "$IOT_NANO_LOCAL_CARGO_BIN_DIR:/usr/bin:/bin:/usr/sbin:/sbin"
    printf 'cd %q\n' "$local_platform_helper_root"
    printf 'exec %q local-platform -- run -p iot-nano-monolith\n' "$IOT_NANO_LOCAL_CARGO_LANE"
  } >"$IOT_NANO_LOCAL_RUNNER_FILE"
  chmod 700 "$IOT_NANO_LOCAL_RUNNER_FILE"
}

local_platform_start() {
  local pid

  local_platform_export_environment
  local_platform_write_runner
  if [[ "$(uname -s)" == Darwin && -x "$IOT_NANO_LOCAL_LAUNCHCTL_BIN" ]]; then
    local_platform_remove_launch_agent
    "$IOT_NANO_LOCAL_LAUNCHCTL_BIN" submit \
      -l "$IOT_NANO_LOCAL_SERVICE_LABEL" \
      -o "$IOT_NANO_LOCAL_LOG_FILE" \
      -e "$IOT_NANO_LOCAL_LOG_FILE" \
      -- "$IOT_NANO_LOCAL_RUNNER_FILE"
  else
    nohup "$IOT_NANO_LOCAL_RUNNER_FILE" >"$IOT_NANO_LOCAL_LOG_FILE" 2>&1 < /dev/null &
  fi

  if ! local_platform_wait_for_http /healthz || ! local_platform_wait_for_http /readyz || \
    ! local_platform_wait_for_management; then
    local_platform_remove_launch_agent
    return 1
  fi
  pid="$(local_platform_listener_pid)"
  if [[ -z "$pid" ]] || ! local_platform_pid_is_expected "$pid"; then
    local_platform_fail 'started process could not be verified as the local monolith'
    return 1
  fi
  printf '%s\n' "$pid" >"$IOT_NANO_LOCAL_PID_FILE"
}
