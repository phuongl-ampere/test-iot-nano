#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
helper="$root/scripts/dev/local-platform-runtime.sh"
seed="$root/scripts/dev/seed-local-platform.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

fail() {
  printf 'test-local-platform-runtime: %s\n' "$*" >&2
  exit 1
}

assert_present() {
  [[ -e "$1" ]] || fail "expected $1 to exist"
}

assert_missing() {
  [[ ! -e "$1" ]] || fail "expected $1 to be absent"
}

assert_file_contains() {
  local file="$1"
  local expected="$2"

  grep -Fq -- "$expected" "$file" || fail "expected $file to contain $expected"
}

assert_file_not_contains() {
  local file="$1"
  local unexpected="$2"

  if grep -Fq -- "$unexpected" "$file"; then
    fail "expected $file not to contain $unexpected"
  fi
}

seed_output=''
seed_status=0
if seed_output="$(IOT_NANO_ALLOW_LOCAL_SEED=1 \
  IOT_NANO_LOCAL_SEED_FILE="$fixture/missing-seed.env" \
  "$seed" 2>&1)"; then
  seed_status=0
else
  seed_status=$?
fi
[[ "$seed_status" == 2 ]] || fail 'seed without --reset must exit 2'
[[ "$seed_output" == *"usage: seed-local-platform.sh --reset"* ]] || \
  fail 'seed without --reset must print reset usage before reading configuration'

platform_root="$fixture/local-platform"
mkdir -p "$platform_root/internal" "$platform_root/ota/test-tenant" "$fixture/bin"
printf 'platform' >"$platform_root/platform.sqlite"
printf 'platform wal' >"$platform_root/platform.sqlite-wal"
printf 'platform shm' >"$platform_root/platform.sqlite-shm"
printf 'stream' >"$platform_root/internal/stream.sqlite"
printf 'mqttd' >"$platform_root/internal/mqttd.sqlite"
printf 'cache' >"$platform_root/internal/cache.sqlite"
printf 'lock' >"$platform_root/internal/instance.lock"
printf 'firmware' >"$platform_root/ota/test-tenant/firmware.bin"
printf 'vault' >"$platform_root/vault.key"
printf 'certificate' >"$platform_root/mqtt-cert.pem"
printf 'private key' >"$platform_root/mqtt-key.pem"

cat >"$fixture/bin/lsof" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ " $* " == *' -t '* && " $* " == *' -iTCP:'* ]]; then
  if [[ -f "$IOT_NANO_TEST_LISTENER" ]]; then
    printf '4242\n'
  fi
  exit 0
fi

if [[ " $* " == *' -p 4242 '* ]]; then
  if [[ "${IOT_NANO_TEST_LSOF_MODE:-verified}" == verified ]]; then
    printf 'iot-nano-monolith 4242 test txt REG 1,1 0 1 %s\n' "$IOT_NANO_TEST_BINARY_PATH"
    printf 'iot-nano-monolith 4242 test 11u REG 1,1 0 1 %s\n' "$IOT_NANO_TEST_PLATFORM_PATH"
  else
    printf 'other-process 4242 test 11u REG 1,1 0 1 /tmp/other.sqlite\n'
  fi
fi
EOF
chmod +x "$fixture/bin/lsof"

cat >"$fixture/bin/kill" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == -0 ]]; then
  [[ -f "$IOT_NANO_TEST_LISTENER" ]]
  exit
fi
printf '%s\n' "$*" >>"$IOT_NANO_TEST_KILL_LOG"
printf 'kill %s\n' "$*" >>"$IOT_NANO_TEST_LIFECYCLE_LOG"
rm -f "$IOT_NANO_TEST_LISTENER"
EOF
chmod +x "$fixture/bin/kill"

cat >"$fixture/bin/launchctl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"$IOT_NANO_TEST_LAUNCHCTL_LOG"
printf 'launchctl %s\n' "$*" >>"$IOT_NANO_TEST_LIFECYCLE_LOG"
EOF
chmod +x "$fixture/bin/launchctl"

export PATH="$fixture/bin:$PATH"
export IOT_NANO_LOCAL_PLATFORM_ROOT="$platform_root"
export IOT_NANO_HTTP_ADDRESS='127.0.0.1:18081'
export IOT_NANO_TEST_LISTENER="$fixture/listener"
export IOT_NANO_TEST_PLATFORM_PATH="$platform_root/platform.sqlite"
export IOT_NANO_TEST_KILL_LOG="$fixture/kill.log"
export IOT_NANO_TEST_LIFECYCLE_LOG="$fixture/lifecycle.log"
export IOT_NANO_LOCAL_KILL_BIN="$fixture/bin/kill"
export IOT_NANO_TEST_LAUNCHCTL_LOG="$fixture/launchctl.log"
export IOT_NANO_LOCAL_LAUNCHCTL_BIN="$fixture/bin/launchctl"
touch "$IOT_NANO_TEST_LISTENER"

# shellcheck disable=SC1090
source "$helper"
local_platform_configure
export IOT_NANO_TEST_BINARY_PATH="$IOT_NANO_LOCAL_BINARY_PATH"

if local_platform_require_reset 2>"$fixture/usage.err"; then
  fail 'reset acknowledgement must require --reset'
fi
local_platform_require_reset --reset

export IOT_NANO_TEST_LSOF_MODE=unverified
if local_platform_stop 2>"$fixture/unverified.err"; then
  fail 'unverified listener must not be stopped'
fi
assert_missing "$IOT_NANO_TEST_KILL_LOG"

export IOT_NANO_TEST_LSOF_MODE=verified
local_platform_stop
assert_present "$IOT_NANO_TEST_KILL_LOG"
assert_present "$IOT_NANO_LOCAL_OWNER_FILE"
assert_file_contains "$IOT_NANO_LOCAL_OWNER_FILE" "$root"
first_lifecycle_action="$(head -n 1 "$IOT_NANO_TEST_LIFECYCLE_LOG")"
[[ "$first_lifecycle_action" == 'launchctl remove '* ]] || \
  fail 'launchd job must be removed before terminating the local monolith'

local_platform_clear_state
assert_missing "$platform_root/platform.sqlite"
assert_missing "$platform_root/platform.sqlite-wal"
assert_missing "$platform_root/platform.sqlite-shm"
assert_missing "$platform_root/internal"
assert_missing "$platform_root/ota"
assert_present "$platform_root/vault.key"
assert_present "$platform_root/mqtt-cert.pem"
assert_present "$platform_root/mqtt-key.pem"

IOT_NANO_HTTP_ADDRESS='0.0.0.0:18081'
if local_platform_preflight 2>"$fixture/non-loopback.err"; then
  fail 'non-loopback runtime bindings must be rejected before reset'
fi
IOT_NANO_HTTP_ADDRESS='127.0.0.1:18081'

assert_file_contains "$seed" 'IOT_NANO_SEED_CONTROLLER_USERNAME'
assert_file_contains "$seed" 'IOT_NANO_SEED_VIEWER_USERNAME'
assert_file_contains "$seed" 'IOT_NANO_SEED_UNASSIGNED_USERNAME'
assert_file_contains "$seed" "ensure_direct_share \"\$IOT_NANO_SEED_CONTROLLER_USERNAME\" control"
assert_file_contains "$seed" "ensure_direct_share \"\$IOT_NANO_SEED_VIEWER_USERNAME\" view"
assert_file_contains "$seed" '"$management_url/api/v1/tenant/auth/login"'
assert_file_contains "$seed" "require_status \"\$tenant_login_status\" 200 'Tenant Account login'"
assert_file_contains "$seed" "--arg tenant_slug \"\$IOT_NANO_SEED_TENANT_SLUG\""
assert_file_contains "$seed" "'{tenant_slug: \$tenant_slug, password: \$password}'"
assert_file_contains "$helper" '"$IOT_NANO_LOCAL_LAUNCHCTL_BIN" submit'
assert_file_contains "$helper" 'IOT_NANO_LOCAL_SERVICE_LABEL'
assert_file_contains "$helper" 'IOT_NANO_LOCAL_CARGO_BIN_DIR'
assert_file_contains "$helper" 'IOT_NANO_HTTPS_ENABLED'
assert_file_contains "$helper" 'IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS'
assert_file_contains "$helper" 'export IOT_NANO_LANE_TARGET_ROOT=%q'
assert_file_contains "$helper" "cd %q\\n' \"\$local_platform_helper_root\""
assert_file_contains "$helper" 'IOT_NANO_LOCAL_STARTUP_ATTEMPTS'
assert_file_contains "$helper" 'local_platform_wait_for_http'
assert_file_contains "$helper" 'local_platform_build'
assert_file_contains "$helper" 'build -p iot-nano-monolith'
assert_file_contains "$helper" "exec %q\\n' \"\$IOT_NANO_LOCAL_BINARY_PATH\""
assert_file_contains "$seed" 'management_url="${IOT_NANO_HTTP_URL:-http://127.0.0.1:18081}"'
assert_file_not_contains "$seed" '/domain-profiles'
assert_file_contains "$seed" 'IOT_NANO_SEED_CONTROLLER_USERNAME:=seed-controller'
assert_file_contains "$seed" 'IOT_NANO_SEED_UNASSIGNED_USERNAME:=seed-unassigned'
assert_file_contains "$seed" 'IOT_NANO_SEED_VIEWER_USERNAME:=${IOT_NANO_SEED_RECIPIENT_USERNAME:-seed-viewer}'
assert_file_contains "$seed" 'require_seed_variables'
assert_file_contains "$seed" 'local_platform_preflight'
assert_file_contains "$seed" '/api/v1/management/profiles/device-profiles'
assert_file_contains "$seed" '/api/v1/management/profiles/asset-profiles'
assert_file_contains "$seed" "'Power Meter'"
assert_file_contains "$seed" "'Power Farm'"
assert_file_contains "$seed" "'Power Zone'"
assert_file_contains "$seed" 'asset_profile_id: $asset_profile_id'
assert_file_contains "$seed" 'device_profile_id: $device_profile_id'
assert_file_contains "$seed" 'ensure_high_power_alert_rule'
assert_file_contains "$seed" '"$management_url/api/v1/management/alert-rules"'
assert_file_contains "$seed" 'name: "High active power"'
assert_file_contains "$seed" 'metric_key: "power_w"'
assert_file_contains "$seed" 'rule_type: "event_threshold"'
assert_file_contains "$seed" 'threshold: 500'
assert_file_contains "$seed" 'ensure_high_power_alert_rule "$device_id"'

printf 'test-local-platform-runtime: ok\n'
