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

  grep -Fq "$expected" "$file" || fail "expected $file to contain $expected"
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
mkdir -p "$platform_root/internal" "$fixture/bin"
printf 'platform' >"$platform_root/platform.sqlite"
printf 'platform wal' >"$platform_root/platform.sqlite-wal"
printf 'platform shm' >"$platform_root/platform.sqlite-shm"
printf 'stream' >"$platform_root/internal/stream.sqlite"
printf 'mqttd' >"$platform_root/internal/mqttd.sqlite"
printf 'cache' >"$platform_root/internal/cache.sqlite"
printf 'lock' >"$platform_root/internal/instance.lock"
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

printf '%s\n' "$*" >>"$IOT_NANO_TEST_KILL_LOG"
rm -f "$IOT_NANO_TEST_LISTENER"
EOF
chmod +x "$fixture/bin/kill"

export PATH="$fixture/bin:$PATH"
export IOT_NANO_LOCAL_PLATFORM_ROOT="$platform_root"
export IOT_NANO_PUBLIC_HTTP_ADDRESS='127.0.0.1:18080'
export IOT_NANO_TEST_LISTENER="$fixture/listener"
export IOT_NANO_TEST_PLATFORM_PATH="$platform_root/platform.sqlite"
export IOT_NANO_TEST_KILL_LOG="$fixture/kill.log"
export IOT_NANO_LOCAL_KILL_BIN="$fixture/bin/kill"
touch "$IOT_NANO_TEST_LISTENER"

# shellcheck disable=SC1090
source "$helper"

if local_platform_require_reset; then
  fail 'reset acknowledgement must require --reset'
fi
local_platform_require_reset --reset

export IOT_NANO_TEST_LSOF_MODE=unverified
if local_platform_stop; then
  fail 'unverified listener must not be stopped'
fi
assert_missing "$IOT_NANO_TEST_KILL_LOG"

export IOT_NANO_TEST_LSOF_MODE=verified
local_platform_stop
assert_present "$IOT_NANO_TEST_KILL_LOG"

local_platform_clear_state
assert_missing "$platform_root/platform.sqlite"
assert_missing "$platform_root/platform.sqlite-wal"
assert_missing "$platform_root/platform.sqlite-shm"
assert_missing "$platform_root/internal"
assert_present "$platform_root/vault.key"
assert_present "$platform_root/mqtt-cert.pem"
assert_present "$platform_root/mqtt-key.pem"

assert_file_contains "$seed" 'IOT_NANO_SEED_CONTROLLER_USERNAME'
assert_file_contains "$seed" 'IOT_NANO_SEED_VIEWER_USERNAME'
assert_file_contains "$seed" 'IOT_NANO_SEED_UNASSIGNED_USERNAME'
assert_file_contains "$seed" "ensure_direct_share \"\$IOT_NANO_SEED_CONTROLLER_USERNAME\" control"
assert_file_contains "$seed" "ensure_direct_share \"\$IOT_NANO_SEED_VIEWER_USERNAME\" view"

printf 'test-local-platform-runtime: ok\n'
