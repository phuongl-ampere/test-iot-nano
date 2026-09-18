#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
wrapper="$root/scripts/dev/cargo-lane.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

fail() {
  printf 'test-cargo-lane: %s\n' "$*" >&2
  exit 1
}

assert_equal() {
  local expected="$1"
  local actual="$2"
  local message="$3"

  if [[ "$actual" != "$expected" ]]; then
    printf 'test-cargo-lane: %s\nexpected: %s\nactual:   %s\n' \
      "$message" "$expected" "$actual" >&2
    exit 1
  fi
}

capture_value() {
  local key="$1"
  local capture="$2"

  sed -n "s/^${key}=//p" "$capture" | head -n 1
}

captured_args() {
  local capture="$1"

  awk 'found { print } /^args:$/ { found = 1 }' "$capture"
}

prepare_repository() {
  local repository="$1"

  mkdir -p "$repository/scripts/dev"
  git init --quiet "$repository"
  cp "$wrapper" "$repository/scripts/dev/cargo-lane.sh"
  chmod +x "$repository/scripts/dev/cargo-lane.sh"
}

run_lane() {
  local repository="$1"
  local lane="$2"
  local capture="$3"
  shift 3

  CAPTURE_FILE="$capture" \
    IOT_NANO_LANE_TARGET_ROOT="$fixture/targets" \
    IOT_NANO_LANE_LOG="$fixture/timing.log" \
    PATH="$fixture/bin:$PATH" \
    "$repository/scripts/dev/cargo-lane.sh" "$lane" -- "$@"
}

if [[ ! -x "$wrapper" ]]; then
  fail "expected executable wrapper at $wrapper"
fi

mkdir -p "$fixture/bin"
cat >"$fixture/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

: "${CAPTURE_FILE:?CAPTURE_FILE must be set}"
{
  printf 'target=%s\n' "$CARGO_TARGET_DIR"
  printf 'rustc_wrapper=%s\n' "${RUSTC_WRAPPER:-}"
  printf 'args:\n'
  printf '%s\n' "$@"
} >"$CAPTURE_FILE"
EOF
chmod +x "$fixture/bin/cargo"

cat >"$fixture/bin/sccache" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$fixture/bin/sccache"

repository_a="$fixture/repository-a"
repository_b="$fixture/repository-b"
prepare_repository "$repository_a"
prepare_repository "$repository_b"

command=(test -p iot-storage --test identity -- --test-threads=1)
run_lane "$repository_a" fast-storage "$fixture/a-first.capture" "${command[@]}"
run_lane "$repository_a" fast-storage "$fixture/a-second.capture" "${command[@]}"
run_lane "$repository_b" fast-storage "$fixture/b.capture" "${command[@]}"
run_lane "$repository_a" sqlite-contract "$fixture/a-sqlite.capture" "${command[@]}"

target_a_first="$(capture_value target "$fixture/a-first.capture")"
target_a_second="$(capture_value target "$fixture/a-second.capture")"
target_b="$(capture_value target "$fixture/b.capture")"
target_a_sqlite="$(capture_value target "$fixture/a-sqlite.capture")"

assert_equal "$target_a_first" "$target_a_second" \
  'the same worktree and lane must reuse its target directory'
[[ "$target_a_first" == "$fixture/targets/"* ]] || fail 'target must be beneath the configured lane target root'
[[ "$target_a_first" != "$target_b" ]] || fail 'unrelated worktrees must not share a target directory'
[[ "$target_a_first" != "$target_a_sqlite" ]] || fail 'different lanes must not share a target directory'

expected_args="$(printf '%s\n' "${command[@]}")"
assert_equal "$expected_args" "$(captured_args "$fixture/a-first.capture")" \
  'the cargo command after the delimiter must be preserved exactly'

CAPTURE_FILE="$fixture/sccache.capture" \
  IOT_NANO_LANE_TARGET_ROOT="$fixture/targets" \
  IOT_NANO_LANE_LOG="$fixture/timing.log" \
  IOT_NANO_USE_SCCACHE=1 \
  PATH="$fixture/bin:$PATH" \
  "$repository_a/scripts/dev/cargo-lane.sh" fast-storage -- check -p iot-storage

assert_equal "$fixture/bin/sccache" \
  "$(capture_value rustc_wrapper "$fixture/sccache.capture")" \
  'sccache must be enabled only when explicitly requested'

[[ -f "$fixture/timing.log" ]] || fail 'wrapper must append a timing log'
[[ "$(wc -l <"$fixture/timing.log" | tr -d ' ')" == 5 ]] || fail 'each lane invocation must append one timing entry'
rg -q 'lane=fast-storage' "$fixture/timing.log" || fail 'timing log must include the lane name'

printf 'test-cargo-lane: ok\n'
