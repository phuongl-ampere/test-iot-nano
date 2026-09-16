#!/usr/bin/env bash
set -euo pipefail

root="${IOT_NANO_VERIFY_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

expected_files=(
  "$root/infra/compose.yaml"
  "$root/infra/compose.timescale.yaml"
  "$root/infra/docker/Dockerfile"
  "$root/infra/monolith/monolith.env.example"
  "$root/infra/monolith/migrate.sh"
  "$root/infra/monolith/rollback.sh"
  "$root/infra/systemd/iot-nano-monolith.service"
  "$root/scripts/e2e-local.sh"
  "$root/scripts/e2e-monolith.sh"
  "$root/scripts/install-raspberry-pi.sh"
  "$root/services/iot-nano-monolith/Cargo.toml"
  "$root/services/iot-nano-monolith/src/adapters.rs"
  "$root/services/iot-nano-monolith/src/cache.rs"
  "$root/services/iot-nano-monolith/src/config.rs"
  "$root/services/iot-nano-monolith/src/lib.rs"
  "$root/services/iot-nano-monolith/src/main.rs"
  "$root/services/iot-nano-monolith/src/management.rs"
  "$root/services/iot-nano-monolith/src/readiness.rs"
  "$root/services/iot-nano-monolith/src/runtime.rs"
  "$root/services/iot-nano-api/Cargo.toml"
  "$root/services/iot-nano-api/src/lib.rs"
  "$root/services/iot-nano-core/Cargo.toml"
  "$root/services/iot-nano-core/src/lib.rs"
  "$root/services/iot-nano-stream/Cargo.toml"
  "$root/services/iot-nano-stream/src/lib.rs"
  "$root/services/iot-nano-mqttd/Cargo.toml"
  "$root/services/iot-nano-mqttd/src/lib.rs"
)
expected_directories=(
  "$root/infra"
  "$root/scripts"
  "$root/services/iot-nano-monolith"
  "$root/services/iot-nano-monolith/src"
  "$root/services/iot-nano-api/src"
  "$root/services/iot-nano-core/src"
  "$root/services/iot-nano-stream/src"
  "$root/services/iot-nano-mqttd/src"
)
retired_paths=(
  "$root/infra/dev/api.env"
  "$root/infra/dev/iot-nano-core.env"
  "$root/infra/dev/iot-nano-mqttd.env"
  "$root/infra/dev/iot-nano-stream.env"
  "$root/infra/systemd/iot-nano-api.service"
  "$root/infra/systemd/iot-nano-core.service"
  "$root/infra/systemd/iot-nano-mqttd.service"
  "$root/infra/systemd/iot-nano-mqttd-standalone.service"
  "$root/infra/systemd/iot-nano-stream.service"
  "$root/scripts/install-mqttd-standalone.sh"
  "$root/scripts/rpc-e2e.py"
  "$root/services/iot-nano-api/src/main.rs"
  "$root/services/iot-nano-api/src/core_client.rs"
  "$root/services/iot-nano-core/src/control.rs"
  "$root/services/iot-nano-core/src/stream_consumer.rs"
  "$root/services/iot-nano-mqttd/src/main.rs"
  "$root/contracts/internal-api-v1.json"
  "$root/contracts/stream-v1.json"
)

retired_binary_literal_pattern='iot-nano-(api|core|stream|mqttd)'
retired_source_pattern='(^|[=:\"[:space:]])/internal/|x-iot-nano-|IOT_NANO_(CORE_URL|STREAM_URL|MQTTD_INTERNAL_URL|MQTTD_API_SECRET|API_MQTTD_SECRET|MQTTD_STREAM_SECRET|CORE_STREAM_SECRET|API_CORE_SECRET|CORE_MQTTD_SECRET)[[:space:]]*[:=]'
deployment_files=()
source_files=()

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

check_path_components() {
  local path="$1"
  local component="$path"

  while :; do
    [[ ! -L "$component" ]] ||
      fail "symlinked production/deployment path found: $component"
    if [[ -d "$component" ]]; then
      [[ -r "$component" && -x "$component" ]] ||
        fail "unreadable production/deployment path found: $component"
    else
      [[ -r "$component" ]] ||
        fail "unreadable production/deployment path found: $component"
    fi
    [[ "$component" == "$root" ]] && break
    component="${component%/*}"
    [[ -n "$component" && "$component" != "$path" ]] ||
      fail "production/deployment path escapes verification root: $path"
  done
}

for path in "${expected_directories[@]}"; do
  [[ ! -L "$path" ]] ||
    fail "symlinked production/deployment path found: $path"
  [[ -d "$path" && -r "$path" && -x "$path" ]] ||
    fail "expected production/deployment path is missing or unreadable: $path"
  check_path_components "$path"
done
for path in "${expected_files[@]}"; do
  [[ ! -L "$path" ]] ||
    fail "symlinked production/deployment path found: $path"
  [[ -f "$path" && -r "$path" ]] ||
    fail "expected production/deployment path is missing or unreadable: $path"
  check_path_components "$path"
done

is_deployment_excluded() {
  case "$1" in
    "$root/scripts/stress-local.sh"|"$root/scripts/verify-no-legacy-runtime.sh"|"$root/scripts/test-verify-no-legacy-runtime.sh"|"$root/scripts/verify-failures.sh"|"$root/scripts/verify-monolith-topology.sh"|"$root/scripts/fixtures"/*)
      return 0
      ;;
    *) return 1 ;;
  esac
}

enumerate_regular_files() {
  local directory="$1"
  local kind="$2"
  local inventory
  local diagnostics
  local path
  local status

  inventory="$(mktemp "${TMPDIR:-/tmp}/iot-nano-verify.XXXXXX")" ||
    fail "failed to create file enumeration output for: $directory"
  diagnostics="$(mktemp "${TMPDIR:-/tmp}/iot-nano-verify.XXXXXX")" || {
    rm -f "$inventory"
    fail "failed to create file enumeration diagnostics for: $directory"
  }
  set +e
  find "$directory" -print0 >"$inventory" 2>"$diagnostics"
  status=$?
  set -e
  if ((status != 0)); then
    printf 'unreadable production/deployment path found while enumerating %s (find exit %s):\n' \
      "$directory" "$status" >&2
    cat "$diagnostics" >&2
    rm -f "$inventory" "$diagnostics"
    exit 1
  fi
  rm -f "$diagnostics"

  while IFS= read -r -d '' path; do
    [[ ! -L "$path" ]] ||
      fail "symlinked production/deployment path found: $path"
    if [[ -d "$path" ]]; then
      [[ -r "$path" && -x "$path" ]] ||
        fail "unreadable production/deployment path found: $path"
      continue
    fi
    [[ -f "$path" ]] || continue
    [[ -r "$path" ]] ||
      fail "unreadable production/deployment path found: $path"
    case "$kind" in
      deployment)
        is_deployment_excluded "$path" || deployment_files+=("$path")
        ;;
      source) source_files+=("$path") ;;
    esac
  done <"$inventory"
  rm -f "$inventory"
}

enumerate_regular_files "$root/infra" deployment
enumerate_regular_files "$root/scripts" deployment
for source_directory in \
  "$root/services/iot-nano-api/src" \
  "$root/services/iot-nano-core/src" \
  "$root/services/iot-nano-stream/src" \
  "$root/services/iot-nano-mqttd/src" \
  "$root/services/iot-nano-monolith/src"; do
  enumerate_regular_files "$source_directory" source
done

check_library_binary_targets() {
  local package
  local manifest

  for package in iot-nano-api iot-nano-core iot-nano-stream iot-nano-mqttd; do
    manifest="$root/services/$package/Cargo.toml"
    if rg -q '^\[\[bin\]\]' "$manifest"; then
      fail "library package declares a retired binary target: $manifest"
    fi
  done

  for package in iot-nano-api iot-nano-mqttd; do
    manifest="$root/services/$package/Cargo.toml"
    rg -q '^autobins[[:space:]]*=[[:space:]]*false$' "$manifest" ||
      fail "library package must disable inferred binaries: $manifest"
  done
}

check_normalized_binary_literals() {
  local path
  local continued
  local normalized
  local matches
  local status

  for path in "${deployment_files[@]}"; do
    continued="$(mktemp "${TMPDIR:-/tmp}/iot-nano-verify.XXXXXX")" ||
      fail "failed to create normalized deployment scan output for: $path"
    normalized="$(mktemp "${TMPDIR:-/tmp}/iot-nano-verify.XXXXXX")" || {
      rm -f "$continued"
      fail "failed to create normalized deployment scan output for: $path"
    }
    if ! LC_ALL=C perl -0pe 's/\\\r?\n//g' -- "$path" >"$continued"; then
      rm -f "$continued" "$normalized"
      fail "failed to normalize deployment file: $path"
    fi
    if ! LC_ALL=C tr -d "'\"\\\\" <"$continued" >"$normalized"; then
      rm -f "$continued" "$normalized"
      fail "failed to normalize deployment file: $path"
    fi
    rm -f "$continued"
    set +e
    matches="$(rg --text -n --no-filename -- "$retired_binary_literal_pattern" "$normalized" 2>&1)"
    status=$?
    set -e
    rm -f "$normalized"
    case "$status" in
      0)
        printf 'retired binary literal found in monolith deployment paths:\n%s:%s\n' \
          "$path" "$matches" >&2
        exit 1
        ;;
      1) ;;
      *) fail "failed to scan normalized deployment file: $path" ;;
    esac
  done
}

check_for_matches() {
  local description="$1"
  local pattern="$2"
  shift 2
  local matches
  local status

  (($# == 0)) && return
  set +e
  matches="$(rg --text -n -- "$pattern" "$@" 2>&1)"
  status=$?
  set -e
  case "$status" in
    0)
      printf '%s:\n%s\n' "$description" "$matches" >&2
      exit 1
      ;;
    1) ;;
    *) fail "failed to scan paths (rg exit $status): $matches" ;;
  esac
}

check_normalized_binary_literals
check_library_binary_targets
check_for_matches \
  'legacy runtime references found in library source paths' \
  "$retired_source_pattern" \
  "${source_files[@]}"

for path in "${retired_paths[@]}"; do
  [[ ! -e "$path" && ! -L "$path" ]] ||
    fail "retired legacy deployment asset remains: $path"
done
