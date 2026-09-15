#!/usr/bin/env bash
set -euo pipefail

root="${IOT_NANO_VERIFY_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Keep this list explicit so the verifier fails closed when a monolith
# production/deployment input is removed or becomes unreadable. The retained
# standalone MQTTD package and installer are intentionally outside this list.
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
)

deployment_paths=(
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
)

source_paths=("$root/services/iot-nano-monolith/src")

retired_paths=(
  "$root/infra/dev/api.env"
  "$root/infra/dev/iot-nano-core.env"
  "$root/infra/dev/iot-nano-mqttd.env"
  "$root/infra/dev/iot-nano-stream.env"
  "$root/infra/systemd/iot-nano-api.service"
  "$root/infra/systemd/iot-nano-core.service"
  "$root/infra/systemd/iot-nano-mqttd.service"
  "$root/infra/systemd/iot-nano-stream.service"
  "$root/scripts/rpc-e2e.py"
)

for path in "${expected_files[@]}"; do
  if [[ ! -f "$path" || ! -r "$path" ]]; then
    printf 'expected production/deployment path is missing or unreadable: %s\n' "$path" >&2
    exit 1
  fi
done

legacy_deployment_pattern='iot-nano-(api|core|stream|mqttd)|target/(debug|release)/iot-nano-(api|core|stream|mqttd)|/internal/|x-iot-nano-|IOT_NANO_(CORE_URL|STREAM_URL|MQTTD_INTERNAL_URL|MQTTD_API_SECRET|API_MQTTD_SECRET|MQTTD_STREAM_SECRET|CORE_STREAM_SECRET|API_CORE_SECRET|CORE_MQTTD_SECRET)|install-mqttd-standalone\.sh|iot-nano-mqttd-standalone\.service'
legacy_source_pattern='/internal/|x-iot-nano-|target/(debug|release)/iot-nano-(api|core|stream|mqttd)'

check_for_matches() {
  local description="$1"
  local pattern="$2"
  shift 2
  local matches
  local status

  set +e
  matches="$(rg -n -- "$pattern" "$@" 2>&1)"
  status=$?
  set -e

  case "$status" in
    0)
      printf '%s:\n%s\n' "$description" "$matches" >&2
      exit 1
      ;;
    1)
      ;;
    *)
      printf 'failed to scan paths (rg exit %s):\n%s\n' \
        "$status" "$matches" >&2
      exit 1
      ;;
  esac
}

check_for_matches \
  'legacy runtime references found in production/deployment paths' \
  "$legacy_deployment_pattern" \
  "${deployment_paths[@]}"

retired_environment_reference_pattern='infra/dev/(api|iot-nano-core|iot-nano-mqttd|iot-nano-stream)\.env'
check_for_matches \
  'retired four-service development environment reference found' \
  "$retired_environment_reference_pattern" \
  "${deployment_paths[@]}"

check_for_matches \
  'legacy runtime references found in monolith source' \
  "$legacy_source_pattern" \
  "${source_paths[@]}"

for path in "${retired_paths[@]}"; do
  if [[ -e "$path" ]]; then
    printf 'retired legacy deployment asset remains: %s\n' "$path" >&2
    exit 1
  fi
done

container_pattern='(^|[[:space:]])(links|network_mode|container_name):|docker compose .*\\b(iot-nano-(api|core|stream|mqttd))\\b'
check_for_matches \
  'legacy container dependency found in production/deployment paths' \
  "$container_pattern" \
  "${deployment_paths[@]}"
