#!/usr/bin/env bash
set -euo pipefail

root="${IOT_NANO_VERIFY_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# These paths make up the monolith production/deployment surface. The standalone
# broker unit is a separately retained package and is not a monolith deployment.
production_paths=(
  "$root/infra/compose.yaml"
  "$root/infra/compose.timescale.yaml"
  "$root/infra/docker/Dockerfile"
  "$root/infra/monolith"
  "$root/infra/systemd"
  "$root/scripts/e2e-local.sh"
  "$root/scripts/e2e-monolith.sh"
  "$root/scripts/install-mqttd-standalone.sh"
  "$root/scripts/install-raspberry-pi.sh"
)

expected_files=(
  "$root/infra/compose.yaml"
  "$root/infra/compose.timescale.yaml"
  "$root/infra/docker/Dockerfile"
  "$root/infra/systemd/iot-nano-monolith.service"
  "$root/scripts/e2e-local.sh"
  "$root/scripts/e2e-monolith.sh"
  "$root/scripts/install-mqttd-standalone.sh"
  "$root/scripts/install-raspberry-pi.sh"
)

expected_directories=(
  "$root/infra/monolith"
  "$root/infra/systemd"
)

retired_paths=(
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

for path in "${expected_directories[@]}"; do
  if [[ ! -d "$path" || ! -r "$path" ]]; then
    printf 'expected production/deployment path is missing or unreadable: %s\n' "$path" >&2
    exit 1
  fi
done

legacy_pattern='iot-nano-(api|core|stream|mqttd)|target/(debug|release)/iot-nano-(api|core|stream|mqttd)|/internal/|x-iot-nano-|IOT_NANO_(CORE_URL|STREAM_URL|MQTTD_INTERNAL_URL|MQTTD_API_SECRET|API_MQTTD_SECRET|MQTTD_STREAM_SECRET|CORE_STREAM_SECRET|API_CORE_SECRET|CORE_MQTTD_SECRET)'

check_for_matches() {
  local description="$1"
  local pattern="$2"
  local matches
  local status

  set +e
  matches="$(rg -n --glob '!iot-nano-mqttd-standalone.service' -- "$pattern" "${production_paths[@]}" 2>&1)"
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
      printf 'failed to scan production/deployment paths (rg exit %s):\n%s\n' \
        "$status" "$matches" >&2
      exit 1
      ;;
  esac
}

check_for_matches \
  'legacy runtime references found in production/deployment paths' \
  "$legacy_pattern"

for path in "${retired_paths[@]}"; do
  if [[ -e "$path" ]]; then
    printf 'retired legacy deployment asset remains: %s\n' "$path" >&2
    exit 1
  fi
done

container_pattern='(^|[[:space:]])(links|network_mode|container_name):|docker compose .*\\b(iot-nano-(api|core|stream|mqttd))\\b'
check_for_matches \
  'legacy container dependency found in production/deployment paths' \
  "$container_pattern"
