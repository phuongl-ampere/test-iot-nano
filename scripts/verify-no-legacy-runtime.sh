#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# These are the files that can launch or package the production monolith.
# Library implementations and test fixtures intentionally remain outside this scan.
production_paths=(
  "$root/infra/compose.yaml"
  "$root/infra/compose.timescale.yaml"
  "$root/infra/docker/Dockerfile"
  "$root/infra/monolith"
  "$root/infra/systemd/iot-nano-monolith.service"
  "$root/scripts/e2e-local.sh"
  "$root/scripts/e2e-monolith.sh"
)

legacy_pattern='iot-nano-(api|core|stream|mqttd)|target/(debug|release)/iot-nano-(api|core|stream|mqttd)|/internal/|x-iot-nano-|IOT_NANO_(CORE_URL|STREAM_URL|MQTTD_INTERNAL_URL|MQTTD_API_SECRET|API_MQTTD_SECRET|MQTTD_STREAM_SECRET|CORE_STREAM_SECRET|API_CORE_SECRET|CORE_MQTTD_SECRET)'

if matches="$(rg -n -- "$legacy_pattern" "${production_paths[@]}" 2>/dev/null || true)" &&
  [[ -n "$matches" ]]; then
  printf 'legacy runtime references found in production/deployment paths:\n%s\n' "$matches" >&2
  exit 1
fi

container_pattern='(^|[[:space:]])(links|network_mode|container_name):|docker compose .*\\b(iot-nano-(api|core|stream|mqttd))\\b'
if matches="$(rg -n -- "$container_pattern" "${production_paths[@]}" 2>/dev/null || true)" &&
  [[ -n "$matches" ]]; then
  printf 'legacy container dependency found in production/deployment paths:\n%s\n' "$matches" >&2
  exit 1
fi
