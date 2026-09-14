#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_args=(--file "$root/infra/compose.yaml")
if [[ "${IOT_NANO_TIMESCALE_COMPOSE:-0}" == "1" ]]; then
  compose_args+=(--file "$root/infra/compose.timescale.yaml")
fi

docker compose "${compose_args[@]}" stop iot-nano-monolith
docker compose "${compose_args[@]}" run --rm --no-deps iot-nano-monolith --migrate-only
docker compose "${compose_args[@]}" up --detach iot-nano-monolith
