#!/usr/bin/env bash
set -euo pipefail

compose_file="infra/compose.yaml"
database_url="${DATABASE_URL:-postgres://iot:iot@127.0.0.1:54329/iot}"
events="${STRESS_EVENTS:-10000}"
devices="${STRESS_DEVICES:-100}"

docker compose --file "$compose_file" up --detach
until docker compose --file "$compose_file" exec --no-TTY timescaledb pg_isready -U iot -d iot >/dev/null; do
  sleep 1
done

STRESS_EVENTS="$events" STRESS_DEVICES="$devices" DATABASE_URL="$database_url" \
  cargo test -p iot-nano-core --test stress configured_events_drain_to_telemetry_and_alert_groups \
  -- --ignored --test-threads=1 --nocapture
