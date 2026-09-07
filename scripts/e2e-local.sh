#!/usr/bin/env bash
set -euo pipefail

compose_file="infra/compose.yaml"
database_url="postgres://iot:iot@127.0.0.1:54329/iot"
stream_dir="$(mktemp -d)"
ingest_pid=""

cleanup() {
  if [ -n "$ingest_pid" ]; then
    kill "$ingest_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

NANOMQ_CONFIG=./nanomq/nanomq.legacy.conf \
  docker compose --file "$compose_file" up --detach
until docker compose --file "$compose_file" exec --no-TTY timescaledb pg_isready -U iot -d iot >/dev/null; do
  sleep 1
done

cargo build --workspace
DATABASE_URL="$database_url" \
IOT_NANOMQ_WEBHOOK_SECRET="e2e-nanomq-webhook-secret-must-have-32-bytes" \
IOT_NANOMQ_WEBHOOK_INBOX_DIR="$stream_dir/webhook-inbox" \
IOT_LEGACY_MQTT_INGRESS=true \
target/debug/iot-ingest \
  --broker-host 127.0.0.1 \
  --broker-port 1883 \
  --stream-dir "$stream_dir" \
  --health-address 127.0.0.1:18081 &
ingest_pid="$!"
sleep 2

docker compose --file "$compose_file" exec --no-TTY timescaledb \
  psql -U iot -d iot -c "TRUNCATE device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices;
INSERT INTO alert_rules (
  id, name, metric_key, rule_type, comparison, threshold, for_seconds,
  resolve_after_seconds, reopen_grace_seconds, severity, reminder_interval_seconds
) VALUES (
  '00000000-0000-0000-0000-000000000001', 'Warm device', 'temperature_c',
  'event_threshold', 'gt', 20.0, 0, 300, 3600, 'warning', 86400
);"

target/debug/device-simulator \
  --broker-host 127.0.0.1 \
  --broker-port 1883 \
  --devices 4 \
  --messages-per-device 4 \
  --interval-ms 200
sleep 2

count="$(docker compose --file "$compose_file" exec --no-TTY timescaledb \
  psql -U iot -d iot --tuples-only --no-align -c 'SELECT COUNT(*) FROM telemetry')"
incidents="$(docker compose --file "$compose_file" exec --no-TTY timescaledb \
  psql -U iot -d iot --tuples-only --no-align -c "SELECT COUNT(*) FROM alert_incidents WHERE status = 'open'")"
test "$count" = "16"
test "$incidents" = "4"
printf 'End-to-end telemetry rows: %s; open incidents: %s\n' "$count" "$incidents"
