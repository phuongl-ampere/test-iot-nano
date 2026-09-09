#!/usr/bin/env bash
set -euo pipefail

if [ "${1:-}" = "--rpc" ]; then
  compose_file="infra/compose.yaml"
  database_url="postgres://iot:iot@127.0.0.1:54329/iot"
  work_dir="$(mktemp -d)"
  api_pid=""
  ingest_pid=""
  transport_pid=""

  cleanup_rpc() {
    for pid in "$transport_pid" "$ingest_pid" "$api_pid"; do
      if [ -n "$pid" ]; then
        kill "$pid" 2>/dev/null || true
      fi
    done
    for pid in "$transport_pid" "$ingest_pid" "$api_pid"; do
      if [ -n "$pid" ]; then
        wait "$pid" 2>/dev/null || true
      fi
    done
    rm -rf "$work_dir"
  }
  trap cleanup_rpc EXIT

  wait_for_http() {
    local url="$1"
    for _ in $(seq 1 100); do
      if curl --fail --silent "$url" >/dev/null; then
        return
      fi
      sleep 0.1
    done
    printf 'Timed out waiting for %s\n' "$url" >&2
    return 1
  }

  NANOMQ_CONFIG=./nanomq/nanomq.legacy.conf \
    docker compose --file "$compose_file" up --detach
  until docker compose --file "$compose_file" exec --no-TTY timescaledb \
    pg_isready -U iot -d iot >/dev/null; do
    sleep 1
  done

  cargo build --workspace
  docker compose --file "$compose_file" exec --no-TTY timescaledb \
    psql -U iot -d iot -c \
    "TRUNCATE command_outbox, device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices CASCADE;"

  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -keyout "$work_dir/mqtt-key.pem" \
    -out "$work_dir/mqtt-cert.pem" \
    -subj '/CN=localhost' \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    -addext 'keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign' \
    -addext 'extendedKeyUsage=serverAuth' >/dev/null 2>&1

  DATABASE_URL="$database_url" \
  IOT_API_ADDRESS=127.0.0.1:18080 \
  IOT_NANOMQ_AUTH_SECRET="e2e-nanomq-auth-secret-must-have-32-bytes" \
  IOT_MQTT_TRANSPORT_SECRET="e2e-mqtt-transport-secret-must-have-32-bytes" \
  IOT_MQTT_TRANSPORT_URL="http://127.0.0.1:18083" \
  target/debug/iot-api >"$work_dir/api.log" 2>&1 &
  api_pid="$!"
  wait_for_http http://127.0.0.1:18080/healthz

  DATABASE_URL="$database_url" \
  IOT_NANOMQ_WEBHOOK_SECRET="e2e-nanomq-webhook-secret-must-have-32-bytes" \
  IOT_MQTT_TRANSPORT_URL="http://127.0.0.1:18083" \
  IOT_MQTT_TRANSPORT_SECRET="e2e-mqtt-transport-secret-must-have-32-bytes" \
  IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET="e2e-transport-ingest-secret-must-have-32-bytes" \
  IOT_NANOMQ_WEBHOOK_INBOX_DIR="$work_dir/webhook-inbox" \
  IOT_LEGACY_MQTT_INGRESS=true \
  target/debug/iot-ingest \
    --broker-host 127.0.0.1 \
    --broker-port 1883 \
    --stream-dir "$work_dir/stream" \
    --health-address 127.0.0.1:18081 >"$work_dir/ingest.log" 2>&1 &
  ingest_pid="$!"
  wait_for_http http://127.0.0.1:18081/healthz

  IOT_MQTT_TRANSPORT_ADDRESS=127.0.0.1:18883 \
  IOT_MQTT_TRANSPORT_INTERNAL_ADDRESS=127.0.0.1:18083 \
  IOT_MQTT_TRANSPORT_TLS_CERT_PATH="$work_dir/mqtt-cert.pem" \
  IOT_MQTT_TRANSPORT_TLS_KEY_PATH="$work_dir/mqtt-key.pem" \
  IOT_MQTT_TRANSPORT_API_BASE_URL=http://127.0.0.1:18080 \
  IOT_MQTT_TRANSPORT_SECRET="e2e-mqtt-transport-secret-must-have-32-bytes" \
  IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_URL=http://127.0.0.1:18081/internal/mqtt-transport/telemetry \
  IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET="e2e-transport-ingest-secret-must-have-32-bytes" \
  target/debug/iot-mqtt-transport >"$work_dir/transport.log" 2>&1 &
  transport_pid="$!"
  wait_for_http http://127.0.0.1:18083/healthz

  python3 scripts/rpc-e2e.py \
    --api-base-url http://127.0.0.1:18080 \
    --mqtt-host 127.0.0.1 \
    --mqtt-port 18883 \
    --ca-path "$work_dir/mqtt-cert.pem"
  exit
fi

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
IOT_MQTT_TRANSPORT_URL="http://127.0.0.1:18083" \
IOT_MQTT_TRANSPORT_SECRET="e2e-mqtt-transport-secret-must-have-32-bytes" \
IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET="e2e-transport-ingest-secret-must-have-32-bytes" \
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
  psql -U iot -d iot -c "TRUNCATE command_outbox, device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices;
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
