#!/usr/bin/env bash
set -euo pipefail

compose_file="infra/compose.yaml"
database_url="postgres://iot:iot@127.0.0.1:54329/iot"
work_dir="$(mktemp -d)"
stream_pid=""
core_pid=""
api_pid=""
mqttd_pid=""

cleanup() {
  local status=$?
  for pid in "$mqttd_pid" "$api_pid" "$core_pid" "$stream_pid"; do
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$mqttd_pid" "$api_pid" "$core_pid" "$stream_pid"; do
    if [ -n "$pid" ]; then
      wait "$pid" 2>/dev/null || true
    fi
  done
  if [ "$status" -eq 0 ]; then
    rm -rf "$work_dir"
  else
    printf 'E2E logs retained at %s\n' "$work_dir" >&2
  fi
}
trap cleanup EXIT

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

docker compose --file "$compose_file" up --detach --remove-orphans timescaledb
until docker compose --file "$compose_file" exec --no-TTY timescaledb \
  pg_isready -U iot -d iot >/dev/null; do
  sleep 1
done

cargo build --workspace
docker compose --file "$compose_file" exec --no-TTY timescaledb \
  psql -U iot -d iot -c \
  "TRUNCATE gateway_event_receipts, device_claim_codes, command_outbox, device_tokens, notification_outbox, alert_incidents, alert_rules, telemetry, devices CASCADE;"

openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -keyout "$work_dir/mqtt-key.pem" \
  -out "$work_dir/mqtt-cert.pem" \
  -subj '/CN=localhost' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign' \
  -addext 'extendedKeyUsage=serverAuth' >/dev/null 2>&1

IOT_NANO_STREAM_ADDRESS=127.0.0.1:18090 \
IOT_NANO_STREAM_DIR="$work_dir/stream" \
IOT_NANO_MQTTD_STREAM_SECRET="e2e-mqttd-stream-secret-must-have-32-bytes" \
IOT_NANO_CORE_STREAM_SECRET="e2e-core-stream-secret-must-have-32-bytes" \
target/debug/iot-nano-stream >"$work_dir/stream.log" 2>&1 &
stream_pid="$!"
wait_for_http http://127.0.0.1:18090/healthz

DATABASE_URL="$database_url" \
IOT_NANO_STREAM_URL=http://127.0.0.1:18090 \
IOT_NANO_CORE_STREAM_SECRET="e2e-core-stream-secret-must-have-32-bytes" \
IOT_NANO_API_CORE_SECRET="e2e-api-core-secret-must-have-32-bytes" \
IOT_NANO_MQTTD_INTERNAL_URL=http://127.0.0.1:18083 \
IOT_NANO_CORE_MQTTD_SECRET="e2e-core-mqttd-secret-must-have-32-bytes" \
target/debug/iot-nano-core --health-address 127.0.0.1:18081 \
  >"$work_dir/core.log" 2>&1 &
core_pid="$!"
wait_for_http http://127.0.0.1:18081/healthz

DATABASE_URL="$database_url" \
IOT_API_ADDRESS=127.0.0.1:18080 \
IOT_NANO_MQTTD_API_SECRET="e2e-mqttd-api-secret-must-have-32-bytes" \
IOT_NANO_API_MQTTD_SECRET="e2e-api-mqttd-secret-must-have-32-bytes" \
IOT_NANO_MQTTD_INTERNAL_URL=http://127.0.0.1:18083 \
IOT_NANO_CORE_URL=http://127.0.0.1:18081 \
IOT_NANO_API_CORE_SECRET="e2e-api-core-secret-must-have-32-bytes" \
target/debug/iot-nano-api >"$work_dir/api.log" 2>&1 &
api_pid="$!"
wait_for_http http://127.0.0.1:18080/healthz

IOT_MQTTD_PLAIN_ADDRESS=127.0.0.1:18882 \
IOT_MQTTD_TLS_ADDRESS=127.0.0.1:18883 \
IOT_MQTTD_MANAGEMENT_ADDRESS=127.0.0.1:18082 \
IOT_MQTTD_TRANSPORT_INTERNAL_ADDRESS=127.0.0.1:18083 \
IOT_MQTTD_TLS_CERT_PATH="$work_dir/mqtt-cert.pem" \
IOT_MQTTD_TLS_KEY_PATH="$work_dir/mqtt-key.pem" \
IOT_MQTTD_API_BASE_URL=http://127.0.0.1:18080 \
IOT_NANO_MQTTD_API_SECRET="e2e-mqttd-api-secret-must-have-32-bytes" \
IOT_NANO_API_MQTTD_SECRET="e2e-api-mqttd-secret-must-have-32-bytes" \
IOT_NANO_CORE_MQTTD_SECRET="e2e-core-mqttd-secret-must-have-32-bytes" \
IOT_NANO_STREAM_URL=http://127.0.0.1:18090 \
IOT_NANO_MQTTD_STREAM_SECRET="e2e-mqttd-stream-secret-must-have-32-bytes" \
target/debug/iot-nano-mqttd >"$work_dir/mqttd.log" 2>&1 &
mqttd_pid="$!"
wait_for_http http://127.0.0.1:18083/healthz

python3 scripts/rpc-e2e.py \
  --api-base-url http://127.0.0.1:18080 \
  --mqtt-host 127.0.0.1 \
  --mqtt-port 18883 \
  --ca-path "$work_dir/mqtt-cert.pem"
