#!/usr/bin/env bash
set -euo pipefail

database_url="${DATABASE_URL:-postgres://iot:iot@127.0.0.1:54329/iot}"

cargo test -p iot-core --test telemetry_contract
cargo test -p iot-nano-stream
cargo test -p iot-nano-core --test mqtt_consumer
DATABASE_URL="$database_url" cargo test -p iot-nano-core --test writer -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-nano-core --test alert --test notification -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-nano-core --test e2e -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-nano-api --test api -- --test-threads=1
