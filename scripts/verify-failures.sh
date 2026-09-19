#!/usr/bin/env bash
set -euo pipefail

database_url="${DATABASE_URL:-postgres://iot:iot@127.0.0.1:54329/iot}"

cargo test -p iot-nano-foundation --test telemetry_contract
cargo test -p iot-nano-stream
cargo test -p iot-nano-core --test mqtt_consumer
DATABASE_URL="$database_url" cargo test -p iot-nano-core --test platform_writer -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-nano-core --test platform_alert --test notification -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-nano-core --test runtime -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-nano-api --test public_v1 -- --test-threads=1
