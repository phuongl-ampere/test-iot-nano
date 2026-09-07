#!/usr/bin/env bash
set -euo pipefail

database_url="${DATABASE_URL:-postgres://iot:iot@127.0.0.1:54329/iot}"

cargo test -p iot-core --test telemetry_contract
cargo test -p iot-stream
cargo test -p iot-ingest --test mqtt_consumer
DATABASE_URL="$database_url" cargo test -p iot-ingest --test writer -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-ingest --test alert --test notification -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-ingest --test e2e -- --test-threads=1
DATABASE_URL="$database_url" cargo test -p iot-api --test api -- --test-threads=1
npm --prefix web test
