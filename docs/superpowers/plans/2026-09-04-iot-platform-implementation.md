# IoT Telemetry Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` or `superpowers:executing-plans`
> to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Deliver a locally runnable IoT telemetry platform with an MQTT
ingestion service, a persistent SQLite queue, TimescaleDB storage, a device
simulator, and a basic monitoring dashboard.

**Architecture:** A Rust workspace owns the telemetry contract, the single
`iot-ingest` process, the HTTP API, and the device simulator. `iot-ingest`
subscribes to NanoMQ-compatible MQTT, commits events into a SQLite WAL queue,
and drains batches to TimescaleDB. A Next.js application consumes the Rust API
and renders device telemetry with ApexCharts.

**Tech Stack:** Rust 1.96, Tokio, Axum, SQLx, SQLite, PostgreSQL/TimescaleDB,
rumqttc, Next.js 16, React, ApexCharts, PlatformIO for ESP32 firmware.

## Global Constraints

- Support 1,000 devices, each publishing up to 10 messages per minute.
- Use topic `iot/v1/devices/{device_id}/telemetry`, MQTT QoS 1, and
  `retain=false`.
- Treat delivery as at-least-once and suppress database duplicates using
  `(event_at, device_id, boot_id, sequence)`.
- Commit a valid MQTT publish to SQLite before acknowledging it.
- Set SQLite `journal_mode=WAL` and `synchronous=FULL` on SSD storage.
- Batch at most 1,000 records or one second of queue latency per database
  transaction.
- Enforce a 12 GiB local queue cap and alert at 80 percent capacity.
- Keep production data on an SSD and provide NanoMQ ARM64 binary/systemd
  deployment assets.
- Use TDD: every behavior change starts with a test that fails for the
  intended missing behavior.

---

## File Structure

```text
Cargo.toml
crates/iot-core/                 Shared protocol, telemetry validation, queue
crates/iot-ingest/               MQTT to SQLite to PostgreSQL service
crates/iot-api/                  Axum API and TimescaleDB queries
crates/device-simulator/         MQTT publisher for local load tests
db/migrations/                   PostgreSQL/TimescaleDB migrations
infra/nanomq/                    NanoMQ configuration and systemd units
infra/compose.yaml               Local TimescaleDB and NanoMQ deployment
firmware/esp32/                  ESP32 PlatformIO project
web/                             Next.js monitoring dashboard
scripts/                         Local start, simulation, and test commands
```

### Task 1: Bootstrap Workspace and Shared Telemetry Contract

**Files:**
- Create: `Cargo.toml`
- Create: `crates/iot-core/Cargo.toml`
- Create: `crates/iot-core/src/lib.rs`
- Create: `crates/iot-core/src/telemetry.rs`
- Create: `crates/iot-core/tests/telemetry_contract.rs`
- Create: `README.md`

**Interfaces:**
- Produces `TelemetryEvent`, `TelemetryValidationError`, and
  `TelemetryEvent::validate_for_topic(&str) -> Result<(), TelemetryValidationError>`.
- The event serializes fields `schema_version`, `device_id`, `boot_id`,
  `sequence`, `event_at`, and `measurements`.

- [x] **Step 1: Write failing contract tests**

Test a valid telemetry event, topic/payload device ID mismatch, unsupported
schema version, and an empty measurements object.

- [x] **Step 2: Run the contract test**

Run: `cargo test -p iot-core --test telemetry_contract`

Expected: compilation failure because `iot-core` and `TelemetryEvent` do not
exist.

- [x] **Step 3: Implement the contract**

Create the Cargo workspace and use Serde types to deserialize/serialize
telemetry. Require schema version `1`, non-empty IDs, non-empty measurements,
and exact match between the topic suffix device ID and payload device ID.

- [x] **Step 4: Verify the contract**

Run: `cargo test -p iot-core --test telemetry_contract`

Expected: all contract tests pass.

### Task 2: Implement the SQLite WAL Local Queue

**Files:**
- Create: `crates/iot-core/src/queue.rs`
- Create: `crates/iot-core/tests/queue.rs`

**Interfaces:**
- Produces `LocalQueue::open(path, QueueLimits)`.
- Produces `enqueue(&TelemetryEvent, topic, payload)`,
  `lease_batch(limit, now)`, `complete(ids)`, `release_expired(now)`, and
  `QueueStats`.
- `QueueLimits` has `max_bytes: 12 * 1024 * 1024 * 1024`.

- [x] **Step 1: Write failing queue tests**

Test that enqueue survives reopening the database, lease expiry returns work to
ready state, completion removes only the completed batch, and over-capacity
enqueue returns `QueueError::CapacityExceeded`.

- [x] **Step 2: Run queue tests**

Run: `cargo test -p iot-core --test queue`

Expected: failure because the queue API does not exist.

- [x] **Step 3: Implement SQLite queue state transitions**

Initialize SQLite with WAL/FULL pragmas, use a transaction for enqueue and
queue byte accounting, atomically lease ready rows, and use a lease timeout
for recovery. Store original payload/topic and parsed identity fields.

- [x] **Step 4: Verify queue tests**

Run: `cargo test -p iot-core --test queue`

Expected: all queue tests pass.

### Task 3: Add TimescaleDB Schema and Ingest Writer

**Files:**
- Create: `db/migrations/0001_telemetry.sql`
- Create: `crates/iot-ingest/Cargo.toml`
- Create: `crates/iot-ingest/src/lib.rs`
- Create: `crates/iot-ingest/src/writer.rs`
- Create: `crates/iot-ingest/tests/writer.rs`

**Interfaces:**
- Consumes `LocalQueue` leases and `TelemetryEvent`.
- Produces `TelemetryWriter::flush_once() -> Result<FlushResult, WriterError>`.
- Inserts into `devices` and `telemetry`; duplicate event keys do not create
  duplicate rows.

- [x] **Step 1: Write failing writer tests**

Use a PostgreSQL test database URL. Test a successful batch is completed only
after commit and publishing the same event twice produces one telemetry row.

- [x] **Step 2: Run writer tests**

Run: `DATABASE_URL=... cargo test -p iot-ingest --test writer`

Expected: failure because migrations and `TelemetryWriter` do not exist.

- [x] **Step 3: Implement schema and writer**

Create `devices`, the Timescale hypertable, idempotent unique index, raw
retention/compression policies, and aggregate views. Implement 1,000 record/
one second batch flushing and retryable lease release on database errors.

- [x] **Step 4: Verify writer tests**

Run: `DATABASE_URL=... cargo test -p iot-ingest --test writer`

Expected: all writer tests pass.

### Task 4: Add MQTT Consumer, Service Runtime, and Device Simulator

**Files:**
- Create: `crates/iot-ingest/src/main.rs`
- Create: `crates/iot-ingest/src/mqtt.rs`
- Create: `crates/iot-ingest/tests/mqtt_queue.rs`
- Create: `crates/device-simulator/Cargo.toml`
- Create: `crates/device-simulator/src/main.rs`
- Create: `scripts/run-simulation.sh`

**Interfaces:**
- `iot-ingest` consumes `iot/v1/devices/+/telemetry` with QoS 1.
- `device-simulator --devices N --messages N --broker URL` publishes valid
  telemetry envelopes.
- The consumer writes SQLite before calling `AsyncClient::ack`.

- [x] **Step 1: Write failing MQTT-to-queue test**

Start a local MQTT broker on a test port, publish one valid QoS 1 event, and
assert the local queue contains it. Publish a mismatched topic/payload event
and assert it is not queued.

- [x] **Step 2: Run the MQTT test**

Run: `cargo test -p iot-ingest --test mqtt_queue`

Expected: failure because consumer/runtime APIs do not exist.

- [x] **Step 3: Implement runtime and simulator**

Configure rumqttc persistent sessions and manual acknowledgements. Enqueue
first, then ACK. Start the writer loop, health endpoint, and Prometheus
metrics. Implement the simulator with deterministic device IDs, UUID boot IDs,
and monotonic sequences.

- [x] **Step 4: Verify local MQTT behavior**

Run: `cargo test -p iot-ingest --test mqtt_queue`

Expected: the valid event reaches SQLite and the invalid event is rejected.

### Task 5: Add Deployment Assets and End-to-End Script

**Files:**
- Create: `infra/compose.yaml`
- Create: `infra/nanomq/nanomq.conf`
- Create: `infra/nanomq/iot-ingest.service`
- Create: `infra/nanomq/nanomq.service`
- Create: `scripts/e2e-local.sh`
- Create: `scripts/install-raspberry-pi.sh`

**Interfaces:**
- Compose exposes PostgreSQL/TimescaleDB and NanoMQ for local integration.
- `e2e-local.sh` starts dependencies, runs migrations and `iot-ingest`, runs
  the simulator, and asserts database rows exist.

- [x] **Step 1: Write failing end-to-end assertion**

Create a shell test that exits non-zero until a simulated device's telemetry
row is available in TimescaleDB.

- [x] **Step 2: Run the assertion**

Run: `scripts/e2e-local.sh`

Expected: failure before Compose configuration, migrations, and service
commands exist.

- [x] **Step 3: Implement deployment assets**

Pin a NanoMQ container image for development and install NanoMQ `0.25.6`
ARM64 SQLite package on Raspberry Pi. Define durable broker persistence,
the MQTT listener, and systemd restart policies.

- [x] **Step 4: Verify end-to-end data flow**

Run: `scripts/e2e-local.sh`

Expected: the script reports a non-zero telemetry count for simulated devices.

### Task 6: Implement Rust API

**Files:**
- Create: `crates/iot-api/Cargo.toml`
- Create: `crates/iot-api/src/main.rs`
- Create: `crates/iot-api/src/routes.rs`
- Create: `crates/iot-api/src/repository.rs`
- Create: `crates/iot-api/tests/api.rs`

**Interfaces:**
- `GET /healthz`
- `GET /api/devices`
- `GET /api/devices/{device_id}/telemetry?from=&to=&bucket=`
- `POST /api/devices/{device_id}/commands`

- [x] **Step 1: Write failing API tests**

Test health response, device list output, a time-bucketed telemetry response,
and rejection of an invalid command request.

- [x] **Step 2: Run API tests**

Run: `DATABASE_URL=... cargo test -p iot-api --test api`

Expected: failure because the API routes do not exist.

- [x] **Step 3: Implement Axum routes**

Use SQLx queries with validated ranges and buckets. Return JSON with stable
field names. Publish accepted command payloads to the device command topic.

- [x] **Step 4: Verify API tests**

Run: `DATABASE_URL=... cargo test -p iot-api --test api`

Expected: all API tests pass.

### Task 7: Implement Basic Next.js Dashboard

**Files:**
- Create: `web/package.json`
- Create: `web/app/layout.tsx`
- Create: `web/app/page.tsx`
- Create: `web/app/devices/[deviceId]/page.tsx`
- Create: `web/components/device-table.tsx`
- Create: `web/components/telemetry-chart.tsx`
- Create: `web/components/time-range-control.tsx`
- Create: `web/app/globals.css`
- Create: `web/tests/dashboard.spec.ts`

**Interfaces:**
- The dashboard reads `NEXT_PUBLIC_API_BASE_URL`.
- It presents device state, last-seen values, a selected device chart, and
  1-hour/24-hour/7-day time range controls.

- [x] **Step 1: Write failing UI tests**

Test the device table, the empty state, range selection changing the telemetry
request, and chart data rendering.

- [x] **Step 2: Run UI tests**

Run: `npm --prefix web test`

Expected: failure because the web application does not exist.

- [x] **Step 3: Implement dashboard**

Use a restrained operational-console layout: a compact top bar, device table,
detail panel, and clear chart canvas. Use ApexCharts for time series, keyboard
focus states, responsive layouts, and explicit loading/error/empty states.

- [x] **Step 4: Verify the web application**

Run: `npm --prefix web test`

Run: `npm --prefix web run build`

Expected: tests pass and the production build succeeds.

### Task 8: Implement ESP32 Firmware and Contract Tests

**Files:**
- Create: `firmware/esp32/platformio.ini`
- Create: `firmware/esp32/src/main.cpp`
- Create: `firmware/esp32/src/device_config.h`
- Create: `firmware/esp32/test/test_telemetry/test_main.cpp`
- Create: `firmware/esp32/README.md`

**Interfaces:**
- Provisioning starts when configuration is missing or reset is requested.
- Normal operation publishes the agreed telemetry envelope to the agreed topic.

- [x] **Step 1: Write failing firmware tests**

Test topic generation, a valid telemetry JSON envelope, and static-IP config
validation.

- [x] **Step 2: Run firmware tests**

Run: `pio test -d firmware/esp32 -e native`

Expected: failure before the firmware project exists.

- [x] **Step 3: Implement firmware**

Implement NVS configuration, AP provisioning server, DHCP/static IP handling,
Wi-Fi reconnect, MQTT QoS 1 publishing, and monotonic telemetry sequence.

- [x] **Step 4: Verify firmware build and tests**

Run: `pio test -d firmware/esp32 -e native`

Run: `pio run -d firmware/esp32 -e esp32dev`

Expected: native tests and ESP32 build both pass.

### Task 9: Run Failure and Load Verification

**Files:**
- Create: `scripts/verify-failures.sh`
- Create: `docs/operations.md`

**Interfaces:**
- The verification script tests duplicate events, ingest restart, temporary
  database unavailability, malformed messages, and a simulator load run.

- [x] **Step 1: Write failing verification checks**

Add checks that fail unless duplicate database rows remain at one and a queued
event drains after the database returns.

- [x] **Step 2: Run the checks**

Run: `scripts/verify-failures.sh`

Expected: failure before the failure harness exists.

- [x] **Step 3: Implement the verification harness**

Use Compose and the simulator to inject each condition, then query the
database and health endpoints. Document operational commands, alerts, SSD
requirements, NanoMQ installation, backup, and restore steps.

- [x] **Step 4: Run all verification**

Run: `cargo test --workspace`

Run: `npm --prefix web test`

Run: `npm --prefix web run build`

Run: `scripts/e2e-local.sh`

Run: `scripts/verify-failures.sh`

Expected: all commands succeed.

## Plan Review

Every design requirement maps to a task: device provisioning (Task 8), NanoMQ
binary/systemd deployment (Task 5), at-least-once SQLite-backed ingestion
(Tasks 2-4), TimescaleDB storage/retention (Task 3), simulator and end-to-end
testing (Tasks 4, 5, and 9), and Rust API plus Next.js/ApexCharts UI (Tasks 6
and 7). The plan deliberately keeps the initial queue inside one Rust service.
