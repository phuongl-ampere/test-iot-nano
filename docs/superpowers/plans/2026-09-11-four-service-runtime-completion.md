# Four-Service Runtime Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `subagent-driven-development` or `executing-plans` task-by-task. Steps use
> checkbox syntax for tracking.

**Goal:** Replace the transitional ingestion stack with a tested four-service
runtime without a legacy process, compatibility setting, shared data-plane
storage, or direct API data-plane query.

**Architecture:** Existing Stream HTTP append/group endpoints and the Core
control API become the only cross-service boundaries. MQTTD authorizes direct
and gateway events through API before appending to Stream. Core consumes only
Stream, and API calls Core for every data-plane operation.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx, SQLite, PostgreSQL/Timescale,
rumqttd, Docker Compose, and systemd.

## Global Constraints

- Reset development state instead of adding migration or compatibility code.
- Keep exactly four deployable services.
- Do not add a fallback endpoint, dual write, compatibility flag, or
  side-by-side broker.
- Use `x-iot-nano-mqttd-api-secret`,
  `x-iot-nano-mqttd-stream-secret`, `x-iot-nano-core-stream-secret`, and
  `x-iot-nano-api-core-secret` only.
- Each task starts with a focused failing test and ends with its focused test.

### Task 1: Normalize Target Names and Startup Configuration

**Files:**
- Modify: `services/iot-nano-core/{Cargo.toml,src/lib.rs,src/main.rs}`
- Modify: `services/iot-nano-mqttd/tests/broker_features.rs`
- Modify: `services/iot-nano-core/tests/*`

- [ ] Write Core startup tests that accept a remote Stream configuration using
  only target environment variables and reject each legacy variable.
- [ ] Run the focused Core binary tests and verify they fail against the
  current old webhook and transport arguments.
- [ ] Rename the Core library to `iot_nano_core`; remove predecessor
  compatibility settings, webhook settings, and retired transport settings
  from Core startup.
- [ ] Update MQTTD feature tests for the current endpoint shape and delete
  legacy endpoint activation coverage.
- [ ] Run `cargo test -p iot-nano-core --bin iot-nano-core` and
  `cargo test -p iot-nano-mqttd --test broker_features -- --test-threads=1`.

### Task 2: Enforce Directed Internal APIs

**Files:**
- Modify: `contracts/{internal-api-v1.json,stream-v1.json}`
- Modify: `services/iot-nano-stream/{src/http.rs,tests/service.rs}`
- Modify: `services/iot-nano-core/{src/control.rs,tests/control.rs}`
- Modify: `services/iot-nano-api/{src/core_client.rs,tests/internal_core_client.rs}`

- [ ] Write failing Stream tests proving the MQTTD append credential cannot
  claim or acknowledge a group and the Core group credential cannot append.
- [ ] Write failing API/Core tests that accept
  `x-iot-nano-api-core-secret` and reject `x-iot-nano-core-secret`.
- [ ] Split Stream HTTP credentials by append and group routes.
- [ ] Rename `CoreCommandClient` to a typed `CoreClient`, require the Core
  URL/secret at API startup, and map Core validation, conflict, not-found, and
  unavailable results explicitly.
- [ ] Run Stream service, Core control, and API client tests.

### Task 3: Move Every Data-Plane API Operation to Core

**Files:**
- Modify: `services/iot-nano-core/{src/control.rs,tests/control.rs}`
- Modify: `services/iot-nano-api/{src/core_client.rs,src/routes.rs,src/powermonitor.rs}`
- Modify: `services/iot-nano-api/tests/{api.rs,sqlite_auth.rs}`

- [ ] Write a failing two-database integration test that proves API telemetry,
  command, alert, and notification requests use an authenticated Core fixture.
- [ ] Add Core endpoints and typed client methods for telemetry buckets/raw
  records, latest measurements, alerts, incidents, notifications, and command
  lifecycle.
- [ ] Have API resolve asset descendants and metadata authorization locally,
  then send authorized device IDs to Core; Core must never receive asset IDs
  or access API storage.
- [ ] Remove every direct API query/helper for telemetry, rollups, alerts,
  notification outbox, and `command_outbox`, including optional Core fallback
  branches.
- [ ] Run API and Core suites with separate disposable SQLite and Timescale
  storage.

### Task 4: Use Stream-Only Direct and Gateway Ingestion

**Files:**
- Modify: `contracts/gateway-telemetry-v1.json`
- Modify: `services/iot-nano-mqttd/{src/transport.rs,tests/transport/http_adapters.rs}`
- Modify: `services/iot-nano-api/{src/routes.rs,tests/api.rs,tests/sqlite_auth.rs}`
- Modify: `services/iot-nano-core/{src/stream_consumer.rs,tests/stream_to_storage.rs}`
- Delete: `services/iot-nano-core/{src/webhook.rs,tests/webhook.rs}`

- [ ] Write a failing MQTTD test for authorized gateway child append and an
  unauthorized-child rejection before Stream durability.
- [ ] Write a failing Core test proving a gateway Stream record commits
  idempotency and storage before its group acknowledgement.
- [ ] Return minimal typed direct/gateway identity from API authorization.
- [ ] Make MQTTD construct direct and gateway versioned Stream messages after
  authorization and preserve MQTT acknowledgement after Stream append.
- [ ] Delete Core webhook workers, routes, spool, secrets, inbox support, and
  all hybrid uplink tests.
- [ ] Run MQTTD transport, Stream-to-Core, and Core integration tests.

### Task 5: Split API and Core Storage

**Files:**
- Create: `db/{api-migrations,core-migrations}/`
- Create: `services/iot-nano-api/src/storage/`
- Create: `services/iot-nano-core/src/storage/`
- Modify: service manifests and startup files
- Delete: `crates/iot-storage/`

- [ ] Write failing tests that reject an API process opening a Core database
  path and a Core process opening an API database path.
- [ ] Create API migrations for users, sessions, metadata, assets, profiles,
  ownership, grants, tokens, and audit records only.
- [ ] Create Core migrations for telemetry, rollups, alerts, outbox, and
  commands only, without foreign keys to API tables.
- [ ] Move SQLite/Postgres adapters into their owner service and delete
  `iot-storage`, predecessor aliases, and unneeded `iot-core` runtime DTOs.
- [ ] Run the service suites against independently configured SQLite and
  Timescale stores.

### Task 6: Define MQTTD's Supported Profile

**Files:**
- Modify: `services/iot-nano-mqttd/{src/config.rs,src/main.rs,src/storage.rs}`
- Modify: `services/iot-nano-mqttd/tests/{config.rs,persistence.rs,protocol.rs,listeners.rs}`
- Modify: `infra/dev/iot-nano-mqttd.env`

- [ ] Write failing startup tests for valid SQLite persistence and invalid
  storage failure before any public listener binds.
- [ ] Add restart acceptance tests for retained messages, persistent sessions,
  offline QoS 1/QoS 2, QoS 2 recovery, expiry, and duplicate packets.
- [ ] Delete unsupported bridge, webhook, rule, shared-subscription,
  WebSocket, and last-will configuration/runtime paths unless an E2E test
  covers the capability.
- [ ] Make readiness depend on broker storage, TLS, API authorization, and
  Stream uplink availability.
- [ ] Run `cargo test -p iot-nano-mqttd -- --test-threads=1`.

### Task 7: Deploy and Prove Only Four Services

**Files:**
- Modify: `infra/compose.yaml`
- Modify: `infra/dev/iot-nano-*.env`
- Modify: `infra/systemd/iot-nano-*.service`
- Modify: `scripts/{e2e-local.sh,install-raspberry-pi.sh,verify-failures.sh}`
- Modify: `README.md`, `docs/operations.md`,
  `docs/iot-nano-four-service-architecture.md`
- Delete: retired broker configuration, predecessor environment files, and
  retired protocol scripts

- [ ] Write a failing deployment assertion requiring Compose to expose exactly
  TimescaleDB plus Stream, Core, API, and MQTTD with health checks.
- [ ] Replace legacy assets with target-only volume, TLS, dependency, and
  directed-secret configuration.
- [ ] Rebuild E2E around disposable service state and assert direct/gateway
  telemetry, commands and responses, rotation/revocation, alerts,
  notifications, TLS/plaintext parity, and broker restart recovery.
- [ ] Add a static scan that fails on legacy process names, headers,
  environment variables, routes, or shared data-plane paths.
- [ ] Run all required verification commands from the authority document.
