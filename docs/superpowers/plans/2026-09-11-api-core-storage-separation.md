# API Core Storage Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `subagent-driven-development` or `executing-plans` task-by-task. Steps use
> checkbox syntax for tracking.

**Goal:** Give API and Core independent SQLite/Postgres stores, route every
data-plane API operation through Core, and delete the shared `iot-storage`
crate.

**Architecture:** API owns identity, metadata, grants, assets, profiles, and
device-token lifecycle. Core owns telemetry, rollups, gateway receipts, alert
rules/incidents, notifications, and commands. API authorizes callers and
resolves device IDs, then makes authenticated `CoreClient` requests; Core
never opens API storage or receives asset IDs.

**Tech Stack:** Rust 2024, Axum, Tokio, SQLx SQLite/Postgres, and existing
typed Core HTTP client.

## Global Constraints

- This is a development reset; do not add data migration, fallback, dual
  write, cross-store foreign key, or compatibility mode.
- Keep `x-iot-nano-api-core-secret` as the only API-to-Core credential.
- SQLite store ownership is detected from a service marker before a process
  opens a file. API and Core must reject a file owned by the other service.
- Postgres migrations run in isolated `iot_nano_api` and `iot_nano_core`
  schemas. Core tables do not reference API tables.
- Every behavior change starts with a focused failing test.

### Task 1: Establish Service-Owned Store Markers

**Files:**
- Create: `services/iot-nano-api/src/storage.rs`
- Create: `services/iot-nano-core/src/storage.rs`
- Modify: `services/iot-nano-api/src/{lib.rs,main.rs,routes.rs}`
- Modify: `services/iot-nano-core/src/{lib.rs,main.rs,control.rs}`
- Test: `services/iot-nano-api/src/main.rs`
- Test: `services/iot-nano-core/src/main.rs`

**Interfaces:**
- API exports `ApiSqliteStore::open(&StorageConfiguration)`.
- Core exports `CoreSqliteStore::open(&StorageConfiguration)`.
- Both stores expose `pool(&self) -> &SqlitePool`.

- [x] Write API and Core startup tests that create a file with the opposite
  service marker and assert `open` returns an ownership error.
- [x] Run those focused tests and verify they fail because both services still
  accept the shared store.
- [x] Implement `PRAGMA application_id` ownership markers, WAL setup, busy
  timeout, and service-local bootstrap schema in each store.
- [x] Run each focused test and verify it passes.

### Task 2: Move Core Data-Plane Persistence and Migrations

**Files:**
- Create: `services/iot-nano-core/migrations/`
- Modify: `services/iot-nano-core/src/{storage.rs,writer.rs,command.rs,alert.rs,control.rs,main.rs}`
- Modify: `services/iot-nano-core/tests/{alert.rs,command.rs,control.rs,notification.rs,stream_to_storage.rs,writer.rs}`

**Interfaces:**
- `CoreSqliteStore` owns telemetry, rollups, gateway receipts, alert state,
  notification outbox, and command outbox methods.
- `migrate_core(pool: &PgPool)` creates/selects `iot_nano_core` before running
  only Core migrations.

- [x] Write a Core SQLite test that confirms its schema lacks API metadata
  tables and persists a telemetry record with only `device_id` text.
- [x] Write a Postgres migration test that confirms Core tables are in
  `iot_nano_core` and no Core table has a foreign key to an API table.
- [x] Move Core-specific `SqliteStore` methods, types, errors, and SQLite DDL
  into `CoreSqliteStore`; remove `devices` foreign keys from Core schema.
- [x] Split Core SQL migrations from `db/migrations`, including telemetry,
  alerts, command outbox, and gateway receipts.
- [x] Update writer, command dispatcher, alert evaluator, notifications,
  control routes, and tests to use Core-local storage.
- [x] Run `cargo test -p iot-nano-core -- --test-threads=1`.

### Task 3: Move API Metadata Persistence and Migrations

**Files:**
- Create: `services/iot-nano-api/migrations/`
- Modify: `services/iot-nano-api/src/{storage.rs,auth.rs,device_tokens.rs,power_switcher.rs,resource_authorization.rs,routes.rs,main.rs}`
- Modify: `services/iot-nano-api/tests/{api.rs,sqlite_auth.rs}`

**Interfaces:**
- `ApiSqliteStore` owns users, sessions, metadata, assets, profiles, grants,
  token lifecycle, claims, shares, and audit records.
- `migrate_api(pool: &PgPool)` creates/selects `iot_nano_api` before running
  only API migrations.

- [x] Write an API SQLite test that confirms its schema lacks Core data-plane
  tables while bootstrap/login/token workflows still work.
- [x] Write a Postgres migration test that confirms API tables are in
  `iot_nano_api` and no API schema contains telemetry/alert/command tables.
- [ ] Move API-specific SQLite DDL and store setup into `ApiSqliteStore`.
- [ ] Split API SQL migrations from `db/migrations`; move direct metadata SQL
  to the API schema.
- [ ] Update API startup, state construction, auth, token, and metadata tests
  to use `ApiSqliteStore`.
- [ ] Run `cargo test -p iot-nano-api --test sqlite_auth -- --test-threads=1`.

### Task 4: Complete the Core HTTP Data-Plane API

**Files:**
- Modify: `contracts/internal-api-v1.json`
- Modify: `services/iot-nano-core/src/control.rs`
- Modify: `services/iot-nano-core/tests/control.rs`
- Modify: `services/iot-nano-api/src/core_client.rs`
- Modify: `services/iot-nano-api/tests/internal_core_client.rs`

**Interfaces:**
- Core accepts authorized requests containing device IDs, time bounds, query
  buckets, alert-rule/incident data, notification state, and command IDs.
- `CoreClient` exposes typed query/mutation methods for telemetry, rollups,
  alert rules/incidents, notifications, and command lifecycle.

- [ ] Write an API/Core two-database test that calls each typed Core operation
  with the API secret and asserts a wrong secret cannot read or mutate state.
- [ ] Add Core routes and typed request/response DTOs for missing data-plane
  operations.
- [ ] Add `CoreClient` methods with explicit validation, forbidden,
  not-found, conflict, and unavailable mappings.
- [ ] Run Core control and API client tests.

### Task 5: Remove API Direct Data-Plane Queries

**Files:**
- Modify: `services/iot-nano-api/src/{routes.rs,powermonitor.rs,core_client.rs}`
- Modify: `services/iot-nano-api/tests/{api.rs,sqlite_auth.rs,core_command_route.rs}`

**Interfaces:**
- API resolves authorized `device_id` values locally.
- API forwards only device IDs and user query parameters to Core.
- API does not contain telemetry, rollup, alert, notification, or command
  SQL/fallback helpers.

- [ ] Write a two-database route test proving telemetry, PowerMonitor,
  alerts, incidents, notifications, and command status succeed with data only
  in Core storage.
- [ ] Run it and verify it fails on the remaining API direct SQL paths.
- [ ] Replace route-level direct data-plane queries and optional Core fallback
  branches with typed Core calls.
- [ ] Keep API-only metadata authorization and filter Core results by locally
  authorized device IDs.
- [ ] Run API Core-route tests with separate SQLite and Postgres stores.

### Task 6: Delete Shared Storage and Prove Isolation

**Files:**
- Modify: `Cargo.toml`, `Cargo.lock`
- Delete: `crates/iot-storage/`
- Delete: `db/migrations/`
- Modify: `infra/{compose.yaml,dev/iot-nano-api.env,dev/iot-nano-core.env}`
- Modify: `scripts/{e2e-local.sh,verify-failures.sh}`
- Modify: `docs/iot-nano-four-service-architecture.md`

- [ ] Write a static ownership test that rejects `iot-storage` dependencies,
  shared migrations, and API data-plane SQL helpers.
- [ ] Remove `iot-storage` from manifests/workspace and delete it only after
  both services compile against their local stores.
- [ ] Configure API and Core deployment values with independent SQLite paths
  and Postgres schemas.
- [ ] Reset development state and run the four-service E2E against separate
  API/Core storage.
- [ ] Run `cargo fmt --all -- --check`, `cargo check --workspace`, all service
  suites, static ownership scan, Compose validation, and `scripts/e2e-local.sh`.
