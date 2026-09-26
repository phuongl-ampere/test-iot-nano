# Hard-Cutover Legacy Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove inactive user-application and database legacy code, then divide the remaining monolith and storage implementations into maintainable private modules without changing current platform behavior.

**Architecture:** This is a development-only hard cutover. Fresh SQLite and Timescale schemas omit removed concepts; existing local data must be reset rather than migrated. `iot-storage` and monolith management retain their current public facades while their private source is split by responsibility.

**Tech Stack:** Rust 2024, SQLx SQLite/Timescale, Axum, Askama, Cargo workspace, Next.js/Vitest.

## Global Constraints

- Do not add compatibility migrations, dual reads/writes, old request fields, or fallback routes.
- Keep `iot_storage::*`, `ManagementSessionRouter`, and `bootstrap_system` public paths stable except for explicitly removed legacy types/functions.
- Do not add a crate or change resource permissions, device claim, MQTT, or PowerMonitor behavior.
- Preserve one `management_sessions` integration test harness; use Rust submodules rather than new integration-test roots.
- Stage only cleanup files; preserve every pre-existing unrelated working-tree change.
- Never print, commit, or move local secret values into source.

---

### Task 1: Prove and remove the legacy user-application contract

**Files:**
- Modify: `crates/iot-storage/src/lib.rs`
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Modify: `crates/iot-storage/src/management.rs`
- Modify: `services/iot-nano-api/src/auth.rs`
- Modify: `services/iot-nano-api/src/lib.rs`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: affected tests under `crates/iot-storage/tests/`, `services/iot-nano-api/tests/`, and `services/iot-nano-monolith/tests/`

**Interfaces:**
- Removes `default_app`, `granted_apps`, `user_app_grants`, `api_access_tokens`, legacy `Role`, legacy `AuthenticatedUser`, and generic credential helpers that expose them.
- Keeps `authenticate_system_account`, `authenticate_tenant_account`, `authenticate_user_account`, and `PlatformAccountCredential` for current session/OAuth flows.
- `CreateManagementUser` becomes `{ tenant_id, username, password_hash }`.
- `UpdateManagementUser` retains only still-active mutable fields such as `role`; capability replacement remains its dedicated endpoint.

- [ ] **Step 1: Write red contract tests**

Add assertions that a created/listed management user serializes username, role, account class, and capabilities but not `default_app` or `granted_apps`; assert the management create/update JSON body rejects those removed fields through `#[serde(deny_unknown_fields)]`. Add schema assertions that fresh SQLite and Timescale install SQL contain none of:

```text
default_app
user_app_grants
api_access_tokens
```

- [ ] **Step 2: Run focused red tests**

Run:

```sh
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-storage --test management_users
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-nano-monolith --test management_sessions management_user -- --nocapture
```

Expected: failures prove the old fields are still accepted or emitted.

- [ ] **Step 3: Remove the active contract and schema**

Remove `default_app` from both `users` DDL definitions; remove the complete `user_app_grants` and `api_access_tokens` DDL/index blocks; remove their SQL queries and validation helpers. Delete old auth types/functions only after `rg` confirms all current callers use account-specific authentication. Create User rows with no app-selection fields and continue inserting exactly:

```rust
[UserCapability::CreateAssets, UserCapability::ClaimDevices]
```

Make management user request structs reject unknown JSON fields and return only active fields. Update seed/e2e fixture JSON accordingly.

- [ ] **Step 4: Run focused green tests**

Run:

```sh
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-storage --test management_users
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-storage --test migration_safety
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-nano-api --test tenant_auth
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-nano-monolith --test management_sessions
```

Expected: all pass; fresh-install schema has no removed identifier.

- [ ] **Step 5: Commit semantic removal**

```sh
git add crates/iot-storage services/iot-nano-api services/iot-nano-monolith
git commit -m "refactor: remove legacy user application contracts"
```

### Task 2: Remove inactive migration and workspace artifacts

**Files:**
- Delete: `db/migrations/`
- Delete: `crates/iot-sqldb-common/`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/iot-storage/tests/migration_safety.rs`
- Modify: stale current-architecture docs under `docs/superpowers/plans/`

**Interfaces:**
- `cargo metadata --no-deps` contains no `iot-sqldb-common` package.
- Only `crates/iot-storage/migrations/0001_platform.sql` and private SQLite schema initialization are active schema sources.

- [ ] **Step 1: Write red static ownership test**

Extend migration-safety/static test coverage to inspect repository paths and assert that `db/migrations` and `crates/iot-sqldb-common` do not exist, while `crates/iot-storage/migrations/0001_platform.sql` does.

- [ ] **Step 2: Run the ownership test and observe red**

Run:

```sh
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-storage --test migration_safety legacy_schema_paths_are_not_present -- --exact
```

Expected: failure because the obsolete paths still exist.

- [ ] **Step 3: Delete only confirmed inactive artifacts**

Remove the root migration directory and `iot-sqldb-common`; remove the workspace member; regenerate only the required Cargo lockfile delta. Mark the obsolete four-service separation plan as superseded, rather than deleting historical context.

- [ ] **Step 4: Verify metadata and focused tests**

Run:

```sh
cargo metadata --no-deps --format-version=1 | jq -e 'all(.packages[]; .name != "iot-sqldb-common")'
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-storage --test migration_safety
```

Expected: jq exits `0`; migration safety passes.

- [ ] **Step 5: Commit artifact removal**

```sh
git add -A db/migrations crates/iot-sqldb-common Cargo.toml Cargo.lock crates/iot-storage/tests/migration_safety.rs docs/superpowers/plans
git commit -m "chore: remove inactive database migration artifacts"
```

### Task 3: Make `iot-storage` a thin stable facade

**Files:**
- Create: `crates/iot-storage/src/schema/mod.rs`
- Create: `crates/iot-storage/src/schema/sqlite.rs`
- Create: `crates/iot-storage/src/schema/postgres.rs`
- Create: `crates/iot-storage/src/management/{mod,users,assets,devices,profiles,alerts,tokens}.rs`
- Modify: `crates/iot-storage/src/lib.rs`
- Modify: `crates/iot-storage/src/management.rs` then delete it after its contents move
- Test: existing focused storage tests

**Interfaces:**
- `PlatformStore::open`, `SqliteStore::open`, and all current public re-exports retain their exact consumer paths.
- `schema::sqlite` owns `SQLITE_SCHEMA` and supported fresh-schema helpers.
- `schema::postgres` owns `PLATFORM_POSTGRES_SCHEMA` and Timescale migration helpers.
- `management::mod` re-exports the same management types and repository traits currently re-exported by `lib.rs`.

- [ ] **Step 1: Establish green behavior baseline**

Run before moving source:

```sh
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test management_assets
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test management_devices
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test management_profiles
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test public_api
```

Expected: baseline passes before a behavior-preserving file move.

- [ ] **Step 2: Move schema implementation without changing SQL**

Move raw SQLite DDL and supported SQLite schema functions into `schema/sqlite.rs`; move the included Timescale DDL constant and migration entry point into `schema/postgres.rs`. `lib.rs` calls private functions such as:

```rust
schema::sqlite::initialize(&pool).await?;
schema::postgres::migrate(&mut transaction).await?;
```

Keep legacy-removal checks absent; this is fresh schema initialization only.

- [ ] **Step 3: Move management implementation by resource boundary**

Move complete type/trait/query groups together: users to `users`, asset/device CRUD to `assets`/`devices`, profiles to `profiles`, alert rules/incidents to `alerts`, and device token operations to `tokens`. `management/mod.rs` owns cross-group shared constants and re-exports. Do not alter SQL semantics while moving.

- [ ] **Step 4: Verify public facade remains stable**

Run:

```sh
./scripts/dev/cargo-lane.sh storage-boundaries -- check -p iot-storage -p iot-nano-api -p iot-nano-monolith
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test management_assets
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test management_devices
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test management_profiles
./scripts/dev/cargo-lane.sh storage-boundaries -- test -p iot-storage --test public_api
```

Expected: all consumers compile and focused tests remain green.

- [ ] **Step 5: Commit storage boundaries**

```sh
git add crates/iot-storage
git commit -m "refactor: split storage schema and management modules"
```

### Task 4: Split monolith management private implementation

**Files:**
- Create: `services/iot-nano-monolith/src/management/mod.rs`
- Create: `services/iot-nano-monolith/src/management/session.rs`
- Create: `services/iot-nano-monolith/src/management/openapi.rs`
- Create: `services/iot-nano-monolith/src/management/errors.rs`
- Create: `services/iot-nano-monolith/src/management/operator_api.rs`
- Create: `services/iot-nano-monolith/src/management/routes/{mod,system,tenant,user}.rs`
- Delete: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-monolith/src/lib.rs`
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs`

**Interfaces:**
- `crate::management::{bootstrap_system, ManagementSessionRouter}` remain exported unchanged.
- Route composition occurs only in `management/mod.rs`; no route URL or HTTP method changes.
- `ManagementState`, request/response DTOs, and shared authorization helpers are `pub(super)` only where a sibling module needs them.

- [ ] **Step 1: Establish current route behavior baseline**

Run:

```sh
./scripts/dev/cargo-lane.sh monolith-management-boundaries -- test -p iot-nano-monolith --test management_sessions
./scripts/dev/cargo-lane.sh monolith-management-boundaries -- test -p iot-nano-monolith --test platform_ui_templates
```

Expected: baseline passes before file extraction.

- [ ] **Step 2: Extract session, error, and OpenAPI code**

Move session structs/verifier/rate-limit/auth gates to `session.rs`; move `ManagementSessionError` and all domain-to-HTTP mapping to `errors.rs`; move only OpenAPI schema/document helpers to `openapi.rs`. Keep function signatures explicit and use `pub(super)` rather than widening public API.

- [ ] **Step 3: Extract routes by authenticated surface**

Move system lifecycle/rendering handlers to `routes/system.rs`, Tenant Console pages/forms to `routes/tenant.rs`, and `/app` user pages/forms/invitations to `routes/user.rs`. Move `/api/management/*` JSON handlers and their DTOs to `operator_api.rs`. Compose all existing paths in `mod.rs` in their current order.

- [ ] **Step 4: Organize tests without increasing test binaries**

Move related test functions from `management_sessions.rs` into files under `services/iot-nano-monolith/tests/management_sessions/`, included with `mod system;`, `mod tenant;`, and `mod user;` from the original integration-test root. Keep fixtures in `management_sessions.rs` or a `common.rs` module imported by that same root.

- [ ] **Step 5: Run behavior and template verification**

Run:

```sh
./scripts/dev/cargo-lane.sh monolith-management-boundaries -- check -p iot-nano-monolith
./scripts/dev/cargo-lane.sh monolith-management-boundaries -- test -p iot-nano-monolith --test management_sessions
./scripts/dev/cargo-lane.sh monolith-management-boundaries -- test -p iot-nano-monolith --test platform_ui_templates
```

Expected: every existing management route contract remains green with no public path change.

- [ ] **Step 6: Commit monolith boundaries**

```sh
git add services/iot-nano-monolith/src services/iot-nano-monolith/tests
git commit -m "refactor: split monolith management routes"
```

### Task 5: Establish local-only file policy and release verification

**Files:**
- Modify: `.gitignore`
- Modify: `apps/powermonitor/.env.example` only if a tracked safe example is needed
- Modify: current architecture/planning docs that claim removed behavior
- Delete: confirmed unreferenced generated local artifacts only

**Interfaces:**
- `.gitignore` excludes exactly `apps/powermonitor/.key`,
  `infra/monolith/local-platform-seed.env`, and `debug/__pycache__/`.
- No runtime reads those ignored paths as required application input.

- [ ] **Step 1: Prove ignored files are not production inputs**

Use `rg` to verify the exact paths have no runtime source readers. Do not print their contents. Add a shell/static check that `git check-ignore -q` succeeds for each path.

- [ ] **Step 2: Add narrow ignore rules and remove generated artifacts**

Add the three exact ignore entries. Remove only `debug/__pycache__/`; preserve `.key` and local seed data if a running local service still owns them, but leave them ignored and unstaged.

- [ ] **Step 3: Run release-focused verification**

Run:

```sh
./scripts/dev/cargo-lane.sh legacy-cutover -- check -p iot-storage -p iot-nano-api -p iot-nano-monolith
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-storage --test migration_safety
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-nano-api --test public_v1
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-nano-monolith --test management_sessions
./scripts/dev/cargo-lane.sh legacy-cutover -- test -p iot-nano-monolith --test platform_ui_templates
(cd apps/powermonitor && npm test && npm run build)
./scripts/verify-no-legacy-runtime.sh
git diff --check
```

Then run Timescale and external-app contract lanes once their declared services are healthy. Finally, scan active source excluding historical docs and intentional runtime-retirement checks:

```sh
rg -n 'default_app|granted_apps|user_app_grants|api_access_tokens|iot_sqldb_common|iot-sqldb-common' \
  Cargo.toml crates services apps infra scripts -g '!services/iot-nano-mqttd/vendor/**'
```

Expected: no active-source match; test output is green.

- [ ] **Step 4: Commit hygiene and verification policy**

```sh
git add .gitignore apps/powermonitor/.env.example docs
git commit -m "chore: protect local development artifacts"
```
