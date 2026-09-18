# Gateway Topology Revision and Delete Protection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist a per-tenant-device gateway topology revision, increment it on child assignment changes, and prevent public gateway deletion while active children remain.

**Architecture:** `devices.gateway_topology_version` stays an internal storage field so management and public request/response shapes do not change. Management topology updates calculate whether `gateway_device_id` changed and increment the column in the same SQL update. Public deletion retains its `Result<bool, PlatformStoreError>` contract and returns `false` when an authorized gateway has active children, using the same tenant-scoped active-child predicate as management deletion.

**Tech Stack:** Rust, SQLx, SQLite, PostgreSQL/Timescale, Tokio integration tests.

## Global Constraints

- Branch from `codex/tenant-platform-integration` at `71e8088` in an isolated worktree.
- Keep `gateway_topology_version` tenant-scoped through the existing `(device_id, tenant_id)` device identity.
- Do not add audit events, UI work, or public API fields.
- Keep SQLite coverage runnable by default; mark Timescale tests `#[ignore]` and require `IOT_NANO_TIMESCALE_TEST_URL`.
- Preserve public delete behavior as `Ok(false)` for rejected or inaccessible mutation attempts.

---

### Task 1: Add Failing Storage Contract Tests

**Files:**
- Modify: `crates/iot-storage/tests/management_devices.rs`
- Modify: `crates/iot-storage/tests/public_api.rs`
- Modify: `crates/iot-storage/tests/migration_safety.rs`

**Interfaces:**
- Consumes: `ManagementDeviceRepository::update_management_device`, `PublicApiRepository::delete_public_device`, and `PlatformStore::open`.
- Produces: regression tests for assignment/reassignment/detachment revisions, tenant isolation, public active-child delete rejection, and schema upgrade behavior.

- [x] **Step 1: Write failing SQLite topology revision tests**

```rust
assert_eq!(gateway_topology_version(&store, "management-direct").await, 0);
update_child_topology(&store, tenant_id, "management-direct", Some("management-gateway".to_owned())).await;
assert_eq!(gateway_topology_version(&store, "management-direct").await, 1);
```

Extend the same test with a second same-tenant gateway and `None` to assert versions `2` and `3` for reassignment and detachment. Attempt a cross-tenant assignment and assert its child remains at version `0`.

- [x] **Step 2: Write a failing public-delete SQLite test**

```rust
assert!(!PublicApiRepository::delete_public_device(&store, &principal, &gateway_id).await?);
assert!(deleted_at(&store, &gateway_id).await.is_none());
assert!(PublicApiRepository::delete_public_device(&store, &principal, &child_id).await?);
assert!(PublicApiRepository::delete_public_device(&store, &principal, &gateway_id).await?);
```

Seed both records with the principal as owner, mark the first as a gateway, and point the active child at it.

- [x] **Step 3: Write schema-migration and guarded Timescale cases**

```rust
sqlx::query("ALTER TABLE devices DROP COLUMN gateway_topology_version")
    .execute(store.sqlite_pool().unwrap())
    .await?;
let reopened = PlatformStore::open(&configuration).await?;
assert_eq!(query_device_version(&reopened, "migration-device").await, 0);
```

Add ignored Timescale tests that exercise the same version transition sequence and public delete rejection against an `iot_nano_test_*` database.

- [x] **Step 4: Run the new SQLite tests and verify failure**

Run: `cargo test -p iot-storage --test management_devices gateway_topology_version --test public_api public_gateway --test migration_safety gateway_topology_version`

Expected: compilation or assertion failure because the column and protections do not yet exist.

### Task 2: Implement Internal Schema and Repository Changes

**Files:**
- Modify: `crates/iot-storage/src/lib.rs`
- Modify: `crates/iot-storage/src/management.rs`
- Modify: `crates/iot-storage/src/public_api.rs`
- Modify: `crates/iot-storage/migrations/0001_platform.sql`

**Interfaces:**
- Consumes: existing `devices` tenant identity, management topology transaction, and public-delete transaction.
- Produces: an internal `gateway_topology_version INTEGER NOT NULL DEFAULT 0` and unchanged public repository signatures.

- [x] **Step 1: Add schema defaults and safe upgrades**

Add `gateway_topology_version INTEGER NOT NULL DEFAULT 0` to both `devices` definitions. Add a SQLite `migrate_gateway_topology_schema` helper called by `SqliteStore::open` that conditionally runs:

```sql
ALTER TABLE devices
ADD COLUMN gateway_topology_version INTEGER NOT NULL DEFAULT 0
```

Add this idempotent Timescale upgrade after the PostgreSQL `devices` table definition:

```sql
ALTER TABLE devices
ADD COLUMN IF NOT EXISTS gateway_topology_version INTEGER NOT NULL DEFAULT 0;
```

- [x] **Step 2: Increment only on assignment changes**

Before each management update, derive:

```rust
let topology_changed = current.gateway_device_id != topology.gateway_device_id;
```

In the existing transaction’s device update, add `gateway_topology_version = gateway_topology_version + ?` for SQLite and `gateway_topology_version = gateway_topology_version + $9` for Timescale, binding `i64::from(topology_changed)`. This preserves the value for metadata-only updates and increments from the database value atomically with the assignment update.

- [x] **Step 3: Reject active-child public deletion**

After public manager permission succeeds and before the soft delete, query active children with the same `tenant_id`, `gateway_device_id`, and `deleted_at IS NULL` predicate. SQLite uses the current `BEGIN IMMEDIATE` transaction; Timescale uses `FOR UPDATE` in its serializable transaction. Return `Ok(false)` without writing `deleted_at` when a child exists.

- [x] **Step 4: Run focused test groups**

Run: `cargo test -p iot-storage --test management_devices --test public_api --test migration_safety`

Expected: all SQLite tests pass and Timescale tests remain skipped without `IOT_NANO_TIMESCALE_TEST_URL`.

### Task 3: Verify, Review, and Commit

**Files:**
- Modify: `docs/superpowers/plans/2026-09-18-gateway-topology-revision-protection.md`

**Interfaces:**
- Consumes: completed tests and clean isolated worktree state.
- Produces: a commit limited to the implementation, focused tests, migration safety coverage, and this plan.

- [x] **Step 1: Format and re-run focused verification**

Run: `cargo fmt --check` and `cargo test -p iot-storage --test management_devices --test public_api --test migration_safety`.

Expected: formatting and all non-ignored test cases pass.

- [x] **Step 2: Run guarded Timescale tests when configured**

Run: `IOT_NANO_TIMESCALE_TEST_URL="$IOT_NANO_TIMESCALE_TEST_URL" cargo test -p iot-storage --test management_devices --test public_api --test migration_safety -- --ignored`.

Expected: only run when the variable points to an isolated `iot_nano_test_*` database; otherwise report the coverage as guarded and skipped.

`IOT_NANO_TIMESCALE_TEST_URL` was unset in this workspace, so the guarded cases were compiled and skipped.

- [x] **Step 3: Inspect the diff and commit**

Run: `git diff --check`, then commit the scoped files with message `fix(storage): protect gateway topology revisions`.
