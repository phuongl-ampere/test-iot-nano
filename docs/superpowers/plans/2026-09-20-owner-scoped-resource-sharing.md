# Owner-Scoped Resource Sharing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tenant Accounts assign a single regular-user owner to each Device or Asset; only that owner can share direct view/control access.

**Architecture:** Keep `tenant_id` as the isolation boundary and `owner_user_id` as the authoritative primary-user relation. Use the existing `resource_permissions` table only for owner-created direct grants. Ownership transfer and grant revocation execute in one storage transaction.

**Tech Stack:** Rust 2024, Axum, SQLx SQLite/Postgres, Askama templates, vanilla JavaScript.

## Global Constraints

- Do not create or restore asset/device permission inheritance.
- Require tenant scope and a regular user for every owner assignment.
- Preserve existing `viewer`/`manager` storage values while exposing them as View/Control to users.
- Use `./scripts/dev/cargo-lane.sh ui-console -- ...` for focused Cargo verification.

---

### Task 1: Atomic Storage Ownership Transfer

**Files:**
- Modify: `crates/iot-storage/src/contracts/resources.rs`
- Modify: `crates/iot-storage/src/domain/authorization.rs`
- Modify: `crates/iot-storage/src/lib.rs`
- Test: `crates/iot-storage/tests/resource_ownership.rs`

**Interfaces:**
- Extends `TenantAuthorizationRepository` with owner-scoped direct grant/revoke operations.
- Changes `transfer_resource_ownership` to accept `Option<Uuid>`: `Some(user_id)` assigns or transfers a same-tenant regular-user owner; `None` clears the owner. Both revoke active direct grants in the same transaction.

- [x] **Step 1: Write failing storage tests**

```rust
#[tokio::test]
async fn transferring_device_owner_revokes_active_direct_grants() {
    let result = ManagementResourceOwnershipRepository::assign_management_device_owner(
        &store, tenant_id, AuditPrincipal::TenantAccount(tenant_account_id),
        "device-a", Some(new_owner_id),
    ).await.unwrap();
    assert_eq!(result.owner_user_id, Some(new_owner_id));
    assert_eq!(result.revoked_permission_count, 1);
}
```

- [x] **Step 2: Run the focused test and verify it fails**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-storage --test resource_ownership transferring_device_owner_revokes_active_direct_grants -- --exact`

Expected: FAIL because the repository interface does not exist.

- [x] **Step 3: Implement owner transfer and owner-scoped grant/revoke in SQLite and Postgres transactions**

```rust
UPDATE devices SET owner_user_id = ? WHERE tenant_id = ? AND device_id = ?;
UPDATE resource_permissions
SET revoked_at = CURRENT_TIMESTAMP
WHERE tenant_id = ? AND device_id = ? AND revoked_at IS NULL;
```

Validate a non-null owner against `users.tenant_id` and `account_class = 'user'`, retain the prior owner if the target is unchanged, revoke active resource permissions during an actual transfer or clear, and insert `OwnershipTransferred` audit data.

- [x] **Step 4: Run the focused storage test and verify it passes**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-storage --test resource_ownership`

Expected: PASS.

### Task 2: Tenant Ownership API and Owner-Only Authorization

**Files:**
- Modify: `services/iot-nano-monolith/src/management.rs`
- Test: `services/iot-nano-monolith/tests/management_sessions.rs`

**Interfaces:**
- Consumes the ownership repository from Task 1.
- Produces tenant-only `PUT /api/management/assets/{asset_id}/owner` and `PUT /api/management/devices/{device_id}/owner`.

- [x] **Step 1: Write failing session tests**

```rust
#[tokio::test]
async fn only_the_current_owner_can_share_a_device() {
    assert_eq!(owner_share.status(), StatusCode::SEE_OTHER);
    assert_eq!(recipient_share.status(), StatusCode::FORBIDDEN);
}
```

Cover same-tenant regular-user validation, transfer revocation, user-visible owner/share source, and rejected Tenant resource grants.

- [x] **Step 2: Run the focused session test and verify it fails**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test management_sessions only_the_current_owner_can_share_a_device -- --exact`

Expected: FAIL because `manager` grants currently permit sharing.

- [x] **Step 3: Implement API and authorization checks**

```rust
let is_owner = resource.access.source == ResourceAccessSource::Owner;
if !is_owner { return Err(ManagementSessionError::Forbidden); }
```

Dispatch ownership mutations through the tenant session, map their storage errors to HTTP responses, and keep grant creation restricted to the owner’s `/app` resource detail route.

- [x] **Step 4: Run focused session tests and verify they pass**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test management_sessions owner_ -- --nocapture`

Expected: PASS.

### Task 3: Tenant Assignment and User Share UX

**Files:**
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_devices.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_assets.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user_device.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user_asset.html`
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Test: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

**Interfaces:**
- Consumes owner API responses from Task 2.
- Uses `owner_user_id` in management resource response payloads and server-rendered access source labels.

- [x] **Step 1: Write failing template assertions**

```rust
assert!(tenant_device_template.contains("Assigned user"));
assert!(!tenant_device_template.contains("User access"));
assert!(user_device_template.contains("Share with user"));
assert!(!user_asset_template.contains("inherit_children"));
```

- [x] **Step 2: Run the focused template test and verify it fails**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test platform_ui_templates owner_scoped_resource_sharing -- --exact`

Expected: FAIL because Tenant editors currently create grants and `manager` users see share controls.

- [x] **Step 3: Implement the UI boundary**

Replace Tenant `User access` sections with one owner selector and explicit transfer warning. Render share controls only when `ResourceAccessSource::Owner`; use View/Control copy and remove the asset inheritance control. Recipients retain the resource detail with an owner/share source but no grant controls.

- [x] **Step 4: Run focused template tests and verify they pass**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test platform_ui_templates owner_scoped_resource_sharing -- --exact`

Expected: PASS.

### Task 4: Focused Integration Verification and Reload

**Files:**
- Modify: `docs/superpowers/plans/2026-09-20-owner-scoped-resource-sharing.md`

- [x] **Step 1: Format and inspect the focused diff**

Run: `rustfmt --edition 2024 --check crates/iot-storage/src/management.rs services/iot-nano-monolith/src/management.rs services/iot-nano-monolith/src/platform_ui.rs && git diff --check`

Expected: PASS.

- [x] **Step 2: Build the monolith once after targeted tests**

Run: `./scripts/dev/cargo-lane.sh ui-console -- build -p iot-nano-monolith`

Expected: PASS.

- [x] **Step 3: Restart only the local UI console launchd job and verify tenant UI**

Run: `launchctl kickstart -k gui/$(id -u)/io.rush-iot-nano.ui-console`, then request `http://127.0.0.1:18081/readyz` and `/login`.

Expected: readiness and login return successfully; no unrelated PowerMonitor process is changed.
