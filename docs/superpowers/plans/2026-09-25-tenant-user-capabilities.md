# Tenant User Capabilities Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a Tenant Account enable or disable the elevated actions available to each User while retaining per-resource View and Control grants.

**Architecture:** Resource permissions remain the source of truth for a User's access to an individual asset or device. A new tenant-scoped capability set is stored for each User and checked in addition to ownership or a resource grant before mutations. The tenant Users page reads and updates that set through the existing management-user API.

**Tech Stack:** Rust, SQLx SQLite/Timescale stores, Axum, Askama templates, existing platform session and public-v1 authorization.

## Global Constraints

- Do not add a global `view` capability; revoke the resource grant or ownership to remove visibility.
- New Users start without elevated capabilities.
- `share_owned_resources` never permits sharing a resource the User does not own.
- Existing resource grants still decide whether a User may view or control a resource.
- Do not change default_app or granted_apps behavior.

---

### Task 1: Persist and expose a user capability set

**Files:**
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Modify: `crates/iot-storage/src/lib.rs`
- Modify: `crates/iot-storage/src/management.rs`
- Test: `crates/iot-storage/tests/management_users.rs`

- [ ] Add a failing repository test proving a newly created User has no capabilities and an update replaces its capability set.
- [ ] Add `user_capabilities(user_id, tenant_id, capability)` with composite primary key and tenant/user foreign key in both schema variants.
- [ ] Define `UserCapability` with `create_assets`, `create_devices`, `edit_resources`, `control_devices`, `share_owned_resources`, `assign_application_profiles`, and `manage_device_tokens`.
- [ ] Add `capabilities` to management-user read and update contracts; validate values and atomically replace their rows during update.
- [ ] Run `cargo test -p iot-storage --test management_users`.

### Task 2: Enforce capabilities in User and public APIs

**Files:**
- Modify: `crates/iot-storage/src/management.rs`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-api/src/public_v1.rs`
- Modify: `services/iot-nano-api/src/device_tokens.rs`
- Test: `services/iot-nano-monolith/tests/management_sessions.rs`
- Test: `services/iot-nano-api/tests/public_v1.rs`

- [ ] Add failing tests for a resource-authorized User denied a disabled action and allowed after the Tenant enables its matching capability.
- [ ] Add a tenant-scoped capability lookup to the storage repository.
- [ ] Gate User asset/device creation, owned-resource edits, sharing, profile assignment, device token management, and commands with their matching capability.
- [ ] Keep the owner check for sharing and the manager resource grant check for commands.
- [ ] Run the focused monolith and public-v1 tests.

### Task 3: Manage capabilities from Tenant Users

**Files:**
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_users.html`
- Modify: `services/iot-nano-monolith/assets/platform-ui.css`
- Test: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

- [ ] Add a failing template/rendering test for the user capability editor.
- [ ] Extend User rows with id and capability flags.
- [ ] Render a compact per-user edit dialog with labelled checkboxes and one Save action.
- [ ] Send only the capabilities field through `PUT /api/management/users/{username}`, preserving the existing default app and role values.
- [ ] Run the template test.

### Task 4: Verify

**Files:**
- Test: `crates/iot-storage/tests/management_users.rs`
- Test: `services/iot-nano-monolith/tests/management_sessions.rs`
- Test: `services/iot-nano-monolith/tests/platform_ui_templates.rs`
- Test: `services/iot-nano-api/tests/public_v1.rs`

- [ ] Run all focused tests and `cargo check -p iot-nano-monolith -p iot-nano-api -p iot-storage` through the shared cargo lane.
- [ ] Run `git diff --check` and report the exact changed files without touching unrelated worktree changes.
