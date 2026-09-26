# User-Owned Resource Invitations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a regular-user owner create and manage own Devices/Assets and share them through recipient-accepted in-app invitations.

**Architecture:** Keep `iot-storage` as the authorization boundary. A pending invitation has no `resource_permissions` row; accepting it creates one in the same transaction. The monolith user workspace exposes owner-only forms and renders the recipient-specific invitation count in the common header.

**Tech Stack:** Rust 2024, SQLx SQLite/Timescale, Axum, Askama, existing `cargo-lane.sh` focused test lane.

## Global Constraints

- Tenant Account remains the only principal able to assign, clear, or transfer ownership.
- User CRUD and Asset assignment require `ResourceAccessSource::Owner`; shared recipients stay read-only.
- Invitations are same-tenant, regular-user-to-regular-user, `View` or `Control`, and create access only after recipient acceptance.
- No email, external link, ownership transfer, legacy generic tenant grants, or workspace-wide build.
- Do not commit, reset, clean, or overwrite unrelated dirty worktree changes.

---

### Task 1: Invitation Persistence and Authorization Contract

**Files:**
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Modify: `crates/iot-storage/src/contracts/resources.rs`
- Modify: `crates/iot-storage/src/domain/authorization.rs`
- Modify: `crates/iot-storage/src/lib.rs`
- Test: `crates/iot-storage/tests/resource_ownership.rs`

**Interfaces:**
- Produces `ResourceInvitation`, `InvitationState`, and `UserInvitationRepository`.
- `create_owner_invitation(tenant_id, sender, recipient, target, permission)` returns `Pending` and creates no permission.
- `accept_resource_invitation(tenant_id, recipient, invitation_id)` atomically returns the accepted invitation and exactly one direct permission.

- [ ] **Step 1: Write failing storage tests**

```rust
#[tokio::test]
async fn pending_owner_invitation_does_not_authorize_the_recipient() {
    let invitation = store.create_owner_invitation(tenant, owner, recipient, target, ResourcePermission::Viewer).await.unwrap();
    assert_eq!(invitation.state, InvitationState::Pending);
    assert!(store.authorized_asset(&recipient_subject, asset_id).await.unwrap().is_none());
}

#[tokio::test]
async fn recipient_acceptance_creates_one_direct_permission() {
    let invitation = store.create_owner_invitation(tenant, owner, recipient, target, ResourcePermission::Manager).await.unwrap();
    store.accept_resource_invitation(tenant, recipient, invitation.id).await.unwrap();
    assert_eq!(store.list_active_resource_permissions(tenant).await.unwrap().len(), 1);
}
```

- [ ] **Step 2: Verify the tests fail because invitation APIs do not exist**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-storage --test resource_ownership pending_owner_invitation_does_not_authorize_the_recipient -- --exact`

- [ ] **Step 3: Add the minimal schema and typed storage API**

```sql
CREATE TABLE IF NOT EXISTS resource_invitations (
    id UUID PRIMARY KEY, tenant_id UUID NOT NULL,
    sender_user_id UUID NOT NULL, recipient_user_id UUID NOT NULL,
    asset_id UUID, device_id TEXT,
    permission TEXT NOT NULL CHECK (permission IN ('viewer', 'manager')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'accepted', 'cancelled', 'withdrawn', 'invalidated')),
    resource_permission_id UUID, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), accepted_at TIMESTAMPTZ, closed_at TIMESTAMPTZ,
    CHECK ((asset_id IS NOT NULL AND device_id IS NULL) OR (asset_id IS NULL AND device_id IS NOT NULL))
);
```

Use existing SQLite and Timescale transaction patterns to validate both users, sender ownership, target tenant scope, and to insert the direct permission only in `accept_resource_invitation`.

- [ ] **Step 4: Verify the two focused storage tests pass**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-storage --test resource_ownership -- --test-threads=1`

### Task 2: Owner-Only Asset and Device Mutations

**Files:**
- Modify: `crates/iot-storage/src/contracts/resources.rs`
- Modify: `crates/iot-storage/src/management.rs`
- Test: `crates/iot-storage/tests/resource_ownership.rs`

**Interfaces:**
- Produces `create_owned_asset`, `update_owned_asset`, `create_owned_device`, and `update_owned_device` storage operations.
- Each operation takes `tenant_id`, `owner_user_id`, and a typed existing management request; it sets or verifies `owner_user_id` without calling ownership transfer.

- [ ] **Step 1: Write failing ownership-boundary tests**

```rust
#[tokio::test]
async fn owner_can_create_a_child_asset_and_device_only_in_owned_assets() {
    let asset = store.create_owned_asset(tenant, owner, owned_parent, "Pump room", json!({})).await.unwrap();
    let device = store.create_owned_device(tenant, owner, Some(asset.id), "pump-01", json!({})).await.unwrap();
    assert_eq!(asset.owner_user_id, Some(owner));
    assert_eq!(device.owner_user_id, Some(owner));
}

#[tokio::test]
async fn shared_recipient_cannot_mutate_or_assign_an_owned_resource() {
    let error = store.update_owned_device(tenant, recipient, device_id, update).await.unwrap_err();
    assert!(matches!(error, ManagementDeviceError::Forbidden));
}
```

- [ ] **Step 2: Verify the tests fail**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-storage --test resource_ownership owner_can_create_a_child_asset_and_device_only_in_owned_assets -- --exact`

- [ ] **Step 3: Implement owner-scoped validation before mutation**

```rust
fn require_owner(resource_owner: Option<Uuid>, actor: Uuid) -> Result<(), ManagementDeviceError> {
    if resource_owner == Some(actor) { Ok(()) } else { Err(ManagementDeviceError::Forbidden) }
}
```

For Assets, validate an optional parent is owned by the actor and preserve existing tenant/cycle checks. For Devices, validate a non-null `asset_id` is owned by the actor, create with `owner_user_id = actor`, and keep existing profile validation. Do not expose topology, ownership transfer, or profile CRUD to user routes.

- [ ] **Step 4: Verify owner and recipient focused tests pass**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-storage --test resource_ownership -- --test-threads=1`

### Task 3: User Session Routes and Invitation State Transitions

**Files:**
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs`
- Modify: `scripts/dev/seed-local-platform.sh`

**Interfaces:**
- Adds `/app/assets` and `/app/devices` owner create/update form routes.
- Replaces owner direct-permission POST routes with invitation creation.
- Adds `/app/invitations`, `/app/invitations/{id}/accept`, and `/app/invitations/{id}/cancel`.

- [ ] **Step 1: Write failing session tests**

```rust
#[tokio::test]
async fn owner_invitation_requires_recipient_acceptance_before_resource_access() {
    let invitation = post_form(&router, "/app/assets/{asset_id}/permissions", owner_cookie, "username=recipient&permission=view").await;
    assert_eq!(invitation.status(), StatusCode::SEE_OTHER);
    assert_eq!(get(&router, "/app/assets/{asset_id}", recipient_cookie).await.status(), StatusCode::NOT_FOUND);
    assert_eq!(post_form(&router, "/app/invitations/{invitation_id}/accept", recipient_cookie, "").await.status(), StatusCode::SEE_OTHER);
}
```

- [ ] **Step 2: Verify the session test fails under immediate sharing**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test management_sessions owner_invitation_requires_recipient_acceptance_before_resource_access -- --exact`

- [ ] **Step 3: Implement minimal forms and redirects**

Use `user_workspace_session` and `user_can_manage_resource` for every owner action. Resolve recipient usernames only through `ManagementUserRepository::list_management_users` for the session tenant. Route accepts/cancels through the invitation repository and redirect to the relevant detail or `/app/invitations` with a non-secret notice.

Update the local seed so it creates two pending invitations using A, logs in as B, accepts each, and remains idempotent by detecting accepted direct access rather than inserting duplicate grants.

- [ ] **Step 4: Verify session authorization and seed path**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test management_sessions -- --test-threads=1`

### Task 4: User Workspace Invitation Badge and Owner Forms

**Files:**
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Modify: `services/iot-nano-monolith/src/lib.rs`
- Modify: `services/iot-nano-monolith/templates/platform_ui/base.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user_assets.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user_device.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user_asset.html`
- Create: `services/iot-nano-monolith/templates/platform_ui/user_invitations.html`
- Modify: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

**Interfaces:**
- `PlatformUiIdentity::with_invitation_count(usize)` lets only user sessions render the common header badge.
- `UserInvitationPage` renders only incoming pending invitations and accept/cancel controls.
- User list/detail pages receive `can_manage` and owned-asset select options; shared pages receive neither.

- [ ] **Step 1: Write failing template tests**

```rust
#[test]
fn user_workspace_renders_recipient_specific_invitation_badge() {
    let rendered = PlatformUiRenderer::render_user(&identity.with_invitation_count(2), &page).unwrap();
    assert!(rendered.contains("Invitations (2)"));
}

#[test]
fn shared_resource_template_has_no_owner_mutation_controls() {
    let rendered = PlatformUiRenderer::render_user_asset(&identity, &shared_page).unwrap();
    assert!(!rendered.contains("Invite user"));
    assert!(!rendered.contains("Save asset"));
}
```

- [ ] **Step 2: Verify the template tests fail**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test platform_ui_templates user_workspace_renders_recipient_specific_invitation_badge -- --exact`

- [ ] **Step 3: Render the smallest usable UI**

Render `Invitations (n)` in `base.html` only when the identity has a user-session count. Add create/edit forms and asset assignment selects only when `can_manage` is true. Rename “Share with user” submit action to “Invite user”; list pending invitations separately from accepted direct grants. The invitation page uses normal POST forms for `Accept` and `Cancel`, with no JavaScript dependency.

- [ ] **Step 4: Run focused UI verification**

Run: `./scripts/dev/cargo-lane.sh ui-console -- test -p iot-nano-monolith --test platform_ui_templates`

### Final Verification

- [ ] Run `rustfmt --edition 2024 --check` on each modified Rust source and test file.
- [ ] Run `git diff --check` and inspect the diff without reverting unrelated changes.
- [ ] Run `./scripts/dev/cargo-lane.sh ui-console -- build -p iot-nano-monolith`.
- [ ] Restart only the local `io.rush-iot-nano.ui-console` service, run the fixed local seed, and verify as user B that pending invitations are visible before acceptance, then only accepted resources are listed.
