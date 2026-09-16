# Tenant Phase 0 and 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-tenant identity foundation with the tenant-aware
System Account, Tenant Account, and User model required before any built-in
platform UI route is exposed.

**Architecture:** This is a development cutover. Fresh SQLite and Timescale
schemas become tenant-aware canonical schemas; old account-class and resource
authorization paths are removed as their replacements land. A session carries
`principal_kind` plus the required tenant scope, and storage/API authorization
always receives that tenant scope explicitly.

**Tech Stack:** Rust, Axum, SQLx SQLite/Timescale, `iot-storage`,
`iot-nano-api`, `iot-nano-monolith`.

## Global Constraints

- No dual schema, old-data runtime fallback, default tenant, or compatibility
  authorization path.
- Fresh development databases must bootstrap through the new System Account
  path only.
- Every tenant-scoped read and write requires an explicit tenant ID.
- System Account must never receive tenant resource authorization implicitly.
- Tenant Account must never resolve another tenant.
- Do not start Askama/HTMX UI routes until this plan's session/API contracts
  pass storage and HTTP authorization tests.

---

### Task 1: Canonical Tenant Identity Schema

**Files:**
- Modify: `crates/iot-storage/src/lib.rs`
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Create: `crates/iot-storage/tests/tenant_identity.rs`

**Interfaces:**
- Produces canonical `system_accounts`, `tenants`, `tenant_accounts`, and
  tenant-scoped `users` schema for SQLite and Timescale.
- Produces `TenantId`, `SystemAccountId`, and `TenantAccountId` storage types.

- [ ] **Step 1: Write fresh-schema contract tests**

Cover both storage backends where available:

```rust
#[tokio::test]
async fn fresh_schema_requires_a_tenant_for_user_and_resource_rows() {
    // User, Asset, Device, token, OAuth, telemetry, alert, and audit writes
    // without their required tenant identity must be rejected by the database.
}

#[tokio::test]
async fn tenant_account_is_unique_per_tenant() {
    // A second tenant account for the same tenant violates the one-to-one key.
}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p iot-storage --test tenant_identity
```

Expected: FAIL because the current schema has no tenant tables or required
tenant columns.

- [ ] **Step 3: Replace the canonical schemas**

Implement the Phase 1 tables and constraints from the approved tenant design:

```text
system_accounts
tenants
tenant_accounts
users.tenant_id NOT NULL
assets.tenant_id NOT NULL
devices.tenant_id NOT NULL
```

Add the corresponding non-null tenant columns and same-tenant composite
foreign keys to profiles, device credentials/tokens, telemetry, rollups,
alerts, dashboards, OAuth applications/codes/tokens, permissions, and audit
records. Remove `account_class = system|admin|user` as the identity source.

- [ ] **Step 4: Run schema tests to verify GREEN**

Run:

```bash
cargo test -p iot-storage --test tenant_identity
cargo test -p iot-storage --test migration_safety
```

Expected: tenant identity and migration ownership tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/iot-storage
git commit -m "feat(storage): add canonical tenant identity schema"
```

### Task 2: Tenant Identity Repository and Bootstrap

**Files:**
- Modify: `crates/iot-storage/src/lib.rs`
- Modify: `crates/iot-storage/src/management.rs`
- Modify: `services/iot-nano-api/src/auth.rs`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Create: `crates/iot-storage/tests/tenant_bootstrap.rs`

**Interfaces:**
- Produces `PrincipalKind::{System,Tenant,User}` and a principal with optional
  tenant ID only for tenant/user principals.
- Produces atomic storage operations for System Account bootstrap, tenant
  creation, Tenant Account credential creation, and tenant-scoped User
  creation.

- [ ] **Step 1: Write failing identity contracts**

```rust
#[tokio::test]
async fn system_bootstrap_creates_exactly_one_system_account() {}

#[tokio::test]
async fn system_account_creates_tenant_and_its_one_tenant_account() {}

#[tokio::test]
async fn tenant_user_cannot_be_created_without_an_active_tenant() {}

#[tokio::test]
async fn user_tenant_id_is_immutable() {}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p iot-storage --test tenant_bootstrap
```

Expected: FAIL because principal kind and tenant lifecycle repository APIs do
not exist.

- [ ] **Step 3: Implement repository and bootstrap APIs**

Implement explicit methods that accept caller principal and tenant IDs rather
than inferring tenant identity from a username or request path. System
bootstrap receives only deployment configuration and is the sole creator of
System Account. Tenant Account creation is atomic with tenant creation.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p iot-storage --test tenant_bootstrap
cargo test -p iot-nano-api --test auth
```

Expected: System/Tenant/User identity contracts pass.

- [ ] **Step 5: Commit**

```bash
git add crates/iot-storage services/iot-nano-api services/iot-nano-monolith
git commit -m "feat(identity): add tenant principal bootstrap"
```

### Task 3: Session Principal Contract and Route Gate

**Files:**
- Modify: `services/iot-nano-api/src/auth.rs`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-api/src/public_v1.rs`
- Create: `services/iot-nano-monolith/tests/tenant_sessions.rs`

**Interfaces:**
- Session response exposes `principal_kind` and `tenant_id` where applicable.
- System APIs require `System`; tenant APIs require same-tenant `Tenant` or
  authorized `User`; resource APIs receive tenant ID before resource lookup.

- [ ] **Step 1: Write failing route-gate tests**

```rust
#[tokio::test]
async fn system_session_cannot_read_tenant_resource_routes() {}

#[tokio::test]
async fn tenant_session_cannot_call_system_routes() {}

#[tokio::test]
async fn tenant_session_cannot_read_another_tenant_resource_by_uuid() {}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p iot-nano-monolith --test tenant_sessions
```

Expected: FAIL because existing sessions expose only legacy account class and
resource routes do not resolve by tenant ID.

- [ ] **Step 3: Implement session and handler gates**

Replace legacy role/account-class checks with principal-kind and tenant-scope
checks. Return not found for cross-tenant resource IDs where enumeration
protection applies. Delete superseded role-only authorization branches.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p iot-nano-monolith --test tenant_sessions
cargo test -p iot-nano-api
```

Expected: session and public API tenant boundaries pass.

- [ ] **Step 5: Commit**

```bash
git add services/iot-nano-api services/iot-nano-monolith
git commit -m "feat(auth): gate routes by tenant principal"
```

### Task 4: System Tenant Lifecycle HTTP Contract

**Files:**
- Modify: `services/iot-nano-monolith/src/management.rs`
- Create: `services/iot-nano-monolith/tests/system_tenants.rs`

**Interfaces:**
- Produces System-only tenant create, suspend, reactivate, delete, and Tenant
  Account reset endpoints.
- Tenant lifecycle responses never include password hashes or raw secrets.

- [ ] **Step 1: Write failing system lifecycle tests**

```rust
#[tokio::test]
async fn system_account_manages_tenant_lifecycle_without_tenant_resource_access() {}

#[tokio::test]
async fn tenant_account_is_denied_system_tenant_lifecycle_routes() {}
```

- [ ] **Step 2: Run tests to verify RED**

Run:

```bash
cargo test -p iot-nano-monolith --test system_tenants
```

Expected: FAIL because system lifecycle endpoints do not exist.

- [ ] **Step 3: Implement lifecycle handlers**

Use repository operations from Task 2. Authenticate System Account before JSON
parsing or mutation. Revoke/disable Tenant Account sessions when its tenant is
suspended, deleted, or credential-reset.

- [ ] **Step 4: Verify GREEN and commit**

Run:

```bash
cargo test -p iot-nano-monolith --test system_tenants
cargo test -p iot-nano-monolith --test tenant_sessions
```

Commit:

```bash
git add services/iot-nano-monolith
git commit -m "feat(system): add tenant lifecycle APIs"
```

## Next Plan

Only after Tasks 1-4 are complete, write and execute the built-in Askama/HTMX
UI plan for `/system`, `/tenant`, and `/app`. Phase 5 sharing, gateway
topology, and descriptive relations remain separate plans because they change
the storage authorization contract again.
