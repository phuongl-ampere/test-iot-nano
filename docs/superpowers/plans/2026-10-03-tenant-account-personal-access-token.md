# Tenant Account Personal Access Token Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a Tenant Account manage exactly one active Personal Access Token that authenticates its tenant-scoped public v1 API authority.

**Architecture:** Add a tenant-account PAT persistence contract with a hash-only one-active-token invariant, then extend public bearer validation to resolve either OAuth or PAT credentials. Add versioned management endpoints and a Tenant Account console page that reveals a newly created/rotated secret once.

**Tech Stack:** Rust, Axum, SQLite/PostgreSQL schemas, existing PlatformStore repository traits, Askama tenant console, Vitest/Cargo tests.

## Global Constraints

- Only Tenant Account may list, create, rotate, or revoke its own PAT; System Account, Platform Account, and Tenant User are denied.
- Exactly one active PAT per Tenant Account; create with an active PAT atomically rotates it.
- Persist token digest and safe metadata only; plaintext is returned once from create/rotate and never returned by list.
- PAT works only as a Bearer credential on public `/api/v1/*`; OAuth, management APIs, pages, and MQTT reject it.
- PAT has no scope picker or expiry; it gets the full tenant-level public API authority of its active Tenant Account.

---

### Task 1: Persist one active Tenant Account PAT safely

**Files:**
- Modify: `crates/iot-storage/src/schema/{sqlite,postgres}.rs`
- Modify: `crates/iot-storage/src/contracts/application.rs`
- Create: `crates/iot-storage/src/management/personal_access_tokens.rs`
- Modify: `crates/iot-storage/src/management/mod.rs`
- Test: `crates/iot-storage/tests/personal_access_tokens.rs`

**Interfaces:**
- Produces: `TenantPersonalAccessTokenRepository` methods `active_tenant_personal_access_token`, `rotate_tenant_personal_access_token`, `revoke_tenant_personal_access_token`, and `resolve_tenant_personal_access_token`.
- Consumes: Tenant Account user id, tenant id, SHA-256 token digest, safe prefix, and current UTC time.

- [ ] **Step 1: Write failing repository tests**

Create `personal_access_tokens.rs` with a SQLite test that creates a Tenant Account token, asserts the returned plaintext is absent from the stored record, rotates it, verifies the first digest is revoked, verifies one active record remains, and verifies a token from tenant A cannot resolve for tenant B.

```rust
let first = store.rotate_tenant_personal_access_token(tenant_id, tenant_user_id, request("CI")).await.unwrap();
assert_ne!(first.secret, first.record.token_hash);
let second = store.rotate_tenant_personal_access_token(tenant_id, tenant_user_id, request("Deploy")).await.unwrap();
assert!(store.resolve_tenant_personal_access_token(&first.secret, now).await.unwrap().is_none());
assert_eq!(store.resolve_tenant_personal_access_token(&second.secret, now).await.unwrap().unwrap().tenant_id, tenant_id);
```

- [ ] **Step 2: Verify the storage test is red**

Run: `cargo test -p iot-storage --test personal_access_tokens -- --nocapture`

Expected: compile failure because the PAT repository contract and table do not exist.

- [ ] **Step 3: Implement hash-only storage and rotation**

Add `tenant_personal_access_tokens` in both schemas with `id`, `tenant_id`, `tenant_account_user_id`, `name`, unique `token_prefix`, `token_hash`, `created_at`, `last_used_at`, and `revoked_at`. Add a partial unique index for active (`revoked_at IS NULL`) tokens per Tenant Account. Generate a `iotpat_` secret with CSPRNG bytes; persist only SHA-256 digest and prefix. In one immediate transaction, revoke the active row then insert the replacement. Resolve by digest, require null `revoked_at`, enabled tenant/account, and update `last_used_at`.

- [ ] **Step 4: Verify the storage contract is green**

Run: `cargo test -p iot-storage --test personal_access_tokens -- --nocapture`

Expected: PASS.

### Task 2: Accept PATs for public bearer authentication only

**Files:**
- Modify: `services/iot-nano-api/src/auth.rs`
- Modify: `services/iot-nano-api/src/public_v1.rs`
- Test: `services/iot-nano-api/tests/personal_access_tokens.rs`

**Interfaces:**
- Consumes: Task 1 resolver and existing `BearerAccessToken` authorization path.
- Produces: a PAT-derived `BearerAccessToken` for public v1 requests with the Tenant Account tenant id and full public scope set.

- [ ] **Step 1: Write failing public API tests**

Add a test that sends `Authorization: Bearer {pat}` to `/api/v1/devices` and expects success for the Tenant Account tenant, then revokes the PAT and expects `401`. Add a request to `/api/v1/management/personal-access-token` with the PAT and expect `401` or `403` rather than management-session access.

- [ ] **Step 2: Verify the authentication test is red**

Run: `cargo test -p iot-nano-api --test personal_access_tokens -- --nocapture`

Expected: PAT is denied because `validate_bearer_access_token` resolves OAuth records only.

- [ ] **Step 3: Extend bearer resolution without changing OAuth**

Keep the current OAuth lookup first. When it returns `OAuthAccessTokenDenied`, attempt the PAT resolver. Construct the same `BearerAccessToken` with the resolved tenant and the fixed full public scope set; keep management session middleware separate so a PAT never authorizes management routes. Map inactive, revoked, disabled-owner, and unknown PATs to `Denied`.

- [ ] **Step 4: Verify public PAT access is green**

Run: `cargo test -p iot-nano-api --test personal_access_tokens -- --nocapture`

Expected: PASS.

### Task 3: Add PAT management API and tenant console

**Files:**
- Modify: `services/iot-nano-monolith/src/management/{mod,operator_api}.rs`
- Modify: `services/iot-nano-monolith/src/management/routes/tenant.rs`
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Modify: `services/iot-nano-monolith/src/management/openapi.rs`
- Test: `services/iot-nano-monolith/tests/{management_sessions,platform_ui_templates}.rs`

**Interfaces:**
- Produces: `GET|POST /api/v1/management/personal-access-token`, `POST /api/v1/management/personal-access-token/revoke`, and `/tenant/personal-access-tokens`.
- Consumes: the Tenant Account session gate and Task 1 safe metadata/one-time secret response.

- [ ] **Step 1: Write failing management and HTML contracts**

Add tests proving a Tenant Account gets a PAT page and can create/rotate/revoke through the v1 API; the GET response excludes `secret`; a Tenant User gets forbidden; and rendered one-time response contains the new secret only after POST, never after a subsequent GET.

- [ ] **Step 2: Verify the console contract is red**

Run: `cargo test -p iot-nano-monolith --test management_sessions personal_access_token -- --nocapture`

Expected: 404 because no PAT routes or page exist.

- [ ] **Step 3: Implement routes, page, and safe responses**

Add the three v1 routes to `ManagementSessionRouter` and a tenant page route. Gate every handler with `require_tenant_account`. GET returns `{ token: null | { name, prefix, created_at, last_used_at } }`; POST validates non-empty name, rotates atomically, and returns `{ token, secret }`; revoke returns `204`. Add a tenant navigation link, metadata table, create/rotate/revoke controls, and a one-time secret panel with Copy action. Add the v1 paths to OpenAPI; do not document a secret in list response schemas.

- [ ] **Step 4: Verify console tests are green**

Run: `cargo test -p iot-nano-monolith --test management_sessions personal_access_token -- --nocapture && cargo test -p iot-nano-monolith --test platform_ui_templates -- --nocapture`

Expected: PASS.

### Task 4: Verify end-to-end behavior

**Files:**
- Modify: `docs/superpowers/plans/2026-10-03-tenant-account-personal-access-token.md`

- [ ] **Step 1: Run complete verification**

```bash
cargo test -p iot-storage --test personal_access_tokens
cargo test -p iot-nano-api --test personal_access_tokens
cargo test -p iot-nano-monolith
(cd apps/powermonitor && npm test && npm run build)
```

Expected: all enabled tests and the Power Monitor build pass; environment-gated release/Timescale tests may remain explicitly ignored.

- [ ] **Step 2: Mark plan and commit complete work**

Mark every completed checkbox, inspect `git diff --check`, then commit the storage, API, console, test, and plan files with `feat: add tenant account personal access token`.

### Task 5: Preserve Tenant Account actors for commands and invitations

**Files:**
- Modify: `crates/iot-storage/src/{schema/sqlite.rs,schema/postgres.rs,migrations/0001_platform.sql,contracts/resources.rs,domain/authorization.rs}`
- Modify: `services/iot-nano-api/src/{core_facade.rs,public_v1.rs}`
- Modify: `services/iot-nano-core` command authorization/storage contracts that consume `CoreAuthorizedCommandCreateRequest`
- Modify: focused resource-ownership, command, and PAT API tests.

**Interfaces:**
- Produces an explicit tenant-scoped actor union (`TenantUser` or `TenantAccount`) for invitation sender and command issuer/audit records.
- Consumes the existing `AuditPrincipal::TenantAccount` value rather than fabricating a `users` row.

- [ ] **Step 1: Write failing PAT command and invitation contracts**

Add PAT integration tests that issue a tenant-scoped command and create/cancel an invitation with a Tenant Account PAT. Assert the persisted command audit actor and invitation sender principal are the real Tenant Account, and a PAT from another tenant receives `403`.

- [ ] **Step 2: Verify the contracts are red**

Run: `cargo test -p iot-nano-api --test personal_access_tokens tenant_account_pat_can_issue_commands_and_invitations -- --exact`

Expected: current public API rejects the `user_id: None` PAT principal before downstream operation, or storage cannot persist a Tenant Account sender.

- [ ] **Step 3: Implement the explicit actor migration**

Replace user-only invitation sender columns/model fields with a principal kind plus principal id constrained to exactly one tenant actor. Update SQLite and PostgreSQL canonical schemas/migration validation, invitation repository SQL, response-name resolution, and audit writes. Replace `CoreAuthorizedCommandCreateRequest.user_id` with `AuditPrincipal`, validate Tenant Account ownership of the target device in the same tenant, and preserve existing Tenant User behavior. Route PAT creation/cancel/command operations through `AuditPrincipal::TenantAccount`.

- [ ] **Step 4: Verify actor parity is green**

Run: `cargo test -p iot-nano-api --test personal_access_tokens -- --nocapture && cargo test -p iot-storage --test resource_ownership -- --nocapture && cargo test -p iot-nano-monolith --test command_response -- --nocapture`

Expected: PAT and password-authenticated Tenant Account operations record the same truthful actor, with no cross-tenant access.
