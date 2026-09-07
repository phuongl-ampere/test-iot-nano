# Token RBAC And Rule Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` (recommended) or `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add two fixed bearer-token roles, token replacement, RBAC enforcement, and editable/archiveable alert rules.

**Architecture:** PostgreSQL stores only Argon2 token hashes for the fixed `admin` and `viewer` roles. The Rust API authenticates each bearer token and gates mutations by role. The client stores its token in `sessionStorage`, renders by role, and attaches bearer headers. Deleting a rule archives it and transactionally resolves active incidents with durable resolved notifications.

**Tech Stack:** Rust 2024, Axum 0.8, SQLx/PostgreSQL, Argon2, Tokio, React 19, Next.js 16, Vitest.

## Global Constraints

- Tokens are at least eight ASCII non-whitespace characters with upper/lower/digit/special categories and initial values must differ.
- Hard-coded `NanoAdmin@1234` and `NanoView@1234` bootstrap an empty database only.
- Store only Argon2 hashes and never log or return plaintext token values.
- `/healthz` and `POST /api/auth/login` are public; all other API routes require bearer authentication.
- Viewer reads and may replace only the viewer token. Admin performs mutations and replaces either token.
- Archive deletion preserves history, resolves active incidents, and queues resolved outbox rows atomically.
- This workspace has no Git repository. Do not create commits.

---

### Task 1: Schema And Migration Registration

**Files:**
- Create: `db/migrations/0003_auth_and_rule_archive.sql`
- Modify: `crates/iot-ingest/src/writer.rs`
- Modify: `crates/iot-ingest/tests/writer.rs`

**Produces:** `api_access_tokens(role, token_hash, updated_at)` and `alert_rules.archived_at`.

- [x] **Step 1: Write a failing migration test**

Add `migration_installs_auth_and_rule_archive_schema`, call `migrate(&pool)`, and assert:

```rust
assert!(query_scalar::<_, bool>(
    "SELECT EXISTS (
       SELECT 1 FROM information_schema.tables WHERE table_name = 'api_access_tokens'
     )",
).fetch_one(&pool).await.unwrap());
assert!(query_scalar::<_, bool>(
    "SELECT EXISTS (
       SELECT 1 FROM information_schema.columns
       WHERE table_name = 'alert_rules' AND column_name = 'archived_at'
     )",
).fetch_one(&pool).await.unwrap());
```

- [x] **Step 2: Run it and verify it fails**

Run:

```bash
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest migration_installs_auth_and_rule_archive_schema
```

Expected: failure because the schema is absent.

- [x] **Step 3: Add minimal migration and register it**

Create:

```sql
CREATE TABLE IF NOT EXISTS api_access_tokens (
    role TEXT PRIMARY KEY CHECK (role IN ('admin', 'viewer')),
    token_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE alert_rules ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS alert_rules_active_index
    ON alert_rules (created_at DESC, id) WHERE archived_at IS NULL;
```

Append `include_str!("../../../db/migrations/0003_auth_and_rule_archive.sql")` to `MIGRATIONS`.

- [x] **Step 4: Re-run the focused test**

Expected: PASS.

### Task 2: Authentication And Authorization

**Files:**
- Create: `crates/iot-api/src/auth.rs`
- Modify: `Cargo.toml`
- Modify: `crates/iot-api/Cargo.toml`
- Modify: `crates/iot-api/src/lib.rs`
- Modify: `crates/iot-api/src/routes.rs`
- Modify: `crates/iot-api/src/main.rs`
- Modify: `crates/iot-api/tests/api.rs`

**Produces:** `Role`, hard-coded bootstrap/authentication/replacement logic, auth routes, bearer middleware, and the in-memory login rate limiter.

- [x] **Step 1: Write failing API tests**

Cover hard-coded bootstrap, login role, missing/wrong token `401`, viewer read
`200`, viewer mutation `403`, admin mutation, old token rejection after
replacement, and the sixth failed login in a minute returning `429`. Assert
replacement rejects duplicate, non-ASCII, whitespace, too-short, and
missing-category tokens.

- [x] **Step 2: Run focused tests**

```bash
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-api auth_
```

Expected: FAIL because the types and routes do not exist.

- [x] **Step 3: Implement the auth domain**

Add `argon2 = "0.5.3"` to workspace dependencies and `iot-api`. In `auth.rs`, validate:

```rust
value.len() == 8
    && value.is_ascii()
    && !value.bytes().any(u8::is_ascii_whitespace)
    && value.bytes().any(u8::is_ascii_uppercase)
    && value.bytes().any(u8::is_ascii_lowercase)
    && value.bytes().any(u8::is_ascii_digit)
    && value.bytes().any(|byte| !byte.is_ascii_alphanumeric())
```

Hash/verify with `Argon2::default()`. Bootstrap hard-coded `NanoAdmin@1234`
and `NanoView@1234` only when the table is empty; never overwrite existing
rows, and reject an incomplete persisted role set.

Add:

```text
POST /api/auth/login          { token } -> { role }
GET  /api/auth/me             -> { role }
POST /api/auth/logout         -> 204
PUT  /api/auth/tokens/{role}  { token } -> 204
```

Use Axum middleware to exempt health/login, require `Authorization: Bearer <token>` elsewhere, place `Role` in extensions, and return `401` or `403` consistently. Keep login attempts in `Mutex<HashMap<IpAddr, LoginAttempt>>`, reject the sixth failure in a rolling 60-second window, and clear attempts after success.

- [x] **Step 4: Re-run focused tests**

Expected: PASS.

### Task 3: Rule Update And Transactional Archive

**Files:**
- Modify: `crates/iot-api/src/routes.rs`
- Modify: `crates/iot-api/tests/api.rs`
- Modify: `crates/iot-ingest/src/alert.rs`

**Produces:** Admin-only `PUT/DELETE /api/alert-rules/{id}` and archive filtering in API/evaluator queries.

- [x] **Step 1: Write failing lifecycle tests**

Seed an enabled rule and an open incident. Verify:

```rust
assert_eq!(put_rule.status(), StatusCode::OK);
assert_eq!(updated_rule["threshold"], 42.0);
assert_eq!(updated_rule["enabled"], true);
assert_eq!(delete_rule.status(), StatusCode::NO_CONTENT);
assert_eq!(active_rule_count(&pool).await, 0);
assert_eq!(incident_status(&pool, incident_id).await, "resolved");
assert_eq!(resolved_outbox_count(&pool, incident_id).await, 1);
```

Add an ingest test proving an archived enabled rule is absent from `load_rules`.

- [x] **Step 2: Run focused tests**

```bash
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-api rule_
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest archived_rule
```

Expected: FAIL because update/archive behavior does not exist.

- [x] **Step 3: Implement update/archive**

Register:

```rust
.route("/api/alert-rules/{id}", put(update_alert_rule).delete(archive_alert_rule))
```

Use the existing `CreateAlertRuleRequest::validate` for full replacement, preserve `enabled`, and restrict all rule list/toggle/update queries to `archived_at IS NULL`. Archive in a transaction: lock the rule, set `enabled = FALSE, archived_at = now()`, transition `pending/open` incidents to resolved with an incremented `state_version`, and insert one idempotent outbox row with:

```text
incident:{incident_id}:resolved:{state_version}
```

Make its subject/body match existing resolved notifications using rule name, device, metric, last value, and threshold. Add `archived_at IS NULL` to `AlertEvaluator::load_rules`.

- [x] **Step 4: Re-run focused tests**

Expected: PASS.

### Task 4: Authenticated Web API Client

**Files:**
- Modify: `web/lib/api.ts`
- Modify: `web/lib/api.test.ts`

**Produces:** `ApiClient`, `Role`, `UnauthorizedApiError`, login/profile/token functions, authenticated versions of existing requests, and rule PUT/DELETE calls.

- [x] **Step 1: Write failing client tests**

Verify an authenticated request sends:

```ts
headers: expect.objectContaining({ authorization: "Bearer Aa1!bcDe" })
```

Add tests for login JSON, PUT `/api/alert-rules/{id}`, DELETE that URL, `PUT /api/auth/tokens/{role}`, and a `401` becoming `UnauthorizedApiError`.

- [x] **Step 2: Run client tests**

```bash
npm --prefix web test -- --run web/lib/api.test.ts
```

Expected: FAIL.

- [x] **Step 3: Implement one request helper**

Use:

```ts
type ApiClient = { apiBaseUrl: string; token: string };
function authenticatedHeaders(token: string, json = false): HeadersInit {
  return { authorization: `Bearer ${token}`, ...(json ? { "content-type": "application/json" } : {}) };
}
```

Every protected request has `cache: "no-store"`. Preserve clear endpoint errors for non-401 failures.

- [x] **Step 4: Re-run client tests**

Expected: PASS.

### Task 5: Login, Profile, And Role-Aware Dashboard

**Files:**
- Create: `web/components/login-screen.tsx`
- Create: `web/components/profile-menu.tsx`
- Create: `web/components/login-screen.test.tsx`
- Create: `web/components/profile-menu.test.tsx`
- Modify: `web/components/dashboard.tsx`
- Modify: `web/components/command-control.tsx`
- Modify: `web/app/globals.css`

**Produces:** token login gate, session storage lifecycle, profile token replacement, logout, and disabled viewer commands.

- [x] **Step 1: Write failing component tests**

Test login input submission calls `onLogin("Aa1!bcDe", "admin")`; viewer mode hides command submission and admin profile target; admin token self-replacement updates browser storage; logout removes `rush-iot-nano.access-token`.

- [x] **Step 2: Run component tests**

```bash
npm --prefix web test -- --run web/components/login-screen.test.tsx web/components/profile-menu.test.tsx
```

Expected: FAIL because the components do not exist.

- [x] **Step 3: Implement dashboard role lifecycle**

Render only `LoginScreen` while unauthenticated. On successful login, persist `rush-iot-nano.access-token`, build the client, call `/api/auth/me`, and then load dashboard data. On `UnauthorizedApiError`, remove storage and return to login. Add a compact profile control showing role and logout. The profile never displays existing tokens; viewer targets viewer only and admin selects either role.

- [x] **Step 4: Re-run component tests**

Expected: PASS.

### Task 6: Role-Aware Rule Controls

**Files:**
- Modify: `web/components/alert-panel.tsx`
- Modify: `web/components/alert-panel.test.tsx`
- Modify: `web/app/globals.css`

**Produces:** create/edit form modes, admin icon controls, archive confirmation, and viewer read-only rules/incidents.

- [x] **Step 1: Write failing UI tests**

For an admin render, click `Edit High temperature`, assert the form is prefilled and `Save changes` triggers PUT. Click `Delete High temperature`, accept confirmation, and assert DELETE. For viewer, assert no create, edit, delete, toggle, or acknowledge controls are present.

- [x] **Step 2: Run focused UI tests**

```bash
npm --prefix web test -- --run web/components/alert-panel.test.tsx
```

Expected: FAIL.

- [x] **Step 3: Implement rule lifecycle controls**

Pass `ApiClient` and `role` into `AlertPanel`. Reuse the current form for edit state, show `Save changes` and `Cancel`, and call `updateAlertRule`. Use `Pencil` and `Trash2` Lucide icon buttons with labels/tooltips; use a confirmation before `archiveAlertRule`. Admin-only controls include form, enable checkbox, icon buttons, and incident acknowledge.

- [x] **Step 4: Re-run focused UI tests**

Expected: PASS.

### Task 7: Operations And Full Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/operations.md`

- [x] **Step 1: Document initial access**

Document the hard-coded initial token values:

```text
admin:  NanoAdmin@1234
viewer: NanoView@1234
```

Document first-start bootstrap, immediate token rotation from profile, and
Argon2 storage after bootstrap.

- [x] **Step 2: Format and run verification**

```bash
cargo fmt --check
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test --workspace
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot scripts/verify-failures.sh
npm --prefix web test
npm --prefix web run build
scripts/e2e-local.sh
STRESS_EVENTS=10000 STRESS_DEVICES=100 scripts/stress-local.sh
```

Expected: every command exits zero. If the data-plane scripts depend on a running local stack, start it first and report the exact blocked dependency if unavailable.

- [x] **Step 3: Inspect authentication-sensitive output**

Confirm test output, docs, frontend text, and Rust errors contain no
unintended token values and that server startup errors do not expose
profile-managed tokens.

## Review Checklist

- Spec coverage: Tasks 1-2 cover bootstrap, bearer auth, roles, profile replacement, and login rate limiting; Task 3 covers edit/archive and notifications; Tasks 4-6 cover client behavior and UI; Task 7 covers operator setup and full verification.
- Placeholder scan: no deferred work or unspecified validation remains.
- Type consistency: `Role`, `ApiClient`, `CreateAlertRuleRequest`, `updateAlertRule`, and `archiveAlertRule` have one definition and are consumed consistently.
