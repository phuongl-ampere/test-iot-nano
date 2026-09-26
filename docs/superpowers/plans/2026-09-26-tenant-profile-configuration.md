# Tenant Profile Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace application-scoped domain profiles with one versioned, importable, exportable JSON profile configuration per tenant.

**Architecture:** A `TenantProfileConfiguration` aggregate is the canonical JSON document. Its asset/device definitions and containment rules are validated and projected into tenant-scoped tables used by assignments and the public runtime. Import validates the aggregate before one transaction replaces the tenant projection; export reconstructs the canonical document. The tenant Profile page edits the aggregate JSON only, while the existing Asset Profile and Device Profile pages remain unchanged.

**Tech Stack:** Rust, Axum, SQLx for SQLite and Timescale/PostgreSQL, Serde JSON, Askama, TypeScript, Next.js, Vitest.

## Global Constraints

- Work only in the existing repository checkout and preserve unrelated dirty changes.
- A tenant profile configuration has `version: 1` and exactly one persisted configuration per `tenant_id`.
- Imported documents contain configuration only: definitions, hierarchy/inheritance, containment rules, and permission definitions; they exclude resources, users, credentials, assignments, telemetry, alerts, and audit data.
- Import validates the full document before entering the replacement transaction; a failure must leave both backends unchanged.
- Asset Profile and Device Profile remain tenant-level pages and APIs with their current behavior.
- No catalog or runtime lookup may require `app_id`; remove the Application Domain page and application-scoped domain-profile routes.
- Commit only files authored for this feature, never existing user changes.

---

### Task 1: Create the tenant-profile storage aggregate and atomic replacement

**Files:**
- Modify: `crates/iot-storage/migrations/0001_platform.sql:202-251`
- Modify: `crates/iot-storage/src/contracts/application_domain.rs:1-221`
- Modify: `crates/iot-storage/src/domain/application_domain.rs:1-952`
- Modify: `crates/iot-storage/src/domain/mod.rs`
- Modify: `crates/iot-storage/src/contracts/mod.rs`
- Modify: `crates/iot-storage/tests/application_domain_profiles.rs`

**Interfaces:**
- Produces `TenantProfileConfiguration { version: u16, profiles: Vec<TenantProfileDefinition>, containment_rules: Vec<TenantProfileContainmentRule>, permission_definitions: serde_json::Value }`.
- Replaces every `app_id: &str` repository argument with tenant-only access.
- Adds `export_tenant_profile_configuration(tenant_id)` and `replace_tenant_profile_configuration(tenant_id, configuration)`.

- [ ] **Step 1: Write failing SQLite storage tests**

  Add tests proving that a tenant export returns this canonical document and that import replaces only its catalog:

  ```rust
  let configuration = TenantProfileConfiguration {
      version: 1,
      profiles: vec![tenant_profile("Farm", ApplicationDomainResourceKind::Asset)],
      containment_rules: vec![],
      permission_definitions: serde_json::json!({"roles": ["operator"]}),
  };
  TenantProfileRepository::replace_tenant_profile_configuration(&store, tenant_id, configuration.clone()).await?;
  assert_eq!(TenantProfileRepository::export_tenant_profile_configuration(&store, tenant_id).await?, configuration);
  ```

  Add a second test that attempts an import with a duplicate profile name or an unknown containment profile ID, then asserts the previously exported document is identical. Add tenant-isolation coverage and an assertion that resource assignments are cleared during a successful replacement rather than exported.

- [ ] **Step 2: Run the focused storage tests and verify RED**

  Run: `./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-storage --test application_domain_profiles -- --test-threads=1`

  Expected: compilation failure because `TenantProfileConfiguration` and `TenantProfileRepository` do not exist.

- [ ] **Step 3: Implement the migration, types, and repository**

  Replace the `app_id` keys and foreign keys in `application_domain_profiles`, `application_asset_profile_relations`, and `resource_application_profile_assignments` with tenant-scoped keys. Add `tenant_profile_configurations(tenant_id PRIMARY KEY, version, permission_definitions, updated_at)` to preserve the root-only configuration fields. Implement validation before beginning the write transaction, including version `1`, unique `(resource_kind, name)`, JSON-object definitions/live views/permission definitions, asset-only containment, no self relation, and every referenced profile ID belonging to the document.

  Within one SQLite transaction and one PostgreSQL transaction, delete old assignments and relations, replace catalog rows plus the configuration row, then insert the new projection. Use the existing typed `ApplicationDomainProfileError` mapping for malformed input, conflicts, unavailable storage, and missing profiles. Rename repository methods and their `PlatformStore` implementation to tenant terminology so no method exposes an `app_id` parameter.

- [ ] **Step 4: Run the focused storage tests and verify GREEN**

  Run: `./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-storage --test application_domain_profiles -- --test-threads=1`

  Expected: all tenant-profile storage tests pass.

- [ ] **Step 5: Commit the storage change**

  ```bash
  git add crates/iot-storage/migrations/0001_platform.sql crates/iot-storage/src/contracts/application_domain.rs crates/iot-storage/src/contracts/mod.rs crates/iot-storage/src/domain/application_domain.rs crates/iot-storage/src/domain/mod.rs crates/iot-storage/tests/application_domain_profiles.rs
  git commit -m "feat(storage): make domain profiles tenant scoped"
  ```

### Task 2: Provide tenant JSON import/export management endpoints and page

**Files:**
- Modify: `services/iot-nano-monolith/src/management/mod.rs:260-380`
- Modify: `services/iot-nano-monolith/src/management/operator_api.rs:658-903`
- Modify: `services/iot-nano-monolith/src/management/routes/tenant.rs:95-1850`
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Create: `services/iot-nano-monolith/templates/platform_ui/tenant_profile.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_navigation.html:1-24`
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs`
- Modify: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

**Interfaces:**
- `GET /tenant/profile` renders the Profile page for the authenticated tenant.
- `GET /api/management/profile/export` returns `TenantProfileConfiguration` as JSON.
- `PUT /api/management/profile/import` accepts `TenantProfileConfiguration` and atomically replaces the tenant configuration.
- `GET /api/management/profile` and `PUT /api/management/profile` are the JSON editor read/write endpoints and share the export/import implementation.

- [ ] **Step 1: Write failing session and template tests**

  Add a tenant-session test that requests `/tenant/profile`, exports a document, updates it with `PUT /api/management/profile/import`, and verifies the next export has the replacement content. Include an invalid-document request that returns `400` and retains the previous export. Assert a second tenant receives neither document nor catalog. In `platform_ui_templates.rs`, assert the page includes `data-tenant-profile-json`, an export control, an import control, and a `Profile` navigation entry while still including `/tenant/profiles/asset` and `/tenant/profiles/device`.

- [ ] **Step 2: Run the focused monolith tests and verify RED**

  Run: `./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-monolith --test management_sessions --test platform_ui_templates -- --test-threads=1`

  Expected: the new `/tenant/profile` and `/api/management/profile/*` routes return `404`.

- [ ] **Step 3: Implement tenant-only handlers and JSON editor**

  Add tenant-only handlers that call `require_tenant_account`, obtain the tenant ID, require the existing mutation authorization on writes, and map typed validation errors to `400` without exposing another tenant. Register only tenant-scoped routes. Implement the Askama model and template with one JSON textarea, a `GET` export button that downloads `tenant-profile.json`, a file picker/import action that parses JSON client-side before calling the import endpoint, and a save action that calls the same import endpoint.

  Add `Profile` to tenant navigation. Retain the Asset Profile and Device Profile navigation items and their existing endpoints. Do not render profile controls from an Application page.

- [ ] **Step 4: Run the focused monolith tests and verify GREEN**

  Run: `./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-monolith --test management_sessions --test platform_ui_templates -- --test-threads=1`

  Expected: tenant profile, template, and existing profile-tab assertions pass.

- [ ] **Step 5: Commit the management/UI change**

  ```bash
  git add services/iot-nano-monolith/src/management/mod.rs services/iot-nano-monolith/src/management/operator_api.rs services/iot-nano-monolith/src/management/routes/tenant.rs services/iot-nano-monolith/src/platform_ui.rs services/iot-nano-monolith/templates/platform_ui/tenant_profile.html services/iot-nano-monolith/templates/platform_ui/tenant_navigation.html services/iot-nano-monolith/tests/management_sessions.rs services/iot-nano-monolith/tests/platform_ui_templates.rs
  git commit -m "feat(management): add tenant profile import and export"
  ```

### Task 3: Switch public profile catalog, assignments, and live views to tenant scope

**Files:**
- Modify: `services/iot-nano-api/src/public_v1.rs:56-1140`
- Modify: `services/iot-nano-api/tests/public_v1.rs`
- Modify: `apps/powermonitor/lib/browser-api.ts`
- Modify: `apps/powermonitor/components/resource-edit-drawer.tsx`
- Modify: `apps/powermonitor/tests/browser-api.test.ts`
- Modify: `apps/powermonitor/tests/resource-edit-drawer.test.tsx`

**Interfaces:**
- Public catalog route is `GET /api/v1/tenant-profile/profiles?kind=asset|device`.
- Assignments use `PUT /api/v1/assets/{asset_id}/tenant-profile` and `PUT /api/v1/devices/{device_id}/tenant-profile`.
- Live views resolve profile assignments by `(tenant_id, resource_kind, resource_id)` only.

- [ ] **Step 1: Write failing API and frontend tests**

  Update public API tests so two OAuth applications belonging to the same tenant read the same tenant catalog, while an application from another tenant cannot. Assert the new tenant-profile assignment URLs and live views resolve the tenant assignment without consuming `principal.app_id`. Update browser API tests to expect `/api/v1/tenant-profile/profiles?kind=...` and the renamed assignment URLs.

- [ ] **Step 2: Run focused API/frontend tests and verify RED**

  Run:

  ```bash
  ./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-api --test public_v1 -- --test-threads=1
  npm --prefix apps/powermonitor test -- browser-api.test.ts resource-edit-drawer.test.tsx
  ```

  Expected: tests fail because old application-domain routes and `app_id` repository calls remain.

- [ ] **Step 3: Implement tenant-scoped runtime calls**

  Replace every public API repository call that passes `principal.app_id` with the tenant-only method. Rename paths and handlers from `application-profile` / `application-domain` to `tenant-profile`; retain OAuth authentication and resource-scope authorization. Update PowerMonitor request helpers and resource editor controls to use the new paths without changing generic Asset Profile or Device Profile behavior.

- [ ] **Step 4: Run focused API/frontend tests and verify GREEN**

  Run the commands from Step 2 again.

  Expected: public API and PowerMonitor targeted tests pass with tenant-shared catalog behavior.

- [ ] **Step 5: Commit the runtime change**

  ```bash
  git add services/iot-nano-api/src/public_v1.rs services/iot-nano-api/tests/public_v1.rs apps/powermonitor/lib/browser-api.ts apps/powermonitor/components/resource-edit-drawer.tsx apps/powermonitor/tests/browser-api.test.ts apps/powermonitor/tests/resource-edit-drawer.test.tsx
  git commit -m "feat(api): resolve profile runtime by tenant"
  ```

### Task 4: Remove the application-domain surface and update registrations/docs

**Files:**
- Modify: `services/iot-nano-monolith/src/management/mod.rs`
- Modify: `services/iot-nano-monolith/src/management/routes/tenant.rs`
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Delete: `services/iot-nano-monolith/templates/platform_ui/tenant_application_domain.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_applications.html`
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs`
- Modify: `services/iot-nano-monolith/tests/platform_ui_templates.rs`
- Modify: `services/iot-nano-api/src/lib.rs`
- Modify: `services/iot-nano-monolith/src/management/openapi.rs`

**Interfaces:**
- `/tenant/applications/{app_id}` no longer renders a profile catalog.
- No `/api/management/applications/{app_id}/domain-profiles*` or `/asset-profile-relations*` route remains.
- Generated OpenAPI documents only publish tenant-profile endpoints.

- [ ] **Step 1: Write failing removal/registration tests**

  Change the old application-domain session test to assert the legacy application route is absent (`404`) and that the tenant Profile page is the only HTML catalog management surface. Add assertions that management and public OpenAPI output excludes `applications/{app_id}/domain-profiles` and includes `/api/management/profile/import` plus `/api/v1/tenant-profile/profiles`.

- [ ] **Step 2: Run the focused test and verify RED**

  Run: `./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-monolith --test management_sessions --test platform_ui_templates -- --test-threads=1`

  Expected: assertions fail while legacy application routes/template registrations exist.

- [ ] **Step 3: Remove app-profile routes, UI references, and OpenAPI entries**

  Delete `platform_tenant_application_domain`, app-bound JSON handlers, page models, renderer/template registration, and every Application-page link into the old profile editor. Keep ordinary tenant OAuth application registration intact. Remove stale OpenAPI paths and replace their documentation with tenant profile import/export endpoints.

- [ ] **Step 4: Run focused removal and API contract tests**

  Run:

  ```bash
  ./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-monolith --test management_sessions --test platform_ui_templates -- --test-threads=1
  ./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-api --test library_boundary --test public_v1 -- --test-threads=1
  ```

  Expected: only tenant profile endpoints and UI remain.

- [ ] **Step 5: Commit the removal change**

  ```bash
  git add services/iot-nano-monolith/src/management/mod.rs services/iot-nano-monolith/src/management/routes/tenant.rs services/iot-nano-monolith/src/platform_ui.rs services/iot-nano-monolith/templates/platform_ui/tenant_applications.html services/iot-nano-monolith/templates/platform_ui/tenant_application_domain.html services/iot-nano-monolith/tests/management_sessions.rs services/iot-nano-monolith/tests/platform_ui_templates.rs services/iot-nano-api/src/lib.rs services/iot-nano-monolith/src/management/openapi.rs
  git commit -m "refactor(profile): remove application scoped management"
  ```

### Task 5: Run full feature verification and record the final changes

**Files:**
- Modify: `docs/superpowers/specs/2026-09-26-tenant-profile-config-design.md` only if verification reveals a contradiction requiring a design correction.

- [ ] **Step 1: Format changed Rust sources**

  Run: `cargo fmt --all -- --check`

  Expected: exit status `0`; otherwise run `cargo fmt --all`, inspect only the feature files, and rerun the check.

- [ ] **Step 2: Run all feature test suites**

  Run:

  ```bash
  ./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-storage --test application_domain_profiles -- --test-threads=1
  ./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-monolith --test management_sessions --test platform_ui_templates -- --test-threads=1
  ./scripts/dev/cargo-lane.sh tenant-profile -- test -p iot-nano-api --test public_v1 --test library_boundary -- --test-threads=1
  npm --prefix apps/powermonitor test -- browser-api.test.ts resource-edit-drawer.test.tsx
  git diff --check
  ```

  Expected: every command exits `0` and no whitespace errors are reported.

- [ ] **Step 3: Inspect the feature-only commit set**

  Run:

  ```bash
  git log --oneline --max-count=5
  git status --short
  ```

  Expected: each tenant-profile commit is scoped to feature paths; unrelated pre-existing work remains unstaged and untouched.
