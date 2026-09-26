# Application Domain Profiles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make resource profiles, chart configuration, and asset containment rules application-scoped, beginning with PowerMonitor.

**Architecture:** Add an application-domain storage model independent of the legacy tenant-global profile foreign keys. OAuth tokens identify the application, so public APIs resolve catalog entries, resource assignments, and live chart configuration from `(tenant_id, app_id)`. Tenant Console manages a selected application's domain catalog; PowerMonitor consumes only its own catalog.

**Tech Stack:** Rust, Axum, SQLx (SQLite and Timescale/PostgreSQL), Askama, TypeScript, Next.js, Vitest.

## Global Constraints

- Work directly in the user-designated `monolith` checkout; preserve all existing dirty changes.
- Do not use a global or tenant-global profile as a fallback in the new application-domain endpoints.
- `powermonitor` is the first application; no PowerMonitor-specific database schema.
- Only asset profiles may define `contains` rules; device profiles have no topology rule.
- Use targeted Rust and PowerMonitor tests, never workspace-wide build/test commands.
- No commit is created as part of this implementation.

---

### Task 1: Persist Application Domain Catalogs and Assignments

**Files:**
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Modify: `crates/iot-storage/src/lib.rs`
- Create: `crates/iot-storage/src/contracts/application_domain.rs`
- Modify: `crates/iot-storage/src/contracts/mod.rs`
- Create: `crates/iot-storage/src/domain/application_domain.rs`
- Modify: `crates/iot-storage/src/domain/mod.rs`
- Test: `crates/iot-storage/tests/application_domain_profiles.rs`

**Interfaces:**
- Produces `ApplicationDomainProfileRepository` for catalog CRUD, assignment lookup/upsert, and asset profile containment rules.
- Produces `ApplicationDomainResourceKind::{Asset, Device}` and `ApplicationDomainProfile` shared by the console and public API.

- [ ] **Step 1: Write failing SQLite storage tests**
  - Create a PowerMonitor application and assert profile catalog isolation by app, assignment replacement/clear, and `contains` relation validation.
- [ ] **Step 2: Run the focused test and verify the missing repository API fails**
  - Run `./scripts/dev/cargo-lane.sh application-domain -- test -p iot-storage --test application_domain_profiles -- --test-threads=1`.
- [ ] **Step 3: Add schema and repository implementation**
  - Add `application_domain_profiles`, `application_asset_profile_relations`, and `resource_application_profile_assignments` for both database backends.
  - Validate JSON objects, resource kind, tenant/application ownership, profile in-use deletion, and asset-only `contains` rules.
- [ ] **Step 4: Run the focused storage test**
  - Re-run the Task 1 command and require all cases to pass.

### Task 2: Expose App-Scoped Profile APIs and Live Views

**Files:**
- Modify: `services/iot-nano-api/src/public_v1.rs`
- Modify: `services/iot-nano-api/tests/public_v1.rs`

**Interfaces:**
- `GET /api/v1/application-domain/profiles?kind=asset|device`
- `PUT /api/v1/assets/{asset_id}/application-profile`
- `PUT /api/v1/devices/{device_id}/application-profile`
- Existing live-view routes resolve the application's assignment, never `assets.asset_profile_id` or `devices.device_profile_id`.

- [ ] **Step 1: Write failing API tests**
  - Assert the app token sees only its catalog, a manager may assign/clear one app profile, and live view ignores a legacy global profile when no application assignment exists.
- [ ] **Step 2: Run the focused API test and verify it fails for the new routes**
  - Run `./scripts/dev/cargo-lane.sh application-domain -- test -p iot-nano-api --test public_v1 -- --test-threads=1`.
- [ ] **Step 3: Implement public handlers**
  - Authorize catalog read by resource scope; authorize assignment by manager permission; use token `app_id` for all storage calls.
  - Return only app profile IDs and chart settings from live view.
- [ ] **Step 4: Run the focused API test**
  - Re-run the Task 2 command and require all cases to pass.

### Task 3: Add Tenant Console Application Domain Management

**Files:**
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_applications.html`
- Create: `services/iot-nano-monolith/templates/platform_ui/tenant_application_domain.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_navigation.html`
- Modify: `services/iot-nano-monolith/tests/management_sessions.rs`
- Modify: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

**Interfaces:**
- `GET /tenant/applications/{app_id}` renders the selected app's profile catalog and asset containment rules.
- Tenant management JSON endpoints create, update, delete profiles and create/delete `contains` rules for that app only.

- [ ] **Step 1: Write failing session/template tests**
  - Assert tenant access cannot open another tenant's app domain, and the rendered PowerMonitor page includes profile and relation controls.
- [ ] **Step 2: Run the focused tests and verify they fail**
  - Run `./scripts/dev/cargo-lane.sh application-domain -- test -p iot-nano-monolith --test management_sessions -- --test-threads=1`.
- [ ] **Step 3: Implement routes, page model, template, and JSON form actions**
  - Link each registered application to its Domain profile page.
  - Remove generic profile pages from tenant navigation so tenant operators configure profiles through an application.
  - Use the existing client-side JSON validation and notice behavior.
- [ ] **Step 4: Run focused monolith tests**
  - Re-run management sessions and `platform_ui_templates` tests.

### Task 4: Switch PowerMonitor to Application-Domain Profiles

**Files:**
- Modify: `apps/powermonitor/lib/browser-api.ts`
- Modify: `apps/powermonitor/components/powermonitor-dashboard.tsx`
- Modify: `apps/powermonitor/tests/browser-api.test.ts`
- Modify: `apps/powermonitor/tests/dashboard-contract.test.tsx`
- Modify: `apps/powermonitor/tests/live-telemetry-charts.test.tsx`

**Interfaces:**
- PowerMonitor reads `/api/v1/application-domain/profiles` and saves profile assignments through dedicated application-profile routes.
- Current profile comes from the selected resource's app-scoped live view.

- [ ] **Step 1: Write failing browser API/dashboard tests**
  - Assert catalog request includes `kind`, assignment uses the dedicated route, and a selected chart uses its app-scoped live view.
- [ ] **Step 2: Run focused frontend tests and verify they fail**
  - Run `npm --prefix apps/powermonitor test -- browser-api.test.ts dashboard-contract.test.tsx live-telemetry-charts.test.tsx`.
- [ ] **Step 3: Replace global profile client calls**
  - Remove use of `asset_profile_id` and `device_profile_id` for PowerMonitor selection and assignment.
  - Preserve five-second, visibility-aware telemetry polling and existing sharing/control behavior.
- [ ] **Step 4: Run focused frontend tests**
  - Re-run the Task 4 command and require all selected tests to pass.

### Task 5: Seed PowerMonitor's Application Domain

**Files:**
- Modify: `scripts/dev/seed-local-platform.sh`
- Test: `scripts/dev/seed-local-platform.sh` manual local seed contract

**Interfaces:**
- The seed creates/updates `powermonitor` before its domain profiles.
- It seeds `Power Farm`, `Power Zone`, and `Power Meter`, a `Farm contains Zone` rule, profile assignments, and the existing two-farm topology.

- [ ] **Step 1: Update the seed to use application-domain management endpoints**
  - Remove calls to generic device/asset profile endpoints and generic profile IDs from resource creation/update payloads.
- [ ] **Step 2: Run shell syntax validation**
  - Run `bash -n scripts/dev/seed-local-platform.sh`.
- [ ] **Step 3: Run final targeted verification**
  - Run Rust formatter check on modified Rust files, `git diff --check`, targeted storage/API/monolith tests, and focused PowerMonitor tests.
