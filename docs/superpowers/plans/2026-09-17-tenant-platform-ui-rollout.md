# Tenant Platform UI Rollout Roadmap

**Source:** `docs/superpowers/specs/2026-09-17-tenant-scoped-authorization-design.md`

## Decisions

- The built-in Platform UI reuses `web/` (Next.js 16, React 19, TypeScript).
  No new frontend framework or second platform console is introduced.
- One authenticated principal has exactly one account kind:
  `system`, `tenant`, or `user`.
- `/system` is System Account only; `/tenant` is Tenant Account only; `/app`
  is the simple built-in User workspace.
- `/app` lists only Devices and Assets that the User owns or is authorized to
  access. It is not the full management console.
- `apps/powermonitor` remains the existing external operational application
  for richer telemetry, alerts, commands, and controls. This rollout does not
  refactor, add tenant UI to, remove features from, or otherwise adapt
  PowerMonitor. Its current feature set remains unchanged.
- This is a development cutover. There is no legacy authorization fallback,
  dual-read/dual-write path, runtime default tenant, compatibility adapter, or
  old role-only route after its replacement is live. Disposable development
  databases and fixtures are reset or migrated once, then obsolete code is
  removed in the same slice.

## Phase 0: Platform Route and Session Contract

**Goal:** Define one secure account-routing model before adding UI screens.

- Add `principal_kind` and optional `tenant_id` to authenticated session/API
  identity responses.
- Define server-enforced route/API boundaries:

  ```text
  System Account -> /system and system lifecycle APIs only
  Tenant Account -> /tenant and its own tenant APIs only
  User           -> /app and authorized resource APIs only
  ```

- Implement a post-login router that never exposes a role switcher or System
  navigation to Tenant or User principals.
- Delete superseded role-only client routing and tests once the new contract
  passes.

**Exit gate:** Every API request can identify account kind and tenant scope;
cross-kind routes are denied by the server.

## Phase 1: Tenant and Identity Boundary

**Goal:** Implement the spec's absolute data boundary before resource UI.

- Create System Account bootstrap and lifecycle support.
- Create `tenants` and one-to-one `tenant_accounts`.
- Add immutable non-null `tenant_id` to every tenant-scoped record: Users,
  Groups, Assets, Devices, tokens, telemetry, alerts, dashboards, OAuth
  applications, permissions, and audit records.
- Add composite same-tenant foreign keys and indexes required by the spec.
- For development databases, create/reset the named initial tenant explicitly;
  do not retain a default-tenant fallback path.
- Implement login and registration forms/requests:

  ```text
  System Account: username + password
  Tenant Account: tenant slug + password
  User:           tenant slug + username + password
  ```

- Make every storage/API lookup resolve by `(tenant_id, resource_id)`.

**Exit gate:** A User, Tenant Account, or resource cannot read, write, own, or
reference data in another Tenant.

## Phase 2: System Account APIs and `/system` UI

**Goal:** Deliver the platform lifecycle console without tenant data access.

- System APIs: create, suspend, reactivate, and delete Tenants; create,
  disable, and reset Tenant Account credentials.
- Infrastructure screen: health, listener status, migration state, and
  non-secret configuration status only.
- `/system` UI: tenant table, tenant lifecycle actions, Tenant Account reset,
  and operational health panels.
- Do not render tenant Devices, Assets, telemetry, Users, or raw deployment
  secrets in this account class.

**Exit gate:** System Account manages tenant lifecycle but receives `not found`
or `forbidden` for tenant resource APIs.

## Phase 3: Tenant Account APIs and `/tenant` UI

**Goal:** Replace the old broad management home with a tenant-scoped console.

- Tenant APIs: Users, Assets, Devices, Profiles, tokens, alerts, OAuth
  applications, and tenant audit reads, always scoped to the authenticated
  Tenant Account's tenant ID.
- `/tenant` UI: tenant home, Users, Assets, Devices, Profiles, device tokens,
  and tenant settings.
- Tenant Account may only create Users in its own Tenant and cannot access
  `/system` routes or another Tenant's data.
- Remove old management pages/routes that accept or infer an arbitrary tenant
  outside the authenticated session.

**Exit gate:** Tenant Account has full management capability inside one tenant
and no visibility outside it.

## Phase 4: Simple Built-in User Workspace at `/app`

**Goal:** Give normal Users a small platform home before advanced sharing UI.

- `/app` shows only authorized Device and Asset lists.
- Device/Asset detail shows permitted state, telemetry, and alerts.
- Viewer actions are read-only; Manager actions expose permitted controls only.
- In this phase, owner access is available immediately. Shared-resource rows
  appear after Phase 5 authorization is complete.
- Do not include tenant lifecycle, user administration, token provisioning,
  profile administration, or System navigation.

**Exit gate:** User list/detail APIs and UI never expose an out-of-scope
resource, even when a guessed UUID is used.

## Phase 5: Groups, Resource Permissions, and Containment Inheritance

**Goal:** Implement the spec's complete user sharing model.

1. Add tenant-scoped User Groups and same-tenant membership checks.
2. Add direct User resource permissions.
3. Add Group resource permissions.
4. Add Asset containment inheritance with depth limit 64 and cycle rejection.
5. Return effective permission and access source from authorized list APIs.
6. Add `/tenant` permission/group management UI.
7. Update only `/app` to display owned and shared resources from the
   server-provided authorized union. PowerMonitor is explicitly out of scope.

**Exit gate:** Direct, group, inherited, revoked, and moved-resource access
matches `owner > manager > viewer > deny` for every list and single-resource
request.

## Phase 6: Gateway Topology, Descriptive Relations, and Audit

**Goal:** Add operational topology without expanding authorization semantics.

- Tenant-scoped gateway-child assignment with exact pair validation for
  telemetry and commands.
- Gateway reassignment, detach, delete protection, revision increment, and
  audit events.
- Tenant-scoped descriptive device relations; reject reserved `gateway_child`,
  cross-tenant endpoints, and self-relations.
- `/tenant` UI for topology and relations; `/app` only shows information
  already authorized by resource permission. PowerMonitor is out of scope.

**Exit gate:** Gateway topology never grants user resource access and stale
gateway traffic is rejected after reassignment.

## Phase 7: Rollout Verification and Legacy Removal

**Goal:** Ship one tenant-aware platform path with no compatibility branch.

- Run the full authorization matrix from the tenant-scoped design spec.
- Run SQLite and Timescale migration/constraint tests.
- Run system, tenant, and built-in `/app` browser end-to-end tests.
- Keep existing PowerMonitor tests unchanged and outside this rollout.
- Remove superseded schema fields, single-tenant assumptions, old role-only
  routers, stale fixtures, and dead UI code after each replacement passes.
- Keep no runtime fallback for old data, old sessions, or old authorization.

**Exit gate:** Clean development bootstrap creates one System Account, tenants,
Tenant Accounts, and Users through only the new tenant-aware path.

## Parallel Work

- Phase 1 storage/identity is the critical path and must land first.
- The `web/` shell and static account-specific layouts may be built with mocked
  contracts during Phase 1, but no real integration lands before Phase 1 exits.
- After Phase 1, System UI, Tenant UI, and `/app` UI can progress in parallel
  against stable server contracts.
- Phase 5 UI work starts only after permission repository/API semantics are
  tested; UI must not calculate authorization locally.
