# Hard-Cutover Legacy Removal and Module Boundaries

## Goal

Remove unused legacy user-application and database infrastructure from the
current monolith development topology. Make the remaining source easier to
navigate without changing current resource authorization, user capabilities,
device claim, MQTT, or PowerMonitor behavior.

This is a development-only hard cutover. Existing local SQLite and Timescale
data are disposable and must be reset before running the resulting code.

## Hard-Cutover Policy

- There is no compatibility migration, dual read, dual write, fallback route,
  or old request contract for removed concepts.
- An existing database containing removed tables or columns is not upgraded.
  Local development must reset and reseed its platform data.
- Tests for migration from the removed schema are deleted rather than retained
  as a supported upgrade path.
- Historical design documents may remain, but any document that could be read
  as current architecture is marked superseded by this design.

## Removed User-Application Model

The following concepts are removed everywhere from active source, schemas,
API contracts, OpenAPI, tests, seed data, and current operational docs:

- `users.default_app`;
- `user_app_grants` and `granted_apps`;
- `api_access_tokens`;
- the obsolete generic `Role`, `AuthenticatedUser`, and credential helpers
  that expose the removed app-selection model, provided no current consumer
  remains after the call-site audit.

A regular User has a tenant-scoped username/password, account class, ownership
and resource permissions, and an explicit capability set. New Users receive
`create_assets` and `claim_devices`; a Tenant Account may change capabilities.

Applications remain tenant OAuth/application registrations and application
domain profiles. They do not select a user's landing application and do not
grant resource access. The built-in platform root continues selecting System,
Tenant, or User workspace from the authenticated session kind.

## Canonical Storage

`iot-storage` remains the single stable crate facade for the monolith. It has
exactly two active fresh-install schema definitions:

- PostgreSQL/Timescale schema under `crates/iot-storage/migrations/`;
- SQLite schema under a private `crates/iot-storage/src/schema/` module.

The legacy `db/migrations/` directory is removed because no current runner
uses it. The unused `iot-sqldb-common` workspace crate is removed after a
dependency audit confirms that no package consumes it. `api_access_tokens` is
removed from both active schemas and its obsolete test assertions.

Schema code is private implementation detail. `iot-storage/src/lib.rs` keeps
only store setup, stable re-exports, shared errors, and thin initialization
entry points; it does not contain a multi-thousand-line SQLite DDL string.

## Source Boundaries

### iot-storage

Keep public re-exports in `lib.rs`, but split implementation by responsibility:

- `schema/{mod,sqlite,postgres}.rs` for fresh schema and only still-supported
  schema checks;
- `management/{mod,users,assets,devices,profiles,alerts,tokens}.rs` for
  management DTOs, validation, repositories, and backend-specific queries;
- existing `domain/` remains the domain boundary; split the large
  authorization implementation internally into access resolution, invitations,
  ownership, and permission mutation only where imports stay acyclic.

No new crate is created and no public `iot_storage::*` type or trait moves from
its consumer-facing path unless that type is itself removed legacy API.

### monolith management

Replace the single `services/iot-nano-monolith/src/management.rs` file with a
private `management/` module tree while preserving the public
`ManagementSessionRouter` and `bootstrap_system` exports:

- `session` for login, cookies, rate limiting, and authorization gates;
- `routes/system`, `routes/tenant`, and `routes/user` for HTML/form flows;
- `operator_api` for `/api/management/*` JSON handlers and response mapping;
- `openapi` for operator documentation only;
- `errors` for shared HTTP error conversion;
- `mod` for router composition and shared state.

Server-rendered platform UI templates and PowerMonitor remain distinct active
surfaces. PowerMonitor is an external application, not a legacy Platform UI.

## Worktree Hygiene

Local-only secrets and generated outputs must never be staged:

- ignore `apps/powermonitor/.key` until its owner/purpose is resolved;
- ignore `infra/monolith/local-platform-seed.env` and keep a safe tracked
  example when configuration documentation is needed;
- ignore `debug/__pycache__/`.

The untracked local files are removed from the worktree only after confirming
they are not inputs to the current development service. No secret value is
printed, moved into source, or committed.

## Verification

Each semantic removal starts with focused failing tests. The completed work
must run, through the persistent cargo lane where applicable:

1. storage SQLite contract, management-user, migration-safety, authorization,
   and public API tests;
2. API auth/public-v1 tests, including absence of removed request fields;
3. monolith management-session and platform template tests;
4. `cargo check` for `iot-storage`, `iot-nano-api`, and `iot-nano-monolith`;
5. PowerMonitor `npm test` and `npm run build`;
6. Timescale and external-app contract tests as the release lane when their
   declared dependencies are available;
7. `scripts/verify-no-legacy-runtime.sh`, `git diff --check`, and a literal
   scan proving removed identifiers have no active-source occurrence.

Splitting `management_sessions.rs` uses submodules beneath one integration test
root so the test remains one harness binary; organization must not needlessly
increase Rust test compile cost.
