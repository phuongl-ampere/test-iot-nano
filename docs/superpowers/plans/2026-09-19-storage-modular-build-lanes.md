# Storage Modular Build Lanes Implementation Plan

Goal: keep one stable iot-storage facade, split lib.rs by domain, and make
focused test lanes the default developer workflow.

Global constraints:

- Preserve every public iot_storage import path and trait signature.
- Do not change schema, migrations, SQLite/Timescale semantics, or public HTTP.
- Do not share one target directory across unrelated worktrees.
- Each task has focused tests and its own commit.

## Task 1: Isolated Timed Cargo Lanes

Files: scripts/dev/cargo-lane.sh, scripts/dev/release-verify.sh,
scripts/dev/test-cargo-lane.sh, .gitignore, README.md.

Create a wrapper that assigns an isolated persistent target by worktree key
and lane, preserves commands after double dash, optionally enables sccache,
and appends elapsed timing to an ignored local log. Add a release-only script
for workspace, Timescale, process, and external checks. Test target selection
and argument preservation before implementation.

## Task 2: External App Release Gate

Files: services/iot-nano-monolith/tests/external_app_contract.rs and
scripts/dev/release-verify.sh.

Mark the PowerMonitor external contract ignored with an explicit release-lane
reason. The release script executes that ignored target explicitly. Verify
listing the test no longer executes npm.

## Task 3: Facade, Store, And Contract Modules

Files: crates/iot-storage/src/lib.rs, store.rs, and contracts modules for
application, identity, resources, commands, telemetry, and alerts.

Move public data types and repository traits to contract modules, re-export
them from lib.rs, and move PlatformStore dispatch/accessors to store.rs.
Add a facade consumer compilation test and run focused application, identity,
and tenant authorization SQLite targets.

## Task 4: Application, OAuth, And Identity Domains

Files: lib.rs, domain/application.rs, domain/identity.rs, backend/sqlite.rs,
backend/timescale.rs.

Move full implementation blocks and their backend helpers together. Preserve
ApplicationRepository, OAuthRepository, and IdentityRepository behavior.
Run application_registry, oauth_persistence, and identity focused tests.

## Task 5: Authorization And Resource Domains

Files: lib.rs, domain/authorization.rs, contracts/resources.rs, backend files.

Move owner access, user groups, direct/group permissions, inheritance, and
authorized list logic. Preserve audit events and tenant scope. Run
resource_authorization, tenant_authorization_writes, device_authorization,
and device_relations tests.

## Task 6: Command And Notification Domains

Files: lib.rs, domain/commands.rs, contracts/commands.rs, backend files.

Move enqueue, idempotency, lifecycle, response, notification, and outbox
implementation. Run command_outbox, public_command_authorization,
notification_outbox, and selected core command tests.

## Task 7: Telemetry, Alert, And Retention Domains

Files: lib.rs, domain/telemetry.rs, domain/alerts.rs, contracts modules,
backend files.

Move ingest, aggregates, alert evaluation, incidents, notification creation,
and retention helpers. Run telemetry_aggregate, alert_evaluation,
alert_incident, and management_alerts SQLite tests.

## Task 8: Release Parity And Documentation

Files: README.md, docs/superpowers plans, release tooling.

Run facade compatibility checks, the workspace compile gate, SQLite suite,
explicit Timescale suite when the disposable database is available, process
tests, and the ignored external contract through the release script. Record
cold/warm lane timing and update the developer command table.
