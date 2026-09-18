# Storage Modular Build Lanes Design

## Goal

Make normal development feedback substantially faster while preserving the
iot-storage public facade and every SQLite/Timescale behavior. Split the
large storage implementation into domain modules without adding new crates.

## Constraints

- Public iot-storage import paths, public types, trait signatures, database
  schema, migration ordering, and API behavior remain compatible.
- iot-storage remains one crate. This work creates no cross-crate API.
- SQLite and Timescale parity remains a contract.
- Unrelated concurrent worktrees never share one Cargo target directory.
- Timescale, process, and external-app tests are explicit release-lane work.

## Module Structure

lib.rs becomes a facade containing module declarations, re-exports,
PlatformStore, shared configuration, and shared errors only. Public callers
continue importing from iot_storage.

The private layout is:

  src/store.rs                    PlatformStore backend dispatch
  src/contracts/application.rs    application and OAuth contract types
  src/contracts/identity.rs       device identity contract types
  src/contracts/resources.rs      assets, devices, groups, permissions
  src/contracts/commands.rs       command and notification contracts
  src/contracts/telemetry.rs      telemetry and aggregate contracts
  src/contracts/alerts.rs         alert and incident contracts
  src/domain/application.rs       application and OAuth persistence
  src/domain/authorization.rs     ownership, groups, inheritance
  src/domain/commands.rs          commands, lifecycle, outbox
  src/domain/telemetry.rs         ingest, aggregate, retention
  src/domain/alerts.rs            evaluation, incidents, notifications
  src/backend/sqlite.rs           SQLite-only query helpers
  src/backend/timescale.rs        Timescale-only query helpers

Existing audit, device_relations, management, public_api, and tenant_identity
modules remain in place until a whole domain can move together.

## Build And Test Lanes

A repository wrapper chooses a persistent Cargo target from worktree identity
and lane name. It can use sccache if installed, but it does not require or
install it.

  fast-storage      one iot-storage target or named test
  fast-monolith     one API, monolith, or MQTT target
  sqlite-contract   SQLite storage suite
  timescale         explicit ignored Timescale tests
  release           workspace compile, process, and external app contract

The external PowerMonitor contract is ignored with a release-only reason and
still executes npm ci, Next build, and process startup when release invokes it.

## Acceptance Criteria

- lib.rs contains no domain SQL implementation and is materially smaller.
- Existing public callers compile through the facade re-exports.
- Focused SQLite tests pass after every extracted domain.
- Two worktrees select different targets by default.
- Release coverage retains workspace, Timescale, process, and external checks.
