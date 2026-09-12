# Task 4c Device Authorization Report

## Status

Implemented Task 4c within `crates/iot-storage` and
`services/iot-nano-monolith`.

## Changes

- Added typed-UUID storage operations for exact device-session and gateway-token authorization.
- PostgreSQL existence predicates decode `SELECT 1` as `i32` before normalizing to
  boolean presence; SQLite retains its `i64` decoding.
- Device sessions require an active, non-deleted device with no gateway child association.
  Both direct devices and gateways are valid sessions.
- Gateway authorization requires an active, non-deleted gateway and exact gateway/token
  identity. A child is optional; when supplied, it must be an active child of that gateway.
- Added the monolith `PlatformDeviceAuthorization` adapter using `IdentityRepository` and
  the storage authorization operations.
- Mapped denials to `AuthorizationError::Denied` and storage errors to a generic
  `AuthorizationError::Unavailable` message.
- Added SQLite contracts, guarded ignored Timescale parity contracts, and monolith adapter
  tests. The adapter tests use no HTTP client, URL, header, or service secret.

## TDD Evidence

Focused RED was run before production implementation. It failed because the new storage
operations, repository trait, and monolith adapter did not exist.

## Verification

- `cargo test -p iot-storage --test device_authorization`: 2 passed, 1 ignored.
- `cargo test -p iot-nano-monolith --test device_authorization`: 2 passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.

The Timescale test was not run because Timescale is stopped and no explicit safe
`IOT_NANO_TIMESCALE_TEST_URL` was supplied.

## Scope

No API, Core, MQTTD, legacy migration, deployment, or user-deleted document sources
were changed.
