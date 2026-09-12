# Task 2e Resource Authorization Report

## Changed Paths

- `crates/iot-storage/src/lib.rs`
- `crates/iot-storage/tests/resource_authorization.rs`

Added storage-owned account, permission, resource-kind, and authorization-subject
types; the `AuthorizationRepository` port; inherent `PlatformStore` methods for
SQLite and Timescale; and SQLite plus ignored guarded approved-storage-
authorization contract tests. No API authorization types or routes were
imported.

## TDD RED Evidence

Initial focused compilation command:

```text
cargo test -p iot-storage --test resource_authorization
```

Before production implementation, the command failed during compilation with
the expected missing-port errors:

```text
error[E0432]: unresolved imports `iot_storage::AccountClass`,
`iot_storage::AuthorizationRepository`, `iot_storage::AuthorizationSubject`,
`iot_storage::ResourcePermission`
error[E0599]: no method named `device_permission` found for reference
`&PlatformStore`
error[E0599]: no method named `asset_permission` found for reference
`&PlatformStore`
```

The RED run exited with status `101`.

## Verification

- Initial post-implementation SQLite contract: 1 passed, 0 failed.
- `cargo test -p iot-storage --tests`: 27 passed, 0 failed, 14 ignored.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- No `IOT_NANO_TIMESCALE_TEST_URL` was available, so no live Timescale test was run.

## Commit

`1776364` (`feat(storage): add resource authorization repository`)

## Concerns

- Timescale runtime behavior was not exercised because Timescale is stopped and
  no safe disposable `iot_nano_test_*` URL was available. The guarded test is
  present and compiled, but remains ignored.
- The worktree still contains unrelated user-owned deleted documents; they were
  not staged or modified.

## Review Resolution

The approved storage authorization contract intentionally diverges from the
legacy API resource authorizer for a deleted device with a stale active direct
share. Storage returns `None`, rather than exposing the stale share. This is
the explicit Task 2e requirement, "A deleted device has no owner/share-derived
permission", and is the fail-closed monolith security behavior.

The legacy API resource-authorizer implementation must be removed when API
routes migrate to the storage authorization port, leaving this approved storage
authorization contract as the single source of truth.

Focused SQLite review verification:

- `cargo test -p iot-storage --test resource_authorization sqlite_resource_authorization`: 5 passed, 0 failed, 1 filtered out.
- Covered independently: existing unshared device and asset, pending-only
  device and asset shares, deleted device with an active direct share, and
  inheritance at the 64-ancestor inclusion boundary with a 65th-only share
  denied.
- The ignored Timescale parity test executes the same assertions when a guarded
  disposable `IOT_NANO_TIMESCALE_TEST_URL` is available; it was not run.

Review fix commit: `d174a3c57b44e41adf655a8ddd8d4cc685edb5d7`
