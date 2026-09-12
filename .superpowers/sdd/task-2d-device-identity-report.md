# Task 2d Device Identity Repository Report

## Status

Implemented and committed. The implementation commit is `41ef3cf`
(`feat(storage): add device identity repository`).

## Changed Paths

- `crates/iot-storage/src/lib.rs`
- `crates/iot-storage/tests/identity.rs`

The pre-existing deleted documentation files were left untouched.

## RED Evidence

Command:

```text
cargo test -p iot-storage --test identity sqlite_identity_repository_authenticates_active_tokens_and_denies_invalid_tokens -- --exact --nocapture
```

Exact result before production implementation:

```text
error[E0432]: unresolved import `iot_storage::IdentityRepository`
 --> crates/iot-storage/tests/identity.rs:5:19
  |
5 | use iot_storage::{IdentityRepository, PlatformStore, PlatformStoreError};
  |                   ^^^^^^^^^^^^^^^^^^ no `IdentityRepository` in the root

error[E0599]: no variant, associated function, or constant named `DeviceTokenDenied` found for enum `PlatformStoreError` in the current scope
  --> crates/iot-storage/tests/identity.rs:45:54

error[E0599]: no method named `resolve_active_device_token` found for enum `PlatformStore` in the current scope
  --> crates/iot-storage/tests/identity.rs:80:24

error[E0599]: no method named `resolve_active_device_token` found for enum `PlatformStore` in the current scope
  --> crates/iot-storage/tests/identity.rs:94:23

error[E0599]: no method named `resolve_active_device_token` found for enum `PlatformStore` in the current scope
   --> crates/iot-storage/tests/identity.rs:115:29

error[E0599]: no method named `resolve_active_device_token` found for enum `PlatformStore` in the current scope
   --> crates/iot-storage/tests/identity.rs:131:25

Some errors have detailed explanations: E0432, E0599.
error: could not compile `iot-storage` (test "identity") due to 6 previous errors
```

## Tests and Results

Focused SQLite test:

```text
cargo test -p iot-storage --test identity sqlite_identity_repository_authenticates_active_tokens_and_denies_invalid_tokens -- --exact --nocapture
test result: ok. 1 passed; 0 failed; 0 ignored
```

Formatting:

```text
cargo fmt --all -- --check
passed
```

Full storage suite:

```text
cargo test -p iot-storage
26 passed; 0 failed; 13 ignored
```

The SQLite contract uses actual `iot_core` token generation, hashing, prefix
parsing, and verification helpers. It covers direct and gateway identities,
gateway parent identity, malformed, unknown, revoked, deleted-device, and
hash-mismatched denial, plus successful RFC3339 `last_used_at` advancement and
unchanged denial timestamps.

Timescale contract evidence:

```text
test timescale_identity_repository_matches_sqlite_identity_contract ... ignored, requires IOT_NANO_TIMESCALE_TEST_URL for an isolated iot_nano_test_* database
```

`IOT_NANO_TIMESCALE_TEST_URL` was unset and the Timescale container was stopped,
so no live Timescale test was run. The ignored test uses the existing
`iot_nano_test_*` database guard and advisory-lock factory.

## Concerns

- Live Timescale behavior remains unverified until a disposable
  `iot_nano_test_*` URL is available.
- The read and conditional `last_used_at` update are transactional. SQLite
  writes RFC3339 values, and both backends fail closed when the conditional
  update affects anything other than exactly one row. The Timescale SELECT
  locks both `device_tokens` and `devices`.
