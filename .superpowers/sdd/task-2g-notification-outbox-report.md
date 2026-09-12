# Task 2g Notification Outbox Report

## Scope

Implemented only the notification-outbox repository port in `crates/iot-storage/**`.
No Core, API, MQTTD, monolith, migration, deployment, or user-deleted documentation
files were changed.

## Implementation

- Added storage-owned `NotificationKind`, `NotificationOutboxState`, and
  `NotificationOutboxRecord` types.
- Added the backend-neutral `NotificationRepository` trait and corresponding
  `PlatformStore` methods:
  - `claim_notifications`
  - `mark_notification_sent`
  - `release_notification_for_retry`
- Added atomic SQLite claims using one `UPDATE ... RETURNING` statement.
- Added PostgreSQL/Timescale claims using `FOR UPDATE SKIP LOCKED` and matching
  state predicates.
- Claims include pending rows due by `next_attempt_at` and leased rows whose
  `lease_until` has expired.
- Completion and retry transitions require `state = 'leased'`; sent rows cannot
  be retried, and stale lifecycle operations return `None`.
- Attempt counts increment only in the claim transition, once per claim.
- Added guarded ignored Timescale parity coverage. The test requires
  `IOT_NANO_TIMESCALE_TEST_URL`, verifies the database name starts with
  `iot_nano_test_`, obtains an advisory lock, and resets only the isolated
  `iot_nano` schema.

## TDD Evidence

1. Added the focused SQLite contract before production implementation.
2. Confirmed RED with:
   `cargo test -p iot-storage --test notification_outbox`
3. The initial failure was the expected missing public notification symbols.
4. Added the minimal implementation and confirmed GREEN.

## Verification

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo test -p iot-storage --test notification_outbox`: passed, 1 SQLite
  test passed and 1 Timescale test ignored.
- `cargo test -p iot-storage`: passed. SQLite tests passed; Timescale tests
  were ignored because no guarded disposable URL was supplied.

## Concerns

Timescale live parity was not run because Timescale is stopped and no
`IOT_NANO_TIMESCALE_TEST_URL` was supplied. The guarded test remains available
for a later run against a disposable `iot_nano_test_*` database.

## P1 Review Fixes

- `mark_notification_sent` and `release_notification_for_retry` now require
  the `expected_lease_until` returned by `claim_notifications`.
- SQLite and Timescale completion/retry updates now match `id`, `state =
  'leased'`, and `lease_until = expected_lease_until`. A worker holding an
  expired, reclaimed lease cannot finalize or release the current lease.
- SQLite notification timestamp parsing now accepts both RFC3339 values and
  SQLite `CURRENT_TIMESTAMP` values in `YYYY-MM-DD HH:MM:SS` UTC format.
- All destructive Timescale fixtures in `backend_contract.rs`, `identity.rs`,
  `resource_authorization.rs`, and `notification_outbox.rs` now acquire
  `iot_nano:platform-storage-test` before dropping the shared schema.

## P1 TDD Evidence

1. Added SQLite and ignored Timescale stale-lease regressions before changing
   production code. Each test claims a row, lets that lease expire, reclaims
   it, then verifies stale sent/retry calls return `None` while the current
   lease can send the row.
2. Added a SQLite regression that inserts a notification without
   `next_attempt_at`, preserving the table's `CURRENT_TIMESTAMP` default, then
   claims it.
3. Confirmed RED with:
   `cargo test -p iot-storage --test notification_outbox`
4. RED failed because `mark_notification_sent` accepted three arguments and
   `release_notification_for_retry` accepted four, while the regressions
   supplied the required expected lease timestamp.
5. After adding the lease guard and timestamp parser, the focused contract
   passed: 3 SQLite tests passed and the guarded Timescale test was ignored.

## P1 Verification

- `cargo test -p iot-storage`: passed.
- The new SQLite regressions passed for stale lease finalization and default
  SQLite timestamps.
- The ignored Timescale regression compiled but was not executed because no
  guarded disposable URL was supplied.
- Lock inventory confirmed exactly four destructive fixture helpers, all using
  `iot_nano:platform-storage-test`.
