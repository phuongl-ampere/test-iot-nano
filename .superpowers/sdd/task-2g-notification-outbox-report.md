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
