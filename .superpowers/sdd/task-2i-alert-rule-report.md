# Task 2i Report

Status: implemented and committed.

Commit: `7c4f364f56b35fee3041e8bbd56e4dd06df525ee`

Tests:

- `cargo test -p iot-storage --test alert_rule`: 3 passed, 1 ignored.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- The broader `cargo test -p iot-storage` run passed through the backend
  contract target, including 10 passed and 13 ignored tests, but was
  interrupted during the later integration targets at the user's request.

Concerns:

- Timescale parity was not executed because `IOT_NANO_TIMESCALE_TEST_URL` was
  not supplied. The parity test is guarded and ignored.
- Existing unrelated documentation deletions remain untouched and unstaged.

## Review P1: Event Timestamp Precision

Status: fixed.

Commit: `f7e9b6f22f8842f66ab06f4d7d0bf1e05677d135`

Cause: alert-rule event claims used the caller's nanosecond timestamp directly.
SQLite therefore treated timestamps that PostgreSQL stores as one microsecond
value as distinct composite keys.

Change: `claim_alert_rule_event` now canonicalizes `event_at` to PostgreSQL
microsecond precision before both SQLite and Timescale inserts. The existing
shared command timestamp helper was renamed to `canonical_postgres_timestamp`
and reused. The guarded Timescale parity contract now checks the same
sub-microsecond claim behavior.

Evidence:

- RED: `cargo test -p iot-storage --test alert_rule
  sqlite_alert_rule_event_claim_canonicalizes_submicrosecond_timestamps`
  failed because the second claim returned `true`.
- GREEN: the same command passed after the fix.
- `cargo test -p iot-storage --test alert_rule`: 4 passed, 1 ignored.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
