# Task 5e2 Report: CoreRuntime Outbox Workers

## Status

Implemented. `CoreRuntime` now starts injected platform command and
notification workers alongside the existing stream and window workers.
Startup readiness waits for five worker barriers, and drain/join cancellation,
deadline abort/await, and first-error propagation cover all five handles.

Stream shutdown behavior is preserved: `stop_claiming` only gates the writer
and event-alert stream consumers. Command and notification workers stop from
runtime cancellation after stream drain.

## Changes

- Added typed command transport and email sender configuration, batch sizes,
  intervals, notification timeout, and delivery policy to `CoreRuntimeConfig`.
- Added the `Arc<T>` forwarding implementation for `EmailSender`.
- Mapped dispatcher/repository failures to database worker failures.
- Kept command unavailable/no-session and notification send failures in their
  durable retry state transitions.
- Recorded notification retry counts through `IngestMetrics`.
- Added runtime tests for five-worker readiness, durable command retry,
  durable notification retry, runtime liveness, and lifecycle cleanup.

## Verification

- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo test -p iot-nano-core --test runtime -- --test-threads=1`: passed, 18 tests.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo test -p iot-nano-core --test command -- --test-threads=1`: passed, 17 tests.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo test -p iot-nano-core --test notification -- --test-threads=1`: 13 passed, 3 failed because `DATABASE_URL` is unset for existing Timescale integration tests.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo check -p iot-nano-core`: passed.
- `cargo fmt --all -- --check`: passed after formatting.
- `git diff --check`: passed.

## Concerns

The three failing notification tests require the local Timescale test
database and were not runnable in this environment without `DATABASE_URL`.
SQLite and platform notification dispatcher tests passed.

## Review Fix Evidence

- `CoreRuntime` tracks received startup barriers and `ready()` now requires
  exactly five before reporting readiness. The runtime test asserts the count.
- Command retry coverage now proves the queued state, nonempty durable error,
  and a retry schedule later than the command's original due time.
- Notification retry coverage now proves the pending state, nonempty durable
  error, and a retry schedule later than the notification's original due time.
- Command and notification repository-failure tests drop their respective
  temporary SQLite outbox table after startup. Each proves `join()` returns
  the named worker failure and increments the database-failure metric.

## Review Verification

- Runtime target: 20 passed.
- Command target against a disposable local Timescale database: 17 passed.
- Notification target against the same disposable local Timescale database:
  16 passed.
- `cargo check -p iot-nano-core`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

The previous `DATABASE_URL` limitation is resolved for this review pass; the
disposable database was removed after verification.
