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
