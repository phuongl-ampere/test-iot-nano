# Task 5e1 CoreRuntime Lifecycle Review Fixes Report

## Environment

- The shared workspace Cargo target stalled while launching the stream integration
  test binary at `_dyld_start`; source-level sampling confirmed the stall was in
  the macOS loader rather than test code.
- Stopped only the stalled `cargo test -p iot-nano-stream
  stop_claiming_rejects_claims_started_after_the_stop_signal` runner.
- All subsequent focused stream tests use
  `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-stream`; runtime tests use
  `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime`.

## RED/GREEN Evidence

1. Stream stop-claiming boundary
   - RED: `cargo test -p iot-nano-stream
     stop_claiming_rejects_claims_started_after_the_stop_signal` failed with
     `E0599`: `LocalStream` had no `stop_claiming` method.
   - GREEN: `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-stream cargo test
     -p iot-nano-stream --lib
     stop_claiming_rejects_claims_started_after_the_stop_signal` passed.

2. Runtime shared stop boundary and concurrent drain serialization
   - RED: `runtime_stop_claiming_closes_the_shared_stream_boundary` observed
     zero stream boundary stop calls. `concurrent_drains_wait_for_one_stream_drain_before_cancellation_and_join`
     observed the second drain return while the first stream drain remained blocked.
   - GREEN: both focused runtime tests passed with
     `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo test -p
     iot-nano-core --test runtime <test-name>`.

3. Deadline cleanup and result classification
   - RED: `drain_deadline_aborts_and_awaits_blocked_workers_before_returning`
     observed two active blocked workers after `drain` returned. The stream
     timeout regression also returned `CoreRuntimeError::Drain` instead of
     `CoreRuntimeError::Deadline`.
   - GREEN: timed-out handles are aborted and awaited before return, and both
     outer and stream-reported deadlines return `CoreRuntimeError::Deadline`.

4. Writer taxonomy and acknowledgement ordering
   - RED: `writer_error_metrics_preserve_stream_and_platform_taxonomy`
     observed a writer stream failure recorded as a database failure.
   - GREEN: stream writer errors increment only stream metrics; platform
     persistence errors increment only database metrics. The acknowledgement
     regression confirms failed persistence remains reclaimable and a
     successful retry acknowledges exactly once.

5. Window loop and join handling
   - RED: `window_loop_does_not_heartbeat_or_claim_the_alert_stream_consumer`
     observed two alert-member heartbeats. `join_reports_a_panicked_worker_and_cancels_its_siblings`
     timed out because a join error did not cancel siblings.
   - GREEN: the window worker has no stream consumer, and join errors cancel
     siblings before the runtime returns the worker join error.

6. Lifecycle coverage
   - GREEN: startup barrier failure returns a worker error without hanging;
     parent cancellation permits a normal worker join.

## Verification

- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo test -p
  iot-nano-core --test runtime`: 16 passed, 0 failed.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-stream cargo test -p
  iot-nano-stream --lib`: 2 passed, 0 failed, including the stop-claiming
  regression.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo check -p
  iot-nano-core`: passed.

## Residual Concern

- The full isolated `iot-nano-stream` package suite has three existing
  `consumer_group` failures: `acknowledge_cannot_commit_beyond_the_active_claim`,
  `consumer_groups_keep_independent_durable_offsets`, and
  `expired_member_lease_reassigns_from_the_durable_offset`. They reproduce
  individually and exercise claim/acknowledgement semantics untouched by this
  task's stream-gate diff.

## Task 5e1 Runtime Test Strengthening

1. Stop-aware runtime fake
   - RED: the strengthened runtime test observed the manually initiated
     post-stop fake claim being accepted and counted as an ordinary claim.
   - GREEN: `RecordingStream` now rejects claims initiated after its
     `stop_claiming` boundary with `StreamError::Draining`; the runtime test
     also confirms no additional worker claims occur during drain.

2. Real LocalStream lease reclaim and acknowledgement
   - RED: the first real-stream fixture initially failed LocalStream telemetry
     validation because its topic and measurements were not valid.
   - GREEN: a one-partition `LocalStream` with a 30 ms lease accepts one
     telemetry record; the first `PlatformTelemetryWriter` persistence fails
     for an unregistered device and records zero successful acknowledgements.
     After 60 ms, a second consumer member reclaims the unacknowledged record
     after registering the device, persists it, and produces exactly one
     successful acknowledgement. A follow-up claim returns no record.

## Task 5e1 Verification

- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo test -p
  iot-nano-core --test runtime`: 16 passed, 0 failed.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-stream cargo test -p
  iot-nano-stream --no-fail-fast`: 23 passed, 0 failed across unit,
  consumer-group, SQLite recovery, and doc-test targets.
- The earlier residual consumer-group failures recorded above did not
  reproduce in this isolated run.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-runtime cargo check -p
  iot-nano-core`: passed.
- `cargo fmt --all -- --check`: passed after formatting.
- `git diff --check`: passed.
