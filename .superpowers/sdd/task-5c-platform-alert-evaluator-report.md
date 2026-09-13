# Task 5c Platform Alert Evaluator Adapter Report

## Status

Implemented `PlatformAlertEvaluator<S>` over the `iot_storage::AlertEvaluationRepository`
port and exported it from `iot-nano-core`. Legacy `AlertEvaluator` and
`SqliteAlertEvaluator` remain unchanged in behavior, and `main.rs` was not modified.

The adapter claims at most `batch_size.max(1)`, counts every claimed record in
`read`, maps telemetry records through `telemetry_parts`, delegates event and
window evaluation once, and acknowledges only after successful event evaluation.
Storage failures are returned as `AlertError::Store` without acknowledgement.

## TDD Evidence

### RED

Command:

```text
CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo test -p iot-nano-core --test platform_alert -- --test-threads=1
```

The initial run failed because `PlatformAlertEvaluator` and `AlertError::Store`
were not implemented. This established that the new tests exercised missing
production behavior.

### GREEN

The same command passed after the minimal implementation:

```text
5 passed; 0 failed
```

Coverage includes fake repository success/failure, non-telemetry `read`
accounting, empty batches, window delegation, acknowledgement ordering, lease
reclaim, and one real SQLite `PlatformStore` incident-opening integration.

## Verification

- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo test -p iot-nano-core --test platform_alert -- --test-threads=1`: passed
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo check -p iot-nano-core`: passed
- `cargo fmt --all -- --check`: passed
- `git diff --check`: passed

## Concerns

The worktree contains pre-existing user changes and deletions outside the task
files. They were left untouched and excluded from the task commit.
