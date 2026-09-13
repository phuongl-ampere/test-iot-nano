# Task 5d Platform Telemetry Writer Adapter

## Status

Implemented `PlatformTelemetryWriter<S>` in the Core writer module and exported
it from the Core crate. Legacy `TelemetryWriter`, `SqliteTelemetryWriter`, and
`main.rs` were left unchanged.

## TDD

### RED

Added `services/iot-nano-core/tests/platform_writer.rs` first and ran:

```text
CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo test -p iot-nano-core --test platform_writer -- --test-threads=1
```

After correcting test-only harness issues, the test failed because
`PlatformTelemetryWriter` was not exported from `iot_nano_core`.

### GREEN

Implemented the adapter and reran the focused suite. The first green attempt
exposed that receipt-only gateway messages must not affect insertion counts,
even if a fake repository reports `telemetry_inserted`; the adapter was
corrected and the suite was rerun successfully.

## Coverage

- Direct and gateway repository failures leave claimed records reclaimable.
- A later failure leaves the entire batch unacknowledged.
- Direct duplicate and gateway telemetry duplicate accounting are distinct.
- Gateway DTO fields and event-kind mapping are asserted.
- Receipt-only gateway messages count neither inserted nor duplicate.
- SQLite `PlatformStore` integration covers registered direct device, active
  gateway/child topology, child telemetry receipt, runtime state, and metric
  rollup/aggregate behavior.

## Verification

- Focused tests: 7 passed, 0 failed.
- `cargo check -p iot-nano-core`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.

## Concerns

No remaining task-specific concern. The adapter contains no SQL or
backend-specific storage coupling, and platform repository failures retain
their typed `PlatformStoreError` through `WriterError::Platform`.

## Review Fixes

### RED

Added the review-requested regression coverage before changing production code:

- direct storage errors must surface as `WriterError::Platform`;
- a later direct failure must leave the entire batch reclaimable;
- Connect, Disconnect, Heartbeat, and ChildTelemetry must each map to their
  corresponding `GatewayIngestEventKind` request;
- a direct duplicate and a successful gateway telemetry insert must be counted
  independently.

The focused test run failed as expected because `WriterError::Platform` did
not exist.

### GREEN

Added `WriterError::Platform(PlatformStoreError)`, mapped platform repository
errors directly to it, and added platform-storage database-failure metric and
logging branches to both existing writer runners. The focused suite then passed
7 tests.

The earlier concern is resolved: platform repository failures are no longer
wrapped in `StreamError::InvalidConfig`.

### Review-Fix Verification

- Focused `platform_writer` tests: 7 passed, 0 failed.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-core cargo check -p iot-nano-core`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
