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

- Focused tests: 5 passed, 0 failed.
- `cargo check -p iot-nano-core`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.

## Concerns

`PlatformStoreError` is adapted through the existing `WriterError::Stream`
variant because adding a new `WriterError` variant would require changing
`main.rs`, which is explicitly outside this task’s allowed scope. The adapter
itself contains no SQL or backend-specific storage coupling.
