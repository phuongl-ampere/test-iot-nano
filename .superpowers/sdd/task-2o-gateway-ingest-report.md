# Task 2o Gateway Ingest Report

## Delivered

- Added the backend-neutral `GatewayIngestRepository` port, request/result DTOs,
  event kind, and typed `GatewayIngestValidationError`.
- Added `PlatformStore::ingest_gateway` plus its trait implementation.
- SQLite uses one transaction, receipt-first idempotency, active topology checks,
  canonical SQLite telemetry composition, child runtime state updates, and SQLite
  rollups through the existing helper.
- Timescale uses one transaction, receipt-first idempotency, `FOR KEY SHARE`
  topology checks, monotonic runtime state updates, and canonical raw telemetry
  insertion without an additional rollup implementation.
- New invalid topology or telemetry-identity requests roll back their receipt and
  return typed errors. Duplicate receipts commit as no-ops.

## TDD Evidence

1. RED: `gateway_ingest` initially failed to compile because the gateway-ingest
   port symbols did not exist.
2. GREEN: SQLite and guarded Timescale connect receipt/state contracts passed.
3. RED/GREEN: unknown gateway and child topology cases established fail-closed
   rollback with `UnknownDevice`.
4. RED/GREEN: disconnect established child `unavailable` state.
5. RED/GREEN: child telemetry established canonical raw insertion, child `good`
   state, and SQLite rollups.
6. RED/GREEN: telemetry child/gateway identity mismatches established
   `InvalidGatewayIngest(GatewayIngestValidationError::...)` with no receipt or
   raw telemetry side effect.

## Final Verification

- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-storage cargo test -p iot-storage --test gateway_ingest -- --test-threads=1`
  - Passed: 1 SQLite test; 1 Timescale test ignored as intended.
- `IOT_NANO_TIMESCALE_TEST_URL=postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-storage cargo test -p iot-storage --test gateway_ingest -- --ignored --test-threads=1`
  - Passed: 1 guarded Timescale test.
- `cargo fmt --all -- --check`
  - Passed.
- `CARGO_TARGET_DIR=/tmp/rush-iot-nano-controller-storage cargo check -p iot-storage`
  - Passed.
- `git diff --check`
  - Passed.

## Concern

The continuation coverage is now present. The Timescale deletion test requires
the declared disposable test database and remains ignored by default.

## Continuation TDD Evidence

7. RED: the first shared raw-duplicate/overflow run exposed test assumptions
   that counted telemetry across the intentionally shared backend store.
   GREEN: delta-based assertions passed for SQLite.
8. GREEN: raw duplicate with a new gateway receipt returned
   `receipt_inserted=true` and `telemetry_inserted=false`, with unchanged
   SQLite rollups.
9. GREEN: out-of-order gateway and child event timestamps preserved the newer
   stored values on SQLite and Timescale.
10. GREEN: sequence overflow returned `TelemetrySequenceOverflow`, left no
    receipt, and allowed the same idempotency key to retry successfully after
    correction on both backends.
11. RED: the first guarded deletion-race run failed in test setup because the
    standalone PostgreSQL blocker connection lacked the `iot_nano` search path.
    GREEN: after setting the search path, ingest waited behind the uncommitted
    delete; after deletion committed it rejected with `UnknownDevice` and no
    receipt.

## Continuation Verification

- SQLite shared suite: 1 passed, 1 ignored.
- Guarded Timescale suite: 2 passed, 1 filtered shared test.
- No production source change was required by continuation coverage.
