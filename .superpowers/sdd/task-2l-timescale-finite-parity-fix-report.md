# Task 2l Timescale Finite Aggregate Parity Fix Report

## Status

Implemented and verified with the focused telemetry aggregate changes. The
Timescale adapter now excludes JSON numbers outside the finite `f64` range
without casting them to `double precision`, while finite JSON integers and
real values remain included.

## TDD Evidence

### RED

Command:

```text
IOT_NANO_TIMESCALE_TEST_URL=postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform cargo test -p iot-storage --test telemetry_aggregate timescale_telemetry_aggregate_matches_sqlite_contract -- --ignored --exact --test-threads=1
```

Result: failed as expected before the production change. The test reached
`average_metric` and PostgreSQL returned `22003`:

```text
"-9000...000" is out of range for type double precision
```

The failure occurred at the test's `unwrap()` because the old Timescale query
cast every JSON number directly to `double precision`.

### GREEN

The same exact command passed once after the SQL change:

```text
running 1 test
test timescale_telemetry_aggregate_matches_sqlite_contract ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out
```

The test uses the declared disposable URL, serial test execution, the seven
required fixture rows, inclusive boundary timestamps, `unwrap().unwrap()`,
and direct assertions of average `-0.625` and sample count `2`.

## Stalled Default-Target Run

A later rerun left parent PID `11272` (`cargo test`) sleeping for over two
minutes with child PID `12046` (the test binary). The child had no database
socket or other open handles beyond its executable, terminal, and working
directory. `/usr/bin/sample` captured only `_dyld_start`; even running the
existing binary with `--list` produced no harness output. The database probe
after terminating both PIDs showed no advisory locks and no competing client
backend.

The stale parent and child were terminated. This was a default-target
test-binary/dynamic-loader launch problem, not SQL execution or PostgreSQL
lock contention.

## Verification

- `cargo test -p iot-storage --test telemetry_aggregate -- --test-threads=1`: passed, `5 passed; 0 failed; 1 ignored`.
- `IOT_NANO_TIMESCALE_TEST_URL=postgres://iot:iot@127.0.0.1:54329/iot_nano_test_platform cargo test -p iot-storage --test telemetry_aggregate timescale_telemetry_aggregate_matches_sqlite_contract -- --ignored --exact --test-threads=1`: passed once with `1 passed; 0 failed`; a later default-target rerun stalled before test harness output as documented above.
- Isolated bounded reproduction with a fresh `CARGO_TARGET_DIR=/tmp/task-2l-timescale-target-19613`: passed, `1 passed; 0 failed`, test execution `0.39s`.
- `cargo fmt --all -- --check`: passed.
- `cargo check -p iot-storage`: passed.
- `git diff --check`: passed.

## Scope and Concerns

Only `crates/iot-storage/src/lib.rs` and
`crates/iot-storage/tests/telemetry_aggregate.rs` contain the focused code
change. Existing unrelated worktree changes were left untouched. The default
Cargo target retains a reproducible pre-harness loader stall after the
interrupted run; the fresh isolated target is the reliable live-test evidence.

## Commit

The focused commit is recorded after this report and final staged-file check.
