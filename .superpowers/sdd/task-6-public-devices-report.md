# Task 6 Public Device List Report

Date: 2026-09-13

## Scope Delivered

- Added public-only GET /api/v1/devices for both ApiState and SQLite router paths.
- Requires a valid OAuth Bearer access token with the exact devices:read scope.
- Rejects missing, legacy Session, invalid, and wrong-scope credentials with the public JSON error envelope.
- Resolves the token subject into the existing AuthorizationRepository ownership/share model before returning each device.
- Uses opaque base64url keyset cursors, a default limit of 50, and a maximum limit of 100.
- Returns items, next_cursor, and has_more.
- Kept the route out of the management router.

## TDD Evidence

RED was observed before production changes.

    cargo test -p iot-nano-api --test public_devices
    5 failed: each expected public-route behavior received 404 because /api/v1/devices was not mounted.

The same test target passed after the implementation.

    5 passed; 0 failed

## Verification

    cargo test -p iot-nano-api --test public_devices --test oauth --test router_split --test public_contract
    37 passed; 0 failed

    cargo check -p iot-nano-api
    passed (existing dead-code warnings only)

    cargo fmt --all --check
    passed

    git diff --check
    passed

The sqlite_auth target has seven unrelated PowerMonitor route failures (19 passed, 7 failed). A representative failure, sqlite_router_lists_power_monitor_devices_from_sqlite_telemetry, fails identically at untouched base commit 0b65f16, so it predates this slice.

## Files Changed

- services/iot-nano-api/src/routes.rs
- services/iot-nano-api/tests/public_devices.rs
- .superpowers/sdd/task-6-public-devices-report.md

Cargo.lock was not changed.
