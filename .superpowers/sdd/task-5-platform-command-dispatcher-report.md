# Task 5a Platform Command Dispatcher Report

## Scope

Added `PlatformCommandDispatcher<C>` in `iot-nano-core`. It consumes
`Arc<PlatformStore>` and a typed `CommandTransport`, while retaining the
legacy SQLite, PostgreSQL, and HTTP dispatchers.

The new path uses `CommandLifecycleRepository` exclusively for expiration,
claiming, publish completion, terminal failure, and retry release. It uses a
30-second lease, retries `Unavailable` and `NoActiveSession`, and treats
configuration, rejected, and invalid command failures as terminal. It
re-checks expiry after transport completion and preserves command identity,
mode, timestamps, method, and params.

`iot-storage` was added as a direct dependency in
`services/iot-nano-core/Cargo.toml`. This manifest change is necessary because
the brief's allowed-file list omitted the dependency required to reference
`PlatformStore` and `CommandLifecycleRepository` from Rust.

## TDD Evidence

RED:

```text
cargo test -p iot-nano-core --test command platform_dispatcher_claims_and_publishes_through_the_platform_store -- --test-threads=1
error[E0432]: unresolved import `iot_nano_core::PlatformCommandDispatcher`
```

GREEN:

```text
running 1 test
test platform_dispatcher_claims_and_publishes_through_the_platform_store ... ok
test result: ok. 1 passed; 0 failed
```

Additional focused PlatformStore tests passed:

- unavailable transport is released for retry
- configuration failure is marked terminal
- transport success after expiry is not marked published

## Verification

- `cargo fmt --all -- --check`: blocked by a pre-existing formatting difference in `crates/iot-storage/tests/alert_incident.rs`, outside task scope. Task-owned files pass `rustfmt --edition 2024`.
- `cargo check -p iot-nano-core`: run in an isolated target; no diagnostics observed while the workspace's concurrent Cargo checks were active.
- `git diff --check`: passed.
- The exact full command-test invocation was started, but the test binary did not return while other concurrent workspace Cargo jobs were active. The focused PlatformStore tests and individual legacy tests completed successfully.

## Concerns

The workspace was already dirty with unrelated edits, deletions, and concurrent Cargo processes. Those changes were not reverted or included in the task commit. The workspace-wide formatter failure is pre-existing and should be resolved separately.
