# Task 2i Report

Status: implemented and committed.

Commit: `7c4f364f56b35fee3041e8bbd56e4dd06df525ee`

Tests:

- `cargo test -p iot-storage --test alert_rule`: 3 passed, 1 ignored.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- The broader `cargo test -p iot-storage` run passed through the backend
  contract target, including 10 passed and 13 ignored tests, but was
  interrupted during the later integration targets at the user's request.

Concerns:

- Timescale parity was not executed because `IOT_NANO_TIMESCALE_TEST_URL` was
  not supplied. The parity test is guarded and ignored.
- Existing unrelated documentation deletions remain untouched and unstaged.
