#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
test_target="$root/services/iot-nano-monolith/tests/e2e_timescale.rs"

cargo test --manifest-path "$root/Cargo.toml" \
  --package iot-nano-monolith \
  --test e2e_sqlite \
  -- --test-threads=1 "$@"

if [[ -n "${IOT_NANO_TIMESCALE_TEST_URL:-}" ]]; then
  if [[ ! -f "$test_target" ]]; then
    printf 'IOT_NANO_TIMESCALE_TEST_URL is set but %s is missing\n' "$test_target" >&2
    exit 1
  fi

  IOT_NANO_TIMESCALE_TEST_URL="$IOT_NANO_TIMESCALE_TEST_URL" \
    cargo test --manifest-path "$root/Cargo.toml" \
      --package iot-nano-monolith \
      --test e2e_timescale \
      -- --ignored --test-threads=1 "$@"
fi
