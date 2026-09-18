#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
lane="$root/scripts/dev/cargo-lane.sh"

"$lane" release -- test --workspace --no-run
"$lane" release -- test --package iot-nano-monolith --test e2e_sqlite -- --test-threads=1

if [[ -n "${IOT_NANO_TIMESCALE_TEST_URL:-}" ]]; then
  "$lane" timescale -- test --package iot-nano-monolith --test e2e_timescale -- \
    --ignored --test-threads=1
else
  printf 'Skipping Timescale release check: set IOT_NANO_TIMESCALE_TEST_URL to run it.\n' >&2
fi

"$lane" release -- test --package iot-nano-monolith --test external_app_contract -- \
  --test-threads=1
