#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
verifier="$root/scripts/verify-no-legacy-runtime.sh"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT

copy_file() {
  local relative_path="$1"
  mkdir -p "$(dirname "$fixture/$relative_path")"
  cp "$root/$relative_path" "$fixture/$relative_path"
}

copy_file infra/compose.yaml
copy_file infra/compose.timescale.yaml
copy_file infra/docker/Dockerfile
cp -R "$root/infra/monolith" "$fixture/infra/monolith"
copy_file infra/systemd/iot-nano-monolith.service
copy_file scripts/e2e-local.sh
copy_file scripts/e2e-monolith.sh
copy_file scripts/install-mqttd-standalone.sh
copy_file scripts/install-raspberry-pi.sh

mkdir -p "$fixture/infra/systemd"
cat >"$fixture/infra/systemd/iot-nano-api.service" <<'EOF'
[Service]
ExecStart=/opt/rush-iot-nano/iot-nano-api
EOF

if output="$(IOT_NANO_VERIFY_ROOT="$fixture" "$verifier" 2>&1)"; then
  printf 'verifier accepted an injected legacy deployment unit\n' >&2
  exit 1
fi

if [[ "$output" != *"legacy runtime references found"* ]] ||
  [[ "$output" != *"iot-nano-api.service"* ]]; then
  printf 'verifier failed for the wrong reason:\n%s\n' "$output" >&2
  exit 1
fi

rm "$fixture/infra/systemd/iot-nano-monolith.service"
if output="$(IOT_NANO_VERIFY_ROOT="$fixture" "$verifier" 2>&1)"; then
  printf 'verifier accepted a fixture missing an expected deployment path\n' >&2
  exit 1
fi

if [[ "$output" != *"expected production/deployment path is missing or unreadable"* ]]; then
  printf 'verifier did not fail closed for a missing expected path:\n%s\n' "$output" >&2
  exit 1
fi

copy_file infra/systemd/iot-nano-monolith.service
chmod 000 "$fixture/infra/systemd/iot-nano-monolith.service"
if output="$(IOT_NANO_VERIFY_ROOT="$fixture" "$verifier" 2>&1)"; then
  printf 'verifier accepted an unreadable expected deployment path\n' >&2
  exit 1
fi

if [[ "$output" != *"expected production/deployment path is missing or unreadable"* ]]; then
  printf 'verifier did not fail closed for an unreadable expected path:\n%s\n' "$output" >&2
  exit 1
fi
