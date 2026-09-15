#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
environment_path="/etc/iot-nano/monolith.env"

if ! id --user iotnano >/dev/null 2>&1; then
  sudo useradd --system --user-group --home-dir /var/lib/iot-nano \
    --shell /usr/sbin/nologin iotnano
fi

sudo install --directory --owner iotnano --group iotnano --mode 0700 \
  /var/lib/iot-nano /var/lib/iot-nano/platform /var/lib/iot-nano/internal
sudo install --directory --owner root --group iotnano --mode 0750 /etc/iot-nano
sudo install --directory --owner root --group root --mode 0755 /opt/rush-iot-nano

cargo build --manifest-path "$root/Cargo.toml" --release --package iot-nano-monolith
sudo install --owner root --group root --mode 0755 \
  "$root/target/release/iot-nano-monolith" /opt/rush-iot-nano/iot-nano-monolith
sudo install --mode 0644 "$root/infra/systemd/iot-nano-monolith.service" \
  /etc/systemd/system/iot-nano-monolith.service
if [[ ! -e "$environment_path" ]]; then
  sudo install --owner root --group iotnano --mode 0640 \
    "$root/infra/monolith/monolith.env.example" "$environment_path"
fi
sudo systemctl daemon-reload

printf 'Edit %s with TLS paths and a unique token vault key, then enable iot-nano-monolith.service.\n' \
  "$environment_path"
