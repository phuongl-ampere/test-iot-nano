#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_path="/etc/rush-iot-nano/iot-nano-mqttd.toml"
state_path="/var/lib/iot-nano-mqttd"
service_path="/etc/systemd/system/iot-nano-mqttd-standalone.service"

cd "$root"
cargo build --release --package iot-nano-mqttd
sudo install --owner root --group root --mode 0755 \
  "$root/target/release/iot-nano-mqttd" /opt/rush-iot-nano/iot-nano-mqttd
sudo install --mode 0644 \
  "$root/infra/systemd/iot-nano-mqttd-standalone.service" "$service_path"
sudo install -d --owner iot --group iot --mode 0700 "$state_path"
sudo install -d --owner root --group iot --mode 0750 /etc/rush-iot-nano
if [ ! -e "$config_path" ]; then
  sudo install --owner root --group iot --mode 0600 \
    "$root/services/iot-nano-mqttd/config/standalone.toml" "$config_path"
fi
sudo systemctl daemon-reload

printf 'Edit %s to replace credentials and set TLS paths, then enable iot-nano-mqttd-standalone.service.\n' \
  "$config_path"
