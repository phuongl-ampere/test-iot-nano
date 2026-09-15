#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
config_path="/etc/rush-iot-nano/iot-nano-mqttd.toml"
state_path="/var/lib/iot-nano-mqttd"
service_path="/etc/systemd/system/iot-nano-mqttd-standalone.service"
install_path="/opt/rush-iot-nano"

if ! sudo -v; then
  printf 'sudo privilege is required to install iot-nano-mqttd-standalone\n' >&2
  exit 1
fi

cd "$root"
cargo build --release --package iot-nano-mqttd

if ! getent group iot >/dev/null; then
  sudo groupadd --system iot
fi
if ! id --user iot >/dev/null 2>&1; then
  sudo useradd --system --gid iot --home-dir "$state_path" \
    --shell /usr/sbin/nologin iot
fi

sudo install -d --owner root --group root --mode 0755 "$install_path"
sudo install -d --owner iot --group iot --mode 0700 "$state_path"
sudo install -d --owner root --group iot --mode 0750 /etc/rush-iot-nano
sudo install --owner root --group root --mode 0755 \
  "$root/target/release/iot-nano-mqttd" "$install_path/iot-nano-mqttd"
sudo install --mode 0644 \
  "$root/infra/systemd/iot-nano-mqttd-standalone.service" "$service_path"
if [ ! -e "$config_path" ]; then
  sudo install --owner root --group iot --mode 0640 \
    "$root/services/iot-nano-mqttd/config/standalone.toml" "$config_path"
fi
sudo chown root:iot "$config_path"
sudo chmod 0640 "$config_path"
sudo systemctl daemon-reload

printf 'Edit %s to replace credentials and set TLS paths, then enable iot-nano-mqttd-standalone.service.\n' \
  "$config_path"
