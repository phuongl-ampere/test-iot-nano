#!/usr/bin/env bash
set -euo pipefail

if ! id --user iot >/dev/null 2>&1; then
  sudo useradd --system --home-dir /var/lib/iot-nano-core --shell /usr/sbin/nologin iot
fi
sudo install --directory --owner iot --group iot --mode 0700 /var/lib/iot-nano-core
sudo install --directory --owner iot --group iot --mode 0700 /var/lib/iot-nano-stream
sudo install --directory --owner root --group iot --mode 0750 /etc/rush-iot-nano
sudo install --directory --owner root --group iot --mode 0750 /etc/rush-iot-nano/tls
sudo install --directory --owner root --group root --mode 0755 /opt/rush-iot-nano
sudo install --mode 0644 infra/systemd/iot-nano-core.service /etc/systemd/system/iot-nano-core.service
sudo install --mode 0644 infra/systemd/iot-nano-api.service /etc/systemd/system/iot-nano-api.service
sudo install --mode 0644 infra/systemd/iot-nano-stream.service /etc/systemd/system/iot-nano-stream.service
sudo install --mode 0644 infra/systemd/iot-nano-mqttd.service /etc/systemd/system/iot-nano-mqttd.service
sudo install --mode 0644 infra/systemd/iot-nano-mqttd-standalone.service /etc/systemd/system/iot-nano-mqttd-standalone.service
cargo build --release \
  --package iot-admin-helper \
  --package iot-nano-api \
  --package iot-nano-core \
  --package iot-nano-stream \
  --package iot-nano-mqttd
sudo install --owner root --group root --mode 0755 \
  target/release/iot-nano-api /opt/rush-iot-nano/iot-nano-api
sudo install --owner root --group root --mode 0755 \
  target/release/iot-nano-core /opt/rush-iot-nano/iot-nano-core
sudo install --owner root --group root --mode 0755 \
  target/release/iot-nano-stream /opt/rush-iot-nano/iot-nano-stream
sudo install --owner root --group root --mode 0755 \
  target/release/iot-nano-mqttd /opt/rush-iot-nano/iot-nano-mqttd
sudo install --owner root --group root --mode 0700 \
  target/release/iot-admin-helper /usr/local/sbin/iot-admin-helper
sudo install --owner root --group root --mode 0440 \
  infra/systemd/rush-iot-nano-api.sudoers /etc/sudoers.d/rush-iot-nano-api
sudo visudo --check --file /etc/sudoers.d/rush-iot-nano-api
sudo systemctl daemon-reload

printf 'Create /etc/rush-iot-nano/{iot-nano-api,iot-nano-core,iot-nano-stream,iot-nano-mqttd}.env and install TLS material before enabling services. See docs/operations.md.\n'
