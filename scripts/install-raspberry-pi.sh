#!/usr/bin/env bash
set -euo pipefail

version="0.25.6"
asset="nanomq-${version}-linux-arm64-full.deb"
release_url="https://github.com/nanomq/nanomq/releases/download/${version}/${asset}"
workdir="$(mktemp -d)"

curl --fail --location --output "$workdir/$asset" "$release_url"
curl --fail --location --output "$workdir/$asset.sha256" "$release_url.sha256"
(
  cd "$workdir"
  sha256sum --check "$asset.sha256"
)

sudo dpkg --install "$workdir/$asset"
if ! id --user nanomq >/dev/null 2>&1; then
  sudo useradd --system --home-dir /var/lib/nanomq --shell /usr/sbin/nologin nanomq
fi
if ! id --user iot >/dev/null 2>&1; then
  sudo useradd --system --home-dir /var/lib/iot-ingest --shell /usr/sbin/nologin iot
fi
sudo install --directory --owner nanomq --group nanomq /var/lib/nanomq
sudo install --directory --owner iot --group iot --mode 0700 /var/lib/iot-ingest
sudo install --directory /etc/rush-iot-nano
sudo install --mode 0644 infra/nanomq/nanomq.conf /etc/nanomq/nanomq.conf
sudo install --mode 0644 infra/nanomq/nanomq.service /etc/systemd/system/nanomq.service
sudo install --mode 0644 infra/systemd/iot-ingest.service /etc/systemd/system/iot-ingest.service
sudo install --mode 0644 infra/systemd/iot-api.service /etc/systemd/system/iot-api.service
cargo build --release --package iot-admin-helper
sudo install --owner root --group root --mode 0700 \
  target/release/iot-admin-helper /usr/local/sbin/iot-admin-helper
sudo install --owner root --group root --mode 0440 \
  infra/systemd/rush-iot-nano-api.sudoers /etc/sudoers.d/rush-iot-nano-api
sudo visudo --check --file /etc/sudoers.d/rush-iot-nano-api
sudo systemctl daemon-reload

printf 'Set NanoMQ HTTP auth and webhook secrets plus TLS certificate paths in /etc/nanomq/nanomq.conf, then enable services.\n'
