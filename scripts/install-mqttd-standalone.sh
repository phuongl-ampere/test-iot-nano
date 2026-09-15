#!/usr/bin/env bash
set -euo pipefail

root="${IOT_NANO_MQTTD_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
config_dir="${IOT_NANO_MQTTD_CONFIG_DIR:-/etc/rush-iot-nano}"
config_path="${IOT_NANO_MQTTD_CONFIG_PATH:-$config_dir/iot-nano-mqttd.toml}"
state_path="${IOT_NANO_MQTTD_STATE_PATH:-/var/lib/iot-nano-mqttd}"
service_path="${IOT_NANO_MQTTD_SERVICE_PATH:-/etc/systemd/system/iot-nano-mqttd-standalone.service}"
install_path="${IOT_NANO_MQTTD_INSTALL_PATH:-/opt/rush-iot-nano}"
config_template_path="${IOT_NANO_MQTTD_CONFIG_TEMPLATE_PATH:-$root/services/iot-nano-mqttd/config/standalone.toml}"
binary_source_path="${IOT_NANO_MQTTD_BINARY_SOURCE_PATH:-$root/target/release/iot-nano-mqttd}"
service_source_path="${IOT_NANO_MQTTD_SERVICE_SOURCE_PATH:-$root/infra/systemd/iot-nano-mqttd-standalone.service}"

sudo_bin="${IOT_NANO_MQTTD_SUDO_BIN:-sudo}"
cargo_bin="${IOT_NANO_MQTTD_CARGO_BIN:-cargo}"
getent_bin="${IOT_NANO_MQTTD_GETENT_BIN:-getent}"
id_bin="${IOT_NANO_MQTTD_ID_BIN:-id}"
groupadd_bin="${IOT_NANO_MQTTD_GROUPADD_BIN:-groupadd}"
useradd_bin="${IOT_NANO_MQTTD_USERADD_BIN:-useradd}"
install_bin="${IOT_NANO_MQTTD_INSTALL_BIN:-install}"
chown_bin="${IOT_NANO_MQTTD_CHOWN_BIN:-chown}"
chmod_bin="${IOT_NANO_MQTTD_CHMOD_BIN:-chmod}"
systemctl_bin="${IOT_NANO_MQTTD_SYSTEMCTL_BIN:-systemctl}"

install_config() {
  if [ ! -e "$config_path" ]; then
    "$sudo_bin" "$install_bin" --owner root --group iot --mode 0640 \
      "$config_template_path" "$config_path"
  fi
  "$sudo_bin" "$chown_bin" root:iot "$config_path"
  "$sudo_bin" "$chmod_bin" 0640 "$config_path"
}

require_sudo() {
  if ! "$sudo_bin" -v; then
    printf 'sudo privilege is required to install iot-nano-mqttd-standalone\n' >&2
    return 1
  fi
}

build_mqttd() {
  (
    cd "$root"
    "$cargo_bin" build --release --package iot-nano-mqttd
  )
}

ensure_iot_account() {
  if ! "$getent_bin" group iot >/dev/null; then
    "$sudo_bin" "$groupadd_bin" --system iot
  fi
  if ! "$id_bin" --user iot >/dev/null 2>&1; then
    "$sudo_bin" "$useradd_bin" --system --gid iot --home-dir "$state_path" \
      --shell /usr/sbin/nologin iot
  fi
}

install_mqttd_standalone() {
  require_sudo
  build_mqttd
  ensure_iot_account

  "$sudo_bin" "$install_bin" -d --owner root --group root --mode 0755 "$install_path"
  "$sudo_bin" "$install_bin" -d --owner iot --group iot --mode 0700 "$state_path"
  "$sudo_bin" "$install_bin" -d --owner root --group iot --mode 0750 "$config_dir"
  "$sudo_bin" "$install_bin" --owner root --group root --mode 0755 \
    "$binary_source_path" "$install_path/iot-nano-mqttd"
  "$sudo_bin" "$install_bin" --mode 0644 "$service_source_path" "$service_path"
  install_config
  "$sudo_bin" "$systemctl_bin" daemon-reload

  printf 'Edit %s to replace credentials and set TLS paths, then enable iot-nano-mqttd-standalone.service.\n' \
    "$config_path"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  install_mqttd_standalone "$@"
fi
