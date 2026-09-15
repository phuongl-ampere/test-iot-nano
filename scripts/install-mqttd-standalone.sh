#!/usr/bin/env bash
set -euo pipefail

installer_configured=0

configure_mqttd_installer_for_production() {
  root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  config_dir="/etc/rush-iot-nano"
  config_path="$config_dir/iot-nano-mqttd.toml"
  state_path="/var/lib/iot-nano-mqttd"
  service_path="/etc/systemd/system/iot-nano-mqttd-standalone.service"
  install_path="/opt/rush-iot-nano"
  config_template_path="$root/services/iot-nano-mqttd/config/standalone.toml"
  binary_source_path="$root/target/release/iot-nano-mqttd"
  service_source_path="$root/infra/systemd/iot-nano-mqttd-standalone.service"

  sudo_bin="sudo"
  cargo_bin="cargo"
  getent_bin="getent"
  id_bin="id"
  groupadd_bin="groupadd"
  useradd_bin="useradd"
  install_bin="install"
  chown_bin="chown"
  chmod_bin="chmod"
  systemctl_bin="systemctl"
  installer_configured=1
}

configure_mqttd_installer_for_test() {
  root="${IOT_NANO_MQTTD_ROOT:?IOT_NANO_MQTTD_ROOT is required for installer tests}"
  config_dir="${IOT_NANO_MQTTD_CONFIG_DIR:?IOT_NANO_MQTTD_CONFIG_DIR is required for installer tests}"
  config_path="${IOT_NANO_MQTTD_CONFIG_PATH:?IOT_NANO_MQTTD_CONFIG_PATH is required for installer tests}"
  state_path="${IOT_NANO_MQTTD_STATE_PATH:?IOT_NANO_MQTTD_STATE_PATH is required for installer tests}"
  service_path="${IOT_NANO_MQTTD_SERVICE_PATH:?IOT_NANO_MQTTD_SERVICE_PATH is required for installer tests}"
  install_path="${IOT_NANO_MQTTD_INSTALL_PATH:?IOT_NANO_MQTTD_INSTALL_PATH is required for installer tests}"
  config_template_path="${IOT_NANO_MQTTD_CONFIG_TEMPLATE_PATH:-$root/services/iot-nano-mqttd/config/standalone.toml}"
  binary_source_path="${IOT_NANO_MQTTD_BINARY_SOURCE_PATH:-$root/target/release/iot-nano-mqttd}"
  service_source_path="${IOT_NANO_MQTTD_SERVICE_SOURCE_PATH:-$root/infra/systemd/iot-nano-mqttd-standalone.service}"

  sudo_bin="${IOT_NANO_MQTTD_SUDO_BIN:?IOT_NANO_MQTTD_SUDO_BIN is required for installer tests}"
  cargo_bin="${IOT_NANO_MQTTD_CARGO_BIN:?IOT_NANO_MQTTD_CARGO_BIN is required for installer tests}"
  getent_bin="${IOT_NANO_MQTTD_GETENT_BIN:?IOT_NANO_MQTTD_GETENT_BIN is required for installer tests}"
  id_bin="${IOT_NANO_MQTTD_ID_BIN:?IOT_NANO_MQTTD_ID_BIN is required for installer tests}"
  groupadd_bin="${IOT_NANO_MQTTD_GROUPADD_BIN:?IOT_NANO_MQTTD_GROUPADD_BIN is required for installer tests}"
  useradd_bin="${IOT_NANO_MQTTD_USERADD_BIN:?IOT_NANO_MQTTD_USERADD_BIN is required for installer tests}"
  install_bin="${IOT_NANO_MQTTD_INSTALL_BIN:?IOT_NANO_MQTTD_INSTALL_BIN is required for installer tests}"
  chown_bin="${IOT_NANO_MQTTD_CHOWN_BIN:?IOT_NANO_MQTTD_CHOWN_BIN is required for installer tests}"
  chmod_bin="${IOT_NANO_MQTTD_CHMOD_BIN:?IOT_NANO_MQTTD_CHMOD_BIN is required for installer tests}"
  systemctl_bin="${IOT_NANO_MQTTD_SYSTEMCTL_BIN:?IOT_NANO_MQTTD_SYSTEMCTL_BIN is required for installer tests}"
  installer_configured=1
}

require_installer_configuration() {
  if [[ "$installer_configured" != "1" ]]; then
    printf 'installer configuration is required before installation\n' >&2
    return 1
  fi
}

reject_test_overrides() {
  local variable

  while IFS= read -r variable; do
    printf 'IOT_NANO_MQTTD_* overrides are only supported by sourced installer tests\n' >&2
    return 1
  done < <(compgen -A variable IOT_NANO_MQTTD_)
}

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
  require_installer_configuration
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
  reject_test_overrides
  configure_mqttd_installer_for_production
  install_mqttd_standalone "$@"
fi
