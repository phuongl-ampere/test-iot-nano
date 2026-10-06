#!/bin/sh
set -eu

state_root="/var/lib/iot-nano"
tls_root="$state_root/tls"
vault_file="$state_root/vault.key"
cert_file="$tls_root/mqtt-cert.pem"
key_file="$tls_root/mqtt-key.pem"

mkdir -p "$state_root/platform" "$tls_root"

if [ ! -s "$vault_file" ]; then
  umask 077
  openssl rand -base64 48 | tr -d '\n' >"$vault_file"
fi

if [ ! -s "$cert_file" ] || [ ! -s "$key_file" ]; then
  umask 077
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -subj "/CN=iot-nano" -keyout "$key_file" -out "$cert_file"
  chmod 0600 "$key_file"
fi

export IOT_NANO_STORAGE="${IOT_NANO_STORAGE:-sqlite}"
export IOT_NANO_SQLITE_PATH="${IOT_NANO_SQLITE_PATH:-$state_root/platform/platform.sqlite}"
export IOT_NANO_INTERNAL_DIR="${IOT_NANO_INTERNAL_DIR:-$state_root/internal}"
export IOT_NANO_HTTP_ADDRESS="${IOT_NANO_HTTP_ADDRESS:-0.0.0.0:18080}"
export IOT_NANO_MQTT_TCP_ADDRESS="${IOT_NANO_MQTT_TCP_ADDRESS:-0.0.0.0:1883}"
export IOT_NANO_MQTT_TLS_ADDRESS="${IOT_NANO_MQTT_TLS_ADDRESS:-0.0.0.0:8883}"
export IOT_NANO_TLS_CERT_PATH="${IOT_NANO_TLS_CERT_PATH:-$cert_file}"
export IOT_NANO_TLS_KEY_PATH="${IOT_NANO_TLS_KEY_PATH:-$key_file}"
export IOT_NANO_HTTPS_ENABLED="${IOT_NANO_HTTPS_ENABLED:-false}"
export IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS="${IOT_NANO_ALLOW_INSECURE_DEFAULT_PASSWORDS:-false}"
export IOT_DEVICE_TOKEN_VAULT_KEY="$(tr -d '\r\n' <"$vault_file")"

exec /usr/local/bin/iot-nano-monolith
