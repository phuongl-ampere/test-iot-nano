# Fresh Linux installation

This guide installs a new, SQLite-backed IoT Nano monolith on a Debian or
Ubuntu host. It creates an empty platform; it does not import data from the
retired multi-service topology.

## 1. Install host prerequisites

Run as an administrator:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl jq openssl
sudo useradd --system --home /var/lib/iot-nano --shell /usr/sbin/nologin iotnano
sudo install -d -o root -g root -m 0755 /opt/rush-iot-nano /etc/iot-nano
sudo install -d -o iotnano -g iotnano -m 0700 \
  /var/lib/iot-nano/platform /var/lib/iot-nano/internal
sudo install -d -o iotnano -g iotnano -m 0700 /run/tls
```

Use a pinned release binary. If building on this host, install the pinned Rust
toolchain plus `build-essential`, `pkg-config`, and `libssl-dev`, then run:

```bash
cargo build --release --package iot-nano-monolith
```

## 2. Install the binary and unit

Copy the reviewed release binary to the fixed path and install the supplied
unit:

```bash
sudo install -o root -g root -m 0755 \
  target/release/iot-nano-monolith /opt/rush-iot-nano/iot-nano-monolith
sudo install -o root -g root -m 0644 \
  infra/systemd/iot-nano-monolith.service /etc/systemd/system/iot-nano-monolith.service
sudo install -o root -g root -m 0600 \
  infra/monolith/monolith.env.example /etc/iot-nano/monolith/monolith.env
```

For a downloaded release, substitute its verified binary for
`target/release/iot-nano-monolith`.

## 3. Create TLS material and vault key

Create a unique vault key and an MQTT TLS certificate. Replace the self-signed
certificate with your production PKI certificate when available.

```bash
sudo sh -c 'umask 077; openssl rand -base64 48 | tr -d "\n" > /etc/iot-nano/vault.key'
sudo openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj '/CN=iot-nano' \
  -keyout /run/tls/mqtt-key.pem \
  -out /run/tls/mqtt-cert.pem
sudo chown iotnano:iotnano /run/tls/mqtt-key.pem /run/tls/mqtt-cert.pem
sudo chmod 0600 /run/tls/mqtt-key.pem
sudo chmod 0644 /run/tls/mqtt-cert.pem
```

Edit `/etc/iot-nano/monolith.env` and set:

```dotenv
IOT_NANO_STORAGE=sqlite
IOT_NANO_SQLITE_PATH=/var/lib/iot-nano/platform/platform.sqlite
IOT_NANO_INTERNAL_DIR=/var/lib/iot-nano/internal
IOT_NANO_TLS_CERT_PATH=/run/tls/mqtt-cert.pem
IOT_NANO_TLS_KEY_PATH=/run/tls/mqtt-key.pem
IOT_NANO_PUBLIC_HTTP_ADDRESS=0.0.0.0:8080
IOT_NANO_MANAGEMENT_ADDRESS=127.0.0.1:8081
IOT_NANO_MQTT_TCP_ADDRESS=0.0.0.0:1883
IOT_NANO_MQTT_TLS_ADDRESS=0.0.0.0:8883
IOT_DEVICE_TOKEN_VAULT_KEY=<contents of /etc/iot-nano/vault.key>
```

Keep port `8081` private. Do not expose it through a public firewall or reverse
proxy without separate operator authentication.

## 4. Validate and migrate

Run validation and migration as the service user. Neither command starts a
listener.

```bash
sudo -u iotnano -H bash -c '
  set -a
  . /etc/iot-nano/monolith.env
  set +a
  /opt/rush-iot-nano/iot-nano-monolith --config-check
  /opt/rush-iot-nano/iot-nano-monolith --migrate-only
'
```

Stop here if either command fails. Do not manually create files in the internal
state directory.

## 5. Bootstrap the System Account

Bootstrap exactly once. Do not add these credentials to the persistent env
file.

```bash
sudo -u iotnano -H env \
  IOT_NANO_BOOTSTRAP_SYSTEM_USERNAME=system-admin \
  IOT_NANO_BOOTSTRAP_SYSTEM_PASSWORD='<use-a-strong-unique-password>' \
  bash -c '
    set -a
    . /etc/iot-nano/monolith.env
    set +a
    /opt/rush-iot-nano/iot-nano-monolith --bootstrap-system
  '
```

Record the password in an approved secret manager, then discard it from the
terminal history according to your host policy.

## 6. Start and verify

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now iot-nano-monolith.service
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
sudo systemctl status iot-nano-monolith.service --no-pager
sudo journalctl -u iot-nano-monolith.service --since '5 minutes ago' --no-pager
```

By default, open the public console directly at
`http://<LAN-IP>:8080`; browser cookies are compatible with HTTP LAN access.
Use SSH port forwarding or an operator-only network path for the management
listener. If you terminate HTTPS at a reverse proxy, set
`IOT_NANO_HTTPS_ENABLED=true` in `/etc/iot-nano/monolith.env` and use matching
HTTPS PowerMonitor URLs.

## 7. Firewall and backup baseline

Expose only the intended public listeners: HTTP/HTTPS at the reverse proxy and
MQTT TCP/TLS if devices require them. Keep `8081` closed externally.

Back up the complete `/var/lib/iot-nano/platform` and
`/var/lib/iot-nano/internal` directories together while the service is stopped,
or use the repository's monolith migration/backup workflow for upgrades. Never
copy a database or internal state directory from the retired four-service
deployment into this installation.
