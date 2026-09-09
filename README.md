# Rush IoT Nano

Rust and Next.js IoT telemetry platform for ESP32 devices:

```text
ESP32 -> iot-mqtt-transport (TLS MQTT session router) -> iot-ingest -> iot-stream -> writer consumer group -> TimescaleDB or SQLite -> Rust API -> Next.js
                                  |
                                  +-> iot-api token/session resolution
```

`iot-ingest` also runs an independent `alert-evaluator` stream group. Alert
rules, incidents, and a durable SMTP notification outbox live in the selected
storage backend. SMTP delivery is optional and configured through environment
variables.

NanoMQ remains an internal loopback broker at `127.0.0.1:1883`. It does not
listen for public device traffic and must never receive a literal
`v1/devices/me/rpc/request/+` subscription from multiple devices. The Rust
transport owns public TLS port `8883`, authenticates the token-only MQTT
connection, and routes each virtual `me` RPC to its one active session.

## Development

```bash
docker compose --file infra/compose.yaml up --detach
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test --workspace
npm --prefix web test
npm --prefix web run build
scripts/e2e-local.sh
```

Run the dashboard after starting `iot-api`:

```bash
NEXT_PUBLIC_API_BASE_URL=http://127.0.0.1:8080 npm --prefix web run dev
```

## API Documentation

`iot-api` generates the canonical OpenAPI 3.1 document from Rust route and
schema annotations. Swagger UI reads that generated document:

```text
http://127.0.0.1:8080/api-docs/openapi.json
http://127.0.0.1:8080/docs/
```

Protected operations use the `sessionAuth` scheme. In Swagger UI, set the
`Authorization` value to `Session <opaque session ID>` after logging in.
Internal NanoMQ HTTP auth/ACL and MQTT transport session-resolution endpoints
are intentionally not included in the public REST API document.

The Docker NanoMQ listener at `1883` is private broker infrastructure, not the
device endpoint. `scripts/e2e-local.sh` explicitly uses
`infra/nanomq/nanomq.legacy.conf` for its legacy simulator only. Modern
token-only device traffic uses the Rust transport at `8883`.

## Device-Facing MQTT Transport

The device-facing MQTT endpoint is configured independently from NanoMQ. Source
each file in its owning process, not one shared shell:

```bash
# iot-api process
set -a
source infra/dev/api.env
set +a

# iot-mqtt-transport process
set -a
source infra/dev/mqtt-transport.env
set +a
```

`infra/dev/mqtt-transport.env` binds only to `127.0.0.1:8883` and deliberately
uses local TLS file paths. Create a local development certificate before
starting the transport:

```bash
mkdir -p /tmp/rush-iot-nano/tls
openssl req -x509 -newkey rsa:2048 -nodes -days 7 \
  -keyout /tmp/rush-iot-nano/tls/mqtt-key.pem \
  -out /tmp/rush-iot-nano/tls/mqtt-cert.pem \
  -subj '/CN=localhost' \
  -addext 'basicConstraints=critical,CA:FALSE' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -addext 'keyUsage=critical,digitalSignature,keyEncipherment' \
  -addext 'extendedKeyUsage=serverAuth'
```

The transport requires `IOT_MQTT_TRANSPORT_ADDRESS`,
`IOT_MQTT_TRANSPORT_TLS_CERT_PATH`, `IOT_MQTT_TRANSPORT_TLS_KEY_PATH`,
`IOT_MQTT_TRANSPORT_API_BASE_URL`, and `IOT_MQTT_TRANSPORT_SECRET`. Its
ingest callback additionally requires
`IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_URL` and
`IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET`. The transport/API secret must
match in `api.env` and `mqtt-transport.env`; the ingest webhook secret must
match in `ingest.env` and `mqtt-transport.env`.

No NanoMQ ACL rule is used to virtualize `v1/devices/me/...`. NanoMQ 0.25.6
routes literal MQTT topic filters, while the Rust transport routes virtual RPC
by authenticated connection.

RPC requests accept an optional `"mode": "one_way" | "two_way"`; omitted mode
is one-way. `published_to_broker` means the device MQTT client acknowledged
the request publish. For two-way commands, the transport accepts only the
matching authenticated session's QoS 1 response on
`v1/devices/me/rpc/response/{id}` or
`v1/gateways/me/rpc/response/{id}`, and the command becomes `responded` only
after the API durably records that response. The command lifecycle endpoint
returns the stored response and response timestamp.

Run `iot-ingest` in a second terminal to persist telemetry and update device
status:

```bash
set -a
source infra/dev/ingest.env
set +a
target/debug/iot-ingest
```

### PowerSwitcher Simulator

`debug/sim.py` simulates a `PowerSwitcher`: it publishes `switch_state` plus
power telemetry and accepts `switch_on`, `switch_off`, and
`set_power` on the virtual RPC request topic. With no `DEVICE_TOKEN`, provide
the API URL and it provisions a device, assigns the built-in `PowerSwitcher`
profile, and prints the generated device ID:

```bash
IOT_API_BASE_URL=http://127.0.0.1:8080 \
MQTT_HOST=127.0.0.1 \
MQTT_PORT=8883 \
MQTT_CA_FILE=/tmp/rush-iot-nano/tls/mqtt-cert.pem \
python3 debug/sim.py
```

Set `DEVICE_TOKEN` to use an existing assigned PowerSwitcher instead. A
two-way control publishes its result to `v1/devices/me/rpc/response/{id}`.

### Power Monitor Demo Seed

For a local development reset that removes existing Power Monitor domain data
but preserves login users, use the Python debug script. TimescaleDB Docker is
the default:

```bash
python3 debug/seed_powermonitor.py --yes
```

For SQLite:

```bash
python3 debug/seed_powermonitor.py \
  --storage sqlite \
  --sqlite-path "$PWD/target/rush-iot-nano.db" \
  --yes
```

## SQLite Mode

SQLite mode is for a compact single-node deployment. Both `iot-api` and
`iot-ingest` must use the same absolute database path:

```bash
export IOT_DATABASE_STORAGE=sqlite
export IOT_SQLITE_PATH="$PWD/target/rush-iot-nano.db"
export IOT_NANOMQ_AUTH_SECRET=development-nanomq-auth-secret-32-bytes
export IOT_NANOMQ_WEBHOOK_SECRET=development-nanomq-webhook-secret-32-bytes
export IOT_MQTT_TRANSPORT_SECRET=development-mqtt-transport-secret-32-bytes
export IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET=development-mqtt-transport-ingest-webhook-secret-32-bytes
```

Do not set `DATABASE_URL` in this mode. SQLite enables WAL and foreign keys,
uses one telemetry writer consumer, maintains 5-minute and hourly rollups, and
prunes raw telemetry after 30 days and rollups after 365 days by default.
Override those database retention windows with
`IOT_SQLITE_RAW_RETENTION_DAYS`, `IOT_SQLITE_ROLLUP_RETENTION_DAYS`, and
`IOT_SQLITE_MAINTENANCE_BATCH_SIZE`. `IOT_SQLITE_BUSY_TIMEOUT_MS` defaults to
5,000 ms. `USE_DATABASE_STORAGE` is accepted as a compatibility alias for
`IOT_DATABASE_STORAGE`.

## Gateway Child Devices

A gateway and every child are independent platform devices. A child must be
created and assigned to an existing gateway from Management before the gateway
can report its data. The platform never creates a child device from an MQTT
payload. A child assignment revokes its active MQTT token; only the gateway
uses MQTT for that relationship.

Gateway MQTT uses its active device token as the username and an empty
password. NanoMQ permits QoS 1 publishes only to:

```text
v1/gateways/me/connect
v1/gateways/me/disconnect
v1/gateways/me/telemetry
```

Gateway telemetry is either a heartbeat:

```json
{
  "schema_version": 1,
  "kind": "heartbeat",
  "boot_id": "UUID",
  "sequence": 42,
  "event_at": "2026-09-07T00:00:00Z"
}
```

or one successful child read:

```json
{
  "schema_version": 1,
  "kind": "child_telemetry",
  "boot_id": "UUID",
  "sequence": 43,
  "event_at": "2026-09-07T00:00:00Z",
  "child_device_id": "UUIDv7",
  "measurements": { "temperature_c": 25.4 }
}
```

`connect` and `disconnect` use `schema_version`, `boot_id`, `sequence`,
`event_at`, and `child_device_id`. They are logical child transport events,
not MQTT connection events. Power Monitor reports gateway `online` or
`offline`, and child `fresh`, `stale`, or `unavailable` separately.

A physical gateway must use a bounded local SQLite WAL outbox. Keep the
original `event_at`, `boot_id`, and `sequence` until QoS 1 PUBACK, then replay
the same records after an Internet outage. Do not generate child IDs from
field payloads; use the pre-provisioned platform ID.

After changing `infra/nanomq/nanomq.conf` or `infra/nanomq/nanomq.dev.conf`,
restart NanoMQ so its webhook subscriptions include the gateway topics.

For local System Configuration testing, start the API in direct mode with
temporary configuration files. Production must continue to use the installed
root-owned helper and sudo allowlist:

```bash
mkdir -p /tmp/rush-iot-nano-config
cp infra/dev/ingest.env /tmp/rush-iot-nano-config/ingest.env
cp infra/dev/ingest.env /tmp/rush-iot-nano-config/api.env
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot \
IOT_SYSTEM_CONFIGURATION_DIRECT=true \
IOT_NANOMQ_AUTH_SECRET=development-nanomq-auth-secret-32-bytes \
IOT_ADMIN_HELPER_PATH=target/debug/iot-admin-helper \
IOT_INGEST_ENV_PATH=/tmp/rush-iot-nano-config/ingest.env \
IOT_API_ENV_PATH=/tmp/rush-iot-nano-config/api.env \
IOT_SMTP_CONFIG_PATH=/tmp/rush-iot-nano-config/smtp.env \
cargo run -p iot-api
```

See `docs/operations.md` for deployment and `firmware/esp32/README.md` for
ESP32 build/provisioning instructions.

## Access Setup

On the first `iot-api` start against a new database, the API creates:

```text
username: admin    password: NanoAdmin@1234
username: viewer   password: NanoView@1234
```

The initial values are hard-coded for this deployment and are used only when
the user table is empty. Log in as each account and change its password from
the dashboard profile. The API stores only Argon2 hashes after bootstrap, so
later restarts do not overwrite changed passwords.

Passwords must use at least eight ASCII non-whitespace characters and include
an uppercase letter, lowercase letter, digit, and special character. The
dashboard stores only an opaque session ID in browser session storage; it does
not store a password or an access token.

## Portal Routes

The browser portal separates operating applications from platform management:

```text
/apps/powermonitor                  Read-only voltage, current, power, and energy view
/management                          Platform administration (admin only)
/management/settings
/management/entities/devices
/management/entities/assets
/management/users
/management/profiles/device-profiles
/management/profiles/asset-profiles
```

At `/`, an `admin` account always opens `/management`. A `viewer` account
opens its configured `default_app`, currently `/apps/powermonitor`.
Management API routes are enforced server-side for administrators; Power
Monitor API routes require the `powermonitor` app grant.

Assets, asset profiles, device profiles, and app grants are generic platform
data. Power Monitor reads optional telemetry JSON measurements
`voltage_v`, `current_a`, `power_w`, `energy_kwh`, `frequency_hz`, and
`power_factor`; it does not add a separate ingestion pipeline.

## Device MQTT Tokens

An administrator provisions a device token by entering its display name in the
dashboard. The server assigns an internal UUIDv7 device ID; the token is shown
at creation, rotation, and later from the selected device drawer. The retained
value is AES-256-GCM encrypted in the database while a separate Argon2 hash is
used for MQTT authentication. Set `IOT_DEVICE_TOKEN_VAULT_KEY` to use a
dedicated vault key; otherwise `iot-api` derives one from
`IOT_NANOMQ_AUTH_SECRET`. Tokens created before this retention feature have no
recoverable full value and must be rotated once. Configure the ESP32 with that token as its MQTT
username and an empty MQTT password; the device publishes QoS 1 telemetry only
to `v1/devices/me/telemetry`.

NanoMQ sends device authentication and ACL checks to `iot-api`, then posts an
accepted publish event to `iot-ingest`. Set both `IOT_NANOMQ_AUTH_SECRET` and
`IOT_NANOMQ_WEBHOOK_SECRET` to separate random ASCII strings with at least
32 characters. The matching values must be configured in `nanomq.conf`.
