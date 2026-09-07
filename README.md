# Rush IoT Nano

Rust and Next.js IoT telemetry platform for ESP32 devices:

```text
ESP32 -> NanoMQ HTTP auth/ACL -> NanoMQ webhook -> iot-ingest -> iot-stream -> writer consumer group -> TimescaleDB or SQLite -> Rust API -> Next.js
```

`iot-ingest` also runs an independent `alert-evaluator` stream group. Alert
rules, incidents, and a durable SMTP notification outbox live in the selected
storage backend. SMTP delivery is optional and configured through environment
variables.

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
Internal NanoMQ HTTP auth/ACL endpoints are intentionally not included in the
public REST API document.

The default Docker NanoMQ broker at `1883` requires an active device token.
Run `iot-api` with
`IOT_NANOMQ_AUTH_SECRET=development-nanomq-auth-secret-32-bytes` before
connecting a local simulator. Use `DEVICE_TOKEN=<iotd_...> python3
debug/sim.py`; an empty token is denied before telemetry is published.

Run the token webhook ingest worker in a second terminal to persist telemetry
and update the device online status:

```bash
set -a
source infra/dev/ingest.env
set +a
target/debug/iot-ingest
```

## SQLite Mode

SQLite mode is for a compact single-node deployment. Both `iot-api` and
`iot-ingest` must use the same absolute database path:

```bash
export IOT_DATABASE_STORAGE=sqlite
export IOT_SQLITE_PATH="$PWD/target/rush-iot-nano.db"
export IOT_NANOMQ_AUTH_SECRET=development-nanomq-auth-secret-32-bytes
export IOT_NANOMQ_WEBHOOK_SECRET=development-nanomq-webhook-secret-32-bytes
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
