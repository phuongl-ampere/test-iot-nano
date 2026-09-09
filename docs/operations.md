# Operations

## Local stack

Start TimescaleDB and NanoMQ:

```bash
docker compose --file infra/compose.yaml up --detach
```

Run the complete MQTT-to-database smoke test:

```bash
scripts/e2e-local.sh
```

The default local NanoMQ configuration is private at `127.0.0.1:1883`.
Device-facing TLS MQTT belongs to `iot-mqtt-transport`, not NanoMQ. This is
required because NanoMQ 0.25.6 routes literal MQTT topic filters and cannot
turn `v1/devices/me/rpc/request/+` into a per-authenticated-session route.

For a local Rust transport, source `infra/dev/api.env`,
`infra/dev/ingest.env`, and `infra/dev/mqtt-transport.env` in their respective
processes. The sample transport binds only to `127.0.0.1:8883`; generate the
temporary local TLS certificate shown in `README.md` before starting it.

`scripts/e2e-local.sh` explicitly selects
`infra/nanomq/nanomq.legacy.conf` for its old multi-device simulator harness
and starts `iot-ingest` with `IOT_LEGACY_MQTT_INGRESS=1`. That legacy mode is
test-only and must never be deployed to a Raspberry Pi.

## Raspberry Pi

Run `scripts/install-raspberry-pi.sh` on Debian ARM64. It downloads NanoMQ
`0.25.6` ARM64 SQLite package, verifies its published SHA-256 file, installs
the systemd units, and creates state directories.

Before enabling services:

1. Install PEM files at `/etc/rush-iot-nano/tls/mqtt-key.pem`,
   `/etc/rush-iot-nano/tls/mqtt-cert.pem`, and
   `/etc/rush-iot-nano/tls/mqtt-ca.pem`. Set the key and certificate files to
   `root:iot` mode `0640`, so only the transport service can read the private
   key. The installer creates the parent directories as `root:iot` mode
   `0750`.
2. Create `/etc/rush-iot-nano/mqtt-transport.env` with:

   ```dotenv
   IOT_MQTT_TRANSPORT_ADDRESS=0.0.0.0:8883
   IOT_MQTT_TRANSPORT_INTERNAL_ADDRESS=127.0.0.1:8083
   IOT_MQTT_TRANSPORT_TLS_CERT_PATH=/etc/rush-iot-nano/tls/mqtt-cert.pem
   IOT_MQTT_TRANSPORT_TLS_KEY_PATH=/etc/rush-iot-nano/tls/mqtt-key.pem
   IOT_MQTT_TRANSPORT_API_BASE_URL=http://127.0.0.1:8080
   IOT_MQTT_TRANSPORT_SECRET=<different-random-ascii-secret-of-at-least-32-characters>
   IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_URL=http://127.0.0.1:8081/internal/mqtt-transport/telemetry
   IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET=<different-random-ascii-secret-of-at-least-32-characters>
   ```

   Make the file `root:iot` mode `0640`. `iot-mqtt-transport.service` owns
   public TLS MQTT port `8883`; NanoMQ binds only to `127.0.0.1:1883`.
   `IOT_MQTT_TRANSPORT_SECRET` authenticates private transport-to-API session
   resolution. The webhook secret authenticates private transport-to-ingest
   telemetry forwarding. Do not reuse either secret for NanoMQ.
3. Replace both placeholder header values in `/etc/nanomq/nanomq.conf` with
   separate random ASCII strings of at least 32 characters when NanoMQ private
   compatibility ingress is enabled. NanoMQ's external TLS listener is
   intentionally absent; do not add virtual-RPC ACL rules there. NanoMQ
   management remains on loopback port `8082`; transport internal command
   dispatch listens only on `127.0.0.1:8083`.
4. Select one storage backend in both environment files. For TimescaleDB, set
   `IOT_DATABASE_STORAGE=timescale` and `DATABASE_URL`. For embedded SQLite,
   set `IOT_DATABASE_STORAGE=sqlite` and the same absolute
   `IOT_SQLITE_PATH=/var/lib/iot-ingest/rush-iot-nano.db` in both files; do not set
   `DATABASE_URL`. Then create `/etc/rush-iot-nano/ingest.env` with the
   selected storage variables, `IOT_NANOMQ_WEBHOOK_SECRET`,
   `IOT_NANOMQ_WEBHOOK_INBOX_DIR`,
   `MQTT_BROKER_HOST=127.0.0.1`, `MQTT_BROKER_PORT=1883`, `IOT_STREAM_DIR`,
   `IOT_STREAM_PARTITIONS`, `IOT_STREAM_SEGMENT_BYTES`,
   `IOT_STREAM_RETENTION_BYTES`, `IOT_STREAM_RETENTION_SECONDS`,
   `IOT_STREAM_MAX_RECORD_BYTES`, `IOT_WRITER_GROUP`,
   `IOT_WRITER_MEMBER_ID`, `IOT_ALERT_GROUP`, and
   `IOT_INGEST_HEALTH_ADDRESS`, `IOT_MQTT_TRANSPORT_URL=http://127.0.0.1:8083`,
   `IOT_MQTT_TRANSPORT_SECRET`, and
   `IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET`. Add all SMTP
   values only when email delivery is required: `SMTP_HOST`, `SMTP_PORT=465`,
   `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_EMAIL_FROM`,
   `ALERT_EMAIL_TO`, and `SMTP_TIMEOUT_SECONDS=15`. Any partial SMTP
   configuration prevents `iot-ingest` from starting.
5. Create `/etc/rush-iot-nano/api.env` with the same selected storage
   variables, `IOT_NANOMQ_AUTH_SECRET`, `MQTT_BROKER_HOST=127.0.0.1`,
   `MQTT_BROKER_PORT=1883`, `MQTT_API_USERNAME`, and
   `MQTT_API_PASSWORD`. These MQTT credentials are only for internal command
   publishing, not device authentication. Add the exact
   `IOT_MQTT_TRANSPORT_SECRET` and
   `IOT_MQTT_TRANSPORT_URL=http://127.0.0.1:8083` from
   `mqtt-transport.env`; they are required for the transport's private
   session-resolution and session-revocation endpoints. On a new database, `iot-api`
   creates two hard-coded accounts: username `admin` with password
   `NanoAdmin@1234`, and username `viewer` with password `NanoView@1234`.
   Change both passwords through their dashboard profiles immediately after
   first login. Passwords require at least eight ASCII non-whitespace characters with
   uppercase, lowercase, digit, and special characters. The API stores only
   Argon2 hashes, and restart does not overwrite profile-managed passwords.
6. Add the exact `IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET` from
   `mqtt-transport.env` to `ingest.env`. It is separate from
   `IOT_NANOMQ_WEBHOOK_SECRET`.
7. Enable `nanomq.service`, `iot-api.service`, `iot-ingest.service`, and
   `iot-mqtt-transport.service`:

   ```bash
   sudo systemctl enable --now nanomq.service iot-api.service iot-ingest.service iot-mqtt-transport.service
   sudo systemctl status iot-mqtt-transport.service --no-pager
   sudo ss -ltnp | grep -E '(:8883|127\.0\.0\.1:1883)'
   ```

   The transport unit starts after API and ingest. A listening `:8883` process
   must be `iot-mqtt-transport`; NanoMQ must only listen on `127.0.0.1:1883`.

`IOT_LEGACY_MQTT_INGRESS` defaults to `false`. Do not enable it in production:
public devices use `iot-mqtt-transport` and do not include their device ID in
the topic or telemetry payload. NanoMQ webhook ingress is only a private
compatibility path.

`IOT_NANOMQ_WEBHOOK_INBOX_DIR` must be local durable storage under the
`iot` service account. The webhook handler fsyncs the broker envelope there
before background token resolution and stream append; its directory is mode
`0700` and its inbox file mode `0600` because the envelope temporarily carries
the device token. The systemd services run with `UMask=0077`; keep the shared
SQLite database under `/var/lib/iot-ingest`, which the installer creates mode
`0700`.

## Device MQTT Transport

Provision a device token by entering a display name in the admin dashboard.
The API assigns the internal device ID as UUIDv7; firmware never needs that
ID. The plaintext token is shown only on create and rotate. Firmware stores it
as the MQTT username, sends an empty password, validates the configured CA,
and connects to the Rust transport at TLS port `8883`.

The transport resolves the token against the private `iot-api` endpoint, keeps
the device-to-session mapping in memory, and forwards accepted telemetry to
the configured private ingest webhook. It owns
`v1/devices/me/rpc/request/+` and
`v1/gateways/me/rpc/request/+` virtual subscriptions. NanoMQ is not a
device-facing route and must not be used to implement those subscriptions.

Commands default to one-way. `published_to_broker` only confirms the device
MQTT QoS 1 `PUBACK`; it does not claim command execution. A two-way command
keeps a transport-local pending correlation for its exact
`device_id/token_id/connection_id`. The device replies QoS 1 to
`v1/devices/me/rpc/response/{command_id}` or
`v1/gateways/me/rpc/response/{command_id}`. Transport forwards the JSON reply
to the private API callback and sends the device `PUBACK` only after the API
stores it atomically as `responded`. A different session, one-way command,
unknown command, revoked token, or expired command cannot become `responded`.

NanoMQ's existing HTTP auth/ACL and webhook settings are retained for private
compatibility paths. NanoMQ 0.25.6 requires `auth.allow_anonymous = true`
when `auth.http_auth` is used with an empty MQTT password. This legacy setting
does not make public device access safe and must not be used as virtual-RPC
authorization.

## System Configuration

The admin dashboard System Configuration view updates SMTP credentials, MQTT
broker host/port, and allowed `iot-ingest` tuning. MQTT host/port is written
atomically to both `/etc/rush-iot-nano/ingest.env` and
`/etc/rush-iot-nano/api.env`; restart both services after changing it:

```bash
sudo systemctl restart iot-ingest.service
sudo systemctl restart iot-api.service
```

The Raspberry Pi installer deploys a root-owned
`/usr/local/sbin/iot-admin-helper` and a sudoers allowlist so `iot-api` may
only read/apply this configuration.

The view cannot change `DATABASE_URL`, `IOT_SQLITE_PATH`, MQTT credentials, stream
directory/partitions, consumer group identity, or health address. Keep
`ingest.env` mode `0600`; API and SMTP environment files are root-owned,
group-readable by `iot`, and contain credentials that are never returned by
the API. SMTP password is write-only in the dashboard.

`IOT_MQTT_TRANSPORT_ADDRESS`, transport TLS paths, API base URL, and both
transport secrets are deployment-owned settings. Change them only in the
root-owned transport/API/ingest environment files, then restart the affected
service manually.

SMTP host/port/credentials/sender/recipient and timeout apply to the next
notification delivery without restarting `iot-ingest`. Stream retention,
segment, and max-record sizes; writer, alert, and notification batch sizes;
worker intervals; notification lease/retry settings; and MQTT broker host/port
require a manual restart. The helper writes the matching `IOT_*` and MQTT
values atomically and leaves all locked deployment values unchanged.

`iot-ingest` defaults to one day of stream retention
(`IOT_STREAM_RETENTION_SECONDS=86400`) and 2 GiB retained stream data
(`IOT_STREAM_RETENTION_BYTES=2147483648`). Override either value in
`ingest.env` when the SSD capacity or replay window requires it.

In SQLite mode, raw telemetry defaults to 30 days and 5-minute/hourly rollups
to 365 days. The ingestion service deletes data in bounded SQLite batches,
then checkpoints WAL and performs incremental vacuum. Tune with
`IOT_SQLITE_RAW_RETENTION_DAYS`, `IOT_SQLITE_ROLLUP_RETENTION_DAYS`, and
`IOT_SQLITE_MAINTENANCE_BATCH_SIZE`; restart `iot-ingest` after changing them.
`IOT_SQLITE_BUSY_TIMEOUT_MS` defaults to 5,000 ms. The legacy
`USE_DATABASE_STORAGE` name remains accepted, but use
`IOT_DATABASE_STORAGE` in new deployments.

## Alerts

Alert when `iot_ingest_stream_group_lag` remains nonzero for 30 minutes,
`iot_ingest_stream_log_bytes` reaches 80 percent of
the configured `IOT_STREAM_RETENTION_BYTES`, `iot_ingest_stream_failures_total` increases,
database failures persist for five minutes, NanoMQ is unavailable, or SSD free
space falls below 15 percent.

Also alert when `iot_ingest_stream_group_lag{group="alert-evaluator"}` grows,
`iot_ingest_alert_open_incidents` rises unexpectedly,
`iot_ingest_notification_outbox_pending` remains nonzero, or
`iot_ingest_notification_failures_total` increases.

## Stress Test

Run the local data-plane stress test after bringing up Docker:

```bash
STRESS_EVENTS=10000 STRESS_DEVICES=100 scripts/stress-local.sh
```

The command fans one telemetry stream into `timescaledb-writer` and
`alert-evaluator`, requires both consumer groups to reach lag zero, and prints
measured messages per second. It has a 120-second watchdog but intentionally
does not enforce a hardware-specific minimum rate.

Back up PostgreSQL daily. For SQLite, use SQLite's consistent `.backup`
command or backup API before copying the resulting file to storage outside the
Raspberry Pi, and test restore monthly.
