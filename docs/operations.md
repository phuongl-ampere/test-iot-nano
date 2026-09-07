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

The default local NanoMQ configuration at port `1883` uses the same device
token HTTP auth/ACL gate as production. NanoMQ 0.25.6 still requires
`allow_anonymous = true` internally for empty MQTT passwords, but
`no_match = deny` and HTTP auth prevent an anonymous client from connecting or
publishing.

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
   `/etc/rush-iot-nano/tls/mqtt-ca.pem`. Replace both placeholder header
   values in `/etc/nanomq/nanomq.conf` with two different random ASCII
   strings of at least 32 characters. NanoMQ exposes device MQTT only over
   TLS port `8883`; port `1883` is loopback-only for the API command service.
2. Select one storage backend in both environment files. For TimescaleDB, set
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
   `IOT_INGEST_HEALTH_ADDRESS`. Add all SMTP
   values only when email delivery is required: `SMTP_HOST`, `SMTP_PORT=465`,
   `SMTP_USERNAME`, `SMTP_PASSWORD`, `ALERT_EMAIL_FROM`,
   `ALERT_EMAIL_TO`, and `SMTP_TIMEOUT_SECONDS=15`. Any partial SMTP
   configuration prevents `iot-ingest` from starting.
3. Create `/etc/rush-iot-nano/api.env` with the same selected storage
   variables, `IOT_NANOMQ_AUTH_SECRET`, `MQTT_BROKER_HOST=127.0.0.1`,
   `MQTT_BROKER_PORT=1883`, `MQTT_API_USERNAME`, and
   `MQTT_API_PASSWORD`. These MQTT credentials are only for internal command
   publishing, not device authentication. On a new database, `iot-api`
   creates two hard-coded accounts: username `admin` with password
   `NanoAdmin@1234`, and username `viewer` with password `NanoView@1234`.
   Change both passwords through their dashboard profiles immediately after
   first login. Passwords require at least eight ASCII non-whitespace characters with
   uppercase, lowercase, digit, and special characters. The API stores only
   Argon2 hashes, and restart does not overwrite profile-managed passwords.
4. Enable `nanomq.service`, `iot-ingest.service`, and `iot-api.service`.

`IOT_LEGACY_MQTT_INGRESS` defaults to `false`. Do not enable it in production:
devices use NanoMQ webhook ingress and no longer include their device ID in
the topic or telemetry payload.

`IOT_NANOMQ_WEBHOOK_INBOX_DIR` must be local durable storage under the
`iot` service account. The webhook handler fsyncs the broker envelope there
before background token resolution and stream append; its directory is mode
`0700` and its inbox file mode `0600` because the envelope temporarily carries
the device token. The systemd services run with `UMask=0077`; keep the shared
SQLite database under `/var/lib/iot-ingest`, which the installer creates mode
`0700`.

## Device MQTT Tokens

Provision a device token by entering a display name in the admin dashboard.
The API assigns the internal device ID as UUIDv7; firmware never needs that
ID. The plaintext token is shown only on create and rotate. Firmware stores it
as the MQTT username, sends an empty password, validates the configured CA,
and publishes QoS 1 only to `v1/devices/me/telemetry`.

NanoMQ validates the token with `POST /internal/nanomq/auth`, allows only the
device telemetry publish through `POST /internal/nanomq/acl`, and forwards
the payload plus `from_username` to
`POST /internal/nanomq/telemetry`. `iot-ingest` resolves the active token a
second time before appending to the durable stream, so revoke and rotate stop
subsequent telemetry even if a broker client remains connected.

NanoMQ 0.25.6 requires `auth.allow_anonymous = true` when `auth.http_auth` is
used with an empty device password. This bypasses only its legacy password-file
check; `POST /internal/nanomq/auth` remains the required connection gate and
must never be removed or pointed at an unauthenticated endpoint.

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
