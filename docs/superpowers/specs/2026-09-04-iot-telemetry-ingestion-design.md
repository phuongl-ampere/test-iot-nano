# Raspberry Pi IoT Telemetry Platform Design

**Date:** 2026-09-04

## Goal

Build a Raspberry Pi-hosted IoT platform for 1,000 ESP devices. Each device
publishes up to 10 telemetry messages per minute. The platform provisions
device connectivity, receives telemetry, preserves it through a database
outage lasting several hours, stores it in TimescaleDB, and exposes it through
a Rust API and Next.js dashboard.

The nominal ingestion rate is 167 messages per second, or 14.4 million
messages per day.

## Decisions

- NanoMQ is the MQTT broker. Install its ARM64 binary and run it under systemd.
- One Rust binary, `iot-ingest`, contains the MQTT consumer, local
  append-only stream producer, and the `timescaledb-writer` consumer member.
- `iot-stream` is a Rust local event stream on SSD, not Kafka and not a
  separate broker. It uses eight fixed partitions, append-only segment files,
  sparse indexes, per-group offsets, and lease-based consumer assignment.
- ESP devices and `iot-ingest` use MQTT QoS 1. The delivered guarantee is
  at-least-once; database writes are idempotent.
- PostgreSQL/TimescaleDB, the Rust API, and Next.js run on the Raspberry Pi.
  TimescaleDB and stream data live on an external SSD, never an SD card.
- The Rust API is built with Axum and SQLx. The dashboard uses Next.js and
  ApexCharts.

## Architecture

```text
ESP devices -- MQTT QoS 1 --> NanoMQ --> Rust iot-ingest --> iot-stream
                                                               |
                                                               +-- eight append-only partitions
                                                               +-- timescaledb-writer group
                                                               +-- per-partition committed offsets
                                                               |
                                                               v
                                                          TimescaleDB

Next.js + ApexCharts <-------------------- Rust API <------------- TimescaleDB
```

`iot-ingest` is one binary and one systemd service. The stream is a local
library with a durable on-disk format, so a future consumer process can join
the same group or a different group without changing MQTT topics or the
database contract.

## ESP Firmware

### Device configuration

Persist these settings in non-volatile storage:

- `device_id`
- Wi-Fi SSID and password
- DHCP or static IPv4 mode
- Static IPv4 address, gateway, DNS, and subnet mask when static mode is used
- MQTT hostname or IP, port, TLS mode, username, and password
- Telemetry interval

### Provisioning mode

Start a local Wi-Fi access point and HTTP configuration server when the device
has no valid configuration, repeatedly cannot join Wi-Fi, or receives an
authorized local reset action. The UI validates values before saving, requires
an administrator password, hides submitted secrets, and disables the access
point after successful connectivity.

### Telemetry envelope

Publish to:

```text
iot/v1/devices/{device_id}/telemetry
```

Use QoS 1 and `retain=false`. The device persists `boot_id` and increments
`sequence` for each telemetry event; `sequence` resets only after `boot_id`
changes.

```json
{
  "schema_version": 1,
  "device_id": "esp-000123",
  "boot_id": "c9c04d99-4e01-4f94-82a8-9e229e47c093",
  "sequence": 1842,
  "event_at": "2026-09-04T10:12:00Z",
  "measurements": {
    "temperature_c": 26.4,
    "humidity_pct": 71.2
  }
}
```

Other reserved topics are:

```text
iot/v1/devices/{device_id}/status
iot/v1/devices/{device_id}/command
iot/v1/devices/{device_id}/config
```

## NanoMQ

NanoMQ accepts device connections on the LAN MQTT listener. Every device uses
individual credentials and an ACL restricted to its own
`iot/v1/devices/{device_id}/#` tree. Enable broker persistence and durable
MQTT sessions so unacknowledged QoS 1 traffic remains available while
`iot-ingest` restarts.

Use TLS when the MQTT network is untrusted. Plain MQTT is allowed only on an
isolated LAN with that risk explicitly accepted.

## Rust iot-ingest

### MQTT consumer

Subscribe to `iot/v1/devices/+/telemetry` with QoS 1 and a persistent session.
Validate the topic and JSON envelope, including equality of the device ID in
the topic and payload. Invalid events are rejected and counted in metrics.

The consumer acknowledges a publish only after an `iot-stream` record has been
appended and `sync_data` completes. The selected Rust MQTT library must
support application-controlled acknowledgement; this is an acceptance
criterion during implementation.

### Local event stream

`iot-stream` runs on the SSD with eight fixed partitions selected by
`CRC32(device_id) % 8`. Each partition uses append-only segment files capped
at 128 MiB, a CRC32-protected length-delimited frame format, and a sparse
offset-to-byte index every 128 records. The producer calls `sync_data` before
the MQTT acknowledgement.

```text
stream/
  manifest.json
  partitions/0000/00000000000000000000.log
  partitions/0000/00000000000000000000.idx
  groups/timescaledb-writer/state.json
```

Each record retains the original topic and payload plus the validated telemetry
event and `received_at`. On restart, the active segment is scanned; only a
truncated final frame is removed. A malformed checksum, invalid payload, or
corrupt closed segment prevents startup.

The default retention policy retains at most 2 GiB and one day of closed
segments. Retention is independent of group offsets, as in Kafka. A lagging
group receives `OffsetOutOfRange` rather than silently rewinding or blocking
retention.

```text
group state:
member heartbeat -> 30-second partition lease -> poll records
PostgreSQL transaction commit -> commit next offset
```

Groups store a next offset for every partition. Members heartbeat every 10
seconds; assignment is round-robin across sorted active member IDs and a
generation invalidates stale commits. Only one member owns a partition lease
at a time. A failed database write leaves offsets unchanged. If PostgreSQL
commits but the offset commit fails, the next member replays records and the
TimescaleDB unique key makes that safe.

At the hard retained-byte limit, a producer append fails without writing a
partial frame, so `iot-ingest` does not acknowledge the MQTT publish and the
broker's persistent QoS 1 session supplies backpressure.

### Database writer

The `timescaledb-writer` group polls up to 1,000 records or waits at most one
second, then writes the batch in one PostgreSQL transaction. It commits each
partition's next offset only after that transaction succeeds. On failure it
keeps offsets unchanged for replay. The writer never advances an offset before
the database transaction succeeds.

## TimescaleDB

Create:

- `devices`: enrolled device identity, display name, metadata, configuration
  version, creation time, and last-seen time.
- `telemetry`: a hypertable partitioned by `event_at`.

The telemetry table has these required columns:

```text
event_at       timestamptz not null
received_at    timestamptz not null
device_id      text not null
boot_id        uuid not null
sequence       bigint not null
measurements   jsonb not null
topic          text not null
```

Use a unique constraint on `(event_at, device_id, boot_id, sequence)`. It
includes the hypertable time column and makes MQTT redelivery idempotent.
Index `(device_id, event_at desc)` for device charts.

Create 5-minute and 1-hour continuous aggregates for dashboard requests.
Compress raw telemetry after seven days, retain compressed raw telemetry for
30 days, and retain aggregates for one year. This bounds SSD growth while
preserving operational history.

## Rust API and Web Application

The Rust API owns authentication, device management, telemetry query filters,
and command/configuration publishing. It reads TimescaleDB directly and does
not query the local stream.

The Next.js application provides:

- device list with online state and last-seen timestamp
- device detail view with time range selection
- ApexCharts telemetry charts using aggregate data by default
- configuration and command controls for authorized users

The API enforces device and user authorization before returning telemetry or
publishing any command.

## Reliability and Operations

The platform provides at-least-once ingestion. A device or broker can
redeliver a QoS 1 message; the TimescaleDB unique key suppresses duplicate
rows. The platform does not claim end-to-end exactly-once delivery.

Expose a local health endpoint and Prometheus metrics for accepted/rejected
messages, stream bytes, partition earliest offsets and high watermarks,
per-group committed offsets and lag, database failures, and stream failures.

Alert when:

- stream retained bytes are above 80 percent of the configured 12 GiB cap
- a `timescaledb-writer` group lag remains nonzero for 30 minutes
- stream failures occur
- database writes fail for five consecutive minutes
- NanoMQ or `iot-ingest` is unavailable
- SSD free space is below 15 percent

Use systemd restart policies for NanoMQ, `iot-ingest`, the Rust API, and
Next.js. Back up PostgreSQL daily to storage outside the Raspberry Pi and test
restore monthly.

## Delivery Phases

1. Validate Raspberry Pi ARM64, SSD, NanoMQ binary, PostgreSQL/TimescaleDB,
   and systemd deployment. Benchmark 167 messages per second with the selected
   message size.
2. Implement ESP provisioning, Wi-Fi reconnect behavior, MQTT QoS 1 publishing,
   and the telemetry envelope.
3. Configure NanoMQ listeners, credentials, ACLs, persistence, and durable
   sessions. Verify an adapter restart causes no loss of unacknowledged data.
4. Implement `iot-ingest`, its Rust append-only stream, consumer groups,
   idempotent TimescaleDB writer, metrics, and overload behavior.
5. Add the TimescaleDB schema, hypertable policies, aggregates, and backup/
   restore verification.
6. Implement Rust API endpoints and Next.js/ApexCharts device monitoring.
7. Run failure tests: process restart, database outage for six hours, broker
   restart, duplicate publish, full stream, malformed payload, and SSD low
   space.

## Acceptance Criteria

- Sustain 167 valid telemetry messages per second for one hour without stream
  growth while TimescaleDB is healthy.
- During a six-hour TimescaleDB outage, retain accepted telemetry locally and
  drain it after recovery without duplicate database rows.
- Restarting `iot-ingest` during active publishing does not lose accepted QoS 1
  messages.
- An unauthorized MQTT client cannot publish to another device's topic.
- The dashboard displays 5-minute aggregates for a selected device without
  querying raw telemetry for the default time range.
