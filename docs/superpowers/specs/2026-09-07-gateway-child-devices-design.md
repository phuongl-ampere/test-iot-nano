# Gateway Child Devices Design

## Goal

Add a strict, single-active-gateway model that lets one token-authenticated
gateway report telemetry and child availability for pre-provisioned devices.

## Scope

- A device may be a gateway (`is_gateway`) or a child of one gateway
  (`gateway_device_id`).
- Direct devices remain supported and retain the existing
  `v1/devices/me/telemetry` protocol.
- Gateways use three QoS 1 topics:
  `v1/gateways/me/connect`, `v1/gateways/me/disconnect`, and
  `v1/gateways/me/telemetry`.
- A gateway token resolves only to its gateway device. Every child ID is
  checked against the stored parent relationship before data reaches
  `iot-stream`.
- A child assignment revokes every active child token in the same transaction.
  Token creation and rotation reject child devices.
- Gateway availability uses gateway activity. Child availability uses the
  gateway's successful-read time and explicit disconnect signal:
  `fresh` within five minutes, `stale` through fifteen minutes, otherwise
  `unavailable`.
- Power Monitor and Management expose gateway identity, parent gateway, and
  the derived state. Token controls are hidden for child devices.

## Data Model

`devices` gains:

- `is_gateway BOOLEAN NOT NULL DEFAULT FALSE`
- `gateway_device_id TEXT NULL REFERENCES devices(device_id) ON DELETE RESTRICT`
- `gateway_last_read_at TIMESTAMPTZ NULL`
- `gateway_read_quality TEXT NULL`, constrained to `good` or `unavailable`

The relationship is constrained so a gateway has no parent and a child cannot
reference itself. V1 deliberately supports exactly one active gateway per
child. Existing devices are direct devices after migration.

`telemetry` gains nullable `gateway_device_id` for provenance. It is retained
as an audit field without a database foreign key so historical TimescaleDB
chunks remain writable during a gateway's lifecycle. Direct device records
retain `NULL`.

## Gateway Protocol

The MQTT username is the gateway's active token and the password is empty.
NanoMQ ACL permits a gateway to publish only the three gateway topics; a
direct device may publish only the existing direct-device telemetry topic.

Gateway telemetry has two payload variants:

```json
{
  "schema_version": 1,
  "kind": "heartbeat",
  "boot_id": "UUID",
  "sequence": 42,
  "event_at": "2026-09-07T00:00:00Z"
}
```

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

`connect` and `disconnect` use one child per message with
`schema_version`, `boot_id`, `sequence`, `event_at`, and `child_device_id`.
They describe the gateway's logical connection to the child, not the MQTT
connection itself.

The platform rejects malformed, future-dated, unknown, deleted, unassigned,
or foreign child messages. It never creates a device from a gateway payload.

## Ingestion And Alerts

The webhook worker resolves the NanoMQ token under a database transaction,
checks the gateway flag and child ownership, then appends an event keyed by
the child device ID. The event carries the gateway device ID as provenance.
The TimescaleDB writer stores that provenance without changing direct-device
storage behavior.

Gateway activity updates gateway presence. A successful child read updates
`gateway_last_read_at`; a disconnect marks child quality unavailable. The API
derives `fresh`, `stale`, or `unavailable` at read time. Alert evaluation for
measurement rules remains unchanged; a future health evaluator can consume
the derived state without fabricating telemetry. Delayed telemetry is stored
with its original UTC timestamp.

## Gateway Edge Contract

The platform contract is idempotent through `boot_id` and `sequence`.
A real Modbus, BLE, RS485, or GPIO gateway must keep a bounded local SQLite
WAL outbox until MQTT QoS 1 publish acknowledgement, preserve the original
timestamp and sequence on replay, and apply a documented disk retention
limit. Hardware adapters and a generic gateway agent are out of scope for
this platform change.
