# Operations

> **Historical architecture reference — not an operational runbook.**
> The four-service topology documented below has been superseded by the single
> `iot-nano-monolith` runtime. Do not start `iot-nano-stream`,
> `iot-nano-core`, `iot-nano-api`, or `iot-nano-mqttd` as separate deployment
> services. For local development, use [Local Development](local-development.md).
> For production deployment, storage, and rollback, use
> [Monolith Operations](operations-monolith.md).

## Runtime

The development target runs only these services:

```text
iot-nano-stream
iot-nano-core
iot-nano-api
iot-nano-mqttd
```

Start them in this order:

```text
Stream -> Core and API -> MQTTD
```

MQTTD is the only broker. It owns device-facing MQTT TCP `1883` and TLS
`8883`. Retired MQTT ingress, side-by-side listeners, and compatibility service
binaries must not be started.

## State Ownership

Each service owns its state:

| Service | State |
|---|---|
| API | users, sessions, device metadata, assets, profiles, tokens |
| Core | telemetry, rollups, alerts, notification outbox, commands |
| Stream | segments, partitions, consumer-group offsets |
| MQTTD | broker persistence and active sessions |

API and Core never share a SQLite file. Internal APIs use distinct secrets for
MQTTD-to-API, MQTTD-to-Stream, Core-to-Stream, API-to-Core, API-to-MQTTD
session revocation, and Core-to-MQTTD RPC publishing.

## Device RPC

Devices use a token-only MQTT connection. The token is the username and the
password is empty.

```text
subscribe: v1/devices/me/rpc/request/+
request:   v1/devices/me/rpc/request/{command_id}
response:  v1/devices/me/rpc/response/{command_id}
```

MQTTD requires QoS 1 for RPC requests and responses. A two-way response must
come from the active authenticated session and match a pending command.

## Development Reset

Development data is disposable. Do not write migration, fallback, dual-write,
or old-schema compatibility code.

1. Stop Stream, Core, API, MQTTD, and the local database.
2. Delete API/Core SQLite files, Stream state, and MQTTD broker state; or
   drop and recreate the development database schema.
3. Start the current database migration and bootstrap an empty API database.
4. Start Stream, Core, API, then MQTTD.
5. Run the four-service E2E suite.

Never apply this reset process to a non-development environment without
explicit approval.

## Verification

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test -p iot-nano-mqttd -- --test-threads=1
cargo test -p iot-nano-stream -- --test-threads=1
cargo test -p iot-nano-core -- --test-threads=1
cargo test -p iot-nano-api -- --test-threads=1
scripts/e2e-local.sh
```

Database-backed tests require the test database provisioned by the test
harness. The E2E suite must fail when it detects a legacy process, route,
header, environment variable, or shared data-plane storage path.

See [iot-nano-four-service-architecture.md](iot-nano-four-service-architecture.md)
for the authoritative contracts, delivery plan, and acceptance gates.
