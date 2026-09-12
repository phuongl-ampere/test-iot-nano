# Rush IoT Nano

Rush IoT Nano is a Rust and Next.js IoT platform for token-authenticated
devices.

## Runtime

The target runtime has four Rust services:

```text
device -> iot-nano-mqttd -> iot-nano-stream -> iot-nano-core -> storage
browser -> iot-nano-api -> iot-nano-core
```

- `iot-nano-mqttd` owns MQTT, token authentication, device sessions, and RPC.
- `iot-nano-stream` owns durable event append and consumer groups.
- `iot-nano-core` owns telemetry, alerts, notifications, and command state.
- `iot-nano-api` owns users, sessions, devices, assets, profiles, and tokens.

Retired broker and ingress processes, compatibility modes, and shared API/Core
databases are not supported target-runtime components.

## Device MQTT

Devices connect with the device token as username and an empty password.
MQTTD supports MQTT 3.1.1 and MQTT 5 over TCP `1883` and TLS `8883`.

```text
subscribe: v1/devices/me/rpc/request/+
request:   v1/devices/me/rpc/request/{command_id}
response:  v1/devices/me/rpc/response/{command_id}
```

RPC uses QoS 1. MQTTD publishes a request only to the authenticated active
device session and waits for device PUBACK. A two-way response is accepted
only from that session, at the matching response topic and command ID.

## Development

Development state is disposable. Architecture or schema changes do not migrate
old data: stop the stack, reset API/Core database state, Stream state, and
MQTTD broker state, then bootstrap the current schema.

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test -p iot-nano-mqttd -- --test-threads=1
cargo test -p iot-nano-stream -- --test-threads=1
cargo test -p iot-nano-core -- --test-threads=1
cargo test -p iot-nano-api -- --test-threads=1
```

## Repository Layout

```text
services/   four deployable Rust services
contracts/  versioned service contracts
infra/      Compose, systemd, and development environment templates
web/        Next.js application
firmware/   ESP32 firmware
```

The authoritative architecture, delivery plan, reset rules, and acceptance
gates are in [docs/iot-nano-four-service-architecture.md](docs/iot-nano-four-service-architecture.md).
Operational guidance is in [docs/operations.md](docs/operations.md).
