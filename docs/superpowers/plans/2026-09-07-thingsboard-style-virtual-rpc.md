# ThingsBoard-Style Virtual RPC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Let token-only devices subscribe to `v1/devices/me/rpc/request/+`, while routing each one-way command to exactly one authenticated device or gateway MQTT session.

**Architecture:** The Rust `iot-mqtt-transport` service owns device-facing TLS MQTT connections and maps `token_id -> device_id -> active connection`. It virtualizes the device-facing `me` topic to a private per-session route, while NanoMQ remains behind it for telemetry broker behavior. `iot-ingest` dispatches durable database-backed command outbox records and records broker publication, not device execution.

**Tech Stack:** Rust 1.96, Tokio, Axum, SQLx, PostgreSQL/SQLite, NanoMQ 0.25.6, MQTT QoS 1, ESP-IDF MQTT.

## Implementation Status

Implemented on 2026-09-08. The public TLS transport, token/session routing,
durable command outbox, gateway child envelopes, token-revoke session control,
firmware request handling, and operations artifacts in this plan are present.

The implementation also includes the follow-on two-way extension:

- Commands default to `one_way`; `two_way` commands move from
  `published_to_broker` to `responded` only after the authenticated MQTT
  session publishes a matching QoS 1 response.
- SQLite and TimescaleDB store `mode`, response JSON, and `responded_at`.
- Offline transport responses (`503`/`504`) release a command for retry until
  its TTL expires instead of reporting false delivery or terminal failure.
- `scripts/e2e-local.sh --rpc` verifies the runtime stack, command isolation,
  publication, token revoke/rotation, and offline expiry.

## Global Constraints

- Firmware knows only MQTT host, TLS CA, and full device token.
- Direct devices subscribe only to `v1/devices/me/rpc/request/+`.
- Gateways subscribe only to `v1/gateways/me/rpc/request/+`.
- Never broadcast a literal `me` topic through plain MQTT routing.
- One-way state is only `queued`, `published_to_broker`, `expired`, or `failed`.
- Every command has UUIDv7 `id`, JSON-object `params`, UTC `issued_at`, and UTC `expires_at`.
- Publish uses QoS 1 and `retain=false`.
- Full tokens never enter topics, command rows, logs, or API responses.
- Virtual session routing is implemented by the Rust `iot-mqtt-transport` service, not a NanoMQ plugin.
- PostgreSQL and SQLite expose the same command API and state machine.
- Every implementation task starts with a failing focused test.

---

### Task 1: Transport Compatibility Spike

**Files:**
- Create: `docs/spikes/iot-mqtt-transport-nanomq-0.25.6.md`
- Create: `infra/nanomq/virtual-rpc-spike.conf`
- Create: `scripts/nanomq-virtual-rpc-spike.sh`

**Produces:** Verified connection ownership and forwarding requirements for `iot-mqtt-transport`.

- [ ] Write a two-client probe. Device A and B authenticate with distinct tokens and both subscribe:

```text
v1/devices/me/rpc/request/+
```

- [ ] Run:

```bash
scripts/nanomq-virtual-rpc-spike.sh
```

Expected initial result: FAIL. Stock NanoMQ routes the literal subscription to every matching client.

- [ ] Document the protocol boundary that `iot-mqtt-transport` must own:

```text
public TLS MQTT CONNECT -> validate token and register device session
public SUBSCRIBE v1/devices/me/rpc/request/+ -> virtual session subscription
target device_id command -> PUBLISH to one public client connection
client DISCONNECT -> invalidate that connection
inbound telemetry -> forward safely to the private NanoMQ path
```

- [ ] Re-run the probe through `iot-mqtt-transport`. Expected: A receives one command and B receives none.

### Task 2: RPC Contract

**Files:**
- Create: `crates/iot-core/src/rpc.rs`
- Modify: `crates/iot-core/src/lib.rs`
- Create: `crates/iot-core/tests/rpc_contract.rs`

**Produces:**

```rust
pub enum RpcTarget {
    DirectDevice { device_id: String },
    GatewayChild { gateway_device_id: String, child_device_id: String },
}

pub struct RpcRequest {
    pub id: Uuid,
    pub method: String,
    pub params: serde_json::Value,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub enum CommandState {
    Queued,
    PublishedToBroker,
    Expired,
    Failed,
}
```

- [ ] Write failing tests that reject non-UUIDv7 IDs, non-object params, expired commands, and invalid method names.
- [ ] Run:

```bash
cargo test -p iot-core --test rpc_contract
```

Expected: FAIL because `RpcRequest` does not exist.

- [ ] Implement validation: method matches existing identifier rules, params is an object, and expiry is after issue time.
- [ ] Re-run the same test. Expected: PASS.

### Task 3: Portable Command Outbox

**Files:**
- Create: `db/migrations/0011_command_outbox.sql`
- Modify: `crates/iot-storage/src/lib.rs`
- Create: `crates/iot-storage/tests/command_outbox.rs`

**Produces:** `enqueue_command`, `claim_due_commands`, `mark_published`, `mark_failed`, and `expire_due_commands` for both stores.

- [ ] Write failing SQLite and PostgreSQL tests proving only one dispatcher claims a queued command.
- [ ] Create equivalent tables:

```text
id, target_kind, device_id, gateway_device_id, child_device_id,
method, params, state, issued_at, expires_at, lease_until,
attempt_count, published_at, last_error, created_by, created_at, updated_at
```

- [ ] Use `FOR UPDATE SKIP LOCKED` for PostgreSQL and a single `UPDATE ... RETURNING` lease claim for SQLite.
- [ ] Commands that pass `expires_at` move to `expired`; expired leased commands are never retried.
- [ ] Run:

```bash
cargo test -p iot-storage --test command_outbox
```

Expected: PASS for duplicate claims, expiry, lease recovery, and PostgreSQL/SQLite state parity.

### Task 4: Rust MQTT Transport and Session Router

**Files:**
- Create: `crates/iot-mqtt-transport/`
- Modify: `Cargo.toml`
- Modify: `infra/nanomq/nanomq.conf`
- Create: `infra/systemd/iot-mqtt-transport.service`
- Create: `crates/iot-mqtt-transport/tests/session_routing.rs`

**Produces:**

```rust
pub trait SessionRouter {
    async fn register(&self, session: ActiveDeviceSession) -> Result<(), SessionError>;
    async fn unregister(&self, connection_id: &str);
    async fn publish_rpc(
        &self,
        target: &RpcTarget,
        request: &RpcRequest,
    ) -> Result<PublishResult, SessionError>;
}
```

- [ ] Write a failing two-session test: both subscribe to `v1/devices/me/rpc/request/+`; a command for A must never reach B.
- [ ] Add tests for reconnect replacement, stale disconnect generation, token rotation, and revoke invalidation.
- [ ] Store only `token_id`, `device_id`, `client_id`, `connection_id`, and generation in memory. Do not retain full tokens.
- [ ] Translate a direct command internally to:

```text
__iot/session/{connection_id}/rpc/request/{command_id}
```

The device-facing subscription remains the virtual `me` topic.

- [ ] Run:

```bash
cargo test -p iot-mqtt-transport --test session_routing
```

Expected: PASS.

### Task 5: Command Dispatcher and API

**Files:**
- Create: `crates/iot-ingest/src/command.rs`
- Modify: `crates/iot-ingest/src/lib.rs`
- Modify: `crates/iot-ingest/src/main.rs`
- Modify: `crates/iot-api/src/routes.rs`
- Create: `crates/iot-ingest/tests/command.rs`
- Modify: `crates/iot-api/tests/api.rs`
- Modify: `crates/iot-api/tests/sqlite_auth.rs`

**Produces:**

```text
POST /api/devices/{device_id}/commands
GET  /api/device-commands/{id}
```

- [ ] Write failing API tests: admin creates one command and receives `202` plus `{ id, state: "queued" }`; viewer receives `403`.
- [ ] Write failing dispatcher test: a connected target becomes `published_to_broker`; a disconnected target expires at TTL without a false delivery claim.
- [ ] Replace the current direct `AsyncClient.publish` handler with an atomic outbox insert.
- [ ] Start `CommandDispatcher` inside `iot-ingest`, beside notification dispatching. It claims due commands, resolves a live session, and invokes `SessionRouter.publish_rpc`.
- [ ] Set `published_to_broker` only after the router receives MQTT QoS 1 broker acknowledgment.
- [ ] Run:

```bash
cargo test -p iot-api --test api
cargo test -p iot-api --test sqlite_auth
cargo test -p iot-ingest --test command
```

Expected: PASS.

### Task 6: Gateway Child Commands

**Files:**
- Modify: `crates/iot-core/src/rpc.rs`
- Modify: `crates/iot-ingest/src/command.rs`
- Modify: `crates/iot-api/src/routes.rs`
- Modify: `crates/iot-ingest/tests/command.rs`

- [ ] Write a failing test proving a child command is delivered only to its assigned gateway.
- [ ] Gateway payload must include:

```json
{
  "id": "UUIDv7",
  "child_device_id": "UUIDv7",
  "method": "read_now",
  "params": {}
}
```

- [ ] Reject child targets with no active assigned gateway. Never create a child from an RPC payload.
- [ ] Revoke child MQTT tokens when assignment occurs, using the existing gateway ownership rule.
- [ ] Run:

```bash
cargo test -p iot-ingest --test command gateway_child_command
```

Expected: PASS.

### Task 7: NanoMQ ACL, Firmware, and Operations

**Files:**
- Modify: `crates/iot-api/src/routes.rs`
- Modify: `firmware/esp32/src/main.cpp`
- Modify: `firmware/esp32/include/device_config.h`
- Modify: `firmware/esp32/test/test_device_config/test_main.cpp`
- Modify: `infra/nanomq/nanomq.conf`
- Modify: `README.md`
- Modify: `docs/operations.md`

- [ ] Write failing firmware tests for RPC subscription after MQTT connect and duplicate `command_id` suppression.
- [ ] Add NanoMQ ACL rules:

```text
device token: subscribe virtual direct-device RPC only
gateway token: subscribe virtual gateway RPC only
service account: publish validated internal downlink routes only
```

- [ ] Firmware subscribes after `MQTT_EVENT_CONNECTED`, validates `expires_at`, and persists the last processed IDs before executing `reboot`.
- [ ] Firmware does not publish a response for one-way RPC.
- [ ] Document that `published_to_broker` is not device execution and that two-way RPC requires an explicit authenticated response.
- [ ] Run:

```bash
pio test -d firmware/esp32 -e native
cargo test --workspace
```

Expected: PASS.

### Task 8: End-to-End Verification

**Files:**
- Create: `crates/iot-ingest/tests/rpc_e2e.rs`
- Modify: `scripts/e2e-local.sh`

- [ ] Start NanoMQ, selected transport adapter, `iot-api`, and `iot-ingest` with two direct simulated devices.
- [ ] Assert command isolation, one-way no-response behavior, broker publication lifecycle, offline expiry, token revoke, token rotation, gateway-child ownership, and SQLite/Timescale parity.
- [ ] Run:

```bash
scripts/e2e-local.sh --rpc
```

Expected: PASS with explicit assertions that no command for device A reaches device B.

## Self-Review

- The plan does not assume HTTP auth/ACL can virtualize a literal MQTT topic.
- The session-routing capability gate occurs before changing firmware or command APIs.
- All persistent work is in the selected database outbox; telemetry stream remains dedicated to telemetry and alert fan-out.
- One-way commands never report business execution success.
- Every task has a focused test command and an observable expected result.

## Execution Handoff

Plan saved to `docs/superpowers/plans/2026-09-07-thingsboard-style-virtual-rpc.md`.

Execution choices:

1. **Subagent-Driven**: implement one task at a time with review gates.
2. **Inline Execution**: execute the plan in this session with checkpoints.
