# Gateway Child Devices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Support token-authenticated gateways that report telemetry and lifecycle state for pre-assigned child devices without weakening direct-device security.

**Architecture:** `iot-core` owns the gateway topic and payload contract. `iot-api` owns gateway assignment, token eligibility, NanoMQ auth/ACL, and read models. `iot-ingest` authorizes gateway payloads before appending child-keyed events to `iot-stream`; the writer persists gateway provenance and existing direct events remain unchanged.

**Tech Stack:** Rust, Axum, SQLx/PostgreSQL/TimescaleDB, `iot-stream`, Next.js, TypeScript, Vitest.

## Global Constraints

- Keep `v1/devices/me/telemetry` behavior unchanged for direct devices.
- Accept only QoS 1 gateway protocol messages.
- Never create a device from gateway payload data.
- A child has exactly one active gateway in v1.
- Child token issuance or rotation must fail; assigning a child must revoke its active token atomically.
- Store gateway telemetry in UTC with original `event_at`.
- Use `boot_id` and `sequence` for replay-safe event identity.
- Do not run database tests against the active development database; use
  `postgres://iot:iot@127.0.0.1:54329/iot_gateway_test`.

---

### Task 1: Schema And Gateway Protocol Contract

**Files:**
- Create: `db/migrations/0010_gateway_child_devices.sql`
- Modify: `crates/iot-ingest/src/writer.rs`
- Modify: `crates/iot-core/src/lib.rs`
- Create: `crates/iot-core/src/gateway.rs`
- Test: `crates/iot-core/tests/gateway.rs`
- Test: `crates/iot-ingest/tests/writer.rs`

**Interfaces:**
- Produces `GatewayTelemetryPayload`, `GatewayChildLifecyclePayload`,
  `GatewayTelemetryKind`, and the three gateway topic constants.
- Extends `TelemetryEvent` with
  `gateway_device_id: Option<String>`.

- [x] **Step 1: Write failing gateway protocol tests**

```rust
#[test]
fn child_telemetry_builds_an_event_for_the_assigned_child() {
    let event = child_payload().into_event("gateway-001").unwrap().unwrap();
    assert_eq!(event.device_id, "child-001");
    assert_eq!(event.gateway_device_id.as_deref(), Some("gateway-001"));
}

#[test]
fn heartbeat_has_no_child_telemetry_event() {
    assert_eq!(heartbeat_payload().into_event("gateway-001").unwrap(), None);
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p iot-core --test gateway`

Expected: FAIL because the gateway module and payload types do not exist.

- [x] **Step 3: Add the migration and core types**

```sql
ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS is_gateway BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS gateway_device_id TEXT REFERENCES devices(device_id) ON DELETE RESTRICT,
    ADD COLUMN IF NOT EXISTS gateway_last_read_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS gateway_read_quality TEXT
        CHECK (gateway_read_quality IN ('good', 'unavailable'));

DO $$
BEGIN
    ALTER TABLE devices
        ADD CONSTRAINT devices_gateway_parent_check
        CHECK (
            (is_gateway = TRUE AND gateway_device_id IS NULL)
            OR (is_gateway = FALSE AND gateway_device_id IS DISTINCT FROM device_id)
        );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE telemetry
    ADD COLUMN IF NOT EXISTS gateway_device_id TEXT;
```

Define tagged `heartbeat` and `child_telemetry` gateway payload variants with
schema version 1, UUID boot ID, sequence, UTC event timestamp, and a child ID
only for child telemetry. Make `child_telemetry` produce `Some(TelemetryEvent)`
with gateway provenance and heartbeat produce `None`.

- [x] **Step 4: Extend the writer**

Persist `TelemetryEvent.gateway_device_id` in `telemetry.gateway_device_id`
while preserving direct-event `NULL` values and current `last_seen_at`
behavior.

- [x] **Step 5: Run focused tests**

Run: `cargo test -p iot-core --test gateway`

Expected: PASS.

- [x] **Step 6: Run writer tests with a disposable database**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-ingest --test writer`

Expected: PASS, including direct telemetry persistence.

### Task 2: Gateway Assignment, Token Eligibility, And NanoMQ ACL

**Files:**
- Modify: `crates/iot-api/src/device_tokens.rs`
- Modify: `crates/iot-api/src/routes.rs`
- Modify: `crates/iot-api/src/powermonitor.rs`
- Test: `crates/iot-api/tests/api.rs`

**Interfaces:**
- Extends `ManagementDevice` and `UpdateManagementDeviceRequest` with
  `is_gateway`, `gateway_device_id`, `gateway_status`, and `child_status`.
- Exposes an internal active-token resolver with device ID, gateway flag, and
  parent gateway ID.

- [x] **Step 1: Write failing API integration tests**

```rust
#[tokio::test]
async fn assigning_a_child_revokes_its_token_and_blocks_token_rotation() {
    // Provision a gateway and a direct device, assign the device to the
    // gateway, then assert its former MQTT token is rejected and rotation is
    // forbidden.
}

#[tokio::test]
async fn nanomq_allows_gateway_topics_only_for_gateway_tokens() {
    // Assert the gateway token receives 200 for all three gateway topics and
    // a direct token receives 403 for them.
}
```

- [x] **Step 2: Run the targeted API tests to verify failure**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-api --test api assigning_a_child`

Expected: FAIL because gateway fields and ACL rules do not exist.

- [x] **Step 3: Implement atomic assignment**

In `update_management_device`, lock the target and selected parent device,
require an active parent gateway, reject self-parenting and gateway children,
update `is_gateway` and `gateway_device_id`, and revoke active target tokens
when the parent is non-null. Return a conflict for an invalid topology.

- [x] **Step 4: Enforce token eligibility**

Lock the device row in token create/rotate/provision paths. Reject a row whose
`gateway_device_id` is non-null. Preserve direct-device and gateway token
issuance.

- [x] **Step 5: Restrict NanoMQ authentication and ACL**

Resolve token ownership once. Allow direct-device telemetry only for
non-gateway devices, allow all three gateway topics only for gateway devices,
and retain service-account command ACL behavior.

- [x] **Step 6: Add gateway-aware API read models**

Derive gateway `online` from `last_seen_at`. Derive child `fresh` under five
minutes, `stale` under fifteen minutes, and `unavailable` otherwise or after
an explicit unavailable quality. Preserve the existing boolean `online` for
existing callers.

- [x] **Step 7: Run focused API tests**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-api --test api`

Expected: PASS.

### Task 3: Strict Gateway Webhook Ingestion

**Files:**
- Modify: `crates/iot-ingest/src/webhook.rs`
- Test: `crates/iot-ingest/tests/webhook.rs`

**Interfaces:**
- `WebhookWorker` accepts the existing direct topic and the three gateway
  topics.
- Gateway telemetry appends a child-keyed `TelemetryEvent` with
  `gateway_device_id`.

- [x] **Step 1: Write failing webhook tests**

```rust
#[tokio::test]
async fn gateway_child_telemetry_is_written_for_an_assigned_child() {
    // Set up a gateway token and assigned child, send the gateway payload,
    // drain the writer, and assert child telemetry carries gateway provenance.
}

#[tokio::test]
async fn gateway_cannot_write_an_unassigned_child() {
    // Send a valid token with a child owned by another gateway and assert no
    // stream record is appended.
}
```

- [x] **Step 2: Run tests to verify failure**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-ingest --test webhook`

Expected: FAIL because the worker rejects every non-direct topic.

- [x] **Step 3: Add token and ownership resolution**

Under the existing transaction, resolve and verify the token, require
`is_gateway = TRUE` for gateway topics, and require an active child whose
`gateway_device_id` equals the resolved gateway. Do not insert devices.

- [x] **Step 4: Handle protocol events**

For a heartbeat or child lifecycle message, update the gateway's
`last_seen_at`. A gateway child telemetry event also updates
`gateway_last_read_at` and quality `good`; a disconnect marks quality
`unavailable`. Reject malformed, unsupported-schema, non-QoS-1, future-dated,
or foreign-child messages before stream append.

- [x] **Step 5: Preserve direct flow**

Keep direct token resolution and `DeviceTelemetryPayload` validation on
`v1/devices/me/telemetry`. A gateway token must not publish the direct topic.

- [x] **Step 6: Run focused tests**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-ingest --test webhook`

Expected: PASS.

### Task 4: Management And Power Monitor UI

**Files:**
- Modify: `web/lib/api.ts`
- Modify: `web/components/management-panels.tsx`
- Modify: `web/components/powermonitor-dashboard.tsx`
- Modify: `web/components/powermonitor-tree.tsx`
- Modify: `web/app/globals.css`
- Test: `web/components/device-drawer.test.tsx`
- Test: `web/components/powermonitor-tree.test.tsx`
- Test: `web/lib/api.test.ts`

**Interfaces:**
- `ManagementDevice` and `PowerDevice` include gateway fields and derived
  status strings.
- Device drawer receives a list of gateway candidates.

- [x] **Step 1: Write failing UI and API-client tests**

```tsx
it("lets an administrator assign a non-gateway device to a gateway", () => {
  render(<DeviceDrawer device={child} gateways={[gateway]} {...props} />);
  expect(screen.getByLabelText("Gateway")).not.toBeNull();
});

it("does not render token controls for a gateway child", () => {
  render(<DeviceDrawer device={child} gateways={[gateway]} {...props} />);
  expect(screen.queryByText("Device token")).toBeNull();
});
```

- [x] **Step 2: Run UI tests to verify failure**

Run: `npm test -- components/device-drawer.test.tsx components/powermonitor-tree.test.tsx lib/api.test.ts`

Expected: FAIL because gateway fields and controls do not exist.

- [x] **Step 3: Extend client types and request bodies**

Add `is_gateway`, `gateway_device_id`, `gateway_status`, and `child_status`
to returned devices. Include topology fields in management device updates.

- [x] **Step 4: Add management controls**

Use a three-option connectivity selector: direct device, gateway, and gateway
child. A child selects only an existing gateway. Hide `DeviceTokenPanel` for
children and show its assigned gateway ID/name and derived state.

- [x] **Step 5: Update Power Monitor presentation**

Render gateway and child state labels in the tree and selected-device header.
Keep the current asset hierarchy and direct-device online marker intact.

- [x] **Step 6: Run web verification**

Run: `npm test`

Expected: PASS.

- [x] **Step 7: Run production build**

Run: `npm run build`

Expected: PASS.

### Task 5: Protocol Documentation And Final Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-09-07-gateway-child-devices-design.md`

- [x] **Step 1: Document configuration and protocol**

Add the gateway topics, token-only authentication rules, no-auto-provisioning
rule, and local SQLite WAL outbox requirement to the README. Do not include
hardware-specific Modbus, BLE, RS485, or GPIO implementation details.

- [x] **Step 2: Verify static Rust packages**

Run: `cargo test -p iot-core`

Expected: PASS.

- [x] **Step 3: Verify application suites against a disposable database**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-api --test api`

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot_gateway_test cargo test -p iot-ingest --test webhook`

Expected: PASS.

- [x] **Step 4: Verify the running application**

Run: `curl --fail http://localhost:3000/apps/powermonitor`

Expected: HTTP 200.
