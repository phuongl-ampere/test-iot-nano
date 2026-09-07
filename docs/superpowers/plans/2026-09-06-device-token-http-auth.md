# Device Token HTTP Auth Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Replace static per-device MQTT credentials and ACL files with opaque device tokens, NanoMQ HTTP auth/ACL, and an authenticated webhook into the durable stream.

**Architecture:** A device MQTT username is a random 256-bit token and its password must be empty. NanoMQ calls `iot-api` for token authentication and authorization, then forwards only accepted publish events to `iot-ingest`; ingest resolves the token a second time, attaches the internal ID, and appends to `iot-stream`.

**Tech Stack:** Rust 2024, Axum 0.8, SQLx/PostgreSQL/TimescaleDB, Argon2, NanoMQ 0.25.6, Next.js 16, ESP32 Arduino.

## Global Constraints

- Device tokens are separate from dashboard admin/viewer tokens.
- Store device tokens only as Argon2 hashes and return plaintext only on create or rotate.
- Require empty MQTT password and exact `v1/devices/me/telemetry` QoS 1 publish.
- Require separate `IOT_NANOMQ_AUTH_SECRET` and `IOT_NANOMQ_WEBHOOK_SECRET`.
- Use TLS at port `8883` in production; do not expose device MQTT over plaintext.
- Revocation must reject the next webhook even if NanoMQ has cached a broker auth result.

### Task 1: Shared Device Token Domain

**Files:**
- Create: `crates/iot-core/src/device_token.rs`
- Modify: `crates/iot-core/src/lib.rs`, `crates/iot-core/src/telemetry.rs`, `crates/iot-core/Cargo.toml`
- Test: `crates/iot-core/tests/device_token.rs`

- [x] **Step 1: Write failing tests for random tokens and device-ID-free payload mapping**

```rust
assert!(generate_device_token().starts_with("iotd_"));
assert_ne!(generate_device_token(), generate_device_token());
assert_eq!(
    serde_json::from_str::<DeviceTelemetryPayload>(PAYLOAD)?
        .into_event("esp-000123")?
        .device_id,
    "esp-000123"
);
```

- [x] **Step 2: Run `cargo test -p iot-core --test device_token` and confirm it fails.**

- [x] **Step 3: Implement `generate_device_token`, Argon2 hash/verify helpers, the exact `DEVICE_TELEMETRY_TOPIC`, and payload-to-event mapping that rejects client-supplied device IDs.**

- [x] **Step 4: Run `cargo test -p iot-core`.**

### Task 2: Device Token Lifecycle and NanoMQ API

**Files:**
- Create: `db/migrations/0004_device_tokens.sql`, `crates/iot-api/src/device_tokens.rs`
- Modify: `crates/iot-ingest/src/writer.rs`, `crates/iot-api/src/lib.rs`, `crates/iot-api/src/main.rs`, `crates/iot-api/src/routes.rs`
- Test: `crates/iot-api/tests/api.rs`

- [x] **Step 1: Write failing integration tests for create/list/rotate/revoke and NanoMQ auth/ACL.**

```rust
let token = create_device_token(&app, "esp-000123").await;
assert_eq!(nanomq_auth(&app, &token, "").await, StatusCode::OK);
assert_eq!(nanomq_auth(&app, &token, "not-empty").await, StatusCode::UNAUTHORIZED);
assert_eq!(nanomq_acl(&app, &token, "publish", DEVICE_TELEMETRY_TOPIC).await, StatusCode::OK);
assert_eq!(nanomq_acl(&app, &token, "subscribe", DEVICE_TELEMETRY_TOPIC).await, StatusCode::FORBIDDEN);
revoke_device_token(&app, &token).await;
assert_eq!(nanomq_auth(&app, &token, "").await, StatusCode::UNAUTHORIZED);
```

- [x] **Step 2: Run the targeted API test with `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot` and confirm it fails.**

- [x] **Step 3: Add `device_tokens` storage with UUID id, device ID, unique token prefix, token hash, created/last-used/revoked timestamps, and one active token per device.**

- [x] **Step 4: Implement admin token routes and NanoMQ-only form endpoints. Both NanoMQ routes verify `IOT_NANOMQ_AUTH_SECRET`; auth requires an empty password; ACL permits only the exact device publish operation.**

- [x] **Step 5: Run `cargo test -p iot-api --test api -- --test-threads=1`.**

### Task 3: Authenticated Webhook to Stream

**Files:**
- Create: `crates/iot-ingest/src/webhook.rs`, `crates/iot-ingest/tests/webhook.rs`
- Modify: `crates/iot-ingest/src/lib.rs`, `crates/iot-ingest/src/main.rs`, `crates/iot-ingest/Cargo.toml`

- [x] **Step 1: Write failing webhook tests for valid token to stream to TimescaleDB, invalid secret, bad topic, payload with device ID, revoked token, and rotated token.**

```rust
let response = fixture.webhook(active_token, telemetry_without_device_id()).await;
assert_eq!(response.status(), StatusCode::NO_CONTENT);
fixture.flush_writer_once().await;
assert_eq!(fixture.telemetry_count("esp-000123").await, 1);
```

- [x] **Step 2: Run `cargo test -p iot-ingest --test webhook` and confirm it fails.**

- [x] **Step 3: Implement the webhook route. Validate the secret, `message_publish`, topic, QoS, timestamp and token. Resolve only non-revoked hashes, update `last_used_at`, build an internal telemetry event, and append in `spawn_blocking`.**

- [x] **Step 4: Start the ingest HTTP router only after the database pool and stream exist, and remove the production MQTT telemetry subscriber from `main`.**

- [x] **Step 5: Run `cargo test -p iot-ingest --test webhook -- --test-threads=1`.**

### Task 4: NanoMQ and Deployment Configuration

**Files:**
- Modify: `infra/nanomq/nanomq.conf`, `infra/nanomq/nanomq.dev.conf`, `infra/compose.yaml`, `infra/dev/ingest.env`, `infra/systemd/iot-api.service`, `infra/systemd/iot-ingest.service`, `scripts/install-raspberry-pi.sh`, `docs/operations.md`, `README.md`
- Delete: `infra/nanomq/passwords.conf.example`, `infra/nanomq/acl.conf.example`

- [x] **Step 1: Configure NanoMQ 0.25.6 HOCON `auth.http_auth` form endpoints with username `%u`, password `%P`, access `%A`, topic `%t`, the auth secret header, and `cache_ttl = 0s`.**

- [x] **Step 2: Configure `webhook` with the webhook secret header and only `on_message_publish` for `v1/devices/me/telemetry`.**

- [x] **Step 3: Disable public TCP port 1883 in production, enable `listeners.ssl` on 8883, and document cert/key/CA file paths plus the two secret values.**

- [x] **Step 4: Validate the production NanoMQ configuration with the local `emqx/nanomq:0.25.6` image.**

### Task 5: Admin Dashboard Token Controls

**Files:**
- Create: `web/components/device-token-panel.tsx`, `web/components/device-token-panel.test.tsx`
- Modify: `web/lib/api.ts`, `web/components/dashboard.tsx`, `web/app/globals.css`

- [x] **Step 1: Write failing component tests for admin create/copy/rotate/revoke and no viewer controls.**

- [x] **Step 2: Run `npm --prefix web test -- components/device-token-panel.test.tsx` and confirm it fails.**

- [x] **Step 3: Add token API client types and a compact selected-device panel. Show plaintext only from create/rotate response in a read-only field with a Copy icon.**

- [x] **Step 4: Run `npm --prefix web test` and `npm --prefix web run build`.**

### Task 6: ESP32 Token-only TLS Telemetry

**Files:**
- Modify: `firmware/esp32/platformio.ini`, `firmware/esp32/include/device_config.h`, `firmware/esp32/src/main.cpp`, `firmware/esp32/test/test_device_config/test_main.cpp`, `firmware/esp32/README.md`

- [x] **Step 1: Write failing native tests that require host, port 8883, device token, and CA certificate, and expect `v1/devices/me/telemetry`.**

- [x] **Step 2: Run `pio test -d firmware/esp32 -e native` and confirm it fails.**

- [x] **Step 3: Replace username/password and device ID provisioning fields with device token and CA PEM. Use a certificate-aware MQTT transport that calls `setCACert`, uses TLS, sends the token as username with an empty password, and never inserts device ID in the telemetry payload.**

- [x] **Step 4: Run the native firmware tests and build `pio run -d firmware/esp32 -e esp32dev`.**

### Task 7: End-to-end Verification

- [x] **Step 1: Start TimescaleDB and NanoMQ with `docker compose --file infra/compose.yaml up --detach`.**
- [x] **Step 2: Run `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test --workspace -- --test-threads=1`.**
- [x] **Step 3: Run `npm --prefix web test` and `npm --prefix web run build`.**
- [x] **Step 4: Exercise NanoMQ auth, ACL, webhook, token revoke, and rotate against the local services; confirm only active tokens append telemetry.**
