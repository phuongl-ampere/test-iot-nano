# Device Self-Claim Pairing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Let an authenticated unassigned device obtain an ephemeral pairing code and let a permitted User claim ownership with its ID and code.

**Architecture:** `iot-storage` owns tenant policy, code hashing, cooldown and the atomic ownership claim. `iot-nano-mqttd` recognizes a separate authenticated pairing request/response protocol and calls a typed claim-code port. Monolith, public-v1, `/app`, and PowerMonitor expose policy, device claim, and the minimal user flow without exposing raw codes outside the device response.

**Tech Stack:** Rust, SQLx SQLite/Timescale, Axum, Askama, MQTT v3/v5 transport, Next.js/TypeScript.

## Global Constraints

- Do not reuse, reveal, or rotate MQTT device tokens for pairing.
- Raw codes only traverse the authenticated MQTT response and user claim request body; never persistence, audit, logs, query strings, or tenant UI.
- Device identity derives only from an authenticated MQTT session.
- One active code per device; all code issue/claim transitions are tenant-scoped and atomic.
- New Users default to `claim_devices` and `create_assets`; `create_devices` remains disabled.
- Claimed-device owner rights are resource-scoped; do not mutate global control/share capabilities.
- Preserve unrelated dirty worktree changes and use focused cargo/Node lanes only.

---

### Task 1: Persist policy, codes, and capability defaults

**Files:**
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Modify: `crates/iot-storage/src/contracts/resources.rs`
- Modify: `crates/iot-storage/src/contracts/mod.rs`
- Modify: `crates/iot-storage/src/management.rs`
- Modify: `crates/iot-storage/src/domain/authorization.rs`
- Modify: `crates/iot-storage/src/lib.rs`
- Test: `crates/iot-storage/tests/management_users.rs`
- Create: `crates/iot-storage/tests/device_claims.rs`

- [ ] Add failing SQLite tests for default User capabilities, policy validation, single active code, cooldown, lockout, expiry, cross-tenant rejection, and atomic owner claim.
- [ ] Add `tenant_device_claim_policies` and `device_claim_codes` in both SQL dialect blocks; add tenant/device composite foreign keys and active-code indexes.
- [ ] Add `UserCapability::ClaimDevices`; create new users with `ClaimDevices` and `CreateAssets` only.
- [ ] Define typed policy, issue result, claim input/result, and repository errors. Generate a random unambiguous code, hash it with Argon2, and return raw code only from the issue operation.
- [ ] Add storage operations `get/update_claim_policy`, `issue_device_claim_code`, `revoke_device_claim_code`, and `claim_device_with_code`.
- [ ] Run `./scripts/dev/cargo-lane.sh device-claim -- test -p iot-storage --test management_users --test device_claims`.

### Task 2: Add authenticated MQTT pairing protocol

**Files:**
- Modify: `services/iot-nano-mqttd/src/ports.rs`
- Modify: `services/iot-nano-mqttd/src/transport.rs`
- Modify: `services/iot-nano-mqttd/src/lib.rs`
- Modify: `services/iot-nano-monolith/src/runtime.rs`
- Test: `services/iot-nano-mqttd/tests/transport/virtual_rpc.rs`
- Test: `services/iot-nano-mqttd/tests/local_ports.rs`

- [ ] Add failing transport tests proving only an authenticated direct device may publish `v1/devices/me/pairing/request`, may subscribe to the pairing response filter, and receives a non-retained response whose ID matches the request.
- [ ] Define a typed `DeviceClaimCodePort` receiving authenticated tenant/device identity and request ID, plus a local storage-backed adapter.
- [ ] Permit the exact request and response topic family in v3/v5 transport. Validate UUIDv7 request IDs and JSON object payloads; reject gateway use, wrong QoS, unknown topics, and raw secrets in errors.
- [ ] On a valid request invoke the port and publish `{status, device_id, code, expires_at}` or a non-secret rejection to the live authenticated connection.
- [ ] Wire the local adapter from monolith runtime to the `PlatformStore` and run focused mqttd tests.

### Task 3: Expose claim APIs and tenant policy

**Files:**
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-api/src/public_v1.rs`
- Modify: `services/iot-nano-api/tests/public_v1.rs`
- Test: `services/iot-nano-monolith/tests/management_sessions.rs`

- [ ] Add failing router tests for Tenant policy read/update/revoke and User claim; assert session kind, tenant scope, capability, unavailable-code, and owner outcomes.
- [ ] Add management routes for policy read/update and per-device code revocation. Require a Tenant Account and document request/response schemas.
- [ ] Add `POST /api/v1/devices/claim` requiring `devices:write`, a User principal, and `claim_devices`; return only the claimed device representation.
- [ ] Change owner authorization so a User may control/share only its own claimed device resource without granting global control/share capability; other elevated mutations retain their current capability gates.
- [ ] Run focused monolith and public-v1 tests.

### Task 4: Render tenant and user claim flows

**Files:**
- Modify: `services/iot-nano-monolith/src/platform_ui.rs`
- Modify: `services/iot-nano-monolith/src/management.rs`
- Modify: `services/iot-nano-monolith/templates/platform_ui/tenant_devices.html`
- Create: `services/iot-nano-monolith/templates/platform_ui/tenant_device_claim_policy.html`
- Modify: `services/iot-nano-monolith/templates/platform_ui/user.html`
- Modify: `services/iot-nano-monolith/assets/platform-ui.css`
- Test: `services/iot-nano-monolith/tests/platform_ui_templates.rs`

- [ ] Add failing template tests for the policy form, device claim status/revoke action, User Add device form, and no raw-code rendering.
- [ ] Add `/tenant/devices/claim-policy` with Devices as the active nav item; render validated policy inputs and save/error notices.
- [ ] Add device claim status/revoke to the existing device editor.
- [ ] Add a normal POST form in `/app` that accepts only Device ID and secret code, renders it only for `claim_devices`, and redirects without a secret in the URL.
- [ ] Run the focused template and management-session tests.

### Task 5: Integrate PowerMonitor and simulator

**Files:**
- Modify: `apps/powermonitor/lib/browser-api.ts`
- Modify: `apps/powermonitor/components/powermonitor-dashboard.tsx`
- Modify: `apps/powermonitor/tests/browser-api.test.ts`
- Modify: `apps/powermonitor/tests/dashboard-contract.test.tsx`
- Modify: `debug/device_lighting_switcher_simulation.py`
- Modify: `debug/test_demo_lighting_switcher.py`

- [ ] Add failing browser API/component tests for a secret-body-only claim request and an Add device control gated by `claim_devices`.
- [ ] Add PowerMonitor claim form and refresh its workspace after success; never retain the entered code in URL or logs.
- [ ] Extend the simulator with a local pairing trigger, pairing response subscription, code display log with expiry, and no automatic polling/request loop.
- [ ] Run focused Vitest files and Python simulation tests.

### Task 6: Verify the integrated contract

**Files:**
- Test: `crates/iot-storage/tests/device_claims.rs`
- Test: `services/iot-nano-mqttd/tests/transport/virtual_rpc.rs`
- Test: `services/iot-nano-monolith/tests/management_sessions.rs`
- Test: `services/iot-nano-api/tests/public_v1.rs`
- Test: `apps/powermonitor/tests/browser-api.test.ts`
- Test: `apps/powermonitor/tests/dashboard-contract.test.tsx`

- [ ] Run focused Rust/Node/Python tests, then `cargo check -p iot-storage -p iot-nano-mqttd -p iot-nano-api -p iot-nano-monolith` through the `device-claim` lane.
- [ ] Run `git diff --check`; inspect changed paths and verify no raw code appears in audit/log/UI response sources.
