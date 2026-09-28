# Device Self-Claim Pairing

## Goal

An unassigned, MQTT-authenticated device can request a short-lived pairing
code after a physical pairing action. A regular User enters the device serial
number and that code in `/app` or PowerMonitor to become the device owner.

## Policy and Credentials

Each tenant has one claim policy:

- `enabled`, default `true`;
- `ttl_seconds`, default `900`, range `60..=86_400`;
- `code_length`, fixed at `6` numeric digits;
- `max_failed_attempts`, default `5`, range `1..=20`;
- `request_cooldown_seconds`, default `30`, range `10..=3_600`.

Each device has at most one active claim code. The database stores only an
Argon2 hash, issuance/expiry timestamps, failed-attempt count, and consumed or
revoked state. Raw codes are never written to audit data, logs, URLs, or
management responses.

## MQTT Contract

After the local physical pairing action, an authenticated direct device
publishes QoS 1 JSON to `v1/devices/me/pairing/request`:

```json
{"request_id":"UUIDv7"}
```

The device subscribes to `v1/devices/me/pairing/response/+`. The broker
responds non-retained at
`v1/devices/me/pairing/response/{request_id}`. Success returns `status`, the
authenticated `device_id`, raw `code`, and `expires_at`; failure returns a
non-secret rejection reason. The device ID in a payload is never accepted as
identity. A device with an owner, disabled tenant policy, or active cooldown
does not receive a code.

Each new request revokes the prior active code after the cooldown. The device
is responsible for showing the one response in RAM and a local expiry
countdown.

## Claim and Authorization

`claim_devices` is a User capability. New regular Users receive
`claim_devices`, `create_assets`, `control_devices`, and
`share_owned_resources`; they do not receive `create_devices` or other
elevated capabilities by default.

Claim is a user-session mutation, available via the monolith `/app` form and
the PowerMonitor public API/BFF. It resolves the serial number within the
caller tenant, then atomically verifies device ownership state,
code hash/expiry/attempt budget, and then sets
`owner_user_id` and `claimed_at`, consumes the code, clears obsolete resource
shares, and records an audit event. The new owner may view, control, and share
that claimed device as resource-scoped owner rights. Claim does not grant
global `control_devices` or `share_owned_resources` permissions, and it does
not reveal or rotate the MQTT device token.

## Tenant UI

`/tenant/devices/claim-policy` remains under the Devices navigation active
state. It lets a Tenant Account configure pairing lifetime, failed attempts,
and cooldown; code length is fixed at six digits. Device edit shows claim state,
generates or revokes an active code, and returns a short-lived QR payload for
the serial-plus-code claim flow.

## Verification

Focused tests cover policy validation/defaults, one active code, cooldown,
failed-attempt lockout, expiry, atomic claim and tenant isolation; MQTT topic
authorization and response delivery; capability/default-user behavior;
management/public APIs; and server-rendered plus PowerMonitor UI contracts.
