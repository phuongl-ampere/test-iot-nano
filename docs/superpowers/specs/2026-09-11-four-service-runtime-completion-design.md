# Four-Service Runtime Completion Design

## Goal

Complete the development-stage migration to exactly four deployable services:
`iot-nano-api`, `iot-nano-core`, `iot-nano-stream`, and
`iot-nano-mqttd`.

## Decisions

- This is a clean development migration. No compatibility mode, data
  migration, fallback reader, dual write, or side-by-side broker remains.
- API owns only identity and metadata. Core owns telemetry, rollups, alerts,
  notification state, and commands. Stream owns segments and group offsets.
  MQTTD owns broker persistence and active sessions.
- Internal calls use four directed secrets and headers:
  `x-iot-nano-mqttd-api-secret`, `x-iot-nano-mqttd-stream-secret`,
  `x-iot-nano-core-stream-secret`, and `x-iot-nano-api-core-secret`.
- MQTTD authorizes direct and gateway device traffic through API, then appends
  versioned envelopes to Stream. Core never receives a broker webhook.
- API calls Core for all data-plane reads and command lifecycle operations.
  It retains only API metadata queries and authorization checks.
- Unsupported MQTTD features are removed from configuration and startup rather
  than exposed without acceptance coverage.

## Data Flow

```text
device -> MQTTD -> API authorization -> Stream append -> Core group claim
       -> Core transaction -> Stream acknowledgement

browser -> API authorization -> Core internal API -> Core data-plane state
browser command -> API -> Core command state -> MQTTD RPC delivery
```

Gateway envelopes include gateway identity, optional child identity, token or
session identity, event kind, event time, payload, and an idempotency key.
MQTTD resolves the identity before append. Core validates and stores an
already-authorized Stream message.

## Storage

API and Core receive independently configured SQLite paths. In Postgres, each
service runs only its own schema migrations and uses distinct schema names.
Core tables must not have a foreign key to API metadata tables; device IDs are
stable external identifiers. A development reset removes both databases,
Stream state, and MQTTD broker state before startup.

## Verification

The implementation is complete only when:

- source, config, contracts, docs, and tests contain no retired runtime
  terminology, paths, headers, or compatibility variables;
- Compose, systemd, templates, and E2E start only the four target services;
- MQTTD is the only broker and owns TCP 1883 and TLS 8883;
- service suites, a clean-reset four-service E2E, and the required formatter
  and workspace checks pass.
