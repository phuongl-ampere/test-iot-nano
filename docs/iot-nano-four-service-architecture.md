# IoT Nano Four-Service Architecture

Status: development migration in progress
Updated: 2026-09-11
Authority: this is the only architecture, status, and delivery-plan document
for the runtime.

## Development Rules

This is a development-stage migration. Backward compatibility is out of scope.

- No fallback, side-by-side broker, compatibility flag, legacy secret, legacy
  route, or legacy binary is allowed after a phase lands.
- No production-data migration is required. Development data is disposable.
- A boundary or schema change resets its owned development state instead of
  preserving or translating old rows, segments, offsets, or broker state.
- Before a reset, stop the affected services. Delete their SQLite files and
  Stream directory, or drop and recreate the development database schema.
  Restarting services must run only current migrations and bootstrap data.
- Never reset a non-development environment without an explicit, separate
  approval.

## Target Runtime

The platform runs exactly four independently deployable Rust services:

```text
Device or generic MQTT client
  -> iot-nano-mqttd
     -> iot-nano-api: token and session resolution
     -> iot-nano-stream: durable event append

iot-nano-stream
  -> iot-nano-core: named consumer groups

iot-nano-core
  -> telemetry storage, rollups, alerts, notification outbox, commands

Browser or integration client
  -> iot-nano-api
     -> iot-nano-core: telemetry, alert, and command APIs
     -> iot-nano-mqttd: internal RPC dispatch
```

`iot-nano-mqttd` is the only MQTT broker. Retired brokers, predecessor ingress
processes, in-process Stream compatibility, and legacy MQTT ingress are not
supported runtime modes.

The MQTT acknowledgement boundary is durable Stream acceptance:

```text
device PUBLISH
  -> MQTTD authenticates and authorizes
  -> Stream accepts the durable record
  -> MQTTD emits PUBACK or PUBCOMP
```

MQTTD never writes telemetry directly to a Core business database.

## Service Ownership

| Service | Owns | Does not own |
|---|---|---|
| `iot-nano-mqttd` | MQTT TCP/TLS/WebSocket listeners, generic broker state, device and gateway sessions, token transport, topic ACLs, MQTT RPC delivery | telemetry, alert, or API metadata databases |
| `iot-nano-stream` | append-only segment directory, retention, stream partitions, named consumer groups, append/claim/ack HTTP API | telemetry normalization and business state |
| `iot-nano-core` | telemetry normalization and idempotency, telemetry storage, rollups, alerts, notification outbox, command state, Stream consumer offsets | users, sessions, device metadata, and token issuance |
| `iot-nano-api` | public HTTP API, users, sessions, device metadata, assets, profiles, token lifecycle, API authorization | telemetry, alert, outbox, and command-processing storage |

Every internal HTTP request uses an independent service secret. A service
cannot open or write another service's SQLite file.

## Storage Boundaries

```text
iot-nano-api   -> users, sessions, device metadata, assets, profiles, tokens
iot-nano-core  -> telemetry, rollups, alerts, outbox, command state
iot-nano-stream -> segment log and consumer-group state
iot-nano-mqttd -> broker state only
```

SQLite deployments use separate absolute database paths for API and Core.
Timescale/Postgres deployments retain the same logical ownership through
service APIs and migrations. MQTTD SQLite broker persistence is service-owned
and covered by restart acceptance tests.

### Development Reset

The API, Core, Stream, and MQTTD states are a single disposable development
unit. When the migration changes an owned schema or contract:

1. Stop all four services and the local database.
2. Delete API/Core SQLite files, Stream segments and group offsets, and MQTTD
   broker state; or drop and recreate the development Timescale/Postgres
   schema.
3. Start the current database migration and API bootstrap from an empty state.
4. Start Stream, Core, API, and MQTTD in dependency order.
5. Run the clean-start E2E suite before retaining the new state.

There is no old-schema reader, data importer, dual writer, or compatibility
deployment mode.

## Contracts

The versioned source contracts are under `contracts/`:

| Contract | Purpose |
|---|---|
| `telemetry-v1.json` | normalized direct-device telemetry |
| `stream-v1.json` | durable append, claim, and acknowledgement records |
| `rpc-v1.json` | device RPC commands and responses |
| `internal-api-v1.json` | authenticated service-to-service requests |
| `gateway-telemetry-v1.json` | authorized gateway lifecycle and child telemetry |

The gateway envelope must contain the gateway device ID, child device ID when
applicable, authenticated token/session identity, event kind, event time,
payload, and idempotency key. MQTTD or API resolves identity before append;
Core consumes an already-authorized event and must not call back into API to
reconstruct authorization.

`internal-api-v1.json` must be rewritten for the target runtime. It needs
distinct authenticated APIs for MQTTD-to-API session and gateway authorization,
MQTTD-to-Stream append, Core-to-Stream group operations, and API-to-Core
command, telemetry, rollup, alert, notification, and query operations. No
predecessor service names, paths, or headers are retained.

## Current Baseline

Implemented source packages:

```text
services/iot-nano-mqttd
services/iot-nano-stream
services/iot-nano-core
services/iot-nano-api
```

The workspace compiles with these package names. Stream provides authenticated
append, consumer-group claim, and acknowledgement endpoints. Core can consume
the remote Stream and acknowledge only after its storage transaction. API has
an authenticated Core client for command handling. MQTTD includes the generic
broker, native MQTT 3.1.1 and MQTT 5 device transport, token-aware RPC
routing, and direct Stream production.

The migration is incomplete in two material areas:

1. API still reaches transitional shared storage for telemetry, rollups,
   alerts, and notifications rather than using Core APIs.
2. API/Core storage still shares migrations and `iot-storage` adapters.

API still opens its own telemetry and alert storage even though Core already
exposes internal control endpoints. This is migration work, not an acceptable
target-runtime exception.

## Delivery Plan

### 1. Break Legacy Boundaries and Reset Development State

Make the target names and dependencies compile without aliases:

- rename the Core Rust library from `iot_ingest` to `iot_nano_core`;
- replace every predecessor-runtime identifier with a target-runtime name;
- move shared domain DTOs into contract-specific local modules or generated
  contract types;
- split `iot-storage` into API metadata storage and Core data-plane storage,
  then remove the shared crate dependency;
- remove `iot-nano-foundation` after each service owns its local implementation or
  contract DTO;
- remove old migrations and create only target API and Core schemas.

Stop the development stack, reset all development state as described in
[Development Reset](#development-reset), then bootstrap only the target
schemas. No data migration code, old schema, fallback reader, or dual-write
test is added.

### 2. Replace Gateway Fallback with a Stream Contract

Create `contracts/gateway-telemetry-v1.json`. It must validate gateway ID,
optional child ID, authenticated token/session ID, event kind, event time,
payload, and an idempotency key. Reject missing or contradictory identities,
unknown event kinds, an unauthorized child, and duplicate payloads.

Add an authenticated MQTTD-to-API gateway authorization endpoint. It resolves
the gateway token, verifies child ownership and topic permission, and returns
only the identity required for the Stream envelope. MQTTD appends both direct
and gateway telemetry to Stream; it does not call Core for ingress.

Core consumes the new envelope through its named group, commits its storage
transaction and idempotency record, then acknowledges Stream. Delete the Core
webhook module, its webhook endpoints, secrets, inbox directory, tests, and
all hybrid uplink fallback behavior.

### 3. Complete the API/Core Boundary

Rewrite `internal-api-v1.json` and extend the API client from
`CoreCommandClient` to a target-named Core client. Add authenticated Core
operations for telemetry, rollups, alerts, notification state, and command
lifecycle, with request/response schemas, pagination, validation errors, and
timeout/error mapping.

Inventory every public API route that directly opens telemetry, rollup, alert,
notification, or command tables. Move each route to the Core client while API
retains user, session, device metadata, asset, profile, token, and public
authorization ownership. Delete direct data-plane queries from API.

The reset creates separate API and Core schemas from scratch. Tests must prove
that API cannot start against Core storage, Core cannot start against API
storage, and public API queries work only through authenticated Core APIs.

### 4. Define the MQTTD Production Profile

Keep only explicitly supported capabilities in the target configuration:
MQTT 3.1.1 and MQTT 5 over TCP/TLS, QoS 0/1/2, retained messages, persistent
sessions, token authentication, generic ACLs, direct/gateway telemetry, and
one-way/two-way RPC.

MQTTD must provide a ThingsBoard-style virtual device RPC capability. A device
subscribes to a `me`-scoped wildcard request namespace and receives only
commands mapped to its active token-authenticated session. The platform maps
the matching response namespace back to the original command, rejects stale
or unrelated sessions, and preserves one-way and two-way command semantics.
The concrete topic spelling is an implementation contract, not an architecture
constraint.

For WebSocket, bridge, webhook, rule, shared-subscription, and last-will
features, either add an end-to-end acceptance suite or delete their
configuration and runtime code. Unsupported features must fail configuration
validation before a listener binds.

Finish the SQLite storage review and add a runtime enablement test:
`[storage] kind = "sqlite"` either starts with a valid database and advertises
the capability, or fails before listeners bind. The enabled matrix covers
retained messages, persistent sessions, offline QoS 1/QoS 2 delivery,
inbound/outbound QoS 2 recovery, expiry, duplicate packets, and a real
restart.

### 5. Deploy Only the Four Services

Remove retired deployment assets:

```text
retired broker configuration
retired ingress environment template
retired protocol spike script
all predecessor service, compose, environment, and documentation references
all compatibility variables
```

Compose must define TimescaleDB plus MQTTD, Stream, Core, and API, with
service-owned volumes, current environment templates, TLS material, and
health checks. Systemd must order Stream before Core and API, then MQTTD; each
caller must retry unavailable internal dependencies and report readiness
truthfully.

Replace the old secret model with distinct directed-channel secrets for
MQTTD-to-API, MQTTD-to-Stream, Core-to-Stream, and API-to-Core. Rename the
headers, configuration variables, code types, and tests in the same change.
MQTTD owns `1883` and `8883`; no side-by-side listener remains.

### 6. Prove the Four-Service Runtime

The E2E suite creates a disposable database, certificate, Stream directory,
and four-service process graph. It must fail when any legacy binary, legacy
environment variable, old header, old route, or shared storage path is
present. It must prove:

```text
token authentication
-> direct and gateway telemetry append
-> Core storage transaction
-> API telemetry query

device command
-> API authorization
-> Core command state
-> MQTTD delivery
-> validated device response
-> API command query
```

Add alert and notification acceptance, token rotation and revocation,
plaintext/TLS protocol parity, broker restart recovery for the selected
storage mode, and load measurements for memory, CPU, publish latency,
reconnect time, and throughput.

CI provisions the Timescale test database before database-backed Core and API
tests. No test may require an undeclared `DATABASE_URL` or an externally
running legacy broker.

## Acceptance Gates

The migration is complete only when all conditions hold:

- `cargo fmt --all -- --check` passes.
- `cargo check --workspace` passes.
- Each service's unit and integration suite passes.
- The four-service E2E suite passes with no legacy compatibility variable.
- A clean development reset creates only the current API/Core schemas, Stream
  state, and MQTTD broker state.
- MQTTD is the only broker in Compose, systemd, installer, environment
  templates, and operations documentation.
- No workspace dependency, Rust crate name, source identifier, route, header,
  environment variable, or test target retains predecessor-runtime terminology.
- API and Core use separate storage ownership in both SQLite and
  Timescale/Postgres configurations.
- Gateway and direct-device traffic use versioned Stream contracts.
- API reaches all data-plane state through the authenticated Core client.
- Compose starts all four services with health checks and service-owned state.
- Each internal channel has a distinct service secret and target-runtime
  header name.
- Every enabled MQTTD capability has a named acceptance test.

## Required Verification Commands

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test -p iot-nano-mqttd -- --test-threads=1
cargo test -p iot-nano-stream -- --test-threads=1
cargo test -p iot-nano-core -- --test-threads=1
cargo test -p iot-nano-api -- --test-threads=1
scripts/e2e-local.sh
```

Database-backed Core and API tests require their configured test database.
The test harness provisions that database itself. The E2E script must fail if
a retired binary, old header, old route, shared data-plane storage, or any
compatibility environment variable is present.
