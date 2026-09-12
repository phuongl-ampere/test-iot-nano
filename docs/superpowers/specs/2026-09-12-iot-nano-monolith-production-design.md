# IoT Nano Monolith Production Design

## Status

Approved design for the `monolith` branch.

## Goal

Deliver `iot-nano-monolith` as one production binary and one application
container. It runs public HTTP, MQTT, stream processing, core workers, cache,
and command delivery in one Tokio runtime while retaining explicit module
boundaries. It supports either SQLite or TimescaleDB for platform data,
selected at startup through environment configuration.

## Decisions

- The production topology is a composition monolith, not a supervisor that
  starts legacy service binaries.
- API, Core, Stream, and MQTTD remain Rust libraries with direct typed ports.
  Internal HTTP URLs and internal service secrets do not exist in monolith
  mode.
- `IOT_NANO_STORAGE=sqlite|timescale` selects the platform-data backend before
  startup. Changing it requires an explicit offline migration or a new
  deployment; no implicit storage conversion occurs at runtime.
- Platform data includes users, sessions, devices, assets, profiles, resource
  grants, telemetry, rollups, alerts, incidents, notifications, and commands.
  Every platform-data repository supports both SQLite and TimescaleDB.
- Internal runtime state always uses local SQLite. It is separate from
  platform data and is not selected by `IOT_NANO_STORAGE`.
- External applications are not loaded into the monolith. They integrate only
  through versioned platform APIs and authorization contracts.

## Runtime Architecture

`services/iot-nano-monolith` is the composition root. It owns a cancellation
token and supervises all tasks:

1. Validate all environment values and storage paths.
2. Open and migrate the selected platform store.
3. Open `stream.sqlite`, `mqttd.sqlite`, and `cache.sqlite`.
4. Start the embedded stream processor, Core workers, command dispatcher, and
   MQTTD session router.
5. Bind public HTTP, MQTT TCP, and MQTT TLS listeners.

Only public listeners bind network addresses. In-process calls replace:

| Former boundary | Monolith port |
|---|---|
| API to Core HTTP | `CoreFacade` |
| MQTTD to Stream HTTP | `StreamPort` |
| Core to MQTTD RPC HTTP | `CommandTransport` backed by `SessionRouter` |
| MQTTD to API token authorization HTTP | `DeviceAuthorizationPort` |

The readiness endpoint is healthy only after platform migration, all internal
SQLite stores, stream recovery, Core workers, MQTTD listeners, and the public
router are ready. Shutdown stops public listeners first, drains command and
stream work within a configured deadline, then closes stores.

## Storage

### Platform Data

The configuration contract is:

```text
IOT_NANO_STORAGE=sqlite|timescale
IOT_NANO_SQLITE_PATH=/var/lib/iot-nano/platform.sqlite
DATABASE_URL=postgres://...
```

Exactly one backend is valid. SQLite mode opens `platform.sqlite` with WAL,
foreign keys, and a busy timeout. Timescale mode opens one PostgreSQL pool and
uses the `iot_nano` schema; telemetry remains a Timescale hypertable while
metadata and operational tables live in the same schema.

Repository traits own dialect differences. HTTP handlers, Core workers, and
external-app APIs use domain operations, not backend-specific SQL. SQLite
backup is an explicit filesystem snapshot before an upgrade. Timescale
migrations use a transactional migration lock. A migration failure prevents
all public listeners from binding.

### Internal Runtime Data

The monolith always owns these files under `IOT_NANO_INTERNAL_DIR`:

```text
stream.sqlite  # durable stream records, consumer offsets, idempotency state
mqttd.sqlite   # broker sessions, retained messages, QoS state
cache.sqlite   # persistent cache entries and local checkpoints
```

The hot cache remains in memory and is backed by `cache.sqlite` when a value
must survive restart. Separate files avoid contention between synchronous MQTT
QoS writes, stream commits, and cache maintenance. `stream.sqlite` is durable:
MQTTD sends `PUBACK` or `PUBCOMP` only after its event commit succeeds.

Both storage modes support one active monolith instance. TimescaleDB improves
database capacity and availability, but does not make the local MQTT session
or stream state multi-replica.

## Data Flow

```text
device -> MQTTD -> device authorization -> stream.sqlite commit -> MQTT ACK
       -> Core worker -> selected platform store

browser -> public API -> authorization -> CoreFacade -> selected platform store

Core command -> SessionRouter -> active MQTT device session
```

Core acknowledges a stream record only after the corresponding platform-store
transaction and idempotency record commit. A failed worker transaction leaves
the stream record available for retry.

## External Application Model

The monolith stores an application registry with:

```text
app_id, kind, launch_url, redirect_uris, client_id, allowed_scopes, enabled
```

`kind` is either `frontend` or `full_stack`.

- A frontend-only app uses OAuth authorization code flow with PKCE and calls
  `/api/v1/...` with the signed-in user's scoped access token.
- A full-stack app has its own frontend and backend deployment. Its backend
  uses a confidential app client or service credential to call
  `/api/v1/...`; the frontend uses a user authorization flow.
- The app backend owns application-specific aggregation, workflows, APIs, and
  optional app database. It never receives direct access to platform or
  internal SQLite files or TimescaleDB.

PowerMonitor is a full-stack external app. Its current app-specific routes and
data shaping move out of the monolith. The monolith exposes generic,
versioned device, asset, telemetry, alert, command, and authorization APIs.
Simple applications may be frontend-only and require no backend.

## Deployment

The application image contains one binary:

```text
iot-nano-monolith
```

It exposes public HTTP, MQTT TCP, and MQTT TLS ports. Management binds
loopback or a separately protected network. SQLite deployment mounts a
platform-data volume and an internal-state volume. Timescale deployment mounts
only the internal-state volume and receives `DATABASE_URL`.

There is no Compose dependency on internal API, Core, Stream, or MQTTD
containers. TimescaleDB remains an optional external deployment dependency.

## Verification

The monolith implementation must add:

- backend-contract tests that execute the same platform repository behavior on
  SQLite and TimescaleDB;
- startup tests that reject incomplete or contradictory storage environment
  values before binding public listeners;
- one-process integration tests for MQTT publish, durable stream commit,
  telemetry query, alert evaluation, command dispatch, and graceful shutdown;
- external-app contract tests for PKCE frontend access, full-stack service
  credentials, scope denial, and absence of direct database access;
- SQLite backup/recovery tests and Timescale migration-lock tests.

## Non-Goals

- Loading external app code, routes, or database migrations into
  `iot-nano-monolith`.
- Automatically migrating live microservice state while both topologies run.
- Multiple active monolith replicas sharing MQTT sessions or local stream
  state.
