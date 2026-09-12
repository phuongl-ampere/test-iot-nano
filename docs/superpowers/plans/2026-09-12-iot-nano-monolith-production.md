# IoT Nano Monolith Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver `iot-nano-monolith`, one production Rust binary and one
application container that exposes public HTTP and MQTT while running the API,
Core, Stream, MQTTD, cache, and command delivery in one supervised Tokio
runtime.

**Architecture:** Keep `iot-nano-api`, `iot-nano-core`, `iot-nano-stream`,
and `iot-nano-mqttd` as Rust libraries. Replace their HTTP adapters with
typed in-process ports, and compose concrete adapters only in
`services/iot-nano-monolith`. A single `PlatformStore` facade selects SQLite
or TimescaleDB before any listener binds; Stream, MQTTD, and durable cache
always use separate local SQLite files in `IOT_NANO_INTERNAL_DIR`.

**Tech Stack:** Rust 2024 and Rust 1.96, Tokio, Axum, SQLx SQLite/PostgreSQL,
rusqlite, rumqttd, Tokio cancellation tokens, Next.js 16, React 19, Vitest,
Docker Compose, systemd, MQTT 3.1.1/MQTT 5, OAuth 2.1 authorization code with
PKCE, and OAuth client credentials.

## Global Constraints

- Work only from the `monolith` branch. Do not revert the user's currently
  deleted four-service documents or plans.
- The production image contains only `iot-nano-monolith`; API, Core, Stream,
  and MQTTD are library packages and are never child processes.
- Public listeners are HTTP, MQTT TCP, and MQTT TLS. Management binds only
  `IOT_NANO_MANAGEMENT_ADDRESS`, defaulting to `127.0.0.1:8081`.
- `IOT_NANO_STORAGE=sqlite|timescale` is validated before opening a listener.
  SQLite requires `IOT_NANO_SQLITE_PATH`; Timescale requires `DATABASE_URL`.
  Supplying both platform backends is an error.
- `IOT_NANO_INTERNAL_DIR` contains exactly `stream.sqlite`, `mqttd.sqlite`,
  `cache.sqlite`, and `instance.lock`. It is independent of platform data.
- `IOT_NANO_STORAGE` never changes data at runtime. Switching backend uses an
  explicit offline operation, not fallback, dual write, or automatic import.
- SQLite platform migrations create an explicit backup before upgrade.
  Timescale migrations run under one transaction-scoped advisory lock in the
  `iot_nano` schema. Migration failure prevents all public binds.
- Only one monolith instance can use an internal-state directory. Acquiring
  `instance.lock` must fail before platform migration or listener binding.
- Monolith mode contains no internal URL, service-to-service header, or
  `IOT_NANO_*_SECRET` setting. The process rejects all retired internal
  service environment variables at startup.
- Public platform APIs use `/api/v1`. No external application receives a
  platform database connection string, an internal-state path, or an internal
  API route.
- OAuth access tokens are opaque, randomly generated, SHA-256 hashed at rest,
  scope-bound, and short lived. Authorization codes are single-use, SHA-256
  hashed, expire in five minutes, and enforce S256 PKCE.
- Every behavior change starts with a focused failing test. Do not delete a
  four-service implementation or deployment asset until its monolith
  replacement has passed focused and integration tests.

## Target File Structure

| Path | Responsibility |
| --- | --- |
| `services/iot-nano-monolith/` | Composition root, runtime configuration, lifecycle supervision, durable cache adapter, and process-level tests. |
| `crates/iot-storage/` | Platform repository interfaces plus SQLite and Timescale implementations, schema, migration, backup, and migration-lock logic. |
| `services/iot-nano-api/src/` | Public HTTP routers, authorization, OAuth, application registry, and typed `CoreFacade` consumer. |
| `services/iot-nano-core/src/` | Telemetry, alert, notification, and command workers consuming `StreamPort`, `CommandTransport`, and platform repositories. |
| `services/iot-nano-stream/src/` | In-process durable stream with SQLite records, idempotency, consumer groups, claims, acknowledgements, and recovery. |
| `services/iot-nano-mqttd/src/` | MQTT protocol, SQLite broker persistence, sessions, typed authorization/uplink/RPC ports, and cache port. |
| `apps/powermonitor/` | Independently deployable full-stack PowerMonitor using OAuth and `/api/v1`; not included in the monolith image. |
| `contracts/public-api-v1.json` | Versioned external resource, telemetry, alert, command, OAuth scope, and error contract. |
| `infra/monolith/` | Production Dockerfile, Compose, environment template, systemd unit, and offline migration/rollback scripts. |

---

### Task 1: Establish the Monolith Package and Fail-Closed Configuration

**Files:**
- Create: `services/iot-nano-monolith/Cargo.toml`
- Create: `services/iot-nano-monolith/src/lib.rs`
- Create: `services/iot-nano-monolith/src/config.rs`
- Create: `services/iot-nano-monolith/src/main.rs`
- Create: `services/iot-nano-monolith/tests/config.rs`
- Modify: `Cargo.toml`
- Modify: `services/iot-nano-{api,core,stream,mqttd}/Cargo.toml`

**Interfaces:**
- Consumes: process environment plus `--config-check` and `--migrate-only`.
- Produces: `MonolithConfig`, validated without opening a socket or database.
- Produces: `validate_retired_environment(&BTreeMap<String, String>) ->
  Result<(), ConfigError>`.

- [ ] **Step 1: Write the failing configuration matrix**

Create `services/iot-nano-monolith/tests/config.rs` with valid SQLite and
Timescale cases plus missing backend, both backends, relative path, duplicate
port, missing TLS pair, and all retired service URLs/secrets.

```rust
#[test]
fn config_rejects_a_retired_internal_service_secret() {
    let values = env([
        ("IOT_NANO_STORAGE", "sqlite"),
        ("IOT_NANO_SQLITE_PATH", "/tmp/platform.sqlite"),
        ("IOT_NANO_INTERNAL_DIR", "/tmp/iot-nano"),
        ("IOT_NANO_API_CORE_SECRET", "must-not-exist"),
    ]);

    assert!(matches!(
        MonolithConfig::from_values(values),
        Err(ConfigError::RetiredEnvironment(name)) if name == "IOT_NANO_API_CORE_SECRET"
    ));
}
```

- [ ] **Step 2: Run the focused test to verify RED**

Run:

```bash
cargo test -p iot-nano-monolith --test config
```

Expected: compilation failure because the package and `MonolithConfig` do not
exist.

- [ ] **Step 3: Register the package and define exact configuration**

Add the workspace member, library dependencies on the four service libraries,
`iot-core`, `iot-storage`, `tokio-util`, and `fs2`. Enable Tokio `signal`.
Parse no legacy service value.

```rust
pub struct MonolithConfig {
    pub storage: StorageConfiguration,
    pub internal_dir: PathBuf,
    pub public_http: SocketAddr,
    pub management_http: SocketAddr,
    pub mqtt_tcp: SocketAddr,
    pub mqtt_tls: SocketAddr,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub shutdown_deadline: Duration,
}
```

- [ ] **Step 4: Add dry-run process behavior**

Make `main.rs` parse configuration before constructing Tokio workers.
`--config-check` prints no secret and exits only after validation;
`--migrate-only` is wired in Task 2 when the store opener exists.

```rust
let arguments = Arguments::parse();
let config = MonolithConfig::from_env()?;
match arguments.mode {
    Mode::ConfigCheck => Ok(()),
    mode => run(config, mode).await,
}
```

- [ ] **Step 5: Verify configuration behavior**

Run:

```bash
cargo fmt --all -- --check
cargo test -p iot-nano-monolith --test config
cargo check -p iot-nano-monolith
```

Expected: all cases pass and no command binds a listener.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock services/iot-nano-monolith \
  services/iot-nano-api/Cargo.toml services/iot-nano-core/Cargo.toml \
  services/iot-nano-stream/Cargo.toml services/iot-nano-mqttd/Cargo.toml
git commit -m "feat: add monolith configuration boundary"
```

### Task 2: Make Platform Storage a Single Backend-Neutral Repository

**Files:**
- Modify: `crates/iot-storage/Cargo.toml`
- Modify: `crates/iot-storage/src/lib.rs`
- Create: `crates/iot-storage/src/{migration,platform,sqlite,timescale}.rs`
- Create: `crates/iot-storage/migrations/0001_platform.sql`
- Create: `crates/iot-storage/tests/{backend_contract,migration_safety}.rs`
- Modify: `services/iot-nano-api/src/{auth.rs,device_tokens.rs,resource_authorization.rs,storage.rs}`
- Modify: `services/iot-nano-core/src/{alert.rs,command.rs,control.rs,storage.rs,writer.rs}`

**Interfaces:**
- Produces: `PlatformStore::open(&StorageConfiguration) -> Result<PlatformStore,
  PlatformStoreError>`.
- Produces: narrow repository ports: `IdentityRepository`,
  `TopologyRepository`, `TelemetryRepository`, `AlertRepository`,
  `NotificationRepository`, `CommandRepository`, and `ApplicationRepository`.
- Guarantees: both adapters implement the same domain behavior; API/Core
  modules and HTTP handlers emit no backend-specific SQL.

- [ ] **Step 1: Write parameterized backend-contract tests**

Add one reusable suite for a `PlatformStore` factory. Cover identity,
resource grants, device tokens, topology, idempotent telemetry, rollups,
alerts, notifications, commands, and application registry. SQLite uses a
`tempdir`; Timescale reads only `IOT_NANO_TIMESCALE_TEST_URL`.

```rust
async fn platform_contract(store: PlatformStore) {
    let device = store.create_device(CreateDevice::named("meter-a")).await.unwrap();
    store.write_telemetry(telemetry(&device.id, 7)).await.unwrap();
    store.write_telemetry(telemetry(&device.id, 7)).await.unwrap();

    assert_eq!(store.raw_telemetry(&device.id, window()).await.unwrap().len(), 1);
    assert_eq!(store.enqueue_command(command(&device.id)).await.unwrap().state, CommandState::Queued);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p iot-storage --test backend_contract sqlite_contract
```

Expected: failure because `PlatformStore` and the common factory do not
exist.

- [ ] **Step 3: Define domain-level repository ports**

Move data transfer objects crossing API/Core boundaries from route modules
into `platform.rs`. Operations accept validated domain values and return typed
errors, never SQL fragments.

```rust
pub trait CommandRepository: Send + Sync {
    fn enqueue_command(
        &self,
        command: NewCommand,
    ) -> Pin<Box<dyn Future<Output = Result<CommandRecord, PlatformStoreError>> + Send + '_>>;
}
```

- [ ] **Step 4: Unify SQLite schema and migration**

Merge the current API metadata schema and Core data-plane schema into
`0001_platform.sql`; preserve platform foreign keys. Open SQLite with WAL,
foreign keys, busy timeout, owner-only parent permissions, and one monolith
application marker.

```rust
let options = SqliteConnectOptions::new()
    .filename(path)
    .create_if_missing(true)
    .foreign_keys(true)
    .journal_mode(SqliteJournalMode::Wal)
    .busy_timeout(busy_timeout);
```

- [ ] **Step 5: Implement Timescale schema ownership**

Create `iot_nano`, acquire a transaction-scoped advisory lock, run the same
migration set, and create telemetry as a hypertable. Do not retain
`iot_nano_api` or `iot_nano_core`.

```rust
let mut transaction = pool.begin().await?;
sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:migrate'))")
    .execute(&mut *transaction).await?;
sqlx::query("CREATE SCHEMA IF NOT EXISTS iot_nano")
    .execute(&mut *transaction).await?;
migrate_in_transaction(&mut transaction).await?;
transaction.commit().await?;
```

- [ ] **Step 6: Replace direct SQL at API/Core module boundaries**

Make API authorization and Core workers consume narrow repositories rather
than `PgPool`, `SqlitePool`, `ApiSqliteStore`, or `CoreSqliteStore`. Delete a
duplicated route-level helper only after its equivalent store method passes the
contract suite.

```rust
pub struct AlertEvaluator<S> {
    store: S,
}

impl<S: AlertRepository + TelemetryRepository> AlertEvaluator<S> {
    pub async fn evaluate(&self, record: TelemetryRecord) -> Result<(), AlertError> {
        self.store.evaluate_alerts(record).await.map_err(AlertError::Store)
    }
}
```

- [ ] **Step 7: Add migration-safety tests**

Test SQLite backup creation before an upgrade, failed migration leaves the
original usable, and two Timescale migration attempts serialize.

```rust
assert!(backup_path.exists());
assert_eq!(read_schema_version(&original).await?, 1);
assert_eq!(read_schema_version(&backup_path).await?, 1);
```

- [ ] **Step 8: Verify the contracts**

Run:

```bash
cargo test -p iot-storage --test backend_contract sqlite_contract -- --test-threads=1
IOT_NANO_TIMESCALE_TEST_URL="$IOT_NANO_TIMESCALE_TEST_URL" \
  cargo test -p iot-storage --test backend_contract timescale_contract -- --test-threads=1
cargo test -p iot-storage --test migration_safety -- --test-threads=1
```

Expected: SQLite passes locally; Timescale passes only against the declared
ephemeral test database.

- [ ] **Step 9: Commit**

```bash
git add crates/iot-storage services/iot-nano-api/src services/iot-nano-core/src
git commit -m "feat: unify platform storage for monolith"
```

### Task 3: Replace Segment-Based Stream State With `stream.sqlite`

**Files:**
- Modify: `services/iot-nano-stream/Cargo.toml`
- Modify: `services/iot-nano-stream/src/{lib.rs,group.rs,record.rs,retention.rs,segment.rs}`
- Create: `services/iot-nano-stream/src/sqlite_store.rs`
- Delete: `services/iot-nano-stream/src/http.rs`
- Delete: `services/iot-nano-stream/src/main.rs`
- Create: `services/iot-nano-stream/tests/{sqlite_recovery,consumer_group}.rs`
- Delete: `services/iot-nano-stream/tests/{contracts,service}.rs`

**Interfaces:**
- Consumes: `StreamConfig { path: PathBuf, partitions: u16, ... }`.
- Produces: `LocalStream::open(StreamConfig) -> Result<LocalStream,
  StreamError>`.
- Produces: `StreamPort::{append, claim, acknowledge, heartbeat, drain}`.
- Guarantees: an MQTT acknowledgement is emitted only after `append` commits;
  consumer acknowledgement occurs only after the caller's platform transaction
  has committed.

- [ ] **Step 1: Write the failing recovery tests**

Cover append durability after reopen, duplicate idempotency key returning the
same record, independent durable group offsets, lease expiry/reassignment, and
no record loss after a simulated process stop.

```rust
#[tokio::test]
async fn append_is_visible_after_stream_sqlite_reopen() {
    let path = tempdir().unwrap().path().join("stream.sqlite");
    let first = LocalStream::open(StreamConfig::sqlite(&path)).await.unwrap();
    let receipt = first.append(message("device-a", "key-1")).await.unwrap();
    drop(first);

    let reopened = LocalStream::open(StreamConfig::sqlite(&path)).await.unwrap();
    assert_eq!(reopened.claim("writer", 1).await.unwrap()[0].offset, receipt.offset);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p iot-nano-stream --test sqlite_recovery
```

Expected: failure because the existing stream uses segment files and has no
SQLite `StreamConfig`.

- [ ] **Step 3: Implement the SQLite stream tables and transactions**

Use a dedicated SQLite file and tables for records, idempotency, groups,
leases, and offsets. Configure WAL, foreign keys, busy timeout, and a
stream-specific `application_id`; never place platform tables in this file.

```sql
CREATE TABLE stream_records (
  partition INTEGER NOT NULL,
  offset INTEGER NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (partition, offset)
);
CREATE TABLE stream_group_offsets (
  group_name TEXT NOT NULL,
  partition INTEGER NOT NULL,
  committed_offset INTEGER NOT NULL,
  lease_owner TEXT,
  lease_until TEXT,
  PRIMARY KEY (group_name, partition)
);
```

- [ ] **Step 4: Expose the direct typed port**

Preserve validation currently performed by Stream HTTP handlers, but expose it
as direct methods. Remove header validation, Axum router construction, URLs,
and HTTP response serialization.

```rust
pub trait StreamPort: Send + Sync {
    fn append(
        &self,
        message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>>;

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>>;
}
```

- [ ] **Step 5: Implement bounded drain semantics**

Add `LocalStream::drain_until(deadline)` that rejects new claims, waits for
in-flight lease owners to commit or expire, and reports the remaining count.
It is used only during monolith shutdown.

```rust
pub async fn drain_until(&self, deadline: Instant) -> Result<(), StreamError> {
    self.accepting.store(false, Ordering::Release);
    while self.inflight_count().await? > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    (self.inflight_count().await? == 0).then_some(()).ok_or(StreamError::DrainTimeout)
}
```

- [ ] **Step 6: Verify direct-stream behavior**

Run:

```bash
cargo test -p iot-nano-stream --test sqlite_recovery -- --test-threads=1
cargo test -p iot-nano-stream --test consumer_group -- --test-threads=1
cargo check -p iot-nano-stream
```

Expected: all persistence, leasing, idempotency, and drain tests pass without
an HTTP server or a service secret.

- [ ] **Step 7: Commit**

```bash
git add services/iot-nano-stream
git commit -m "feat: persist monolith stream in sqlite"
```

### Task 4: Introduce Typed In-Process Ports and Adapters

**Files:**
- Create: `services/iot-nano-api/src/core_facade.rs`
- Modify: `services/iot-nano-api/src/{lib.rs,core_client.rs,routes.rs}`
- Create: `services/iot-nano-core/src/stream_port.rs`
- Modify: `services/iot-nano-core/src/{lib.rs,command.rs,stream_consumer.rs}`
- Modify: `services/iot-nano-mqttd/src/{lib.rs,transport.rs,policy.rs}`
- Create: `services/iot-nano-monolith/src/adapters.rs`
- Create: `services/iot-nano-monolith/tests/ports.rs`
- Delete: `services/iot-nano-api/tests/internal_core_client.rs`
- Delete: `services/iot-nano-mqttd/tests/transport/http_adapters.rs`

**Interfaces:**
- API consumes `CoreFacade`.
- Core consumes `StreamPort` and `CommandTransport`.
- MQTTD consumes `DeviceAuthorizationPort`, `StreamPort`, `CommandResponsePort`,
  and `CachePort`.
- The monolith creates all concrete adapters; no library imports the monolith.

- [ ] **Step 1: Write port-contract tests using deterministic fakes**

Verify API command creation calls `CoreFacade` exactly once, MQTT connection
authorization has no HTTP request, stream forwarding returns only after
durable append, and command transport waits for the active-session PUBACK.

```rust
#[tokio::test]
async fn mqtt_uplink_returns_only_after_local_stream_commit() {
    let stream = RecordingStream::with_append_barrier();
    let uplink = LocalUplinkForwarder::new(stream.clone());
    let forwarding = uplink.forward("token", uplink_message());
    assert!(!stream.appended());
    stream.release_append();
    forwarding.await.unwrap();
    assert!(stream.appended());
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p iot-nano-monolith --test ports
```

Expected: failure because API uses `CoreClient`, MQTTD uses HTTP adapters, and
Core uses `HttpStreamConsumer`/`HttpTransportRpcClient`.

- [ ] **Step 3: Replace API-to-Core HTTP with `CoreFacade`**

Replace `CoreClient` fields, URL construction, reqwest calls, status mapping,
and `x-iot-nano-api-core-secret` with a typed facade. Keep API route behavior
and public error mapping stable.

```rust
pub trait CoreFacade: Send + Sync {
    fn create_command(
        &self,
        request: CreateCommand,
    ) -> Pin<Box<dyn Future<Output = Result<CommandRecord, CoreFacadeError>> + Send + '_>>;

    fn telemetry(
        &self,
        request: TelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<TelemetryPage, CoreFacadeError>> + Send + '_>>;
}
```

- [ ] **Step 4: Replace Core HTTP clients**

Make worker loops accept `Arc<dyn StreamPort>` and
`Arc<dyn CommandTransport>`. Reuse current command state transitions and
stream acknowledgement order; delete HTTP URL parsing, internal headers, and
HTTP-only error variants.

```rust
pub trait CommandTransport: Send + Sync {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>>;
}
```

- [ ] **Step 5: Replace MQTTD outbound HTTP ports**

Adapt `DeviceAuthenticator`, `UplinkForwarder`, and `RpcResponseForwarder` to
the platform authorization service, `LocalStream`, and Core facade in
`adapters.rs`. Authorization stays fail-closed. Cache misses may call an
in-process port but never a URL.

```rust
pub trait DeviceAuthorizationPort: Send + Sync {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>;
}
```

- [ ] **Step 6: Add compile-time forbidden-boundary checks**

Create a test that reads only these source directories and rejects
`reqwest::Client`, `/internal/`, `x-iot-nano-`, `IOT_NANO_*_URL`, and
`IOT_NANO_*_SECRET` in monolith library paths.

```rust
for source in monolith_library_sources() {
    let text = std::fs::read_to_string(source)?;
    assert!(!text.contains("/internal/"), "internal HTTP remains in {source:?}");
    assert!(!text.contains("x-iot-nano-"), "internal header remains in {source:?}");
}
```

- [ ] **Step 7: Verify direct ports**

Run:

```bash
cargo test -p iot-nano-monolith --test ports -- --test-threads=1
cargo test -p iot-nano-api --lib -- --test-threads=1
cargo test -p iot-nano-core --lib -- --test-threads=1
cargo test -p iot-nano-mqttd --test transport -- --test-threads=1
```

Expected: the same domain outcomes are proven without HTTP clients, URLs, or
service credentials.

- [ ] **Step 8: Commit**

```bash
git add services/iot-nano-api services/iot-nano-core services/iot-nano-mqttd \
  services/iot-nano-monolith
git commit -m "feat: replace internal http with monolith ports"
```

### Task 5: Rewire Core Workers to the Platform Store and Local Stream

**Files:**
- Modify: `services/iot-nano-core/src/{alert.rs,command.rs,control.rs,main.rs,notification.rs,stream_consumer.rs,writer.rs}`
- Modify: `services/iot-nano-core/tests/{alert.rs,command.rs,control.rs,e2e.rs,notification.rs,stream_to_storage.rs,writer.rs}`
- Create: `services/iot-nano-core/src/runtime.rs`
- Delete: `services/iot-nano-core/src/main.rs`

**Interfaces:**
- Produces: `CoreRuntime::start(CoreRuntimeConfig) -> CoreRuntime`.
- Produces: `CoreRuntime::{ready, stop_claiming, drain, join}`.
- Guarantees: Core acknowledges a stream record only after the platform
  transaction and idempotency state commit.

- [ ] **Step 1: Write a transaction-before-ack test**

Use a stream fake that records acknowledgements and a platform-store fake that
fails once. Assert failed persistence leaves the record claimable and a
successful retry creates one telemetry row and one acknowledgement.

```rust
assert!(stream.acknowledgements().is_empty());
runtime.process_once().await.unwrap_err();
assert_eq!(stream.claimable_offsets(), vec![0]);

runtime.process_once().await.unwrap();
assert_eq!(store.telemetry_count().await, 1);
assert_eq!(stream.acknowledgements(), vec![0]);
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p iot-nano-core --test stream_to_storage transaction_commits_before_ack
```

Expected: the new runtime API does not exist.

- [ ] **Step 3: Extract worker startup from the service binary**

Move writer, alert evaluator, notification dispatcher, command dispatcher,
retention, and metrics loops into `CoreRuntime`. Pass typed platform, stream,
and command ports plus a child cancellation token.

```rust
pub struct CoreRuntime {
    cancellation: CancellationToken,
    tasks: JoinSet<Result<(), CoreRuntimeError>>,
}

impl CoreRuntime {
    pub async fn start(config: CoreRuntimeConfig) -> Result<Self, CoreRuntimeError> {
        // Start all worker loops after their dependencies have been validated.
    }
}
```

- [ ] **Step 4: Remove service process configuration**

Delete Core's Clap listener, health endpoint, `DATABASE_URL` parsing,
`IOT_NANO_STREAM_URL`, `IOT_NANO_CORE_STREAM_SECRET`,
`IOT_NANO_MQTTD_INTERNAL_URL`, and `IOT_NANO_CORE_MQTTD_SECRET`. Keep tuning
as an explicit `CoreRuntimeConfig` passed from the composition root.

```rust
pub struct CoreRuntimeConfig {
    pub store: Arc<PlatformStore>,
    pub stream: Arc<dyn StreamPort>,
    pub command_transport: Arc<dyn CommandTransport>,
    pub cancellation: CancellationToken,
}
```

- [ ] **Step 5: Verify Core worker invariants**

Run:

```bash
cargo test -p iot-nano-core --test stream_to_storage -- --test-threads=1
cargo test -p iot-nano-core --test command -- --test-threads=1
cargo test -p iot-nano-core --test alert -- --test-threads=1
cargo test -p iot-nano-core --test notification -- --test-threads=1
```

Expected: no test starts a Core HTTP server or requires an internal service
secret.

- [ ] **Step 6: Commit**

```bash
git add services/iot-nano-core
git commit -m "feat: run core workers in process"
```

### Task 6: Build the Versioned Public API, Application Registry, and OAuth

**Files:**
- Create: `contracts/public-api-v1.json`
- Create: `services/iot-nano-api/src/{application_registry,oauth,public_v1}.rs`
- Modify: `services/iot-nano-api/src/{auth.rs,lib.rs,routes.rs}`
- Modify: `crates/iot-storage/src/{platform.rs,sqlite.rs,timescale.rs}`
- Modify: `crates/iot-storage/migrations/0001_platform.sql`
- Create: `services/iot-nano-api/tests/{oauth,public_v1}.rs`
- Delete: `services/iot-nano-api/src/core_client.rs`

**Interfaces:**
- Produces: `public_router(ApiState) -> Router` mounted only at `/api/v1`.
- Produces: OAuth `GET /oauth/authorize` and `POST /oauth/token`.
- Produces: application registry records with
  `app_id, kind, launch_url, redirect_uris, client_id, allowed_scopes,
  enabled`.
- Consumes: bearer access tokens with exact resource scopes; OAuth token
  handlers never accept a browser session as a bearer token.

- [ ] **Step 1: Write the external contract before routes**

Define public resources, pagination, cursor format, error envelope, scope
requirements, command idempotency key, OAuth errors, and no internal endpoint
in `contracts/public-api-v1.json`.

```json
{
  "version": "v1",
  "resources": {
    "devices": {"read_scope": "devices:read", "write_scope": "devices:write"},
    "telemetry": {"read_scope": "telemetry:read"},
    "commands": {"write_scope": "commands:write"},
    "alerts": {"read_scope": "alerts:read"}
  },
  "error": {"required": ["code", "message", "request_id"]}
}
```

- [ ] **Step 2: Write OAuth and scope-denial tests**

Test authorization code plus S256 PKCE success, verifier mismatch, reused
code, expired code, invalid redirect URI, disabled app, confidential client
secret mismatch, client-credentials scope expansion, expired token, and
cross-app scope denial.

```rust
#[tokio::test]
async fn authorization_code_requires_the_original_s256_verifier() {
    let code = authorize(&app, "S256", code_challenge("correct")).await;
    let response = exchange_code(&app, code, "incorrect").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.json().await["error"], "invalid_grant");
}
```

- [ ] **Step 3: Verify RED**

Run:

```bash
cargo test -p iot-nano-api --test oauth
```

Expected: failure because no OAuth handler, registry, or token persistence
exists.

- [ ] **Step 4: Add normalized application and OAuth persistence**

Create application, redirect URI, OAuth authorization-code, access-token, and
client-secret tables in both backend migrations. Store redirect URIs as
individual exact-match rows; store client-secret, code, and access-token
digests only.

```sql
CREATE TABLE oauth_authorization_codes (
  code_hash TEXT PRIMARY KEY,
  app_id TEXT NOT NULL REFERENCES applications(app_id) ON DELETE CASCADE,
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  redirect_uri TEXT NOT NULL,
  code_challenge TEXT NOT NULL,
  scopes_json TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  consumed_at TEXT
);
```

- [ ] **Step 5: Implement OAuth token issuance**

Use random 256-bit URL-safe tokens, SHA-256 digests, constant-time secret
comparison, one transaction to consume a code and issue an access token, and
explicit scopes. Do not expose `client_secret`, token digests, database
errors, or user sessions in a response.

```rust
pub async fn exchange_authorization_code(
    store: &impl ApplicationRepository,
    request: CodeExchange,
    now: DateTime<Utc>,
) -> Result<TokenResponse, OAuthError> {
    validate_s256(&request.code_verifier)?;
    store.consume_code_and_issue_token(request, now).await
}
```

- [ ] **Step 6: Add `/api/v1` public routers**

Implement generic device, asset, telemetry, alert, command, and authorization
operations against repository ports and `CoreFacade`. Scope check occurs
before resource authorization; resource grants further reduce the result set.
Do not mount PowerMonitor-specific `/api/apps/powermonitor/*` routes.

```rust
Router::new()
    .route("/api/v1/devices", get(list_devices).post(create_device))
    .route("/api/v1/devices/{device_id}/telemetry", get(device_telemetry))
    .route("/api/v1/devices/{device_id}/commands", post(create_command))
    .route("/api/v1/alerts", get(list_alerts))
    .route("/oauth/authorize", get(authorize))
    .route("/oauth/token", post(token));
```

- [ ] **Step 7: Split public and management routers**

Keep public OAuth and `/api/v1` on the public listener. Put management,
OpenAPI/Swagger, and administrative mutation routes in `management_router`;
the monolith binds it only to the configured protected address.

```rust
pub struct ApiRouters {
    pub public: Router,
    pub management: Router,
}

pub fn routers(state: ApiState) -> ApiRouters {
    ApiRouters { public: public_router(state.clone()), management: management_router(state) }
}
```

- [ ] **Step 8: Verify API and registry behavior**

Run:

```bash
cargo test -p iot-nano-api --test oauth -- --test-threads=1
cargo test -p iot-nano-api --test public_v1 -- --test-threads=1
cargo test -p iot-nano-api --test sqlite_auth -- --test-threads=1
```

Expected: PKCE, client credentials, exact scopes, `/api/v1`, and management
listener separation pass for SQLite; repeat contract tests against Timescale.

- [ ] **Step 9: Commit**

```bash
git add contracts/public-api-v1.json crates/iot-storage services/iot-nano-api
git commit -m "feat: add versioned external api and oauth"
```

### Task 7: Embed MQTTD With Local Authorization, Stream, RPC, and Cache Ports

**Files:**
- Modify: `services/iot-nano-mqttd/src/{lib.rs,main.rs,policy.rs,storage.rs,transport.rs}`
- Create: `services/iot-nano-mqttd/src/cache.rs`
- Modify: `services/iot-nano-mqttd/tests/{authorization.rs,broker_features.rs,config.rs,persistence.rs,protocol.rs}`
- Create: `services/iot-nano-monolith/src/cache.rs`
- Create: `services/iot-nano-monolith/tests/mqtt_ports.rs`
- Delete: `services/iot-nano-mqttd/tests/{standalone.rs,transport/internal_rpc.rs}`

**Interfaces:**
- Produces: `MqttRuntime::start(MqttRuntimeConfig) -> MqttRuntime`.
- Consumes: `DeviceAuthorizationPort`, `StreamPort`, `CommandResponsePort`,
  `RpcSessionRouter`, `CachePort`, `mqttd.sqlite`, and a cancellation token.
- Guarantees: device connect/publish authorization remains fail-closed; QoS
  acknowledgement waits for stream commit; command delivery waits for active
  session PUBACK.

- [ ] **Step 1: Write in-process MQTT acceptance tests**

Test token authentication, denied token, direct telemetry durability before
PUBACK, gateway child authorization, exact active session RPC delivery,
two-way response recording, token rotation/revocation, TLS parity, and
restart recovery with `mqttd.sqlite`.

```rust
#[tokio::test]
async fn qos_one_ack_follows_stream_commit() {
    let fixture = MonolithMqttFixture::new().await;
    fixture.stream.pause_next_append();
    let publish = fixture.device.publish_qos1(telemetry_payload());
    assert!(fixture.device.no_puback_yet().await);
    fixture.stream.release_next_append();
    publish.await.unwrap();
    assert_eq!(fixture.stream.records().await.len(), 1);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p iot-nano-monolith --test mqtt_ports qos_one_ack_follows_stream_commit
```

Expected: failure because MQTTD startup still builds HTTP adapter URLs and
service secrets.

- [ ] **Step 3: Extract MQTT runtime from `main.rs`**

Move configuration-independent startup into a library runtime. The monolith
passes listener addresses, TLS material, storage, typed ports, cache, and
cancellation; MQTTD reads no process environment.

```rust
pub struct MqttRuntimeConfig {
    pub listeners: ListenerConfiguration,
    pub storage: Arc<dyn BrokerStorage>,
    pub authorization: Arc<dyn DeviceAuthorizationPort>,
    pub stream: Arc<dyn StreamPort>,
    pub session_router: Arc<RpcSessionRouter>,
    pub cache: Arc<dyn CachePort>,
    pub cancellation: CancellationToken,
}
```

- [ ] **Step 4: Implement `cache.sqlite` behind a cache port**

Use an in-memory LRU-style map for hot values and persist only cache entries
that require restart survival. Apply an expiry timestamp on every read and
write. Cache failure must fail open only for cached authorization decisions:
the next request must re-authorize through `DeviceAuthorizationPort`.

```rust
pub trait CachePort: Send + Sync {
    fn get(&self, key: &str) -> Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, CacheError>> + Send + '_>>;
    fn put(&self, entry: CacheEntry) -> Pin<Box<dyn Future<Output = Result<(), CacheError>> + Send + '_>>;
}
```

- [ ] **Step 5: Remove HTTP transport configuration**

Delete API base URL, stream URL, internal management transport listener,
HTTP clients, request headers, and all related secrets. Preserve normal
public MQTT TCP/TLS and loopback-only management observability.

```rust
assert!(config.api_base_url.is_none());
assert!(config.stream_url.is_none());
assert!(config.internal_transport_address.is_none());
```

- [ ] **Step 6: Verify MQTT behavior**

Run:

```bash
cargo test -p iot-nano-monolith --test mqtt_ports -- --test-threads=1
cargo test -p iot-nano-mqttd --test authorization -- --test-threads=1
cargo test -p iot-nano-mqttd --test persistence -- --test-threads=1
cargo test -p iot-nano-mqttd --test protocol -- --test-threads=1
```

Expected: public MQTT behavior passes with local ports and no internal HTTP
listener.

- [ ] **Step 7: Commit**

```bash
git add services/iot-nano-mqttd services/iot-nano-monolith
git commit -m "feat: embed mqtt runtime in monolith"
```

### Task 8: Compose, Supervise, and Gracefully Stop One Runtime

**Files:**
- Create: `services/iot-nano-monolith/src/{adapters,runtime,readiness,supervisor}.rs`
- Modify: `services/iot-nano-monolith/src/{lib.rs,main.rs}`
- Create: `services/iot-nano-monolith/tests/{startup,shutdown}.rs`
- Modify: `services/iot-nano-{api,core,stream,mqttd}/src/lib.rs`
- Delete: `services/iot-nano-{api,mqttd}/src/main.rs`
- Modify: `services/iot-nano-{api,core,stream,mqttd}/Cargo.toml`

**Interfaces:**
- Produces: `MonolithRuntime::start(MonolithConfig) -> Result<MonolithRuntime,
  StartupError>`.
- Produces: `MonolithRuntime::{readiness, shutdown}`.
- Guarantees: no public listener binds until storage migration, all three
  internal SQLite files, stream recovery, Core workers, MQTTD, and both API
  routers are ready.

- [ ] **Step 1: Write startup-order and readiness tests**

Use temporary directories and ephemeral ports to prove invalid platform
configuration, migration failure, locked internal directory, corrupt
`stream.sqlite`, corrupt `mqttd.sqlite`, missing TLS, or failed worker start
leave all public addresses unbound. Assert `/healthz` is unavailable until the
whole runtime reports ready.

```rust
#[tokio::test]
async fn migration_failure_binds_no_public_listener() {
    let fixture = Fixture::sqlite().with_failing_migration();
    let error = MonolithRuntime::start(fixture.config()).await.unwrap_err();
    assert!(matches!(error, StartupError::PlatformMigration(_)));
    assert!(fixture.public_http_is_unbound().await);
    assert!(fixture.mqtt_tcp_is_unbound().await);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p iot-nano-monolith --test startup
```

Expected: failure because `MonolithRuntime` and process supervision do not
exist.

- [ ] **Step 3: Acquire resources in the required order**

Create the owner-only internal directory, acquire a nonblocking exclusive
`instance.lock`, open/migrate platform storage, then open/recover
`stream.sqlite`, `mqttd.sqlite`, and `cache.sqlite`. Any error drops all
opened resources and returns before listener startup.

```rust
let lock = InstanceLock::acquire(&config.internal_dir.join("instance.lock"))?;
let platform = PlatformStore::open(&config.storage).await?;
let stream = Arc::new(LocalStream::open(StreamConfig::sqlite(internal.join("stream.sqlite"))).await?);
let mqtt_storage = Arc::new(SqliteStorage::open(internal.join("mqttd.sqlite"))?);
let cache = Arc::new(PersistentCache::open(internal.join("cache.sqlite")).await?);
```

- [ ] **Step 4: Start internal components before public listeners**

Build repository adapters, `CoreRuntime`, MQTTD runtime, and API routers in
memory. Register a readiness bit only after each start call succeeds. Bind
public HTTP, management HTTP, MQTT TCP, and MQTT TLS last.

```rust
let core = CoreRuntime::start(core_config).await?;
let mqtt = MqttRuntime::start(mqtt_config).await?;
let routers = iot_api::routers(api_state);
readiness.require_all(["platform", "stream", "core", "mqttd", "public-router"]);
```

- [ ] **Step 5: Implement supervision and deadline shutdown**

On SIGINT/SIGTERM, stop HTTP and MQTT accepts first, reject new stream claims,
drain command/stream work to the configured deadline, cancel workers, join all
tasks, flush cache, close database pools, and release the instance lock.
Collect every task failure; the first unexpected child failure triggers the
same bounded shutdown and a nonzero exit.

```rust
pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), ShutdownError> {
    self.public_listeners.stop().await?;
    self.mqtt.stop_accepting().await?;
    self.stream.stop_accepting();
    self.core.drain(deadline).await?;
    self.cancellation.cancel();
    self.join_until(deadline).await?;
    Ok(())
}
```

- [ ] **Step 6: Write shutdown tests**

Prove listener closure precedes drain, a queued command is delivered before
deadline, an uncommitted stream record survives restart, deadline expiry is
reported, and all four SQLite handles can reopen after the process exits.

```rust
assert!(fixture.public_http_is_unbound().await);
assert_eq!(fixture.command_state(command_id).await, CommandState::PublishedToBroker);
assert!(LocalStream::open(fixture.stream_config()).await.is_ok());
```

- [ ] **Step 7: Retire the remaining service binaries only after behavior passes**

Remove API and MQTTD `main.rs` targets and their binary-only CLI dependencies;
Stream and Core targets were removed in Tasks 3 and 5. Leave each library's
unit tests and public library API intact; add `autobins = false` if Cargo
would otherwise infer an obsolete target.

```toml
[package]
autobins = false
```

- [ ] **Step 8: Verify lifecycle and one-binary workspace**

Run:

```bash
cargo test -p iot-nano-monolith --test startup -- --test-threads=1
cargo test -p iot-nano-monolith --test shutdown -- --test-threads=1
cargo check --workspace
cargo build --release -p iot-nano-monolith
```

Expected: startup/shutdown tests pass and only the production monolith binary
is selected by the release command. Task 10 verifies the final image contains
only that binary.

- [ ] **Step 9: Commit**

```bash
git add services/iot-nano-monolith services/iot-nano-api services/iot-nano-core \
  services/iot-nano-stream services/iot-nano-mqttd Cargo.lock
git commit -m "feat: compose the single process runtime"
```

### Task 9: Extract PowerMonitor as an External Full-Stack Application

**Files:**
- Create: `apps/powermonitor/package.json`
- Create: `apps/powermonitor/{app,components,lib,tests}/`
- Create: `apps/powermonitor/.env.example`
- Create: `apps/powermonitor/Dockerfile`
- Modify: `web/package.json`
- Delete: `web/app/apps/powermonitor/`
- Delete: `web/components/powermonitor-*.tsx`
- Delete: `web/components/power-{switcher-control,telemetry-chart,telemetry-table}.tsx`
- Modify: `web/{app,components,lib}/`
- Create: `apps/powermonitor/tests/{oauth_callback,platform_client}.test.ts`

**Interfaces:**
- Consumes: `PLATFORM_BASE_URL`, `OAUTH_CLIENT_ID`, and server-only
  `OAUTH_CLIENT_SECRET`.
- Produces: a separately deployable Next.js application with a server-side
  OAuth callback and BFF routes calling only `/api/v1`.
- Guarantees: browser code never gets the client secret; the app never reads a
  platform or internal SQLite file and never receives `DATABASE_URL`.

- [ ] **Step 1: Read the local Next.js instructions and write extraction tests**

Before editing `web` or the new application, read `web/AGENTS.md` and the
installed Next.js documentation required there. Add tests that mock OAuth
callback exchange, bearer-token calls, denied scope, and no leaked secret in
the client bundle.

```ts
it("exchanges the callback code only in the server route", async () => {
  const response = await GET(callbackRequest({ code: "code", state: signedState }));
  expect(platform.token).toHaveBeenCalledWith(expect.objectContaining({
    grant_type: "authorization_code",
    code_verifier: expect.any(String),
  }));
  expect(response.headers.get("set-cookie")).toContain("powermonitor_session=");
});
```

- [ ] **Step 2: Verify RED**

Run:

```bash
npm --prefix apps/powermonitor test
```

Expected: failure because the external application package does not exist.

- [ ] **Step 3: Create an independent OAuth BFF**

Implement login redirect with a generated state and PKCE verifier in an
encrypted, HttpOnly, Secure, SameSite=Lax application cookie. The callback
exchanges the code server-side, stores opaque tokens in the application
session, and returns the user to PowerMonitor. Browser fetches call only
PowerMonitor BFF endpoints.

```ts
export async function platformRequest(path: string, session: AppSession) {
  return fetch(new URL(`/api/v1${path}`, process.env.PLATFORM_BASE_URL), {
    headers: { authorization: `Bearer ${session.accessToken}` },
    cache: "no-store",
  });
}
```

- [ ] **Step 4: Move PowerMonitor behavior and map it to generic APIs**

Move the existing dashboard, asset tree, device details, telemetry, alert, and
command UI into `apps/powermonitor`. Replace PowerMonitor-specific API calls
with generic `/api/v1` resources and retain component tests. The platform's
remaining `web` application is an operator management console and has no
PowerMonitor route or component import.

```ts
const telemetry = await platformRequest(
  `/devices/${encodeURIComponent(deviceId)}/telemetry?from=${from}&to=${to}`,
  session,
);
```

- [ ] **Step 5: Add confidential service-client behavior**

For PowerMonitor server-side aggregation only, use a confidential
client-credentials token with explicitly configured service scopes. Do not
reuse the user token, elevate user scopes, or expose the service credential to
the browser.

```ts
const token = await fetch(`${platformBaseUrl}/oauth/token`, {
  method: "POST",
  headers: authorizationHeader(clientId, clientSecret),
  body: new URLSearchParams({ grant_type: "client_credentials", scope: "telemetry:read" }),
});
```

- [ ] **Step 6: Add no-direct-database contract tests**

Fail the build if PowerMonitor source contains `DATABASE_URL`, `sqlite`,
`postgres://`, `IOT_NANO_INTERNAL_DIR`, `/internal/`, or an import from the
platform workspace. Fail its Compose service if it mounts a platform volume.

```ts
for (const file of applicationSourceFiles()) {
  expect(readFileSync(file, "utf8")).not.toMatch(
    /DATABASE_URL|postgres:\/\/|IOT_NANO_INTERNAL_DIR|\/internal\//,
  );
}
```

- [ ] **Step 7: Verify the external application**

Run:

```bash
npm --prefix apps/powermonitor test
npm --prefix apps/powermonitor run build
npm --prefix web test
npm --prefix web run build
```

Expected: the extracted app passes OAuth and generic API tests; the platform
console no longer contains PowerMonitor code.

- [ ] **Step 8: Commit**

```bash
git add apps/powermonitor web
git commit -m "feat: extract powermonitor external app"
```

### Task 10: Package, Deploy, and Enforce the One-Container Topology

**Files:**
- Modify: `infra/docker/Dockerfile`
- Modify: `infra/compose.yaml`
- Create: `infra/compose.timescale.yaml`
- Create: `infra/monolith/{monolith.env.example,migrate.sh,rollback.sh}`
- Create: `infra/systemd/iot-nano-monolith.service`
- Create: `scripts/verify-monolith-topology.sh`
- Modify: `scripts/e2e-local.sh`
- Modify: `README.md`
- Create: `docs/operations-monolith.md`
- Delete: `infra/dev/{api.env,iot-nano-core.env,iot-nano-mqttd.env,iot-nano-stream.env}`
- Delete: `infra/systemd/{iot-nano-api.service,iot-nano-core.service,iot-nano-mqttd.service,iot-nano-stream.service}`
- Delete: `scripts/{install-mqttd-standalone.sh,rpc-e2e.py}`

**Interfaces:**
- Produces: one image containing `/opt/rush-iot-nano/iot-nano-monolith`.
- Produces: an optional Timescale Compose profile and one monolith service.
- Consumes: platform-data and internal-state volumes, or only internal-state
  volume with an external `DATABASE_URL`.
- Produces: explicit `migrate.sh` and `rollback.sh` for monolith-to-monolith
  offline upgrades only.

- [ ] **Step 1: Write topology assertion tests**

Add a shell test that parses rendered Compose configuration and Docker image
commands. It must require exactly one application service named
`iot-nano-monolith`, forbid service images/binaries for API/Core/Stream/MQTTD,
forbid internal URLs/secrets, and assert the correct mounted volumes for each
storage mode.

```bash
services="$(docker compose --file infra/compose.yaml config --services | sort)"
test "$services" = $'iot-nano-monolith\ntimescaledb'
! rg -n 'iot-nano-(api|core|stream|mqttd)|IOT_NANO_.*_(URL|SECRET)' \
  infra/compose.yaml infra/monolith
```

- [ ] **Step 2: Verify RED**

Run:

```bash
scripts/verify-monolith-topology.sh
```

Expected: failure because the current Compose file runs four application
services and the Dockerfile copies four binaries.

- [ ] **Step 3: Build only the monolith image**

Replace the Dockerfile build targets and copied artifacts. The runtime image
contains CA certificates, the one binary, and no source, Node application, or
external-app credential.

```dockerfile
RUN cargo build --release --package iot-nano-monolith

COPY --from=build /workspace/target/release/iot-nano-monolith \
  /opt/rush-iot-nano/iot-nano-monolith
ENTRYPOINT ["/opt/rush-iot-nano/iot-nano-monolith"]
```

- [ ] **Step 4: Create storage-safe Compose and environment templates**

Set `IOT_NANO_STORAGE`, `IOT_NANO_SQLITE_PATH`, and
`IOT_NANO_INTERNAL_DIR` explicitly. The base Compose file is SQLite-only and
mounts platform/internal volumes. `infra/compose.timescale.yaml` is an
explicit override that unsets `IOT_NANO_SQLITE_PATH`, sets `DATABASE_URL`, and
mounts only internal state; it must never supply both platform backends.

```yaml
# infra/compose.yaml
iot-nano-monolith:
  environment:
    IOT_NANO_STORAGE: sqlite
    IOT_NANO_SQLITE_PATH: /var/lib/iot-nano/platform/platform.sqlite
    IOT_NANO_INTERNAL_DIR: /var/lib/iot-nano/internal
  volumes:
    - platform-data:/var/lib/iot-nano/platform
    - internal-data:/var/lib/iot-nano/internal

# infra/compose.timescale.yaml
iot-nano-monolith:
  environment:
    IOT_NANO_STORAGE: timescale
    IOT_NANO_SQLITE_PATH:
    DATABASE_URL: ${DATABASE_URL:?DATABASE_URL is required}
  volumes:
    - internal-data:/var/lib/iot-nano/internal
```

- [ ] **Step 5: Add offline migration and rollback scripts**

`migrate.sh` stops the monolith, takes an SQLite filesystem backup through
the binary's `--migrate-only` flow, runs migration with no listener, and
starts the pinned image only after success. `rollback.sh` refuses to run
unless a timestamped pre-migration SQLite backup or an operator-provided
Timescale restore point exists.

```bash
docker compose --file infra/compose.yaml stop iot-nano-monolith
docker compose --file infra/compose.yaml run --rm --no-deps iot-nano-monolith --migrate-only
docker compose --file infra/compose.yaml up --detach iot-nano-monolith
```

- [ ] **Step 6: Record topology migration policy explicitly**

In `docs/operations-monolith.md`, state that this plan does not migrate live
four-service production data. The first monolith deployment is a fresh
environment. Moving an existing four-service installation requires a separate
approved offline export/import design; operators must not run the two
topologies simultaneously or point monolith at a service-owned database.

```text
Unsupported: stopping API/Core/Stream/MQTTD and opening their prior state
directly with iot-nano-monolith. The platform must be newly provisioned, or a
separately approved offline migration must be completed before this release.
```

- [ ] **Step 7: Add a restricted systemd unit**

Create one unit ordered after network readiness, with an owner-only state
directory, `UMask=0077`, `Restart=on-failure`, no ambient database secrets in
logs, and only the public MQTT/HTTP ports granted when privileged ports are
selected.

```ini
[Service]
User=iotnano
Group=iotnano
UMask=0077
StateDirectory=iot-nano
ExecStart=/opt/rush-iot-nano/iot-nano-monolith
Restart=on-failure
NoNewPrivileges=true
```

- [ ] **Step 8: Verify deployment assets**

Run:

```bash
scripts/verify-monolith-topology.sh
docker compose --file infra/compose.yaml config --quiet
docker build --file infra/docker/Dockerfile --tag iot-nano-monolith:test .
```

Expected: rendered topology has one application container and the image has
one application binary.

- [ ] **Step 9: Commit**

```bash
git add infra scripts README.md docs/operations-monolith.md
git commit -m "feat: package the monolith deployment"
```

### Task 11: Prove the Complete One-Process System and Remove Legacy Assets

**Files:**
- Create: `services/iot-nano-monolith/tests/{e2e_sqlite,e2e_timescale,external_app_contract}.rs`
- Create: `scripts/e2e-monolith.sh`
- Create: `scripts/verify-no-legacy-runtime.sh`
- Modify: `scripts/e2e-local.sh`
- Modify: `README.md`
- Modify: `Cargo.toml`
- Delete: `contracts/internal-api-v1.json`
- Delete: `contracts/stream-v1.json`
- Delete: `docs/iot-nano-four-service-architecture.md`
- Delete: `docs/operations.md`

**Interfaces:**
- Produces: reproducible clean-start acceptance for SQLite and Timescale.
- Produces: static proof that the repository has no production four-service
  topology, internal HTTP route, service secret, or legacy process script.
- Guarantees: every behavior named in the approved design has at least one
  named test.

- [ ] **Step 1: Write the single-process SQLite E2E**

Spawn only `CARGO_BIN_EXE_iot-nano-monolith` with temporary platform and
internal directories, ephemeral public/management/MQTT ports, and generated
TLS. Verify device authentication, MQTT append, durable `stream.sqlite`,
Core telemetry persistence, `/api/v1` telemetry query, alert creation and
evaluation, command dispatch and response, readiness, and graceful shutdown.

```rust
let runtime = MonolithProcess::spawn(SqliteFixture::new()).await?;
runtime.wait_ready().await?;
runtime.device().publish_qos1(telemetry_payload()).await?;
assert_eq!(runtime.api().telemetry(device_id).await?.points.len(), 1);
assert_eq!(runtime.stream_record_count().await?, 1);
runtime.shutdown().await?;
```

- [ ] **Step 2: Write the Timescale E2E**

Run only when `IOT_NANO_TIMESCALE_TEST_URL` is declared by the harness. Use a
fresh `iot_nano_test_<uuid>` database/schema, verify migration lock behavior,
hypertable telemetry, all the SQLite E2E flows, and no platform SQLite file.

```rust
assert!(fixture.internal_dir().join("stream.sqlite").exists());
assert!(fixture.internal_dir().join("mqttd.sqlite").exists());
assert!(fixture.internal_dir().join("cache.sqlite").exists());
assert!(!fixture.platform_dir().join("platform.sqlite").exists());
```

- [ ] **Step 3: Write external-app contract tests**

Run a PKCE browser-style authorization flow, PowerMonitor BFF callback,
scoped `/api/v1` request, confidential client request, denied scope, disabled
app, and an attempted request to a removed `/internal/*` route. Verify app
container environment and mounts contain neither platform database nor
internal-state access.

```rust
assert_eq!(client.get("/internal/commands").send().await?.status(), StatusCode::NOT_FOUND);
assert_eq!(scoped_client.get("/api/v1/alerts").send().await?.status(), StatusCode::FORBIDDEN);
```

- [ ] **Step 4: Write durable recovery and shutdown tests**

Cover SQLite platform backup/restore, `stream.sqlite` replay after a forced
worker failure, `mqttd.sqlite` retained/session/QoS recovery, cache expiry,
single-instance lock rejection, and deadline shutdown with durable unfinished
work on restart.

```rust
let first = MonolithProcess::spawn(fixture.clone()).await?;
first.kill_after_stream_commit().await?;
let second = MonolithProcess::spawn(fixture).await?;
assert_eq!(second.api().telemetry(device_id).await?.points.len(), 1);
```

- [ ] **Step 5: Replace four-service E2E and static scans**

Make `scripts/e2e-local.sh` delegate to `scripts/e2e-monolith.sh`. The static
scan rejects retired packages, release binaries, Compose services, internal
routes, headers, named internal secrets/URLs, environment templates, and
container dependencies. It scopes to production source/deployment paths so it
does not reject the declared Timescale test URL.

```bash
rg -n \
  'iot-nano-(api|core|stream|mqttd)|/internal/|x-iot-nano-|IOT_NANO_(CORE_URL|STREAM_URL|MQTTD_INTERNAL_URL|MQTTD_API_SECRET|API_MQTTD_SECRET|MQTTD_STREAM_SECRET|CORE_STREAM_SECRET|API_CORE_SECRET|CORE_MQTTD_SECRET)' \
  services/iot-nano-monolith infra scripts && exit 1 || exit 0
```

- [ ] **Step 6: Delete legacy contracts and operational material**

Delete only files superseded by the new verified monolith assets, including
the old internal HTTP contracts and four-service runtime documents. If an old
document is already a user deletion, preserve that deletion and stage it with
`git add -u`; do not recreate it merely to run `git rm`. Keep domain contracts
`telemetry-v1.json`, `gateway-telemetry-v1.json`, and `rpc-v1.json`; they
remain internal typed data formats, not HTTP APIs.

```bash
git rm contracts/internal-api-v1.json contracts/stream-v1.json
git add -u docs/iot-nano-four-service-architecture.md docs/operations.md
```

- [ ] **Step 7: Run all final verification commands**

Run:

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo test -p iot-storage -- --test-threads=1
cargo test -p iot-nano-stream -- --test-threads=1
cargo test -p iot-nano-core -- --test-threads=1
cargo test -p iot-nano-api -- --test-threads=1
cargo test -p iot-nano-mqttd -- --test-threads=1
cargo test -p iot-nano-monolith -- --test-threads=1
IOT_NANO_TIMESCALE_TEST_URL="$IOT_NANO_TIMESCALE_TEST_URL" \
  cargo test -p iot-nano-monolith --test e2e_timescale -- --test-threads=1
npm --prefix web test
npm --prefix web run build
npm --prefix apps/powermonitor test
npm --prefix apps/powermonitor run build
scripts/verify-monolith-topology.sh
scripts/verify-no-legacy-runtime.sh
scripts/e2e-monolith.sh
```

Expected: all commands exit zero. The Timescale command is run only by a
harness that has provisioned its declared disposable database.

- [ ] **Step 8: Perform final requirement audit**

Compare the results with every approved-design requirement: one binary,
public-only listeners, typed ports, two platform backends, three internal
SQLite files, MQTT durable acknowledgement, transaction-before-ack, OAuth
PKCE/client credentials, external app isolation, backup/lock behavior, and
graceful shutdown. Record command output and artifact versions in the release
notes.

```text
Release gate: do not deploy when any check above is skipped, when any test is
red, or when an old four-service database is proposed as monolith input.
```

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "feat: complete monolith production topology"
```

## Execution Order and Review Gates

1. Complete Tasks 1-2 and review storage contracts before changing stream or
   network behavior.
2. Complete Tasks 3-5 and review direct-port boundaries before creating the
   composition root.
3. Complete Tasks 6-8 and review OAuth, listener exposure, readiness, and
   shutdown before packaging.
4. Complete Task 9 before advertising PowerMonitor as an external app.
5. Complete Tasks 10-11 only after all focused tests are green. Do not remove
   retired files earlier.

## Safe Release Procedure

1. Build and verify the release image and signed checksums in a clean CI
   environment.
2. For a new monolith environment, provision empty platform and internal
   volumes, run `--config-check`, then `--migrate-only`, then start the image.
3. For an existing monolith upgrade, stop the single instance, create the
   verified SQLite backup or Timescale restore point, run `migrate.sh`, wait
   for `/healthz`, and execute `scripts/e2e-monolith.sh` against staging.
4. Do not use this plan to convert an existing four-service production
   runtime. That conversion needs a separately approved offline migration
   design.
5. If a monolith-to-monolith upgrade fails before readiness, stop it, restore
   the pre-migration platform backup and matching internal-state backup, then
   deploy the previous monolith image. Do not resume traffic against a partial
   migration.
