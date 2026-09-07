# Single-Node Rust Event Stream Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQLite WAL work queue with a local Rust append-only
telemetry stream and a lease-based `timescaledb-writer` consumer group, while
preserving MQTT QoS 1 at-least-once delivery into TimescaleDB.

**Architecture:** `iot-ingest` remains the MQTT subscriber. Every valid MQTT
publish selects one of eight stable partitions from `device_id`, appends a
checksummed record to a local segment log, calls `sync_data`, and only then
acknowledges NanoMQ. A `timescaledb-writer` group in the same process
coordinates members through locked, atomically written metadata files; it
commits each partition's next offset only after a TimescaleDB transaction
commits.

**Tech Stack:** Rust 1.96, standard-library file I/O, `fs2` advisory file
locks, `crc32fast`, Serde JSON, Tokio, rumqttc, SQLx, TimescaleDB, NanoMQ.

## Global Constraints

- Deploy on one Raspberry Pi and one local SSD. This is not a distributed
  broker and has no replication, network stream protocol, or consensus layer.
- Keep topic `iot/v1/devices/{device_id}/telemetry`, MQTT QoS 1, and
  `retain=false`.
- Keep at-least-once delivery. The TimescaleDB unique key
  `(event_at, device_id, boot_id, sequence)` remains the duplicate boundary.
- Append and `sync_data` a valid record before acknowledging MQTT.
- Use eight fixed partitions, selected by CRC-32 of UTF-8 `device_id` modulo
  `8`; partition count is immutable after stream initialization.
- Use 128 MiB maximum segment files, 2 GiB maximum retained stream bytes,
  one-day stream retention, a 1 MiB maximum encoded record, and a sparse
  index entry every 128 records.
- A consumer lease lasts 30 seconds and a member heartbeats every 10 seconds.
  Commits require the current assignment generation and current lease owner.
- Retention is Kafka-like and independent of consumer offsets. A lagging
  group can receive `OffsetOutOfRange`; expose lag and earliest offsets.
- Do not migrate `queue.db`. Drain it to zero using the existing binary before
  deployment, then retain it until the new stream smoke test completes.
- Preserve `#![forbid(unsafe_code)]` in local Rust crates. Use no local
  `unsafe` code or memory mapping.
- Use TDD: every behavior change starts with a focused test that fails.

---

## File Structure

```text
Cargo.toml                                  Workspace members and shared crates
crates/iot-core/                            Telemetry schema and validation only
crates/iot-stream/
  Cargo.toml                                Local stream crate dependencies
  src/lib.rs                                Public stream, record, group, and error exports
  src/config.rs                             Stream configuration and validation
  src/record.rs                             Telemetry envelope and frame codec
  src/segment.rs                            Append, recovery, sparse index, reads
  src/group.rs                              Group metadata, leases, offsets, commits
  src/retention.rs                          Closed segment deletion and stream statistics
  tests/segment_log.rs                      Durability, order, recovery, capacity tests
  tests/consumer_group.rs                   Assignment, expiry, commit, replay tests
  tests/retention.rs                        Age/size retention and offset-expiry tests
crates/iot-ingest/
  src/mqtt.rs                               MQTT producer writes LocalStream before ACK
  src/writer.rs                             Writer polls and commits StreamConsumer offsets
  src/metrics.rs                            Stream and group Prometheus metrics
  src/main.rs                               Stream configuration and runtime integration
  tests/mqtt_consumer.rs                    MQTT-to-stream tests
  tests/writer.rs                           DB transaction and offset-commit tests
  tests/e2e.rs                              NanoMQ -> stream -> group -> TimescaleDB test
  tests/metrics.rs                          Metric rendering tests
README.md                                   Updated architecture
docs/operations.md                          New environment variables and alerts
docs/rush-iot-nano-architecture.drawio      Updated architecture diagram
scripts/e2e-local.sh                        Uses --stream-dir
scripts/verify-failures.sh                  Runs stream and revised integration tests
infra/systemd/iot-ingest.service            Describes durable stream service
docs/superpowers/specs/2026-09-04-iot-telemetry-ingestion-design.md
                                             Replaces obsolete SQLite statements
```

Remove only in Task 8, after all ingest callers use `iot-stream`:

```text
crates/iot-core/src/queue.rs
crates/iot-core/tests/queue.rs
```

### Task 1: Create the Stream Crate and Its Public Contract

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/iot-stream/Cargo.toml`
- Create: `crates/iot-stream/src/lib.rs`
- Create: `crates/iot-stream/src/config.rs`
- Create: `crates/iot-stream/src/record.rs`
- Create: `crates/iot-stream/tests/segment_log.rs`

**Interfaces:**
- Consumes `iot_core::TelemetryEvent`.
- Produces `LocalStream::open(path, StreamConfig)`,
  `LocalStream::append(TelemetryMessage) -> Result<AppendedRecord, StreamError>`,
  and `LocalStream::partition_for(&str) -> PartitionId`.
- Produces `TelemetryMessage`, `StreamRecord`, `StreamConfig`,
  `StreamStats`, `PartitionStats`, `PartitionId`, `Offset`, and
  `StreamError`.

- [x] **Step 1: Write failing contract tests**

```rust
#[test]
fn device_id_selects_a_stable_fixed_partition() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(8)).unwrap();

    assert_eq!(stream.partition_for("esp-000123").get(), 4);
    assert_eq!(
        stream.partition_for("esp-000123"),
        stream.partition_for("esp-000123")
    );
}

#[test]
fn stream_rejects_an_event_larger_than_the_record_limit() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(
        directory.path(),
        StreamConfig::for_test(1).with_max_record_bytes(32),
    )
    .unwrap();

    let error = stream.append(message_with_payload(vec![b'x'; 33])).unwrap_err();

    assert!(matches!(error, StreamError::RecordTooLarge { .. }));
}
```

- [x] **Step 2: Run the test and verify it fails**

Run: `cargo test -p iot-stream --test segment_log device_id_selects_a_stable_fixed_partition`

Expected: failure because `iot-stream`, `LocalStream`, and `StreamConfig` do
not exist.

- [x] **Step 3: Add the workspace member, dependencies, and public types**

Add `"crates/iot-stream"` to workspace `members` and add:

```toml
[workspace.dependencies]
crc32fast = "1.5.0"
fs2 = "0.4.3"
```

Create `crates/iot-stream/Cargo.toml`:

```toml
[package]
name = "iot-stream"
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
chrono.workspace = true
crc32fast.workspace = true
fs2.workspace = true
iot-core = { path = "../iot-core" }
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true

[dev-dependencies]
tempfile.workspace = true
uuid.workspace = true
```

Define this boundary in `crates/iot-stream/src/lib.rs`:

```rust
#![forbid(unsafe_code)]

pub type Offset = u64;

#[derive(Debug, Clone)]
pub struct LocalStream {
    inner: Arc<StreamInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PartitionId(u16);

impl PartitionId {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub event: TelemetryEvent,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub partition: PartitionId,
    pub offset: Offset,
    pub message: TelemetryMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedRecord {
    pub partition: PartitionId,
    pub offset: Offset,
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("invalid stream configuration: {0}")]
    InvalidConfig(String),
    #[error("encoded record is {encoded_bytes} bytes; maximum is {max_bytes}")]
    RecordTooLarge { encoded_bytes: usize, max_bytes: usize },
    #[error("stream capacity {max_bytes} exceeded by {current_bytes} + {requested_bytes} bytes")]
    CapacityExceeded { max_bytes: u64, current_bytes: u64, requested_bytes: u64 },
    #[error(transparent)]
    InvalidTelemetry(#[from] TelemetryValidationError),
    #[error("offset {requested} for partition {partition:?} precedes retained offset {earliest}")]
    OffsetOutOfRange { partition: PartitionId, requested: Offset, earliest: Offset },
    #[error("consumer group assignment generation changed")]
    StaleGeneration,
    #[error("corrupt segment {path}: {reason}")]
    CorruptSegment { path: PathBuf, reason: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub partition_count: u16,
    pub segment_max_bytes: u64,
    pub retention_max_bytes: u64,
    pub retention_max_age: Duration,
    pub max_record_bytes: usize,
    pub index_stride: u64,
}
```

Do not remove the legacy queue in this task. It remains compiled until Task 8
so the existing writer and service runtime remain buildable while the stream
library is developed and tested independently.

- [x] **Step 4: Implement deterministic config validation and partitioning**

Implement:

```rust
impl LocalStream {
    pub fn partition_for(&self, device_id: &str) -> PartitionId {
        let partition = crc32fast::hash(device_id.as_bytes())
            % u32::from(self.config.partition_count);
        PartitionId(partition as u16)
    }
}

impl StreamConfig {
    pub fn for_test(partition_count: u16) -> Self {
        Self {
            partition_count,
            segment_max_bytes: 256,
            retention_max_bytes: 4096,
            retention_max_age: Duration::from_secs(24 * 60 * 60),
            max_record_bytes: 1024,
            index_stride: 2,
        }
    }

    pub fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    pub fn production() -> Self {
        Self {
            partition_count: 8,
            segment_max_bytes: 128 * 1024 * 1024,
            retention_max_bytes: 2 * 1024 * 1024 * 1024,
            retention_max_age: Duration::from_secs(24 * 60 * 60),
            max_record_bytes: 1024 * 1024,
            index_stride: 128,
        }
    }
}
```

Reject zero partitions, a segment larger than retention, zero index stride,
and a zero record limit with distinct `StreamError::InvalidConfig` messages.

- [x] **Step 5: Verify contract tests**

Run: `cargo test -p iot-stream --test segment_log`

Expected: all Task 1 tests pass.

- [x] **Step 6: Commit the isolated stream contract**

```bash
git add Cargo.toml Cargo.lock crates/iot-stream
git commit -m "feat: add local stream contract"
```

### Task 2: Implement Durable Partition Segment Logs

**Files:**
- Modify: `crates/iot-stream/src/lib.rs`
- Create: `crates/iot-stream/src/segment.rs`
- Modify: `crates/iot-stream/src/record.rs`
- Modify: `crates/iot-stream/tests/segment_log.rs`

**Interfaces:**
- Consumes validated `TelemetryMessage`.
- Produces `LocalStream::append`, `LocalStream::read_partition`,
  `LocalStream::stats`, and segment-tail recovery on `open`.
- `append` returns `AppendedRecord { partition, offset }` only after the log
  data is durable.

- [x] **Step 1: Write failing append, reopen, rotation, and recovery tests**

```rust
#[test]
fn append_survives_reopen_and_preserves_partition_offset_order() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    assert_eq!(stream.append(message(1)).unwrap().offset, 0);
    assert_eq!(stream.append(message(2)).unwrap().offset, 1);
    drop(stream);

    let reopened = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    let records = reopened.read_partition(PartitionId::new(0), 0, 10).unwrap();

    assert_eq!(records.iter().map(|record| record.message.event.sequence).collect::<Vec<_>>(), [1, 2]);
}

#[test]
fn open_discards_only_a_truncated_tail_frame() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();
    stream.append(message(1)).unwrap();
    append_incomplete_frame(&directory.path().join("partitions/0000/00000000000000000000.log"));
    drop(stream);

    let reopened = LocalStream::open(directory.path(), StreamConfig::for_test(1)).unwrap();

    assert_eq!(reopened.stats().unwrap().partitions[0].next_offset, 1);
}
```

- [x] **Step 2: Run the focused test and verify it fails**

Run: `cargo test -p iot-stream --test segment_log append_survives_reopen_and_preserves_partition_offset_order`

Expected: failure because append, read, and segment recovery are absent.

- [x] **Step 3: Implement the on-disk layout and append protocol**

Use this exact layout:

```text
${IOT_STREAM_DIR}/
  manifest.json
  partitions/
    0000/
      00000000000000000000.log
      00000000000000000000.idx
```

Use a framed log record:

```text
u32 little-endian payload_length
u32 little-endian crc32(payload)
payload = serde_json(StreamRecord without partition and offset)
```

Write the frame with `write_all`, call `File::sync_data`, then append the
fixed-width sparse index pair `(offset: u64, byte_position: u64)` every 128
records and call `sync_data` on the index. The partition lock serializes
append and segment rotation. Never acknowledge an MQTT message until this
method returns:

```rust
pub fn append(&self, message: TelemetryMessage) -> Result<AppendedRecord, StreamError> {
    message.event.validate_for_topic(&message.topic)?;
    let encoded = encode_message(&message)?;
    self.partition(self.partition_for(&message.event.device_id))
        .lock()
        .expect("partition mutex poisoned")
        .append_synced(encoded, message)
}
```

- [x] **Step 4: Implement recovery, index rebuild, and segment rotation**

At `open`, create and `sync_all` the manifest if absent. If present, require
`format_version == 1` and an exact partition-count match. Scan each segment
in base-offset order:

```rust
match read_frame(&mut log, position, max_record_bytes) {
    Ok(Some(frame)) => rebuild_index_if_due(frame.offset, position),
    Ok(None) => break,
    Err(FrameError::Truncated) if is_active_segment => {
        log.set_len(position)?;
        log.sync_all()?;
        break;
    }
    Err(error) => return Err(StreamError::CorruptSegment { path, reason: error.to_string() }),
}
```

Reject CRC mismatch, invalid JSON, non-contiguous offsets, or a malformed
closed segment. Rotate before an append that would exceed
`segment_max_bytes`; create the next `.log` and `.idx` files, sync them and
their directory, then make them active.

- [x] **Step 5: Verify segment behavior**

Run: `cargo test -p iot-stream --test segment_log`

Expected: append/reopen, sparse-index seek, segment rotation, truncated-tail
recovery, closed-segment corruption rejection, and oversized-frame rejection
all pass.

- [x] **Step 6: Commit the durable log implementation**

```bash
git add crates/iot-stream
git commit -m "feat: add durable partition segment logs"
```

### Task 3: Implement the Lease-Based Consumer Group

**Files:**
- Modify: `crates/iot-stream/src/lib.rs`
- Create: `crates/iot-stream/src/group.rs`
- Create: `crates/iot-stream/tests/consumer_group.rs`

**Interfaces:**
- Produces `LocalStream::join_group(group, member, GroupStart, now)`.
- Produces `StreamConsumer::heartbeat(now)`, `poll(limit, now)`,
  `commit(PollBatch, now)`, and `group_stats(now)`.
- `PollBatch` has contiguous records per partition and a generation captured
  at poll time. `commit` rejects a stale generation or expired lease.

- [x] **Step 1: Write failing consumer-group tests**

```rust
#[test]
fn two_members_receive_disjoint_partition_leases() {
    let stream = stream_with_records_for_all_partitions();
    let now = fixed_now();
    let mut first = stream.join_group("timescaledb-writer", "writer-a", GroupStart::Earliest, now).unwrap();
    let mut second = stream.join_group("timescaledb-writer", "writer-b", GroupStart::Earliest, now).unwrap();

    let first_partitions = first.heartbeat(now).unwrap().partitions;
    let second_partitions = second.heartbeat(now).unwrap().partitions;

    assert!(first_partitions.iter().all(|partition| !second_partitions.contains(partition)));
    assert_eq!(first_partitions.len() + second_partitions.len(), 8);
}

#[test]
fn expired_member_lease_reassigns_partition_from_last_committed_offset() {
    let stream = stream_with_two_records();
    let now = fixed_now();
    let mut first = stream.join_group("timescaledb-writer", "writer-a", GroupStart::Earliest, now).unwrap();
    let batch = first.poll(1, now).unwrap();
    first.commit(batch, now).unwrap();

    let mut replacement = stream
        .join_group("timescaledb-writer", "writer-b", GroupStart::Earliest, now + chrono::Duration::seconds(31))
        .unwrap();
    let replay = replacement.poll(10, now + chrono::Duration::seconds(31)).unwrap();

    assert_eq!(replay.records[0].offset, 1);
}
```

- [x] **Step 2: Run the group test and verify it fails**

Run: `cargo test -p iot-stream --test consumer_group two_members_receive_disjoint_partition_leases`

Expected: failure because no group metadata or member lease API exists.

- [x] **Step 3: Implement durable group metadata and atomic updates**

Use one state directory per group:

```text
${IOT_STREAM_DIR}/groups/timescaledb-writer/
  state.lock
  state.json
```

Define and serialize:

```rust
#[derive(Debug, Serialize, Deserialize)]
struct GroupState {
    format_version: u16,
    generation: u64,
    members: BTreeMap<String, MemberState>,
    assignments: BTreeMap<PartitionId, PartitionLease>,
    next_offsets: BTreeMap<PartitionId, Offset>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MemberState {
    heartbeat_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PartitionLease {
    member_id: String,
    expires_at: DateTime<Utc>,
}
```

Every read-modify-write must take an exclusive `fs2::FileExt::lock_exclusive`
on `state.lock`, write `state.json.tmp`, call `sync_all`, rename it to
`state.json`, then `sync_all` the containing directory. On first group join,
initialize every partition offset to the stream's earliest offset for
`GroupStart::Earliest`, or its next offset for `GroupStart::Latest`.

- [x] **Step 4: Implement heartbeat, assignment, rebalance, and commit guards**

On each `join_group` or `heartbeat`:

```rust
fn refresh_group(state: &mut GroupState, now: DateTime<Utc>, partitions: u16) {
    state.members.retain(|_, member| member.heartbeat_at + LEASE_DURATION > now);
    let active_members = state.members.keys().cloned().collect::<Vec<_>>();
    let membership_changed = assignments_need_rebalance(state, &active_members, now);

    if membership_changed {
        state.generation += 1;
        state.assignments = round_robin_leases(active_members, partitions, now + LEASE_DURATION);
    }
}
```

The sorted active member IDs own partitions round-robin. `poll` reads only
partitions leased to its member and returns records in offset order within each
partition. Every successful heartbeat must extend its assigned leases to
`now + LEASE_DURATION` and copy the persisted `generation` into
`StreamConsumer`. `commit` must reject with
`StreamError::StaleGeneration` unless:

```rust
lease.member_id == self.member_id
    && lease.expires_at > now
    && self.generation == state.generation
    && batch.generation == state.generation
```

Commit the *next* offset, not the last processed offset. It must be monotonic
and no greater than the partition high watermark. If a DB transaction commits
but the guarded stream commit fails, return the stream error; replay is safe
because TimescaleDB deduplicates the event identity.

- [x] **Step 5: Add negative-path group tests**

```rust
#[test]
fn stale_member_cannot_commit_after_rebalance() {
    let (stream, mut old_member, now) = joined_stream();
    let batch = old_member.poll(1, now).unwrap();
    stream.join_group("timescaledb-writer", "new-member", GroupStart::Earliest, now).unwrap();

    let error = old_member.commit(batch, now).unwrap_err();

    assert!(matches!(error, StreamError::StaleGeneration { .. }));
}

#[test]
fn groups_keep_independent_offsets() {
    let stream = stream_with_two_records();
    let now = fixed_now();
    let mut writer = stream.join_group("timescaledb-writer", "writer", GroupStart::Earliest, now).unwrap();
    let mut alerts = stream.join_group("alerts", "alerts", GroupStart::Earliest, now).unwrap();
    writer.commit(writer.poll(1, now).unwrap(), now).unwrap();

    assert_eq!(alerts.poll(10, now).unwrap().records[0].offset, 0);
}
```

- [x] **Step 6: Verify consumer-group behavior**

Run: `cargo test -p iot-stream --test consumer_group`

Expected: independent groups, disjoint active-member partition leases,
expired-member reassignment, stale commit rejection, and replay from the
committed next offset all pass.

- [x] **Step 7: Commit consumer-group coordination**

```bash
git add crates/iot-stream
git commit -m "feat: add lease based stream consumer groups"
```

### Task 4: Add Retention, Capacity Backpressure, and Stream Observability

**Files:**
- Modify: `crates/iot-stream/src/lib.rs`
- Create: `crates/iot-stream/src/retention.rs`
- Modify: `crates/iot-stream/src/segment.rs`
- Create: `crates/iot-stream/tests/retention.rs`

**Interfaces:**
- Produces `LocalStream::enforce_retention(now) -> Result<RetentionResult, StreamError>`.
- Produces `LocalStream::stats() -> Result<StreamStats, StreamError>` and
  `StreamConsumer::group_stats(now) -> Result<GroupStats, StreamError>`.
- `poll` returns `StreamError::OffsetOutOfRange { partition, requested,
  earliest }` when a committed offset was removed by retention.

- [x] **Step 1: Write failing retention and capacity tests**

```rust
#[test]
fn retention_deletes_only_closed_segments_and_advances_earliest_offset() {
    let (stream, now) = stream_with_rotated_segments();

    let result = stream.enforce_retention(now + chrono::Duration::days(8)).unwrap();
    let stats = stream.stats().unwrap();

    assert!(result.deleted_segments > 0);
    assert_eq!(stats.partitions[0].earliest_offset, 2);
    assert!(active_segment_exists(&stream, PartitionId::new(0)));
}

#[test]
fn append_returns_capacity_error_without_modifying_the_log() {
    let stream = LocalStream::open(tempdir(), tiny_capacity_config()).unwrap();
    stream.append(message(1)).unwrap();

    let error = stream.append(message(2)).unwrap_err();

    assert!(matches!(error, StreamError::CapacityExceeded { .. }));
    assert_eq!(stream.stats().unwrap().partitions[0].next_offset, 1);
}

#[test]
fn lagging_group_gets_an_expired_offset_error_after_retention() {
    let (stream, now) = stream_with_rotated_segments();
    let mut consumer = stream.join_group("alerts", "alerts-a", GroupStart::Earliest, now).unwrap();
    stream.enforce_retention(now + chrono::Duration::days(8)).unwrap();

    let error = consumer.poll(1, now + chrono::Duration::days(8)).unwrap_err();

    assert!(matches!(error, StreamError::OffsetOutOfRange { .. }));
}
```

- [x] **Step 2: Run retention tests and verify they fail**

Run: `cargo test -p iot-stream --test retention`

Expected: failure because retention, capacity accounting, and group lag are
not implemented.

- [x] **Step 3: Implement retention and capacity behavior**

Prune only closed segments. Delete the oldest closed segment when either its
newest received timestamp is older than `retention_max_age` or total retained
bytes exceed `retention_max_bytes`; keep deleting eligible oldest closed
segments until both limits are satisfied. Do not inspect group offsets before
deleting. This matches log-retention semantics and makes lag an operational
concern rather than silently preserving data forever.

Call `enforce_retention` before a producer capacity check. If no deletable
closed segment can free enough space, return:

```rust
StreamError::CapacityExceeded {
    max_bytes: self.config.retention_max_bytes,
    current_bytes,
    requested_bytes,
}
```

Do not write a partial frame and let MQTT remain unacknowledged. Deleting a
segment must remove its `.log` and `.idx`, then `sync_all` its partition
directory.

- [x] **Step 4: Implement stats and lag calculations**

Expose:

```rust
pub struct PartitionStats {
    pub partition: PartitionId,
    pub earliest_offset: Offset,
    pub next_offset: Offset,
    pub bytes: u64,
}

pub struct StreamStats {
    pub total_bytes: u64,
    pub partitions: Vec<PartitionStats>,
}

pub struct GroupPartitionStats {
    pub partition: PartitionId,
    pub committed_next_offset: Offset,
    pub high_watermark: Offset,
    pub lag: u64,
}

pub struct GroupStats {
    pub group: String,
    pub generation: u64,
    pub partitions: Vec<GroupPartitionStats>,
}

impl GroupStats {
    pub fn total_lag(&self) -> u64 {
        self.partitions.iter().map(|partition| partition.lag).sum()
    }

    pub fn committed_offset(&self, partition: PartitionId) -> Offset {
        self.partitions
            .iter()
            .find(|stats| stats.partition == partition)
            .map(|stats| stats.committed_next_offset)
            .expect("group contains all stream partitions")
    }
}
```

`high_watermark` is `next_offset`; `lag` is
`high_watermark.saturating_sub(committed_next_offset)`. Validate the requested
offset against each partition's earliest offset before opening its sparse
index; report `OffsetOutOfRange` instead of clamping or silently resetting.

- [x] **Step 5: Verify retention and all stream tests**

Run: `cargo test -p iot-stream`

Expected: all segment, group, retention, recovery, and capacity tests pass.

- [x] **Step 6: Commit retention and observability**

```bash
git add crates/iot-stream
git commit -m "feat: add stream retention and lag metrics"
```

### Task 5: Make MQTT Ingestion Produce to the Durable Stream

**Files:**
- Modify: `crates/iot-ingest/Cargo.toml`
- Modify: `crates/iot-ingest/src/lib.rs`
- Modify: `crates/iot-ingest/src/mqtt.rs`
- Modify: `crates/iot-ingest/tests/mqtt_consumer.rs`

**Interfaces:**
- Consumes `LocalStream` and valid MQTT `Publish` packets.
- Replaces `MqttQueueConsumer` with `MqttStreamProducer`.
- `MqttRuntime::new(MqttRuntimeConfig, LocalStream)` retains manual ACK
  behavior.
- Invalid telemetry produces `IngestOutcome::Rejected`; capacity, I/O, or
  corruption errors return `MqttRuntimeError` and do not ACK that packet.

- [x] **Step 1: Write failing MQTT-to-stream tests**

```rust
#[test]
fn valid_mqtt_payload_is_durably_appended_to_the_stream() {
    let directory = tempfile::tempdir().unwrap();
    let stream = LocalStream::open(directory.path(), StreamConfig::for_test(8)).unwrap();
    let producer = MqttStreamProducer::new(stream.clone());

    let outcome = producer.ingest(TOPIC, &telemetry_payload("esp-000123"), fixed_now()).unwrap();
    let partition = stream.partition_for("esp-000123");

    assert_eq!(outcome, IngestOutcome::Accepted);
    assert_eq!(stream.read_partition(partition, 0, 10).unwrap().len(), 1);
}

#[test]
fn topic_payload_mismatch_is_rejected_without_appending() {
    let stream = LocalStream::open(tempdir(), StreamConfig::for_test(8)).unwrap();
    let producer = MqttStreamProducer::new(stream.clone());

    let outcome = producer.ingest(TOPIC, &telemetry_payload("esp-000456"), fixed_now()).unwrap();

    assert_eq!(outcome, IngestOutcome::Rejected);
    assert!(stream
        .stats()
        .unwrap()
        .partitions
        .iter()
        .all(|partition| partition.next_offset == 0));
}
```

Keep the existing Mosquitto integration test and change its final assertion to
read the record from the stream after `poll_once` returns `Accepted`.

- [x] **Step 2: Run the focused MQTT test and verify it fails**

Run: `cargo test -p iot-ingest --test mqtt_consumer valid_mqtt_payload_is_durably_appended_to_the_stream`

Expected: failure because `iot-ingest` does not depend on `iot-stream` and
`MqttStreamProducer` does not exist.

- [x] **Step 3: Replace queue ownership with stream producer ownership**

Add the local dependency:

```toml
[dependencies]
iot-stream = { path = "../iot-stream" }
```

Replace the queue-specific implementation with:

```rust
pub struct MqttStreamProducer {
    stream: LocalStream,
}

impl MqttStreamProducer {
    pub fn ingest(
        &self,
        topic: &str,
        payload: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<IngestOutcome, MqttConsumerError> {
        let event = match serde_json::from_slice::<TelemetryEvent>(payload) {
            Ok(event) => event,
            Err(_) => return Ok(IngestOutcome::Rejected),
        };
        let message = TelemetryMessage {
            topic: topic.to_owned(),
            payload: payload.to_vec(),
            event,
            received_at,
        };

        match self.stream.append(message) {
            Ok(_) => Ok(IngestOutcome::Accepted),
            Err(StreamError::InvalidTelemetry(_)) => Ok(IngestOutcome::Rejected),
            Err(error) => Err(MqttConsumerError::Stream(error)),
        }
    }
}
```

Remove `queue_mut`, `queue_stats`, `LocalQueue`, and `QueueError` from
`mqtt.rs`. `MqttRuntime` owns a `MqttStreamProducer`; its `poll_once` sequence
remains:

```rust
let outcome = self.producer.ingest(&publish.topic, &publish.payload, received_at)?;
self.client.ack(&publish).await?;
Ok(Some(outcome))
```

This exact order is the ACK guarantee. Do not ACK in the error branch.

- [x] **Step 4: Verify unit and live-broker producer behavior**

Run: `cargo test -p iot-ingest --test mqtt_consumer`

Expected: valid events are in the stream, invalid events are rejected without
append, and QoS 1 packets are acknowledged only after durable append.

- [x] **Step 5: Commit MQTT producer migration**

```bash
git add crates/iot-ingest/Cargo.toml crates/iot-ingest/src crates/iot-ingest/tests/mqtt_consumer.rs Cargo.lock
git commit -m "feat: append MQTT telemetry to local stream"
```

### Task 6: Consume and Commit the `timescaledb-writer` Group

**Files:**
- Modify: `crates/iot-ingest/src/writer.rs`
- Modify: `crates/iot-ingest/src/lib.rs`
- Modify: `crates/iot-ingest/tests/writer.rs`

**Interfaces:**
- Consumes `&mut StreamConsumer`.
- `TelemetryWriter::flush_once(&mut StreamConsumer, DateTime<Utc>)` writes at
  most `batch_size` records, commits the PostgreSQL transaction, then commits
  the returned `PollBatch`.
- `FlushResult` exposes `read`, `inserted`, `duplicates`, and
  `committed_partitions`.

- [x] **Step 1: Write failing database and offset tests**

```rust
#[tokio::test]
async fn flush_commits_group_offsets_only_after_the_database_transaction() {
    let pool = prepared_pool().await;
    let (stream, mut consumer) = writer_consumer_with_duplicate_events();
    let writer = TelemetryWriter::new(pool.clone(), 1_000);

    let result = writer.flush_once(&mut consumer, fixed_now()).await.unwrap();

    assert_eq!(result.read, 2);
    assert_eq!(result.inserted, 1);
    assert_eq!(result.duplicates, 1);
    assert_eq!(consumer.group_stats(fixed_now()).unwrap().total_lag(), 0);
}

#[tokio::test]
async fn writer_does_not_advance_offset_when_database_conversion_fails() {
    let pool = prepared_pool().await;
    let (stream, mut consumer) = writer_consumer_with_sequence(u64::MAX);
    let writer = TelemetryWriter::new(pool, 1_000);
    let partition = stream.partition_for("esp-000123");

    assert!(writer.flush_once(&mut consumer, fixed_now()).await.is_err());
    assert_eq!(consumer.group_stats(fixed_now()).unwrap().committed_offset(partition), 0);
}
```

- [x] **Step 2: Run the focused writer test and verify it fails**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test writer flush_commits_group_offsets_only_after_the_database_transaction -- --test-threads=1`

Expected: failure because `TelemetryWriter` still receives `LocalQueue`.

- [x] **Step 3: Replace leasing/deletion with poll/commit**

Use this control flow:

```rust
pub async fn flush_once(
    &self,
    consumer: &mut StreamConsumer,
    now: DateTime<Utc>,
) -> Result<FlushResult, WriterError> {
    let batch = consumer.poll(self.batch_size, now)?;
    if batch.records.is_empty() {
        return Ok(FlushResult::empty());
    }

    let mut transaction = self.pool.begin().await?;
    let inserted = insert_all(&mut transaction, &batch.records).await?;
    transaction.commit().await?;

    let committed_partitions = consumer.commit(batch, now)?;
    Ok(FlushResult::from_records(inserted, committed_partitions))
}
```

Keep the current SQL text, device last-seen upsert, `i64::try_from(sequence)`
guard, and `ON CONFLICT DO NOTHING` behavior. Remove all references to
`LeasedTelemetry`, queue IDs, `lease_batch`, and `complete`. Add
`WriterError::Stream(#[from] StreamError)`.

- [x] **Step 4: Verify writer success, duplicate, and failure behavior**

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test writer -- --test-threads=1`

Expected: migration tests remain green; one transaction writes duplicates
idempotently; a database/conversion failure leaves the group offset unchanged;
and a post-commit stream error is replay-safe.

- [x] **Step 5: Commit the stream writer**

```bash
git add crates/iot-ingest/src/writer.rs crates/iot-ingest/src/lib.rs crates/iot-ingest/tests/writer.rs
git commit -m "feat: consume stream with timescaledb writer group"
```

### Task 7: Run the Stream, Group, and Metrics in `iot-ingest`

**Files:**
- Modify: `crates/iot-ingest/src/main.rs`
- Modify: `crates/iot-ingest/src/metrics.rs`
- Modify: `crates/iot-ingest/tests/metrics.rs`
- Modify: `crates/iot-ingest/tests/e2e.rs`

**Interfaces:**
- Replaces `--queue-path` / `IOT_QUEUE_PATH` with `--stream-dir` /
  `IOT_STREAM_DIR`.
- Starts the `timescaledb-writer` consumer group with one local member.
- Exposes Prometheus stream size, high-watermark, earliest offset, committed
  offset, lag, group generation, and existing ingest/failure counters.

- [x] **Step 1: Write failing runtime metrics and end-to-end tests**

```rust
#[test]
fn metrics_render_partition_watermarks_and_writer_group_lag() {
    let metrics = IngestMetrics::default();
    metrics.update_stream(StreamStats {
        total_bytes: 4096,
        partitions: vec![
            PartitionStats { partition: PartitionId::new(0), earliest_offset: 4, next_offset: 12, bytes: 4096 },
        ],
    });
    metrics.update_group(GroupStats {
        group: "timescaledb-writer".to_owned(),
        generation: 3,
        partitions: vec![
            GroupPartitionStats {
                partition: PartitionId::new(0),
                committed_next_offset: 8,
                high_watermark: 12,
                lag: 4,
            },
        ],
    });

    let rendered = metrics.render_prometheus();

    assert!(rendered.contains("iot_ingest_stream_log_bytes 4096"));
    assert!(rendered.contains("iot_ingest_stream_high_watermark{partition=\"0\"} 12"));
    assert!(rendered.contains("iot_ingest_stream_group_lag{group=\"timescaledb-writer\",partition=\"0\"} 4"));
}
```

In `e2e.rs`, replace local queue construction with:

```rust
let stream = LocalStream::open(tempdir.path().join("stream"), StreamConfig::for_test(8)).unwrap();
let mut consumer = stream
    .join_group("timescaledb-writer", "e2e-writer", GroupStart::Earliest, Utc::now())
    .unwrap();
let mut runtime = MqttRuntime::new(mqtt_config, stream);
```

After two accepted simulator messages, call `writer.flush_once(&mut consumer,
Utc::now())`, then assert two rows in `telemetry` and zero writer-group lag.

- [x] **Step 2: Run focused tests and verify they fail**

Run: `cargo test -p iot-ingest --test metrics metrics_render_partition_watermarks_and_writer_group_lag`

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test e2e -- --test-threads=1`

Expected: failures because runtime configuration, metrics snapshots, and E2E
code still refer to the SQLite queue.

- [x] **Step 3: Replace queue CLI configuration and initialize the group**

Replace `queue_path` and `queue_max_bytes` in `Arguments` with:

```rust
#[arg(long, env = "IOT_STREAM_DIR", default_value = "/var/lib/iot-ingest/stream")]
stream_dir: PathBuf,
#[arg(long, env = "IOT_STREAM_PARTITIONS", default_value_t = 8)]
stream_partitions: u16,
#[arg(long, env = "IOT_STREAM_SEGMENT_BYTES", default_value_t = 128 * 1024 * 1024)]
stream_segment_bytes: u64,
#[arg(long, env = "IOT_STREAM_RETENTION_BYTES", default_value_t = 2 * 1024 * 1024 * 1024)]
stream_retention_bytes: u64,
#[arg(long, env = "IOT_STREAM_RETENTION_SECONDS", default_value_t = 24 * 60 * 60)]
stream_retention_seconds: u64,
#[arg(long, env = "IOT_STREAM_MAX_RECORD_BYTES", default_value_t = 1024 * 1024)]
stream_max_record_bytes: usize,
#[arg(long, env = "IOT_WRITER_GROUP", default_value = "timescaledb-writer")]
writer_group: String,
#[arg(long, env = "IOT_WRITER_MEMBER_ID")]
writer_member_id: Option<String>,
```

Initialize shared ownership once:

```rust
let stream = LocalStream::open(arguments.stream_dir, StreamConfig {
    partition_count: arguments.stream_partitions,
    segment_max_bytes: arguments.stream_segment_bytes,
    retention_max_bytes: arguments.stream_retention_bytes,
    retention_max_age: std::time::Duration::from_secs(arguments.stream_retention_seconds),
    max_record_bytes: arguments.stream_max_record_bytes,
    index_stride: 128,
})?;
let member_id = arguments.writer_member_id.unwrap_or_else(default_member_id);
let mut writer_consumer = stream.join_group(
    &arguments.writer_group,
    &member_id,
    GroupStart::Earliest,
    Utc::now(),
)?;
let mut runtime = MqttRuntime::new(mqtt_config, stream.clone());
```

`default_member_id` must return
`format!("{}-{}", std::env::var("HOSTNAME").unwrap_or_else(|_| "iot-ingest".into()), std::process::id())`.

- [x] **Step 4: Add writer heartbeat, retention, and metric update ticks**

Keep the existing one-second writer tick. Add a 10-second heartbeat tick and
a 60-second retention tick:

```rust
let mut heartbeat_tick = interval(Duration::from_secs(10));
let mut retention_tick = interval(Duration::from_secs(60));

tokio::select! {
    _ = heartbeat_tick.tick() => writer_consumer.heartbeat(Utc::now())?,
    _ = retention_tick.tick() => {
        let result = stream.enforce_retention(Utc::now())?;
        eprintln!("stream retention deleted {} segments", result.deleted_segments);
    }
}
```

After every MQTT, writer, heartbeat, and retention branch, update the
snapshot used by `/metrics`:

```rust
metrics.update_stream(stream.stats()?);
metrics.update_group(writer_consumer.group_stats(Utc::now())?);
```

Keep database error counting only for `WriterError::Database`; count
`WriterError::Stream` with a separate
`iot_ingest_stream_failures_total` metric and leave its offset uncommitted.

- [x] **Step 5: Replace queue metrics with bounded stream snapshots**

Store `StreamStats` and `GroupStats` in
`Mutex<Option<...>>` fields in `IngestMetrics`; retain atomics for accepted,
rejected, database failures, and stream failures. Render these exact metrics:

```text
iot_ingest_stream_log_bytes
iot_ingest_stream_earliest_offset{partition="N"}
iot_ingest_stream_high_watermark{partition="N"}
iot_ingest_stream_group_committed_offset{group="timescaledb-writer",partition="N"}
iot_ingest_stream_group_lag{group="timescaledb-writer",partition="N"}
iot_ingest_stream_group_generation{group="timescaledb-writer"}
```

Do not retain or render obsolete `iot_ingest_queue_ready`,
`iot_ingest_queue_leased`, or `iot_ingest_queue_bytes`.

- [x] **Step 6: Verify service-level behavior**

Run: `cargo test -p iot-ingest --test metrics`

Run: `DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot cargo test -p iot-ingest --test e2e -- --test-threads=1`

Expected: metrics show all eight partition watermarks and the writer group;
the E2E test confirms NanoMQ -> durable stream -> consumer group ->
TimescaleDB and ends with group lag zero.

- [x] **Step 7: Commit runtime integration**

```bash
git add crates/iot-ingest/src crates/iot-ingest/tests
git commit -m "feat: run stream consumer group in ingest service"
```

### Task 8: Update Deployment Assets, Documentation, and Failure Checks

**Files:**
- Modify: `crates/iot-core/Cargo.toml`
- Modify: `crates/iot-core/src/lib.rs`
- Delete: `crates/iot-core/src/queue.rs`
- Delete: `crates/iot-core/tests/queue.rs`
- Modify: `README.md`
- Modify: `docs/operations.md`
- Modify: `docs/rush-iot-nano-architecture.drawio`
- Modify: `docs/rush-iot-nano-architecture.drawio.png`
- Modify: `docs/superpowers/specs/2026-09-04-iot-telemetry-ingestion-design.md`
- Modify: `infra/systemd/iot-ingest.service`
- Modify: `scripts/e2e-local.sh`
- Modify: `scripts/verify-failures.sh`

**Interfaces:**
- Operators set `IOT_STREAM_DIR` and stream retention configuration, not
  `IOT_QUEUE_PATH`.
- The local smoke script starts `iot-ingest --stream-dir "$stream_dir"` and
  verifies 16 simulator events arrive in TimescaleDB.
- Documentation describes the exact topology:
  `ESP32 -> NanoMQ -> iot-ingest -> iot-stream -> timescaledb-writer -> TimescaleDB`.

- [x] **Step 1: Write a shell-level failing smoke assertion**

Replace the local variable and service invocation in `scripts/e2e-local.sh`:

```bash
stream_dir="$(mktemp -d)"

DATABASE_URL="$database_url" target/debug/iot-ingest \
  --broker-host 127.0.0.1 \
  --broker-port 1883 \
  --stream-dir "$stream_dir" \
  --health-address 127.0.0.1:18081 &
```

Leave the final `test "$count" = "16"` assertion unchanged. Run it before
implementation so it fails on the unsupported `--stream-dir` flag.

- [x] **Step 2: Update operator-facing configuration and alerts**

In `docs/operations.md`, replace the queue environment variable and alert
guidance with:

```text
DATABASE_URL, MQTT_BROKER_HOST, MQTT_BROKER_PORT, IOT_STREAM_DIR,
IOT_STREAM_PARTITIONS, IOT_STREAM_SEGMENT_BYTES,
IOT_STREAM_RETENTION_BYTES, IOT_STREAM_RETENTION_SECONDS,
IOT_STREAM_MAX_RECORD_BYTES, IOT_WRITER_GROUP, and IOT_INGEST_HEALTH_ADDRESS
```

Document these alerts:

```text
iot_ingest_stream_group_lag > 0 for 30 minutes
iot_ingest_stream_log_bytes > 80% of IOT_STREAM_RETENTION_BYTES
iot_ingest_stream_failures_total increases
iot_ingest_database_failures_total increases for five minutes
```

Update `README.md` to name the stream and its writer group. Update the
systemd description from "durable MQTT ingest service" to "durable MQTT
stream ingest service"; leave `StateDirectory=iot-ingest` unchanged so
`/var/lib/iot-ingest/stream` remains on persistent storage.

- [x] **Step 3: Update the architecture source and local export**

In `docs/rush-iot-nano-architecture.drawio`, replace the SQLite WAL node with
an `iot-stream (Rust)` node labeled:

```text
8 fixed partitions
append-only segments + sparse indexes
fsync before MQTT ACK
1d / 2 GiB retention
```

Add a `timescaledb-writer consumer group` node between stream and TimescaleDB
labeled:

```text
lease + heartbeat
per-partition offsets
commit after PostgreSQL transaction
```

Export locally, repair the embedded PNG, and validate source structure:

```bash
drawio -x -f png -e --width 2000 -o docs/rush-iot-nano-architecture.drawio.png docs/rush-iot-nano-architecture.drawio
python3 /Users/phuongl/.codex/skills/drawio-skill/scripts/repair_png.py docs/rush-iot-nano-architecture.drawio.png
python3 /Users/phuongl/.codex/skills/drawio-skill/scripts/validate.py docs/rush-iot-nano-architecture.drawio
```

- [x] **Step 4: Reconcile historical design and failure script**

In the design document, replace the `Local queue` and SQLite/WAL sections
with the segment-log, consumer-group, retention, and offset semantics defined
in this plan. Preserve the existing QoS 1, TimescaleDB uniqueness, SSD, and
throughput requirements.

In `scripts/verify-failures.sh`, replace:

```bash
cargo test -p iot-core --test queue
```

with:

```bash
cargo test -p iot-stream
cargo test -p iot-ingest --test mqtt_consumer
```

Keep writer and E2E tests serialized with `--test-threads=1`.

Now remove the legacy queue only after `cargo test --workspace` succeeds with
the new stream integration:

```toml
# crates/iot-core/Cargo.toml
[dependencies]
chrono.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
uuid.workspace = true
```

```rust
// crates/iot-core/src/lib.rs
#![forbid(unsafe_code)]

mod telemetry;

pub use telemetry::{TELEMETRY_TOPIC_PREFIX, TelemetryEvent, TelemetryValidationError};
```

Delete `crates/iot-core/src/queue.rs` and
`crates/iot-core/tests/queue.rs`, then verify no `LocalQueue`, `QueueLimits`,
`QueueError`, `QueueStats`, `IOT_QUEUE_PATH`, or `--queue-path` references
remain outside the historical implementation plan.

- [x] **Step 5: Run the complete verification suite**

Run:

```bash
cargo fmt --check
cargo test --workspace
DATABASE_URL=postgres://iot:iot@127.0.0.1:54329/iot scripts/verify-failures.sh
scripts/e2e-local.sh
npm --prefix web test
npm --prefix web run build
```

Expected: Rust formatting and all workspace tests pass; stream/group failure
tests pass against local TimescaleDB; the local end-to-end script reports
`End-to-end telemetry rows: 16`; web tests and production build pass.

- [x] **Step 6: Commit operational migration**

```bash
git add README.md docs infra scripts Cargo.lock
git commit -m "docs: document local telemetry stream operations"
```

## Rollout and Rollback

1. Stop the existing `iot-ingest` service after its `/metrics` reports
   `iot_ingest_queue_ready 0` and `iot_ingest_queue_leased 0`.
2. Keep `/var/lib/iot-ingest/queue.db` intact and install the new binary.
3. Set `IOT_STREAM_DIR=/var/lib/iot-ingest/stream`, start the service, and
   verify it creates eight partition directories and group state for
   `timescaledb-writer`.
4. Publish one QoS 1 test event and verify stream high watermark advances,
   group committed offset catches up, group lag returns to zero, and one
   TimescaleDB row is present.
5. Retain `queue.db` until the next successful backup and retention cycle.
   It is not read by the new binary.
6. If stream startup, disk capacity, or group commits fail, stop the new
   binary, restore the previous binary and `IOT_QUEUE_PATH`, then restart the
   old service. NanoMQ's persistent QoS 1 session redelivers messages that
   were not acknowledged by the new service.
