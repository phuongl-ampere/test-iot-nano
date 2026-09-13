#![forbid(unsafe_code)]

mod config;
mod group;
pub mod maintenance;
mod record;
mod retention;
mod segment;
mod sqlite_store;

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use thiserror::Error;

pub use config::StreamConfig;
pub use group::{
    AcknowledgeRequest, ClaimRequest, ClaimedRecord, GroupAssignment, GroupPartitionStats,
    GroupStart, GroupStats, HeartbeatRequest, PartitionCommit,
};
pub use record::{
    AppendReceipt, GatewayEvent, GatewayEventKind, GatewayMessage, StreamMessage, StreamRecord,
    TelemetryMessage,
};
pub use retention::{PartitionStats, RetentionResult, StreamStats};

pub type AppendedRecord = AppendReceipt;
pub type Offset = u64;

#[derive(Clone)]
pub struct LocalStream {
    inner: Arc<StreamInner>,
}

struct StreamInner {
    config: StreamConfig,
    store: Arc<sqlite_store::SqliteStore>,
    accepting: AtomicBool,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct PartitionId(u16);

impl PartitionId {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("invalid stream configuration: {0}")]
    InvalidConfig(String),
    #[error("encoded record is {encoded_bytes} bytes; maximum is {max_bytes}")]
    RecordTooLarge {
        encoded_bytes: usize,
        max_bytes: usize,
    },
    #[error("stream capacity {max_bytes} exceeded by {current_bytes} + {requested_bytes} bytes")]
    CapacityExceeded {
        max_bytes: u64,
        current_bytes: u64,
        requested_bytes: u64,
    },
    #[error("partition {partition} is outside the configured partition range")]
    InvalidPartition { partition: u16 },
    #[error("stream store lock was poisoned")]
    LockPoisoned,
    #[error("stream SQLite file belongs to application id {application_id}")]
    ForeignDatabase { application_id: i32 },
    #[error("stream SQLite file contains non-stream table {table:?}")]
    ForeignTable { table: String },
    #[error("stream store is corrupt: {0}")]
    CorruptStore(String),
    #[error("invalid consumer group {kind}: {value:?}")]
    InvalidGroupIdentifier { kind: &'static str, value: String },
    #[error("consumer member {member_id:?} is not active in group {group:?}")]
    GroupMemberNotFound { group: String, member_id: String },
    #[error("consumer member {member_id:?} lease expired in group {group:?}")]
    LeaseExpired { group: String, member_id: String },
    #[error("consumer group assignment generation changed")]
    StaleGeneration,
    #[error("offset {requested} for partition {partition:?} precedes retained offset {earliest}")]
    OffsetOutOfRange {
        partition: PartitionId,
        requested: Offset,
        earliest: Offset,
    },
    #[error(
        "commit for partition {partition:?} is outside [{current}, {high_watermark}]: {requested}"
    )]
    InvalidCommit {
        partition: PartitionId,
        current: Offset,
        requested: Offset,
        high_watermark: Offset,
    },
    #[error(
        "consumer member {member_id:?} has no active in-flight claim for partition {partition:?} in group {group:?}"
    )]
    NoInflightClaim {
        group: String,
        member_id: String,
        partition: PartitionId,
    },
    #[error("stream offset overflowed")]
    OffsetOverflow,
    #[error("stream is draining and no longer accepts claims")]
    Draining,
    #[error("stream drain deadline elapsed with {remaining} in-flight claims")]
    DrainTimeout { remaining: u64 },
    #[error(transparent)]
    InvalidTelemetry(#[from] iot_core::TelemetryValidationError),
    #[error("invalid gateway event")]
    InvalidGatewayEvent,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("stream blocking task failed: {0}")]
    TaskJoin(String),
}

pub trait StreamPort: Send + Sync {
    fn stop_claiming(&self) {}

    fn append(
        &self,
        message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>>;

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>>;

    fn acknowledge(
        &self,
        request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>>;

    fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>>;

    fn drain(
        &self,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>>;
}

impl LocalStream {
    pub async fn open(config: StreamConfig) -> Result<Self, StreamError> {
        config.validate()?;
        let open_config = config.clone();
        let store =
            tokio::task::spawn_blocking(move || sqlite_store::SqliteStore::open(&open_config))
                .await
                .map_err(|error| StreamError::TaskJoin(error.to_string()))??;
        Ok(Self {
            inner: Arc::new(StreamInner {
                config,
                store: Arc::new(store),
                accepting: AtomicBool::new(true),
            }),
        })
    }

    pub fn partition_for(&self, partition_key: &str) -> PartitionId {
        segment::partition_for(partition_key, &self.inner.config)
    }

    pub fn stop_claiming(&self) {
        self.inner.accepting.store(false, Ordering::Release);
    }

    pub async fn append(
        &self,
        message: impl Into<StreamMessage>,
    ) -> Result<AppendReceipt, StreamError> {
        let config = self.inner.config.clone();
        let message = message.into();
        self.blocking(move |store| store.append(&config, message))
            .await
    }

    pub async fn claim(&self, request: ClaimRequest) -> Result<Vec<ClaimedRecord>, StreamError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(StreamError::Draining);
        }
        let config = self.inner.config.clone();
        let inner = self.inner.clone();
        self.blocking(move |store| {
            if !inner.accepting.load(Ordering::Acquire) {
                return Err(StreamError::Draining);
            }
            store.claim(&config, request)
        })
        .await
    }

    pub async fn acknowledge(&self, request: AcknowledgeRequest) -> Result<(), StreamError> {
        let config = self.inner.config.clone();
        self.blocking(move |store| store.acknowledge(&config, request))
            .await
    }

    pub async fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Result<GroupAssignment, StreamError> {
        let config = self.inner.config.clone();
        self.blocking(move |store| store.heartbeat(&config, request))
            .await
    }

    pub async fn enforce_retention(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<RetentionResult, StreamError> {
        let config = self.inner.config.clone();
        self.blocking(move |store| store.enforce_retention(&config, now.timestamp_millis()))
            .await
    }

    pub async fn stats(&self) -> Result<StreamStats, StreamError> {
        let config = self.inner.config.clone();
        self.blocking(move |store| store.stats(&config)).await
    }

    pub async fn drain_until(&self, deadline: Instant) -> Result<(), StreamError> {
        self.stop_claiming();
        let mut remaining = 0;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(StreamError::DrainTimeout { remaining });
            }
            remaining = match tokio::time::timeout(
                deadline - now,
                self.blocking(|store| store.inflight_count(chrono::Utc::now().timestamp_millis())),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => return Err(StreamError::DrainTimeout { remaining }),
            };
            let after_query = Instant::now();
            if after_query >= deadline {
                return Err(StreamError::DrainTimeout { remaining });
            }
            if remaining == 0 {
                return Ok(());
            }
            tokio::time::sleep((deadline - after_query).min(Duration::from_millis(25))).await;
        }
    }

    async fn blocking<T, F>(&self, operation: F) -> Result<T, StreamError>
    where
        T: Send + 'static,
        F: FnOnce(&sqlite_store::SqliteStore) -> Result<T, StreamError> + Send + 'static,
    {
        let store = self.inner.store.clone();
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(|error| StreamError::TaskJoin(error.to_string()))?
    }
}

impl StreamPort for LocalStream {
    fn stop_claiming(&self) {
        LocalStream::stop_claiming(self);
    }

    fn append(
        &self,
        message: StreamMessage,
    ) -> Pin<Box<dyn Future<Output = Result<AppendReceipt, StreamError>> + Send + '_>> {
        Box::pin(LocalStream::append(self, message))
    }

    fn claim(
        &self,
        request: ClaimRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ClaimedRecord>, StreamError>> + Send + '_>> {
        Box::pin(LocalStream::claim(self, request))
    }

    fn acknowledge(
        &self,
        request: AcknowledgeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(LocalStream::acknowledge(self, request))
    }

    fn heartbeat(
        &self,
        request: HeartbeatRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GroupAssignment, StreamError>> + Send + '_>> {
        Box::pin(LocalStream::heartbeat(self, request))
    }

    fn drain(
        &self,
        deadline: Instant,
    ) -> Pin<Box<dyn Future<Output = Result<(), StreamError>> + Send + '_>> {
        Box::pin(LocalStream::drain_until(self, deadline))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn stop_claiming_rejects_claims_started_after_the_stop_signal() {
        let directory = tempdir().unwrap();
        let stream =
            LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
                .await
                .unwrap();

        let claim_started_before_stop = stream
            .claim(ClaimRequest {
                group: "workers".to_owned(),
                member_id: "worker-1".to_owned(),
                start: GroupStart::Earliest,
                limit: 1,
            })
            .await;
        assert!(claim_started_before_stop.is_ok());

        stream.stop_claiming();

        let claim_started_after_stop = stream
            .claim(ClaimRequest {
                group: "workers".to_owned(),
                member_id: "worker-1".to_owned(),
                start: GroupStart::Earliest,
                limit: 1,
            })
            .await;

        assert!(matches!(
            claim_started_after_stop,
            Err(StreamError::Draining)
        ));
    }

    #[tokio::test]
    async fn drain_until_honors_deadline_while_sqlite_work_is_contended() {
        let directory = tempdir().unwrap();
        let stream =
            LocalStream::open(StreamConfig::sqlite(directory.path().join("stream.sqlite")))
                .await
                .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let store = stream.inner.store.clone();
        let blocker = tokio::task::spawn_blocking({
            let barrier = barrier.clone();
            move || {
                let _guard = store.locked().unwrap();
                barrier.wait();
                barrier.wait();
            }
        });

        let barrier_for_ready = barrier.clone();
        tokio::task::spawn_blocking(move || barrier_for_ready.wait())
            .await
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            stream.drain_until(Instant::now() + Duration::from_millis(20)),
        )
        .await;

        let barrier_for_release = barrier.clone();
        tokio::task::spawn_blocking(move || barrier_for_release.wait())
            .await
            .unwrap();
        blocker.await.unwrap();
        assert!(matches!(result, Ok(Err(StreamError::DrainTimeout { .. }))));
    }
}
