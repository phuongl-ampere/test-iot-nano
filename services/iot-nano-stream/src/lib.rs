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
        self.inner.accepting.store(false, Ordering::Release);
        loop {
            let remaining = self
                .blocking(|store| store.inflight_count(chrono::Utc::now().timestamp_millis()))
                .await?;
            if remaining == 0 {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(StreamError::DrainTimeout { remaining });
            }
            tokio::time::sleep((deadline - now).min(Duration::from_millis(25))).await;
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
