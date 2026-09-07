#![forbid(unsafe_code)]

mod config;
mod group;
mod record;
mod retention;
mod segment;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use iot_core::TelemetryValidationError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use config::StreamConfig;
pub use group::{
    GroupAssignment, GroupPartitionStats, GroupStart, GroupStats, PartitionCommit, PollBatch,
    StreamConsumer,
};
pub use record::{AppendedRecord, StreamRecord, TelemetryMessage};
pub use retention::{PartitionStats, RetentionResult, StreamStats};

pub type Offset = u64;

#[derive(Debug, Clone)]
pub struct LocalStream {
    inner: Arc<StreamInner>,
}

#[derive(Debug)]
struct StreamInner {
    root: PathBuf,
    config: StreamConfig,
    capacity_lock: Mutex<()>,
    partitions: Vec<Mutex<segment::PartitionLog>>,
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
    #[error("stream partition lock was poisoned")]
    LockPoisoned,
    #[error("segment {path} is corrupt: {reason}")]
    CorruptSegment { path: PathBuf, reason: String },
    #[error("invalid consumer group {kind}: {value:?}")]
    InvalidGroupIdentifier { kind: &'static str, value: String },
    #[error("consumer member {member_id:?} is not an active member of group {group:?}")]
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
    #[error(transparent)]
    InvalidTelemetry(#[from] TelemetryValidationError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

impl LocalStream {
    pub fn open(path: impl AsRef<Path>, config: StreamConfig) -> Result<Self, StreamError> {
        config.validate()?;
        let root = path.as_ref().to_path_buf();
        segment::initialize_root(&root, config.partition_count)?;
        let mut partitions = Vec::with_capacity(usize::from(config.partition_count));
        for id in 0..config.partition_count {
            partitions.push(Mutex::new(segment::PartitionLog::open(
                &root,
                PartitionId(id),
                &config,
            )?));
        }

        Ok(Self {
            inner: Arc::new(StreamInner {
                root,
                config,
                capacity_lock: Mutex::new(()),
                partitions,
            }),
        })
    }

    pub fn partition_for(&self, device_id: &str) -> PartitionId {
        let partition =
            crc32fast::hash(device_id.as_bytes()) % u32::from(self.inner.config.partition_count);
        PartitionId(partition as u16)
    }

    pub fn append(&self, message: TelemetryMessage) -> Result<AppendedRecord, StreamError> {
        message.event.validate_for_topic(&message.topic)?;
        let encoded = record::encode_message(&message)?;
        if encoded.len() > self.inner.config.max_record_bytes {
            return Err(StreamError::RecordTooLarge {
                encoded_bytes: encoded.len(),
                max_bytes: self.inner.config.max_record_bytes,
            });
        }

        let frame_bytes = u64::try_from(encoded.len())
            .map_err(|_| StreamError::RecordTooLarge {
                encoded_bytes: encoded.len(),
                max_bytes: self.inner.config.max_record_bytes,
            })?
            .checked_add(segment::frame_header_bytes())
            .ok_or_else(|| StreamError::Io(std::io::Error::other("frame length overflow")))?;
        let _capacity_guard = self
            .inner
            .capacity_lock
            .lock()
            .map_err(|_| StreamError::LockPoisoned)?;
        retention::enforce_locked(self, chrono::Utc::now(), frame_bytes, false)?;

        let partition = self.partition_for(&message.event.device_id);
        let mut partition_log = self
            .partition(partition)?
            .lock()
            .map_err(|_| StreamError::LockPoisoned)?;

        partition_log.append(
            &self.inner.root,
            &self.inner.config,
            encoded,
            message.received_at,
        )
    }

    pub fn read_partition(
        &self,
        partition: PartitionId,
        offset: Offset,
        limit: usize,
    ) -> Result<Vec<StreamRecord>, StreamError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let partition_log = self
            .partition(partition)?
            .lock()
            .map_err(|_| StreamError::LockPoisoned)?;
        let (earliest, _) = partition_log.bounds();
        if offset < earliest {
            return Err(StreamError::OffsetOutOfRange {
                partition,
                requested: offset,
                earliest,
            });
        }
        partition_log.read(offset, limit)
    }

    pub fn join_group(
        &self,
        group: &str,
        member_id: &str,
        start: GroupStart,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<StreamConsumer, StreamError> {
        group::join_group(self.clone(), group, member_id, start, now)
    }

    pub fn enforce_retention(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<RetentionResult, StreamError> {
        let _capacity_guard = self
            .inner
            .capacity_lock
            .lock()
            .map_err(|_| StreamError::LockPoisoned)?;
        retention::enforce_locked(self, now, 0, true)
    }

    pub fn stats(&self) -> Result<StreamStats, StreamError> {
        retention::stats(self)
    }

    fn partition(
        &self,
        partition: PartitionId,
    ) -> Result<&Mutex<segment::PartitionLog>, StreamError> {
        self.inner
            .partitions
            .get(usize::from(partition.0))
            .ok_or(StreamError::InvalidPartition {
                partition: partition.0,
            })
    }

    pub(crate) fn partition_bounds(
        &self,
        partition: PartitionId,
    ) -> Result<(Offset, Offset), StreamError> {
        let partition_log = self
            .partition(partition)?
            .lock()
            .map_err(|_| StreamError::LockPoisoned)?;
        Ok(partition_log.bounds())
    }

    pub(crate) fn partition_count(&self) -> u16 {
        self.inner.config.partition_count
    }

    pub(crate) fn root(&self) -> &Path {
        &self.inner.root
    }
}
