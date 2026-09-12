use std::collections::BTreeMap;

use crate::{Offset, PartitionId, StreamError, StreamRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupStart {
    #[default]
    Earliest,
    Latest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignment {
    pub generation: u64,
    pub partitions: Vec<PartitionId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCommit {
    pub partition: PartitionId,
    pub next_offset: Offset,
}

#[derive(Debug, Clone)]
pub struct ClaimRequest {
    pub group: String,
    pub member_id: String,
    pub start: GroupStart,
    pub limit: usize,
}

impl ClaimRequest {
    pub fn new(group: impl Into<String>, member_id: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            member_id: member_id.into(),
            start: GroupStart::Earliest,
            limit: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedRecord {
    pub partition: PartitionId,
    pub offset: Offset,
    pub message: crate::StreamMessage,
    pub generation: u64,
}

impl From<ClaimedRecord> for StreamRecord {
    fn from(record: ClaimedRecord) -> Self {
        Self {
            partition: record.partition,
            offset: record.offset,
            message: record.message,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AcknowledgeRequest {
    pub group: String,
    pub member_id: String,
    pub generation: u64,
    pub commits: Vec<PartitionCommit>,
}

impl AcknowledgeRequest {
    pub fn from_claims(
        group: impl Into<String>,
        member_id: impl Into<String>,
        records: &[ClaimedRecord],
    ) -> Self {
        let mut commits = BTreeMap::new();
        for record in records {
            commits
                .entry(record.partition)
                .and_modify(|next_offset: &mut Offset| {
                    *next_offset = (*next_offset).max(record.offset.saturating_add(1));
                })
                .or_insert_with(|| record.offset.saturating_add(1));
        }
        Self {
            group: group.into(),
            member_id: member_id.into(),
            generation: records.first().map_or(0, |record| record.generation),
            commits: commits
                .into_iter()
                .map(|(partition, next_offset)| PartitionCommit {
                    partition,
                    next_offset,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group: String,
    pub member_id: String,
}

impl HeartbeatRequest {
    pub fn new(group: impl Into<String>, member_id: impl Into<String>) -> Self {
        Self {
            group: group.into(),
            member_id: member_id.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPartitionStats {
    pub partition: PartitionId,
    pub committed_next_offset: Offset,
    pub high_watermark: Offset,
    pub lag: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupStats {
    pub group: String,
    pub generation: u64,
    pub partitions: Vec<GroupPartitionStats>,
}

impl GroupStats {
    pub fn total_lag(&self) -> u64 {
        self.partitions.iter().map(|partition| partition.lag).sum()
    }

    pub fn committed_offset(&self, partition: PartitionId) -> Option<Offset> {
        self.partitions
            .iter()
            .find(|stats| stats.partition == partition)
            .map(|stats| stats.committed_next_offset)
    }
}

pub(crate) fn validate_identifier(kind: &'static str, value: &str) -> Result<(), StreamError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Ok(());
    }

    Err(StreamError::InvalidGroupIdentifier {
        kind,
        value: value.to_owned(),
    })
}
