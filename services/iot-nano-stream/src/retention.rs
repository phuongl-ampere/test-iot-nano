use crate::{Offset, PartitionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStats {
    pub partition: PartitionId,
    pub earliest_offset: Offset,
    pub next_offset: Offset,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamStats {
    pub total_bytes: u64,
    pub partitions: Vec<PartitionStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionResult {
    pub deleted_records: u64,
    pub deleted_bytes: u64,
}
