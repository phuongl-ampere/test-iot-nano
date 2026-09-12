use crate::{PartitionId, StreamConfig};

// Partition selection remains stable while record persistence moves to SQLite.
pub(crate) fn partition_for(partition_key: &str, config: &StreamConfig) -> PartitionId {
    let partition = crc32fast::hash(partition_key.as_bytes()) % u32::from(config.partitions);
    PartitionId::new(partition as u16)
}
