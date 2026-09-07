use chrono::{DateTime, Utc};

use crate::{LocalStream, Offset, PartitionId, StreamError};

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
    pub deleted_segments: usize,
    pub deleted_bytes: u64,
}

pub(crate) fn enforce_locked(
    stream: &LocalStream,
    now: DateTime<Utc>,
    requested_bytes: u64,
    apply_age_retention: bool,
) -> Result<RetentionResult, StreamError> {
    let mut partitions = stream
        .inner
        .partitions
        .iter()
        .map(|partition| partition.lock().map_err(|_| StreamError::LockPoisoned))
        .collect::<Result<Vec<_>, _>>()?;
    let retention_age = chrono::Duration::from_std(stream.inner.config.retention_max_age)
        .map_err(|error| StreamError::InvalidConfig(error.to_string()))?;
    let cutoff = now - retention_age;
    let mut result = RetentionResult {
        deleted_segments: 0,
        deleted_bytes: 0,
    };

    if apply_age_retention {
        loop {
            let candidate = partitions
                .iter()
                .enumerate()
                .filter_map(|(partition_index, partition)| {
                    partition
                        .oldest_closed_segment()
                        .filter(|(_, newest)| *newest < cutoff)
                        .map(|(segment_index, newest)| (partition_index, segment_index, newest))
                })
                .min_by_key(|(_, _, newest)| *newest);
            let Some((partition_index, segment_index, _)) = candidate else {
                break;
            };
            let deleted = partitions[partition_index].remove_closed_segment(segment_index)?;
            result.deleted_segments += 1;
            result.deleted_bytes = result
                .deleted_bytes
                .checked_add(deleted)
                .ok_or_else(|| StreamError::Io(std::io::Error::other("retention byte overflow")))?;
        }
    }

    let mut current_bytes = total_bytes(&partitions)?;
    while current_bytes
        .checked_add(requested_bytes)
        .is_none_or(|bytes| bytes > stream.inner.config.retention_max_bytes)
    {
        let candidate = partitions
            .iter()
            .enumerate()
            .filter_map(|(partition_index, partition)| {
                partition
                    .oldest_closed_segment()
                    .map(|(segment_index, newest)| (partition_index, segment_index, newest))
            })
            .min_by_key(|(_, _, newest)| *newest);
        let Some((partition_index, segment_index, _)) = candidate else {
            return Err(StreamError::CapacityExceeded {
                max_bytes: stream.inner.config.retention_max_bytes,
                current_bytes,
                requested_bytes,
            });
        };
        let deleted = partitions[partition_index].remove_closed_segment(segment_index)?;
        result.deleted_segments += 1;
        result.deleted_bytes = result
            .deleted_bytes
            .checked_add(deleted)
            .ok_or_else(|| StreamError::Io(std::io::Error::other("retention byte overflow")))?;
        current_bytes = total_bytes(&partitions)?;
    }

    Ok(result)
}

pub(crate) fn stats(stream: &LocalStream) -> Result<StreamStats, StreamError> {
    let mut partitions = Vec::with_capacity(stream.inner.partitions.len());
    let mut total_bytes = 0_u64;
    for (index, partition) in stream.inner.partitions.iter().enumerate() {
        let partition = partition.lock().map_err(|_| StreamError::LockPoisoned)?;
        let (earliest_offset, next_offset) = partition.bounds();
        let bytes = partition.bytes();
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| StreamError::Io(std::io::Error::other("stream byte overflow")))?;
        partitions.push(PartitionStats {
            partition: PartitionId::new(index as u16),
            earliest_offset,
            next_offset,
            bytes,
        });
    }

    Ok(StreamStats {
        total_bytes,
        partitions,
    })
}

fn total_bytes(
    partitions: &[std::sync::MutexGuard<'_, crate::segment::PartitionLog>],
) -> Result<u64, StreamError> {
    partitions.iter().try_fold(0_u64, |total, partition| {
        total
            .checked_add(partition.bytes())
            .ok_or_else(|| StreamError::Io(std::io::Error::other("stream byte overflow")))
    })
}
