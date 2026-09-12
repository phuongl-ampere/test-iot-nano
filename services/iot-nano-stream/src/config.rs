use std::time::Duration;

use crate::StreamError;

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub partition_count: u16,
    pub segment_max_bytes: u64,
    pub retention_max_bytes: u64,
    pub retention_max_age: Duration,
    pub max_record_bytes: usize,
    pub index_stride: u64,
}

impl StreamConfig {
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

    pub fn for_test(partition_count: u16) -> Self {
        Self {
            partition_count,
            segment_max_bytes: 256,
            retention_max_bytes: 4 * 1024,
            retention_max_age: Duration::from_secs(24 * 60 * 60),
            max_record_bytes: 1024,
            index_stride: 2,
        }
    }

    pub fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        if self.partition_count == 0 {
            return Err(StreamError::InvalidConfig(
                "partition_count must be greater than zero".to_owned(),
            ));
        }
        if self.segment_max_bytes == 0 {
            return Err(StreamError::InvalidConfig(
                "segment_max_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.segment_max_bytes > self.retention_max_bytes {
            return Err(StreamError::InvalidConfig(
                "segment_max_bytes must not exceed retention_max_bytes".to_owned(),
            ));
        }
        if self.retention_max_age.is_zero() {
            return Err(StreamError::InvalidConfig(
                "retention_max_age must be greater than zero".to_owned(),
            ));
        }
        if self.max_record_bytes == 0 {
            return Err(StreamError::InvalidConfig(
                "max_record_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.index_stride == 0 {
            return Err(StreamError::InvalidConfig(
                "index_stride must be greater than zero".to_owned(),
            ));
        }

        Ok(())
    }
}
