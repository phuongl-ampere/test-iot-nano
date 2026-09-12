use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::StreamError;

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub path: PathBuf,
    pub partitions: u16,
    pub retention_max_bytes: u64,
    pub retention_max_age: Duration,
    pub max_record_bytes: usize,
    pub busy_timeout: Duration,
    pub lease_duration: Duration,
}

impl StreamConfig {
    pub fn sqlite(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            partitions: 8,
            retention_max_bytes: 2 * 1024 * 1024 * 1024,
            retention_max_age: Duration::from_secs(24 * 60 * 60),
            max_record_bytes: 1024 * 1024,
            busy_timeout: Duration::from_secs(5),
            lease_duration: Duration::from_secs(5 * 60),
        }
    }

    pub fn with_partitions(mut self, partitions: u16) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn with_max_record_bytes(mut self, max_record_bytes: usize) -> Self {
        self.max_record_bytes = max_record_bytes;
        self
    }

    pub fn with_lease_duration(mut self, lease_duration: Duration) -> Self {
        self.lease_duration = lease_duration;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        if self.path.as_os_str().is_empty() {
            return Err(StreamError::InvalidConfig(
                "path must not be empty".to_owned(),
            ));
        }
        if self.partitions == 0 {
            return Err(StreamError::InvalidConfig(
                "partitions must be greater than zero".to_owned(),
            ));
        }
        if self.retention_max_bytes == 0 {
            return Err(StreamError::InvalidConfig(
                "retention_max_bytes must be greater than zero".to_owned(),
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
        if self.busy_timeout.is_zero() {
            return Err(StreamError::InvalidConfig(
                "busy_timeout must be greater than zero".to_owned(),
            ));
        }
        if self.lease_duration.is_zero() {
            return Err(StreamError::InvalidConfig(
                "lease_duration must be greater than zero".to_owned(),
            ));
        }

        Ok(())
    }
}
