use std::{collections::BTreeMap, path::PathBuf};

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseStorage {
    Timescale,
    Sqlite,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfiguration {
    pub storage: DatabaseStorage,
    pub database_url: Option<String>,
    pub sqlite_path: Option<PathBuf>,
    pub sqlite_busy_timeout_ms: u64,
}

impl StorageConfiguration {
    pub fn from_values(
        values: &BTreeMap<String, String>,
    ) -> Result<Self, StorageConfigurationError> {
        let storage = values
            .get("IOT_DATABASE_STORAGE")
            .or_else(|| values.get("USE_DATABASE_STORAGE"))
            .map(String::as_str)
            .unwrap_or("timescale");
        match storage {
            "timescale" => {
                let database_url = values
                    .get("DATABASE_URL")
                    .filter(|value| !value.trim().is_empty())
                    .cloned()
                    .ok_or(StorageConfigurationError::MissingDatabaseUrl)?;
                Ok(Self {
                    storage: DatabaseStorage::Timescale,
                    database_url: Some(database_url),
                    sqlite_path: None,
                    sqlite_busy_timeout_ms: 5_000,
                })
            }
            "sqlite" => {
                let sqlite_path = values
                    .get("IOT_SQLITE_PATH")
                    .map(PathBuf::from)
                    .filter(|path| path.is_absolute())
                    .ok_or(StorageConfigurationError::InvalidSqlitePath)?;
                let sqlite_busy_timeout_ms = values
                    .get("IOT_SQLITE_BUSY_TIMEOUT_MS")
                    .map(|value| value.parse::<u64>())
                    .transpose()
                    .map_err(|_| StorageConfigurationError::InvalidBusyTimeout)?
                    .unwrap_or(5_000);
                if sqlite_busy_timeout_ms == 0 || sqlite_busy_timeout_ms > 60_000 {
                    return Err(StorageConfigurationError::InvalidBusyTimeout);
                }
                Ok(Self {
                    storage: DatabaseStorage::Sqlite,
                    database_url: None,
                    sqlite_path: Some(sqlite_path),
                    sqlite_busy_timeout_ms,
                })
            }
            _ => Err(StorageConfigurationError::UnsupportedStorage(
                storage.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StorageConfigurationError {
    #[error("IOT_DATABASE_STORAGE must be `timescale` or `sqlite`, got {0:?}")]
    UnsupportedStorage(String),
    #[error("DATABASE_URL is required when IOT_DATABASE_STORAGE=timescale")]
    MissingDatabaseUrl,
    #[error("IOT_SQLITE_PATH must be an absolute path when IOT_DATABASE_STORAGE=sqlite")]
    InvalidSqlitePath,
    #[error("IOT_SQLITE_BUSY_TIMEOUT_MS must be between 1 and 60000")]
    InvalidBusyTimeout,
}
