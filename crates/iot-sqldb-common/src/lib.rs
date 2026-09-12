#![forbid(unsafe_code)]

use std::{fs, path::Path, time::Duration};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqlitePoolError {
    #[error("SQLite file belongs to another service")]
    ForeignOwnership,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub async fn open_owned_sqlite_pool(
    path: &Path,
    busy_timeout_ms: u64,
    application_id: i64,
) -> Result<SqlitePool, SqlitePoolError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        && !parent.exists()
    {
        fs::create_dir_all(parent)?;
        #[cfg(unix)]
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    }

    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_millis(busy_timeout_ms));
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;

    let existing_application_id = sqlx::query_scalar::<_, i64>("PRAGMA application_id")
        .fetch_one(&pool)
        .await?;
    if existing_application_id != 0 && existing_application_id != application_id {
        return Err(SqlitePoolError::ForeignOwnership);
    }
    match application_id {
        0x4150_4931 => {
            sqlx::query("PRAGMA application_id = 0x41504931")
                .execute(&pool)
                .await?;
        }
        0x434F_5231 => {
            sqlx::query("PRAGMA application_id = 0x434F5231")
                .execute(&pool)
                .await?;
        }
        _ => return Err(SqlitePoolError::ForeignOwnership),
    }
    sqlx::query("PRAGMA auto_vacuum = INCREMENTAL")
        .execute(&pool)
        .await?;

    Ok(pool)
}
