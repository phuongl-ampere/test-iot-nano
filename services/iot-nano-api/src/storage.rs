use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_sqldb_common::{SqlitePoolError, open_owned_sqlite_pool};
use sqlx::{Executor, PgPool, SqlitePool, postgres::PgPoolOptions};
use thiserror::Error;

const API_SQLITE_APPLICATION_ID: i64 = 0x4150_4931;
const API_SQLITE_SCHEMA: &str = include_str!("api_sqlite_schema.sql");
const API_POSTGRES_SCHEMA: &str = include_str!("../migrations/0001_api.sql");

pub struct ApiSqliteStore {
    pool: SqlitePool,
}

#[derive(Debug, Error)]
pub enum ApiSqliteStoreError {
    #[error("API storage requires an SQLite configuration with a file path")]
    InvalidConfiguration,
    #[error("SQLite file belongs to another service")]
    ForeignOwnership,
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

impl ApiSqliteStore {
    pub async fn open(configuration: &StorageConfiguration) -> Result<Self, ApiSqliteStoreError> {
        if configuration.storage != DatabaseStorage::Sqlite {
            return Err(ApiSqliteStoreError::InvalidConfiguration);
        }
        let path = configuration
            .sqlite_path
            .as_ref()
            .ok_or(ApiSqliteStoreError::InvalidConfiguration)?;
        let pool = open_owned_sqlite_pool(
            path,
            configuration.sqlite_busy_timeout_ms,
            API_SQLITE_APPLICATION_ID,
        )
        .await
        .map_err(|error| match error {
            SqlitePoolError::ForeignOwnership => ApiSqliteStoreError::ForeignOwnership,
            error => ApiSqliteStoreError::Pool(error),
        })?;
        sqlx::raw_sql(API_SQLITE_SCHEMA).execute(&pool).await?;

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

pub async fn migrate_api(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS iot_nano_api")
        .execute(pool)
        .await?;
    let mut connection = pool.acquire().await?;
    connection
        .execute("SET search_path TO iot_nano_api")
        .await?;
    sqlx::raw_sql(API_POSTGRES_SCHEMA)
        .execute(&mut *connection)
        .await?;
    Ok(())
}

pub async fn connect_api_database(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                connection
                    .execute("SET search_path TO iot_nano_api")
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
}
