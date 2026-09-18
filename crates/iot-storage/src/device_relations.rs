use std::{future::Future, pin::Pin};

use sqlx::{Postgres, Row, Sqlite, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::{PlatformStore, PlatformStoreError};

pub const RESERVED_GATEWAY_CHILD_RELATION_TYPE: &str = "gateway_child";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRelation {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub from_device_id: String,
    pub to_device_id: String,
    pub relation_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDeviceRelation {
    pub from_device_id: String,
    pub to_device_id: String,
    pub relation_type: String,
}

#[derive(Debug, Error)]
pub enum DeviceRelationError {
    #[error("invalid relation device ID: {0:?}")]
    InvalidDeviceId(String),
    #[error("invalid device relation type: {0:?}")]
    InvalidRelationType(String),
    #[error("gateway_child is reserved for gateway topology")]
    ReservedRelationType,
    #[error("a device relation cannot reference the same device twice")]
    SelfRelation,
    #[error("device relation endpoint is unavailable: {device_id:?}")]
    DeviceNotFound { device_id: String },
    #[error("device relation already exists")]
    RelationConflict,
    #[error("device relation was not found")]
    RelationNotFound,
    #[error("stored device relation is invalid")]
    InvalidStoredRelation,
    #[error("device relation storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for DeviceRelationError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for DeviceRelationError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait DeviceRelationRepository: Send + Sync {
    fn list_device_relations<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<DeviceRelation>, DeviceRelationError>> + Send + 'a>>;
    fn create_device_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        relation: CreateDeviceRelation,
    ) -> Pin<Box<dyn Future<Output = Result<DeviceRelation, DeviceRelationError>> + Send + 'a>>;
    fn delete_device_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        relation_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DeviceRelationError>> + Send + 'a>>;
}

impl DeviceRelationRepository for PlatformStore {
    fn list_device_relations<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<DeviceRelation>, DeviceRelationError>> + Send + 'a>>
    {
        Box::pin(async move { list_device_relations(self, tenant_id).await })
    }

    fn create_device_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        relation: CreateDeviceRelation,
    ) -> Pin<Box<dyn Future<Output = Result<DeviceRelation, DeviceRelationError>> + Send + 'a>>
    {
        Box::pin(async move { create_device_relation(self, tenant_id, relation).await })
    }

    fn delete_device_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        relation_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DeviceRelationError>> + Send + 'a>> {
        Box::pin(async move { delete_device_relation(self, tenant_id, relation_id).await })
    }
}

async fn list_device_relations(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<DeviceRelation>, DeviceRelationError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, tenant_id, from_device_id, to_device_id, relation_type
             FROM device_relations
             WHERE tenant_id = ?
             ORDER BY relation_type, from_device_id, to_device_id, id",
        )
        .bind(tenant_id.to_string())
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .map(sqlite_relation_from_row)
        .collect(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, tenant_id, from_device_id, to_device_id, relation_type
             FROM device_relations
             WHERE tenant_id = $1
             ORDER BY relation_type, from_device_id, to_device_id, id",
        )
        .bind(tenant_id)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(timescale_relation_from_row)
        .collect(),
    }
}

async fn create_device_relation(
    store: &PlatformStore,
    tenant_id: Uuid,
    relation: CreateDeviceRelation,
) -> Result<DeviceRelation, DeviceRelationError> {
    let relation = validate_new_relation(relation)?;
    let id = Uuid::now_v7();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
            sqlite_require_tenant_device(&mut transaction, tenant_id, &relation.from_device_id)
                .await?;
            sqlite_require_tenant_device(&mut transaction, tenant_id, &relation.to_device_id)
                .await?;
            sqlx::query(
                "INSERT INTO device_relations (
                    id, tenant_id, from_device_id, to_device_id, relation_type
                 ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(tenant_id.to_string())
            .bind(&relation.from_device_id)
            .bind(&relation.to_device_id)
            .bind(&relation.relation_type)
            .execute(&mut *transaction)
            .await
            .map_err(map_relation_conflict)?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            timescale_require_tenant_device(&mut transaction, tenant_id, &relation.from_device_id)
                .await?;
            timescale_require_tenant_device(&mut transaction, tenant_id, &relation.to_device_id)
                .await?;
            sqlx::query(
                "INSERT INTO device_relations (
                    id, tenant_id, from_device_id, to_device_id, relation_type
                 ) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(id)
            .bind(tenant_id)
            .bind(&relation.from_device_id)
            .bind(&relation.to_device_id)
            .bind(&relation.relation_type)
            .execute(&mut *transaction)
            .await
            .map_err(map_relation_conflict)?;
            transaction.commit().await?;
        }
    }
    Ok(DeviceRelation {
        id,
        tenant_id,
        from_device_id: relation.from_device_id,
        to_device_id: relation.to_device_id,
        relation_type: relation.relation_type,
    })
}

async fn delete_device_relation(
    store: &PlatformStore,
    tenant_id: Uuid,
    relation_id: Uuid,
) -> Result<bool, DeviceRelationError> {
    let deleted = match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query("DELETE FROM device_relations WHERE id = ? AND tenant_id = ?")
                .bind(relation_id.to_string())
                .bind(tenant_id.to_string())
                .execute(store.pool())
                .await?
                .rows_affected()
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query("DELETE FROM device_relations WHERE id = $1 AND tenant_id = $2")
                .bind(relation_id)
                .bind(tenant_id)
                .execute(pool)
                .await?
                .rows_affected()
        }
    };
    if deleted == 0 {
        return Err(DeviceRelationError::RelationNotFound);
    }
    Ok(true)
}

fn validate_new_relation(
    relation: CreateDeviceRelation,
) -> Result<CreateDeviceRelation, DeviceRelationError> {
    let from_device_id = relation.from_device_id.trim();
    let to_device_id = relation.to_device_id.trim();
    let relation_type = relation.relation_type.trim();
    if !device_identifier(from_device_id) {
        return Err(DeviceRelationError::InvalidDeviceId(
            relation.from_device_id,
        ));
    }
    if !device_identifier(to_device_id) {
        return Err(DeviceRelationError::InvalidDeviceId(relation.to_device_id));
    }
    if from_device_id == to_device_id {
        return Err(DeviceRelationError::SelfRelation);
    }
    if relation_type == RESERVED_GATEWAY_CHILD_RELATION_TYPE {
        return Err(DeviceRelationError::ReservedRelationType);
    }
    if !relation_identifier(relation_type) {
        return Err(DeviceRelationError::InvalidRelationType(
            relation.relation_type,
        ));
    }
    Ok(CreateDeviceRelation {
        from_device_id: from_device_id.to_owned(),
        to_device_id: to_device_id.to_owned(),
        relation_type: relation_type.to_owned(),
    })
}

fn device_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn relation_identifier(value: &str) -> bool {
    device_identifier(value)
}

async fn sqlite_require_tenant_device(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceRelationError> {
    let found = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM devices
         WHERE tenant_id = ? AND device_id = ? AND deleted_at IS NULL",
    )
    .bind(tenant_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if found {
        Ok(())
    } else {
        Err(DeviceRelationError::DeviceNotFound {
            device_id: device_id.to_owned(),
        })
    }
}

async fn timescale_require_tenant_device(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: &str,
) -> Result<(), DeviceRelationError> {
    let found = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM devices
         WHERE tenant_id = $1 AND device_id = $2 AND deleted_at IS NULL
         FOR KEY SHARE",
    )
    .bind(tenant_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if found {
        Ok(())
    } else {
        Err(DeviceRelationError::DeviceNotFound {
            device_id: device_id.to_owned(),
        })
    }
}

fn map_relation_conflict(error: sqlx::Error) -> DeviceRelationError {
    if error
        .as_database_error()
        .is_some_and(|database| database.is_unique_violation())
    {
        DeviceRelationError::RelationConflict
    } else {
        DeviceRelationError::from(error)
    }
}

fn sqlite_relation_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<DeviceRelation, DeviceRelationError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse()
        .map_err(|_| DeviceRelationError::InvalidStoredRelation)?;
    let tenant_id = row
        .try_get::<String, _>("tenant_id")?
        .parse()
        .map_err(|_| DeviceRelationError::InvalidStoredRelation)?;
    let from_device_id = row.try_get::<String, _>("from_device_id")?;
    let to_device_id = row.try_get::<String, _>("to_device_id")?;
    let relation_type = row.try_get::<String, _>("relation_type")?;
    if !device_identifier(&from_device_id)
        || !device_identifier(&to_device_id)
        || from_device_id == to_device_id
        || !relation_identifier(&relation_type)
        || relation_type == RESERVED_GATEWAY_CHILD_RELATION_TYPE
    {
        return Err(DeviceRelationError::InvalidStoredRelation);
    }
    Ok(DeviceRelation {
        id,
        tenant_id,
        from_device_id,
        to_device_id,
        relation_type,
    })
}

fn timescale_relation_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<DeviceRelation, DeviceRelationError> {
    let relation = DeviceRelation {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        from_device_id: row.try_get("from_device_id")?,
        to_device_id: row.try_get("to_device_id")?,
        relation_type: row.try_get("relation_type")?,
    };
    if !device_identifier(&relation.from_device_id)
        || !device_identifier(&relation.to_device_id)
        || relation.from_device_id == relation.to_device_id
        || !relation_identifier(&relation.relation_type)
        || relation.relation_type == RESERVED_GATEWAY_CHILD_RELATION_TYPE
    {
        return Err(DeviceRelationError::InvalidStoredRelation);
    }
    Ok(relation)
}
