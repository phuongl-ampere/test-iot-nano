use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{
    PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction, postgres::PgRow, sqlite::SqliteRow,
    types::Json,
};
use thiserror::Error;
use uuid::Uuid;

use crate::PlatformStore;

const MAX_AUDIT_EVENT_LIMIT: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditPrincipal {
    SystemAccount(Uuid),
    TenantAccount(Uuid),
    User(Uuid),
}

impl AuditPrincipal {
    pub(crate) fn storage_fields(self) -> (&'static str, Uuid) {
        match self {
            Self::SystemAccount(id) => ("system_account", id),
            Self::TenantAccount(id) => ("tenant_account", id),
            Self::User(id) => ("user", id),
        }
    }

    fn from_storage(kind: &str, id: Uuid) -> Result<Self, AuditEventError> {
        match kind {
            "system_account" => Ok(Self::SystemAccount(id)),
            "tenant_account" => Ok(Self::TenantAccount(id)),
            "user" => Ok(Self::User(id)),
            _ => Err(AuditEventError::InvalidStoredEvent),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditAction {
    PermissionGranted,
    PermissionRevoked,
    GroupMemberAdded,
    GroupMemberRemoved,
    OwnershipTransferred,
    AssetContainmentChanged,
    GatewayAssigned,
    GatewayDetached,
    GatewayReassigned,
    DeviceRelationCreated,
    DeviceRelationDeleted,
}

impl AuditAction {
    pub(crate) fn as_storage(self) -> &'static str {
        match self {
            Self::PermissionGranted => "permission.granted",
            Self::PermissionRevoked => "permission.revoked",
            Self::GroupMemberAdded => "group.member_added",
            Self::GroupMemberRemoved => "group.member_removed",
            Self::OwnershipTransferred => "ownership.transferred",
            Self::AssetContainmentChanged => "asset.containment_changed",
            Self::GatewayAssigned => "gateway.assigned",
            Self::GatewayDetached => "gateway.detached",
            Self::GatewayReassigned => "gateway.reassigned",
            Self::DeviceRelationCreated => "device_relation.created",
            Self::DeviceRelationDeleted => "device_relation.deleted",
        }
    }

    fn from_storage(value: &str) -> Result<Self, AuditEventError> {
        match value {
            "permission.granted" => Ok(Self::PermissionGranted),
            "permission.revoked" => Ok(Self::PermissionRevoked),
            "group.member_added" => Ok(Self::GroupMemberAdded),
            "group.member_removed" => Ok(Self::GroupMemberRemoved),
            "ownership.transferred" => Ok(Self::OwnershipTransferred),
            "asset.containment_changed" => Ok(Self::AssetContainmentChanged),
            "gateway.assigned" => Ok(Self::GatewayAssigned),
            "gateway.detached" => Ok(Self::GatewayDetached),
            "gateway.reassigned" => Ok(Self::GatewayReassigned),
            "device_relation.created" => Ok(Self::DeviceRelationCreated),
            "device_relation.deleted" => Ok(Self::DeviceRelationDeleted),
            _ => Err(AuditEventError::InvalidStoredEvent),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditTargetType {
    ResourcePermission,
    UserGroup,
    Asset,
    Device,
    DeviceRelation,
}

impl AuditTargetType {
    pub(crate) fn as_storage(self) -> &'static str {
        match self {
            Self::ResourcePermission => "resource_permission",
            Self::UserGroup => "user_group",
            Self::Asset => "asset",
            Self::Device => "device",
            Self::DeviceRelation => "device_relation",
        }
    }

    fn from_storage(value: &str) -> Result<Self, AuditEventError> {
        match value {
            "resource_permission" => Ok(Self::ResourcePermission),
            "user_group" => Ok(Self::UserGroup),
            "asset" => Ok(Self::Asset),
            "device" => Ok(Self::Device),
            "device_relation" => Ok(Self::DeviceRelation),
            _ => Err(AuditEventError::InvalidStoredEvent),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AuditEvent {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub actor: AuditPrincipal,
    pub action: AuditAction,
    pub target_type: AuditTargetType,
    pub target_id: String,
    pub changes: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct NewAuditEvent {
    pub tenant_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub actor: AuditPrincipal,
    pub action: AuditAction,
    pub target_type: AuditTargetType,
    pub target_id: String,
    pub changes: Value,
}

impl NewAuditEvent {
    pub(crate) fn new(
        tenant_id: Uuid,
        actor: AuditPrincipal,
        action: AuditAction,
        target_type: AuditTargetType,
        target_id: String,
        changes: Value,
    ) -> Self {
        Self {
            tenant_id,
            occurred_at: Utc::now(),
            actor,
            action,
            target_type,
            target_id,
            changes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditEventCursor {
    pub occurred_at: DateTime<Utc>,
    pub id: Uuid,
}

#[derive(Debug, Error)]
pub enum AuditEventError {
    #[error("tenant audit event limit must be between 1 and {MAX_AUDIT_EVENT_LIMIT}, got {limit}")]
    InvalidLimit { limit: usize },
    #[error("stored tenant audit event is invalid")]
    InvalidStoredEvent,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub trait AuditEventRepository: Send + Sync {
    fn list_tenant_audit_events<'a>(
        &'a self,
        tenant_id: Uuid,
        after: Option<AuditEventCursor>,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AuditEvent>, AuditEventError>> + Send + 'a>>;
}

impl AuditEventRepository for PlatformStore {
    fn list_tenant_audit_events<'a>(
        &'a self,
        tenant_id: Uuid,
        after: Option<AuditEventCursor>,
        limit: usize,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AuditEvent>, AuditEventError>> + Send + 'a>> {
        Box::pin(async move { list_tenant_audit_events(self, tenant_id, after, limit).await })
    }
}

pub(crate) async fn sqlite_tenant_account_audit_actor(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
) -> Result<AuditPrincipal, sqlx::Error> {
    let id: String = sqlx::query_scalar("SELECT id FROM tenant_accounts WHERE tenant_id = ?")
        .bind(tenant_id.to_string())
        .fetch_one(&mut **transaction)
        .await?;
    let id = Uuid::parse_str(&id).map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
    Ok(AuditPrincipal::TenantAccount(id))
}

pub(crate) async fn timescale_tenant_account_audit_actor(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
) -> Result<AuditPrincipal, sqlx::Error> {
    let id: Uuid = sqlx::query_scalar("SELECT id FROM tenant_accounts WHERE tenant_id = $1")
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
    Ok(AuditPrincipal::TenantAccount(id))
}

pub(crate) async fn insert_sqlite_audit_event(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &NewAuditEvent,
) -> Result<(), sqlx::Error> {
    let (actor_principal_kind, actor_principal_id) = event.actor.storage_fields();
    sqlx::query(
        "INSERT INTO audit_events (
            id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
            action, target_type, target_id, changes
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(event.tenant_id.to_string())
    .bind(event.occurred_at.to_rfc3339())
    .bind(actor_principal_kind)
    .bind(actor_principal_id.to_string())
    .bind(event.action.as_storage())
    .bind(event.target_type.as_storage())
    .bind(&event.target_id)
    .bind(event.changes.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn insert_timescale_audit_event(
    transaction: &mut Transaction<'_, Postgres>,
    event: &NewAuditEvent,
) -> Result<(), sqlx::Error> {
    let (actor_principal_kind, actor_principal_id) = event.actor.storage_fields();
    sqlx::query(
        "INSERT INTO audit_events (
            id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
            action, target_type, target_id, changes
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(Uuid::now_v7())
    .bind(event.tenant_id)
    .bind(event.occurred_at)
    .bind(actor_principal_kind)
    .bind(actor_principal_id)
    .bind(event.action.as_storage())
    .bind(event.target_type.as_storage())
    .bind(&event.target_id)
    .bind(Json(event.changes.clone()))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn list_tenant_audit_events(
    store: &PlatformStore,
    tenant_id: Uuid,
    after: Option<AuditEventCursor>,
    limit: usize,
) -> Result<Vec<AuditEvent>, AuditEventError> {
    if !(1..=MAX_AUDIT_EVENT_LIMIT).contains(&limit) {
        return Err(AuditEventError::InvalidLimit { limit });
    }
    match store {
        PlatformStore::Sqlite(store) => {
            sqlite_tenant_audit_events(store.pool(), tenant_id, after, limit).await
        }
        PlatformStore::Timescale(pool) => {
            timescale_tenant_audit_events(pool, tenant_id, after, limit).await
        }
    }
}

async fn sqlite_tenant_audit_events(
    pool: &SqlitePool,
    tenant_id: Uuid,
    after: Option<AuditEventCursor>,
    limit: usize,
) -> Result<Vec<AuditEvent>, AuditEventError> {
    let limit = i64::try_from(limit).expect("audit limit fits i64");
    let rows = match after {
        Some(cursor) => {
            sqlx::query(
                "SELECT id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                    action, target_type, target_id, changes
             FROM audit_events
             WHERE tenant_id = ?
               AND (occurred_at < ? OR (occurred_at = ? AND id < ?))
             ORDER BY occurred_at DESC, id DESC
             LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(cursor.occurred_at.to_rfc3339())
            .bind(cursor.occurred_at.to_rfc3339())
            .bind(cursor.id.to_string())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query(
                "SELECT id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                    action, target_type, target_id, changes
             FROM audit_events
             WHERE tenant_id = ?
             ORDER BY occurred_at DESC, id DESC
             LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    rows.into_iter().map(sqlite_audit_event).collect()
}

async fn timescale_tenant_audit_events(
    pool: &PgPool,
    tenant_id: Uuid,
    after: Option<AuditEventCursor>,
    limit: usize,
) -> Result<Vec<AuditEvent>, AuditEventError> {
    let limit = i64::try_from(limit).expect("audit limit fits i64");
    let rows = match after {
        Some(cursor) => {
            sqlx::query(
                "SELECT id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                    action, target_type, target_id, changes
             FROM audit_events
             WHERE tenant_id = $1
               AND (occurred_at < $2 OR (occurred_at = $2 AND id < $3))
             ORDER BY occurred_at DESC, id DESC
             LIMIT $4",
            )
            .bind(tenant_id)
            .bind(cursor.occurred_at)
            .bind(cursor.id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query(
                "SELECT id, tenant_id, occurred_at, actor_principal_kind, actor_principal_id,
                    action, target_type, target_id, changes
             FROM audit_events
             WHERE tenant_id = $1
             ORDER BY occurred_at DESC, id DESC
             LIMIT $2",
            )
            .bind(tenant_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    rows.into_iter().map(timescale_audit_event).collect()
}

fn sqlite_audit_event(row: SqliteRow) -> Result<AuditEvent, AuditEventError> {
    let id = parse_uuid(row.try_get("id")?)?;
    let tenant_id = parse_uuid(row.try_get("tenant_id")?)?;
    let occurred_at = DateTime::parse_from_rfc3339(&row.try_get::<String, _>("occurred_at")?)
        .map_err(|_| AuditEventError::InvalidStoredEvent)?
        .with_timezone(&Utc);
    let actor = AuditPrincipal::from_storage(
        &row.try_get::<String, _>("actor_principal_kind")?,
        parse_uuid(row.try_get("actor_principal_id")?)?,
    )?;
    let action = AuditAction::from_storage(&row.try_get::<String, _>("action")?)?;
    let target_type = AuditTargetType::from_storage(&row.try_get::<String, _>("target_type")?)?;
    let target_id: String = row.try_get("target_id")?;
    let changes = serde_json::from_str(&row.try_get::<String, _>("changes")?)
        .map_err(|_| AuditEventError::InvalidStoredEvent)?;
    audit_event(
        id,
        tenant_id,
        occurred_at,
        actor,
        action,
        target_type,
        target_id,
        changes,
    )
}

fn timescale_audit_event(row: PgRow) -> Result<AuditEvent, AuditEventError> {
    let id: Uuid = row.try_get("id")?;
    let tenant_id: Uuid = row.try_get("tenant_id")?;
    let occurred_at: DateTime<Utc> = row.try_get("occurred_at")?;
    let actor = AuditPrincipal::from_storage(
        &row.try_get::<String, _>("actor_principal_kind")?,
        row.try_get("actor_principal_id")?,
    )?;
    let action = AuditAction::from_storage(&row.try_get::<String, _>("action")?)?;
    let target_type = AuditTargetType::from_storage(&row.try_get::<String, _>("target_type")?)?;
    let target_id: String = row.try_get("target_id")?;
    let changes: Json<Value> = row.try_get("changes")?;
    audit_event(
        id,
        tenant_id,
        occurred_at,
        actor,
        action,
        target_type,
        target_id,
        changes.0,
    )
}

fn audit_event(
    id: Uuid,
    tenant_id: Uuid,
    occurred_at: DateTime<Utc>,
    actor: AuditPrincipal,
    action: AuditAction,
    target_type: AuditTargetType,
    target_id: String,
    changes: Value,
) -> Result<AuditEvent, AuditEventError> {
    if !changes.is_object() {
        return Err(AuditEventError::InvalidStoredEvent);
    }
    Ok(AuditEvent {
        id,
        tenant_id,
        occurred_at,
        actor,
        action,
        target_type,
        target_id,
        changes,
    })
}

fn parse_uuid(value: String) -> Result<Uuid, AuditEventError> {
    Uuid::parse_str(&value).map_err(|_| AuditEventError::InvalidStoredEvent)
}
