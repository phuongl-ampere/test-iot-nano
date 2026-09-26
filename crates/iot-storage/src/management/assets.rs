use std::{future::Future, pin::Pin};

use chrono::Utc;
use sqlx::{Postgres, Row, Sqlite, Transaction, error::DatabaseError, types::Json};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    AuditAction, AuditPrincipal, AuditTargetType, PlatformStore, PlatformStoreError, audit,
};

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAsset {
    pub id: Uuid,
    pub name: String,
    pub owner_user_id: Option<Uuid>,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub attributes: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementAsset {
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementAsset {
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Error)]
pub enum ManagementAssetError {
    #[error("invalid asset name")]
    InvalidName,
    #[error("asset metadata must be an object")]
    MetadataMustBeObject,
    #[error("asset attributes must be an object")]
    AttributesMustBeObject,
    #[error("management asset was not found")]
    AssetNotFound,
    #[error("asset profile is unavailable: {0}")]
    AssetProfileUnavailable(Uuid),
    #[error("parent asset is unavailable: {0}")]
    ParentAssetUnavailable(Uuid),
    #[error("the current user must own the selected parent asset")]
    ParentAssetNotOwned,
    #[error("the current principal cannot create user-owned assets")]
    OwnerMustBeRegularUser,
    #[error("an asset cannot be its own parent")]
    AssetCannotBeOwnParent,
    #[error("an asset cannot have a descendant as its parent")]
    AssetCannotHaveDescendantParent,
    #[error("an asset named {name:?} already exists under the same parent")]
    SiblingNameConflict {
        name: String,
        parent_asset_id: Option<Uuid>,
    },
    #[error("stored management asset ID is invalid")]
    InvalidStoredAssetId,
    #[error("stored management asset references are invalid")]
    InvalidStoredReferences,
    #[error("stored management asset metadata is invalid")]
    InvalidStoredMetadata,
    #[error("management asset storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAssetError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAssetError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAssetRepository: Send + Sync {
    fn list_management_assets<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAsset>, ManagementAssetError>> + Send + 'a>>;
    fn create_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset: CreateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>;
    fn update_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
        asset: UpdateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>;
    fn delete_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetError>> + Send + 'a>>;
}

impl ManagementAssetRepository for PlatformStore {
    fn list_management_assets<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAsset>, ManagementAssetError>> + Send + 'a>>
    {
        Box::pin(async move { list_management_assets(self, tenant_id).await })
    }

    fn create_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset: CreateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>
    {
        Box::pin(async move { create_management_asset(self, tenant_id, actor, asset).await })
    }

    fn update_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
        asset: UpdateManagementAsset,
    ) -> Pin<Box<dyn Future<Output = Result<ManagementAsset, ManagementAssetError>> + Send + 'a>>
    {
        Box::pin(
            async move { update_management_asset(self, tenant_id, actor, asset_id, asset).await },
        )
    }

    fn delete_management_asset<'a>(
        &'a self,
        tenant_id: Uuid,
        actor: AuditPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAssetError>> + Send + 'a>> {
        Box::pin(async move { delete_management_asset(self, tenant_id, actor, asset_id).await })
    }
}

#[derive(Debug)]
struct ValidatedManagementAsset {
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: serde_json::Value,
}

async fn list_management_assets(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAsset>, ManagementAssetError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, owner_user_id, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE tenant_id = ?
                 ORDER BY name, id",
            )
            .bind(tenant_id.to_string())
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_asset_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, owner_user_id, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE tenant_id = $1
                 ORDER BY name, id",
            )
            .bind(tenant_id)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_asset_from_row)
                .collect()
        }
    }
}

async fn create_management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    asset: CreateManagementAsset,
) -> Result<ManagementAsset, ManagementAssetError> {
    let asset = validate_management_asset(
        asset.name,
        asset.asset_profile_id,
        asset.parent_asset_id,
        asset.metadata,
        asset.attributes,
    )?;
    let asset_id = Uuid::now_v7();
    let owner_user_id = match actor {
        AuditPrincipal::User(user_id) => Some(user_id),
        AuditPrincipal::SystemAccount(_) | AuditPrincipal::TenantAccount(_) => None,
    };
    let sibling_name = asset.name.clone();
    let sibling_parent_asset_id = asset.parent_asset_id;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            if let Some(owner_user_id) = owner_user_id {
                sqlite_require_regular_management_user(&mut transaction, tenant_id, owner_user_id)
                    .await?;
                if let Some(parent_asset_id) = asset.parent_asset_id {
                    sqlite_require_management_asset_owner(
                        &mut transaction,
                        tenant_id,
                        parent_asset_id,
                        owner_user_id,
                    )
                    .await?;
                }
            }
            validate_sqlite_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            sqlx::query(
                "INSERT INTO assets (
                    id, tenant_id, name, owner_user_id, asset_profile_id, parent_asset_id, metadata
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .bind(asset.name)
            .bind(owner_user_id.map(|id| id.to_string()))
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(asset.parent_asset_id.map(|id| id.to_string()))
            .bind(asset.metadata.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": null,
                            "after": parent_asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            if let Some(owner_user_id) = owner_user_id {
                timescale_require_regular_management_user(
                    &mut transaction,
                    tenant_id,
                    owner_user_id,
                )
                .await?;
                if let Some(parent_asset_id) = asset.parent_asset_id {
                    timescale_require_management_asset_owner(
                        &mut transaction,
                        tenant_id,
                        parent_asset_id,
                        owner_user_id,
                    )
                    .await?;
                }
            }
            validate_timescale_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            sqlx::query(
                "INSERT INTO assets (
                    id, tenant_id, name, owner_user_id, asset_profile_id, parent_asset_id, metadata
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .bind(asset.name)
            .bind(owner_user_id)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(Json(asset.metadata))
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": null,
                            "after": parent_asset_id.to_string(),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
    }
    management_asset(store, tenant_id, asset_id).await
}

async fn update_management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    asset_id: Uuid,
    asset: UpdateManagementAsset,
) -> Result<ManagementAsset, ManagementAssetError> {
    let asset = validate_management_asset(
        asset.name,
        asset.asset_profile_id,
        asset.parent_asset_id,
        asset.metadata,
        asset.attributes,
    )?;
    if asset.parent_asset_id == Some(asset_id) {
        return Err(ManagementAssetError::AssetCannotBeOwnParent);
    }
    let sibling_name = asset.name.clone();
    let sibling_parent_asset_id = asset.parent_asset_id;
    let user_owner_id = match actor {
        AuditPrincipal::User(user_id) => Some(user_id),
        AuditPrincipal::SystemAccount(_) | AuditPrincipal::TenantAccount(_) => None,
    };
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            if let Some(user_owner_id) = user_owner_id {
                sqlite_require_regular_management_user(&mut transaction, tenant_id, user_owner_id)
                    .await?;
                sqlite_require_management_asset_owner(
                    &mut transaction,
                    tenant_id,
                    asset_id,
                    user_owner_id,
                )
                .await?;
                if let Some(parent_asset_id) = asset.parent_asset_id {
                    sqlite_require_management_asset_owner(
                        &mut transaction,
                        tenant_id,
                        parent_asset_id,
                        user_owner_id,
                    )
                    .await?;
                }
            }
            sqlite_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            let previous_parent_id: Option<String> = sqlx::query_scalar(
                "SELECT parent_asset_id FROM assets WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            let next_parent_id = asset.parent_asset_id.map(|id| id.to_string());
            validate_sqlite_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if sqlite_asset_is_descendant(
                    &mut transaction,
                    tenant_id,
                    asset_id,
                    parent_asset_id,
                )
                .await?
                {
                    return Err(ManagementAssetError::AssetCannotHaveDescendantParent);
                }
            }
            sqlx::query(
                "UPDATE assets
                 SET name = ?, asset_profile_id = ?, parent_asset_id = ?, metadata = ?,
                     updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset.name)
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(next_parent_id.as_deref())
            .bind(asset.metadata.to_string())
            .bind(Utc::now().to_rfc3339())
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if previous_parent_id != next_parent_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": previous_parent_id,
                            "after": next_parent_id,
                        }
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            // Match profile deletion before taking hierarchy row locks.
            sqlx::query("LOCK TABLE assets IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *transaction)
                .await?;
            lock_timescale_management_asset_update_scope(
                &mut transaction,
                tenant_id,
                asset_id,
                asset.parent_asset_id,
            )
            .await?;
            if let Some(user_owner_id) = user_owner_id {
                timescale_require_regular_management_user(
                    &mut transaction,
                    tenant_id,
                    user_owner_id,
                )
                .await?;
                timescale_require_management_asset_owner(
                    &mut transaction,
                    tenant_id,
                    asset_id,
                    user_owner_id,
                )
                .await?;
                if let Some(parent_asset_id) = asset.parent_asset_id {
                    timescale_require_management_asset_owner(
                        &mut transaction,
                        tenant_id,
                        parent_asset_id,
                        user_owner_id,
                    )
                    .await?;
                }
            }
            timescale_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            let previous_parent_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT parent_asset_id FROM assets WHERE id = $1 AND tenant_id = $2",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
            let next_parent_id = asset.parent_asset_id;
            validate_timescale_asset_references(
                &mut transaction,
                tenant_id,
                asset.asset_profile_id,
                asset.parent_asset_id,
            )
            .await?;
            if let Some(parent_asset_id) = asset.parent_asset_id {
                if timescale_asset_is_descendant(
                    &mut transaction,
                    tenant_id,
                    asset_id,
                    parent_asset_id,
                )
                .await?
                {
                    return Err(ManagementAssetError::AssetCannotHaveDescendantParent);
                }
            }
            sqlx::query(
                "UPDATE assets
                 SET name = $2, asset_profile_id = $3, parent_asset_id = $4, metadata = $5,
                     updated_at = now()
                 WHERE id = $1 AND tenant_id = $6",
            )
            .bind(asset_id)
            .bind(asset.name)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(Json(asset.metadata))
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await
            .map_err(|error| {
                map_management_asset_sibling_name_conflict(
                    error,
                    &sibling_name,
                    sibling_parent_asset_id,
                )
            })?;
            if previous_parent_id != next_parent_id {
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::AssetContainmentChanged,
                    AuditTargetType::Asset,
                    asset_id.to_string(),
                    serde_json::json!({
                        "parent_asset_id": {
                            "before": previous_parent_id.map(|id| id.to_string()),
                            "after": next_parent_id.map(|id| id.to_string()),
                        }
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
            }
            transaction.commit().await?;
        }
    }
    management_asset(store, tenant_id, asset_id).await
}

async fn delete_management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    actor: AuditPrincipal,
    asset_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let deleted = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            audit::validate_sqlite_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
            sqlite_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            if let Some(name) =
                sqlite_promoted_asset_root_name_conflict(&mut transaction, tenant_id, asset_id)
                    .await?
            {
                return Err(ManagementAssetError::SiblingNameConflict {
                    name,
                    parent_asset_id: None,
                });
            }
            let detached_asset_ids = sqlx::query_scalar::<_, String>(
                "UPDATE assets
                 SET parent_asset_id = NULL
                 WHERE parent_asset_id = ? AND tenant_id = ?
                 RETURNING id",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_all(&mut *transaction)
            .await?;
            let detached_device_ids = sqlx::query_scalar::<_, String>(
                "UPDATE devices
                 SET asset_id = NULL
                 WHERE asset_id = ? AND tenant_id = ?
                 RETURNING device_id",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_all(&mut *transaction)
            .await?;
            if !detached_asset_ids.is_empty() || !detached_device_ids.is_empty() {
                for detached_asset_id in detached_asset_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Asset,
                        detached_asset_id,
                        serde_json::json!({
                            "parent_asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
                for detached_device_id in detached_device_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Device,
                        detached_device_id,
                        serde_json::json!({
                            "asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
            }
            let deleted = sqlx::query("DELETE FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(asset_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            deleted
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            audit::validate_timescale_tenant_audit_actor(&mut transaction, tenant_id, actor)
                .await?;
            timescale_require_management_asset(&mut transaction, tenant_id, asset_id).await?;
            if let Some(name) =
                timescale_promoted_asset_root_name_conflict(&mut transaction, tenant_id, asset_id)
                    .await?
            {
                return Err(ManagementAssetError::SiblingNameConflict {
                    name,
                    parent_asset_id: None,
                });
            }
            let detached_asset_ids = sqlx::query_scalar::<_, Uuid>(
                "UPDATE assets
                 SET parent_asset_id = NULL
                 WHERE parent_asset_id = $1 AND tenant_id = $2
                 RETURNING id",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_all(&mut *transaction)
            .await?;
            let detached_device_ids = sqlx::query_scalar::<_, String>(
                "UPDATE devices
                 SET asset_id = NULL
                 WHERE asset_id = $1 AND tenant_id = $2
                 RETURNING device_id",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_all(&mut *transaction)
            .await?;
            if !detached_asset_ids.is_empty() || !detached_device_ids.is_empty() {
                for detached_asset_id in detached_asset_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Asset,
                        detached_asset_id.to_string(),
                        serde_json::json!({
                            "parent_asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
                for detached_device_id in detached_device_ids {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::AssetContainmentChanged,
                        AuditTargetType::Device,
                        detached_device_id,
                        serde_json::json!({
                            "asset_id": {
                                "before": asset_id.to_string(),
                                "after": null,
                            }
                        }),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
            }
            let deleted = sqlx::query("DELETE FROM assets WHERE id = $1 AND tenant_id = $2")
                .bind(asset_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected();
            transaction.commit().await?;
            deleted
        }
    };
    if deleted == 0 {
        Err(ManagementAssetError::AssetNotFound)
    } else {
        Ok(())
    }
}

async fn sqlite_promoted_asset_root_name_conflict(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<Option<String>, ManagementAssetError> {
    sqlx::query_scalar(
        "SELECT child.name
         FROM assets AS child
         JOIN assets AS root
           ON root.name = child.name
          AND root.tenant_id = child.tenant_id
          AND root.parent_asset_id IS NULL
          AND root.id <> ?
         WHERE child.parent_asset_id = ? AND child.tenant_id = ?
         ORDER BY child.name, child.id
         LIMIT 1",
    )
    .bind(asset_id.to_string())
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(ManagementAssetError::from)
}

async fn timescale_promoted_asset_root_name_conflict(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<Option<String>, ManagementAssetError> {
    sqlx::query_scalar(
        "SELECT child.name
         FROM assets AS child
         JOIN assets AS root
           ON root.name = child.name
          AND root.tenant_id = child.tenant_id
          AND root.parent_asset_id IS NULL
          AND root.id <> $1
         WHERE child.parent_asset_id = $1 AND child.tenant_id = $2
         ORDER BY child.name, child.id
         LIMIT 1",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(ManagementAssetError::from)
}

async fn management_asset(
    store: &PlatformStore,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<ManagementAsset, ManagementAssetError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT id, name, owner_user_id, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementAssetError::AssetNotFound)?;
            sqlite_management_asset_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT id, name, owner_user_id, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE id = $1 AND tenant_id = $2",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementAssetError::AssetNotFound)?;
            timescale_management_asset_from_row(row)
        }
    }
}

fn sqlite_management_asset_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<ManagementAsset, ManagementAssetError> {
    let metadata: serde_json::Value = serde_json::from_str(&row.try_get::<String, _>("metadata")?)
        .map_err(|_| ManagementAssetError::InvalidStoredMetadata)?;
    if !metadata.is_object() {
        return Err(ManagementAssetError::InvalidStoredMetadata);
    }
    Ok(ManagementAsset {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| ManagementAssetError::InvalidStoredAssetId)?,
        name: row.try_get("name")?,
        owner_user_id: row
            .try_get::<Option<String>, _>("owner_user_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementAssetError::InvalidStoredReferences)?,
        asset_profile_id: row
            .try_get::<Option<String>, _>("asset_profile_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementAssetError::InvalidStoredReferences)?,
        parent_asset_id: row
            .try_get::<Option<String>, _>("parent_asset_id")?
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ManagementAssetError::InvalidStoredReferences)?,
        attributes: metadata.clone(),
        metadata,
    })
}

fn timescale_management_asset_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<ManagementAsset, ManagementAssetError> {
    let metadata = row.try_get::<Json<serde_json::Value>, _>("metadata")?.0;
    if !metadata.is_object() {
        return Err(ManagementAssetError::InvalidStoredMetadata);
    }
    Ok(ManagementAsset {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        owner_user_id: row.try_get("owner_user_id")?,
        asset_profile_id: row.try_get("asset_profile_id")?,
        parent_asset_id: row.try_get("parent_asset_id")?,
        attributes: metadata.clone(),
        metadata,
    })
}

fn validate_management_asset(
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: serde_json::Value,
    attributes: Option<serde_json::Value>,
) -> Result<ValidatedManagementAsset, ManagementAssetError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err(ManagementAssetError::InvalidName);
    }
    let metadata = match attributes {
        Some(attributes) => validate_asset_attributes(attributes)?,
        None => validate_asset_metadata(metadata)?,
    };
    Ok(ValidatedManagementAsset {
        name: name.to_owned(),
        asset_profile_id,
        parent_asset_id,
        metadata,
    })
}

fn map_management_asset_sibling_name_conflict(
    error: sqlx::Error,
    name: &str,
    parent_asset_id: Option<Uuid>,
) -> ManagementAssetError {
    if error
        .as_database_error()
        .is_some_and(is_management_asset_sibling_name_unique_violation)
    {
        ManagementAssetError::SiblingNameConflict {
            name: name.to_owned(),
            parent_asset_id,
        }
    } else {
        ManagementAssetError::from(error)
    }
}

fn is_management_asset_sibling_name_unique_violation(
    database_error: &(dyn DatabaseError + 'static),
) -> bool {
    match database_error.code().as_deref() {
        Some("23505") => {
            database_error.constraint() == Some("assets_tenant_id_parent_asset_id_name_key")
                || database_error.constraint() == Some("assets_tenant_root_name_unique_index")
                || database_error
                    .message()
                    .contains("assets_tenant_id_parent_asset_id_name_key")
                || database_error
                    .message()
                    .contains("assets_tenant_root_name_unique_index")
        }
        Some("19") | Some("2067") => {
            let message = database_error.message();
            message.contains(
                "UNIQUE constraint failed: assets.tenant_id, assets.parent_asset_id, assets.name",
            ) || message.contains("UNIQUE constraint failed: assets.tenant_id, assets.name")
        }
        _ => false,
    }
}

fn validate_asset_metadata(
    value: serde_json::Value,
) -> Result<serde_json::Value, ManagementAssetError> {
    if value.is_null() {
        Ok(serde_json::json!({}))
    } else if value.is_object() {
        Ok(value)
    } else {
        Err(ManagementAssetError::MetadataMustBeObject)
    }
}

fn validate_asset_attributes(
    value: serde_json::Value,
) -> Result<serde_json::Value, ManagementAssetError> {
    if value.is_null() {
        Ok(serde_json::json!({}))
    } else if value.is_object() {
        Ok(value)
    } else {
        Err(ManagementAssetError::AttributesMustBeObject)
    }
}

async fn sqlite_require_management_asset(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let exists =
        sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?")
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .is_some();
    if exists {
        Ok(())
    } else {
        Err(ManagementAssetError::AssetNotFound)
    }
}

async fn sqlite_require_regular_management_user(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let is_regular_user = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE id = ? AND tenant_id = ? AND account_class = 'user'",
    )
    .bind(user_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if is_regular_user {
        Ok(())
    } else {
        Err(ManagementAssetError::OwnerMustBeRegularUser)
    }
}

async fn sqlite_require_management_asset_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
    owner_user_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id FROM assets WHERE id = ? AND tenant_id = ?",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if owner.flatten().as_deref() == Some(owner_user_id.to_string().as_str()) {
        Ok(())
    } else {
        Err(ManagementAssetError::ParentAssetNotOwned)
    }
}

async fn timescale_require_management_asset(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let exists = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM assets WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(ManagementAssetError::AssetNotFound)
    }
}

async fn timescale_require_regular_management_user(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    user_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let is_regular_user = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2 AND account_class = 'user'",
    )
    .bind(user_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if is_regular_user {
        Ok(())
    } else {
        Err(ManagementAssetError::OwnerMustBeRegularUser)
    }
}

async fn timescale_require_management_asset_owner(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
    owner_user_id: Uuid,
) -> Result<(), ManagementAssetError> {
    let owner = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT owner_user_id FROM assets WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
    )
    .bind(asset_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if owner.flatten() == Some(owner_user_id) {
        Ok(())
    } else {
        Err(ManagementAssetError::ParentAssetNotOwned)
    }
}

async fn lock_timescale_management_asset_update_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
    proposed_parent_asset_id: Option<Uuid>,
) -> Result<(), ManagementAssetError> {
    // Lock every row that can affect this hierarchy transition in UUID order. This
    // serializes reciprocal reparenting before validation sees a stale hierarchy.
    sqlx::query_scalar::<_, Uuid>(
        "WITH RECURSIVE roots(id) AS (
             SELECT id FROM assets WHERE id = $1 AND tenant_id = $3
             UNION
             SELECT id FROM assets WHERE id = $2 AND tenant_id = $3
         ),
         ancestors(id) AS (
             SELECT id FROM roots
             UNION
             SELECT asset.parent_asset_id
             FROM assets AS asset
             JOIN ancestors ON asset.id = ancestors.id
             WHERE asset.parent_asset_id IS NOT NULL AND asset.tenant_id = $3
         ),
         descendants(id) AS (
             SELECT id FROM roots
             UNION
             SELECT child.id
             FROM assets AS child
             JOIN descendants ON child.parent_asset_id = descendants.id
             WHERE child.tenant_id = $3
         )
         SELECT asset.id
         FROM assets AS asset
         WHERE asset.tenant_id = $3 AND asset.id IN (
             SELECT id FROM ancestors
             UNION
             SELECT id FROM descendants
         )
         ORDER BY asset.id
         FOR UPDATE",
    )
    .bind(asset_id)
    .bind(proposed_parent_asset_id)
    .bind(tenant_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(())
}

async fn validate_sqlite_asset_references(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
) -> Result<(), ManagementAssetError> {
    if let Some(asset_profile_id) = asset_profile_id {
        let exists = sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM asset_profiles WHERE id = ? AND tenant_id = ?",
        )
        .bind(asset_profile_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !exists {
            return Err(ManagementAssetError::AssetProfileUnavailable(
                asset_profile_id,
            ));
        }
    }
    if let Some(parent_asset_id) = parent_asset_id {
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?")
                .bind(parent_asset_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .is_some();
        if !exists {
            return Err(ManagementAssetError::ParentAssetUnavailable(
                parent_asset_id,
            ));
        }
    }
    Ok(())
}

async fn validate_timescale_asset_references(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
) -> Result<(), ManagementAssetError> {
    if let Some(asset_profile_id) = asset_profile_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(
                SELECT 1 FROM asset_profiles WHERE id = $1 AND tenant_id = $2
             )",
        )
        .bind(asset_profile_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementAssetError::AssetProfileUnavailable(
                asset_profile_id,
            ));
        }
    }
    if let Some(parent_asset_id) = parent_asset_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2)",
        )
        .bind(parent_asset_id)
        .bind(tenant_id)
        .fetch_one(&mut **transaction)
        .await?;
        if !exists {
            return Err(ManagementAssetError::ParentAssetUnavailable(
                parent_asset_id,
            ));
        }
    }
    Ok(())
}

async fn sqlite_asset_is_descendant(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    asset_id: Uuid,
    candidate_parent_id: Uuid,
) -> Result<bool, ManagementAssetError> {
    let descendant = sqlx::query_scalar::<_, i64>(
        "WITH RECURSIVE descendants(id) AS (
             SELECT id FROM assets WHERE parent_asset_id = ? AND tenant_id = ?
             UNION
             SELECT child.id
             FROM assets AS child
             JOIN descendants ON child.parent_asset_id = descendants.id
             WHERE child.tenant_id = ?
         )
         SELECT 1 FROM descendants WHERE id = ? LIMIT 1",
    )
    .bind(asset_id.to_string())
    .bind(tenant_id.to_string())
    .bind(tenant_id.to_string())
    .bind(candidate_parent_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    Ok(descendant)
}

async fn timescale_asset_is_descendant(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    asset_id: Uuid,
    candidate_parent_id: Uuid,
) -> Result<bool, ManagementAssetError> {
    let descendant = sqlx::query_scalar::<_, bool>(
        "WITH RECURSIVE descendants(id) AS (
             SELECT id FROM assets WHERE parent_asset_id = $1 AND tenant_id = $3
             UNION
             SELECT child.id
             FROM assets AS child
             JOIN descendants ON child.parent_asset_id = descendants.id
             WHERE child.tenant_id = $3
         )
         SELECT EXISTS(SELECT 1 FROM descendants WHERE id = $2)",
    )
    .bind(asset_id)
    .bind(candidate_parent_id)
    .bind(tenant_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(descendant)
}
