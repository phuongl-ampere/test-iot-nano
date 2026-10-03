use std::{future::Future, pin::Pin};

use chrono::Utc;
use sqlx::{PgPool, Postgres, Row, Sqlite, SqlitePool, Transaction, types::Json};

use crate::{
    AccountClass, AuditAction, AuditPrincipal, AuditTargetType, AuthorizationRepository,
    AuthorizationSubject, AuthorizedAssetListEntry, AuthorizedAssetSummary,
    AuthorizedDeviceListEntry, AuthorizedDeviceSummary, NewResourcePermission, NewUserGroup,
    OwnershipTransferTarget, PermissionCreator, PlatformStore, PlatformStoreError, ResourceAccess,
    ResourceAccessSource, ResourceInvitation, ResourceInvitationRepository,
    ResourceInvitationState, ResourcePermission, ResourcePermissionRecord, TenantActor,
    TenantAuthorizationError, TenantAuthorizationRepository, TenantUserGroup,
    TenantUserGroupMember, UserGroup, audit, authorization_account_class,
    parse_authorized_device_timestamp,
};

impl PlatformStore {
    pub async fn authorization_subject(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<Option<AuthorizationSubject>, PlatformStoreError> {
        let identity: Option<(uuid::Uuid, String)> = match self {
            Self::Sqlite(store) => sqlx::query_as::<_, (String, String)>(
                "SELECT tenant_id, account_class FROM users WHERE id = ?",
            )
            .bind(user_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .map(|(tenant_id, account_class)| {
                uuid::Uuid::parse_str(&tenant_id)
                    .map(|tenant_id| (tenant_id, account_class))
                    .map_err(|_| PlatformStoreError::InvalidAuthorizationAccountClass(tenant_id))
            })
            .transpose()?,
            Self::Timescale(pool) => {
                sqlx::query_as::<_, (uuid::Uuid, String)>(
                    "SELECT tenant_id, account_class FROM users WHERE id = $1",
                )
                .bind(user_id)
                .fetch_optional(pool)
                .await?
            }
        };
        match identity {
            Some((tenant_id, account_class)) => {
                let account_class = authorization_account_class(&account_class)?;
                if account_class == AccountClass::System {
                    return Ok(None);
                }
                Ok(Some(AuthorizationSubject {
                    user_id,
                    tenant_id,
                    account_class,
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn list_authorized_devices(
        &self,
        subject: &AuthorizationSubject,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<AuthorizedDeviceListEntry>, PlatformStoreError> {
        self.list_authorized_devices_matching(subject, after, limit, None)
            .await
    }

    pub async fn list_authorized_assets(
        &self,
        subject: &AuthorizationSubject,
        after: Option<uuid::Uuid>,
        limit: u32,
    ) -> Result<Vec<AuthorizedAssetListEntry>, PlatformStoreError> {
        self.list_authorized_assets_matching(subject, after, limit, None)
            .await
    }

    pub async fn authorized_asset(
        &self,
        subject: &AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Result<Option<AuthorizedAssetSummary>, PlatformStoreError> {
        Ok(self
            .list_authorized_assets_matching(subject, None, 1, Some(asset_id))
            .await?
            .into_iter()
            .next()
            .map(|asset| AuthorizedAssetSummary {
                asset_id: asset.asset_id,
                name: asset.name,
                parent_asset_id: asset.parent_asset_id,
                access: asset.access,
            }))
    }

    async fn list_authorized_devices_matching(
        &self,
        subject: &AuthorizationSubject,
        after: Option<&str>,
        limit: u32,
        device_id: Option<&str>,
    ) -> Result<Vec<AuthorizedDeviceListEntry>, PlatformStoreError> {
        let limit = i64::from(limit);
        match self {
            Self::Sqlite(store) => {
                let user_id = subject.user_id.to_string();
                let tenant_id = subject.tenant_id.to_string();
                let rows = sqlx::query(
                    "WITH RECURSIVE candidates(
                         device_id, display_name, last_seen_at, asset_id, owner_user_id
                     ) AS (
                         SELECT device_id, display_name, last_seen_at, asset_id, owner_user_id
                         FROM devices
                         WHERE tenant_id = ?
                           AND deleted_at IS NULL
                           AND (? IS NULL OR device_id > ?)
                           AND (? IS NULL OR device_id = ?)
                     ),
                     ancestors(device_id, asset_id, depth) AS (
                         SELECT device_id, asset_id, 0
                         FROM candidates
                         WHERE asset_id IS NOT NULL
                         UNION ALL
                         SELECT ancestors.device_id, asset.parent_asset_id, ancestors.depth + 1
                         FROM ancestors
                         JOIN assets AS asset
                           ON asset.id = ancestors.asset_id AND asset.tenant_id = ?
                         WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     ),
                     access_candidates(device_id, permission_rank, source_rank, access_source) AS (
                         SELECT device_id, 3, 1, 'owner'
                         FROM candidates
                         WHERE owner_user_id = ?
                         UNION ALL
                         SELECT candidate.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                2,
                                'direct_user'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.device_id = candidate.device_id
                          AND permission.revoked_at IS NULL
                         WHERE permission.subject_user_id = ?
                         UNION ALL
                         SELECT candidate.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                3,
                                'group'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.device_id = candidate.device_id
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                         UNION ALL
                         SELECT ancestors.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                4,
                                'inherited_user'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.asset_id = ancestors.asset_id
                          AND permission.inherit_children = 1
                          AND permission.revoked_at IS NULL
                         WHERE permission.subject_user_id = ?
                         UNION ALL
                         SELECT ancestors.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                5,
                                'inherited_group'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.asset_id = ancestors.asset_id
                          AND permission.inherit_children = 1
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                     ),
                     authorized(device_id, effective_permission, access_source) AS (
                         SELECT device_id,
                                CASE permission_rank
                                    WHEN 3 THEN 'owner'
                                    WHEN 2 THEN 'manager'
                                    ELSE 'viewer'
                                END,
                                access_source
                         FROM (
                             SELECT access_candidates.*,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY device_id
                                        ORDER BY permission_rank DESC, source_rank ASC
                                    ) AS access_rank
                             FROM access_candidates
                         ) AS ranked_access
                         WHERE access_rank = 1
                     )
                     SELECT candidate.device_id, candidate.display_name, candidate.last_seen_at,
                            authorized.effective_permission, authorized.access_source
                     FROM candidates AS candidate
                     JOIN authorized ON authorized.device_id = candidate.device_id
                     ORDER BY candidate.device_id
                     LIMIT ?",
                )
                .bind(&tenant_id)
                .bind(after)
                .bind(after)
                .bind(device_id)
                .bind(device_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(limit)
                .fetch_all(store.pool())
                .await?;
                rows.into_iter()
                    .map(|row| {
                        let last_seen_at = row
                            .try_get::<Option<String>, _>("last_seen_at")?
                            .map(|value| parse_authorized_device_timestamp(&value))
                            .transpose()?;
                        Ok(AuthorizedDeviceListEntry {
                            device_id: row.try_get("device_id")?,
                            display_name: row.try_get("display_name")?,
                            last_seen_at,
                            access: resource_access_from_storage(
                                row.try_get("effective_permission")?,
                                row.try_get("access_source")?,
                            )?,
                        })
                    })
                    .collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "WITH RECURSIVE candidates(
                         device_id, display_name, asset_id, owner_user_id
                     ) AS (
                         SELECT d.device_id, d.display_name, d.asset_id, d.owner_user_id
                         FROM devices AS d
                         WHERE d.tenant_id = $1
                           AND d.deleted_at IS NULL
                           AND ($2::text IS NULL OR d.device_id > $2)
                           AND ($4::text IS NULL OR d.device_id = $4)
                     ),
                     ancestors(device_id, asset_id, depth) AS (
                         SELECT device_id, asset_id, 0
                         FROM candidates
                         WHERE asset_id IS NOT NULL
                         UNION ALL
                         SELECT ancestors.device_id, asset.parent_asset_id, ancestors.depth + 1
                         FROM ancestors
                         JOIN assets AS asset
                           ON asset.id = ancestors.asset_id AND asset.tenant_id = $1
                         WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     ),
                     access_candidates(device_id, permission_rank, source_rank, access_source) AS (
                         SELECT device_id, 3, 1, 'owner'
                         FROM candidates
                         WHERE owner_user_id = $3
                         UNION ALL
                         SELECT candidate.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                2,
                                'direct_user'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.device_id = candidate.device_id
                          AND permission.revoked_at IS NULL
                         WHERE permission.subject_user_id = $3
                         UNION ALL
                         SELECT candidate.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                3,
                                'group'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.device_id = candidate.device_id
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                         UNION ALL
                         SELECT ancestors.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                4,
                                'inherited_user'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.asset_id = ancestors.asset_id
                          AND permission.inherit_children = TRUE
                          AND permission.revoked_at IS NULL
                         WHERE permission.subject_user_id = $3
                         UNION ALL
                         SELECT ancestors.device_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                5,
                                'inherited_group'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.asset_id = ancestors.asset_id
                          AND permission.inherit_children = TRUE
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                     ),
                     authorized(device_id, effective_permission, access_source) AS (
                         SELECT device_id,
                                CASE permission_rank
                                    WHEN 3 THEN 'owner'
                                    WHEN 2 THEN 'manager'
                                    ELSE 'viewer'
                                END,
                                access_source
                         FROM (
                             SELECT access_candidates.*,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY device_id
                                        ORDER BY permission_rank DESC, source_rank ASC
                                    ) AS access_rank
                             FROM access_candidates
                         ) AS ranked_access
                         WHERE access_rank = 1
                     )
                     SELECT candidate.device_id, candidate.display_name, runtime.last_seen_at,
                            authorized.effective_permission, authorized.access_source
                     FROM candidates AS candidate
                     JOIN authorized ON authorized.device_id = candidate.device_id
                     LEFT JOIN device_runtime_state AS runtime
                       ON runtime.tenant_id = $1
                      AND runtime.device_id = candidate.device_id
                     ORDER BY candidate.device_id
                     LIMIT $5",
                )
                .bind(subject.tenant_id)
                .bind(after)
                .bind(subject.user_id)
                .bind(device_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(|row| {
                        Ok(AuthorizedDeviceListEntry {
                            device_id: row.try_get("device_id")?,
                            display_name: row.try_get("display_name")?,
                            last_seen_at: row.try_get("last_seen_at")?,
                            access: resource_access_from_storage(
                                row.try_get("effective_permission")?,
                                row.try_get("access_source")?,
                            )?,
                        })
                    })
                    .collect()
            }
        }
    }

    async fn list_authorized_assets_matching(
        &self,
        subject: &AuthorizationSubject,
        after: Option<uuid::Uuid>,
        limit: u32,
        asset_id: Option<uuid::Uuid>,
    ) -> Result<Vec<AuthorizedAssetListEntry>, PlatformStoreError> {
        let limit = i64::from(limit);
        match self {
            Self::Sqlite(store) => {
                let user_id = subject.user_id.to_string();
                let tenant_id = subject.tenant_id.to_string();
                let after = after.map(|value| value.to_string());
                let asset_id = asset_id.map(|value| value.to_string());
                let rows = sqlx::query(
                    "WITH RECURSIVE candidates(asset_id, name, parent_asset_id, owner_user_id) AS (
                         SELECT id, name, parent_asset_id, owner_user_id
                         FROM assets
                         WHERE tenant_id = ?
                           AND (? IS NULL OR id > ?)
                           AND (? IS NULL OR id = ?)
                     ),
                     ancestors(asset_id, ancestor_asset_id, depth) AS (
                         SELECT asset_id, asset_id, 0
                         FROM candidates
                         UNION ALL
                         SELECT ancestors.asset_id, asset.parent_asset_id, ancestors.depth + 1
                         FROM ancestors
                         JOIN assets AS asset
                           ON asset.id = ancestors.ancestor_asset_id AND asset.tenant_id = ?
                         WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     ),
                     access_candidates(asset_id, permission_rank, source_rank, access_source) AS (
                         SELECT asset_id, 3, 1, 'owner'
                         FROM candidates
                         WHERE owner_user_id = ?
                         UNION ALL
                         SELECT candidate.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                2,
                                'direct_user'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.asset_id = candidate.asset_id
                          AND permission.revoked_at IS NULL
                         WHERE permission.subject_user_id = ?
                         UNION ALL
                         SELECT candidate.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                3,
                                'group'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.asset_id = candidate.asset_id
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                         UNION ALL
                         SELECT ancestors.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                4,
                                'inherited_user'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.asset_id = ancestors.ancestor_asset_id
                          AND permission.inherit_children = 1
                          AND permission.revoked_at IS NULL
                         WHERE ancestors.depth > 0 AND permission.subject_user_id = ?
                         UNION ALL
                         SELECT ancestors.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                5,
                                'inherited_group'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = ?
                          AND permission.asset_id = ancestors.ancestor_asset_id
                          AND permission.inherit_children = 1
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                         WHERE ancestors.depth > 0
                     ),
                     authorized(asset_id, effective_permission, access_source) AS (
                         SELECT asset_id,
                                CASE permission_rank
                                    WHEN 3 THEN 'owner'
                                    WHEN 2 THEN 'manager'
                                    ELSE 'viewer'
                                END,
                                access_source
                         FROM (
                             SELECT access_candidates.*,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY asset_id
                                        ORDER BY permission_rank DESC, source_rank ASC
                                    ) AS access_rank
                             FROM access_candidates
                         ) AS ranked_access
                         WHERE access_rank = 1
                     )
                     SELECT candidate.asset_id, candidate.name, candidate.parent_asset_id,
                            authorized.effective_permission, authorized.access_source
                     FROM candidates AS candidate
                     JOIN authorized ON authorized.asset_id = candidate.asset_id
                     ORDER BY candidate.asset_id
                     LIMIT ?",
                )
                .bind(&tenant_id)
                .bind(&after)
                .bind(&after)
                .bind(&asset_id)
                .bind(&asset_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(&tenant_id)
                .bind(&user_id)
                .bind(limit)
                .fetch_all(store.pool())
                .await?;
                rows.into_iter()
                    .map(sqlite_authorized_asset_from_row)
                    .collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "WITH RECURSIVE candidates(asset_id, name, parent_asset_id, owner_user_id) AS (
                         SELECT id, name, parent_asset_id, owner_user_id
                         FROM assets
                         WHERE tenant_id = $1
                           AND ($2::uuid IS NULL OR id > $2)
                           AND ($4::uuid IS NULL OR id = $4)
                     ),
                     ancestors(asset_id, ancestor_asset_id, depth) AS (
                         SELECT asset_id, asset_id, 0
                         FROM candidates
                         UNION ALL
                         SELECT ancestors.asset_id, asset.parent_asset_id, ancestors.depth + 1
                         FROM ancestors
                         JOIN assets AS asset
                           ON asset.id = ancestors.ancestor_asset_id AND asset.tenant_id = $1
                         WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
                     ),
                     access_candidates(asset_id, permission_rank, source_rank, access_source) AS (
                         SELECT asset_id, 3, 1, 'owner'
                         FROM candidates
                         WHERE owner_user_id = $3
                         UNION ALL
                         SELECT candidate.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                2,
                                'direct_user'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.asset_id = candidate.asset_id
                          AND permission.revoked_at IS NULL
                         WHERE permission.subject_user_id = $3
                         UNION ALL
                         SELECT candidate.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                3,
                                'group'
                         FROM candidates AS candidate
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.asset_id = candidate.asset_id
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                         UNION ALL
                         SELECT ancestors.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                4,
                                'inherited_user'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.asset_id = ancestors.ancestor_asset_id
                          AND permission.inherit_children = TRUE
                          AND permission.revoked_at IS NULL
                         WHERE ancestors.depth > 0 AND permission.subject_user_id = $3
                         UNION ALL
                         SELECT ancestors.asset_id,
                                CASE permission.permission WHEN 'manager' THEN 2 ELSE 1 END,
                                5,
                                'inherited_group'
                         FROM ancestors
                         JOIN resource_permissions AS permission
                           ON permission.tenant_id = $1
                          AND permission.asset_id = ancestors.ancestor_asset_id
                          AND permission.inherit_children = TRUE
                          AND permission.revoked_at IS NULL
                         JOIN user_group_members AS membership
                           ON membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                         WHERE ancestors.depth > 0
                     ),
                     authorized(asset_id, effective_permission, access_source) AS (
                         SELECT asset_id,
                                CASE permission_rank
                                    WHEN 3 THEN 'owner'
                                    WHEN 2 THEN 'manager'
                                    ELSE 'viewer'
                                END,
                                access_source
                         FROM (
                             SELECT access_candidates.*,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY asset_id
                                        ORDER BY permission_rank DESC, source_rank ASC
                                    ) AS access_rank
                             FROM access_candidates
                         ) AS ranked_access
                         WHERE access_rank = 1
                     )
                     SELECT candidate.asset_id, candidate.name, candidate.parent_asset_id,
                            authorized.effective_permission, authorized.access_source
                     FROM candidates AS candidate
                     JOIN authorized ON authorized.asset_id = candidate.asset_id
                     ORDER BY candidate.asset_id
                     LIMIT $5",
                )
                .bind(subject.tenant_id)
                .bind(after)
                .bind(subject.user_id)
                .bind(asset_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(timescale_authorized_asset_from_row)
                    .collect()
            }
        }
    }

    pub async fn authorized_device(
        &self,
        subject: &AuthorizationSubject,
        device_id: &str,
    ) -> Result<Option<AuthorizedDeviceSummary>, PlatformStoreError> {
        let mut devices = self
            .list_authorized_devices_matching(subject, None, 1, Some(device_id))
            .await?;
        Ok(devices.pop().map(|device| AuthorizedDeviceSummary {
            device_id: device.device_id,
            display_name: device.display_name,
            last_seen_at: device.last_seen_at,
            access: device.access,
        }))
    }

    pub async fn device_permission(
        &self,
        subject: &AuthorizationSubject,
        device_id: &str,
    ) -> Result<Option<ResourcePermission>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                sqlite_device_resource_permission(store.pool(), subject, device_id).await
            }
            Self::Timescale(pool) => {
                timescale_device_resource_permission(pool, subject, device_id).await
            }
        }
    }

    pub async fn asset_permission(
        &self,
        subject: &AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Result<Option<ResourcePermission>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                sqlite_asset_resource_permission(store.pool(), subject, asset_id).await
            }
            Self::Timescale(pool) => {
                timescale_asset_resource_permission(pool, subject, asset_id).await
            }
        }
    }

    pub async fn list_tenant_user_groups(
        &self,
        tenant_id: uuid::Uuid,
    ) -> Result<Vec<TenantUserGroup>, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query(
                    "SELECT groups.id AS group_id, groups.owner_user_id, groups.name,
                            members.user_id AS member_user_id, users.username AS member_username
                     FROM user_groups AS groups
                     LEFT JOIN user_group_members AS members
                       ON members.tenant_id = groups.tenant_id AND members.group_id = groups.id
                     LEFT JOIN users
                       ON users.tenant_id = groups.tenant_id AND users.id = members.user_id
                     WHERE groups.tenant_id = ?
                     ORDER BY groups.name, groups.id, users.username, users.id",
                )
                .bind(tenant_id.to_string())
                .fetch_all(store.pool())
                .await?;
                let mut groups = Vec::new();
                for row in rows {
                    let group_id = tenant_authorization_uuid(row.try_get("group_id")?)?;
                    let owner_user_id = tenant_authorization_uuid(row.try_get("owner_user_id")?)?;
                    let group_changed = groups
                        .last()
                        .is_none_or(|group: &TenantUserGroup| group.id != group_id);
                    if group_changed {
                        groups.push(TenantUserGroup {
                            id: group_id,
                            owner_user_id,
                            name: row.try_get("name")?,
                            members: Vec::new(),
                        });
                    }
                    let member_user_id: Option<String> = row.try_get("member_user_id")?;
                    let member_username: Option<String> = row.try_get("member_username")?;
                    match (member_user_id, member_username) {
                        (None, None) => {}
                        (Some(user_id), Some(username)) => {
                            groups
                                .last_mut()
                                .expect("group row was inserted")
                                .members
                                .push(TenantUserGroupMember {
                                    user_id: tenant_authorization_uuid(user_id)?,
                                    username,
                                });
                        }
                        _ => return Err(TenantAuthorizationError::InvalidStoredRecord),
                    }
                }
                Ok(groups)
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT groups.id AS group_id, groups.owner_user_id, groups.name,
                            members.user_id AS member_user_id, users.username AS member_username
                     FROM user_groups AS groups
                     LEFT JOIN user_group_members AS members
                       ON members.tenant_id = groups.tenant_id AND members.group_id = groups.id
                     LEFT JOIN users
                       ON users.tenant_id = groups.tenant_id AND users.id = members.user_id
                     WHERE groups.tenant_id = $1
                     ORDER BY groups.name, groups.id, users.username, users.id",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                let mut groups = Vec::new();
                for row in rows {
                    let group_id: uuid::Uuid = row.try_get("group_id")?;
                    let owner_user_id: uuid::Uuid = row.try_get("owner_user_id")?;
                    let group_changed = groups
                        .last()
                        .is_none_or(|group: &TenantUserGroup| group.id != group_id);
                    if group_changed {
                        groups.push(TenantUserGroup {
                            id: group_id,
                            owner_user_id,
                            name: row.try_get("name")?,
                            members: Vec::new(),
                        });
                    }
                    let member_user_id: Option<uuid::Uuid> = row.try_get("member_user_id")?;
                    let member_username: Option<String> = row.try_get("member_username")?;
                    match (member_user_id, member_username) {
                        (None, None) => {}
                        (Some(user_id), Some(username)) => {
                            groups
                                .last_mut()
                                .expect("group row was inserted")
                                .members
                                .push(TenantUserGroupMember { user_id, username });
                        }
                        _ => return Err(TenantAuthorizationError::InvalidStoredRecord),
                    }
                }
                Ok(groups)
            }
        }
    }

    pub async fn list_active_resource_permissions(
        &self,
        tenant_id: uuid::Uuid,
    ) -> Result<Vec<ResourcePermissionRecord>, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query(
                    "SELECT id, subject_user_id, subject_group_id, asset_id, device_id,
                            permission, inherit_children, created_by_user_id,
                            created_by_tenant_account_id
                     FROM resource_permissions
                     WHERE tenant_id = ? AND revoked_at IS NULL
                     ORDER BY created_at, id",
                )
                .bind(tenant_id.to_string())
                .fetch_all(store.pool())
                .await?;
                rows.into_iter()
                    .map(|row| {
                        resource_permission_record(
                            tenant_id,
                            tenant_authorization_uuid(row.try_get("id")?)?,
                            tenant_authorization_optional_uuid(row.try_get("subject_user_id")?)?,
                            tenant_authorization_optional_uuid(row.try_get("subject_group_id")?)?,
                            tenant_authorization_optional_uuid(row.try_get("asset_id")?)?,
                            row.try_get("device_id")?,
                            row.try_get("permission")?,
                            sqlite_permission_inheritance(row.try_get("inherit_children")?)?,
                            tenant_authorization_optional_uuid(row.try_get("created_by_user_id")?)?,
                            tenant_authorization_optional_uuid(
                                row.try_get("created_by_tenant_account_id")?,
                            )?,
                        )
                    })
                    .collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT id, subject_user_id, subject_group_id, asset_id, device_id,
                            permission, inherit_children, created_by_user_id,
                            created_by_tenant_account_id
                     FROM resource_permissions
                     WHERE tenant_id = $1 AND revoked_at IS NULL
                     ORDER BY created_at, id",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(|row| {
                        resource_permission_record(
                            tenant_id,
                            row.try_get("id")?,
                            row.try_get("subject_user_id")?,
                            row.try_get("subject_group_id")?,
                            row.try_get("asset_id")?,
                            row.try_get("device_id")?,
                            row.try_get("permission")?,
                            row.try_get("inherit_children")?,
                            row.try_get("created_by_user_id")?,
                            row.try_get("created_by_tenant_account_id")?,
                        )
                    })
                    .collect()
            }
        }
    }

    pub async fn create_user_group(
        &self,
        group: NewUserGroup,
    ) -> Result<UserGroup, TenantAuthorizationError> {
        let id = uuid::Uuid::now_v7();
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_tenant_user(&mut transaction, group.tenant_id, group.owner_user_id)
                    .await?;
                sqlx::query(
                    "INSERT INTO user_groups (id, tenant_id, owner_user_id, name, metadata)
                     VALUES (?, ?, ?, ?, ?)",
                )
                .bind(id.to_string())
                .bind(group.tenant_id.to_string())
                .bind(group.owner_user_id.to_string())
                .bind(&group.name)
                .bind(group.metadata.to_string())
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_tenant_user(
                    &mut transaction,
                    group.tenant_id,
                    group.owner_user_id,
                )
                .await?;
                sqlx::query(
                    "INSERT INTO user_groups (id, tenant_id, owner_user_id, name, metadata)
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(id)
                .bind(group.tenant_id)
                .bind(group.owner_user_id)
                .bind(&group.name)
                .bind(Json(group.metadata.clone()))
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
            }
        }
        Ok(UserGroup {
            id,
            tenant_id: group.tenant_id,
            owner_user_id: group.owner_user_id,
            name: group.name,
            metadata: group.metadata,
        })
    }

    pub async fn add_user_to_group(
        &self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        group_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Result<bool, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
                sqlite_require_tenant_group(&mut transaction, tenant_id, group_id).await?;
                sqlite_require_tenant_user(&mut transaction, tenant_id, user_id).await?;
                let inserted = sqlx::query(
                    "INSERT INTO user_group_members (tenant_id, group_id, user_id)
                     VALUES (?, ?, ?)
                     ON CONFLICT (group_id, user_id) DO NOTHING",
                )
                .bind(tenant_id.to_string())
                .bind(group_id.to_string())
                .bind(user_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if inserted {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::GroupMemberAdded,
                        AuditTargetType::UserGroup,
                        group_id.to_string(),
                        serde_json::json!({"member_user_id": user_id.to_string()}),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(inserted)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
                timescale_require_tenant_group(&mut transaction, tenant_id, group_id).await?;
                timescale_require_tenant_user(&mut transaction, tenant_id, user_id).await?;
                let inserted = sqlx::query(
                    "INSERT INTO user_group_members (tenant_id, group_id, user_id)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (group_id, user_id) DO NOTHING",
                )
                .bind(tenant_id)
                .bind(group_id)
                .bind(user_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if inserted {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::GroupMemberAdded,
                        AuditTargetType::UserGroup,
                        group_id.to_string(),
                        serde_json::json!({"member_user_id": user_id.to_string()}),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(inserted)
            }
        }
    }

    pub async fn remove_user_from_group(
        &self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        group_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Result<bool, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
                sqlite_require_tenant_group(&mut transaction, tenant_id, group_id).await?;
                sqlite_require_tenant_user(&mut transaction, tenant_id, user_id).await?;
                let removed = sqlx::query(
                    "DELETE FROM user_group_members
                     WHERE tenant_id = ? AND group_id = ? AND user_id = ?",
                )
                .bind(tenant_id.to_string())
                .bind(group_id.to_string())
                .bind(user_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if removed {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::GroupMemberRemoved,
                        AuditTargetType::UserGroup,
                        group_id.to_string(),
                        serde_json::json!({"member_user_id": user_id.to_string()}),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(removed)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
                timescale_require_tenant_group(&mut transaction, tenant_id, group_id).await?;
                timescale_require_tenant_user(&mut transaction, tenant_id, user_id).await?;
                let removed = sqlx::query(
                    "DELETE FROM user_group_members
                     WHERE tenant_id = $1 AND group_id = $2 AND user_id = $3",
                )
                .bind(tenant_id)
                .bind(group_id)
                .bind(user_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if removed {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::GroupMemberRemoved,
                        AuditTargetType::UserGroup,
                        group_id.to_string(),
                        serde_json::json!({"member_user_id": user_id.to_string()}),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(removed)
            }
        }
    }

    pub async fn create_resource_permission(
        &self,
        permission: NewResourcePermission,
    ) -> Result<ResourcePermissionRecord, TenantAuthorizationError> {
        validate_new_resource_permission(&permission)?;
        let id = uuid::Uuid::now_v7();
        let (created_by_user_id, created_by_tenant_account_id) =
            permission_creator_ids(permission.created_by);
        let subject = match permission.subject_user_id {
            Some(user_id) => serde_json::json!({"kind": "user", "id": user_id.to_string()}),
            None => serde_json::json!({
                "kind": "group",
                "id": permission.subject_group_id.expect("validated resource permission").to_string(),
            }),
        };
        let resource = match permission.asset_id {
            Some(asset_id) => serde_json::json!({"kind": "asset", "id": asset_id.to_string()}),
            None => serde_json::json!({
                "kind": "device",
                "id": permission.device_id.as_deref().expect("validated resource permission"),
            }),
        };
        let audit_event = audit::NewAuditEvent::new(
            permission.tenant_id,
            audit_principal_for_permission_creator(permission.created_by),
            AuditAction::PermissionGranted,
            AuditTargetType::ResourcePermission,
            id.to_string(),
            serde_json::json!({
                "subject": subject,
                "resource": resource,
                "permission": permission.permission.as_str(),
                "inherit_children": permission.inherit_children,
            }),
        );
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_validate_permission_references(&mut transaction, &permission).await?;
                sqlx::query(
                    "INSERT INTO resource_permissions (
                        id, tenant_id, subject_user_id, subject_group_id, asset_id, device_id,
                        permission, inherit_children, created_by_user_id, created_by_tenant_account_id
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(id.to_string())
                .bind(permission.tenant_id.to_string())
                .bind(permission.subject_user_id.map(|value| value.to_string()))
                .bind(permission.subject_group_id.map(|value| value.to_string()))
                .bind(permission.asset_id.map(|value| value.to_string()))
                .bind(permission.device_id.as_deref())
                .bind(permission.permission.as_str())
                .bind(i64::from(permission.inherit_children))
                .bind(created_by_user_id.map(|value| value.to_string()))
                .bind(created_by_tenant_account_id.map(|value| value.to_string()))
                .execute(&mut *transaction)
                .await?;
                audit::insert_sqlite_audit_event(&mut transaction, &audit_event).await?;
                transaction.commit().await?;
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_validate_permission_references(&mut transaction, &permission).await?;
                sqlx::query(
                    "INSERT INTO resource_permissions (
                        id, tenant_id, subject_user_id, subject_group_id, asset_id, device_id,
                        permission, inherit_children, created_by_user_id, created_by_tenant_account_id
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                )
                .bind(id)
                .bind(permission.tenant_id)
                .bind(permission.subject_user_id)
                .bind(permission.subject_group_id)
                .bind(permission.asset_id)
                .bind(permission.device_id.as_deref())
                .bind(permission.permission.as_str())
                .bind(permission.inherit_children)
                .bind(created_by_user_id)
                .bind(created_by_tenant_account_id)
                .execute(&mut *transaction)
                .await?;
                audit::insert_timescale_audit_event(&mut transaction, &audit_event).await?;
                transaction.commit().await?;
            }
        }
        Ok(ResourcePermissionRecord {
            id,
            tenant_id: permission.tenant_id,
            subject_user_id: permission.subject_user_id,
            subject_group_id: permission.subject_group_id,
            asset_id: permission.asset_id,
            device_id: permission.device_id,
            permission: permission.permission,
            inherit_children: permission.inherit_children,
            created_by: permission.created_by,
        })
    }

    pub async fn revoke_resource_permission(
        &self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        permission_id: uuid::Uuid,
    ) -> Result<bool, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
                sqlite_require_tenant_permission(&mut transaction, tenant_id, permission_id)
                    .await?;
                let revoked = sqlx::query(
                    "UPDATE resource_permissions
                     SET revoked_at = ?
                     WHERE id = ? AND tenant_id = ? AND revoked_at IS NULL",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(permission_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if revoked {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::PermissionRevoked,
                        AuditTargetType::ResourcePermission,
                        permission_id.to_string(),
                        serde_json::json!({"revoked": true}),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(revoked)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_tenant_audit_actor(&mut transaction, tenant_id, actor).await?;
                timescale_require_tenant_permission(&mut transaction, tenant_id, permission_id)
                    .await?;
                let revoked = sqlx::query(
                    "UPDATE resource_permissions
                     SET revoked_at = now()
                     WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
                )
                .bind(permission_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if revoked {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        actor,
                        AuditAction::PermissionRevoked,
                        AuditTargetType::ResourcePermission,
                        permission_id.to_string(),
                        serde_json::json!({"revoked": true}),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(revoked)
            }
        }
    }

    pub async fn create_owner_resource_permission(
        &self,
        tenant_id: uuid::Uuid,
        owner_user_id: uuid::Uuid,
        recipient_user_id: uuid::Uuid,
        target: OwnershipTransferTarget,
        permission: ResourcePermission,
    ) -> Result<ResourcePermissionRecord, TenantAuthorizationError> {
        if !matches!(
            permission,
            ResourcePermission::Viewer | ResourcePermission::Manager
        ) {
            return Err(TenantAuthorizationError::InvalidPermissionLevel { permission });
        }
        if owner_user_id == recipient_user_id {
            return Err(TenantAuthorizationError::OwnerCannotShareWithSelf);
        }

        let id = uuid::Uuid::now_v7();
        let (asset_id, device_id) = resource_permission_scope(&target);
        let audit_event = audit::NewAuditEvent::new(
            tenant_id,
            AuditPrincipal::User(owner_user_id),
            AuditAction::PermissionGranted,
            AuditTargetType::ResourcePermission,
            id.to_string(),
            serde_json::json!({
                "subject": {"kind": "user", "id": recipient_user_id.to_string()},
                "resource": resource_permission_target_json(&target),
                "permission": permission.as_str(),
                "inherit_children": false,
            }),
        );

        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_regular_tenant_user(&mut transaction, tenant_id, owner_user_id)
                    .await?;
                sqlite_require_regular_tenant_user(&mut transaction, tenant_id, recipient_user_id)
                    .await?;
                sqlite_require_resource_owner(&mut transaction, tenant_id, &target, owner_user_id)
                    .await?;
                sqlx::query(
                    "INSERT INTO resource_permissions (
                        id, tenant_id, subject_user_id, asset_id, device_id,
                        permission, inherit_children, created_by_user_id
                     ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
                )
                .bind(id.to_string())
                .bind(tenant_id.to_string())
                .bind(recipient_user_id.to_string())
                .bind(asset_id.map(|value| value.to_string()))
                .bind(device_id.as_deref())
                .bind(permission.as_str())
                .bind(owner_user_id.to_string())
                .execute(&mut *transaction)
                .await?;
                audit::insert_sqlite_audit_event(&mut transaction, &audit_event).await?;
                transaction.commit().await?;
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_regular_tenant_user(&mut transaction, tenant_id, owner_user_id)
                    .await?;
                timescale_require_regular_tenant_user(
                    &mut transaction,
                    tenant_id,
                    recipient_user_id,
                )
                .await?;
                timescale_require_resource_owner(
                    &mut transaction,
                    tenant_id,
                    &target,
                    owner_user_id,
                )
                .await?;
                sqlx::query(
                    "INSERT INTO resource_permissions (
                        id, tenant_id, subject_user_id, asset_id, device_id,
                        permission, inherit_children, created_by_user_id
                     ) VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7)",
                )
                .bind(id)
                .bind(tenant_id)
                .bind(recipient_user_id)
                .bind(asset_id)
                .bind(device_id.as_deref())
                .bind(permission.as_str())
                .bind(owner_user_id)
                .execute(&mut *transaction)
                .await?;
                audit::insert_timescale_audit_event(&mut transaction, &audit_event).await?;
                transaction.commit().await?;
            }
        }

        Ok(ResourcePermissionRecord {
            id,
            tenant_id,
            subject_user_id: Some(recipient_user_id),
            subject_group_id: None,
            asset_id,
            device_id,
            permission,
            inherit_children: false,
            created_by: PermissionCreator::User(owner_user_id),
        })
    }

    pub async fn create_owner_resource_invitation(
        &self,
        tenant_id: uuid::Uuid,
        sender: TenantActor,
        recipient_user_id: uuid::Uuid,
        target: OwnershipTransferTarget,
        permission: ResourcePermission,
    ) -> Result<ResourceInvitation, TenantAuthorizationError> {
        if !matches!(
            permission,
            ResourcePermission::Viewer | ResourcePermission::Manager
        ) {
            return Err(TenantAuthorizationError::InvalidPermissionLevel { permission });
        }
        if matches!(sender, TenantActor::TenantUser(sender_user_id) if sender_user_id == recipient_user_id)
        {
            return Err(TenantAuthorizationError::OwnerCannotShareWithSelf);
        }
        let (asset_id, device_id) = resource_permission_scope(&target);
        let (sender_principal_kind, sender_principal_id) = tenant_actor_storage_fields(sender);
        let invitation_id = uuid::Uuid::now_v7();

        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_resource_invitation_sender(
                    &mut transaction,
                    tenant_id,
                    &target,
                    sender,
                )
                .await?;
                sqlite_require_regular_tenant_user(&mut transaction, tenant_id, recipient_user_id)
                    .await?;
                let existing_id = match asset_id {
                    Some(asset_id) => {
                        sqlx::query_scalar::<_, String>(
                            "SELECT id FROM resource_invitations
                         WHERE tenant_id = ? AND asset_id = ? AND recipient_user_id = ?
                           AND state = 'pending'",
                        )
                        .bind(tenant_id.to_string())
                        .bind(asset_id.to_string())
                        .bind(recipient_user_id.to_string())
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                    None => {
                        sqlx::query_scalar::<_, String>(
                            "SELECT id FROM resource_invitations
                         WHERE tenant_id = ? AND device_id = ? AND recipient_user_id = ?
                           AND state = 'pending'",
                        )
                        .bind(tenant_id.to_string())
                        .bind(device_id.as_deref())
                        .bind(recipient_user_id.to_string())
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                };
                let id = match existing_id {
                    Some(existing_id) => {
                        sqlx::query(
                            "UPDATE resource_invitations
                             SET permission = ?, sender_principal_kind = ?,
                                 sender_principal_id = ?, updated_at = ?
                             WHERE id = ? AND tenant_id = ?",
                        )
                        .bind(permission.as_str())
                        .bind(sender_principal_kind)
                        .bind(sender_principal_id.to_string())
                        .bind(Utc::now().to_rfc3339())
                        .bind(&existing_id)
                        .bind(tenant_id.to_string())
                        .execute(&mut *transaction)
                        .await?;
                        uuid::Uuid::parse_str(&existing_id)
                            .map_err(|_| TenantAuthorizationError::InvalidStoredRecord)?
                    }
                    None => {
                        sqlx::query(
                            "INSERT INTO resource_invitations (
                                id, tenant_id, sender_principal_kind, sender_principal_id,
                                recipient_user_id, asset_id, device_id, permission, state
                             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
                        )
                        .bind(invitation_id.to_string())
                        .bind(tenant_id.to_string())
                        .bind(sender_principal_kind)
                        .bind(sender_principal_id.to_string())
                        .bind(recipient_user_id.to_string())
                        .bind(asset_id.map(|id| id.to_string()))
                        .bind(device_id.as_deref())
                        .bind(permission.as_str())
                        .execute(&mut *transaction)
                        .await?;
                        invitation_id
                    }
                };
                transaction.commit().await?;
                Ok(ResourceInvitation {
                    id,
                    tenant_id,
                    sender,
                    recipient_user_id,
                    asset_id,
                    device_id,
                    permission,
                    state: ResourceInvitationState::Pending,
                })
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_resource_invitation_sender(
                    &mut transaction,
                    tenant_id,
                    &target,
                    sender,
                )
                .await?;
                timescale_require_regular_tenant_user(
                    &mut transaction,
                    tenant_id,
                    recipient_user_id,
                )
                .await?;
                let existing_id = match asset_id {
                    Some(asset_id) => {
                        sqlx::query_scalar::<_, uuid::Uuid>(
                            "SELECT id FROM resource_invitations
                         WHERE tenant_id = $1 AND asset_id = $2 AND recipient_user_id = $3
                           AND state = 'pending'
                         FOR UPDATE",
                        )
                        .bind(tenant_id)
                        .bind(asset_id)
                        .bind(recipient_user_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                    None => {
                        sqlx::query_scalar::<_, uuid::Uuid>(
                            "SELECT id FROM resource_invitations
                         WHERE tenant_id = $1 AND device_id = $2 AND recipient_user_id = $3
                           AND state = 'pending'
                         FOR UPDATE",
                        )
                        .bind(tenant_id)
                        .bind(device_id.as_deref())
                        .bind(recipient_user_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                };
                let id = match existing_id {
                    Some(existing_id) => {
                        sqlx::query(
                            "UPDATE resource_invitations
                             SET permission = $2, sender_principal_kind = $3,
                                 sender_principal_id = $4, updated_at = now()
                             WHERE id = $1 AND tenant_id = $5",
                        )
                        .bind(existing_id)
                        .bind(permission.as_str())
                        .bind(sender_principal_kind)
                        .bind(sender_principal_id)
                        .bind(tenant_id)
                        .execute(&mut *transaction)
                        .await?;
                        existing_id
                    }
                    None => {
                        sqlx::query(
                            "INSERT INTO resource_invitations (
                                id, tenant_id, sender_principal_kind, sender_principal_id,
                                recipient_user_id, asset_id, device_id, permission, state
                             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')",
                        )
                        .bind(invitation_id)
                        .bind(tenant_id)
                        .bind(sender_principal_kind)
                        .bind(sender_principal_id)
                        .bind(recipient_user_id)
                        .bind(asset_id)
                        .bind(device_id.as_deref())
                        .bind(permission.as_str())
                        .execute(&mut *transaction)
                        .await?;
                        invitation_id
                    }
                };
                transaction.commit().await?;
                Ok(ResourceInvitation {
                    id,
                    tenant_id,
                    sender,
                    recipient_user_id,
                    asset_id,
                    device_id,
                    permission,
                    state: ResourceInvitationState::Pending,
                })
            }
        }
    }

    pub async fn accept_resource_invitation(
        &self,
        tenant_id: uuid::Uuid,
        recipient_user_id: uuid::Uuid,
        invitation_id: uuid::Uuid,
    ) -> Result<ResourceInvitation, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_regular_tenant_user(&mut transaction, tenant_id, recipient_user_id)
                    .await?;
                let (sender_kind, sender_id, asset_id, device_id, permission, state) =
                    sqlx::query_as::<
                        _,
                        (
                            String,
                            String,
                            Option<String>,
                            Option<String>,
                            String,
                            String,
                        ),
                    >(
                        "SELECT sender_principal_kind, sender_principal_id,
                                asset_id, device_id, permission, state
                     FROM resource_invitations
                     WHERE id = ? AND tenant_id = ? AND recipient_user_id = ?",
                    )
                    .bind(invitation_id.to_string())
                    .bind(tenant_id.to_string())
                    .bind(recipient_user_id.to_string())
                    .fetch_optional(&mut *transaction)
                    .await?
                    .ok_or(TenantAuthorizationError::InvitationNotFound {
                        tenant_id,
                        invitation_id,
                    })?;
                let state = ResourceInvitationState::parse(&state)
                    .ok_or(TenantAuthorizationError::InvalidStoredRecord)?;
                if state != ResourceInvitationState::Pending {
                    return Err(TenantAuthorizationError::InvitationNotPending { invitation_id });
                }
                let sender = tenant_actor_from_storage(&sender_kind, &sender_id)?;
                let target =
                    invitation_target_from_sqlite(asset_id.as_deref(), device_id.as_deref())?;
                let permission = invitation_permission(&permission)?;
                sqlite_require_resource_invitation_sender(
                    &mut transaction,
                    tenant_id,
                    &target,
                    sender,
                )
                .await?;
                let (target_asset_id, target_device_id) = resource_permission_scope(&target);
                let existing_permission = match target_asset_id {
                    Some(asset_id) => {
                        sqlx::query_as::<_, (String, String)>(
                            "SELECT id, permission FROM resource_permissions
                         WHERE tenant_id = ? AND subject_user_id = ? AND asset_id = ?
                           AND revoked_at IS NULL",
                        )
                        .bind(tenant_id.to_string())
                        .bind(recipient_user_id.to_string())
                        .bind(asset_id.to_string())
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                    None => {
                        sqlx::query_as::<_, (String, String)>(
                            "SELECT id, permission FROM resource_permissions
                         WHERE tenant_id = ? AND subject_user_id = ? AND device_id = ?
                           AND revoked_at IS NULL",
                        )
                        .bind(tenant_id.to_string())
                        .bind(recipient_user_id.to_string())
                        .bind(target_device_id.as_deref())
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                };
                match existing_permission {
                    Some((permission_id, current_permission)) => {
                        let current_permission = ResourcePermission::parse(&current_permission)
                            .ok_or(TenantAuthorizationError::InvalidStoredRecord)?;
                        if permission > current_permission {
                            sqlx::query(
                                "UPDATE resource_permissions
                                 SET permission = ?
                                 WHERE id = ? AND tenant_id = ? AND revoked_at IS NULL",
                            )
                            .bind(permission.as_str())
                            .bind(permission_id)
                            .bind(tenant_id.to_string())
                            .execute(&mut *transaction)
                            .await?;
                        }
                    }
                    None => {
                        let permission_id = uuid::Uuid::now_v7();
                        let (created_by_user_id, created_by_tenant_account_id) =
                            invitation_permission_creator_ids(sender);
                        sqlx::query(
                            "INSERT INTO resource_permissions (
                                id, tenant_id, subject_user_id, asset_id, device_id,
                                permission, inherit_children, created_by_user_id,
                                created_by_tenant_account_id
                             ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)",
                        )
                        .bind(permission_id.to_string())
                        .bind(tenant_id.to_string())
                        .bind(recipient_user_id.to_string())
                        .bind(target_asset_id.map(|id| id.to_string()))
                        .bind(target_device_id.as_deref())
                        .bind(permission.as_str())
                        .bind(created_by_user_id.map(|id| id.to_string()))
                        .bind(created_by_tenant_account_id.map(|id| id.to_string()))
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
                sqlx::query(
                    "UPDATE resource_invitations
                     SET state = 'accepted', updated_at = ?, accepted_at = ?, closed_at = ?
                     WHERE id = ? AND tenant_id = ?",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(Utc::now().to_rfc3339())
                .bind(Utc::now().to_rfc3339())
                .bind(invitation_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(ResourceInvitation {
                    id: invitation_id,
                    tenant_id,
                    sender,
                    recipient_user_id,
                    asset_id: target_asset_id,
                    device_id: target_device_id,
                    permission,
                    state: ResourceInvitationState::Accepted,
                })
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_regular_tenant_user(
                    &mut transaction,
                    tenant_id,
                    recipient_user_id,
                )
                .await?;
                let (sender_kind, sender_id, asset_id, device_id, permission, state) =
                    sqlx::query_as::<
                        _,
                        (
                            String,
                            uuid::Uuid,
                            Option<uuid::Uuid>,
                            Option<String>,
                            String,
                            String,
                        ),
                    >(
                        "SELECT sender_principal_kind, sender_principal_id,
                            asset_id, device_id, permission, state
                     FROM resource_invitations
                     WHERE id = $1 AND tenant_id = $2 AND recipient_user_id = $3
                     FOR UPDATE",
                    )
                    .bind(invitation_id)
                    .bind(tenant_id)
                    .bind(recipient_user_id)
                    .fetch_optional(&mut *transaction)
                    .await?
                    .ok_or(TenantAuthorizationError::InvitationNotFound {
                        tenant_id,
                        invitation_id,
                    })?;
                let state = ResourceInvitationState::parse(&state)
                    .ok_or(TenantAuthorizationError::InvalidStoredRecord)?;
                if state != ResourceInvitationState::Pending {
                    return Err(TenantAuthorizationError::InvitationNotPending { invitation_id });
                }
                let sender = tenant_actor_from_postgres_storage(&sender_kind, sender_id)?;
                let target = invitation_target_from_timescale(asset_id, device_id.as_deref())?;
                let permission = invitation_permission(&permission)?;
                timescale_require_resource_invitation_sender(
                    &mut transaction,
                    tenant_id,
                    &target,
                    sender,
                )
                .await?;
                let (target_asset_id, target_device_id) = resource_permission_scope(&target);
                let existing_permission = match target_asset_id {
                    Some(asset_id) => {
                        sqlx::query_as::<_, (uuid::Uuid, String)>(
                            "SELECT id, permission FROM resource_permissions
                         WHERE tenant_id = $1 AND subject_user_id = $2 AND asset_id = $3
                           AND revoked_at IS NULL
                         FOR UPDATE",
                        )
                        .bind(tenant_id)
                        .bind(recipient_user_id)
                        .bind(asset_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                    None => {
                        sqlx::query_as::<_, (uuid::Uuid, String)>(
                            "SELECT id, permission FROM resource_permissions
                         WHERE tenant_id = $1 AND subject_user_id = $2 AND device_id = $3
                           AND revoked_at IS NULL
                         FOR UPDATE",
                        )
                        .bind(tenant_id)
                        .bind(recipient_user_id)
                        .bind(target_device_id.as_deref())
                        .fetch_optional(&mut *transaction)
                        .await?
                    }
                };
                match existing_permission {
                    Some((permission_id, current_permission)) => {
                        let current_permission = ResourcePermission::parse(&current_permission)
                            .ok_or(TenantAuthorizationError::InvalidStoredRecord)?;
                        if permission > current_permission {
                            sqlx::query(
                                "UPDATE resource_permissions
                                 SET permission = $1
                                 WHERE id = $2 AND tenant_id = $3 AND revoked_at IS NULL",
                            )
                            .bind(permission.as_str())
                            .bind(permission_id)
                            .bind(tenant_id)
                            .execute(&mut *transaction)
                            .await?;
                        }
                    }
                    None => {
                        let (created_by_user_id, created_by_tenant_account_id) =
                            invitation_permission_creator_ids(sender);
                        sqlx::query(
                            "INSERT INTO resource_permissions (
                                id, tenant_id, subject_user_id, asset_id, device_id,
                                permission, inherit_children, created_by_user_id,
                                created_by_tenant_account_id
                             ) VALUES ($1, $2, $3, $4, $5, $6, FALSE, $7, $8)",
                        )
                        .bind(uuid::Uuid::now_v7())
                        .bind(tenant_id)
                        .bind(recipient_user_id)
                        .bind(target_asset_id)
                        .bind(target_device_id.as_deref())
                        .bind(permission.as_str())
                        .bind(created_by_user_id)
                        .bind(created_by_tenant_account_id)
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
                sqlx::query(
                    "UPDATE resource_invitations
                     SET state = 'accepted', updated_at = now(), accepted_at = now(), closed_at = now()
                     WHERE id = $1 AND tenant_id = $2",
                )
                .bind(invitation_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(ResourceInvitation {
                    id: invitation_id,
                    tenant_id,
                    sender,
                    recipient_user_id,
                    asset_id: target_asset_id,
                    device_id: target_device_id,
                    permission,
                    state: ResourceInvitationState::Accepted,
                })
            }
        }
    }

    pub async fn cancel_resource_invitation(
        &self,
        tenant_id: uuid::Uuid,
        actor: TenantActor,
        invitation_id: uuid::Uuid,
    ) -> Result<bool, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                let cancelled = match actor {
                    TenantActor::TenantUser(recipient_user_id) => {
                        sqlite_require_regular_tenant_user(
                            &mut transaction,
                            tenant_id,
                            recipient_user_id,
                        )
                        .await?;
                        sqlx::query(
                            "UPDATE resource_invitations
                             SET state = 'cancelled', updated_at = ?, closed_at = ?
                             WHERE id = ? AND tenant_id = ? AND recipient_user_id = ?
                               AND state = 'pending'",
                        )
                        .bind(Utc::now().to_rfc3339())
                        .bind(Utc::now().to_rfc3339())
                        .bind(invitation_id.to_string())
                        .bind(tenant_id.to_string())
                        .bind(recipient_user_id.to_string())
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected()
                            > 0
                    }
                    TenantActor::TenantAccount(tenant_account_id) => {
                        sqlite_require_tenant_account(
                            &mut transaction,
                            tenant_id,
                            tenant_account_id,
                        )
                        .await?;
                        sqlx::query(
                            "UPDATE resource_invitations
                             SET state = 'cancelled', updated_at = ?, closed_at = ?
                             WHERE id = ? AND tenant_id = ? AND state = 'pending'",
                        )
                        .bind(Utc::now().to_rfc3339())
                        .bind(Utc::now().to_rfc3339())
                        .bind(invitation_id.to_string())
                        .bind(tenant_id.to_string())
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected()
                            > 0
                    }
                };
                transaction.commit().await?;
                Ok(cancelled)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let cancelled = match actor {
                    TenantActor::TenantUser(recipient_user_id) => {
                        timescale_require_regular_tenant_user(
                            &mut transaction,
                            tenant_id,
                            recipient_user_id,
                        )
                        .await?;
                        sqlx::query(
                            "UPDATE resource_invitations
                             SET state = 'cancelled', updated_at = now(), closed_at = now()
                             WHERE id = $1 AND tenant_id = $2 AND recipient_user_id = $3
                               AND state = 'pending'",
                        )
                        .bind(invitation_id)
                        .bind(tenant_id)
                        .bind(recipient_user_id)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected()
                            > 0
                    }
                    TenantActor::TenantAccount(tenant_account_id) => {
                        timescale_require_tenant_account(
                            &mut transaction,
                            tenant_id,
                            tenant_account_id,
                        )
                        .await?;
                        sqlx::query(
                            "UPDATE resource_invitations
                             SET state = 'cancelled', updated_at = now(), closed_at = now()
                             WHERE id = $1 AND tenant_id = $2 AND state = 'pending'",
                        )
                        .bind(invitation_id)
                        .bind(tenant_id)
                        .execute(&mut *transaction)
                        .await?
                        .rows_affected()
                            > 0
                    }
                };
                transaction.commit().await?;
                Ok(cancelled)
            }
        }
    }

    pub async fn list_pending_resource_invitations(
        &self,
        tenant_id: uuid::Uuid,
        recipient_user_id: uuid::Uuid,
    ) -> Result<Vec<ResourceInvitation>, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query_as::<
                    _,
                    (
                        String,
                        String,
                        String,
                        String,
                        Option<String>,
                        Option<String>,
                        String,
                    ),
                >(
                    "SELECT id, sender_principal_kind, sender_principal_id,
                            recipient_user_id, asset_id, device_id, permission
                     FROM resource_invitations
                     WHERE tenant_id = ? AND recipient_user_id = ? AND state = 'pending'
                     ORDER BY created_at, id",
                )
                .bind(tenant_id.to_string())
                .bind(recipient_user_id.to_string())
                .fetch_all(store.pool())
                .await?;
                rows.into_iter()
                    .map(
                        |(
                            id,
                            sender_kind,
                            sender_id,
                            recipient,
                            asset_id,
                            device_id,
                            permission,
                        )| {
                            let id = uuid::Uuid::parse_str(&id)
                                .map_err(|_| TenantAuthorizationError::InvalidStoredRecord)?;
                            let sender = tenant_actor_from_storage(&sender_kind, &sender_id)?;
                            let recipient_user_id = uuid::Uuid::parse_str(&recipient)
                                .map_err(|_| TenantAuthorizationError::InvalidStoredRecord)?;
                            let target = invitation_target_from_sqlite(
                                asset_id.as_deref(),
                                device_id.as_deref(),
                            )?;
                            let (asset_id, device_id) = resource_permission_scope(&target);
                            Ok(ResourceInvitation {
                                id,
                                tenant_id,
                                sender,
                                recipient_user_id,
                                asset_id,
                                device_id,
                                permission: invitation_permission(&permission)?,
                                state: ResourceInvitationState::Pending,
                            })
                        },
                    )
                    .collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query_as::<
                    _,
                    (
                        uuid::Uuid,
                        String,
                        uuid::Uuid,
                        uuid::Uuid,
                        Option<uuid::Uuid>,
                        Option<String>,
                        String,
                    ),
                >(
                    "SELECT id, sender_principal_kind, sender_principal_id,
                            recipient_user_id, asset_id, device_id, permission
                     FROM resource_invitations
                     WHERE tenant_id = $1 AND recipient_user_id = $2 AND state = 'pending'
                     ORDER BY created_at, id",
                )
                .bind(tenant_id)
                .bind(recipient_user_id)
                .fetch_all(pool)
                .await?;
                rows.into_iter()
                    .map(
                        |(
                            id,
                            sender_kind,
                            sender_id,
                            recipient_user_id,
                            asset_id,
                            device_id,
                            permission,
                        )| {
                            let sender =
                                tenant_actor_from_postgres_storage(&sender_kind, sender_id)?;
                            let target =
                                invitation_target_from_timescale(asset_id, device_id.as_deref())?;
                            let (asset_id, device_id) = resource_permission_scope(&target);
                            Ok(ResourceInvitation {
                                id,
                                tenant_id,
                                sender,
                                recipient_user_id,
                                asset_id,
                                device_id,
                                permission: invitation_permission(&permission)?,
                                state: ResourceInvitationState::Pending,
                            })
                        },
                    )
                    .collect()
            }
        }
    }

    pub async fn revoke_owner_resource_permission(
        &self,
        tenant_id: uuid::Uuid,
        owner_user_id: uuid::Uuid,
        target: OwnershipTransferTarget,
        permission_id: uuid::Uuid,
    ) -> Result<bool, TenantAuthorizationError> {
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_regular_tenant_user(&mut transaction, tenant_id, owner_user_id)
                    .await?;
                sqlite_require_resource_owner(&mut transaction, tenant_id, &target, owner_user_id)
                    .await?;
                let (asset_id, device_id) = sqlx::query_as::<_, (Option<String>, Option<String>)>(
                    "SELECT asset_id, device_id FROM resource_permissions WHERE id = ? AND tenant_id = ?",
                )
                .bind(permission_id.to_string())
                .bind(tenant_id.to_string())
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(TenantAuthorizationError::PermissionNotFound {
                    tenant_id,
                    permission_id,
                })?;
                if !resource_permission_scope_matches(
                    &target,
                    asset_id.as_deref(),
                    device_id.as_deref(),
                ) {
                    return Err(TenantAuthorizationError::PermissionNotFound {
                        tenant_id,
                        permission_id,
                    });
                }
                let revoked = sqlx::query(
                    "UPDATE resource_permissions
                     SET revoked_at = ?
                     WHERE id = ? AND tenant_id = ? AND revoked_at IS NULL",
                )
                .bind(Utc::now().to_rfc3339())
                .bind(permission_id.to_string())
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if revoked {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        AuditPrincipal::User(owner_user_id),
                        AuditAction::PermissionRevoked,
                        AuditTargetType::ResourcePermission,
                        permission_id.to_string(),
                        serde_json::json!({"revoked": true}),
                    );
                    audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(revoked)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_regular_tenant_user(&mut transaction, tenant_id, owner_user_id)
                    .await?;
                timescale_require_resource_owner(
                    &mut transaction,
                    tenant_id,
                    &target,
                    owner_user_id,
                )
                .await?;
                let (asset_id, device_id) = sqlx::query_as::<_, (Option<uuid::Uuid>, Option<String>)>(
                    "SELECT asset_id, device_id FROM resource_permissions WHERE id = $1 AND tenant_id = $2 FOR UPDATE",
                )
                .bind(permission_id)
                .bind(tenant_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(TenantAuthorizationError::PermissionNotFound {
                    tenant_id,
                    permission_id,
                })?;
                if !resource_permission_scope_matches(
                    &target,
                    asset_id.as_ref().map(uuid::Uuid::to_string).as_deref(),
                    device_id.as_deref(),
                ) {
                    return Err(TenantAuthorizationError::PermissionNotFound {
                        tenant_id,
                        permission_id,
                    });
                }
                let revoked = sqlx::query(
                    "UPDATE resource_permissions
                     SET revoked_at = now()
                     WHERE id = $1 AND tenant_id = $2 AND revoked_at IS NULL",
                )
                .bind(permission_id)
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?
                .rows_affected()
                    > 0;
                if revoked {
                    let event = audit::NewAuditEvent::new(
                        tenant_id,
                        AuditPrincipal::User(owner_user_id),
                        AuditAction::PermissionRevoked,
                        AuditTargetType::ResourcePermission,
                        permission_id.to_string(),
                        serde_json::json!({"revoked": true}),
                    );
                    audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                }
                transaction.commit().await?;
                Ok(revoked)
            }
        }
    }

    pub async fn transfer_resource_ownership(
        &self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        target: OwnershipTransferTarget,
        new_owner_user_id: Option<uuid::Uuid>,
    ) -> Result<bool, TenantAuthorizationError> {
        let tenant_account_id = match actor {
            AuditPrincipal::TenantAccount(tenant_account_id) => tenant_account_id,
            AuditPrincipal::SystemAccount(_) | AuditPrincipal::User(_) => {
                return Err(TenantAuthorizationError::TenantAccountRequiredForOwnership);
            }
        };
        let actor = AuditPrincipal::TenantAccount(tenant_account_id);
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin_with("BEGIN IMMEDIATE").await?;
                sqlite_require_tenant_account(&mut transaction, tenant_id, tenant_account_id)
                    .await?;
                if let Some(new_owner_user_id) = new_owner_user_id {
                    sqlite_require_regular_tenant_user(
                        &mut transaction,
                        tenant_id,
                        new_owner_user_id,
                    )
                    .await?;
                }
                let (target_type, target_id, previous_owner_user_id) = match &target {
                    OwnershipTransferTarget::Asset(asset_id) => {
                        let owner = sqlx::query_scalar::<_, Option<String>>(
                            "SELECT owner_user_id FROM assets WHERE id = ? AND tenant_id = ?",
                        )
                        .bind(asset_id.to_string())
                        .bind(tenant_id.to_string())
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or(
                            TenantAuthorizationError::AssetNotFound {
                                tenant_id,
                                asset_id: *asset_id,
                            },
                        )?;
                        (AuditTargetType::Asset, asset_id.to_string(), owner)
                    }
                    OwnershipTransferTarget::Device(device_id) => {
                        let owner = sqlx::query_scalar::<_, Option<String>>(
                            "SELECT owner_user_id FROM devices
                             WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
                        )
                        .bind(device_id)
                        .bind(tenant_id.to_string())
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or_else(|| {
                            TenantAuthorizationError::DeviceNotFound {
                                tenant_id,
                                device_id: device_id.clone(),
                            }
                        })?;
                        (AuditTargetType::Device, device_id.clone(), owner)
                    }
                };
                let next_owner_user_id = new_owner_user_id.map(|user_id| user_id.to_string());
                if previous_owner_user_id == next_owner_user_id {
                    transaction.commit().await?;
                    return Ok(false);
                }
                let revoked_permission_count = match &target {
                    OwnershipTransferTarget::Asset(asset_id) => sqlx::query(
                        "UPDATE resource_permissions
                         SET revoked_at = ?
                         WHERE tenant_id = ? AND asset_id = ? AND revoked_at IS NULL",
                    )
                    .bind(Utc::now().to_rfc3339())
                    .bind(tenant_id.to_string())
                    .bind(asset_id.to_string())
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected(),
                    OwnershipTransferTarget::Device(device_id) => sqlx::query(
                        "UPDATE resource_permissions
                         SET revoked_at = ?
                         WHERE tenant_id = ? AND device_id = ? AND revoked_at IS NULL",
                    )
                    .bind(Utc::now().to_rfc3339())
                    .bind(tenant_id.to_string())
                    .bind(device_id)
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected(),
                };
                match target {
                    OwnershipTransferTarget::Asset(asset_id) => {
                        sqlx::query(
                            "UPDATE assets SET owner_user_id = ? WHERE id = ? AND tenant_id = ?",
                        )
                        .bind(&next_owner_user_id)
                        .bind(asset_id.to_string())
                        .bind(tenant_id.to_string())
                        .execute(&mut *transaction)
                        .await?;
                    }
                    OwnershipTransferTarget::Device(device_id) => {
                        sqlx::query(
                            "UPDATE devices
                             SET owner_user_id = ?
                             WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
                        )
                        .bind(&next_owner_user_id)
                        .bind(device_id)
                        .bind(tenant_id.to_string())
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::OwnershipTransferred,
                    target_type,
                    target_id,
                    serde_json::json!({
                        "owner_user_id": {
                            "before": previous_owner_user_id,
                            "after": next_owner_user_id,
                        },
                        "revoked_permission_count": revoked_permission_count,
                    }),
                );
                audit::insert_sqlite_audit_event(&mut transaction, &event).await?;
                transaction.commit().await?;
                Ok(true)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                timescale_require_tenant_account(&mut transaction, tenant_id, tenant_account_id)
                    .await?;
                if let Some(new_owner_user_id) = new_owner_user_id {
                    timescale_require_regular_tenant_user(
                        &mut transaction,
                        tenant_id,
                        new_owner_user_id,
                    )
                    .await?;
                }
                let (target_type, target_id, previous_owner_user_id) = match &target {
                    OwnershipTransferTarget::Asset(asset_id) => {
                        let owner = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
                            "SELECT owner_user_id FROM assets
                             WHERE id = $1 AND tenant_id = $2
                             FOR UPDATE",
                        )
                        .bind(*asset_id)
                        .bind(tenant_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or(
                            TenantAuthorizationError::AssetNotFound {
                                tenant_id,
                                asset_id: *asset_id,
                            },
                        )?;
                        (
                            AuditTargetType::Asset,
                            asset_id.to_string(),
                            owner.map(|id| id.to_string()),
                        )
                    }
                    OwnershipTransferTarget::Device(device_id) => {
                        let owner = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
                            "SELECT owner_user_id FROM devices
                             WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
                             FOR UPDATE",
                        )
                        .bind(device_id)
                        .bind(tenant_id)
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or_else(|| {
                            TenantAuthorizationError::DeviceNotFound {
                                tenant_id,
                                device_id: device_id.clone(),
                            }
                        })?;
                        (
                            AuditTargetType::Device,
                            device_id.clone(),
                            owner.map(|id| id.to_string()),
                        )
                    }
                };
                let next_owner_user_id = new_owner_user_id.map(|user_id| user_id.to_string());
                if previous_owner_user_id == next_owner_user_id {
                    transaction.commit().await?;
                    return Ok(false);
                }
                let revoked_permission_count = match &target {
                    OwnershipTransferTarget::Asset(asset_id) => sqlx::query(
                        "UPDATE resource_permissions
                         SET revoked_at = now()
                         WHERE tenant_id = $1 AND asset_id = $2 AND revoked_at IS NULL",
                    )
                    .bind(tenant_id)
                    .bind(*asset_id)
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected(),
                    OwnershipTransferTarget::Device(device_id) => sqlx::query(
                        "UPDATE resource_permissions
                         SET revoked_at = now()
                         WHERE tenant_id = $1 AND device_id = $2 AND revoked_at IS NULL",
                    )
                    .bind(tenant_id)
                    .bind(device_id)
                    .execute(&mut *transaction)
                    .await?
                    .rows_affected(),
                };
                match target {
                    OwnershipTransferTarget::Asset(asset_id) => {
                        sqlx::query(
                            "UPDATE assets SET owner_user_id = $1 WHERE id = $2 AND tenant_id = $3",
                        )
                        .bind(new_owner_user_id)
                        .bind(asset_id)
                        .bind(tenant_id)
                        .execute(&mut *transaction)
                        .await?;
                    }
                    OwnershipTransferTarget::Device(device_id) => {
                        sqlx::query(
                            "UPDATE devices
                             SET owner_user_id = $1
                             WHERE device_id = $2 AND tenant_id = $3 AND deleted_at IS NULL",
                        )
                        .bind(new_owner_user_id)
                        .bind(device_id)
                        .bind(tenant_id)
                        .execute(&mut *transaction)
                        .await?;
                    }
                }
                let event = audit::NewAuditEvent::new(
                    tenant_id,
                    actor,
                    AuditAction::OwnershipTransferred,
                    target_type,
                    target_id,
                    serde_json::json!({
                        "owner_user_id": {
                            "before": previous_owner_user_id,
                            "after": next_owner_user_id,
                        },
                        "revoked_permission_count": revoked_permission_count,
                    }),
                );
                audit::insert_timescale_audit_event(&mut transaction, &event).await?;
                transaction.commit().await?;
                Ok(true)
            }
        }
    }
}

impl AuthorizationRepository for PlatformStore {
    fn authorization_subject<'a>(
        &'a self,
        user_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizationSubject>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::authorization_subject(self, user_id).await })
    }

    fn list_authorized_devices<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<AuthorizedDeviceListEntry>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::list_authorized_devices(self, subject, after, limit).await
        })
    }

    fn list_authorized_assets<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        after: Option<uuid::Uuid>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<AuthorizedAssetListEntry>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(
            async move { PlatformStore::list_authorized_assets(self, subject, after, limit).await },
        )
    }

    fn authorized_device<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizedDeviceSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::authorized_device(self, subject, device_id).await })
    }

    fn authorized_asset<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<AuthorizedAssetSummary>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::authorized_asset(self, subject, asset_id).await })
    }

    fn device_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::device_permission(self, subject, device_id).await })
    }

    fn asset_permission<'a>(
        &'a self,
        subject: &'a AuthorizationSubject,
        asset_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::asset_permission(self, subject, asset_id).await })
    }
}

impl TenantAuthorizationRepository for PlatformStore {
    fn list_tenant_user_groups<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<TenantUserGroup>, TenantAuthorizationError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::list_tenant_user_groups(self, tenant_id).await })
    }

    fn list_active_resource_permissions<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ResourcePermissionRecord>, TenantAuthorizationError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(
            async move { PlatformStore::list_active_resource_permissions(self, tenant_id).await },
        )
    }

    fn create_user_group<'a>(
        &'a self,
        group: NewUserGroup,
    ) -> Pin<Box<dyn Future<Output = Result<UserGroup, TenantAuthorizationError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::create_user_group(self, group).await })
    }

    fn add_user_to_group<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        group_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::add_user_to_group(self, tenant_id, actor, group_id, user_id).await
        })
    }

    fn remove_user_from_group<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        group_id: uuid::Uuid,
        user_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::remove_user_from_group(self, tenant_id, actor, group_id, user_id).await
        })
    }

    fn create_resource_permission<'a>(
        &'a self,
        permission: NewResourcePermission,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ResourcePermissionRecord, TenantAuthorizationError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { PlatformStore::create_resource_permission(self, permission).await })
    }

    fn revoke_resource_permission<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        permission_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::revoke_resource_permission(self, tenant_id, actor, permission_id).await
        })
    }

    fn create_owner_resource_permission<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        owner_user_id: uuid::Uuid,
        recipient_user_id: uuid::Uuid,
        target: OwnershipTransferTarget,
        permission: ResourcePermission,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ResourcePermissionRecord, TenantAuthorizationError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::create_owner_resource_permission(
                self,
                tenant_id,
                owner_user_id,
                recipient_user_id,
                target,
                permission,
            )
            .await
        })
    }

    fn revoke_owner_resource_permission<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        owner_user_id: uuid::Uuid,
        target: OwnershipTransferTarget,
        permission_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::revoke_owner_resource_permission(
                self,
                tenant_id,
                owner_user_id,
                target,
                permission_id,
            )
            .await
        })
    }

    fn transfer_resource_ownership<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: AuditPrincipal,
        target: OwnershipTransferTarget,
        new_owner_user_id: Option<uuid::Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::transfer_resource_ownership(
                self,
                tenant_id,
                actor,
                target,
                new_owner_user_id,
            )
            .await
        })
    }
}

impl ResourceInvitationRepository for PlatformStore {
    fn create_owner_resource_invitation<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        sender: TenantActor,
        recipient_user_id: uuid::Uuid,
        target: OwnershipTransferTarget,
        permission: ResourcePermission,
    ) -> Pin<
        Box<dyn Future<Output = Result<ResourceInvitation, TenantAuthorizationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            PlatformStore::create_owner_resource_invitation(
                self,
                tenant_id,
                sender,
                recipient_user_id,
                target,
                permission,
            )
            .await
        })
    }

    fn accept_resource_invitation<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        recipient_user_id: uuid::Uuid,
        invitation_id: uuid::Uuid,
    ) -> Pin<
        Box<dyn Future<Output = Result<ResourceInvitation, TenantAuthorizationError>> + Send + 'a>,
    > {
        Box::pin(async move {
            PlatformStore::accept_resource_invitation(
                self,
                tenant_id,
                recipient_user_id,
                invitation_id,
            )
            .await
        })
    }

    fn cancel_resource_invitation<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        actor: TenantActor,
        invitation_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, TenantAuthorizationError>> + Send + 'a>> {
        Box::pin(async move {
            PlatformStore::cancel_resource_invitation(self, tenant_id, actor, invitation_id).await
        })
    }

    fn list_pending_resource_invitations<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        recipient_user_id: uuid::Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ResourceInvitation>, TenantAuthorizationError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            PlatformStore::list_pending_resource_invitations(self, tenant_id, recipient_user_id)
                .await
        })
    }
}

fn strongest_resource_permission(rows: Vec<String>) -> Option<ResourcePermission> {
    rows.into_iter()
        .filter_map(|value| match value.as_str() {
            "viewer" => Some(ResourcePermission::Viewer),
            "manager" => Some(ResourcePermission::Manager),
            _ => None,
        })
        .max()
}

fn resource_access_from_storage(
    permission: String,
    source: String,
) -> Result<ResourceAccess, PlatformStoreError> {
    let permission = ResourcePermission::parse(&permission).ok_or_else(|| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid resource access permission".to_owned(),
        ))
    })?;
    let source = ResourceAccessSource::parse(&source).ok_or_else(|| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid resource access source".to_owned(),
        ))
    })?;
    Ok(ResourceAccess { permission, source })
}

fn parse_authorized_asset_id(value: &str) -> Result<uuid::Uuid, PlatformStoreError> {
    value.parse().map_err(|_| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid authorized asset ID".to_owned(),
        ))
    })
}

fn sqlite_authorized_asset_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<AuthorizedAssetListEntry, PlatformStoreError> {
    Ok(AuthorizedAssetListEntry {
        asset_id: parse_authorized_asset_id(&row.try_get::<String, _>("asset_id")?)?,
        name: row.try_get("name")?,
        parent_asset_id: row
            .try_get::<Option<String>, _>("parent_asset_id")?
            .as_deref()
            .map(parse_authorized_asset_id)
            .transpose()?,
        access: resource_access_from_storage(
            row.try_get("effective_permission")?,
            row.try_get("access_source")?,
        )?,
    })
}

fn timescale_authorized_asset_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<AuthorizedAssetListEntry, PlatformStoreError> {
    Ok(AuthorizedAssetListEntry {
        asset_id: row.try_get("asset_id")?,
        name: row.try_get("name")?,
        parent_asset_id: row.try_get("parent_asset_id")?,
        access: resource_access_from_storage(
            row.try_get("effective_permission")?,
            row.try_get("access_source")?,
        )?,
    })
}

async fn sqlite_device_resource_permission(
    pool: &SqlitePool,
    subject: &AuthorizationSubject,
    device_id: &str,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(subject.tenant_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some(owner) = owner else {
        return Ok(None);
    };
    if owner.as_deref() == Some(&subject.user_id.to_string()) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let user_id = subject.user_id.to_string();
    let tenant_id = subject.tenant_id.to_string();
    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT device.asset_id, 0
            FROM devices AS device
            WHERE device.device_id = ?
              AND device.tenant_id = ?
              AND device.deleted_at IS NULL
              AND device.asset_id IS NOT NULL
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = ?
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT permission.permission
         FROM resource_permissions AS permission
         WHERE permission.tenant_id = ?
           AND permission.revoked_at IS NULL
           AND (
                (permission.device_id = ? AND (
                    permission.subject_user_id = ?
                    OR EXISTS (
                        SELECT 1
                        FROM user_group_members AS membership
                        WHERE membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = ?
                    )
                ))
                OR (permission.asset_id IN (SELECT id FROM ancestors)
                    AND permission.inherit_children = 1 AND (
                        permission.subject_user_id = ?
                        OR EXISTS (
                            SELECT 1
                            FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = ?
                        )
                    ))
           )",
    )
    .bind(device_id)
    .bind(&tenant_id)
    .bind(&tenant_id)
    .bind(&tenant_id)
    .bind(device_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .bind(&user_id)
    .fetch_all(pool)
    .await?;
    Ok(strongest_resource_permission(rows))
}

async fn sqlite_asset_resource_permission(
    pool: &SqlitePool,
    subject: &AuthorizationSubject,
    asset_id: uuid::Uuid,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    let asset_id = asset_id.to_string();
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id FROM assets WHERE id = ? AND tenant_id = ?",
    )
    .bind(&asset_id)
    .bind(subject.tenant_id.to_string())
    .fetch_optional(pool)
    .await?;
    let Some(owner) = owner else {
        return Ok(None);
    };
    if owner.as_deref() == Some(&subject.user_id.to_string()) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let user_id = subject.user_id.to_string();
    let tenant_id = subject.tenant_id.to_string();
    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT ?, 0
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = ?
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT permission.permission
         FROM resource_permissions AS permission
         JOIN ancestors ON permission.asset_id = ancestors.id
         WHERE permission.tenant_id = ?
           AND permission.revoked_at IS NULL
           AND (ancestors.depth = 0 OR permission.inherit_children = 1)
           AND (
                permission.subject_user_id = ?
                OR EXISTS (
                    SELECT 1
                    FROM user_group_members AS membership
                    WHERE membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = ?
                )
           )",
    )
    .bind(&asset_id)
    .bind(&tenant_id)
    .bind(&tenant_id)
    .bind(&user_id)
    .bind(&user_id)
    .fetch_all(pool)
    .await?;
    Ok(strongest_resource_permission(rows))
}

async fn timescale_device_resource_permission(
    pool: &PgPool,
    subject: &AuthorizationSubject,
    device_id: &str,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    let owner = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(subject.tenant_id)
    .fetch_optional(pool)
    .await?;
    let Some(owner) = owner else {
        return Ok(None);
    };
    if owner == Some(subject.user_id) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT device.asset_id, 0
            FROM devices AS device
            WHERE device.device_id = $1
              AND device.tenant_id = $2
              AND device.deleted_at IS NULL
              AND device.asset_id IS NOT NULL
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = $2
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT permission.permission
         FROM resource_permissions AS permission
         WHERE permission.tenant_id = $2
           AND permission.revoked_at IS NULL
           AND (
                (permission.device_id = $1 AND (
                    permission.subject_user_id = $3
                    OR EXISTS (
                        SELECT 1
                        FROM user_group_members AS membership
                        WHERE membership.tenant_id = permission.tenant_id
                          AND membership.group_id = permission.subject_group_id
                          AND membership.user_id = $3
                    )
                ))
                OR (permission.asset_id IN (SELECT id FROM ancestors)
                    AND permission.inherit_children = TRUE AND (
                        permission.subject_user_id = $3
                        OR EXISTS (
                            SELECT 1
                            FROM user_group_members AS membership
                            WHERE membership.tenant_id = permission.tenant_id
                              AND membership.group_id = permission.subject_group_id
                              AND membership.user_id = $3
                        )
                    ))
           )",
    )
    .bind(device_id)
    .bind(subject.tenant_id)
    .bind(subject.user_id)
    .fetch_all(pool)
    .await?;
    Ok(strongest_resource_permission(rows))
}

async fn timescale_asset_resource_permission(
    pool: &PgPool,
    subject: &AuthorizationSubject,
    asset_id: uuid::Uuid,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    let owner = sqlx::query_scalar::<_, Option<uuid::Uuid>>(
        "SELECT owner_user_id FROM assets WHERE id = $1 AND tenant_id = $2",
    )
    .bind(asset_id)
    .bind(subject.tenant_id)
    .fetch_optional(pool)
    .await?;
    let Some(owner) = owner else {
        return Ok(None);
    };
    if owner == Some(subject.user_id) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT $1::uuid, 0
            UNION ALL
            SELECT asset.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets AS asset
              ON asset.id = ancestors.id AND asset.tenant_id = $2
            WHERE asset.parent_asset_id IS NOT NULL AND ancestors.depth < 64
         )
         SELECT permission.permission
         FROM resource_permissions AS permission
         JOIN ancestors
           ON permission.asset_id = ancestors.id AND permission.tenant_id = $2
         WHERE permission.revoked_at IS NULL
           AND (ancestors.depth = 0 OR permission.inherit_children = TRUE)
           AND (
                permission.subject_user_id = $3
                OR EXISTS (
                    SELECT 1
                    FROM user_group_members AS membership
                    WHERE membership.tenant_id = permission.tenant_id
                      AND membership.group_id = permission.subject_group_id
                      AND membership.user_id = $3
                )
           )",
    )
    .bind(asset_id)
    .bind(subject.tenant_id)
    .bind(subject.user_id)
    .fetch_all(pool)
    .await?;
    Ok(strongest_resource_permission(rows))
}

fn invitation_permission(value: &str) -> Result<ResourcePermission, TenantAuthorizationError> {
    match ResourcePermission::parse(value) {
        Some(ResourcePermission::Viewer) => Ok(ResourcePermission::Viewer),
        Some(ResourcePermission::Manager) => Ok(ResourcePermission::Manager),
        _ => Err(TenantAuthorizationError::InvalidStoredRecord),
    }
}

fn invitation_target_from_sqlite(
    asset_id: Option<&str>,
    device_id: Option<&str>,
) -> Result<OwnershipTransferTarget, TenantAuthorizationError> {
    match (asset_id, device_id) {
        (Some(asset_id), None) => uuid::Uuid::parse_str(asset_id)
            .map(OwnershipTransferTarget::Asset)
            .map_err(|_| TenantAuthorizationError::InvalidStoredRecord),
        (None, Some(device_id)) if !device_id.is_empty() => {
            Ok(OwnershipTransferTarget::Device(device_id.to_owned()))
        }
        _ => Err(TenantAuthorizationError::InvalidStoredRecord),
    }
}

fn invitation_target_from_timescale(
    asset_id: Option<uuid::Uuid>,
    device_id: Option<&str>,
) -> Result<OwnershipTransferTarget, TenantAuthorizationError> {
    match (asset_id, device_id) {
        (Some(asset_id), None) => Ok(OwnershipTransferTarget::Asset(asset_id)),
        (None, Some(device_id)) if !device_id.is_empty() => {
            Ok(OwnershipTransferTarget::Device(device_id.to_owned()))
        }
        _ => Err(TenantAuthorizationError::InvalidStoredRecord),
    }
}

fn tenant_actor_storage_fields(actor: TenantActor) -> (&'static str, uuid::Uuid) {
    match actor {
        TenantActor::TenantUser(id) => ("tenant_user", id),
        TenantActor::TenantAccount(id) => ("tenant_account", id),
    }
}

fn tenant_actor_from_storage(
    kind: &str,
    id: &str,
) -> Result<TenantActor, TenantAuthorizationError> {
    let id =
        uuid::Uuid::parse_str(id).map_err(|_| TenantAuthorizationError::InvalidStoredRecord)?;
    tenant_actor_from_postgres_storage(kind, id)
}

fn tenant_actor_from_postgres_storage(
    kind: &str,
    id: uuid::Uuid,
) -> Result<TenantActor, TenantAuthorizationError> {
    match kind {
        "tenant_user" => Ok(TenantActor::TenantUser(id)),
        "tenant_account" => Ok(TenantActor::TenantAccount(id)),
        _ => Err(TenantAuthorizationError::InvalidStoredRecord),
    }
}

fn invitation_permission_creator_ids(
    actor: TenantActor,
) -> (Option<uuid::Uuid>, Option<uuid::Uuid>) {
    match actor {
        TenantActor::TenantUser(id) => (Some(id), None),
        TenantActor::TenantAccount(id) => (None, Some(id)),
    }
}

fn resource_permission_scope(
    target: &OwnershipTransferTarget,
) -> (Option<uuid::Uuid>, Option<String>) {
    match target {
        OwnershipTransferTarget::Asset(asset_id) => (Some(*asset_id), None),
        OwnershipTransferTarget::Device(device_id) => (None, Some(device_id.clone())),
    }
}

fn resource_permission_scope_matches(
    target: &OwnershipTransferTarget,
    asset_id: Option<&str>,
    device_id: Option<&str>,
) -> bool {
    match target {
        OwnershipTransferTarget::Asset(expected_asset_id) => {
            asset_id.and_then(|value| value.parse::<uuid::Uuid>().ok()) == Some(*expected_asset_id)
                && device_id.is_none()
        }
        OwnershipTransferTarget::Device(expected_device_id) => {
            asset_id.is_none() && device_id == Some(expected_device_id)
        }
    }
}

fn resource_permission_target_json(target: &OwnershipTransferTarget) -> serde_json::Value {
    match target {
        OwnershipTransferTarget::Asset(asset_id) => {
            serde_json::json!({"kind": "asset", "id": asset_id.to_string()})
        }
        OwnershipTransferTarget::Device(device_id) => {
            serde_json::json!({"kind": "device", "id": device_id})
        }
    }
}

fn validate_new_resource_permission(
    permission: &NewResourcePermission,
) -> Result<(), TenantAuthorizationError> {
    if permission.subject_user_id.is_some() == permission.subject_group_id.is_some() {
        return Err(TenantAuthorizationError::InvalidPermissionSubject);
    }
    if permission.asset_id.is_some() == permission.device_id.is_some() {
        return Err(TenantAuthorizationError::InvalidPermissionResource);
    }
    if !matches!(
        permission.permission,
        ResourcePermission::Viewer | ResourcePermission::Manager
    ) {
        return Err(TenantAuthorizationError::InvalidPermissionLevel {
            permission: permission.permission,
        });
    }
    if permission.device_id.is_some() && permission.inherit_children {
        return Err(TenantAuthorizationError::DevicePermissionCannotInherit);
    }
    Ok(())
}
fn permission_creator_ids(creator: PermissionCreator) -> (Option<uuid::Uuid>, Option<uuid::Uuid>) {
    match creator {
        PermissionCreator::User(user_id) => (Some(user_id), None),
        PermissionCreator::TenantAccount(tenant_account_id) => (None, Some(tenant_account_id)),
    }
}

fn audit_principal_for_permission_creator(creator: PermissionCreator) -> AuditPrincipal {
    match creator {
        PermissionCreator::User(user_id) => AuditPrincipal::User(user_id),
        PermissionCreator::TenantAccount(tenant_account_id) => {
            AuditPrincipal::TenantAccount(tenant_account_id)
        }
    }
}

fn permission_creator(
    created_by_user_id: Option<uuid::Uuid>,
    created_by_tenant_account_id: Option<uuid::Uuid>,
) -> Result<PermissionCreator, TenantAuthorizationError> {
    match (created_by_user_id, created_by_tenant_account_id) {
        (Some(user_id), None) => Ok(PermissionCreator::User(user_id)),
        (None, Some(tenant_account_id)) => Ok(PermissionCreator::TenantAccount(tenant_account_id)),
        _ => Err(TenantAuthorizationError::InvalidStoredRecord),
    }
}

fn tenant_authorization_uuid(value: String) -> Result<uuid::Uuid, TenantAuthorizationError> {
    uuid::Uuid::parse_str(&value).map_err(|_| TenantAuthorizationError::InvalidStoredRecord)
}

fn tenant_authorization_optional_uuid(
    value: Option<String>,
) -> Result<Option<uuid::Uuid>, TenantAuthorizationError> {
    value.map(tenant_authorization_uuid).transpose()
}

fn sqlite_permission_inheritance(value: i64) -> Result<bool, TenantAuthorizationError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(TenantAuthorizationError::InvalidStoredRecord),
    }
}

#[allow(clippy::too_many_arguments)]
fn resource_permission_record(
    tenant_id: uuid::Uuid,
    id: uuid::Uuid,
    subject_user_id: Option<uuid::Uuid>,
    subject_group_id: Option<uuid::Uuid>,
    asset_id: Option<uuid::Uuid>,
    device_id: Option<String>,
    permission: String,
    inherit_children: bool,
    created_by_user_id: Option<uuid::Uuid>,
    created_by_tenant_account_id: Option<uuid::Uuid>,
) -> Result<ResourcePermissionRecord, TenantAuthorizationError> {
    let permission = ResourcePermission::parse(&permission)
        .ok_or(TenantAuthorizationError::InvalidStoredRecord)?;
    let created_by = permission_creator(created_by_user_id, created_by_tenant_account_id)?;
    let record = ResourcePermissionRecord {
        id,
        tenant_id,
        subject_user_id,
        subject_group_id,
        asset_id,
        device_id,
        permission,
        inherit_children,
        created_by,
    };
    validate_new_resource_permission(&NewResourcePermission {
        tenant_id: record.tenant_id,
        subject_user_id: record.subject_user_id,
        subject_group_id: record.subject_group_id,
        asset_id: record.asset_id,
        device_id: record.device_id.clone(),
        permission: record.permission,
        inherit_children: record.inherit_children,
        created_by: record.created_by,
    })?;
    Ok(record)
}

async fn sqlite_tenant_uuid_record_exists(
    transaction: &mut Transaction<'_, Sqlite>,
    query: &'static str,
    tenant_id: uuid::Uuid,
    id: uuid::Uuid,
) -> Result<bool, TenantAuthorizationError> {
    Ok(sqlx::query_scalar::<_, i32>(query)
        .bind(id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some())
}

async fn sqlite_require_tenant_user(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    user_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if sqlite_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM users WHERE id = ? AND tenant_id = ?",
        tenant_id,
        user_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::UserNotFound { tenant_id, user_id })
    }
}

async fn sqlite_require_regular_tenant_user(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    user_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if sqlite_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM users WHERE id = ? AND tenant_id = ? AND account_class = 'user'",
        tenant_id,
        user_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::OwnerMustBeRegularUser { tenant_id, user_id })
    }
}

async fn sqlite_require_resource_owner(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    target: &OwnershipTransferTarget,
    user_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    let owner_user_id = match target {
        OwnershipTransferTarget::Asset(asset_id) => sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_user_id FROM assets WHERE id = ? AND tenant_id = ?",
        )
        .bind(asset_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(TenantAuthorizationError::AssetNotFound {
            tenant_id,
            asset_id: *asset_id,
        })?,
        OwnershipTransferTarget::Device(device_id) => sqlx::query_scalar::<_, Option<String>>(
            "SELECT owner_user_id FROM devices
             WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
        )
        .bind(device_id)
        .bind(tenant_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| TenantAuthorizationError::DeviceNotFound {
            tenant_id,
            device_id: device_id.clone(),
        })?,
    };
    if owner_user_id.as_deref() == Some(user_id.to_string().as_str()) {
        Ok(())
    } else {
        Err(TenantAuthorizationError::ResourceOwnerRequired { tenant_id, user_id })
    }
}

async fn sqlite_require_tenant_account(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    tenant_account_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if sqlite_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM tenant_accounts WHERE id = ? AND tenant_id = ?",
        tenant_id,
        tenant_account_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::TenantAccountNotFound {
            tenant_id,
            tenant_account_id,
        })
    }
}

async fn sqlite_require_resource_invitation_sender(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    target: &OwnershipTransferTarget,
    sender: TenantActor,
) -> Result<(), TenantAuthorizationError> {
    match sender {
        TenantActor::TenantUser(user_id) => {
            sqlite_require_regular_tenant_user(transaction, tenant_id, user_id).await?;
            sqlite_require_resource_owner(transaction, tenant_id, target, user_id).await
        }
        TenantActor::TenantAccount(tenant_account_id) => {
            sqlite_require_tenant_account(transaction, tenant_id, tenant_account_id).await?;
            match target {
                OwnershipTransferTarget::Asset(asset_id) => {
                    sqlite_require_tenant_asset(transaction, tenant_id, *asset_id).await
                }
                OwnershipTransferTarget::Device(device_id) => {
                    sqlite_require_tenant_device(transaction, tenant_id, device_id).await
                }
            }
        }
    }
}

async fn sqlite_require_tenant_audit_actor(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    actor: AuditPrincipal,
) -> Result<(), TenantAuthorizationError> {
    match actor {
        AuditPrincipal::User(user_id) => {
            sqlite_require_tenant_user(transaction, tenant_id, user_id).await
        }
        AuditPrincipal::TenantAccount(tenant_account_id) => {
            sqlite_require_tenant_account(transaction, tenant_id, tenant_account_id).await
        }
        AuditPrincipal::SystemAccount(_) => {
            Err(TenantAuthorizationError::SystemAccountCannotTransferOwnership)
        }
    }
}

async fn sqlite_require_tenant_group(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    group_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if sqlite_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM user_groups WHERE id = ? AND tenant_id = ?",
        tenant_id,
        group_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::GroupNotFound {
            tenant_id,
            group_id,
        })
    }
}

async fn sqlite_require_tenant_asset(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    asset_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if sqlite_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?",
        tenant_id,
        asset_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::AssetNotFound {
            tenant_id,
            asset_id,
        })
    }
}

async fn sqlite_require_tenant_permission(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    permission_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if sqlite_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM resource_permissions WHERE id = ? AND tenant_id = ?",
        tenant_id,
        permission_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::PermissionNotFound {
            tenant_id,
            permission_id,
        })
    }
}

async fn sqlite_require_tenant_device(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<(), TenantAuthorizationError> {
    let exists =
        sqlx::query_scalar::<_, i64>("SELECT 1 FROM devices WHERE device_id = ? AND tenant_id = ?")
            .bind(device_id)
            .bind(tenant_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .is_some();
    if exists {
        Ok(())
    } else {
        Err(TenantAuthorizationError::DeviceNotFound {
            tenant_id,
            device_id: device_id.to_owned(),
        })
    }
}

async fn sqlite_validate_permission_references(
    transaction: &mut Transaction<'_, Sqlite>,
    permission: &NewResourcePermission,
) -> Result<(), TenantAuthorizationError> {
    match permission.created_by {
        PermissionCreator::User(user_id) => {
            sqlite_require_tenant_user(transaction, permission.tenant_id, user_id).await?
        }
        PermissionCreator::TenantAccount(tenant_account_id) => {
            sqlite_require_tenant_account(transaction, permission.tenant_id, tenant_account_id)
                .await?
        }
    }
    if let Some(user_id) = permission.subject_user_id {
        sqlite_require_tenant_user(transaction, permission.tenant_id, user_id).await?;
    }
    if let Some(group_id) = permission.subject_group_id {
        sqlite_require_tenant_group(transaction, permission.tenant_id, group_id).await?;
    }
    if let Some(asset_id) = permission.asset_id {
        sqlite_require_tenant_asset(transaction, permission.tenant_id, asset_id).await?;
    }
    if let Some(device_id) = permission.device_id.as_deref() {
        sqlite_require_tenant_device(transaction, permission.tenant_id, device_id).await?;
    }
    Ok(())
}

async fn timescale_tenant_uuid_record_exists(
    transaction: &mut Transaction<'_, Postgres>,
    query: &'static str,
    tenant_id: uuid::Uuid,
    id: uuid::Uuid,
) -> Result<bool, TenantAuthorizationError> {
    Ok(sqlx::query_scalar::<_, i32>(query)
        .bind(id)
        .bind(tenant_id)
        .fetch_optional(&mut **transaction)
        .await?
        .is_some())
}

async fn timescale_require_tenant_user(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    user_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if timescale_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2",
        tenant_id,
        user_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::UserNotFound { tenant_id, user_id })
    }
}

async fn timescale_require_regular_tenant_user(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    user_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if timescale_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2 AND account_class = 'user'",
        tenant_id,
        user_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::OwnerMustBeRegularUser { tenant_id, user_id })
    }
}

async fn timescale_require_resource_owner(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    target: &OwnershipTransferTarget,
    user_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    let owner_user_id = match target {
        OwnershipTransferTarget::Asset(asset_id) => sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "SELECT owner_user_id FROM assets
             WHERE id = $1 AND tenant_id = $2
             FOR UPDATE",
        )
        .bind(*asset_id)
        .bind(tenant_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(TenantAuthorizationError::AssetNotFound {
            tenant_id,
            asset_id: *asset_id,
        })?,
        OwnershipTransferTarget::Device(device_id) => sqlx::query_scalar::<_, Option<uuid::Uuid>>(
            "SELECT owner_user_id FROM devices
             WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
             FOR UPDATE",
        )
        .bind(device_id)
        .bind(tenant_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| TenantAuthorizationError::DeviceNotFound {
            tenant_id,
            device_id: device_id.clone(),
        })?,
    };
    if owner_user_id == Some(user_id) {
        Ok(())
    } else {
        Err(TenantAuthorizationError::ResourceOwnerRequired { tenant_id, user_id })
    }
}

async fn timescale_require_tenant_account(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    tenant_account_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if timescale_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM tenant_accounts WHERE id = $1 AND tenant_id = $2",
        tenant_id,
        tenant_account_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::TenantAccountNotFound {
            tenant_id,
            tenant_account_id,
        })
    }
}

async fn timescale_require_resource_invitation_sender(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    target: &OwnershipTransferTarget,
    sender: TenantActor,
) -> Result<(), TenantAuthorizationError> {
    match sender {
        TenantActor::TenantUser(user_id) => {
            timescale_require_regular_tenant_user(transaction, tenant_id, user_id).await?;
            timescale_require_resource_owner(transaction, tenant_id, target, user_id).await
        }
        TenantActor::TenantAccount(tenant_account_id) => {
            timescale_require_tenant_account(transaction, tenant_id, tenant_account_id).await?;
            match target {
                OwnershipTransferTarget::Asset(asset_id) => {
                    timescale_require_tenant_asset(transaction, tenant_id, *asset_id).await
                }
                OwnershipTransferTarget::Device(device_id) => {
                    timescale_require_tenant_device(transaction, tenant_id, device_id).await
                }
            }
        }
    }
}

async fn timescale_require_tenant_audit_actor(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    actor: AuditPrincipal,
) -> Result<(), TenantAuthorizationError> {
    match actor {
        AuditPrincipal::User(user_id) => {
            timescale_require_tenant_user(transaction, tenant_id, user_id).await
        }
        AuditPrincipal::TenantAccount(tenant_account_id) => {
            timescale_require_tenant_account(transaction, tenant_id, tenant_account_id).await
        }
        AuditPrincipal::SystemAccount(_) => {
            Err(TenantAuthorizationError::SystemAccountCannotTransferOwnership)
        }
    }
}

async fn timescale_require_tenant_group(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    group_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if timescale_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM user_groups WHERE id = $1 AND tenant_id = $2",
        tenant_id,
        group_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::GroupNotFound {
            tenant_id,
            group_id,
        })
    }
}

async fn timescale_require_tenant_asset(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    asset_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if timescale_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2",
        tenant_id,
        asset_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::AssetNotFound {
            tenant_id,
            asset_id,
        })
    }
}

async fn timescale_require_tenant_permission(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    permission_id: uuid::Uuid,
) -> Result<(), TenantAuthorizationError> {
    if timescale_tenant_uuid_record_exists(
        transaction,
        "SELECT 1 FROM resource_permissions WHERE id = $1 AND tenant_id = $2",
        tenant_id,
        permission_id,
    )
    .await?
    {
        Ok(())
    } else {
        Err(TenantAuthorizationError::PermissionNotFound {
            tenant_id,
            permission_id,
        })
    }
}

async fn timescale_require_tenant_device(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<(), TenantAuthorizationError> {
    let exists = sqlx::query_scalar::<_, i32>(
        "SELECT 1 FROM devices WHERE device_id = $1 AND tenant_id = $2",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(TenantAuthorizationError::DeviceNotFound {
            tenant_id,
            device_id: device_id.to_owned(),
        })
    }
}

async fn timescale_validate_permission_references(
    transaction: &mut Transaction<'_, Postgres>,
    permission: &NewResourcePermission,
) -> Result<(), TenantAuthorizationError> {
    match permission.created_by {
        PermissionCreator::User(user_id) => {
            timescale_require_tenant_user(transaction, permission.tenant_id, user_id).await?
        }
        PermissionCreator::TenantAccount(tenant_account_id) => {
            timescale_require_tenant_account(transaction, permission.tenant_id, tenant_account_id)
                .await?
        }
    }
    if let Some(user_id) = permission.subject_user_id {
        timescale_require_tenant_user(transaction, permission.tenant_id, user_id).await?;
    }
    if let Some(group_id) = permission.subject_group_id {
        timescale_require_tenant_group(transaction, permission.tenant_id, group_id).await?;
    }
    if let Some(asset_id) = permission.asset_id {
        timescale_require_tenant_asset(transaction, permission.tenant_id, asset_id).await?;
    }
    if let Some(device_id) = permission.device_id.as_deref() {
        timescale_require_tenant_device(transaction, permission.tenant_id, device_id).await?;
    }
    Ok(())
}
