use sqlx::{PgPool, SqlitePool};
use uuid::Uuid;

use crate::auth::{AccountClass, AuthContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Asset,
    Device,
}

impl ResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asset => "asset",
            Self::Device => "device",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResourcePermission {
    Viewer,
    Controller,
    Manager,
    Owner,
}

impl ResourcePermission {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Controller => "controller",
            Self::Manager => "manager",
            Self::Owner => "owner",
        }
    }

    pub fn parse_share(value: &str) -> Option<Self> {
        match value {
            "viewer" => Some(Self::Viewer),
            "controller" => Some(Self::Controller),
            "manager" => Some(Self::Manager),
            _ => None,
        }
    }

    pub const fn allows(self, required: Self) -> bool {
        self as u8 >= required as u8
    }
}

pub async fn device_permission(
    pool: &PgPool,
    context: &AuthContext,
    device_id: &str,
) -> Result<Option<ResourcePermission>, sqlx::Error> {
    if let Some(permission) = privileged_permission(context) {
        return Ok(Some(permission));
    }

    let owner = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = $1 AND deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(pool)
    .await?
    .flatten();
    if owner == Some(context.user_id) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT asset_id, 0
            FROM devices
            WHERE device_id = $1
              AND deleted_at IS NULL
              AND asset_id IS NOT NULL
            UNION ALL
            SELECT assets.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets ON assets.id = ancestors.id
            WHERE assets.parent_asset_id IS NOT NULL
              AND ancestors.depth < 64
         )
         SELECT permission
         FROM resource_shares
         WHERE resource_type = 'device'
           AND resource_id = $1
           AND target_user_id = $2
           AND state = 'active'
         UNION ALL
         SELECT shares.permission
         FROM resource_shares AS shares
         JOIN ancestors ON shares.resource_id = ancestors.id::text
         WHERE shares.resource_type = 'asset'
           AND shares.target_user_id = $2
           AND shares.state = 'active'
           AND shares.inherit_children = TRUE",
    )
    .bind(device_id)
    .bind(context.user_id)
    .fetch_all(pool)
    .await?;
    Ok(strongest_share_permission(rows))
}

pub async fn asset_permission(
    pool: &PgPool,
    context: &AuthContext,
    asset_id: Uuid,
) -> Result<Option<ResourcePermission>, sqlx::Error> {
    if let Some(permission) = privileged_permission(context) {
        return Ok(Some(permission));
    }

    let owner = sqlx::query_scalar::<_, Option<Uuid>>(
        "SELECT owner_user_id
         FROM assets
         WHERE id = $1",
    )
    .bind(asset_id)
    .fetch_optional(pool)
    .await?
    .flatten();
    if owner == Some(context.user_id) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT $1::uuid, 0
            UNION ALL
            SELECT assets.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets ON assets.id = ancestors.id
            WHERE assets.parent_asset_id IS NOT NULL
              AND ancestors.depth < 64
         )
         SELECT permission
         FROM resource_shares
         WHERE resource_type = 'asset'
           AND resource_id = $1
           AND target_user_id = $2
           AND state = 'active'
         UNION ALL
         SELECT shares.permission
         FROM resource_shares AS shares
         JOIN ancestors ON shares.resource_id = ancestors.id::text
         WHERE shares.resource_type = 'asset'
           AND shares.target_user_id = $2
           AND shares.state = 'active'
           AND shares.inherit_children = TRUE",
    )
    .bind(asset_id.to_string())
    .bind(context.user_id)
    .fetch_all(pool)
    .await?;
    Ok(strongest_share_permission(rows))
}

pub async fn sqlite_device_permission(
    pool: &SqlitePool,
    context: &AuthContext,
    device_id: &str,
) -> Result<Option<ResourcePermission>, sqlx::Error> {
    if let Some(permission) = privileged_permission(context) {
        return Ok(Some(permission));
    }

    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM devices
         WHERE device_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .fetch_optional(pool)
    .await?
    .flatten();
    if owner.as_deref() == Some(&context.user_id.to_string()) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT asset_id, 0
            FROM devices
            WHERE device_id = ?
              AND deleted_at IS NULL
              AND asset_id IS NOT NULL
            UNION ALL
            SELECT assets.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets ON assets.id = ancestors.id
            WHERE assets.parent_asset_id IS NOT NULL
              AND ancestors.depth < 64
         )
         SELECT permission
         FROM resource_shares
         WHERE resource_type = 'device'
           AND resource_id = ?
           AND target_user_id = ?
           AND state = 'active'
         UNION ALL
         SELECT shares.permission
         FROM resource_shares AS shares
         JOIN ancestors ON shares.resource_id = ancestors.id
         WHERE shares.resource_type = 'asset'
           AND shares.target_user_id = ?
           AND shares.state = 'active'
           AND shares.inherit_children = 1",
    )
    .bind(device_id)
    .bind(device_id)
    .bind(context.user_id.to_string())
    .bind(context.user_id.to_string())
    .fetch_all(pool)
    .await?;
    Ok(strongest_share_permission(rows))
}

pub async fn sqlite_asset_permission(
    pool: &SqlitePool,
    context: &AuthContext,
    asset_id: Uuid,
) -> Result<Option<ResourcePermission>, sqlx::Error> {
    if let Some(permission) = privileged_permission(context) {
        return Ok(Some(permission));
    }

    let asset_id = asset_id.to_string();
    let owner = sqlx::query_scalar::<_, Option<String>>(
        "SELECT owner_user_id
         FROM assets
         WHERE id = ?",
    )
    .bind(&asset_id)
    .fetch_optional(pool)
    .await?
    .flatten();
    if owner.as_deref() == Some(&context.user_id.to_string()) {
        return Ok(Some(ResourcePermission::Owner));
    }

    let rows = sqlx::query_scalar::<_, String>(
        "WITH RECURSIVE ancestors(id, depth) AS (
            SELECT ?, 0
            UNION ALL
            SELECT assets.parent_asset_id, ancestors.depth + 1
            FROM ancestors
            JOIN assets ON assets.id = ancestors.id
            WHERE assets.parent_asset_id IS NOT NULL
              AND ancestors.depth < 64
         )
         SELECT permission
         FROM resource_shares
         WHERE resource_type = 'asset'
           AND resource_id = ?
           AND target_user_id = ?
           AND state = 'active'
         UNION ALL
         SELECT shares.permission
         FROM resource_shares AS shares
         JOIN ancestors ON shares.resource_id = ancestors.id
         WHERE shares.resource_type = 'asset'
           AND shares.target_user_id = ?
           AND shares.state = 'active'
           AND shares.inherit_children = 1",
    )
    .bind(&asset_id)
    .bind(&asset_id)
    .bind(context.user_id.to_string())
    .bind(context.user_id.to_string())
    .fetch_all(pool)
    .await?;
    Ok(strongest_share_permission(rows))
}

fn privileged_permission(context: &AuthContext) -> Option<ResourcePermission> {
    match context.account_class {
        AccountClass::Admin => Some(ResourcePermission::Owner),
        AccountClass::System | AccountClass::User => None,
    }
}

fn strongest_share_permission(rows: Vec<String>) -> Option<ResourcePermission> {
    rows.into_iter()
        .filter_map(|value| ResourcePermission::parse_share(&value))
        .max()
}
