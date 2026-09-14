use super::*;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicPrincipal {
    pub user_id: Option<Uuid>,
    pub app_id: String,
    pub account_class: AccountClass,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicAsset {
    pub id: Uuid,
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewPublicAsset {
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicTelemetry {
    pub event_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub device_id: String,
    pub boot_id: String,
    pub sequence: i64,
    pub measurements: serde_json::Value,
    pub topic: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublicAlert {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub rule_name: String,
    pub severity: String,
    pub device_id: String,
    pub status: String,
    pub condition_started_at: DateTime<Utc>,
    pub opened_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub last_value: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicResourceGrant {
    pub id: Uuid,
    pub resource_type: String,
    pub resource_id: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub permission: String,
    pub created_by_user_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPublicResourceGrant {
    pub resource_type: String,
    pub resource_id: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub permission: String,
}

pub trait PublicApiRepository: Send + Sync {
    fn public_device_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
    fn public_asset_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    >;
    fn list_public_assets<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAsset>, PlatformStoreError>> + Send + 'a>>;
    fn get_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PlatformStoreError>> + Send + 'a>>;
    fn create_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<PublicAsset, PlatformStoreError>> + Send + 'a>>;
    fn update_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PlatformStoreError>> + Send + 'a>>;
    fn delete_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
    fn list_public_telemetry<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: Option<&'a str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicTelemetry>, PlatformStoreError>> + Send + 'a>>;
    fn list_public_alerts<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAlert>, PlatformStoreError>> + Send + 'a>>;
    fn get_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>;
    fn acknowledge_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
        actor: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>;
    fn list_public_grants<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<PublicResourceGrant>, PlatformStoreError>> + Send + 'a>,
    >;
    fn get_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<PublicResourceGrant>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn create_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant: NewPublicResourceGrant,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<PublicResourceGrant>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn update_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant_id: Uuid,
        grant: NewPublicResourceGrant,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<PublicResourceGrant>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn delete_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>>;
}

impl PublicApiRepository for PlatformStore {
    fn public_device_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { public_device_permission(self, principal, device_id).await })
    }

    fn public_asset_permission<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<ResourcePermission>, PlatformStoreError>> + Send + 'a,
        >,
    > {
        Box::pin(async move { public_asset_permission(self, principal, asset_id).await })
    }

    fn list_public_assets<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAsset>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { list_public_assets(self, principal, after, limit).await })
    }

    fn get_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            if public_asset_permission(self, principal, asset_id)
                .await?
                .is_none()
            {
                return Ok(None);
            }
            get_public_asset(self, asset_id).await
        })
    }

    fn create_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<PublicAsset, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { create_public_asset(self, principal, asset).await })
    }

    fn update_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
        asset: NewPublicAsset,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAsset>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { update_public_asset(self, principal, asset_id, asset).await })
    }

    fn delete_public_asset<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        asset_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { delete_public_asset(self, principal, asset_id).await })
    }

    fn list_public_telemetry<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        device_id: Option<&'a str>,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicTelemetry>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            list_public_telemetry(self, principal, device_id, from, to, after, limit).await
        })
    }

    fn list_public_alerts<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<PublicAlert>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { list_public_alerts(self, principal, after, limit).await })
    }

    fn get_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { get_public_alert(self, principal, alert_id).await })
    }

    fn acknowledge_public_alert<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        alert_id: Uuid,
        actor: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<PublicAlert>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { acknowledge_public_alert(self, principal, alert_id, actor).await })
    }

    fn list_public_grants<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        after: Option<&'a str>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<PublicResourceGrant>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { list_public_grants(self, principal, after, limit).await })
    }

    fn get_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<PublicResourceGrant>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { get_public_grant(self, principal, grant_id).await })
    }

    fn create_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant: NewPublicResourceGrant,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<PublicResourceGrant>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { create_public_grant(self, principal, grant).await })
    }

    fn update_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant_id: Uuid,
        grant: NewPublicResourceGrant,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<PublicResourceGrant>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { update_public_grant(self, principal, grant_id, grant).await })
    }

    fn delete_public_grant<'a>(
        &'a self,
        principal: &'a PublicPrincipal,
        grant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<bool, PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { delete_public_grant(self, principal, grant_id).await })
    }
}

async fn public_device_permission(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: &str,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    if principal.account_class == AccountClass::Admin {
        return Ok(Some(ResourcePermission::Owner));
    }
    match store {
        PlatformStore::Sqlite(store) => {
            let owner = sqlx::query_scalar::<_, Option<String>>(
                "SELECT owner_user_id FROM devices
                 WHERE device_id = ? AND deleted_at IS NULL",
            )
            .bind(device_id)
            .fetch_optional(store.pool())
            .await?
            .flatten();
            if principal.user_id.map(|id| id.to_string()) == owner {
                return Ok(Some(ResourcePermission::Owner));
            }
            let user_id = principal.user_id.map(|id| id.to_string());
            let rows = sqlx::query_scalar::<_, String>(
                "SELECT permission FROM resource_shares
                 WHERE resource_type = 'device' AND resource_id = ?
                   AND target_user_id = ? AND state = 'active'
                 UNION ALL
                 SELECT permission FROM resource_grants
                 WHERE resource_type = 'device' AND resource_id = ?
                   AND ((grantee_type = 'user' AND grantee_id = ?)
                        OR (grantee_type = 'application' AND grantee_id = ?))",
            )
            .bind(device_id)
            .bind(user_id.clone())
            .bind(device_id)
            .bind(user_id)
            .bind(&principal.app_id)
            .fetch_all(store.pool())
            .await?;
            Ok(strongest_share_permission(rows))
        }
        PlatformStore::Timescale(pool) => {
            let owner = sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT owner_user_id FROM devices
                 WHERE device_id = $1 AND deleted_at IS NULL",
            )
            .bind(device_id)
            .fetch_optional(pool)
            .await?
            .flatten();
            if principal.user_id == owner {
                return Ok(Some(ResourcePermission::Owner));
            }
            let rows = sqlx::query_scalar::<_, String>(
                "SELECT permission FROM resource_shares
                 WHERE resource_type = 'device' AND resource_id = $1
                   AND target_user_id = $2 AND state = 'active'
                 UNION ALL
                 SELECT permission FROM resource_grants
                 WHERE resource_type = 'device' AND resource_id = $1
                   AND ((grantee_type = 'user' AND grantee_id = $2::text)
                        OR (grantee_type = 'application' AND grantee_id = $3))",
            )
            .bind(device_id)
            .bind(principal.user_id)
            .bind(&principal.app_id)
            .fetch_all(pool)
            .await?;
            Ok(strongest_share_permission(rows))
        }
    }
}

fn public_cursor_parts(
    after: Option<&str>,
) -> Result<Option<(DateTime<Utc>, String, i64)>, PlatformStoreError> {
    let Some(after) = after else {
        return Ok(None);
    };
    let mut parts = after.splitn(3, '|');
    let event_at = parts
        .next()
        .ok_or_else(|| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?
        .parse::<DateTime<Utc>>()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?;
    let device_id = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?
        .to_owned();
    let sequence = parts
        .next()
        .ok_or_else(|| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?
        .parse::<i64>()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid public cursor".to_owned()))
        })?;
    Ok(Some((event_at, device_id, sequence)))
}

async fn list_public_telemetry(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    device_id: Option<&str>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicTelemetry>, PlatformStoreError> {
    let cursor = public_cursor_parts(after)?;
    let limit = i64::from(limit);
    match store {
        PlatformStore::Sqlite(store) => {
            let user_id = principal.user_id.map(|id| id.to_string());
            let cursor_at = cursor.as_ref().map(|value| value.0.to_rfc3339());
            let cursor_device = cursor.as_ref().map(|value| value.1.clone());
            let cursor_sequence = cursor.as_ref().map(|value| value.2);
            let rows = sqlx::query(
                "SELECT t.event_at, t.received_at, t.device_id, t.boot_id,
                        t.sequence, t.measurements, t.topic
                 FROM telemetry AS t
                 JOIN devices AS d ON d.device_id = t.device_id
                 WHERE d.deleted_at IS NULL
                   AND t.event_at >= ? AND t.event_at <= ?
                   AND (? IS NULL OR t.device_id = ?)
                   AND (? = 1 OR d.owner_user_id = ?
                    OR EXISTS (
                        SELECT 1 FROM resource_shares
                        WHERE resource_type = 'device' AND resource_id = t.device_id
                          AND target_user_id = ? AND state = 'active'
                    )
                    OR EXISTS (
                        SELECT 1 FROM resource_grants
                        WHERE resource_type = 'device' AND resource_id = t.device_id
                          AND ((grantee_type = 'user' AND grantee_id = ?)
                               OR (grantee_type = 'application' AND grantee_id = ?))
                    ))
                   AND (? IS NULL OR t.event_at > ?
                        OR (t.event_at = ? AND (t.device_id > ?
                            OR (t.device_id = ? AND t.sequence > ?))))
                 ORDER BY t.event_at, t.device_id, t.sequence
                 LIMIT ?",
            )
            .bind(from.to_rfc3339())
            .bind(to.to_rfc3339())
            .bind(device_id)
            .bind(device_id)
            .bind(i64::from(principal.account_class == AccountClass::Admin))
            .bind(&user_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&principal.app_id)
            .bind(&cursor_at)
            .bind(&cursor_at)
            .bind(&cursor_at)
            .bind(&cursor_device)
            .bind(&cursor_device)
            .bind(cursor_sequence)
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_telemetry_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT t.event_at, t.received_at, t.device_id, t.boot_id,
                        t.sequence, t.measurements, t.topic
                 FROM telemetry AS t
                 JOIN devices AS d ON d.device_id = t.device_id
                 WHERE d.deleted_at IS NULL
                   AND t.event_at >= $1 AND t.event_at <= $2
                   AND ($3::text IS NULL OR t.device_id = $3)
                   AND ($4::boolean OR d.owner_user_id = $5
                    OR EXISTS (
                        SELECT 1 FROM resource_shares
                        WHERE resource_type = 'device' AND resource_id = t.device_id
                          AND target_user_id = $5 AND state = 'active'
                    )
                    OR EXISTS (
                        SELECT 1 FROM resource_grants
                        WHERE resource_type = 'device' AND resource_id = t.device_id
                          AND ((grantee_type = 'user' AND grantee_id = $5::text)
                               OR (grantee_type = 'application' AND grantee_id = $6))
                    ))
                   AND ($7::timestamptz IS NULL OR t.event_at > $7
                        OR (t.event_at = $7 AND (t.device_id > $8
                            OR (t.device_id = $8 AND t.sequence > $9))))
                 ORDER BY t.event_at, t.device_id, t.sequence
                 LIMIT $10",
            )
            .bind(from)
            .bind(to)
            .bind(device_id)
            .bind(principal.account_class == AccountClass::Admin)
            .bind(principal.user_id)
            .bind(&principal.app_id)
            .bind(cursor.as_ref().map(|value| value.0))
            .bind(cursor.as_ref().map(|value| value.1.clone()))
            .bind(cursor.as_ref().map(|value| value.2))
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter().map(timescale_telemetry_record).collect()
        }
    }
}

fn parse_public_timestamp(value: String) -> Result<DateTime<Utc>, PlatformStoreError> {
    if let Ok(value) = DateTime::parse_from_rfc3339(&value) {
        return Ok(value.with_timezone(&Utc));
    }
    NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S"))
        .map(|value| value.and_utc())
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public timestamp".to_owned(),
            ))
        })
}

fn sqlite_telemetry_record(row: SqliteRow) -> Result<PublicTelemetry, PlatformStoreError> {
    Ok(PublicTelemetry {
        event_at: parse_public_timestamp(row.try_get("event_at")?)?,
        received_at: parse_public_timestamp(row.try_get("received_at")?)?,
        device_id: row.try_get("device_id")?,
        boot_id: row.try_get("boot_id")?,
        sequence: row.try_get("sequence")?,
        measurements: serde_json::from_str(&row.try_get::<String, _>("measurements")?).map_err(
            |_| {
                PlatformStoreError::Database(sqlx::Error::Protocol(
                    "invalid public telemetry measurements".to_owned(),
                ))
            },
        )?,
        topic: row.try_get("topic")?,
    })
}

fn timescale_telemetry_record(row: PgRow) -> Result<PublicTelemetry, PlatformStoreError> {
    Ok(PublicTelemetry {
        event_at: row.try_get("event_at")?,
        received_at: row.try_get("received_at")?,
        device_id: row.try_get("device_id")?,
        boot_id: row.try_get::<Uuid, _>("boot_id")?.to_string(),
        sequence: row.try_get("sequence")?,
        measurements: row.try_get::<Json<serde_json::Value>, _>("measurements")?.0,
        topic: row.try_get("topic")?,
    })
}

async fn list_public_alerts(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicAlert>, PlatformStoreError> {
    let limit = i64::from(limit);
    match store {
        PlatformStore::Sqlite(store) => {
            let user_id = principal.user_id.map(|id| id.to_string());
            let rows = sqlx::query(
                "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.condition_started_at,
                        incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                        incidents.acknowledged_by, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules ON rules.id = incidents.rule_id
                 JOIN devices AS devices ON devices.device_id = incidents.device_id
                 WHERE devices.deleted_at IS NULL
                   AND (? = 1 OR devices.owner_user_id = ?
                    OR EXISTS (
                        SELECT 1 FROM resource_shares
                        WHERE resource_type = 'device' AND resource_id = incidents.device_id
                          AND target_user_id = ? AND state = 'active'
                    )
                    OR EXISTS (
                        SELECT 1 FROM resource_grants
                        WHERE resource_type = 'device' AND resource_id = incidents.device_id
                          AND ((grantee_type = 'user' AND grantee_id = ?)
                               OR (grantee_type = 'application' AND grantee_id = ?))
                    ))
                   AND (? IS NULL OR incidents.id > ?)
                 ORDER BY incidents.id
                 LIMIT ?",
            )
            .bind(i64::from(principal.account_class == AccountClass::Admin))
            .bind(&user_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&principal.app_id)
            .bind(after)
            .bind(after)
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_alert_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.condition_started_at,
                        incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                        incidents.acknowledged_by, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules ON rules.id = incidents.rule_id
                 JOIN devices AS devices ON devices.device_id = incidents.device_id
                 WHERE devices.deleted_at IS NULL
                   AND ($1::boolean OR devices.owner_user_id = $2
                    OR EXISTS (
                        SELECT 1 FROM resource_shares
                        WHERE resource_type = 'device' AND resource_id = incidents.device_id
                          AND target_user_id = $2 AND state = 'active'
                    )
                    OR EXISTS (
                        SELECT 1 FROM resource_grants
                        WHERE resource_type = 'device' AND resource_id = incidents.device_id
                          AND ((grantee_type = 'user' AND grantee_id = $2::text)
                               OR (grantee_type = 'application' AND grantee_id = $3))
                    ))
                   AND ($4::uuid IS NULL OR incidents.id > $4)
                 ORDER BY incidents.id
                 LIMIT $5",
            )
            .bind(principal.account_class == AccountClass::Admin)
            .bind(principal.user_id)
            .bind(&principal.app_id)
            .bind(after.and_then(|value| Uuid::parse_str(value).ok()))
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter().map(timescale_alert_record).collect()
        }
    }
}

async fn get_public_alert(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    alert_id: Uuid,
) -> Result<Option<PublicAlert>, PlatformStoreError> {
    let alert = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                    incidents.device_id, incidents.status, incidents.condition_started_at,
                    incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                    incidents.acknowledged_by, incidents.last_value, incidents.updated_at
             FROM alert_incidents AS incidents
             JOIN alert_rules AS rules ON rules.id = incidents.rule_id
             WHERE incidents.id = ?",
        )
        .bind(alert_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_alert_record)
        .transpose()?,
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                    incidents.device_id, incidents.status, incidents.condition_started_at,
                    incidents.opened_at, incidents.resolved_at, incidents.acknowledged_at,
                    incidents.acknowledged_by, incidents.last_value, incidents.updated_at
             FROM alert_incidents AS incidents
             JOIN alert_rules AS rules ON rules.id = incidents.rule_id
             WHERE incidents.id = $1",
        )
        .bind(alert_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_alert_record)
        .transpose()?,
    };
    let Some(alert) = alert else {
        return Ok(None);
    };
    if public_device_permission(store, principal, &alert.device_id)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    Ok(Some(alert))
}

async fn acknowledge_public_alert(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    alert_id: Uuid,
    actor: &str,
) -> Result<Option<PublicAlert>, PlatformStoreError> {
    let Some(alert) = get_public_alert(store, principal, alert_id).await? else {
        return Ok(None);
    };
    if !public_device_permission(store, principal, &alert.device_id)
        .await?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Ok(None);
    }
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET acknowledged_at = ?, acknowledged_by = ?, updated_at = ?
                 WHERE id = ?",
            )
            .bind(Utc::now().to_rfc3339())
            .bind(actor)
            .bind(Utc::now().to_rfc3339())
            .bind(alert_id.to_string())
            .execute(store.pool())
            .await?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET acknowledged_at = now(), acknowledged_by = $2, updated_at = now()
                 WHERE id = $1",
            )
            .bind(alert_id)
            .bind(actor)
            .execute(pool)
            .await?;
        }
    }
    get_public_alert(store, principal, alert_id).await
}

fn sqlite_optional_timestamp(
    value: Option<String>,
) -> Result<Option<DateTime<Utc>>, PlatformStoreError> {
    value.map(parse_public_timestamp).transpose()
}

fn sqlite_alert_record(row: SqliteRow) -> Result<PublicAlert, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let rule_id: String = row.try_get("rule_id")?;
    Ok(PublicAlert {
        id: Uuid::parse_str(&id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public alert ID".to_owned(),
            ))
        })?,
        rule_id: Uuid::parse_str(&rule_id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public alert rule ID".to_owned(),
            ))
        })?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        condition_started_at: parse_public_timestamp(row.try_get("condition_started_at")?)?,
        opened_at: sqlite_optional_timestamp(row.try_get("opened_at")?)?,
        resolved_at: sqlite_optional_timestamp(row.try_get("resolved_at")?)?,
        acknowledged_at: sqlite_optional_timestamp(row.try_get("acknowledged_at")?)?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        last_value: row.try_get("last_value")?,
        updated_at: parse_public_timestamp(row.try_get("updated_at")?)?,
    })
}

fn timescale_alert_record(row: PgRow) -> Result<PublicAlert, PlatformStoreError> {
    Ok(PublicAlert {
        id: row.try_get("id")?,
        rule_id: row.try_get("rule_id")?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        condition_started_at: row.try_get("condition_started_at")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        last_value: row.try_get("last_value")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn public_resource_permission(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    resource_type: &str,
    resource_id: &str,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    match resource_type {
        "asset" => match Uuid::parse_str(resource_id) {
            Ok(id) => public_asset_permission(store, principal, id).await,
            Err(_) => Ok(None),
        },
        "device" => public_device_permission(store, principal, resource_id).await,
        _ => Ok(None),
    }
}

fn valid_public_grant(grant: &NewPublicResourceGrant) -> bool {
    matches!(grant.resource_type.as_str(), "asset" | "device")
        && matches!(grant.grantee_type.as_str(), "user" | "application")
        && matches!(
            grant.permission.as_str(),
            "viewer" | "controller" | "manager"
        )
        && !grant.grantee_id.is_empty()
        && (grant.resource_type != "asset" || Uuid::parse_str(&grant.resource_id).is_ok())
}

async fn public_grant_visible(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    grant: &PublicResourceGrant,
) -> Result<bool, PlatformStoreError> {
    if grant.created_by_user_id == principal.user_id
        || (grant.grantee_type == "application" && grant.grantee_id == principal.app_id)
        || (grant.grantee_type == "user"
            && principal
                .user_id
                .is_some_and(|user_id| grant.grantee_id == user_id.to_string()))
    {
        return Ok(true);
    }
    Ok(
        public_resource_permission(store, principal, &grant.resource_type, &grant.resource_id)
            .await?
            .is_some_and(|permission| permission.allows(ResourcePermission::Manager)),
    )
}

async fn public_grant_record(
    store: &PlatformStore,
    grant_id: Uuid,
) -> Result<Option<PublicResourceGrant>, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, resource_type, resource_id, grantee_type, grantee_id, permission,
                    created_by_user_id, created_at, updated_at
             FROM resource_grants WHERE id = ?",
        )
        .bind(grant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_grant_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, resource_type, resource_id, grantee_type, grantee_id, permission,
                    created_by_user_id, created_at, updated_at
             FROM resource_grants WHERE id = $1",
        )
        .bind(grant_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_grant_record)
        .transpose(),
    }
}

async fn list_public_grants(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicResourceGrant>, PlatformStoreError> {
    let rows = match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, resource_type, resource_id, grantee_type, grantee_id, permission,
                        created_by_user_id, created_at, updated_at
                 FROM resource_grants
                 WHERE (? IS NULL OR id > ?)
                 ORDER BY id LIMIT ?",
            )
            .bind(after)
            .bind(after)
            .bind(i64::from(limit))
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_grant_record)
                .collect::<Result<Vec<_>, _>>()?
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, resource_type, resource_id, grantee_type, grantee_id, permission,
                        created_by_user_id, created_at, updated_at
                 FROM resource_grants
                 WHERE ($1::uuid IS NULL OR id > $1)
                 ORDER BY id LIMIT $2",
            )
            .bind(after.and_then(|value| Uuid::parse_str(value).ok()))
            .bind(i64::from(limit))
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_grant_record)
                .collect::<Result<Vec<_>, _>>()?
        }
    };
    let mut visible = Vec::new();
    for grant in rows {
        if public_grant_visible(store, principal, &grant).await? {
            visible.push(grant);
        }
    }
    Ok(visible)
}

async fn get_public_grant(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    grant_id: Uuid,
) -> Result<Option<PublicResourceGrant>, PlatformStoreError> {
    let Some(grant) = public_grant_record(store, grant_id).await? else {
        return Ok(None);
    };
    if public_grant_visible(store, principal, &grant).await? {
        Ok(Some(grant))
    } else {
        Ok(None)
    }
}

async fn create_public_grant(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    grant: NewPublicResourceGrant,
) -> Result<Option<PublicResourceGrant>, PlatformStoreError> {
    if !valid_public_grant(&grant)
        || !public_resource_permission(store, principal, &grant.resource_type, &grant.resource_id)
            .await?
            .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Ok(None);
    }
    let id = Uuid::now_v7();
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO resource_grants (
                    id, resource_type, resource_id, grantee_type, grantee_id, permission,
                    created_by_user_id
                 ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(id.to_string())
            .bind(&grant.resource_type)
            .bind(&grant.resource_id)
            .bind(&grant.grantee_type)
            .bind(&grant.grantee_id)
            .bind(&grant.permission)
            .bind(principal.user_id.map(|id| id.to_string()))
            .execute(store.pool())
            .await?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO resource_grants (
                    id, resource_type, resource_id, grantee_type, grantee_id, permission,
                    created_by_user_id
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(id)
            .bind(&grant.resource_type)
            .bind(&grant.resource_id)
            .bind(&grant.grantee_type)
            .bind(&grant.grantee_id)
            .bind(&grant.permission)
            .bind(principal.user_id)
            .execute(pool)
            .await?;
        }
    }
    public_grant_record(store, id).await
}

async fn update_public_grant(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    grant_id: Uuid,
    grant: NewPublicResourceGrant,
) -> Result<Option<PublicResourceGrant>, PlatformStoreError> {
    let Some(current) = public_grant_record(store, grant_id).await? else {
        return Ok(None);
    };
    if !public_grant_visible(store, principal, &current).await?
        || !public_resource_permission(
            store,
            principal,
            &current.resource_type,
            &current.resource_id,
        )
        .await?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
        || !valid_public_grant(&grant)
        || grant.resource_type != current.resource_type
        || grant.resource_id != current.resource_id
    {
        return Ok(None);
    }
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "UPDATE resource_grants
                 SET grantee_type = ?, grantee_id = ?, permission = ?, updated_at = ?
                 WHERE id = ?",
            )
            .bind(&grant.grantee_type)
            .bind(&grant.grantee_id)
            .bind(&grant.permission)
            .bind(Utc::now().to_rfc3339())
            .bind(grant_id.to_string())
            .execute(store.pool())
            .await?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "UPDATE resource_grants
                 SET grantee_type = $2, grantee_id = $3, permission = $4, updated_at = now()
                 WHERE id = $1",
            )
            .bind(grant_id)
            .bind(&grant.grantee_type)
            .bind(&grant.grantee_id)
            .bind(&grant.permission)
            .execute(pool)
            .await?;
        }
    }
    public_grant_record(store, grant_id).await
}

async fn delete_public_grant(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    grant_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    let Some(grant) = public_grant_record(store, grant_id).await? else {
        return Ok(false);
    };
    if !public_resource_permission(store, principal, &grant.resource_type, &grant.resource_id)
        .await?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Ok(false);
    }
    let affected = match store {
        PlatformStore::Sqlite(store) => sqlx::query("DELETE FROM resource_grants WHERE id = ?")
            .bind(grant_id.to_string())
            .execute(store.pool())
            .await?
            .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query("DELETE FROM resource_grants WHERE id = $1")
            .bind(grant_id)
            .execute(pool)
            .await?
            .rows_affected(),
    };
    Ok(affected == 1)
}

fn sqlite_grant_record(row: SqliteRow) -> Result<PublicResourceGrant, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let created_by_user_id = row
        .try_get::<Option<String>, _>("created_by_user_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public grant actor ID".to_owned(),
            ))
        })?;
    Ok(PublicResourceGrant {
        id: Uuid::parse_str(&id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public grant ID".to_owned(),
            ))
        })?,
        resource_type: row.try_get("resource_type")?,
        resource_id: row.try_get("resource_id")?,
        grantee_type: row.try_get("grantee_type")?,
        grantee_id: row.try_get("grantee_id")?,
        permission: row.try_get("permission")?,
        created_by_user_id,
        created_at: parse_public_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_public_timestamp(row.try_get("updated_at")?)?,
    })
}

fn timescale_grant_record(row: PgRow) -> Result<PublicResourceGrant, PlatformStoreError> {
    Ok(PublicResourceGrant {
        id: row.try_get("id")?,
        resource_type: row.try_get("resource_type")?,
        resource_id: row.try_get("resource_id")?,
        grantee_type: row.try_get("grantee_type")?,
        grantee_id: row.try_get("grantee_id")?,
        permission: row.try_get("permission")?,
        created_by_user_id: row.try_get("created_by_user_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn public_asset_permission(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<Option<ResourcePermission>, PlatformStoreError> {
    if principal.account_class == AccountClass::Admin {
        return Ok(Some(ResourcePermission::Owner));
    }

    match store {
        PlatformStore::Sqlite(store) => {
            let owner = sqlx::query_scalar::<_, Option<String>>(
                "SELECT owner_user_id FROM assets WHERE id = ?",
            )
            .bind(asset_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .flatten();
            if principal.user_id.map(|id| id.to_string()) == owner {
                return Ok(Some(ResourcePermission::Owner));
            }
            let user_id = principal.user_id.map(|id| id.to_string());
            let rows = sqlx::query_scalar::<_, String>(
                "SELECT permission FROM resource_shares
                 WHERE resource_type = 'asset' AND resource_id = ?
                   AND target_user_id = ? AND state = 'active'
                 UNION ALL
                 SELECT permission FROM resource_grants
                 WHERE resource_type = 'asset' AND resource_id = ?
                   AND ((grantee_type = 'user' AND grantee_id = ?)
                        OR (grantee_type = 'application' AND grantee_id = ?))",
            )
            .bind(asset_id.to_string())
            .bind(user_id.clone())
            .bind(asset_id.to_string())
            .bind(user_id)
            .bind(&principal.app_id)
            .fetch_all(store.pool())
            .await?;
            Ok(strongest_share_permission(rows))
        }
        PlatformStore::Timescale(pool) => {
            let owner = sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT owner_user_id FROM assets WHERE id = $1",
            )
            .bind(asset_id)
            .fetch_optional(pool)
            .await?
            .flatten();
            if principal.user_id == owner {
                return Ok(Some(ResourcePermission::Owner));
            }
            let rows = sqlx::query_scalar::<_, String>(
                "SELECT permission FROM resource_shares
                 WHERE resource_type = 'asset' AND resource_id = $1::text
                   AND target_user_id = $2 AND state = 'active'
                 UNION ALL
                 SELECT permission FROM resource_grants
                 WHERE resource_type = 'asset' AND resource_id = $1::text
                   AND ((grantee_type = 'user' AND grantee_id = $2::text)
                        OR (grantee_type = 'application' AND grantee_id = $3))",
            )
            .bind(asset_id)
            .bind(principal.user_id)
            .bind(&principal.app_id)
            .fetch_all(pool)
            .await?;
            Ok(strongest_share_permission(rows))
        }
    }
}

async fn list_public_assets(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    after: Option<&str>,
    limit: u32,
) -> Result<Vec<PublicAsset>, PlatformStoreError> {
    let limit = i64::from(limit);
    match store {
        PlatformStore::Sqlite(store) => {
            let user_id = principal.user_id.map(|id| id.to_string());
            let rows = sqlx::query(
                "SELECT id, name, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE (? = 1 OR owner_user_id = ?
                    OR EXISTS (
                        SELECT 1 FROM resource_shares
                        WHERE resource_type = 'asset' AND resource_id = assets.id
                          AND target_user_id = ? AND state = 'active'
                    )
                    OR EXISTS (
                        SELECT 1 FROM resource_grants
                        WHERE resource_type = 'asset' AND resource_id = assets.id
                          AND ((grantee_type = 'user' AND grantee_id = ?)
                               OR (grantee_type = 'application' AND grantee_id = ?))
                    ))
                   AND (? IS NULL OR id > ?)
                 ORDER BY id
                 LIMIT ?",
            )
            .bind(i64::from(principal.account_class == AccountClass::Admin))
            .bind(&user_id)
            .bind(&user_id)
            .bind(&user_id)
            .bind(&principal.app_id)
            .bind(after)
            .bind(after)
            .bind(limit)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter().map(sqlite_asset_record).collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, asset_profile_id, parent_asset_id, metadata
                 FROM assets
                 WHERE ($1::boolean OR owner_user_id = $2
                    OR EXISTS (
                        SELECT 1 FROM resource_shares
                        WHERE resource_type = 'asset' AND resource_id = assets.id::text
                          AND target_user_id = $2 AND state = 'active'
                    )
                    OR EXISTS (
                        SELECT 1 FROM resource_grants
                        WHERE resource_type = 'asset' AND resource_id = assets.id::text
                          AND ((grantee_type = 'user' AND grantee_id = $2::text)
                               OR (grantee_type = 'application' AND grantee_id = $3))
                    ))
                   AND ($4::uuid IS NULL OR id > $4)
                 ORDER BY id
                 LIMIT $5",
            )
            .bind(principal.account_class == AccountClass::Admin)
            .bind(principal.user_id)
            .bind(&principal.app_id)
            .bind(after.and_then(|value| Uuid::parse_str(value).ok()))
            .bind(limit)
            .fetch_all(pool)
            .await?;
            rows.into_iter().map(timescale_asset_record).collect()
        }
    }
}

async fn get_public_asset(
    store: &PlatformStore,
    asset_id: Uuid,
) -> Result<Option<PublicAsset>, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, name, asset_profile_id, parent_asset_id, metadata
             FROM assets WHERE id = ?",
        )
        .bind(asset_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_asset_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, name, asset_profile_id, parent_asset_id, metadata
             FROM assets WHERE id = $1",
        )
        .bind(asset_id)
        .fetch_optional(pool)
        .await?
        .map(timescale_asset_record)
        .transpose(),
    }
}

async fn create_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset: NewPublicAsset,
) -> Result<PublicAsset, PlatformStoreError> {
    let id = Uuid::now_v7();
    let created = match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "INSERT INTO assets (id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata)
                 VALUES (?, ?, ?, ?, ?, ?)
                 RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
            )
            .bind(id.to_string())
            .bind(asset.name)
            .bind(asset.asset_profile_id.map(|id| id.to_string()))
            .bind(asset.parent_asset_id.map(|id| id.to_string()))
            .bind(principal.user_id.map(|id| id.to_string()))
            .bind(asset.metadata.to_string())
            .fetch_one(store.pool())
            .await?;
            sqlite_asset_record(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "INSERT INTO assets (id, name, asset_profile_id, parent_asset_id, owner_user_id, metadata)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
            )
            .bind(id)
            .bind(asset.name)
            .bind(asset.asset_profile_id)
            .bind(asset.parent_asset_id)
            .bind(principal.user_id)
            .bind(sqlx::types::Json(asset.metadata))
            .fetch_one(pool)
            .await?;
            timescale_asset_record(row)
        }
    }?;
    if principal.user_id.is_none() {
        match store {
            PlatformStore::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO resource_grants (
                        id, resource_type, resource_id, grantee_type, grantee_id, permission
                     ) VALUES (?, 'asset', ?, 'application', ?, 'manager')",
                )
                .bind(Uuid::now_v7().to_string())
                .bind(id.to_string())
                .bind(&principal.app_id)
                .execute(store.pool())
                .await?;
            }
            PlatformStore::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO resource_grants (
                        id, resource_type, resource_id, grantee_type, grantee_id, permission
                     ) VALUES ($1, 'asset', $2::text, 'application', $3, 'manager')",
                )
                .bind(Uuid::now_v7())
                .bind(id)
                .bind(&principal.app_id)
                .execute(pool)
                .await?;
            }
        }
    }
    Ok(created)
}

async fn update_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
    asset: NewPublicAsset,
) -> Result<Option<PublicAsset>, PlatformStoreError> {
    if !public_asset_permission(store, principal, asset_id)
        .await?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Ok(None);
    }
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE assets
             SET name = ?, asset_profile_id = ?, parent_asset_id = ?, metadata = ?, updated_at = ?
             WHERE id = ?
             RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
        )
        .bind(asset.name)
        .bind(asset.asset_profile_id.map(|id| id.to_string()))
        .bind(asset.parent_asset_id.map(|id| id.to_string()))
        .bind(asset.metadata.to_string())
        .bind(Utc::now().to_rfc3339())
        .bind(asset_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_asset_record)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE assets
             SET name = $2, asset_profile_id = $3, parent_asset_id = $4,
                 metadata = $5, updated_at = now()
             WHERE id = $1
             RETURNING id, name, asset_profile_id, parent_asset_id, metadata",
        )
        .bind(asset_id)
        .bind(asset.name)
        .bind(asset.asset_profile_id)
        .bind(asset.parent_asset_id)
        .bind(sqlx::types::Json(asset.metadata))
        .fetch_optional(pool)
        .await?
        .map(timescale_asset_record)
        .transpose(),
    }
}

async fn delete_public_asset(
    store: &PlatformStore,
    principal: &PublicPrincipal,
    asset_id: Uuid,
) -> Result<bool, PlatformStoreError> {
    if !public_asset_permission(store, principal, asset_id)
        .await?
        .is_some_and(|permission| permission.allows(ResourcePermission::Manager))
    {
        return Ok(false);
    }
    let affected = match store {
        PlatformStore::Sqlite(store) => sqlx::query("DELETE FROM assets WHERE id = ?")
            .bind(asset_id.to_string())
            .execute(store.pool())
            .await?
            .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query("DELETE FROM assets WHERE id = $1")
            .bind(asset_id)
            .execute(pool)
            .await?
            .rows_affected(),
    };
    Ok(affected == 1)
}

fn sqlite_asset_record(row: SqliteRow) -> Result<PublicAsset, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let asset_profile_id = row
        .try_get::<Option<String>, _>("asset_profile_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public asset profile ID".to_owned(),
            ))
        })?;
    let parent_asset_id = row
        .try_get::<Option<String>, _>("parent_asset_id")?
        .map(|value| Uuid::parse_str(&value))
        .transpose()
        .map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public parent asset ID".to_owned(),
            ))
        })?;
    let metadata = serde_json::from_str(&row.try_get::<String, _>("metadata")?).map_err(|_| {
        PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid public asset metadata".to_owned(),
        ))
    })?;
    Ok(PublicAsset {
        id: Uuid::parse_str(&id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid public asset ID".to_owned(),
            ))
        })?,
        name: row.try_get("name")?,
        asset_profile_id,
        parent_asset_id,
        metadata,
    })
}

fn timescale_asset_record(row: PgRow) -> Result<PublicAsset, PlatformStoreError> {
    Ok(PublicAsset {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        asset_profile_id: row.try_get("asset_profile_id")?,
        parent_asset_id: row.try_get("parent_asset_id")?,
        metadata: row.try_get::<Json<serde_json::Value>, _>("metadata")?.0,
    })
}
