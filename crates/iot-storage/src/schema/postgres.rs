use sqlx::{PgPool, Postgres, Transaction};

use crate::{PLATFORM_SCHEMA_VERSION, PlatformStoreError};

const PLATFORM_POSTGRES_SCHEMA: &str = include_str!("../../migrations/0001_platform.sql");

const CANONICAL_TABLES: &[&str] = &[
    "platform_schema",
    "system_accounts",
    "tenants",
    "tenant_accounts",
    "login_usernames",
    "users",
    "applications",
    "user_capabilities",
    "application_redirect_uris",
    "oauth_client_secrets",
    "oauth_authorization_codes",
    "oauth_access_tokens",
    "asset_profiles",
    "device_profiles",
    "application_domain_profiles",
    "application_asset_profile_relations",
    "resource_application_profile_assignments",
    "tenant_profile_configurations",
    "resource_tenant_profile_assignments",
    "assets",
    "devices",
    "tenant_device_claim_policies",
    "device_claim_codes",
    "device_relations",
    "device_asset_relations",
    "device_tokens",
    "tenant_personal_access_tokens",
    "user_groups",
    "user_group_members",
    "resource_permissions",
    "resource_invitations",
    "audit_events",
    "device_runtime_state",
    "telemetry",
    "alert_rules",
    "alert_rule_event_evaluations",
    "alert_incidents",
    "notification_outbox",
    "command_outbox",
    "gateway_event_receipts",
];

enum TimescaleSchemaState {
    Fresh,
    Current,
}

pub(crate) async fn migrate(pool: &PgPool) -> Result<(), PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('iot_nano:migrate'))")
        .execute(&mut *transaction)
        .await?;
    let schema_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'iot_nano'
         )",
    )
    .fetch_one(&mut *transaction)
    .await?;
    if !schema_exists {
        sqlx::query("CREATE SCHEMA iot_nano")
            .execute(&mut *transaction)
            .await?;
    }
    sqlx::query("SET LOCAL search_path TO iot_nano, public")
        .execute(&mut *transaction)
        .await?;

    let schema_state = if schema_exists {
        require_current_schema_marker(&mut transaction).await?
    } else {
        TimescaleSchemaState::Fresh
    };
    if matches!(schema_state, TimescaleSchemaState::Fresh) {
        sqlx::query("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" WITH SCHEMA public")
            .execute(&mut *transaction)
            .await?;
        sqlx::raw_sql(PLATFORM_POSTGRES_SCHEMA)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("INSERT INTO platform_schema (singleton, version) VALUES (1, $1)")
            .bind(PLATFORM_SCHEMA_VERSION)
            .execute(&mut *transaction)
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn require_current_schema_marker(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<TimescaleSchemaState, PlatformStoreError> {
    let objects = sqlx::query_scalar::<_, String>(
        "SELECT relation.relname::text
         FROM pg_catalog.pg_class AS relation
         JOIN pg_catalog.pg_namespace AS namespace
           ON namespace.oid = relation.relnamespace
         WHERE namespace.nspname = 'iot_nano'
           AND relation.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
         UNION ALL
         SELECT 'function:' || procedure.proname::text
         FROM pg_catalog.pg_proc AS procedure
         JOIN pg_catalog.pg_namespace AS namespace
           ON namespace.oid = procedure.pronamespace
         WHERE namespace.nspname = 'iot_nano'
         ORDER BY 1",
    )
    .fetch_all(&mut **transaction)
    .await?;
    let tables = sqlx::query_scalar::<_, String>(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = 'iot_nano' AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .fetch_all(&mut **transaction)
    .await?;
    if !tables.iter().any(|table| table == "platform_schema") {
        return match objects.first() {
            Some(table) => Err(PlatformStoreError::ResetRequiredTimescaleSchema {
                table: table.clone(),
            }),
            None => Ok(TimescaleSchemaState::Fresh),
        };
    }

    let marker = sqlx::query_as::<_, (i64, i64)>(
        "SELECT singleton, version FROM platform_schema ORDER BY singleton",
    )
    .fetch_all(&mut **transaction)
    .await;
    if !matches!(marker, Ok(ref marker) if marker.as_slice() == [(1, PLATFORM_SCHEMA_VERSION)]) {
        return Err(PlatformStoreError::ResetRequiredTimescaleSchema {
            table: "platform_schema".to_owned(),
        });
    }
    if let Some(table) = tables
        .iter()
        .find(|table| !CANONICAL_TABLES.contains(&table.as_str()))
    {
        return Err(PlatformStoreError::ResetRequiredTimescaleSchema {
            table: table.clone(),
        });
    }
    if let Some(table) = CANONICAL_TABLES
        .iter()
        .find(|table| !tables.iter().any(|existing| existing == *table))
    {
        return Err(PlatformStoreError::ResetRequiredTimescaleSchema {
            table: (*table).to_owned(),
        });
    }

    let has_legacy_default_app: bool = sqlx::query_scalar(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.columns
             WHERE table_schema = 'iot_nano'
               AND table_name = 'users'
               AND column_name = 'default_app'
         )",
    )
    .fetch_one(&mut **transaction)
    .await?;
    if has_legacy_default_app {
        return Err(PlatformStoreError::ResetRequiredTimescaleSchema {
            table: "users.default_app".to_owned(),
        });
    }
    Ok(TimescaleSchemaState::Current)
}
