use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use chrono::Utc;
use sqlx::{Row, postgres::PgRow, sqlite::SqliteRow, types::Json};
use uuid::Uuid;

use crate::{
    ApplicationAssetProfileRelation, ApplicationDomainProfile, ApplicationDomainProfileError,
    ApplicationDomainProfileRepository, ApplicationDomainResourceKind, ApplicationId,
    CreateApplicationAssetProfileRelation, CreateApplicationDomainProfile, PlatformStore,
    TenantProfileConfiguration, TenantProfileDefinition, TenantProfileRepository,
    UpdateApplicationDomainProfile,
};

impl TenantProfileRepository for PlatformStore {
    fn export_tenant_profile_configuration<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<TenantProfileConfiguration, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { export_tenant_profile_configuration(self, tenant_id).await })
    }

    fn replace_tenant_profile_configuration<'a>(
        &'a self,
        tenant_id: Uuid,
        configuration: TenantProfileConfiguration,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>> {
        Box::pin(async move {
            replace_tenant_profile_configuration(self, tenant_id, configuration).await
        })
    }

    fn list_tenant_profile_definitions<'a>(
        &'a self,
        tenant_id: Uuid,
        resource_kind: Option<ApplicationDomainResourceKind>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<TenantProfileDefinition>, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(
            async move { list_tenant_profile_definitions(self, tenant_id, resource_kind).await },
        )
    }

    fn assign_tenant_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        resource_kind: ApplicationDomainResourceKind,
        resource_id: &'a str,
        profile_id: Option<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>> {
        Box::pin(async move {
            assign_tenant_profile(self, tenant_id, resource_kind, resource_id, profile_id).await
        })
    }

    fn tenant_profile_assignment<'a>(
        &'a self,
        tenant_id: Uuid,
        resource_kind: ApplicationDomainResourceKind,
        resource_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Option<TenantProfileDefinition>, ApplicationDomainProfileError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            tenant_profile_assignment(self, tenant_id, resource_kind, resource_id).await
        })
    }
}

impl ApplicationDomainProfileRepository for PlatformStore {
    fn list_application_domain_profiles<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        resource_kind: Option<ApplicationDomainResourceKind>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Vec<ApplicationDomainProfile>, ApplicationDomainProfileError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            list_application_domain_profiles(self, tenant_id, app_id, resource_kind).await
        })
    }

    fn create_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        profile: CreateApplicationDomainProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ApplicationDomainProfile, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { create_application_domain_profile(self, tenant_id, profile).await })
    }

    fn update_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        profile_id: Uuid,
        profile: UpdateApplicationDomainProfile,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ApplicationDomainProfile, ApplicationDomainProfileError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            update_application_domain_profile(self, tenant_id, app_id, profile_id, profile).await
        })
    }

    fn delete_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        profile_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>> {
        Box::pin(async move {
            delete_application_domain_profile(self, tenant_id, app_id, profile_id).await
        })
    }

    fn list_application_asset_profile_relations<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Vec<ApplicationAssetProfileRelation>,
                        ApplicationDomainProfileError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(
            async move { list_application_asset_profile_relations(self, tenant_id, app_id).await },
        )
    }

    fn create_application_asset_profile_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        relation: CreateApplicationAssetProfileRelation,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<ApplicationAssetProfileRelation, ApplicationDomainProfileError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            create_application_asset_profile_relation(self, tenant_id, relation).await
        })
    }

    fn delete_application_asset_profile_relation<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        relation_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>> {
        Box::pin(async move {
            delete_application_asset_profile_relation(self, tenant_id, app_id, relation_id).await
        })
    }

    fn assign_application_domain_profile<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        resource_kind: ApplicationDomainResourceKind,
        resource_id: &'a str,
        profile_id: Option<Uuid>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ApplicationDomainProfileError>> + Send + 'a>> {
        Box::pin(async move {
            assign_application_domain_profile(
                self,
                tenant_id,
                app_id,
                resource_kind,
                resource_id,
                profile_id,
            )
            .await
        })
    }

    fn application_domain_profile_assignment<'a>(
        &'a self,
        tenant_id: Uuid,
        app_id: &'a str,
        resource_kind: ApplicationDomainResourceKind,
        resource_id: &'a str,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Option<ApplicationDomainProfile>,
                        ApplicationDomainProfileError,
                    >,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            application_domain_profile_assignment(
                self,
                tenant_id,
                app_id,
                resource_kind,
                resource_id,
            )
            .await
        })
    }
}

fn parse_app_id(app_id: &str) -> Result<ApplicationId, ApplicationDomainProfileError> {
    app_id.parse().map_err(ApplicationDomainProfileError::from)
}

fn validate_profile(
    id: Uuid,
    app_id: ApplicationId,
    resource_kind: ApplicationDomainResourceKind,
    name: String,
    definition: serde_json::Value,
    live_view: serde_json::Value,
) -> Result<ApplicationDomainProfile, ApplicationDomainProfileError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err(ApplicationDomainProfileError::InvalidName);
    }
    if !definition.is_object() {
        return Err(ApplicationDomainProfileError::DefinitionMustBeObject);
    }
    if !live_view.is_object() {
        return Err(ApplicationDomainProfileError::LiveViewMustBeObject);
    }
    Ok(ApplicationDomainProfile {
        id,
        app_id,
        resource_kind,
        name: name.to_owned(),
        definition,
        live_view,
    })
}

async fn ensure_application(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &ApplicationId,
) -> Result<(), ApplicationDomainProfileError> {
    let found = match store {
        PlatformStore::Sqlite(store) => sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM applications WHERE app_id = ? AND tenant_id = ?",
        )
        .bind(app_id.as_str())
        .bind(tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .is_some(),
        PlatformStore::Timescale(pool) => sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM applications WHERE app_id = $1 AND tenant_id = $2",
        )
        .bind(app_id.as_str())
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?
        .is_some(),
    };
    if found {
        Ok(())
    } else {
        Err(ApplicationDomainProfileError::ApplicationNotFound)
    }
}

async fn list_application_domain_profiles(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
    resource_kind: Option<ApplicationDomainResourceKind>,
) -> Result<Vec<ApplicationDomainProfile>, ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    match (store, resource_kind) {
        (PlatformStore::Sqlite(store), Some(resource_kind)) => sqlx::query(
            "SELECT id, app_id, resource_kind, name, definition, live_view
             FROM application_domain_profiles
             WHERE tenant_id = ? AND app_id = ? AND resource_kind = ?
             ORDER BY name, id",
        )
        .bind(tenant_id.to_string())
        .bind(app_id.as_str())
        .bind(resource_kind.as_str())
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .map(sqlite_domain_profile)
        .collect(),
        (PlatformStore::Sqlite(store), None) => sqlx::query(
            "SELECT id, app_id, resource_kind, name, definition, live_view
             FROM application_domain_profiles
             WHERE tenant_id = ? AND app_id = ?
             ORDER BY resource_kind, name, id",
        )
        .bind(tenant_id.to_string())
        .bind(app_id.as_str())
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .map(sqlite_domain_profile)
        .collect(),
        (PlatformStore::Timescale(pool), Some(resource_kind)) => sqlx::query(
            "SELECT id, app_id, resource_kind, name, definition, live_view
             FROM application_domain_profiles
             WHERE tenant_id = $1 AND app_id = $2 AND resource_kind = $3
             ORDER BY name, id",
        )
        .bind(tenant_id)
        .bind(app_id.as_str())
        .bind(resource_kind.as_str())
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(timescale_domain_profile)
        .collect(),
        (PlatformStore::Timescale(pool), None) => sqlx::query(
            "SELECT id, app_id, resource_kind, name, definition, live_view
             FROM application_domain_profiles
             WHERE tenant_id = $1 AND app_id = $2
             ORDER BY resource_kind, name, id",
        )
        .bind(tenant_id)
        .bind(app_id.as_str())
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(timescale_domain_profile)
        .collect(),
    }
}

async fn create_application_domain_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile: CreateApplicationDomainProfile,
) -> Result<ApplicationDomainProfile, ApplicationDomainProfileError> {
    ensure_application(store, tenant_id, &profile.app_id).await?;
    let profile = validate_profile(
        Uuid::now_v7(),
        profile.app_id,
        profile.resource_kind,
        profile.name,
        profile.definition,
        profile.live_view,
    )?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO application_domain_profiles (
                id, app_id, tenant_id, resource_kind, name, definition, live_view, updated_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(profile.id.to_string())
            .bind(profile.app_id.as_str())
            .bind(tenant_id.to_string())
            .bind(profile.resource_kind.as_str())
            .bind(&profile.name)
            .bind(profile.definition.to_string())
            .bind(profile.live_view.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await
            .map_err(|error| profile_create_error(error, &profile.name))?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO application_domain_profiles (
                id, app_id, tenant_id, resource_kind, name, definition, live_view
             ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(profile.id)
            .bind(profile.app_id.as_str())
            .bind(tenant_id)
            .bind(profile.resource_kind.as_str())
            .bind(&profile.name)
            .bind(Json(profile.definition.clone()))
            .bind(Json(profile.live_view.clone()))
            .execute(pool)
            .await
            .map_err(|error| profile_create_error(error, &profile.name))?;
        }
    }
    Ok(profile)
}

async fn update_application_domain_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
    profile_id: Uuid,
    update: UpdateApplicationDomainProfile,
) -> Result<ApplicationDomainProfile, ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    let current = find_domain_profile(store, tenant_id, &app_id, profile_id)
        .await?
        .ok_or(ApplicationDomainProfileError::ProfileNotFound)?;
    let profile = validate_profile(
        profile_id,
        app_id,
        current.resource_kind,
        update.name,
        update.definition,
        update.live_view,
    )?;
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "UPDATE application_domain_profiles
             SET name = ?, definition = ?, live_view = ?, updated_at = ?
             WHERE id = ? AND tenant_id = ? AND app_id = ?",
            )
            .bind(&profile.name)
            .bind(profile.definition.to_string())
            .bind(profile.live_view.to_string())
            .bind(Utc::now().to_rfc3339())
            .bind(profile.id.to_string())
            .bind(tenant_id.to_string())
            .bind(profile.app_id.as_str())
            .execute(store.pool())
            .await
            .map_err(|error| profile_create_error(error, &profile.name))?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "UPDATE application_domain_profiles
             SET name = $1, definition = $2, live_view = $3, updated_at = now()
             WHERE id = $4 AND tenant_id = $5 AND app_id = $6",
            )
            .bind(&profile.name)
            .bind(Json(profile.definition.clone()))
            .bind(Json(profile.live_view.clone()))
            .bind(profile.id)
            .bind(tenant_id)
            .bind(profile.app_id.as_str())
            .execute(pool)
            .await
            .map_err(|error| profile_create_error(error, &profile.name))?;
        }
    }
    Ok(profile)
}

async fn delete_application_domain_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
    profile_id: Uuid,
) -> Result<(), ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    if find_domain_profile(store, tenant_id, &app_id, profile_id)
        .await?
        .is_none()
    {
        return Err(ApplicationDomainProfileError::ProfileNotFound);
    }
    let references = match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT (
                (SELECT COUNT(*) FROM resource_application_profile_assignments
                 WHERE tenant_id = ? AND profile_id = ?)
                +
                (SELECT COUNT(*) FROM application_asset_profile_relations
                 WHERE tenant_id = ? AND (parent_profile_id = ? OR child_profile_id = ?))
             )",
            )
            .bind(tenant_id.to_string())
            .bind(profile_id.to_string())
            .bind(tenant_id.to_string())
            .bind(profile_id.to_string())
            .bind(profile_id.to_string())
            .fetch_one(store.pool())
            .await?
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query_scalar::<_, i64>(
                "SELECT (
                (SELECT COUNT(*) FROM resource_application_profile_assignments
                 WHERE tenant_id = $1 AND profile_id = $2)
                +
                (SELECT COUNT(*) FROM application_asset_profile_relations
                 WHERE tenant_id = $1 AND (parent_profile_id = $2 OR child_profile_id = $2))
             )",
            )
            .bind(tenant_id)
            .bind(profile_id)
            .fetch_one(pool)
            .await?
        }
    };
    if references > 0 {
        return Err(ApplicationDomainProfileError::ProfileInUse(profile_id));
    }
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
            "DELETE FROM application_domain_profiles WHERE id = ? AND tenant_id = ? AND app_id = ?",
        )
        .bind(profile_id.to_string())
        .bind(tenant_id.to_string())
        .bind(app_id.as_str())
            .execute(store.pool())
            .await?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
            "DELETE FROM application_domain_profiles WHERE id = $1 AND tenant_id = $2 AND app_id = $3",
        )
        .bind(profile_id)
        .bind(tenant_id)
        .bind(app_id.as_str())
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

async fn list_application_asset_profile_relations(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
) -> Result<Vec<ApplicationAssetProfileRelation>, ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, app_id, parent_profile_id, child_profile_id
             FROM application_asset_profile_relations
             WHERE tenant_id = ? AND app_id = ? ORDER BY id",
        )
        .bind(tenant_id.to_string())
        .bind(app_id.as_str())
        .fetch_all(store.pool())
        .await?
        .into_iter()
        .map(sqlite_asset_profile_relation)
        .collect(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, app_id, parent_profile_id, child_profile_id
             FROM application_asset_profile_relations
             WHERE tenant_id = $1 AND app_id = $2 ORDER BY id",
        )
        .bind(tenant_id)
        .bind(app_id.as_str())
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(timescale_asset_profile_relation)
        .collect(),
    }
}

async fn create_application_asset_profile_relation(
    store: &PlatformStore,
    tenant_id: Uuid,
    relation: CreateApplicationAssetProfileRelation,
) -> Result<ApplicationAssetProfileRelation, ApplicationDomainProfileError> {
    ensure_application(store, tenant_id, &relation.app_id).await?;
    if relation.parent_profile_id == relation.child_profile_id {
        return Err(ApplicationDomainProfileError::AssetProfilesOnly);
    }
    for profile_id in [relation.parent_profile_id, relation.child_profile_id] {
        let profile = find_domain_profile(store, tenant_id, &relation.app_id, profile_id)
            .await?
            .ok_or(ApplicationDomainProfileError::ProfileNotFound)?;
        if profile.resource_kind != ApplicationDomainResourceKind::Asset {
            return Err(ApplicationDomainProfileError::AssetProfilesOnly);
        }
    }
    let relation = ApplicationAssetProfileRelation {
        id: Uuid::now_v7(),
        app_id: relation.app_id,
        parent_profile_id: relation.parent_profile_id,
        child_profile_id: relation.child_profile_id,
    };
    match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query(
                "INSERT INTO application_asset_profile_relations (
                id, app_id, tenant_id, parent_profile_id, child_profile_id
             ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(relation.id.to_string())
            .bind(relation.app_id.as_str())
            .bind(tenant_id.to_string())
            .bind(relation.parent_profile_id.to_string())
            .bind(relation.child_profile_id.to_string())
            .execute(store.pool())
            .await
            .map_err(|error| {
                if error.to_string().contains("UNIQUE") || error.to_string().contains("duplicate") {
                    ApplicationDomainProfileError::RelationConflict
                } else {
                    ApplicationDomainProfileError::from(error)
                }
            })?;
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query(
                "INSERT INTO application_asset_profile_relations (
                id, app_id, tenant_id, parent_profile_id, child_profile_id
             ) VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(relation.id)
            .bind(relation.app_id.as_str())
            .bind(tenant_id)
            .bind(relation.parent_profile_id)
            .bind(relation.child_profile_id)
            .execute(pool)
            .await
            .map_err(|error| {
                if error.to_string().contains("UNIQUE") || error.to_string().contains("duplicate") {
                    ApplicationDomainProfileError::RelationConflict
                } else {
                    ApplicationDomainProfileError::from(error)
                }
            })?;
        }
    }
    Ok(relation)
}

async fn delete_application_asset_profile_relation(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
    relation_id: Uuid,
) -> Result<(), ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    let deleted = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "DELETE FROM application_asset_profile_relations
             WHERE id = ? AND tenant_id = ? AND app_id = ?",
        )
        .bind(relation_id.to_string())
        .bind(tenant_id.to_string())
        .bind(app_id.as_str())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "DELETE FROM application_asset_profile_relations
             WHERE id = $1 AND tenant_id = $2 AND app_id = $3",
        )
        .bind(relation_id)
        .bind(tenant_id)
        .bind(app_id.as_str())
        .execute(pool)
        .await?
        .rows_affected(),
    };
    if deleted == 0 {
        Err(ApplicationDomainProfileError::RelationNotFound)
    } else {
        Ok(())
    }
}

async fn assign_application_domain_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
    resource_kind: ApplicationDomainResourceKind,
    resource_id: &str,
    profile_id: Option<Uuid>,
) -> Result<(), ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    if !resource_exists(store, tenant_id, resource_kind, resource_id).await? {
        return Err(ApplicationDomainProfileError::ResourceNotFound);
    }
    if let Some(profile_id) = profile_id {
        let profile = find_domain_profile(store, tenant_id, &app_id, profile_id)
            .await?
            .ok_or(ApplicationDomainProfileError::ProfileNotFound)?;
        if profile.resource_kind != resource_kind {
            return Err(ApplicationDomainProfileError::ProfileKindMismatch);
        }
        match store {
            PlatformStore::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO resource_application_profile_assignments (
                    app_id, tenant_id, resource_kind, resource_id, profile_id, updated_at
                 ) VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT (app_id, tenant_id, resource_kind, resource_id) DO UPDATE SET
                    profile_id = excluded.profile_id, updated_at = excluded.updated_at",
                )
                .bind(app_id.as_str())
                .bind(tenant_id.to_string())
                .bind(resource_kind.as_str())
                .bind(resource_id)
                .bind(profile.id.to_string())
                .bind(Utc::now().to_rfc3339())
                .execute(store.pool())
                .await?;
            }
            PlatformStore::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO resource_application_profile_assignments (
                    app_id, tenant_id, resource_kind, resource_id, profile_id
                 ) VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (app_id, tenant_id, resource_kind, resource_id) DO UPDATE SET
                    profile_id = EXCLUDED.profile_id, updated_at = now()",
                )
                .bind(app_id.as_str())
                .bind(tenant_id)
                .bind(resource_kind.as_str())
                .bind(resource_id)
                .bind(profile.id)
                .execute(pool)
                .await?;
            }
        }
    } else {
        match store {
            PlatformStore::Sqlite(store) => {
                sqlx::query(
                    "DELETE FROM resource_application_profile_assignments
                 WHERE app_id = ? AND tenant_id = ? AND resource_kind = ? AND resource_id = ?",
                )
                .bind(app_id.as_str())
                .bind(tenant_id.to_string())
                .bind(resource_kind.as_str())
                .bind(resource_id)
                .execute(store.pool())
                .await?;
            }
            PlatformStore::Timescale(pool) => {
                sqlx::query(
                    "DELETE FROM resource_application_profile_assignments
                 WHERE app_id = $1 AND tenant_id = $2 AND resource_kind = $3 AND resource_id = $4",
                )
                .bind(app_id.as_str())
                .bind(tenant_id)
                .bind(resource_kind.as_str())
                .bind(resource_id)
                .execute(pool)
                .await?;
            }
        }
    }
    Ok(())
}

async fn application_domain_profile_assignment(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &str,
    resource_kind: ApplicationDomainResourceKind,
    resource_id: &str,
) -> Result<Option<ApplicationDomainProfile>, ApplicationDomainProfileError> {
    let app_id = parse_app_id(app_id)?;
    ensure_application(store, tenant_id, &app_id).await?;
    let profile_id = match store {
        PlatformStore::Sqlite(store) => sqlx::query_scalar::<_, String>(
            "SELECT profile_id FROM resource_application_profile_assignments
             WHERE app_id = ? AND tenant_id = ? AND resource_kind = ? AND resource_id = ?",
        )
        .bind(app_id.as_str())
        .bind(tenant_id.to_string())
        .bind(resource_kind.as_str())
        .bind(resource_id)
        .fetch_optional(store.pool())
        .await?
        .map(|value| value.parse())
        .transpose()
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?,
        PlatformStore::Timescale(pool) => {
            sqlx::query_scalar::<_, Uuid>(
                "SELECT profile_id FROM resource_application_profile_assignments
             WHERE app_id = $1 AND tenant_id = $2 AND resource_kind = $3 AND resource_id = $4",
            )
            .bind(app_id.as_str())
            .bind(tenant_id)
            .bind(resource_kind.as_str())
            .bind(resource_id)
            .fetch_optional(pool)
            .await?
        }
    };
    let Some(profile_id) = profile_id else {
        return Ok(None);
    };
    let profile = find_domain_profile(store, tenant_id, &app_id, profile_id).await?;
    match profile {
        Some(profile) if profile.resource_kind == resource_kind => Ok(Some(profile)),
        Some(_) => Err(ApplicationDomainProfileError::ProfileKindMismatch),
        None => Err(ApplicationDomainProfileError::ProfileNotFound),
    }
}

async fn resource_exists(
    store: &PlatformStore,
    tenant_id: Uuid,
    resource_kind: ApplicationDomainResourceKind,
    resource_id: &str,
) -> Result<bool, ApplicationDomainProfileError> {
    match (store, resource_kind) {
        (PlatformStore::Sqlite(store), ApplicationDomainResourceKind::Asset) => {
            let asset_id = resource_id
                .parse::<Uuid>()
                .map_err(|_| ApplicationDomainProfileError::ResourceNotFound)?;
            Ok(sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM assets WHERE id = ? AND tenant_id = ?",
            )
            .bind(asset_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .is_some())
        }
        (PlatformStore::Sqlite(store), ApplicationDomainResourceKind::Device) => Ok(
            sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM devices WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
            )
            .bind(resource_id)
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .is_some(),
        ),
        (PlatformStore::Timescale(pool), ApplicationDomainResourceKind::Asset) => {
            let asset_id = resource_id
                .parse::<Uuid>()
                .map_err(|_| ApplicationDomainProfileError::ResourceNotFound)?;
            Ok(sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM assets WHERE id = $1 AND tenant_id = $2",
            )
            .bind(asset_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .is_some())
        }
        (PlatformStore::Timescale(pool), ApplicationDomainResourceKind::Device) => Ok(
            sqlx::query_scalar::<_, i64>(
                "SELECT 1 FROM devices WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL",
            )
            .bind(resource_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .is_some(),
        ),
    }
}

async fn find_domain_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    app_id: &ApplicationId,
    profile_id: Uuid,
) -> Result<Option<ApplicationDomainProfile>, ApplicationDomainProfileError> {
    match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "SELECT id, app_id, resource_kind, name, definition, live_view
             FROM application_domain_profiles
             WHERE id = ? AND tenant_id = ? AND app_id = ?",
        )
        .bind(profile_id.to_string())
        .bind(tenant_id.to_string())
        .bind(app_id.as_str())
        .fetch_optional(store.pool())
        .await?
        .map(sqlite_domain_profile)
        .transpose(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "SELECT id, app_id, resource_kind, name, definition, live_view
             FROM application_domain_profiles
             WHERE id = $1 AND tenant_id = $2 AND app_id = $3",
        )
        .bind(profile_id)
        .bind(tenant_id)
        .bind(app_id.as_str())
        .fetch_optional(pool)
        .await?
        .map(timescale_domain_profile)
        .transpose(),
    }
}

fn sqlite_domain_profile(
    row: SqliteRow,
) -> Result<ApplicationDomainProfile, ApplicationDomainProfileError> {
    let id = row
        .try_get::<String, _>("id")?
        .parse()
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?;
    let app_id = row
        .try_get::<String, _>("app_id")?
        .parse()
        .map_err(ApplicationDomainProfileError::from)?;
    let resource_kind =
        ApplicationDomainResourceKind::parse(&row.try_get::<String, _>("resource_kind")?)
            .ok_or(ApplicationDomainProfileError::InvalidStoredProfile)?;
    let definition = serde_json::from_str(&row.try_get::<String, _>("definition")?)
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?;
    let live_view = serde_json::from_str(&row.try_get::<String, _>("live_view")?)
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?;
    validate_profile(
        id,
        app_id,
        resource_kind,
        row.try_get("name")?,
        definition,
        live_view,
    )
    .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)
}

fn timescale_domain_profile(
    row: PgRow,
) -> Result<ApplicationDomainProfile, ApplicationDomainProfileError> {
    let app_id = row
        .try_get::<String, _>("app_id")?
        .parse()
        .map_err(ApplicationDomainProfileError::from)?;
    let resource_kind =
        ApplicationDomainResourceKind::parse(&row.try_get::<String, _>("resource_kind")?)
            .ok_or(ApplicationDomainProfileError::InvalidStoredProfile)?;
    let definition: Json<serde_json::Value> = row.try_get("definition")?;
    let live_view: Json<serde_json::Value> = row.try_get("live_view")?;
    validate_profile(
        row.try_get("id")?,
        app_id,
        resource_kind,
        row.try_get("name")?,
        definition.0,
        live_view.0,
    )
    .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)
}

fn sqlite_asset_profile_relation(
    row: SqliteRow,
) -> Result<ApplicationAssetProfileRelation, ApplicationDomainProfileError> {
    Ok(ApplicationAssetProfileRelation {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?,
        app_id: row
            .try_get::<String, _>("app_id")?
            .parse()
            .map_err(ApplicationDomainProfileError::from)?,
        parent_profile_id: row
            .try_get::<String, _>("parent_profile_id")?
            .parse()
            .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?,
        child_profile_id: row
            .try_get::<String, _>("child_profile_id")?
            .parse()
            .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?,
    })
}

fn timescale_asset_profile_relation(
    row: PgRow,
) -> Result<ApplicationAssetProfileRelation, ApplicationDomainProfileError> {
    Ok(ApplicationAssetProfileRelation {
        id: row.try_get("id")?,
        app_id: row
            .try_get::<String, _>("app_id")?
            .parse()
            .map_err(ApplicationDomainProfileError::from)?,
        parent_profile_id: row.try_get("parent_profile_id")?,
        child_profile_id: row.try_get("child_profile_id")?,
    })
}

fn profile_create_error(error: sqlx::Error, name: &str) -> ApplicationDomainProfileError {
    if error.to_string().contains("UNIQUE") || error.to_string().contains("duplicate") {
        ApplicationDomainProfileError::NameConflict(name.to_owned())
    } else {
        ApplicationDomainProfileError::from(error)
    }
}

async fn export_tenant_profile_configuration(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<TenantProfileConfiguration, ApplicationDomainProfileError> {
    let stored = match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query_scalar::<_, String>(
                "SELECT configuration FROM tenant_profile_configurations WHERE tenant_id = ?",
            )
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
        }
        PlatformStore::Timescale(pool) => sqlx::query_scalar::<_, String>(
            "SELECT configuration::text FROM tenant_profile_configurations WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?,
    };
    let Some(stored) = stored else {
        return Ok(TenantProfileConfiguration::empty());
    };
    let configuration = serde_json::from_str::<TenantProfileConfiguration>(&stored)
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?;
    validate_tenant_profile_configuration(&configuration)
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?;
    Ok(configuration)
}

async fn replace_tenant_profile_configuration(
    store: &PlatformStore,
    tenant_id: Uuid,
    configuration: TenantProfileConfiguration,
) -> Result<(), ApplicationDomainProfileError> {
    validate_tenant_profile_configuration(&configuration)?;
    let serialized = serde_json::to_string(&configuration)
        .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)?;
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            sqlx::query("DELETE FROM resource_tenant_profile_assignments WHERE tenant_id = ?")
                .bind(tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
            sqlx::query(
                "INSERT INTO tenant_profile_configurations (tenant_id, version, configuration, updated_at)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(tenant_id) DO UPDATE SET
                    version = excluded.version,
                    configuration = excluded.configuration,
                    updated_at = excluded.updated_at",
            )
            .bind(tenant_id.to_string())
            .bind(i64::from(configuration.version))
            .bind(&serialized)
            .bind(Utc::now().to_rfc3339())
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            sqlx::query("DELETE FROM resource_tenant_profile_assignments WHERE tenant_id = $1")
                .bind(tenant_id)
                .execute(&mut *transaction)
                .await?;
            sqlx::query(
                "INSERT INTO tenant_profile_configurations (tenant_id, version, configuration)
                 VALUES ($1, $2, $3::jsonb)
                 ON CONFLICT(tenant_id) DO UPDATE SET
                    version = EXCLUDED.version,
                    configuration = EXCLUDED.configuration,
                    updated_at = now()",
            )
            .bind(tenant_id)
            .bind(i32::from(configuration.version))
            .bind(&serialized)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
    }
    Ok(())
}

async fn list_tenant_profile_definitions(
    store: &PlatformStore,
    tenant_id: Uuid,
    resource_kind: Option<ApplicationDomainResourceKind>,
) -> Result<Vec<TenantProfileDefinition>, ApplicationDomainProfileError> {
    let configuration = export_tenant_profile_configuration(store, tenant_id).await?;
    Ok(configuration
        .profiles
        .into_iter()
        .filter(|profile| resource_kind.is_none_or(|kind| profile.resource_kind == kind))
        .collect())
}

async fn assign_tenant_profile(
    store: &PlatformStore,
    tenant_id: Uuid,
    resource_kind: ApplicationDomainResourceKind,
    resource_id: &str,
    profile_id: Option<Uuid>,
) -> Result<(), ApplicationDomainProfileError> {
    if !resource_exists(store, tenant_id, resource_kind, resource_id).await? {
        return Err(ApplicationDomainProfileError::ResourceNotFound);
    }
    if let Some(profile_id) = profile_id {
        let profile = tenant_profile_definition(store, tenant_id, profile_id).await?;
        if profile.resource_kind != resource_kind {
            return Err(ApplicationDomainProfileError::ProfileKindMismatch);
        }
    }
    match (store, profile_id) {
        (PlatformStore::Sqlite(store), Some(profile_id)) => {
            sqlx::query(
                "INSERT INTO resource_tenant_profile_assignments \
                    (tenant_id, resource_kind, resource_id, profile_id, updated_at) \
                 VALUES (?, ?, ?, ?, ?) \
                 ON CONFLICT(tenant_id, resource_kind, resource_id) DO UPDATE SET \
                    profile_id = excluded.profile_id, updated_at = excluded.updated_at",
            )
            .bind(tenant_id.to_string())
            .bind(resource_kind.as_str())
            .bind(resource_id)
            .bind(profile_id.to_string())
            .bind(Utc::now().to_rfc3339())
            .execute(store.pool())
            .await?;
        }
        (PlatformStore::Sqlite(store), None) => {
            sqlx::query(
                "DELETE FROM resource_tenant_profile_assignments \
                 WHERE tenant_id = ? AND resource_kind = ? AND resource_id = ?",
            )
            .bind(tenant_id.to_string())
            .bind(resource_kind.as_str())
            .bind(resource_id)
            .execute(store.pool())
            .await?;
        }
        (PlatformStore::Timescale(pool), Some(profile_id)) => {
            sqlx::query(
                "INSERT INTO resource_tenant_profile_assignments \
                    (tenant_id, resource_kind, resource_id, profile_id) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT(tenant_id, resource_kind, resource_id) DO UPDATE SET \
                    profile_id = EXCLUDED.profile_id, updated_at = now()",
            )
            .bind(tenant_id)
            .bind(resource_kind.as_str())
            .bind(resource_id)
            .bind(profile_id)
            .execute(pool)
            .await?;
        }
        (PlatformStore::Timescale(pool), None) => {
            sqlx::query(
                "DELETE FROM resource_tenant_profile_assignments \
                 WHERE tenant_id = $1 AND resource_kind = $2 AND resource_id = $3",
            )
            .bind(tenant_id)
            .bind(resource_kind.as_str())
            .bind(resource_id)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

async fn tenant_profile_assignment(
    store: &PlatformStore,
    tenant_id: Uuid,
    resource_kind: ApplicationDomainResourceKind,
    resource_id: &str,
) -> Result<Option<TenantProfileDefinition>, ApplicationDomainProfileError> {
    let profile_id = match store {
        PlatformStore::Sqlite(store) => sqlx::query_scalar::<_, String>(
            "SELECT profile_id FROM resource_tenant_profile_assignments \
             WHERE tenant_id = ? AND resource_kind = ? AND resource_id = ?",
        )
        .bind(tenant_id.to_string())
        .bind(resource_kind.as_str())
        .bind(resource_id)
        .fetch_optional(store.pool())
        .await?
        .map(|value| {
            value
                .parse()
                .map_err(|_| ApplicationDomainProfileError::InvalidStoredProfile)
        })
        .transpose()?,
        PlatformStore::Timescale(pool) => {
            sqlx::query_scalar::<_, Uuid>(
                "SELECT profile_id FROM resource_tenant_profile_assignments \
             WHERE tenant_id = $1 AND resource_kind = $2 AND resource_id = $3",
            )
            .bind(tenant_id)
            .bind(resource_kind.as_str())
            .bind(resource_id)
            .fetch_optional(pool)
            .await?
        }
    };
    let Some(profile_id) = profile_id else {
        return Ok(None);
    };
    let profile = tenant_profile_definition(store, tenant_id, profile_id).await?;
    if profile.resource_kind != resource_kind {
        return Err(ApplicationDomainProfileError::InvalidStoredProfile);
    }
    Ok(Some(profile))
}

async fn tenant_profile_definition(
    store: &PlatformStore,
    tenant_id: Uuid,
    profile_id: Uuid,
) -> Result<TenantProfileDefinition, ApplicationDomainProfileError> {
    export_tenant_profile_configuration(store, tenant_id)
        .await?
        .profiles
        .into_iter()
        .find(|profile| profile.id == profile_id)
        .ok_or(ApplicationDomainProfileError::ProfileNotFound)
}

fn validate_tenant_profile_configuration(
    configuration: &TenantProfileConfiguration,
) -> Result<(), ApplicationDomainProfileError> {
    if configuration.version != 1 {
        return Err(ApplicationDomainProfileError::UnsupportedConfigurationVersion);
    }
    if !configuration.permission_definitions.is_object() {
        return Err(ApplicationDomainProfileError::PermissionDefinitionsMustBeObject);
    }
    let mut profile_ids = HashSet::new();
    let mut names = HashSet::new();
    let mut kinds = HashMap::new();
    for profile in &configuration.profiles {
        if !profile_ids.insert(profile.id) {
            return Err(ApplicationDomainProfileError::InvalidStoredProfile);
        }
        if !names.insert((profile.resource_kind, profile.name.trim().to_owned())) {
            return Err(ApplicationDomainProfileError::NameConflict(
                profile.name.clone(),
            ));
        }
        validate_tenant_profile_definition(profile)?;
        kinds.insert(profile.id, profile.resource_kind);
    }
    for relation in &configuration.containment_rules {
        if relation.parent_profile_id == relation.child_profile_id {
            return Err(ApplicationDomainProfileError::AssetProfilesOnly);
        }
        let Some(parent_kind) = kinds.get(&relation.parent_profile_id) else {
            return Err(ApplicationDomainProfileError::UnknownContainedProfile);
        };
        let Some(child_kind) = kinds.get(&relation.child_profile_id) else {
            return Err(ApplicationDomainProfileError::UnknownContainedProfile);
        };
        if *parent_kind != ApplicationDomainResourceKind::Asset
            || *child_kind != ApplicationDomainResourceKind::Asset
        {
            return Err(ApplicationDomainProfileError::AssetProfilesOnly);
        }
    }
    Ok(())
}

fn validate_tenant_profile_definition(
    profile: &crate::TenantProfileDefinition,
) -> Result<(), ApplicationDomainProfileError> {
    if profile.name.trim().is_empty() || profile.name.chars().count() > 128 {
        return Err(ApplicationDomainProfileError::InvalidName);
    }
    if !profile.definition.is_object() {
        return Err(ApplicationDomainProfileError::DefinitionMustBeObject);
    }
    if !profile.live_view.is_object() {
        return Err(ApplicationDomainProfileError::LiveViewMustBeObject);
    }
    Ok(())
}
