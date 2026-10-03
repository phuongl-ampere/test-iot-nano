use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use iot_storage::{IdentityRepository, PlatformStore};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct OtaState {
    store: Arc<PlatformStore>,
}

#[derive(Deserialize)]
struct ManifestQuery {
    current_version: Option<String>,
}

#[derive(Deserialize)]
struct DownloadQuery {
    current_version: Option<String>,
}

#[derive(Serialize)]
struct NoUpdate {
    update: bool,
}

#[derive(Serialize)]
struct UpdateManifest {
    update: bool,
    artifact_id: String,
    version: String,
    sha256: String,
    size_bytes: u64,
    download_path: String,
}

pub(crate) fn router(store: Arc<PlatformStore>) -> Router {
    Router::new()
        .route("/api/v1/device/ota/manifest", get(manifest))
        .route("/api/v1/device/ota/artifacts/{artifact_id}", get(download))
        .with_state(OtaState { store })
}

async fn download(
    State(state): State<OtaState>,
    headers: HeaderMap,
    Path(artifact_id): Path<uuid::Uuid>,
    Query(query): Query<DownloadQuery>,
) -> Result<axum::response::Response, StatusCode> {
    validate_current_version(query.current_version.as_deref())?;
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let device = IdentityRepository::resolve_active_device_token(state.store.as_ref(), token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let profile_id =
        device_profile_id(state.store.as_ref(), device.tenant_id, &device.device_id).await?;
    let policy = state
        .store
        .ota_policy(device.tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let artifact = state
        .store
        .list_ota_artifacts(device.tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .find(|artifact| {
            artifact.id == artifact_id
                && (!policy.require_matching_device_profile
                    || artifact.device_profile_id == profile_id)
                && (!policy.require_newer_version
                    || is_newer(&artifact.version, query.current_version.as_deref()))
        })
        .ok_or(StatusCode::NOT_FOUND)?;
    let bytes = tokio::fs::read(&artifact.storage_path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    Ok((
        [
            (axum::http::header::CONTENT_TYPE, "application/octet-stream"),
            (axum::http::header::CONTENT_DISPOSITION, "attachment"),
        ],
        bytes,
    )
        .into_response())
}

async fn device_profile_id(
    store: &PlatformStore,
    tenant_id: uuid::Uuid,
    device_id: &str,
) -> Result<uuid::Uuid, StatusCode> {
    let raw = match store {
        PlatformStore::Sqlite(store) => sqlx::query_scalar::<_, Option<String>>("SELECT device_profile_id FROM devices WHERE tenant_id = ? AND device_id = ? AND deleted_at IS NULL")
            .bind(tenant_id.to_string()).bind(device_id).fetch_optional(store.pool()).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?.flatten(),
        PlatformStore::Timescale(pool) => sqlx::query_scalar::<_, Option<uuid::Uuid>>("SELECT device_profile_id FROM devices WHERE tenant_id = $1 AND device_id = $2 AND deleted_at IS NULL")
            .bind(tenant_id).bind(device_id).fetch_optional(pool).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?.flatten().map(|id| id.to_string()),
    }.ok_or(StatusCode::NOT_FOUND)?;
    uuid::Uuid::parse_str(&raw).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn manifest(
    State(state): State<OtaState>,
    headers: HeaderMap,
    Query(query): Query<ManifestQuery>,
) -> Result<axum::response::Response, StatusCode> {
    validate_current_version(query.current_version.as_deref())?;
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let device = IdentityRepository::resolve_active_device_token(state.store.as_ref(), token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let profile_id =
        device_profile_id(state.store.as_ref(), device.tenant_id, &device.device_id).await?;
    let policy = state
        .store
        .ota_policy(device.tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let artifact = state
        .store
        .list_ota_artifacts(device.tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .filter(|artifact| {
            !policy.require_matching_device_profile || artifact.device_profile_id == profile_id
        })
        .filter(|artifact| {
            !policy.require_newer_version
                || is_newer(&artifact.version, query.current_version.as_deref())
        })
        .max_by(|left, right| version_key(&left.version).cmp(&version_key(&right.version)));
    match artifact {
        Some(artifact) => Ok(Json(UpdateManifest {
            update: true,
            artifact_id: artifact.id.to_string(),
            version: artifact.version,
            sha256: artifact.sha256,
            size_bytes: artifact.size_bytes,
            download_path: format!("/api/v1/device/ota/artifacts/{}", artifact.id),
        })
        .into_response()),
        None => Ok(Json(NoUpdate { update: false }).into_response()),
    }
}

fn validate_current_version(value: Option<&str>) -> Result<(), StatusCode> {
    match value {
        Some(value) if version_key(value).is_none() => Err(StatusCode::BAD_REQUEST),
        _ => Ok(()),
    }
}

fn version_key(value: &str) -> Option<(u64, u64, u64)> {
    let mut parts = value.split('.').map(str::parse::<u64>);
    let version = (
        parts.next()?.ok()?,
        parts.next()?.ok()?,
        parts.next()?.ok()?,
    );
    if parts.next().is_some() {
        return None;
    }
    Some(version)
}

fn is_newer(candidate: &str, current: Option<&str>) -> bool {
    match (version_key(candidate), current.and_then(version_key)) {
        (Some(candidate), Some(current)) => candidate > current,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use iot_api::{TokenVault, provision_management_device_token};
    use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
    use iot_storage::{
        CreateManagementDeviceProfile, ManagementDeviceProfileRepository, OtaArtifact, OtaPolicy,
        PlatformStore,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tower::ServiceExt;
    use uuid::Uuid;

    use super::router;

    async fn fixture() -> (TempDir, Arc<PlatformStore>, axum::Router, String, Uuid) {
        let directory = tempfile::tempdir().unwrap();
        let store = PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("platform.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap();
        let tenant_id = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO tenants (id, slug, status, metadata) VALUES (?, 'ota-test', 'active', '{}')",
        )
        .bind(tenant_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
        let profile = ManagementDeviceProfileRepository::create_management_device_profile(
            &store,
            tenant_id,
            CreateManagementDeviceProfile {
                name: "OTA meter".to_owned(),
                telemetry_schema: json!({}),
                metric_mapping: json!({}),
                reporting_settings: json!({}),
            },
        )
        .await
        .unwrap();
        let device = provision_management_device_token(
            &store,
            &TokenVault::from_key_material("ota-router-test-vault-key-material-0001"),
            tenant_id,
            "OTA-TEST-001",
            "OTA meter",
            None,
            Some(profile.id),
            json!({}),
        )
        .await
        .unwrap();
        let artifact_path = directory.path().join("firmware.bin");
        tokio::fs::write(&artifact_path, b"firmware-v2")
            .await
            .unwrap();
        store
            .create_ota_artifact(&OtaArtifact {
                id: Uuid::now_v7(),
                tenant_id,
                device_profile_id: profile.id,
                version: "2.0.0".to_owned(),
                filename: "firmware.bin".to_owned(),
                storage_path: artifact_path.to_string_lossy().into_owned(),
                sha256: "a".repeat(64),
                size_bytes: 11,
            })
            .await
            .unwrap();
        let store = Arc::new(store);
        (
            directory,
            Arc::clone(&store),
            router(store),
            device.token.unwrap(),
            profile.id,
        )
    }

    #[tokio::test]
    async fn newer_version_policy_requires_a_valid_current_version_for_manifest_and_download() {
        let (_directory, _store, router, token, _profile_id) = fixture().await;

        let missing_current = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/device/ota/manifest")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_current.status(), StatusCode::OK);
        let manifest: serde_json::Value = serde_json::from_slice(
            &to_bytes(missing_current.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(manifest, json!({"update": false}));

        let malformed_current = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/device/ota/manifest?current_version=latest")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(malformed_current.status(), StatusCode::BAD_REQUEST);

        let current = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/device/ota/manifest?current_version=1.0.0")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(current.status(), StatusCode::OK);
        let manifest: serde_json::Value =
            serde_json::from_slice(&to_bytes(current.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(manifest["update"], true);
        let artifact_id = manifest["artifact_id"].as_str().unwrap();

        let missing_current_download = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/device/ota/artifacts/{artifact_id}"))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_current_download.status(), StatusCode::NOT_FOUND);

        let download = router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/v1/device/ota/artifacts/{artifact_id}?current_version=1.0.0"
                    ))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(download.into_body(), usize::MAX).await.unwrap(),
            "firmware-v2"
        );
    }

    #[tokio::test]
    async fn profile_policy_hides_mismatched_artifacts_until_tenant_disables_the_check() {
        let (_directory, store, router, _token, _profile_id) = fixture().await;
        let tenant_id = Uuid::parse_str(
            &sqlx::query_scalar::<_, String>("SELECT id FROM tenants WHERE slug = 'ota-test'")
                .fetch_one(store.sqlite_pool().unwrap())
                .await
                .unwrap(),
        )
        .unwrap();
        let other_profile = ManagementDeviceProfileRepository::create_management_device_profile(
            store.as_ref(),
            tenant_id,
            CreateManagementDeviceProfile {
                name: "Other meter".to_owned(),
                telemetry_schema: json!({}),
                metric_mapping: json!({}),
                reporting_settings: json!({}),
            },
        )
        .await
        .unwrap();
        let other_device = provision_management_device_token(
            store.as_ref(),
            &TokenVault::from_key_material("ota-router-test-vault-key-material-0001"),
            tenant_id,
            "OTA-TEST-002",
            "Other meter",
            None,
            Some(other_profile.id),
            json!({}),
        )
        .await
        .unwrap();
        let token = other_device.token.unwrap();

        let hidden = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/device/ota/manifest?current_version=1.0.0")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let hidden: serde_json::Value =
            serde_json::from_slice(&to_bytes(hidden.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(hidden, json!({"update": false}));

        store
            .set_ota_policy(
                tenant_id,
                OtaPolicy {
                    require_matching_device_profile: false,
                    require_newer_version: true,
                },
            )
            .await
            .unwrap();
        let offered = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/device/ota/manifest?current_version=1.0.0")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let offered: serde_json::Value =
            serde_json::from_slice(&to_bytes(offered.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(offered["update"], true);
    }
}
