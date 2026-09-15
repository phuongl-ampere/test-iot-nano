use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{ConnectInfo, FromRequest, Path, Request, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{COOKIE, SET_COOKIE},
    },
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use iot_api::{
    AuthError, DeviceTokenResponse, DeviceTokenStoreError, OAuthBrowserSessionVerifier,
    POWER_MONITOR_APP, Role, TokenVault, authenticate_credentials, authenticate_credentials_sqlite,
    create_platform_device_token, generate_session_id, hash_password,
    provision_platform_device_token, validate_password,
};
use iot_storage::{
    ApplicationKind, ApplicationRepository, ClientId, CreateManagementAsset,
    CreateManagementAssetProfile, CreateManagementDeviceProfile, CreateManagementUser,
    ManagementAsset as StorageManagementAsset, ManagementAssetError, ManagementAssetProfile,
    ManagementAssetProfileError, ManagementAssetProfileRepository, ManagementAssetRepository,
    ManagementChildStatus, ManagementDevice as StorageManagementDevice, ManagementDeviceError,
    ManagementDeviceProfile, ManagementDeviceProfileError, ManagementDeviceProfileRepository,
    ManagementDeviceRepository, ManagementDeviceTopology, ManagementGatewayStatus, ManagementUser,
    ManagementUserError, ManagementUserRepository, ManagementUserRole, NewApplication,
    NewOAuthClientSecret, OAuthRepository, PlatformStore, PlatformStoreError, RedirectUri,
    UpdateManagementAsset, UpdateManagementAssetProfile, UpdateManagementDevice,
    UpdateManagementDeviceProfile, UpdateManagementUser,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use thiserror::Error;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use tokio::sync::Notify;

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use tokio::sync::Barrier;

const SESSION_COOKIE: &str = "iot_nano_session";
const SESSION_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_LOGIN_FAILURES: u8 = 5;

#[derive(Debug, Error)]
pub enum BootstrapAdminError {
    #[error(
        "bootstrap admin username must use 3-64 ASCII letters, digits, hyphens, or underscores"
    )]
    InvalidUsername,
    #[error("bootstrap admin password is invalid")]
    InvalidPassword(#[source] AuthError),
    #[error("bootstrap admin can run only when the platform has no users")]
    AlreadyInitialized,
    #[error("platform store has no selected backend")]
    NoBackend,
    #[error("bootstrap admin storage operation failed")]
    Storage(#[source] sqlx::Error),
    #[error("bootstrap admin platform migration failed")]
    PlatformMigration(#[source] PlatformStoreError),
}

pub async fn bootstrap_admin(
    store: &PlatformStore,
    username: &str,
    password: &str,
) -> Result<(), BootstrapAdminError> {
    if !is_bootstrap_username(username) {
        return Err(BootstrapAdminError::InvalidUsername);
    }
    validate_password(password).map_err(BootstrapAdminError::InvalidPassword)?;
    let password_hash = hash_password(password).map_err(BootstrapAdminError::InvalidPassword)?;
    let user_id = Uuid::now_v7();
    if let Some(pool) = store.sqlite_pool() {
        return bootstrap_admin_sqlite(pool, user_id, username, &password_hash).await;
    }
    let pool = store
        .timescale_pool()
        .ok_or(BootstrapAdminError::NoBackend)?;
    bootstrap_admin_timescale(pool, user_id, username, &password_hash).await
}

async fn bootstrap_admin_sqlite(
    pool: &sqlx::SqlitePool,
    user_id: Uuid,
    username: &str,
    password_hash: &str,
) -> Result<(), BootstrapAdminError> {
    let mut transaction = pool
        .begin_with("BEGIN IMMEDIATE")
        .await
        .map_err(BootstrapAdminError::Storage)?;
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    if users != 0 {
        return Err(BootstrapAdminError::AlreadyInitialized);
    }
    sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app, created_at, updated_at
         ) VALUES (?1, ?2, ?3, 'admin', 'admin', '/apps/powermonitor', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(user_id.to_string())
    .bind(username)
    .bind(password_hash)
    .execute(&mut *transaction)
    .await
    .map_err(BootstrapAdminError::Storage)?;
    sqlx::query("INSERT INTO user_app_grants (user_id, app_key) VALUES (?1, ?2)")
        .bind(user_id.to_string())
        .bind(POWER_MONITOR_APP)
        .execute(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    transaction
        .commit()
        .await
        .map_err(BootstrapAdminError::Storage)
}

async fn bootstrap_admin_timescale(
    pool: &sqlx::PgPool,
    user_id: Uuid,
    username: &str,
    password_hash: &str,
) -> Result<(), BootstrapAdminError> {
    let mut transaction = pool.begin().await.map_err(BootstrapAdminError::Storage)?;
    sqlx::query("LOCK TABLE users IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    let users: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    if users != 0 {
        return Err(BootstrapAdminError::AlreadyInitialized);
    }
    sqlx::query(
        "INSERT INTO users (
            id, username, password_hash, role, account_class, default_app, created_at, updated_at
         ) VALUES ($1, $2, $3, 'admin', 'admin', '/apps/powermonitor', now(), now())",
    )
    .bind(user_id)
    .bind(username)
    .bind(password_hash)
    .execute(&mut *transaction)
    .await
    .map_err(BootstrapAdminError::Storage)?;
    sqlx::query("INSERT INTO user_app_grants (user_id, app_key) VALUES ($1, $2)")
        .bind(user_id)
        .bind(POWER_MONITOR_APP)
        .execute(&mut *transaction)
        .await
        .map_err(BootstrapAdminError::Storage)?;
    transaction
        .commit()
        .await
        .map_err(BootstrapAdminError::Storage)
}

fn is_bootstrap_username(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[derive(Clone)]
pub struct ManagementSessionRouter {
    pub router: Router,
    pub session_verifier: Arc<ManagementSessionVerifier>,
}

impl ManagementSessionRouter {
    pub fn new(store: Arc<PlatformStore>, token_vault: TokenVault) -> Self {
        let session_verifier = Arc::new(ManagementSessionVerifier::default());
        let state = ManagementState {
            store,
            session_verifier: Arc::clone(&session_verifier),
            token_vault,
            login_limiter: Arc::new(Mutex::new(LoginRateLimiter::default())),
            authorization_gate: Arc::new(ManagementAuthorizationGate::default()),
            #[cfg(test)]
            authorization_test_hooks: None,
        };
        let router = Router::new()
            .route("/api/auth/login", post(login))
            .route("/api/auth/logout", post(logout))
            .route("/api/auth/me", get(current_session))
            .route("/api/management/applications", post(create_application))
            .route(
                "/api/management/users",
                get(list_management_users).post(create_management_user),
            )
            .route(
                "/api/management/users/{username}",
                put(update_management_user),
            )
            .route(
                "/api/management/profiles/device-profiles",
                get(list_management_device_profiles).post(create_management_device_profile),
            )
            .route(
                "/api/management/profiles/device-profiles/{profile_id}",
                put(update_management_device_profile).delete(delete_management_device_profile),
            )
            .route(
                "/api/management/profiles/asset-profiles",
                get(list_management_asset_profiles).post(create_management_asset_profile),
            )
            .route(
                "/api/management/profiles/asset-profiles/{profile_id}",
                put(update_management_asset_profile).delete(delete_management_asset_profile),
            )
            .route(
                "/api/management/devices",
                get(list_management_devices).post(provision_device),
            )
            .route(
                "/api/management/devices/{device_id}",
                put(update_management_device).delete(delete_management_device),
            )
            .route(
                "/api/management/assets",
                get(list_management_assets).post(create_management_asset),
            )
            .route(
                "/api/management/assets/{asset_id}",
                put(update_management_asset).delete(delete_management_asset),
            )
            .route(
                "/api/management/devices/{device_id}/tokens",
                post(create_device_token),
            )
            .with_state(state)
            .merge(
                SwaggerUi::new("/docs/")
                    .external_url_unchecked("/api-docs/openapi.json", management_openapi()),
            );
        Self {
            router,
            session_verifier,
        }
    }
}

fn management_openapi() -> Value {
    let mut paths = Map::new();

    documented_path(
        &mut paths,
        "/api/auth/login",
        vec![(
            "post",
            documented_operation(
                "Start a management session",
                None,
                Some(("application/json", "LoginRequest")),
                ("200", "Management session started", Some("SessionResponse")),
                &[
                    ("401", "Invalid credentials"),
                    ("429", "Too many attempts"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/auth/logout",
        vec![(
            "post",
            documented_operation(
                "End the current management session",
                None,
                None,
                ("204", "Management session ended", None),
                &[],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/auth/me",
        vec![(
            "get",
            documented_operation(
                "Get the current management session",
                Some("managementSession"),
                None,
                ("200", "Current session", Some("SessionResponse")),
                &[("401", "No active management session")],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/applications",
        vec![(
            "post",
            management_operation(
                "Create or update an OAuth application",
                "ApplicationRequest",
                "201",
                "ApplicationResponse",
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/users",
        vec![
            (
                "get",
                management_list_operation("List management users", "ManagementUserList"),
            ),
            (
                "post",
                management_operation(
                    "Create a management user",
                    "ManagementUserCreateRequest",
                    "201",
                    "ManagementUser",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/users/{username}",
        vec![(
            "put",
            management_operation(
                "Update a management user",
                "ManagementUserUpdateRequest",
                "200",
                "ManagementUser",
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/device-profiles",
        vec![
            (
                "get",
                management_list_operation("List device profiles", "DeviceProfileList"),
            ),
            (
                "post",
                management_operation(
                    "Create a device profile",
                    "DeviceProfileRequest",
                    "201",
                    "DeviceProfile",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/device-profiles/{profile_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update a device profile",
                    "DeviceProfileRequest",
                    "200",
                    "DeviceProfile",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete a device profile"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/asset-profiles",
        vec![
            (
                "get",
                management_list_operation("List asset profiles", "AssetProfileList"),
            ),
            (
                "post",
                management_operation(
                    "Create an asset profile",
                    "AssetProfileRequest",
                    "201",
                    "AssetProfile",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/asset-profiles/{profile_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update an asset profile",
                    "AssetProfileRequest",
                    "200",
                    "AssetProfile",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete an asset profile"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/devices",
        vec![
            (
                "get",
                management_list_operation("List management devices", "ManagementDeviceList"),
            ),
            (
                "post",
                management_operation(
                    "Provision a device",
                    "DeviceProvisionRequest",
                    "201",
                    "DeviceToken",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update a management device",
                    "ManagementDeviceUpdateRequest",
                    "200",
                    "ManagementDevice",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete a management device"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/tokens",
        vec![(
            "post",
            documented_operation(
                "Issue a device token",
                Some("managementSession"),
                None,
                ("201", "Device token issued", Some("DeviceToken")),
                &[
                    ("400", "Invalid device identifier"),
                    ("401", "No active management session"),
                    ("403", "Administrator role required"),
                    ("404", "Device not found"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/assets",
        vec![
            (
                "get",
                management_list_operation("List management assets", "ManagementAssetList"),
            ),
            (
                "post",
                management_operation(
                    "Create a management asset",
                    "ManagementAssetRequest",
                    "201",
                    "ManagementAsset",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/assets/{asset_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update a management asset",
                    "ManagementAssetRequest",
                    "200",
                    "ManagementAsset",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete a management asset"),
            ),
        ],
    );

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "IoT Nano management API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Management and authentication operations."
        },
        "paths": paths,
        "components": {
            "securitySchemes": {
                "managementSession": {
                    "type": "apiKey",
                    "in": "cookie",
                    "name": SESSION_COOKIE
                }
            },
            "schemas": management_openapi_schemas()
        }
    })
}

fn documented_path(paths: &mut Map<String, Value>, path: &str, operations: Vec<(&str, Value)>) {
    let mut item = Map::new();
    for (method, operation) in operations {
        item.insert(method.to_owned(), operation);
    }
    let parameters = path_parameters(path);
    if !parameters.is_empty() {
        item.insert("parameters".to_owned(), Value::Array(parameters));
    }
    paths.insert(path.to_owned(), Value::Object(item));
}

fn path_parameters(path: &str) -> Vec<Value> {
    path.split('/')
        .filter_map(|segment| {
            segment
                .strip_prefix('{')
                .and_then(|segment| segment.strip_suffix('}'))
        })
        .map(|name| {
            json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            })
        })
        .collect()
}

fn management_operation(
    summary: &str,
    request_schema: &str,
    success_status: &str,
    response_schema: &str,
) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        Some(("application/json", request_schema)),
        (success_status, "Request completed", Some(response_schema)),
        &management_errors(),
    )
}

fn management_list_operation(summary: &str, response_schema: &str) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        None,
        ("200", "Request completed", Some(response_schema)),
        &management_errors(),
    )
}

fn management_no_content_operation(summary: &str) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        None,
        ("204", "Request completed", None),
        &management_errors(),
    )
}

fn management_errors() -> [(&'static str, &'static str); 6] {
    [
        ("400", "Invalid request"),
        ("401", "No active management session"),
        ("403", "Administrator role required"),
        ("404", "Resource not found"),
        ("409", "Conflicting request"),
        ("503", "Service unavailable"),
    ]
}

fn documented_operation(
    summary: &str,
    security: Option<&str>,
    request: Option<(&str, &str)>,
    success: (&str, &str, Option<&str>),
    errors: &[(&str, &str)],
) -> Value {
    let mut operation = Map::new();
    operation.insert("summary".to_owned(), Value::String(summary.to_owned()));
    if let Some((content_type, schema)) = request {
        operation.insert(
            "requestBody".to_owned(),
            json!({
                "required": true,
                "content": {content_type: {"schema": schema_reference(schema)}}
            }),
        );
    }
    if let Some(scheme) = security {
        let mut requirement = Map::new();
        requirement.insert(scheme.to_owned(), json!([]));
        operation.insert(
            "security".to_owned(),
            Value::Array(vec![Value::Object(requirement)]),
        );
    }
    let mut responses = Map::new();
    responses.insert(success.0.to_owned(), response_value(success.1, success.2));
    for (status, description) in errors {
        responses.insert(
            (*status).to_owned(),
            response_value(description, Some("Error")),
        );
    }
    operation.insert("responses".to_owned(), Value::Object(responses));
    Value::Object(operation)
}

fn response_value(description: &str, schema: Option<&str>) -> Value {
    let mut response = Map::new();
    response.insert(
        "description".to_owned(),
        Value::String(description.to_owned()),
    );
    if let Some(schema) = schema {
        response.insert(
            "content".to_owned(),
            json!({"application/json": {"schema": schema_reference(schema)}}),
        );
    }
    Value::Object(response)
}

fn schema_reference(schema: &str) -> Value {
    json!({"$ref": format!("#/components/schemas/{schema}")})
}

fn management_openapi_schemas() -> Value {
    let mut schemas = Map::new();
    schemas.insert(
        "Error".to_owned(),
        object_schema(json!({"error": {"type": "string"}}), &["error"]),
    );
    schemas.insert(
        "SessionResponse".to_owned(),
        object_schema(json!({"user_id": uuid_schema()}), &["user_id"]),
    );
    schemas.insert(
        "LoginRequest".to_owned(),
        object_schema(
            json!({
                "username": {"type": "string"},
                "password": {"type": "string", "format": "password"}
            }),
            &["username", "password"],
        ),
    );
    schemas.insert(
        "ApplicationRequest".to_owned(),
        object_schema(
            json!({
                "app_id": {"type": "string"},
                "kind": {"type": "string"},
                "launch_url": {"type": "string", "format": "uri"},
                "client_id": {"type": "string"},
                "redirect_uris": {"type": "array", "items": {"type": "string", "format": "uri"}},
                "allowed_scopes": {"type": "array", "items": {"type": "string"}},
                "enabled": {"type": "boolean"}
            }),
            &[
                "app_id",
                "kind",
                "launch_url",
                "client_id",
                "redirect_uris",
                "allowed_scopes",
                "enabled",
            ],
        ),
    );
    schemas.insert(
        "ApplicationResponse".to_owned(),
        object_schema(
            json!({"app_id": {"type": "string"}, "client_id": {"type": "string"}}),
            &["app_id", "client_id"],
        ),
    );
    schemas.insert(
        "ManagementUserCreateRequest".to_owned(),
        object_schema(
            json!({
                "username": {"type": "string"},
                "password": {"type": "string", "format": "password"},
                "default_app": {"type": "string"},
                "granted_apps": {"type": "array", "items": {"type": "string"}}
            }),
            &["username", "password", "default_app", "granted_apps"],
        ),
    );
    schemas.insert(
        "ManagementUserUpdateRequest".to_owned(),
        object_schema(
            json!({
                "default_app": {"type": "string"},
                "granted_apps": {"type": "array", "items": {"type": "string"}},
                "role": {"type": "string"}
            }),
            &["default_app", "granted_apps"],
        ),
    );
    schemas.insert(
        "ManagementUser".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema(),
                "username": {"type": "string"},
                "role": {"type": "string"},
                "account_class": {"type": "string"},
                "default_app": {"type": "string"},
                "granted_apps": {"type": "array", "items": {"type": "string"}}
            }),
            &[
                "id",
                "username",
                "role",
                "account_class",
                "default_app",
                "granted_apps",
            ],
        ),
    );
    schemas.insert(
        "DeviceProfileRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "telemetry_schema": json_object_schema(),
                "metric_mapping": json_object_schema(),
                "reporting_settings": json_object_schema()
            }),
            &[
                "name",
                "telemetry_schema",
                "metric_mapping",
                "reporting_settings",
            ],
        ),
    );
    schemas.insert(
        "DeviceProfile".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema(),
                "name": {"type": "string"},
                "telemetry_schema": json_object_schema(),
                "metric_mapping": json_object_schema(),
                "reporting_settings": json_object_schema()
            }),
            &[
                "id",
                "name",
                "telemetry_schema",
                "metric_mapping",
                "reporting_settings",
            ],
        ),
    );
    schemas.insert(
        "AssetProfileRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "fields": json_object_schema(),
                "dashboard_defaults": json_object_schema()
            }),
            &["name", "fields", "dashboard_defaults"],
        ),
    );
    schemas.insert(
        "AssetProfile".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema(),
                "name": {"type": "string"},
                "fields": json_object_schema(),
                "dashboard_defaults": json_object_schema()
            }),
            &["id", "name", "fields", "dashboard_defaults"],
        ),
    );
    schemas.insert(
        "DeviceProvisionRequest".to_owned(),
        object_schema(
            json!({"display_name": {"type": "string"}}),
            &["display_name"],
        ),
    );
    schemas.insert(
        "ManagementDeviceUpdateRequest".to_owned(),
        object_schema(
            json!({
                "display_name": {"type": "string"},
                "asset_id": uuid_schema(),
                "device_profile_id": uuid_schema(),
                "attributes": json_object_schema(),
                "topology": json_object_schema()
            }),
            &["display_name"],
        ),
    );
    schemas.insert(
        "ManagementDevice".to_owned(),
        object_schema(
            json!({
                "device_id": {"type": "string"},
                "display_name": {"type": ["string", "null"]},
                "asset_id": uuid_schema(),
                "device_profile_id": uuid_schema(),
                "attributes": json_object_schema(),
                "online": {"type": "boolean"},
                "last_seen_at": {"type": ["string", "null"], "format": "date-time"},
                "is_gateway": {"type": "boolean"},
                "gateway_device_id": {"type": ["string", "null"]},
                "gateway_status": {"type": ["string", "null"]},
                "child_status": {"type": ["string", "null"]}
            }),
            &["device_id", "attributes", "online", "is_gateway"],
        ),
    );
    schemas.insert(
        "DeviceToken".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema(),
                "device_id": {"type": "string"},
                "token_prefix": {"type": "string"},
                "token": {"type": "string"}
            }),
            &["id", "device_id", "token_prefix"],
        ),
    );
    schemas.insert(
        "ManagementAssetRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "asset_profile_id": uuid_schema(),
                "parent_asset_id": uuid_schema(),
                "metadata": json_object_schema(),
                "attributes": json_object_schema()
            }),
            &["name", "metadata"],
        ),
    );
    schemas.insert(
        "ManagementAsset".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema(),
                "name": {"type": "string"},
                "asset_profile_id": uuid_schema(),
                "parent_asset_id": uuid_schema(),
                "metadata": json_object_schema(),
                "attributes": json_object_schema()
            }),
            &["id", "name", "metadata", "attributes"],
        ),
    );

    for (name, item) in [
        ("ManagementUserList", "ManagementUser"),
        ("DeviceProfileList", "DeviceProfile"),
        ("AssetProfileList", "AssetProfile"),
        ("ManagementDeviceList", "ManagementDevice"),
        ("ManagementAssetList", "ManagementAsset"),
    ] {
        schemas.insert(name.to_owned(), array_schema(item));
    }

    Value::Object(schemas)
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn array_schema(item: &str) -> Value {
    json!({"type": "array", "items": schema_reference(item)})
}

fn json_object_schema() -> Value {
    json!({"type": "object", "additionalProperties": true})
}

fn uuid_schema() -> Value {
    json!({"type": ["string", "null"], "format": "uuid"})
}

#[derive(Default)]
pub struct ManagementSessionVerifier {
    sessions: Mutex<HashMap<String, Session>>,
}

impl ManagementSessionVerifier {
    fn issue(&self, user_id: Uuid, role: Role) -> String {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        loop {
            let session_id = generate_session_id();
            if !sessions.contains_key(&session_id) {
                sessions.insert(
                    session_id.clone(),
                    Session {
                        user_id,
                        role: Some(role),
                        expires_at: Instant::now() + SESSION_TTL,
                    },
                );
                return session_id;
            }
        }
    }

    fn revoke(&self, headers: &HeaderMap) {
        let Some(session_id) = session_id(headers) else {
            return;
        };
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(session_id);
    }

    fn authorization(&self, headers: &HeaderMap) -> ManagementAuthorization {
        let Some(session_id) = session_id(headers) else {
            return ManagementAuthorization::Unauthenticated;
        };
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        let role = sessions.get(session_id).map(|session| session.role);
        match role {
            Some(Some(Role::Admin)) => ManagementAuthorization::Admin,
            Some(_) => ManagementAuthorization::Forbidden,
            None => ManagementAuthorization::Unauthenticated,
        }
    }

    fn invalidate_user(&self, user_id: Uuid) {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        for session in sessions.values_mut() {
            if session.user_id == user_id {
                session.role = None;
            }
        }
    }
}

impl OAuthBrowserSessionVerifier for ManagementSessionVerifier {
    fn authenticated_user_id(&self, headers: &HeaderMap) -> Option<Uuid> {
        let session_id = session_id(headers)?;
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_sessions(&mut sessions);
        sessions.get(session_id).map(|session| session.user_id)
    }
}

#[derive(Clone)]
struct ManagementState {
    store: Arc<PlatformStore>,
    session_verifier: Arc<ManagementSessionVerifier>,
    token_vault: TokenVault,
    login_limiter: Arc<Mutex<LoginRateLimiter>>,
    authorization_gate: Arc<ManagementAuthorizationGate>,
    #[cfg(test)]
    authorization_test_hooks: Option<Arc<ManagementAuthorizationTestHooks>>,
}

#[derive(Default)]
struct ManagementAuthorizationGate {
    state: Mutex<ManagementAuthorizationGateState>,
    changed: Notify,
}

#[derive(Default)]
struct ManagementAuthorizationGateState {
    active_mutations: usize,
    role_change_in_progress: bool,
    role_version: u64,
}

impl ManagementAuthorizationGate {
    async fn stable_role_version(&self) -> u64 {
        loop {
            let notified = self.changed.notified();
            let role_version = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                (!state.role_change_in_progress).then_some(state.role_version)
            };
            if let Some(role_version) = role_version {
                return role_version;
            }
            notified.await;
        }
    }

    fn issue_if_current(
        &self,
        expected_role_version: u64,
        session_verifier: &ManagementSessionVerifier,
        user_id: Uuid,
        role: Role,
    ) -> Option<String> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.role_change_in_progress || state.role_version != expected_role_version {
            return None;
        }
        Some(session_verifier.issue(user_id, role))
    }

    async fn acquire_mutation(&self) -> ManagementMutationLease<'_> {
        loop {
            let notified = self.changed.notified();
            let acquired = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !state.role_change_in_progress {
                    state.active_mutations = state.active_mutations.saturating_add(1);
                    true
                } else {
                    false
                }
            };
            if acquired {
                return ManagementMutationLease { gate: self };
            }
            notified.await;
        }
    }

    async fn begin_role_change(&self) -> ManagementRoleChangeLease<'_> {
        loop {
            let notified = self.changed.notified();
            let acquired = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !state.role_change_in_progress {
                    state.role_change_in_progress = true;
                    true
                } else {
                    false
                }
            };
            if acquired {
                break;
            }
            notified.await;
        }

        loop {
            let notified = self.changed.notified();
            let drained = {
                self.state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .active_mutations
                    == 0
            };
            if drained {
                return ManagementRoleChangeLease {
                    gate: self,
                    finished: false,
                };
            }
            notified.await;
        }
    }

    fn finish_role_change(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .role_change_in_progress = false;
        self.changed.notify_waiters();
    }
}

struct ManagementMutationLease<'a> {
    gate: &'a ManagementAuthorizationGate,
}

impl Drop for ManagementMutationLease<'_> {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_mutations = state.active_mutations.saturating_sub(1);
        let notify = state.active_mutations == 0;
        drop(state);
        if notify {
            self.gate.changed.notify_waiters();
        }
    }
}

struct ManagementRoleChangeLease<'a> {
    gate: &'a ManagementAuthorizationGate,
    finished: bool,
}

impl ManagementRoleChangeLease<'_> {
    fn commit(mut self, session_verifier: &ManagementSessionVerifier, user_id: Uuid) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.role_version = state.role_version.wrapping_add(1);
        session_verifier.invalidate_user(user_id);
        state.role_change_in_progress = false;
        self.finished = true;
        drop(state);
        self.gate.changed.notify_waiters();
    }
}

impl Drop for ManagementRoleChangeLease<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.gate.finish_role_change();
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
struct ManagementAuthorizationTestHooks {
    login_authenticated: Arc<Barrier>,
    release_login: Arc<Barrier>,
    mutation_authorized: Arc<Barrier>,
    release_mutation: Arc<Barrier>,
    pause_login: Arc<AtomicBool>,
    pause_mutation: Arc<AtomicBool>,
}

struct Session {
    user_id: Uuid,
    role: Option<Role>,
    expires_at: Instant,
}

enum ManagementAuthorization {
    Unauthenticated,
    Admin,
    Forbidden,
}

#[derive(Default)]
struct LoginRateLimiter {
    attempts: HashMap<IpAddr, LoginAttempt>,
}

struct LoginAttempt {
    failures: u8,
    in_flight: u8,
    started_at: Instant,
}

impl LoginRateLimiter {
    fn reserve(&mut self, address: IpAddr) -> bool {
        self.attempts
            .retain(|_, attempt| attempt.started_at.elapsed() < LOGIN_WINDOW);
        let attempt = self.attempts.entry(address).or_insert(LoginAttempt {
            failures: 0,
            in_flight: 0,
            started_at: Instant::now(),
        });
        if attempt.failures.saturating_add(attempt.in_flight) >= MAX_LOGIN_FAILURES {
            return false;
        }
        attempt.in_flight = attempt.in_flight.saturating_add(1);
        true
    }

    fn record_failure(&mut self, address: IpAddr) {
        if let Some(attempt) = self.attempts.get_mut(&address) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures = attempt.failures.saturating_add(1);
        }
    }

    fn record_success(&mut self, address: IpAddr) {
        let remove = if let Some(attempt) = self.attempts.get_mut(&address) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures = 0;
            attempt.in_flight == 0
        } else {
            false
        };
        if remove {
            self.attempts.remove(&address);
        }
    }

    fn release(&mut self, address: IpAddr) {
        let remove = if let Some(attempt) = self.attempts.get_mut(&address) {
            attempt.in_flight = attempt.in_flight.saturating_sub(1);
            attempt.failures == 0 && attempt.in_flight == 0
        } else {
            false
        };
        if remove {
            self.attempts.remove(&address);
        }
    }
}

#[derive(Deserialize)]
struct LoginRequest {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct SessionResponse {
    user_id: Uuid,
}

#[derive(Deserialize)]
struct CreateApplicationRequest {
    app_id: String,
    kind: String,
    launch_url: String,
    client_id: String,
    redirect_uris: Vec<String>,
    allowed_scopes: Vec<String>,
    enabled: bool,
    client_secret: Option<String>,
}

#[derive(Serialize)]
struct ApplicationResponse {
    app_id: String,
    client_id: String,
}

#[derive(Deserialize)]
struct CreateManagementUserRequest {
    username: String,
    password: String,
    default_app: String,
    granted_apps: Vec<String>,
}

#[derive(Deserialize)]
struct UpdateManagementUserRequest {
    default_app: String,
    granted_apps: Vec<String>,
    #[serde(default)]
    role: Option<String>,
}

#[derive(Serialize)]
struct ManagementUserResponse {
    id: Uuid,
    username: String,
    role: String,
    account_class: String,
    default_app: String,
    granted_apps: Vec<String>,
}

#[derive(Deserialize)]
struct ManagementDeviceProfileRequest {
    name: String,
    telemetry_schema: Value,
    metric_mapping: Value,
    reporting_settings: Value,
}

#[derive(Serialize)]
struct ManagementDeviceProfileResponse {
    id: Uuid,
    name: String,
    telemetry_schema: Value,
    metric_mapping: Value,
    reporting_settings: Value,
}

#[derive(Deserialize)]
struct ManagementAssetProfileRequest {
    name: String,
    fields: Value,
    dashboard_defaults: Value,
}

#[derive(Serialize)]
struct ManagementAssetProfileResponse {
    id: Uuid,
    name: String,
    fields: Value,
    dashboard_defaults: Value,
}

#[derive(Deserialize)]
struct ProvisionDeviceRequest {
    display_name: String,
}

#[derive(Deserialize)]
struct UpdateManagementDeviceRequest {
    display_name: String,
    #[serde(default)]
    asset_id: Option<Uuid>,
    #[serde(default)]
    device_profile_id: Option<Uuid>,
    #[serde(default)]
    attributes: Option<Value>,
    #[serde(default)]
    topology: Option<ManagementTopologyRequest>,
}

#[derive(Deserialize)]
struct ManagementAssetRequest {
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: Value,
    attributes: Option<Value>,
}

#[derive(Serialize)]
struct ManagementAssetResponse {
    id: Uuid,
    name: String,
    asset_profile_id: Option<Uuid>,
    parent_asset_id: Option<Uuid>,
    metadata: Value,
    attributes: Value,
}

#[derive(Deserialize)]
struct ManagementTopologyRequest {
    is_gateway: bool,
    #[serde(default)]
    gateway_device_id: Option<String>,
}

#[derive(Serialize)]
struct ManagementDeviceResponse {
    device_id: String,
    display_name: Option<String>,
    asset_id: Option<Uuid>,
    device_profile_id: Option<Uuid>,
    attributes: Value,
    online: bool,
    last_seen_at: Option<chrono::DateTime<chrono::Utc>>,
    is_gateway: bool,
    gateway_device_id: Option<String>,
    gateway_status: Option<String>,
    child_status: Option<String>,
}

async fn login(
    State(state): State<ManagementState>,
    ConnectInfo(address): ConnectInfo<SocketAddr>,
    Json(request): Json<LoginRequest>,
) -> Result<(HeaderMap, Json<SessionResponse>), ManagementSessionError> {
    let address = address.ip();
    let reserved = {
        let mut limiter = state
            .login_limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        limiter.reserve(address)
    };
    if !reserved {
        return Err(ManagementSessionError::TooManyRequests);
    }
    let (session_id, user_id) = loop {
        let role_version = state.authorization_gate.stable_role_version().await;
        let user = match authenticate(&state.store, &request).await {
            Ok(user) => user,
            Err(AuthError::AuthenticationFailed) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .record_failure(address);
                return Err(ManagementSessionError::Unauthorized);
            }
            Err(_) => {
                state
                    .login_limiter
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .release(address);
                return Err(ManagementSessionError::Unavailable);
            }
        };
        #[cfg(test)]
        pause_after_login_authentication(&state).await;
        if let Some(session_id) = state.authorization_gate.issue_if_current(
            role_version,
            &state.session_verifier,
            user.user_id,
            user.role,
        ) {
            break (session_id, user.user_id);
        }
    };
    state
        .login_limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_success(address);
    Ok((
        session_cookie_headers(&session_id),
        Json(SessionResponse { user_id }),
    ))
}

async fn logout(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> (StatusCode, HeaderMap) {
    state.session_verifier.revoke(&headers);
    (StatusCode::NO_CONTENT, expired_session_cookie_headers())
}

async fn current_session(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<SessionResponse>, ManagementSessionError> {
    let user_id = state
        .session_verifier
        .authenticated_user_id(&headers)
        .ok_or(ManagementSessionError::Unauthorized)?;
    Ok(Json(SessionResponse { user_id }))
}

async fn create_application(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ApplicationResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: CreateApplicationRequest = management_request_json(&state, request).await?;
    let app_id = request
        .app_id
        .parse()
        .map_err(|_| ManagementSessionError::BadRequest)?;
    let kind =
        ApplicationKind::from_str(&request.kind).map_err(|_| ManagementSessionError::BadRequest)?;
    let client_id =
        ClientId::from_str(&request.client_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let redirect_uris = request
        .redirect_uris
        .into_iter()
        .map(|value| RedirectUri::from_str(&value).map_err(|_| ManagementSessionError::BadRequest))
        .collect::<Result<Vec<_>, _>>()?;
    if request.launch_url.is_empty() || request.allowed_scopes.is_empty() {
        return Err(ManagementSessionError::BadRequest);
    }
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let application = ApplicationRepository::upsert_application(
        state.store.as_ref(),
        NewApplication {
            app_id,
            kind,
            launch_url: request.launch_url,
            client_id,
            redirect_uris,
            allowed_scopes: request.allowed_scopes,
            enabled: request.enabled,
        },
    )
    .await
    .map_err(|_| ManagementSessionError::Unavailable)?;
    if let Some(client_secret) = request.client_secret {
        if client_secret.is_empty() {
            return Err(ManagementSessionError::BadRequest);
        }
        OAuthRepository::register_client_secret(
            state.store.as_ref(),
            NewOAuthClientSecret {
                app_id: application.app_id.clone(),
                client_secret,
            },
        )
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    }
    Ok((
        StatusCode::CREATED,
        Json(ApplicationResponse {
            app_id: application.app_id.as_str().to_owned(),
            client_id: application.client_id.as_str().to_owned(),
        }),
    ))
}

async fn provision_device(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: ProvisionDeviceRequest = management_request_json(&state, request).await?;
    let display_name = request.display_name.trim();
    if display_name.is_empty() || display_name.len() > 128 {
        return Err(ManagementSessionError::BadRequest);
    }
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let token = provision_platform_device_token(&state.store, &state.token_vault, display_name)
        .await
        .map_err(|_| ManagementSessionError::Unavailable)?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn list_management_users(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementUserResponse>>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementUserRepository::list_management_users(state.store.as_ref())
        .await
        .map(|users| Json(users.into_iter().map(management_user_response).collect()))
        .map_err(management_user_error)
}

async fn create_management_user(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementUserResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    #[cfg(test)]
    pause_after_mutation_authorization(&state).await;
    let request: CreateManagementUserRequest = management_request_json(&state, request).await?;
    validate_password(&request.password).map_err(|_| ManagementSessionError::BadRequest)?;
    let password_hash =
        hash_password(&request.password).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let user = ManagementUserRepository::create_management_user(
        state.store.as_ref(),
        CreateManagementUser {
            username: request.username,
            password_hash,
            default_app: request.default_app,
            granted_apps: request.granted_apps,
        },
    )
    .await
    .map_err(management_user_error)?;
    Ok((StatusCode::CREATED, Json(management_user_response(user))))
}

async fn update_management_user(
    State(state): State<ManagementState>,
    Path(username): Path<String>,
    request: Request,
) -> Result<Json<ManagementUserResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: UpdateManagementUserRequest = management_request_json(&state, request).await?;
    let role = request
        .role
        .as_deref()
        .map(management_user_role)
        .transpose()?;
    let update = UpdateManagementUser {
        default_app: request.default_app,
        granted_apps: request.granted_apps,
        role,
    };
    if update.role.is_some() {
        let role_change = state.authorization_gate.begin_role_change().await;
        require_management_admin(&state.session_verifier, &headers)?;
        let user = ManagementUserRepository::update_management_user(
            state.store.as_ref(),
            &username,
            update,
        )
        .await
        .map_err(management_user_error)?;
        role_change.commit(&state.session_verifier, user.id);
        return Ok(Json(management_user_response(user)));
    }
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let user =
        ManagementUserRepository::update_management_user(state.store.as_ref(), &username, update)
            .await
            .map_err(management_user_error)?;
    Ok(Json(management_user_response(user)))
}

async fn list_management_device_profiles(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementDeviceProfileResponse>>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementDeviceProfileRepository::list_management_device_profiles(state.store.as_ref())
        .await
        .map(|profiles| {
            Json(
                profiles
                    .into_iter()
                    .map(management_device_profile_response)
                    .collect(),
            )
        })
        .map_err(management_device_profile_error)
}

async fn create_management_device_profile(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementDeviceProfileResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: ManagementDeviceProfileRequest = management_request_json(&state, request).await?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let profile = ManagementDeviceProfileRepository::create_management_device_profile(
        state.store.as_ref(),
        CreateManagementDeviceProfile {
            name: request.name,
            telemetry_schema: request.telemetry_schema,
            metric_mapping: request.metric_mapping,
            reporting_settings: request.reporting_settings,
        },
    )
    .await
    .map_err(management_device_profile_error)?;
    Ok((
        StatusCode::CREATED,
        Json(management_device_profile_response(profile)),
    ))
}

async fn update_management_device_profile(
    State(state): State<ManagementState>,
    Path(profile_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementDeviceProfileResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: ManagementDeviceProfileRequest = management_request_json(&state, request).await?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let profile = ManagementDeviceProfileRepository::update_management_device_profile(
        state.store.as_ref(),
        profile_id,
        UpdateManagementDeviceProfile {
            name: request.name,
            telemetry_schema: request.telemetry_schema,
            metric_mapping: request.metric_mapping,
            reporting_settings: request.reporting_settings,
        },
    )
    .await
    .map_err(management_device_profile_error)?;
    Ok(Json(management_device_profile_response(profile)))
}

async fn delete_management_device_profile(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    ManagementDeviceProfileRepository::delete_management_device_profile(
        state.store.as_ref(),
        profile_id,
    )
    .await
    .map_err(management_device_profile_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_management_asset_profiles(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAssetProfileResponse>>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementAssetProfileRepository::list_management_asset_profiles(state.store.as_ref())
        .await
        .map(|profiles| {
            Json(
                profiles
                    .into_iter()
                    .map(management_asset_profile_response)
                    .collect(),
            )
        })
        .map_err(management_asset_profile_error)
}

async fn create_management_asset_profile(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementAssetProfileResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: ManagementAssetProfileRequest = management_request_json(&state, request).await?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let profile = ManagementAssetProfileRepository::create_management_asset_profile(
        state.store.as_ref(),
        CreateManagementAssetProfile {
            name: request.name,
            fields: request.fields,
            dashboard_defaults: request.dashboard_defaults,
        },
    )
    .await
    .map_err(management_asset_profile_error)?;
    Ok((
        StatusCode::CREATED,
        Json(management_asset_profile_response(profile)),
    ))
}

async fn update_management_asset_profile(
    State(state): State<ManagementState>,
    Path(profile_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementAssetProfileResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: ManagementAssetProfileRequest = management_request_json(&state, request).await?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let profile = ManagementAssetProfileRepository::update_management_asset_profile(
        state.store.as_ref(),
        profile_id,
        UpdateManagementAssetProfile {
            name: request.name,
            fields: request.fields,
            dashboard_defaults: request.dashboard_defaults,
        },
    )
    .await
    .map_err(management_asset_profile_error)?;
    Ok(Json(management_asset_profile_response(profile)))
}

async fn delete_management_asset_profile(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(profile_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    let profile_id =
        Uuid::parse_str(&profile_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    ManagementAssetProfileRepository::delete_management_asset_profile(
        state.store.as_ref(),
        profile_id,
    )
    .await
    .map_err(management_asset_profile_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_management_devices(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementDeviceResponse>>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementDeviceRepository::list_management_devices(state.store.as_ref())
        .await
        .map(|devices| {
            Json(
                devices
                    .into_iter()
                    .map(management_device_response)
                    .collect(),
            )
        })
        .map_err(management_device_error)
}

async fn update_management_device(
    State(state): State<ManagementState>,
    Path(device_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementDeviceResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: UpdateManagementDeviceRequest = management_request_json(&state, request).await?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let device = ManagementDeviceRepository::update_management_device(
        state.store.as_ref(),
        &device_id,
        UpdateManagementDevice {
            display_name: request.display_name,
            asset_id: request.asset_id,
            device_profile_id: request.device_profile_id,
            attributes: request.attributes,
            topology: request.topology.map(|topology| ManagementDeviceTopology {
                is_gateway: topology.is_gateway,
                gateway_device_id: topology.gateway_device_id,
            }),
        },
    )
    .await
    .map_err(management_device_error)?;
    Ok(Json(management_device_response(device)))
}

async fn delete_management_device(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    ManagementDeviceRepository::delete_management_device(state.store.as_ref(), &device_id)
        .await
        .map_err(management_device_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_management_assets(
    State(state): State<ManagementState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ManagementAssetResponse>>, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    ManagementAssetRepository::list_management_assets(state.store.as_ref())
        .await
        .map(|assets| Json(assets.into_iter().map(management_asset_response).collect()))
        .map_err(management_asset_error)
}

async fn create_management_asset(
    State(state): State<ManagementState>,
    request: Request,
) -> Result<(StatusCode, Json<ManagementAssetResponse>), ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let request: ManagementAssetRequest = management_request_json(&state, request).await?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let asset = ManagementAssetRepository::create_management_asset(
        state.store.as_ref(),
        CreateManagementAsset {
            name: request.name,
            asset_profile_id: request.asset_profile_id,
            parent_asset_id: request.parent_asset_id,
            metadata: request.metadata,
            attributes: request.attributes,
        },
    )
    .await
    .map_err(management_asset_error)?;
    Ok((StatusCode::CREATED, Json(management_asset_response(asset))))
}

async fn update_management_asset(
    State(state): State<ManagementState>,
    Path(asset_id): Path<String>,
    request: Request,
) -> Result<Json<ManagementAssetResponse>, ManagementSessionError> {
    let headers = request.headers().clone();
    require_management_admin(&state.session_verifier, &headers)?;
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let request: ManagementAssetRequest = management_request_json(&state, request).await?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let asset = ManagementAssetRepository::update_management_asset(
        state.store.as_ref(),
        asset_id,
        UpdateManagementAsset {
            name: request.name,
            asset_profile_id: request.asset_profile_id,
            parent_asset_id: request.parent_asset_id,
            metadata: request.metadata,
            attributes: request.attributes,
        },
    )
    .await
    .map_err(management_asset_error)?;
    Ok(Json(management_asset_response(asset)))
}

async fn delete_management_asset(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(asset_id): Path<String>,
) -> Result<StatusCode, ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    let asset_id = Uuid::parse_str(&asset_id).map_err(|_| ManagementSessionError::BadRequest)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    ManagementAssetRepository::delete_management_asset(state.store.as_ref(), asset_id)
        .await
        .map_err(management_asset_error)?;
    Ok(StatusCode::NO_CONTENT)
}

fn management_user_role(value: &str) -> Result<ManagementUserRole, ManagementSessionError> {
    match value {
        "admin" => Ok(ManagementUserRole::Admin),
        "viewer" => Ok(ManagementUserRole::Viewer),
        _ => Err(ManagementSessionError::BadRequest),
    }
}

fn management_user_response(user: ManagementUser) -> ManagementUserResponse {
    ManagementUserResponse {
        id: user.id,
        username: user.username,
        role: user.role.as_str().to_owned(),
        account_class: user.account_class.as_str().to_owned(),
        default_app: user.default_app,
        granted_apps: user.granted_apps,
    }
}

fn management_device_profile_response(
    profile: ManagementDeviceProfile,
) -> ManagementDeviceProfileResponse {
    ManagementDeviceProfileResponse {
        id: profile.id,
        name: profile.name,
        telemetry_schema: profile.telemetry_schema,
        metric_mapping: profile.metric_mapping,
        reporting_settings: profile.reporting_settings,
    }
}

fn management_asset_profile_response(
    profile: ManagementAssetProfile,
) -> ManagementAssetProfileResponse {
    ManagementAssetProfileResponse {
        id: profile.id,
        name: profile.name,
        fields: profile.fields,
        dashboard_defaults: profile.dashboard_defaults,
    }
}

fn management_user_error(error: ManagementUserError) -> ManagementSessionError {
    match error {
        ManagementUserError::InvalidUsername(_)
        | ManagementUserError::EmptyPasswordHash
        | ManagementUserError::InvalidDefaultApp(_)
        | ManagementUserError::InvalidGrantedApps => ManagementSessionError::BadRequest,
        ManagementUserError::UsernameConflict(_)
        | ManagementUserError::SystemUserImmutable
        | ManagementUserError::LastAdministrator => ManagementSessionError::Conflict,
        ManagementUserError::UserNotFound => ManagementSessionError::NotFound,
        ManagementUserError::InvalidStoredUserId
        | ManagementUserError::InvalidStoredRole(_)
        | ManagementUserError::InvalidStoredAccountClass(_)
        | ManagementUserError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

fn management_device_profile_error(error: ManagementDeviceProfileError) -> ManagementSessionError {
    match error {
        ManagementDeviceProfileError::InvalidName
        | ManagementDeviceProfileError::TelemetrySchemaMustBeObject
        | ManagementDeviceProfileError::MetricMappingMustBeObject
        | ManagementDeviceProfileError::ReportingSettingsMustBeObject => {
            ManagementSessionError::BadRequest
        }
        ManagementDeviceProfileError::DeviceProfileNotFound => ManagementSessionError::NotFound,
        ManagementDeviceProfileError::NameConflict(_)
        | ManagementDeviceProfileError::DeviceProfileInUse(_) => ManagementSessionError::Conflict,
        ManagementDeviceProfileError::InvalidStoredProfile
        | ManagementDeviceProfileError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

fn management_asset_profile_error(error: ManagementAssetProfileError) -> ManagementSessionError {
    match error {
        ManagementAssetProfileError::InvalidName
        | ManagementAssetProfileError::FieldsMustBeObject
        | ManagementAssetProfileError::DashboardDefaultsMustBeObject => {
            ManagementSessionError::BadRequest
        }
        ManagementAssetProfileError::AssetProfileNotFound => ManagementSessionError::NotFound,
        ManagementAssetProfileError::NameConflict(_)
        | ManagementAssetProfileError::AssetProfileInUse(_) => ManagementSessionError::Conflict,
        ManagementAssetProfileError::InvalidStoredProfile
        | ManagementAssetProfileError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

fn management_device_response(device: StorageManagementDevice) -> ManagementDeviceResponse {
    ManagementDeviceResponse {
        device_id: device.device_id,
        display_name: device.display_name,
        asset_id: device.asset_id,
        device_profile_id: device.device_profile_id,
        attributes: device.attributes,
        online: device.health.online,
        last_seen_at: device.health.last_seen_at,
        is_gateway: device.topology.is_gateway,
        gateway_device_id: device.topology.gateway_device_id,
        gateway_status: device.health.gateway_status.map(|status| match status {
            ManagementGatewayStatus::Online => "online".to_owned(),
            ManagementGatewayStatus::Offline => "offline".to_owned(),
        }),
        child_status: device.health.child_status.map(|status| match status {
            ManagementChildStatus::Fresh => "fresh".to_owned(),
            ManagementChildStatus::Stale => "stale".to_owned(),
            ManagementChildStatus::Unavailable => "unavailable".to_owned(),
        }),
    }
}

fn management_asset_response(asset: StorageManagementAsset) -> ManagementAssetResponse {
    ManagementAssetResponse {
        id: asset.id,
        name: asset.name,
        asset_profile_id: asset.asset_profile_id,
        parent_asset_id: asset.parent_asset_id,
        metadata: asset.metadata,
        attributes: asset.attributes,
    }
}

fn management_device_error(error: ManagementDeviceError) -> ManagementSessionError {
    match error {
        ManagementDeviceError::InvalidDeviceId(_)
        | ManagementDeviceError::InvalidDisplayName
        | ManagementDeviceError::AttributesMustBeObject => ManagementSessionError::BadRequest,
        ManagementDeviceError::DeviceNotFound => ManagementSessionError::NotFound,
        ManagementDeviceError::GatewayCannotHaveParent
        | ManagementDeviceError::DeviceCannotBeOwnGateway
        | ManagementDeviceError::GatewayHasChildren
        | ManagementDeviceError::GatewayUnavailable
        | ManagementDeviceError::GatewayIsNotGateway
        | ManagementDeviceError::AssetUnavailable(_)
        | ManagementDeviceError::DeviceProfileUnavailable(_) => ManagementSessionError::Conflict,
        ManagementDeviceError::InvalidStoredAttributes
        | ManagementDeviceError::InvalidStoredTimestamp
        | ManagementDeviceError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

fn management_asset_error(error: ManagementAssetError) -> ManagementSessionError {
    match error {
        ManagementAssetError::InvalidName
        | ManagementAssetError::MetadataMustBeObject
        | ManagementAssetError::AttributesMustBeObject => ManagementSessionError::BadRequest,
        ManagementAssetError::AssetNotFound => ManagementSessionError::NotFound,
        ManagementAssetError::AssetProfileUnavailable(_)
        | ManagementAssetError::ParentAssetUnavailable(_)
        | ManagementAssetError::AssetCannotBeOwnParent
        | ManagementAssetError::AssetCannotHaveDescendantParent
        | ManagementAssetError::SiblingNameConflict { .. } => ManagementSessionError::Conflict,
        ManagementAssetError::InvalidStoredAssetId
        | ManagementAssetError::InvalidStoredReferences
        | ManagementAssetError::InvalidStoredMetadata
        | ManagementAssetError::Storage { .. } => ManagementSessionError::Unavailable,
    }
}

fn require_management_admin(
    session_verifier: &ManagementSessionVerifier,
    headers: &HeaderMap,
) -> Result<(), ManagementSessionError> {
    match session_verifier.authorization(headers) {
        ManagementAuthorization::Unauthenticated => Err(ManagementSessionError::Unauthorized),
        ManagementAuthorization::Admin => Ok(()),
        ManagementAuthorization::Forbidden => Err(ManagementSessionError::Forbidden),
    }
}

async fn authorize_management_mutation<'a>(
    state: &'a ManagementState,
    headers: &HeaderMap,
) -> Result<ManagementMutationLease<'a>, ManagementSessionError> {
    let lease = state.authorization_gate.acquire_mutation().await;
    require_management_admin(&state.session_verifier, headers)?;
    Ok(lease)
}

async fn management_request_json<T>(
    state: &ManagementState,
    request: Request,
) -> Result<T, ManagementSessionError>
where
    T: DeserializeOwned,
{
    Json::<T>::from_request(request, state)
        .await
        .map(|Json(value)| value)
        .map_err(|rejection| match rejection.into_response().status() {
            StatusCode::UNSUPPORTED_MEDIA_TYPE => ManagementSessionError::UnsupportedMediaType,
            StatusCode::PAYLOAD_TOO_LARGE => ManagementSessionError::PayloadTooLarge,
            _ => ManagementSessionError::BadRequest,
        })
}

fn management_device_token_error(error: DeviceTokenStoreError) -> ManagementSessionError {
    match error {
        DeviceTokenStoreError::NotFound => ManagementSessionError::NotFound,
        DeviceTokenStoreError::GatewayChild => ManagementSessionError::Conflict,
        _ => ManagementSessionError::Unavailable,
    }
}

async fn create_device_token(
    State(state): State<ManagementState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
) -> Result<(StatusCode, Json<DeviceTokenResponse>), ManagementSessionError> {
    require_management_admin(&state.session_verifier, &headers)?;
    let _lease = authorize_management_mutation(&state, &headers).await?;
    let token = create_platform_device_token(&state.store, &state.token_vault, &device_id)
        .await
        .map_err(management_device_token_error)?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn authenticate(
    store: &PlatformStore,
    request: &LoginRequest,
) -> Result<iot_api::AuthenticatedUser, AuthError> {
    if let Some(pool) = store.sqlite_pool() {
        return authenticate_credentials_sqlite(pool, &request.username, &request.password).await;
    }
    let pool = store
        .timescale_pool()
        .ok_or_else(|| AuthError::AuthenticationFailed)?;
    authenticate_credentials(pool, &request.username, &request.password).await
}

#[cfg(test)]
async fn pause_after_login_authentication(state: &ManagementState) {
    let Some(hooks) = &state.authorization_test_hooks else {
        return;
    };
    if !hooks.pause_login.swap(false, Ordering::SeqCst) {
        return;
    }
    hooks.login_authenticated.wait().await;
    hooks.release_login.wait().await;
}

#[cfg(test)]
async fn pause_after_mutation_authorization(state: &ManagementState) {
    let Some(hooks) = &state.authorization_test_hooks else {
        return;
    };
    if !hooks.pause_mutation.swap(false, Ordering::SeqCst) {
        return;
    }
    hooks.mutation_authorized.wait().await;
    hooks.release_mutation.wait().await;
}

fn session_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookies| {
            cookies
                .split(';')
                .map(str::trim)
                .find_map(|cookie| cookie.strip_prefix("iot_nano_session="))
        })
        .filter(|session_id| !session_id.is_empty())
}

fn prune_expired_sessions(sessions: &mut HashMap<String, Session>) {
    let now = Instant::now();
    sessions.retain(|_, session| session.expires_at > now);
}

fn session_cookie_headers(session_id: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    let value = HeaderValue::try_from(format!(
        "{SESSION_COOKIE}={session_id}; HttpOnly; Secure; SameSite=Lax; Path=/"
    ))
    .expect("generated session IDs are valid cookie values");
    headers.insert(SET_COOKIE, value);
    headers
}

fn expired_session_cookie_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        SET_COOKIE,
        HeaderValue::from_static(
            "iot_nano_session=; HttpOnly; Secure; SameSite=Lax; Path=/; Max-Age=0",
        ),
    );
    headers
}

enum ManagementSessionError {
    Unauthorized,
    TooManyRequests,
    Forbidden,
    BadRequest,
    UnsupportedMediaType,
    PayloadTooLarge,
    NotFound,
    Conflict,
    Unavailable,
}

impl IntoResponse for ManagementSessionError {
    fn into_response(self) -> Response {
        let (status, code) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::BadRequest => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::UnsupportedMediaType => {
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, "unsupported_media_type")
            }
            Self::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict => (StatusCode::CONFLICT, "conflict"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        };
        (status, Json(json!({ "error": code }))).into_response()
    }
}

#[cfg(test)]
mod unit_tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, atomic::AtomicBool},
    };

    use axum::{
        Json,
        body::Body,
        extract::{ConnectInfo, Path, State},
        http::{
            HeaderMap, HeaderValue, Request,
            header::{CONTENT_TYPE, COOKIE, SET_COOKIE},
        },
    };
    use iot_api::bootstrap_users_sqlite;
    use iot_core::{DatabaseStorage, StorageConfiguration};
    use iot_storage::{
        CreateManagementUser, ManagementUserRepository, ManagementUserRole, PlatformStore,
        UpdateManagementUser,
    };
    use tokio::sync::Barrier;

    use super::{
        LoginRateLimiter, LoginRequest, MAX_LOGIN_FAILURES, ManagementAuthorizationTestHooks,
        ManagementSessionError, ManagementSessionVerifier, ManagementState, authenticate,
        create_management_user, hash_password, login, require_management_admin,
        update_management_user,
    };

    async fn management_state_with_admin_target(
        hooks: Arc<ManagementAuthorizationTestHooks>,
    ) -> (tempfile::TempDir, ManagementState, HeaderMap, HeaderMap) {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(
            PlatformStore::open(&StorageConfiguration {
                storage: DatabaseStorage::Sqlite,
                database_url: None,
                sqlite_path: Some(directory.path().join("platform.sqlite")),
                sqlite_busy_timeout_ms: 5_000,
            })
            .await
            .unwrap(),
        );
        bootstrap_users_sqlite(store.sqlite_pool().unwrap())
            .await
            .unwrap();
        ManagementUserRepository::create_management_user(
            store.as_ref(),
            CreateManagementUser {
                username: "alice".to_owned(),
                password_hash: hash_password("AlicePassword@123").unwrap(),
                default_app: "/apps/fleet".to_owned(),
                granted_apps: vec!["fleet".to_owned()],
            },
        )
        .await
        .unwrap();
        let alice = ManagementUserRepository::update_management_user(
            store.as_ref(),
            "alice",
            UpdateManagementUser {
                default_app: "/apps/fleet".to_owned(),
                granted_apps: vec!["fleet".to_owned()],
                role: Some(ManagementUserRole::Admin),
            },
        )
        .await
        .unwrap();
        let session_verifier = Arc::new(ManagementSessionVerifier::default());
        let state = ManagementState {
            store,
            session_verifier: Arc::clone(&session_verifier),
            token_vault: super::TokenVault::from_key_material(
                "management-session-linearization-test-vault-key-0001",
            ),
            login_limiter: Arc::new(std::sync::Mutex::new(LoginRateLimiter::default())),
            authorization_gate: Arc::new(super::ManagementAuthorizationGate::default()),
            authorization_test_hooks: Some(hooks),
        };
        let admin = authenticate(
            state.store.as_ref(),
            &LoginRequest {
                username: "admin".to_owned(),
                password: "NanoAdmin@1234".to_owned(),
            },
        )
        .await
        .unwrap();
        let admin_headers = session_headers(session_verifier.issue(admin.user_id, admin.role));
        let alice_headers = session_headers(session_verifier.issue(alice.id, super::Role::Admin));
        (directory, state, admin_headers, alice_headers)
    }

    async fn demote_alice(state: ManagementState, admin_headers: HeaderMap) {
        let request = Request::builder()
            .method("PUT")
            .uri("/api/management/users/alice")
            .header(CONTENT_TYPE, "application/json")
            .header(COOKIE, admin_headers[COOKIE].clone())
            .body(Body::from(
                r#"{"default_app":"/apps/fleet","granted_apps":["fleet"],"role":"viewer"}"#,
            ))
            .unwrap();
        assert!(
            update_management_user(State(state), Path("alice".to_owned()), request)
                .await
                .is_ok()
        );
    }

    fn session_headers(session_id: String) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            HeaderValue::from_str(&format!("iot_nano_session={session_id}")).unwrap(),
        );
        headers
    }

    fn session_headers_from_login(login_headers: &HeaderMap) -> HeaderMap {
        let cookie = login_headers[SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(COOKIE, HeaderValue::from_str(cookie).unwrap());
        headers
    }

    #[test]
    fn login_limiter_reserves_in_flight_attempts_before_authentication() {
        let mut limiter = LoginRateLimiter::default();
        let address = IpAddr::V4(Ipv4Addr::LOCALHOST);

        for _ in 0..MAX_LOGIN_FAILURES {
            assert!(limiter.reserve(address));
        }
        assert!(!limiter.reserve(address));
    }

    #[tokio::test]
    async fn login_racing_a_role_demotion_does_not_issue_an_admin_session_after_commit() {
        let hooks = Arc::new(ManagementAuthorizationTestHooks {
            login_authenticated: Arc::new(Barrier::new(2)),
            release_login: Arc::new(Barrier::new(2)),
            mutation_authorized: Arc::new(Barrier::new(2)),
            release_mutation: Arc::new(Barrier::new(2)),
            pause_login: Arc::new(AtomicBool::new(true)),
            pause_mutation: Arc::new(AtomicBool::new(true)),
        });
        let (_directory, state, admin_headers, old_alice_headers) =
            management_state_with_admin_target(Arc::clone(&hooks)).await;
        let login_state = state.clone();
        let login_task = tokio::spawn(async move {
            login(
                State(login_state),
                ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))),
                Json(LoginRequest {
                    username: "alice".to_owned(),
                    password: "AlicePassword@123".to_owned(),
                }),
            )
            .await
            .map(|response| response.0)
        });

        hooks.login_authenticated.wait().await;
        demote_alice(state.clone(), admin_headers).await;
        assert!(matches!(
            require_management_admin(&state.session_verifier, &old_alice_headers),
            Err(ManagementSessionError::Forbidden)
        ));
        hooks.release_login.wait().await;
        let Ok(login_headers) = login_task.await.unwrap() else {
            panic!("login must succeed after a role demotion");
        };
        let new_alice_headers = session_headers_from_login(&login_headers);
        assert!(matches!(
            require_management_admin(&state.session_verifier, &new_alice_headers),
            Err(ManagementSessionError::Forbidden)
        ));
    }

    #[tokio::test]
    async fn mutation_authorized_before_body_parsing_cannot_run_after_role_demotion_commits() {
        let hooks = Arc::new(ManagementAuthorizationTestHooks {
            login_authenticated: Arc::new(Barrier::new(2)),
            release_login: Arc::new(Barrier::new(2)),
            mutation_authorized: Arc::new(Barrier::new(2)),
            release_mutation: Arc::new(Barrier::new(2)),
            pause_login: Arc::new(AtomicBool::new(true)),
            pause_mutation: Arc::new(AtomicBool::new(true)),
        });
        let (_directory, state, admin_headers, alice_headers) =
            management_state_with_admin_target(Arc::clone(&hooks)).await;
        let mutation_state = state.clone();
        let mutation_task = tokio::spawn(async move {
            let request = Request::builder()
                .method("POST")
                .uri("/api/management/users")
                .header(CONTENT_TYPE, "application/json")
                .header(COOKIE, alice_headers[COOKIE].clone())
                .body(Body::from(
                    r#"{"username":"blocked","password":"BlockedPassword@123","default_app":"/apps/fleet","granted_apps":["fleet"]}"#,
                ))
                .unwrap();
            create_management_user(State(mutation_state), request).await
        });

        hooks.mutation_authorized.wait().await;
        demote_alice(state.clone(), admin_headers).await;
        hooks.release_mutation.wait().await;
        assert!(matches!(
            mutation_task.await.unwrap(),
            Err(ManagementSessionError::Forbidden)
        ));
        assert!(
            ManagementUserRepository::list_management_users(state.store.as_ref())
                .await
                .unwrap()
                .iter()
                .all(|user| user.username != "blocked")
        );
    }
}
