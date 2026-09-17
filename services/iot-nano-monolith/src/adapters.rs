use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, Utc};
use iot_api::{
    CoreCommandCreateRequest, CoreCommandRecord, CoreCommandResponseRequest, CoreFacade,
    CoreFacadeError, CoreTelemetryBucket, CoreTelemetryPoint, CoreTelemetryQuery,
};
use iot_core::RpcRequest;
use iot_nano_core::{CommandTransport, CommandTransportError, TransportRpcPublishRequest};
use iot_nano_mqttd::{
    AuthenticatedDevice, AuthorizationError, CommandResponseError, CommandResponsePort,
    DeviceAuthorizationPort, GatewayAuthorization, GatewayAuthorizationRequest, RpcSessionRouter,
    SessionError, TransportAuthRequest, TransportRpcResponse,
};
use iot_storage::{
    CommandOutboxRecord, CommandOutboxState, DeviceAuthorizationRepository, IdentityRepository,
    NewCommandOutboxEntry, PlatformStore, PlatformStoreError,
};
use sqlx::Row;

const DEVICE_TOKEN_USERNAME: &str = "iotd_device_token";
const STORAGE_UNAVAILABLE: &str = "platform storage unavailable";
const COMMAND_PUBLICATION_UNAVAILABLE: &str = "command publication unavailable";
const COMMAND_REQUEST_EXPIRED: &str = "command request has expired";

pub struct PlatformCoreFacade {
    store: Arc<PlatformStore>,
}

impl PlatformCoreFacade {
    pub fn new(store: Arc<PlatformStore>) -> Self {
        Self { store }
    }
}

impl CoreFacade for PlatformCoreFacade {
    fn create_command(
        &self,
        request: CoreCommandCreateRequest,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            validate_device_id(&request.device_id)?;
            let device_id = request.device_id;
            let command = RpcRequest::with_mode(
                request.id,
                request.method,
                request.params,
                request.issued_at,
                request.expires_at,
                request.mode,
            )
            .map_err(|_| CoreFacadeError::Rejected(400))?;
            let record = store
                .enqueue_command(NewCommandOutboxEntry {
                    id: command.id.to_string(),
                    tenant_id: request.tenant_id,
                    device_id,
                    method: command.method,
                    params: command.params.to_string(),
                    mode: command.mode,
                    expires_at: command.expires_at,
                    next_attempt_at: command.issued_at,
                })
                .await
                .map_err(map_command_storage_error)?;
            core_command_record(record)
        })
    }

    fn get_command(
        &self,
        tenant_id: uuid::Uuid,
        id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<CoreCommandRecord, CoreFacadeError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move { get_command(&store, tenant_id, id).await })
    }

    fn record_command_response(
        &self,
        request: CoreCommandResponseRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreFacadeError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move { record_command_response(&store, request).await })
    }

    fn telemetry(
        &self,
        query: CoreTelemetryQuery,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<CoreTelemetryPoint>, CoreFacadeError>> + Send + '_>>
    {
        let store = Arc::clone(&self.store);
        Box::pin(async move { telemetry(&store, query).await })
    }
}

fn validate_device_id(device_id: &str) -> Result<(), CoreFacadeError> {
    if device_id.is_empty()
        || device_id.len() > 128
        || !device_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        Err(CoreFacadeError::Rejected(400))
    } else {
        Ok(())
    }
}

fn map_command_storage_error(error: PlatformStoreError) -> CoreFacadeError {
    match error {
        PlatformStoreError::CommandConflict(_) => CoreFacadeError::Rejected(409),
        _ => CoreFacadeError::Unavailable,
    }
}

fn core_command_record(record: CommandOutboxRecord) -> Result<CoreCommandRecord, CoreFacadeError> {
    let id = record
        .id
        .parse()
        .map_err(|_| CoreFacadeError::Unavailable)?;
    let response = record
        .response
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|_| CoreFacadeError::Unavailable)?;
    Ok(CoreCommandRecord {
        id,
        tenant_id: record.tenant_id,
        device_id: record.device_id,
        state: command_state_name(record.state).to_owned(),
        expires_at: record.expires_at,
        mode: record.mode,
        response,
        responded_at: record.responded_at,
    })
}

async fn get_command(
    store: &PlatformStore,
    tenant_id: uuid::Uuid,
    id: uuid::Uuid,
) -> Result<CoreCommandRecord, CoreFacadeError> {
    if let Some(pool) = store.sqlite_pool() {
        let row = sqlx::query(
            "SELECT id, tenant_id, device_id, state, expires_at, mode, response, responded_at
             FROM command_outbox
             WHERE id = ? AND tenant_id = ?",
        )
        .bind(id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(pool)
        .await
        .map_err(|_| CoreFacadeError::Unavailable)?
        .ok_or(CoreFacadeError::NotFound)?;
        return sqlite_command_record(row);
    }
    let pool = store.timescale_pool().ok_or(CoreFacadeError::Unavailable)?;
    let row = sqlx::query(
        "SELECT id, tenant_id, device_id, state, expires_at, mode, response, responded_at
         FROM command_outbox
         WHERE id = $1 AND tenant_id = $2",
    )
    .bind(id)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await
    .map_err(|_| CoreFacadeError::Unavailable)?
    .ok_or(CoreFacadeError::NotFound)?;
    postgres_command_record(row)
}

fn sqlite_command_record(
    row: sqlx::sqlite::SqliteRow,
) -> Result<CoreCommandRecord, CoreFacadeError> {
    let id = row
        .try_get::<String, _>("id")
        .map_err(|_| CoreFacadeError::Unavailable)?
        .parse()
        .map_err(|_| CoreFacadeError::Unavailable)?;
    let state = row
        .try_get::<String, _>("state")
        .map_err(|_| CoreFacadeError::Unavailable)?;
    let mode = rpc_mode_from_database(
        &row.try_get::<String, _>("mode")
            .map_err(|_| CoreFacadeError::Unavailable)?,
    )?;
    let response = row
        .try_get::<Option<String>, _>("response")
        .map_err(|_| CoreFacadeError::Unavailable)?
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|_| CoreFacadeError::Unavailable)?;
    Ok(CoreCommandRecord {
        id,
        tenant_id: uuid::Uuid::parse_str(
            &row.try_get::<String, _>("tenant_id")
                .map_err(|_| CoreFacadeError::Unavailable)?,
        )
        .map_err(|_| CoreFacadeError::Unavailable)?,
        device_id: row
            .try_get("device_id")
            .map_err(|_| CoreFacadeError::Unavailable)?,
        state: command_state_name_from_database(&state)?.to_owned(),
        expires_at: parse_timestamp(
            &row.try_get::<String, _>("expires_at")
                .map_err(|_| CoreFacadeError::Unavailable)?,
        )?,
        mode,
        response,
        responded_at: row
            .try_get::<Option<String>, _>("responded_at")
            .map_err(|_| CoreFacadeError::Unavailable)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
    })
}

fn postgres_command_record(
    row: sqlx::postgres::PgRow,
) -> Result<CoreCommandRecord, CoreFacadeError> {
    let state = row
        .try_get::<String, _>("state")
        .map_err(|_| CoreFacadeError::Unavailable)?;
    let response = row
        .try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("response")
        .map_err(|_| CoreFacadeError::Unavailable)?
        .map(|value| value.0);
    Ok(CoreCommandRecord {
        id: row
            .try_get("id")
            .map_err(|_| CoreFacadeError::Unavailable)?,
        tenant_id: row
            .try_get("tenant_id")
            .map_err(|_| CoreFacadeError::Unavailable)?,
        device_id: row
            .try_get("device_id")
            .map_err(|_| CoreFacadeError::Unavailable)?,
        state: command_state_name_from_database(&state)?.to_owned(),
        expires_at: row
            .try_get("expires_at")
            .map_err(|_| CoreFacadeError::Unavailable)?,
        mode: rpc_mode_from_database(
            &row.try_get::<String, _>("mode")
                .map_err(|_| CoreFacadeError::Unavailable)?,
        )?,
        response,
        responded_at: row
            .try_get("responded_at")
            .map_err(|_| CoreFacadeError::Unavailable)?,
    })
}

fn command_state_name(state: CommandOutboxState) -> &'static str {
    match state {
        CommandOutboxState::Queued | CommandOutboxState::Leased => "queued",
        CommandOutboxState::PublishedToBroker => "published_to_broker",
        CommandOutboxState::Responded => "responded",
        CommandOutboxState::Expired => "expired",
        CommandOutboxState::Failed => "failed",
    }
}

fn command_state_name_from_database(state: &str) -> Result<&'static str, CoreFacadeError> {
    match state {
        "queued" | "leased" => Ok("queued"),
        "published_to_broker" => Ok("published_to_broker"),
        "responded" => Ok("responded"),
        "expired" => Ok("expired"),
        "failed" => Ok("failed"),
        _ => Err(CoreFacadeError::Unavailable),
    }
}

fn rpc_mode_from_database(mode: &str) -> Result<iot_core::RpcMode, CoreFacadeError> {
    match mode {
        "one_way" => Ok(iot_core::RpcMode::OneWay),
        "two_way" => Ok(iot_core::RpcMode::TwoWay),
        _ => Err(CoreFacadeError::Unavailable),
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, CoreFacadeError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| CoreFacadeError::Unavailable)
}

async fn record_command_response(
    store: &PlatformStore,
    request: CoreCommandResponseRequest,
) -> Result<(), CoreFacadeError> {
    validate_device_id(&request.device_id)?;
    let response =
        serde_json::to_string(&request.response).map_err(|_| CoreFacadeError::Rejected(400))?;
    let response_at = request.responded_at.to_rfc3339();

    if store
        .mark_command_responded(
            request.tenant_id,
            request.command_id,
            &request.device_id,
            request.token_id,
            &response,
            request.responded_at,
        )
        .await
        .map_err(|_| CoreFacadeError::Unavailable)?
        .is_some()
    {
        return Ok(());
    }

    if let Some(pool) = store.sqlite_pool() {
        let expired = sqlx::query(
            "UPDATE command_outbox AS command
             SET state = 'expired', lease_until = NULL
             WHERE command.id = ?
               AND command.tenant_id = ?
               AND command.device_id = ?
               AND command.mode = 'two_way'
               AND command.state = 'published_to_broker'
               AND command.expires_at <= ?
               AND EXISTS (
                    SELECT 1
                    FROM device_tokens
                    JOIN devices ON devices.device_id = device_tokens.device_id
                    WHERE device_tokens.id = ?
                      AND device_tokens.device_id = command.device_id
                      AND devices.tenant_id = command.tenant_id
                      AND device_tokens.revoked_at IS NULL
                      AND devices.deleted_at IS NULL
               )",
        )
        .bind(request.command_id.to_string())
        .bind(request.tenant_id.to_string())
        .bind(&request.device_id)
        .bind(&response_at)
        .bind(request.token_id.to_string())
        .execute(pool)
        .await
        .map_err(|_| CoreFacadeError::Unavailable)?
        .rows_affected();
        return if expired == 1 {
            Ok(())
        } else {
            Err(CoreFacadeError::Rejected(409))
        };
    }

    let pool = store.timescale_pool().ok_or(CoreFacadeError::Unavailable)?;
    let expired = sqlx::query(
        "UPDATE command_outbox AS command
         SET state = 'expired', lease_until = NULL
         WHERE command.id = $1
           AND command.tenant_id = $2
           AND command.device_id = $3
           AND command.mode = 'two_way'
           AND command.state = 'published_to_broker'
           AND command.expires_at <= $4
           AND EXISTS (
                SELECT 1
                FROM device_tokens
                JOIN devices ON devices.device_id = device_tokens.device_id
                WHERE device_tokens.id = $5
                  AND device_tokens.device_id = command.device_id
                  AND devices.tenant_id = command.tenant_id
                  AND device_tokens.revoked_at IS NULL
                  AND devices.deleted_at IS NULL
           )",
    )
    .bind(request.command_id)
    .bind(request.tenant_id)
    .bind(&request.device_id)
    .bind(request.responded_at)
    .bind(request.token_id)
    .execute(pool)
    .await
    .map_err(|_| CoreFacadeError::Unavailable)?
    .rows_affected();
    if expired == 1 {
        Ok(())
    } else {
        Err(CoreFacadeError::Rejected(409))
    }
}

async fn telemetry(
    store: &PlatformStore,
    query: CoreTelemetryQuery,
) -> Result<Vec<CoreTelemetryPoint>, CoreFacadeError> {
    validate_device_id(&query.device_id)?;
    if query.from >= query.to {
        return Err(CoreFacadeError::Rejected(400));
    }
    if let Some(pool) = store.sqlite_pool() {
        return sqlite_telemetry_points(pool, &query.device_id, query.from, query.to, query.bucket)
            .await;
    }
    let pool = store.timescale_pool().ok_or(CoreFacadeError::Unavailable)?;
    postgres_telemetry_points(pool, &query.device_id, query.from, query.to, query.bucket).await
}

async fn sqlite_telemetry_points(
    pool: &sqlx::SqlitePool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: CoreTelemetryBucket,
) -> Result<Vec<CoreTelemetryPoint>, CoreFacadeError> {
    let query = match bucket {
        CoreTelemetryBucket::Raw => {
            "SELECT event_at AS at,
                    CASE
                        WHEN json_type(measurements, '$.temperature_c') IN ('integer', 'real')
                        THEN json_extract(measurements, '$.temperature_c')
                    END AS temperature_c,
                    CASE
                        WHEN json_type(measurements, '$.humidity_pct') IN ('integer', 'real')
                        THEN json_extract(measurements, '$.humidity_pct')
                    END AS humidity_pct,
                    1 AS event_count
             FROM telemetry
             WHERE device_id = ? AND event_at >= ? AND event_at <= ?
             ORDER BY event_at"
        }
        CoreTelemetryBucket::FiveMinutes => {
            "SELECT bucket_at AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_rollups_5m
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
        CoreTelemetryBucket::OneHour => {
            "SELECT bucket_at AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_rollups_1h
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
    };
    let rows = sqlx::query(query)
        .bind(device_id)
        .bind(from.to_rfc3339())
        .bind(to.to_rfc3339())
        .fetch_all(pool)
        .await
        .map_err(|_| CoreFacadeError::Unavailable)?;
    rows.into_iter()
        .map(|row| {
            Ok(CoreTelemetryPoint {
                at: parse_timestamp(
                    &row.try_get::<String, _>("at")
                        .map_err(|_| CoreFacadeError::Unavailable)?,
                )?,
                temperature_c: row
                    .try_get("temperature_c")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
                humidity_pct: row
                    .try_get("humidity_pct")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
                event_count: row
                    .try_get("event_count")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
            })
        })
        .collect()
}

async fn postgres_telemetry_points(
    pool: &sqlx::PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: CoreTelemetryBucket,
) -> Result<Vec<CoreTelemetryPoint>, CoreFacadeError> {
    let query = match bucket {
        CoreTelemetryBucket::Raw => {
            "WITH f64_limits AS (
                SELECT
                    '-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368'::numeric AS min,
                    '179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368'::numeric AS max
             )
             SELECT
                event_at AS at,
                CASE
                    WHEN jsonb_typeof(measurements -> 'temperature_c') = 'number'
                    THEN CASE
                        WHEN (measurements ->> 'temperature_c')::numeric BETWEEN f64_limits.min AND f64_limits.max
                        THEN (measurements ->> 'temperature_c')::double precision
                    END
                END AS temperature_c,
                CASE
                    WHEN jsonb_typeof(measurements -> 'humidity_pct') = 'number'
                    THEN CASE
                        WHEN (measurements ->> 'humidity_pct')::numeric BETWEEN f64_limits.min AND f64_limits.max
                        THEN (measurements ->> 'humidity_pct')::double precision
                    END
                END AS humidity_pct,
                1::bigint AS event_count
             FROM telemetry
             CROSS JOIN f64_limits
             WHERE device_id = $1 AND event_at >= $2 AND event_at <= $3
             ORDER BY event_at"
        }
        CoreTelemetryBucket::FiveMinutes => {
            "SELECT bucket AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_5m
             WHERE device_id = $1 AND bucket >= $2 AND bucket <= $3
             ORDER BY bucket"
        }
        CoreTelemetryBucket::OneHour => {
            "SELECT bucket AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_1h
             WHERE device_id = $1 AND bucket >= $2 AND bucket <= $3
             ORDER BY bucket"
        }
    };
    let rows = sqlx::query(query)
        .bind(device_id)
        .bind(from)
        .bind(to)
        .fetch_all(pool)
        .await
        .map_err(|_| CoreFacadeError::Unavailable)?;
    rows.into_iter()
        .map(|row| {
            Ok(CoreTelemetryPoint {
                at: row
                    .try_get("at")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
                temperature_c: row
                    .try_get("temperature_c")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
                humidity_pct: row
                    .try_get("humidity_pct")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
                event_count: row
                    .try_get("event_count")
                    .map_err(|_| CoreFacadeError::Unavailable)?,
            })
        })
        .collect()
}

pub struct PlatformCommandTransport {
    router: RpcSessionRouter,
    authorization: Arc<dyn DeviceAuthorizationPort>,
}

impl PlatformCommandTransport {
    pub fn new(router: RpcSessionRouter, authorization: Arc<dyn DeviceAuthorizationPort>) -> Self {
        Self {
            router,
            authorization,
        }
    }

    async fn publish_request(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Result<(), CommandTransportError> {
        if request.expires_at <= Utc::now() {
            return Err(CommandTransportError::Unavailable(
                COMMAND_REQUEST_EXPIRED.to_owned(),
            ));
        }
        let tenant_id = request.tenant_id;
        let device_id = request.device_id.clone();
        let rpc = RpcRequest::with_mode(
            request.id,
            request.method,
            request.params,
            request.issued_at,
            request.expires_at,
            request.mode,
        )
        .map_err(|_| CommandTransportError::Configuration("request is not valid".to_owned()))?;
        let snapshot = self
            .router
            .active_snapshot(&device_id)
            .await
            .ok_or(CommandTransportError::NoActiveSession)?;
        let session = snapshot.authenticated_device();
        if session.tenant_id != tenant_id {
            return Err(CommandTransportError::NoActiveSession);
        }
        self.authorization
            .authorize_session(session)
            .await
            .map_err(map_authorization_error)?;
        self.router
            .publish_to_snapshot(tenant_id, &snapshot, rpc)
            .await
            .map_err(map_session_error)
    }
}

impl CommandTransport for PlatformCommandTransport {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(self.publish_request(request))
    }
}

fn map_session_error(error: SessionError) -> CommandTransportError {
    match error {
        SessionError::DeviceOffline => CommandTransportError::NoActiveSession,
        SessionError::SessionUnavailable
        | SessionError::PublicationTimeout
        | SessionError::PublicationWaiterUnavailable
        | SessionError::AcknowledgementAlreadyConsumed => {
            CommandTransportError::Unavailable(COMMAND_PUBLICATION_UNAVAILABLE.to_owned())
        }
    }
}

fn map_authorization_error(error: AuthorizationError) -> CommandTransportError {
    match error {
        AuthorizationError::Denied => CommandTransportError::NoActiveSession,
        AuthorizationError::Unavailable(_) => {
            CommandTransportError::Unavailable(COMMAND_PUBLICATION_UNAVAILABLE.to_owned())
        }
    }
}

pub struct PlatformCommandResponse {
    store: Arc<PlatformStore>,
}

impl PlatformCommandResponse {
    pub fn new(store: Arc<PlatformStore>) -> Self {
        Self { store }
    }
}

impl CommandResponsePort for PlatformCommandResponse {
    fn record_response(
        &self,
        response: TransportRpcResponse,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandResponseError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let response_json = serde_json::to_string(&response.response)
                .map_err(|_| CommandResponseError::Unavailable(STORAGE_UNAVAILABLE.to_owned()))?;
            store
                .mark_command_responded(
                    response.tenant_id,
                    response.command_id,
                    &response.device_id,
                    response.token_id,
                    &response_json,
                    Utc::now(),
                )
                .await
                .map_err(|_| CommandResponseError::Unavailable(STORAGE_UNAVAILABLE.to_owned()))?
                .ok_or_else(|| CommandResponseError::Unavailable(STORAGE_UNAVAILABLE.to_owned()))?;
            Ok(())
        })
    }
}

pub struct PlatformDeviceAuthorization {
    store: Arc<PlatformStore>,
}

impl PlatformDeviceAuthorization {
    pub fn new(store: Arc<PlatformStore>) -> Self {
        Self { store }
    }
}

impl DeviceAuthorizationPort for PlatformDeviceAuthorization {
    fn authenticate(
        &self,
        request: TransportAuthRequest,
    ) -> Pin<Box<dyn Future<Output = Result<AuthenticatedDevice, AuthorizationError>> + Send + '_>>
    {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            if request.username != DEVICE_TOKEN_USERNAME {
                return Err(AuthorizationError::Denied);
            }
            IdentityRepository::resolve_active_device_token(store.as_ref(), &request.password)
                .await
                .map(|device| AuthenticatedDevice {
                    token_id: device.token_id,
                    tenant_id: device.tenant_id,
                    device_id: device.device_id,
                    is_gateway: device.is_gateway,
                })
                .map_err(map_storage_error)
        })
    }

    fn authorize_session(
        &self,
        device: AuthenticatedDevice,
    ) -> Pin<Box<dyn Future<Output = Result<(), AuthorizationError>> + Send + '_>> {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            DeviceAuthorizationRepository::authorize_device_session(
                store.as_ref(),
                device.token_id,
                device.tenant_id,
                &device.device_id,
            )
            .await
            .map_err(map_storage_error)
        })
    }

    fn authorize_gateway_uplink(
        &self,
        request: GatewayAuthorizationRequest,
    ) -> Pin<Box<dyn Future<Output = Result<GatewayAuthorization, AuthorizationError>> + Send + '_>>
    {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            DeviceAuthorizationRepository::authorize_gateway_token(
                store.as_ref(),
                request.token_id,
                request.tenant_id,
                &request.gateway_device_id,
                request.child_device_id.as_deref(),
            )
            .await
            .map_err(map_storage_error)?;
            Ok(GatewayAuthorization {
                tenant_id: request.tenant_id,
                gateway_device_id: request.gateway_device_id,
                token_id: request.token_id,
                child_device_id: request.child_device_id,
                topic: request.topic,
                event_kind: request.event_kind,
            })
        })
    }
}

fn map_storage_error(error: PlatformStoreError) -> AuthorizationError {
    match error {
        PlatformStoreError::DeviceTokenDenied => AuthorizationError::Denied,
        _ => AuthorizationError::Unavailable(STORAGE_UNAVAILABLE.to_owned()),
    }
}
