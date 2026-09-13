use std::sync::Arc;

use crate::{CommandOutboxRecord, CommandOutboxState, CoreSqliteStore, NewCommandOutboxEntry};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use iot_core::{RpcMode, RpcRequest};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

const CONTROL_SECRET_HEADER: &str = "x-iot-nano-api-core-secret";

#[derive(Clone)]
pub enum CoreControlState {
    Sqlite {
        store: CoreSqliteStore,
        secret: Arc<str>,
    },
    Timescale {
        pool: PgPool,
        secret: Arc<str>,
    },
}

#[derive(Debug, Deserialize)]
struct CreateCommandRequest {
    id: Uuid,
    device_id: String,
    method: String,
    params: serde_json::Value,
    mode: RpcMode,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct CommandResponseRequest {
    command_id: Uuid,
    device_id: String,
    response: serde_json::Value,
    responded_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct TelemetryQuery {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    #[serde(default)]
    bucket: Option<TelemetryBucket>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TelemetryBucket {
    Raw,
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}

#[derive(Debug, Serialize)]
struct TelemetryPoint {
    at: DateTime<Utc>,
    temperature_c: Option<f64>,
    humidity_pct: Option<f64>,
    event_count: i64,
}

#[derive(Debug, Serialize)]
struct CommandResponse {
    id: Uuid,
    device_id: String,
    state: &'static str,
    expires_at: DateTime<Utc>,
    mode: RpcMode,
    response: Option<serde_json::Value>,
    responded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
enum ControlError {
    #[error("core control authentication failed")]
    Unauthorized,
    #[error("invalid command: {0}")]
    InvalidCommand(String),
    #[error("invalid telemetry query: {0}")]
    InvalidQuery(String),
    #[error("command not found")]
    NotFound,
    #[error("command response does not match a published two-way command")]
    Conflict,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Sqlite(#[from] crate::CoreSqliteStoreError),
    #[error("stored command data is invalid")]
    InvalidStoredData,
}

impl CoreControlState {
    pub fn sqlite(
        store: CoreSqliteStore,
        secret: impl AsRef<str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self::Sqlite {
            store,
            secret: Arc::from(validate_secret(secret.as_ref())?),
        })
    }

    pub fn timescale(
        pool: PgPool,
        secret: impl AsRef<str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self::Timescale {
            pool,
            secret: Arc::from(validate_secret(secret.as_ref())?),
        })
    }

    fn secret(&self) -> &str {
        match self {
            Self::Sqlite { secret, .. } | Self::Timescale { secret, .. } => secret,
        }
    }
}

pub fn core_control_router(state: CoreControlState) -> Router {
    Router::new()
        .route("/internal/commands", post(create_command))
        .route("/internal/commands/response", post(record_command_response))
        .route("/internal/commands/{id}", get(get_command))
        .route(
            "/internal/telemetry/devices/{device_id}",
            get(device_telemetry),
        )
        .with_state(state)
}

async fn create_command(
    State(state): State<CoreControlState>,
    headers: HeaderMap,
    Json(request): Json<CreateCommandRequest>,
) -> Result<(StatusCode, Json<CommandResponse>), ControlError> {
    authorize(&state, &headers)?;
    let command = RpcRequest::with_mode(
        request.id,
        request.method,
        request.params,
        request.issued_at,
        request.expires_at,
        request.mode,
    )
    .map_err(|error| ControlError::InvalidCommand(error.to_string()))?;
    validate_device_id(&request.device_id)?;

    let response = match state {
        CoreControlState::Sqlite { store, .. } => {
            let record = store
                .enqueue_command(NewCommandOutboxEntry {
                    id: command.id.to_string(),
                    device_id: request.device_id,
                    method: command.method,
                    params: command.params.to_string(),
                    mode: command.mode,
                    expires_at: command.expires_at,
                    next_attempt_at: command.issued_at,
                })
                .await?;
            command_response_from_sqlite(record)?
        }
        CoreControlState::Timescale { pool, .. } => {
            let mut transaction = pool.begin().await?;
            let row = sqlx::query(
                "INSERT INTO command_outbox (
                    id, device_id, method, params, mode, expires_at, next_attempt_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                 RETURNING id, device_id, state, expires_at, mode, response, responded_at",
            )
            .bind(command.id)
            .bind(&request.device_id)
            .bind(command.method)
            .bind(sqlx::types::Json(command.params))
            .bind(mode_name(command.mode))
            .bind(command.expires_at)
            .bind(command.issued_at)
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            command_response_from_postgres(row)?
        }
    };
    Ok((StatusCode::ACCEPTED, Json(response)))
}

async fn get_command(
    State(state): State<CoreControlState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<CommandResponse>, ControlError> {
    authorize(&state, &headers)?;
    let response = match state {
        CoreControlState::Sqlite { store, .. } => {
            let row = sqlx::query(
                "SELECT id, device_id, state, expires_at, mode, response, responded_at
                 FROM command_outbox WHERE id = ?",
            )
            .bind(id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ControlError::NotFound)?;
            command_response_from_sqlite_row(row)?
        }
        CoreControlState::Timescale { pool, .. } => {
            let row = sqlx::query(
                "SELECT id, device_id, state, expires_at, mode, response, responded_at
                 FROM command_outbox WHERE id = $1",
            )
            .bind(id)
            .fetch_optional(&pool)
            .await?
            .ok_or(ControlError::NotFound)?;
            command_response_from_postgres(row)?
        }
    };
    Ok(Json(response))
}

async fn record_command_response(
    State(state): State<CoreControlState>,
    headers: HeaderMap,
    Json(request): Json<CommandResponseRequest>,
) -> Result<StatusCode, ControlError> {
    authorize(&state, &headers)?;
    validate_device_id(&request.device_id)?;
    let response = serde_json::to_string(&request.response)
        .map_err(|_| ControlError::InvalidCommand("response is not serializable".to_owned()))?;
    let updated = match &state {
        CoreControlState::Sqlite { store, .. } => {
            let response_at = request.responded_at.to_rfc3339();
            sqlx::query(
                "UPDATE command_outbox
                 SET state = 'responded',
                     response = ?,
                     responded_at = ?,
                     lease_until = NULL
                 WHERE id = ?
                   AND device_id = ?
                   AND mode = 'two_way'
                   AND state = 'published_to_broker'
                   AND expires_at > ?",
            )
            .bind(response)
            .bind(&response_at)
            .bind(request.command_id.to_string())
            .bind(&request.device_id)
            .bind(&response_at)
            .execute(store.pool())
            .await?
            .rows_affected()
                == 1
        }
        CoreControlState::Timescale { pool, .. } => {
            sqlx::query(
                "UPDATE command_outbox
                 SET state = 'responded',
                     response = $1,
                     responded_at = $2,
                     lease_until = NULL
                 WHERE id = $3
                   AND device_id = $4
                   AND mode = 'two_way'
                   AND state = 'published_to_broker'
                   AND expires_at > $2",
            )
            .bind(sqlx::types::Json(request.response))
            .bind(request.responded_at)
            .bind(request.command_id)
            .bind(&request.device_id)
            .execute(pool)
            .await?
            .rows_affected()
                == 1
        }
    };
    if updated {
        return Ok(StatusCode::NO_CONTENT);
    }

    let expired = match &state {
        CoreControlState::Sqlite { store, .. } => {
            let response_at = request.responded_at.to_rfc3339();
            sqlx::query(
                "UPDATE command_outbox
                 SET state = 'expired', lease_until = NULL
                 WHERE id = ?
                   AND device_id = ?
                   AND mode = 'two_way'
                   AND state = 'published_to_broker'
                   AND expires_at <= ?",
            )
            .bind(request.command_id.to_string())
            .bind(&request.device_id)
            .bind(response_at)
            .execute(store.pool())
            .await?
            .rows_affected()
                == 1
        }
        CoreControlState::Timescale { pool, .. } => {
            sqlx::query(
                "UPDATE command_outbox
                 SET state = 'expired', lease_until = NULL
                 WHERE id = $1
                   AND device_id = $2
                   AND mode = 'two_way'
                   AND state = 'published_to_broker'
                   AND expires_at <= $3",
            )
            .bind(request.command_id)
            .bind(&request.device_id)
            .bind(request.responded_at)
            .execute(pool)
            .await?
            .rows_affected()
                == 1
        }
    };
    if expired {
        return Ok(StatusCode::NO_CONTENT);
    }

    let idempotent = match &state {
        CoreControlState::Sqlite { store, .. } => sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM command_outbox
                 WHERE id = ? AND device_id = ? AND mode = 'two_way' AND state = 'responded'",
        )
        .bind(request.command_id.to_string())
        .bind(&request.device_id)
        .fetch_optional(store.pool())
        .await?
        .is_some(),
        CoreControlState::Timescale { pool, .. } => sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM command_outbox
                 WHERE id = $1 AND device_id = $2 AND mode = 'two_way' AND state = 'responded'",
        )
        .bind(request.command_id)
        .bind(&request.device_id)
        .fetch_optional(pool)
        .await?
        .is_some(),
    };
    if idempotent {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ControlError::Conflict)
    }
}

async fn device_telemetry(
    State(state): State<CoreControlState>,
    headers: HeaderMap,
    Path(device_id): Path<String>,
    Query(query): Query<TelemetryQuery>,
) -> Result<Json<Vec<TelemetryPoint>>, ControlError> {
    authorize(&state, &headers)?;
    validate_device_id(&device_id)?;
    if query.from >= query.to {
        return Err(ControlError::InvalidQuery(
            "`from` must be earlier than `to`".to_owned(),
        ));
    }

    let bucket = query.bucket.unwrap_or(TelemetryBucket::FiveMinutes);
    let points = match &state {
        CoreControlState::Sqlite { store, .. } => {
            sqlite_telemetry_points(store, &device_id, query.from, query.to, bucket).await?
        }
        CoreControlState::Timescale { pool, .. } => {
            postgres_telemetry_points(pool, &device_id, query.from, query.to, bucket).await?
        }
    };
    Ok(Json(points))
}

async fn sqlite_telemetry_points(
    store: &CoreSqliteStore,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: TelemetryBucket,
) -> Result<Vec<TelemetryPoint>, ControlError> {
    let query = match bucket {
        TelemetryBucket::Raw => {
            "SELECT
                event_at AS at,
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
        TelemetryBucket::FiveMinutes => {
            "SELECT bucket_at AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_rollups_5m
             WHERE device_id = ? AND bucket_at >= ? AND bucket_at <= ?
             ORDER BY bucket_at"
        }
        TelemetryBucket::OneHour => {
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
        .fetch_all(store.pool())
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(TelemetryPoint {
                at: sqlite_timestamp(&row.try_get::<String, _>("at")?)?,
                temperature_c: row.try_get("temperature_c")?,
                humidity_pct: row.try_get("humidity_pct")?,
                event_count: row.try_get("event_count")?,
            })
        })
        .collect()
}

async fn postgres_telemetry_points(
    pool: &PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: TelemetryBucket,
) -> Result<Vec<TelemetryPoint>, ControlError> {
    let query = match bucket {
        TelemetryBucket::Raw => {
            "SELECT
                event_at AS at,
                CASE
                    WHEN jsonb_typeof(measurements -> 'temperature_c') = 'number'
                    THEN CASE
                        WHEN (measurements ->> 'temperature_c')::numeric BETWEEN
                                 '-1.7976931348623157e308'::numeric
                             AND '1.7976931348623157e308'::numeric
                        THEN (measurements ->> 'temperature_c')::double precision
                    END
                END AS temperature_c,
                CASE
                    WHEN jsonb_typeof(measurements -> 'humidity_pct') = 'number'
                    THEN CASE
                        WHEN (measurements ->> 'humidity_pct')::numeric BETWEEN
                                 '-1.7976931348623157e308'::numeric
                             AND '1.7976931348623157e308'::numeric
                        THEN (measurements ->> 'humidity_pct')::double precision
                    END
                END AS humidity_pct,
                1::bigint AS event_count
             FROM telemetry
             WHERE device_id = $1 AND event_at >= $2 AND event_at <= $3
             ORDER BY event_at"
        }
        TelemetryBucket::FiveMinutes => {
            "SELECT bucket AS at, avg_temperature_c AS temperature_c,
                    avg_humidity_pct AS humidity_pct, event_count
             FROM telemetry_5m
             WHERE device_id = $1 AND bucket >= $2 AND bucket <= $3
             ORDER BY bucket"
        }
        TelemetryBucket::OneHour => {
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
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(TelemetryPoint {
                at: row.try_get("at")?,
                temperature_c: row.try_get("temperature_c")?,
                humidity_pct: row.try_get("humidity_pct")?,
                event_count: row.try_get("event_count")?,
            })
        })
        .collect()
}

fn command_response_from_sqlite(
    record: CommandOutboxRecord,
) -> Result<CommandResponse, ControlError> {
    Ok(CommandResponse {
        id: record
            .id
            .parse()
            .map_err(|_| ControlError::InvalidStoredData)?,
        device_id: record.device_id,
        state: command_state_name(record.state),
        expires_at: record.expires_at,
        mode: record.mode,
        response: record
            .response
            .map(|response| serde_json::from_str(&response))
            .transpose()
            .map_err(|_| ControlError::InvalidStoredData)?,
        responded_at: record.responded_at,
    })
}

fn command_response_from_sqlite_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<CommandResponse, ControlError> {
    Ok(CommandResponse {
        id: row
            .try_get::<String, _>("id")?
            .parse()
            .map_err(|_| ControlError::InvalidStoredData)?,
        device_id: row.try_get("device_id")?,
        state: state_name(&row.try_get::<String, _>("state")?)?,
        expires_at: DateTime::parse_from_rfc3339(&row.try_get::<String, _>("expires_at")?)
            .map_err(|_| ControlError::InvalidStoredData)?
            .with_timezone(&Utc),
        mode: mode_from_name(&row.try_get::<String, _>("mode")?)?,
        response: row
            .try_get::<Option<String>, _>("response")?
            .map(|response| serde_json::from_str(&response))
            .transpose()
            .map_err(|_| ControlError::InvalidStoredData)?,
        responded_at: row
            .try_get::<Option<String>, _>("responded_at")?
            .map(|responded_at| {
                DateTime::parse_from_rfc3339(&responded_at)
                    .map(|value| value.with_timezone(&Utc))
                    .map_err(|_| ControlError::InvalidStoredData)
            })
            .transpose()?,
    })
}

fn command_response_from_postgres(
    row: sqlx::postgres::PgRow,
) -> Result<CommandResponse, ControlError> {
    Ok(CommandResponse {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        state: state_name(&row.try_get::<String, _>("state")?)?,
        expires_at: row.try_get("expires_at")?,
        mode: mode_from_name(&row.try_get::<String, _>("mode")?)?,
        response: row
            .try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("response")?
            .map(|response| response.0),
        responded_at: row.try_get("responded_at")?,
    })
}

fn authorize(state: &CoreControlState, headers: &HeaderMap) -> Result<(), ControlError> {
    let supplied = headers
        .get(CONTROL_SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(ControlError::Unauthorized)?;
    if constant_time_equal(state.secret().as_bytes(), supplied.as_bytes()) {
        Ok(())
    } else {
        Err(ControlError::Unauthorized)
    }
}

fn validate_secret(secret: &str) -> Result<&str, ControlError> {
    if secret.len() < 32
        || !secret.is_ascii()
        || secret.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        Err(ControlError::InvalidCommand(
            "control secret must be at least 32 ASCII non-whitespace characters".to_owned(),
        ))
    } else {
        Ok(secret)
    }
}

fn validate_device_id(device_id: &str) -> Result<(), ControlError> {
    if device_id.is_empty()
        || device_id.len() > 128
        || !device_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        Err(ControlError::InvalidCommand(
            "device ID must contain only ASCII letters, digits, '-' or '_'".to_owned(),
        ))
    } else {
        Ok(())
    }
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

fn state_name(state: &str) -> Result<&'static str, ControlError> {
    match state {
        "queued" | "leased" => Ok("queued"),
        "published_to_broker" => Ok("published_to_broker"),
        "responded" => Ok("responded"),
        "expired" => Ok("expired"),
        "failed" => Ok("failed"),
        _ => Err(ControlError::InvalidStoredData),
    }
}

fn mode_name(mode: RpcMode) -> &'static str {
    match mode {
        RpcMode::OneWay => "one_way",
        RpcMode::TwoWay => "two_way",
    }
}

fn mode_from_name(mode: &str) -> Result<RpcMode, ControlError> {
    match mode {
        "one_way" => Ok(RpcMode::OneWay),
        "two_way" => Ok(RpcMode::TwoWay),
        _ => Err(ControlError::InvalidStoredData),
    }
}

fn sqlite_timestamp(value: &str) -> Result<DateTime<Utc>, ControlError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| ControlError::InvalidStoredData)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InvalidCommand(_) | Self::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::Database(_) | Self::Sqlite(_) | Self::InvalidStoredData => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        (status, self.to_string()).into_response()
    }
}
