use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use iot_core::{
    DEVICE_TELEMETRY_TOPIC, DeviceTelemetryPayload, GATEWAY_CONNECT_TOPIC,
    GATEWAY_DISCONNECT_TOPIC, GATEWAY_TELEMETRY_TOPIC, GatewayChildLifecyclePayload,
    GatewayTelemetryPayload, device_token_prefix, verify_device_token,
};
use iot_storage::SqliteStore;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row, SqlitePool};
use thiserror::Error;
use uuid::Uuid;

use crate::{IngestMetrics, IngestOutcome, MqttConsumerError, MqttStreamProducer};

const MAX_WEBHOOK_INBOX_BYTES: u64 = 64 * 1024 * 1024;
const WORKER_IDLE_DELAY: Duration = Duration::from_millis(100);
const WORKER_FAILURE_DELAY: Duration = Duration::from_millis(250);
const MAX_FUTURE_EVENT_SKEW: ChronoDuration = ChronoDuration::minutes(5);

#[derive(Debug, Clone)]
pub struct TokenWebhookIngress {
    spool: WebhookSpool,
    secret: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct SqliteTokenWebhookIngress {
    spool: WebhookSpool,
    secret: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NanoMqWebhook {
    action: String,
    from_username: String,
    topic: String,
    qos: u8,
    ts: i64,
    payload: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpoolRecord {
    id: Uuid,
    webhook: NanoMqWebhook,
}

#[derive(Debug, Clone)]
struct WebhookSpool {
    path: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct WebhookState {
    ingress: TokenWebhookIngress,
    transport_secret: Option<Arc<str>>,
}

#[derive(Clone)]
struct SqliteWebhookState {
    ingress: SqliteTokenWebhookIngress,
    transport_secret: Option<Arc<str>>,
}

struct WebhookWorker {
    pool: PgPool,
    producer: MqttStreamProducer,
    spool: WebhookSpool,
    metrics: Arc<IngestMetrics>,
}

struct SqliteWebhookWorker {
    pool: SqlitePool,
    producer: MqttStreamProducer,
    spool: WebhookSpool,
    metrics: Arc<IngestMetrics>,
}

struct ResolvedDeviceToken {
    id: Uuid,
    device_id: String,
    is_gateway: bool,
}

struct SqliteResolvedDeviceToken {
    id: String,
    device_id: String,
    is_gateway: bool,
}

#[derive(Debug, Error)]
pub enum WebhookError {
    #[error("webhook authentication failed")]
    Unauthorized,
    #[error("device token is inactive")]
    InactiveDeviceToken,
    #[error("{0}")]
    InvalidRequest(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Stream(#[from] MqttConsumerError),
    #[error("stream append task failed")]
    Join,
    #[error(transparent)]
    Spool(#[from] std::io::Error),
    #[error("webhook inbox serialization failed")]
    SpoolSerialization(#[source] serde_json::Error),
    #[error("webhook inbox capacity reached")]
    SpoolFull,
}

impl TokenWebhookIngress {
    pub fn new(
        pool: PgPool,
        stream: iot_stream::LocalStream,
        secret: impl AsRef<str>,
        inbox_dir: impl AsRef<Path>,
        metrics: Arc<IngestMetrics>,
    ) -> Result<Self, WebhookError> {
        let spool = WebhookSpool::open(inbox_dir)?;
        tokio::spawn(
            WebhookWorker {
                pool,
                producer: MqttStreamProducer::new(stream),
                spool: spool.clone(),
                metrics,
            }
            .run(),
        );
        Ok(Self {
            spool,
            secret: Arc::from(secret.as_ref()),
        })
    }

    async fn enqueue(
        &self,
        supplied_secret: Option<&str>,
        webhook: NanoMqWebhook,
    ) -> Result<(), WebhookError> {
        let supplied_secret = supplied_secret.ok_or(WebhookError::Unauthorized)?;
        if !constant_time_equal(self.secret.as_bytes(), supplied_secret.as_bytes()) {
            return Err(WebhookError::Unauthorized);
        }
        self.enqueue_trusted(webhook).await
    }

    async fn enqueue_trusted(&self, webhook: NanoMqWebhook) -> Result<(), WebhookError> {
        let spool = self.spool.clone();
        let record = SpoolRecord {
            id: Uuid::new_v4(),
            webhook,
        };
        tokio::task::spawn_blocking(move || spool.append(&record))
            .await
            .map_err(|_| WebhookError::Join)?
    }
}

impl SqliteTokenWebhookIngress {
    pub fn new(
        store: SqliteStore,
        stream: iot_stream::LocalStream,
        secret: impl AsRef<str>,
        inbox_dir: impl AsRef<Path>,
        metrics: Arc<IngestMetrics>,
    ) -> Result<Self, WebhookError> {
        let spool = WebhookSpool::open(inbox_dir)?;
        tokio::spawn(
            SqliteWebhookWorker {
                pool: store.pool().clone(),
                producer: MqttStreamProducer::new(stream),
                spool: spool.clone(),
                metrics,
            }
            .run(),
        );
        Ok(Self {
            spool,
            secret: Arc::from(secret.as_ref()),
        })
    }

    async fn enqueue(
        &self,
        supplied_secret: Option<&str>,
        webhook: NanoMqWebhook,
    ) -> Result<(), WebhookError> {
        let supplied_secret = supplied_secret.ok_or(WebhookError::Unauthorized)?;
        if !constant_time_equal(self.secret.as_bytes(), supplied_secret.as_bytes()) {
            return Err(WebhookError::Unauthorized);
        }
        self.enqueue_trusted(webhook).await
    }

    async fn enqueue_trusted(&self, webhook: NanoMqWebhook) -> Result<(), WebhookError> {
        let spool = self.spool.clone();
        let record = SpoolRecord {
            id: Uuid::new_v4(),
            webhook,
        };
        tokio::task::spawn_blocking(move || spool.append(&record))
            .await
            .map_err(|_| WebhookError::Join)?
    }
}

impl WebhookWorker {
    async fn run(self) {
        loop {
            let spool = self.spool.clone();
            let record = match tokio::task::spawn_blocking(move || spool.first()).await {
                Ok(Ok(record)) => record,
                Ok(Err(error)) => {
                    self.metrics.record_stream_failure();
                    eprintln!("webhook inbox read error: {error}");
                    tokio::time::sleep(WORKER_FAILURE_DELAY).await;
                    continue;
                }
                Err(_) => {
                    self.metrics.record_stream_failure();
                    tokio::time::sleep(WORKER_FAILURE_DELAY).await;
                    continue;
                }
            };
            let Some(record) = record else {
                tokio::time::sleep(WORKER_IDLE_DELAY).await;
                continue;
            };

            let remove = match self.ingest(record.webhook.clone()).await {
                Ok(outcome) => {
                    self.metrics.record_outcome(outcome);
                    true
                }
                Err(WebhookError::InactiveDeviceToken | WebhookError::InvalidRequest(_)) => {
                    self.metrics.record_outcome(IngestOutcome::Rejected);
                    true
                }
                Err(error) => {
                    self.metrics.record_stream_failure();
                    eprintln!("webhook ingestion error: {error}");
                    false
                }
            };
            if remove {
                let spool = self.spool.clone();
                let id = record.id;
                match tokio::task::spawn_blocking(move || spool.remove(id)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        self.metrics.record_stream_failure();
                        eprintln!("webhook inbox remove error: {error}");
                    }
                    Err(_) => self.metrics.record_stream_failure(),
                }
            } else {
                tokio::time::sleep(WORKER_FAILURE_DELAY).await;
            }
        }
    }

    async fn ingest(&self, webhook: NanoMqWebhook) -> Result<IngestOutcome, WebhookError> {
        if webhook.action != "message_publish" {
            return Err(WebhookError::InvalidRequest(
                "unsupported webhook action".to_owned(),
            ));
        }
        if webhook.qos != 1 {
            return Err(WebhookError::InvalidRequest(
                "telemetry must use QoS 1".to_owned(),
            ));
        }
        if webhook.ts < 0 {
            return Err(WebhookError::InvalidRequest(
                "invalid webhook timestamp".to_owned(),
            ));
        }

        let now = Utc::now();
        let topic = webhook.topic;
        let raw_payload = webhook.payload.into_bytes();
        let prefix = device_token_prefix(&webhook.from_username)
            .map_err(|_| WebhookError::InactiveDeviceToken)?;
        let mut transaction = self.pool.begin().await?;
        let token = resolve_token(&mut transaction, prefix, &webhook.from_username).await?;

        match topic.as_str() {
            DEVICE_TELEMETRY_TOPIC => {
                if token.is_gateway {
                    return Err(WebhookError::InvalidRequest(
                        "gateway tokens must use gateway topics".to_owned(),
                    ));
                }
                let payload = serde_json::from_slice::<DeviceTelemetryPayload>(&raw_payload)
                    .map_err(|_| {
                        WebhookError::InvalidRequest("invalid telemetry payload".to_owned())
                    })?;
                let event = payload
                    .into_event(&token.device_id)
                    .map_err(|error| WebhookError::InvalidRequest(error.to_string()))?;
                let outcome = append_event(&self.producer, topic, raw_payload, event, now).await?;
                if outcome == IngestOutcome::Rejected {
                    return Ok(outcome);
                }
                mark_token_used(&mut transaction, token.id).await?;
                transaction.commit().await?;
                Ok(outcome)
            }
            GATEWAY_TELEMETRY_TOPIC => {
                require_gateway(&token)?;
                let payload = serde_json::from_slice::<GatewayTelemetryPayload>(&raw_payload)
                    .map_err(|_| {
                        WebhookError::InvalidRequest("invalid gateway telemetry payload".to_owned())
                    })?;
                validate_event_at(payload.event_at(), now)?;
                mark_gateway_seen(&mut transaction, &token.device_id, now).await?;
                let Some(event) = payload
                    .into_event(&token.device_id)
                    .map_err(|error| WebhookError::InvalidRequest(error.to_string()))?
                else {
                    mark_token_used(&mut transaction, token.id).await?;
                    transaction.commit().await?;
                    return Ok(IngestOutcome::Accepted);
                };
                ensure_child_ownership(&mut transaction, &event.device_id, &token.device_id)
                    .await?;
                let child_device_id = event.device_id.clone();
                let event_at = event.event_at;
                let outcome = append_event(&self.producer, topic, raw_payload, event, now).await?;
                if outcome == IngestOutcome::Rejected {
                    return Ok(outcome);
                }
                sqlx::query(
                    "UPDATE devices
                     SET gateway_last_read_at = COALESCE(
                             GREATEST(gateway_last_read_at, $3),
                             $3
                         ),
                         gateway_read_quality = 'good'
                     WHERE device_id = $1 AND gateway_device_id = $2",
                )
                .bind(&child_device_id)
                .bind(&token.device_id)
                .bind(event_at)
                .execute(&mut *transaction)
                .await?;
                mark_token_used(&mut transaction, token.id).await?;
                transaction.commit().await?;
                Ok(outcome)
            }
            GATEWAY_CONNECT_TOPIC | GATEWAY_DISCONNECT_TOPIC => {
                require_gateway(&token)?;
                let payload = serde_json::from_slice::<GatewayChildLifecyclePayload>(&raw_payload)
                    .map_err(|_| {
                        WebhookError::InvalidRequest("invalid gateway lifecycle payload".to_owned())
                    })?;
                payload
                    .validate()
                    .map_err(|error| WebhookError::InvalidRequest(error.to_string()))?;
                validate_event_at(payload.event_at, now)?;
                ensure_child_ownership(
                    &mut transaction,
                    &payload.child_device_id,
                    &token.device_id,
                )
                .await?;
                mark_gateway_seen(&mut transaction, &token.device_id, now).await?;
                if topic == GATEWAY_DISCONNECT_TOPIC {
                    sqlx::query(
                        "UPDATE devices
                         SET gateway_read_quality = 'unavailable'
                         WHERE device_id = $1 AND gateway_device_id = $2",
                    )
                    .bind(&payload.child_device_id)
                    .bind(&token.device_id)
                    .execute(&mut *transaction)
                    .await?;
                }
                mark_token_used(&mut transaction, token.id).await?;
                transaction.commit().await?;
                Ok(IngestOutcome::Accepted)
            }
            _ => Err(WebhookError::InvalidRequest(
                "unexpected telemetry topic".to_owned(),
            )),
        }
    }
}

impl SqliteWebhookWorker {
    async fn run(self) {
        loop {
            let spool = self.spool.clone();
            let record = match tokio::task::spawn_blocking(move || spool.first()).await {
                Ok(Ok(record)) => record,
                Ok(Err(error)) => {
                    self.metrics.record_stream_failure();
                    eprintln!("webhook inbox read error: {error}");
                    tokio::time::sleep(WORKER_FAILURE_DELAY).await;
                    continue;
                }
                Err(_) => {
                    self.metrics.record_stream_failure();
                    tokio::time::sleep(WORKER_FAILURE_DELAY).await;
                    continue;
                }
            };
            let Some(record) = record else {
                tokio::time::sleep(WORKER_IDLE_DELAY).await;
                continue;
            };

            let remove = match self.ingest(record.webhook.clone()).await {
                Ok(outcome) => {
                    self.metrics.record_outcome(outcome);
                    true
                }
                Err(WebhookError::InactiveDeviceToken | WebhookError::InvalidRequest(_)) => {
                    self.metrics.record_outcome(IngestOutcome::Rejected);
                    true
                }
                Err(error) => {
                    self.metrics.record_stream_failure();
                    eprintln!("webhook ingestion error: {error}");
                    false
                }
            };
            if remove {
                let spool = self.spool.clone();
                let id = record.id;
                match tokio::task::spawn_blocking(move || spool.remove(id)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        self.metrics.record_stream_failure();
                        eprintln!("webhook inbox remove error: {error}");
                    }
                    Err(_) => self.metrics.record_stream_failure(),
                }
            } else {
                tokio::time::sleep(WORKER_FAILURE_DELAY).await;
            }
        }
    }

    async fn ingest(&self, webhook: NanoMqWebhook) -> Result<IngestOutcome, WebhookError> {
        if webhook.action != "message_publish" {
            return Err(WebhookError::InvalidRequest(
                "unsupported webhook action".to_owned(),
            ));
        }
        if webhook.qos != 1 {
            return Err(WebhookError::InvalidRequest(
                "telemetry must use QoS 1".to_owned(),
            ));
        }
        if webhook.ts < 0 {
            return Err(WebhookError::InvalidRequest(
                "invalid webhook timestamp".to_owned(),
            ));
        }

        let now = Utc::now();
        let topic = webhook.topic;
        let raw_payload = webhook.payload.into_bytes();
        let prefix = device_token_prefix(&webhook.from_username)
            .map_err(|_| WebhookError::InactiveDeviceToken)?;
        let mut transaction = self.pool.begin().await?;
        let token = resolve_sqlite_token(&mut transaction, prefix, &webhook.from_username).await?;

        match topic.as_str() {
            DEVICE_TELEMETRY_TOPIC => {
                if token.is_gateway {
                    return Err(WebhookError::InvalidRequest(
                        "gateway tokens must use gateway topics".to_owned(),
                    ));
                }
                let payload = serde_json::from_slice::<DeviceTelemetryPayload>(&raw_payload)
                    .map_err(|_| {
                        WebhookError::InvalidRequest("invalid telemetry payload".to_owned())
                    })?;
                let event = payload
                    .into_event(&token.device_id)
                    .map_err(|error| WebhookError::InvalidRequest(error.to_string()))?;
                mark_sqlite_token_used(&mut transaction, &token.id, now).await?;
                let outcome = append_event(&self.producer, topic, raw_payload, event, now).await?;
                if outcome == IngestOutcome::Rejected {
                    return Ok(outcome);
                }
                transaction.commit().await?;
                Ok(outcome)
            }
            GATEWAY_TELEMETRY_TOPIC => {
                require_sqlite_gateway(&token)?;
                let payload = serde_json::from_slice::<GatewayTelemetryPayload>(&raw_payload)
                    .map_err(|_| {
                        WebhookError::InvalidRequest("invalid gateway telemetry payload".to_owned())
                    })?;
                validate_event_at(payload.event_at(), now)?;
                mark_sqlite_gateway_seen(&mut transaction, &token.device_id, now).await?;
                let Some(event) = payload
                    .into_event(&token.device_id)
                    .map_err(|error| WebhookError::InvalidRequest(error.to_string()))?
                else {
                    mark_sqlite_token_used(&mut transaction, &token.id, now).await?;
                    transaction.commit().await?;
                    return Ok(IngestOutcome::Accepted);
                };
                ensure_sqlite_child_ownership(&mut transaction, &event.device_id, &token.device_id)
                    .await?;
                let child_device_id = event.device_id.clone();
                let event_at = event.event_at;
                mark_sqlite_token_used(&mut transaction, &token.id, now).await?;
                let outcome = append_event(&self.producer, topic, raw_payload, event, now).await?;
                if outcome == IngestOutcome::Rejected {
                    return Ok(outcome);
                }
                mark_sqlite_child_read(
                    &mut transaction,
                    &child_device_id,
                    &token.device_id,
                    event_at,
                )
                .await?;
                transaction.commit().await?;
                Ok(outcome)
            }
            GATEWAY_CONNECT_TOPIC | GATEWAY_DISCONNECT_TOPIC => {
                require_sqlite_gateway(&token)?;
                let payload = serde_json::from_slice::<GatewayChildLifecyclePayload>(&raw_payload)
                    .map_err(|_| {
                        WebhookError::InvalidRequest("invalid gateway lifecycle payload".to_owned())
                    })?;
                payload
                    .validate()
                    .map_err(|error| WebhookError::InvalidRequest(error.to_string()))?;
                validate_event_at(payload.event_at, now)?;
                ensure_sqlite_child_ownership(
                    &mut transaction,
                    &payload.child_device_id,
                    &token.device_id,
                )
                .await?;
                mark_sqlite_gateway_seen(&mut transaction, &token.device_id, now).await?;
                if topic == GATEWAY_DISCONNECT_TOPIC {
                    mark_sqlite_child_unavailable(
                        &mut transaction,
                        &payload.child_device_id,
                        &token.device_id,
                    )
                    .await?;
                }
                mark_sqlite_token_used(&mut transaction, &token.id, now).await?;
                transaction.commit().await?;
                Ok(IngestOutcome::Accepted)
            }
            _ => Err(WebhookError::InvalidRequest(
                "unexpected telemetry topic".to_owned(),
            )),
        }
    }
}

async fn resolve_token(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    prefix: &str,
    token: &str,
) -> Result<ResolvedDeviceToken, WebhookError> {
    let row = sqlx::query(
        "SELECT device_tokens.id, device_tokens.device_id, device_tokens.token_hash,
                devices.is_gateway
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.token_prefix = $1
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(prefix)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(WebhookError::InactiveDeviceToken)?;
    let hash = row.try_get::<String, _>("token_hash")?;
    if !verify_device_token(token, &hash).unwrap_or(false) {
        return Err(WebhookError::InactiveDeviceToken);
    }
    Ok(ResolvedDeviceToken {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        is_gateway: row.try_get("is_gateway")?,
    })
}

async fn resolve_sqlite_token(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    prefix: &str,
    token: &str,
) -> Result<SqliteResolvedDeviceToken, WebhookError> {
    let row = sqlx::query(
        "SELECT device_tokens.id, device_tokens.device_id, device_tokens.token_hash,
                devices.is_gateway
         FROM device_tokens
         JOIN devices ON devices.device_id = device_tokens.device_id
         WHERE device_tokens.token_prefix = ?
           AND device_tokens.revoked_at IS NULL
           AND devices.deleted_at IS NULL",
    )
    .bind(prefix)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(WebhookError::InactiveDeviceToken)?;
    let hash = row.try_get::<String, _>("token_hash")?;
    if !verify_device_token(token, &hash).unwrap_or(false) {
        return Err(WebhookError::InactiveDeviceToken);
    }
    Ok(SqliteResolvedDeviceToken {
        id: row.try_get("id")?,
        device_id: row.try_get("device_id")?,
        is_gateway: row.try_get::<i64, _>("is_gateway")? != 0,
    })
}

fn require_gateway(token: &ResolvedDeviceToken) -> Result<(), WebhookError> {
    if token.is_gateway {
        Ok(())
    } else {
        Err(WebhookError::InvalidRequest(
            "direct device tokens cannot use gateway topics".to_owned(),
        ))
    }
}

fn require_sqlite_gateway(token: &SqliteResolvedDeviceToken) -> Result<(), WebhookError> {
    if token.is_gateway {
        Ok(())
    } else {
        Err(WebhookError::InvalidRequest(
            "direct device tokens cannot use gateway topics".to_owned(),
        ))
    }
}

fn validate_event_at(event_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<(), WebhookError> {
    if event_at > now + MAX_FUTURE_EVENT_SKEW {
        return Err(WebhookError::InvalidRequest(
            "gateway event timestamp is too far in the future".to_owned(),
        ));
    }
    Ok(())
}

async fn ensure_child_ownership(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<(), WebhookError> {
    let owned = sqlx::query(
        "SELECT 1
         FROM devices
         WHERE device_id = $1
           AND gateway_device_id = $2
           AND deleted_at IS NULL
         FOR UPDATE",
    )
    .bind(child_device_id)
    .bind(gateway_device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if owned {
        Ok(())
    } else {
        Err(WebhookError::InvalidRequest(
            "gateway does not own child device".to_owned(),
        ))
    }
}

async fn ensure_sqlite_child_ownership(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<(), WebhookError> {
    let owned = sqlx::query(
        "SELECT 1
         FROM devices
         WHERE device_id = ?
           AND gateway_device_id = ?
           AND deleted_at IS NULL",
    )
    .bind(child_device_id)
    .bind(gateway_device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if owned {
        Ok(())
    } else {
        Err(WebhookError::InvalidRequest(
            "gateway does not own child device".to_owned(),
        ))
    }
}

async fn mark_gateway_seen(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    gateway_device_id: &str,
    seen_at: DateTime<Utc>,
) -> Result<(), WebhookError> {
    sqlx::query(
        "UPDATE devices
         SET last_seen_at = COALESCE(GREATEST(last_seen_at, $2), $2)
         WHERE device_id = $1 AND is_gateway = TRUE",
    )
    .bind(gateway_device_id)
    .bind(seen_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn mark_sqlite_gateway_seen(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    gateway_device_id: &str,
    seen_at: DateTime<Utc>,
) -> Result<(), WebhookError> {
    let seen_at = seen_at.to_rfc3339();
    sqlx::query(
        "UPDATE devices
         SET last_seen_at = CASE
                 WHEN last_seen_at IS NULL OR last_seen_at < ? THEN ?
                 ELSE last_seen_at
             END
         WHERE device_id = ? AND is_gateway = 1",
    )
    .bind(&seen_at)
    .bind(&seen_at)
    .bind(gateway_device_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn mark_sqlite_child_read(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    child_device_id: &str,
    gateway_device_id: &str,
    event_at: DateTime<Utc>,
) -> Result<(), WebhookError> {
    let event_at = event_at.to_rfc3339();
    sqlx::query(
        "UPDATE devices
         SET gateway_last_read_at = CASE
                 WHEN gateway_last_read_at IS NULL OR gateway_last_read_at < ? THEN ?
                 ELSE gateway_last_read_at
             END,
             gateway_read_quality = 'good'
         WHERE device_id = ? AND gateway_device_id = ?",
    )
    .bind(&event_at)
    .bind(&event_at)
    .bind(child_device_id)
    .bind(gateway_device_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn mark_sqlite_child_unavailable(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    child_device_id: &str,
    gateway_device_id: &str,
) -> Result<(), WebhookError> {
    sqlx::query(
        "UPDATE devices
         SET gateway_read_quality = 'unavailable'
         WHERE device_id = ? AND gateway_device_id = ?",
    )
    .bind(child_device_id)
    .bind(gateway_device_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn mark_token_used(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    token_id: Uuid,
) -> Result<(), WebhookError> {
    sqlx::query(
        "UPDATE device_tokens
         SET last_used_at = now()
         WHERE id = $1",
    )
    .bind(token_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn mark_sqlite_token_used(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    token_id: &str,
    used_at: DateTime<Utc>,
) -> Result<(), WebhookError> {
    let result = sqlx::query(
        "UPDATE device_tokens
         SET last_used_at = ?
         WHERE id = ? AND revoked_at IS NULL",
    )
    .bind(used_at.to_rfc3339())
    .bind(token_id)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(WebhookError::InactiveDeviceToken);
    }
    Ok(())
}

async fn append_event(
    producer: &MqttStreamProducer,
    topic: String,
    raw_payload: Vec<u8>,
    event: iot_core::TelemetryEvent,
    received_at: DateTime<Utc>,
) -> Result<IngestOutcome, WebhookError> {
    let producer = producer.clone();
    tokio::task::spawn_blocking(move || {
        producer.ingest_event(&topic, raw_payload, event, received_at)
    })
    .await
    .map_err(|_| WebhookError::Join)?
    .map_err(WebhookError::Stream)
}

impl WebhookSpool {
    fn open(directory: impl AsRef<Path>) -> Result<Self, WebhookError> {
        fs::create_dir_all(directory.as_ref())?;
        fs::set_permissions(directory.as_ref(), fs::Permissions::from_mode(0o700))?;
        let path = directory.as_ref().join("webhook-inbox.jsonl");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.sync_all()?;
        sync_parent(directory.as_ref())?;
        Ok(Self {
            path: Arc::new(path),
            lock: Arc::new(Mutex::new(())),
        })
    }

    fn append(&self, record: &SpoolRecord) -> Result<(), WebhookError> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if fs::metadata(&*self.path)?.len() >= MAX_WEBHOOK_INBOX_BYTES {
            return Err(WebhookError::SpoolFull);
        }
        let payload = serde_json::to_vec(record).map_err(WebhookError::SpoolSerialization)?;
        let mut file = OpenOptions::new()
            .append(true)
            .mode(0o600)
            .open(&*self.path)?;
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    fn first(&self) -> Result<Option<SpoolRecord>, WebhookError> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.records_locked()
            .map(|records| records.into_iter().next())
    }

    fn remove(&self, id: Uuid) -> Result<(), WebhookError> {
        let _guard = self
            .lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut records = self.records_locked()?;
        if records.first().is_none_or(|record| record.id != id) {
            return Err(WebhookError::Spool(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "webhook inbox order changed",
            )));
        }
        records.remove(0);
        let temporary = self
            .path
            .with_file_name(format!(".webhook-inbox-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&temporary)?;
        for record in records {
            serde_json::to_writer(&mut file, &record).map_err(WebhookError::SpoolSerialization)?;
            file.write_all(b"\n")?;
        }
        file.sync_all()?;
        fs::rename(&temporary, &*self.path)?;
        sync_parent(self.path.parent().expect("webhook inbox has a parent"))?;
        Ok(())
    }

    fn records_locked(&self) -> Result<Vec<SpoolRecord>, WebhookError> {
        let payload = fs::read(&*self.path)?;
        let complete_length = payload
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        if complete_length != payload.len() {
            let file = OpenOptions::new().write(true).open(&*self.path)?;
            file.set_len(u64::try_from(complete_length).expect("usize fits u64"))?;
            file.sync_data()?;
        }
        let complete = std::str::from_utf8(&payload[..complete_length]).map_err(|error| {
            WebhookError::Spool(std::io::Error::new(std::io::ErrorKind::InvalidData, error))
        })?;
        complete
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).map_err(WebhookError::SpoolSerialization))
            .collect()
    }
}

fn sync_parent(path: &Path) -> Result<(), std::io::Error> {
    File::open(path)?.sync_all()
}

pub fn webhook_router(ingress: TokenWebhookIngress) -> Router {
    webhook_router_with_optional_transport_secret(ingress, None)
}

pub fn webhook_router_with_transport_secret(
    ingress: TokenWebhookIngress,
    secret: impl AsRef<str>,
) -> Router {
    webhook_router_with_optional_transport_secret(ingress, Some(Arc::from(secret.as_ref())))
}

fn webhook_router_with_optional_transport_secret(
    ingress: TokenWebhookIngress,
    transport_secret: Option<Arc<str>>,
) -> Router {
    Router::new()
        .route("/internal/nanomq/telemetry", post(receive_webhook))
        .route(
            "/internal/mqtt-transport/telemetry",
            post(receive_transport_webhook),
        )
        .with_state(WebhookState {
            ingress,
            transport_secret,
        })
}

pub fn sqlite_webhook_router(ingress: SqliteTokenWebhookIngress) -> Router {
    sqlite_webhook_router_with_optional_transport_secret(ingress, None)
}

pub fn sqlite_webhook_router_with_transport_secret(
    ingress: SqliteTokenWebhookIngress,
    secret: impl AsRef<str>,
) -> Router {
    sqlite_webhook_router_with_optional_transport_secret(ingress, Some(Arc::from(secret.as_ref())))
}

fn sqlite_webhook_router_with_optional_transport_secret(
    ingress: SqliteTokenWebhookIngress,
    transport_secret: Option<Arc<str>>,
) -> Router {
    Router::new()
        .route("/internal/nanomq/telemetry", post(receive_sqlite_webhook))
        .route(
            "/internal/mqtt-transport/telemetry",
            post(receive_sqlite_transport_webhook),
        )
        .with_state(SqliteWebhookState {
            ingress,
            transport_secret,
        })
}

async fn receive_webhook(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    Json(webhook): Json<NanoMqWebhook>,
) -> Result<StatusCode, WebhookError> {
    let secret = headers
        .get("x-iot-nanomq-webhook")
        .and_then(|value| value.to_str().ok());
    state.ingress.enqueue(secret, webhook).await?;
    Ok(StatusCode::OK)
}

async fn receive_sqlite_webhook(
    State(state): State<SqliteWebhookState>,
    headers: HeaderMap,
    Json(webhook): Json<NanoMqWebhook>,
) -> Result<StatusCode, WebhookError> {
    let secret = headers
        .get("x-iot-nanomq-webhook")
        .and_then(|value| value.to_str().ok());
    state.ingress.enqueue(secret, webhook).await?;
    Ok(StatusCode::OK)
}

async fn receive_transport_webhook(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    Json(webhook): Json<NanoMqWebhook>,
) -> Result<StatusCode, WebhookError> {
    let expected = state
        .transport_secret
        .as_deref()
        .ok_or(WebhookError::Unauthorized)?;
    let supplied = headers
        .get("x-iot-mqtt-transport-webhook")
        .and_then(|value| value.to_str().ok())
        .ok_or(WebhookError::Unauthorized)?;
    if !constant_time_equal(expected.as_bytes(), supplied.as_bytes()) {
        return Err(WebhookError::Unauthorized);
    }
    state.ingress.enqueue_trusted(webhook).await?;
    Ok(StatusCode::OK)
}

async fn receive_sqlite_transport_webhook(
    State(state): State<SqliteWebhookState>,
    headers: HeaderMap,
    Json(webhook): Json<NanoMqWebhook>,
) -> Result<StatusCode, WebhookError> {
    let expected = state
        .transport_secret
        .as_deref()
        .ok_or(WebhookError::Unauthorized)?;
    let supplied = headers
        .get("x-iot-mqtt-transport-webhook")
        .and_then(|value| value.to_str().ok())
        .ok_or(WebhookError::Unauthorized)?;
    if !constant_time_equal(expected.as_bytes(), supplied.as_bytes()) {
        return Err(WebhookError::Unauthorized);
    }
    state.ingress.enqueue_trusted(webhook).await?;
    Ok(StatusCode::OK)
}

impl IntoResponse for WebhookError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::InactiveDeviceToken => StatusCode::FORBIDDEN,
            Self::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            Self::Database(_)
            | Self::Stream(_)
            | Self::Join
            | Self::Spool(_)
            | Self::SpoolSerialization(_)
            | Self::SpoolFull => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, self.to_string()).into_response()
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write, os::unix::fs::PermissionsExt};

    use super::{NanoMqWebhook, OpenOptions, SpoolRecord, WebhookSpool};
    use uuid::Uuid;

    fn record(sequence: u64) -> SpoolRecord {
        SpoolRecord {
            id: Uuid::new_v4(),
            webhook: NanoMqWebhook {
                action: "message_publish".to_owned(),
                from_username:
                    "iotd_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .to_owned(),
                topic: "v1/devices/me/telemetry".to_owned(),
                qos: 1,
                ts: 1,
                payload: format!(r#"{{"sequence":{sequence}}}"#),
            },
        }
    }

    #[test]
    fn webhook_inbox_survives_reopen_and_removes_records_in_order() {
        let tempdir = tempfile::tempdir().unwrap();
        let spool = WebhookSpool::open(tempdir.path()).unwrap();
        let first = record(1);
        let second = record(2);

        spool.append(&first).unwrap();
        spool.append(&second).unwrap();
        let reopened = WebhookSpool::open(tempdir.path()).unwrap();

        assert_eq!(reopened.first().unwrap().unwrap().id, first.id);
        reopened.remove(first.id).unwrap();
        assert_eq!(reopened.first().unwrap().unwrap().id, second.id);
        reopened.remove(second.id).unwrap();
        assert!(reopened.first().unwrap().is_none());
    }

    #[test]
    fn webhook_inbox_discards_a_torn_final_record_after_reopen() {
        let tempdir = tempfile::tempdir().unwrap();
        let spool = WebhookSpool::open(tempdir.path()).unwrap();
        let first = record(1);
        spool.append(&first).unwrap();
        let mut file = OpenOptions::new().append(true).open(&*spool.path).unwrap();
        file.write_all(b"{\"incomplete\":").unwrap();
        file.sync_data().unwrap();

        let reopened = WebhookSpool::open(tempdir.path()).unwrap();

        assert_eq!(reopened.first().unwrap().unwrap().id, first.id);
        assert!(fs::read(&*reopened.path).unwrap().ends_with(b"\n"));
    }

    #[test]
    fn reopening_an_inbox_restores_owner_only_permissions() {
        let tempdir = tempfile::tempdir().unwrap();
        let spool = WebhookSpool::open(tempdir.path()).unwrap();
        fs::set_permissions(&*spool.path, fs::Permissions::from_mode(0o644)).unwrap();

        let reopened = WebhookSpool::open(tempdir.path()).unwrap();

        assert_eq!(
            fs::metadata(&*reopened.path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
