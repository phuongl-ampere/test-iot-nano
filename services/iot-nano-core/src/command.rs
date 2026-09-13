#![forbid(unsafe_code)]

use std::{
    collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration as StdDuration,
};

use crate::{CommandOutboxRecord, CoreSqliteStore, CoreSqliteStoreError};
use chrono::{DateTime, Duration, Utc};
use iot_core::{RpcMode, RpcRequest};
use iot_storage::{
    CommandLifecycleRepository, CommandOutboxRecord as PlatformCommandOutboxRecord, PlatformStore,
    PlatformStoreError,
};
use serde_json::Value;
use sqlx::{PgPool, Row};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use uuid::Uuid;

const COMMAND_LEASE_DURATION: Duration = Duration::seconds(30);
const COMMAND_RETRY_DELAY: Duration = Duration::seconds(1);
const TRANSPORT_RPC_PATH: &str = "/internal/rpc/publish";
const MAX_HTTP_RESPONSE_HEADER_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TransportRpcPublishRequest {
    pub device_id: String,
    pub id: Uuid,
    pub method: String,
    pub params: Value,
    pub mode: RpcMode,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub trait CommandTransport: Send + Sync {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>>;
}

impl<T> CommandTransport for Arc<T>
where
    T: CommandTransport + ?Sized,
{
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        self.as_ref().publish(request)
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum CommandTransportError {
    #[error("transport is unavailable: {0}")]
    Unavailable(String),
    #[error("transport configuration is invalid: {0}")]
    Configuration(String),
    #[error("device has no active MQTT session")]
    NoActiveSession,
    #[error("transport rejected the command: {0}")]
    Rejected(String),
}

#[derive(Debug, Clone)]
pub struct HttpTransportRpcClient {
    authority: String,
    host_header: String,
    request_path: String,
    secret: String,
    timeout: StdDuration,
}

impl HttpTransportRpcClient {
    pub fn new(
        base_url: impl Into<String>,
        secret: impl Into<String>,
    ) -> Result<Self, CommandTransportError> {
        let (authority, request_path) = transport_url_parts(&base_url.into())?;
        let secret = secret.into();
        if secret.len() < 32
            || !secret.is_ascii()
            || secret.bytes().any(|byte| byte.is_ascii_whitespace())
        {
            return Err(CommandTransportError::Configuration(
                "transport secret must be at least 32 ASCII non-whitespace characters".to_owned(),
            ));
        }
        Ok(Self {
            host_header: authority.clone(),
            authority,
            request_path,
            secret,
            timeout: StdDuration::from_secs(15),
        })
    }

    pub fn with_timeout(mut self, timeout: StdDuration) -> Self {
        self.timeout = timeout;
        self
    }

    async fn publish_request(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Result<(), CommandTransportError> {
        let body = serde_json::to_vec(&request).map_err(|_| {
            CommandTransportError::Configuration("request is not serializable".to_owned())
        })?;
        let request_head = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nx-iot-nano-core-mqttd-secret: {}\r\nConnection: close\r\n\r\n",
            self.request_path,
            self.host_header,
            body.len(),
            self.secret,
        );
        let authority = self.authority.clone();
        let exchange = async {
            let mut stream = TcpStream::connect(&authority)
                .await
                .map_err(|error| CommandTransportError::Unavailable(error.to_string()))?;
            stream
                .write_all(request_head.as_bytes())
                .await
                .map_err(|error| CommandTransportError::Unavailable(error.to_string()))?;
            stream
                .write_all(&body)
                .await
                .map_err(|error| CommandTransportError::Unavailable(error.to_string()))?;
            stream
                .flush()
                .await
                .map_err(|error| CommandTransportError::Unavailable(error.to_string()))?;
            read_http_status(&mut stream).await
        };
        timeout(self.timeout, exchange).await.map_err(|_| {
            CommandTransportError::Unavailable("transport request timed out".to_owned())
        })?
    }
}

impl CommandTransport for HttpTransportRpcClient {
    fn publish(
        &self,
        request: TransportRpcPublishRequest,
    ) -> Pin<Box<dyn Future<Output = Result<(), CommandTransportError>> + Send + '_>> {
        Box::pin(self.publish_request(request))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommandDispatchResult {
    pub expired: usize,
    pub claimed: usize,
    pub published: usize,
    pub failed: usize,
}

#[derive(Debug, Error)]
pub enum CommandError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Sqlite(#[from] CoreSqliteStoreError),
    #[error(transparent)]
    Platform(#[from] PlatformStoreError),
    #[error("command ID is not a UUID: {0}")]
    InvalidCommandId(String),
}

#[derive(Clone)]
pub struct SqliteCommandDispatcher<C> {
    store: CoreSqliteStore,
    transport: C,
    batch_size: u32,
}

impl<C> SqliteCommandDispatcher<C>
where
    C: CommandTransport,
{
    pub fn new(store: CoreSqliteStore, transport: C, batch_size: u32) -> Self {
        Self {
            store,
            transport,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CommandDispatchResult, CommandError> {
        let mut result = CommandDispatchResult {
            expired: self.store.expire_commands(now).await?.len(),
            ..CommandDispatchResult::default()
        };
        let commands = self
            .store
            .claim_commands(now, now + COMMAND_LEASE_DURATION, self.batch_size)
            .await?;
        result.claimed = commands.len();

        for command in commands.into_iter().map(ClaimedCommand::from) {
            dispatch_sqlite_command(&self.store, &self.transport, &command, &mut result).await?;
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct CommandDispatcher<C> {
    pool: PgPool,
    transport: C,
    batch_size: u32,
}

impl<C> CommandDispatcher<C>
where
    C: CommandTransport,
{
    pub fn new(pool: PgPool, transport: C, batch_size: u32) -> Self {
        Self {
            pool,
            transport,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CommandDispatchResult, CommandError> {
        let mut result = CommandDispatchResult {
            expired: expire_postgres_commands(&self.pool, now).await? as usize,
            ..CommandDispatchResult::default()
        };
        let commands = claim_postgres_commands(
            &self.pool,
            now,
            now + COMMAND_LEASE_DURATION,
            self.batch_size,
        )
        .await?;
        result.claimed = commands.len();

        for command in commands {
            dispatch_postgres_command(&self.pool, &self.transport, &command, &mut result).await?;
        }
        Ok(result)
    }
}

#[derive(Clone)]
pub struct PlatformCommandDispatcher<C> {
    store: Arc<PlatformStore>,
    transport: C,
    batch_size: u32,
    issue_times: Arc<tokio::sync::Mutex<HashMap<String, DateTime<Utc>>>>,
}

impl<C> PlatformCommandDispatcher<C>
where
    C: CommandTransport,
{
    pub fn new(store: Arc<PlatformStore>, transport: C, batch_size: u32) -> Self {
        Self {
            store,
            transport,
            batch_size: batch_size.max(1),
            issue_times: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CommandDispatchResult, CommandError> {
        let mut result = CommandDispatchResult {
            expired: CommandLifecycleRepository::expire_commands(self.store.as_ref(), now)
                .await?
                .len(),
            ..CommandDispatchResult::default()
        };
        let commands = CommandLifecycleRepository::claim_commands(
            self.store.as_ref(),
            now,
            now + COMMAND_LEASE_DURATION,
            self.batch_size,
        )
        .await?;
        result.claimed = commands.len();

        for record in commands {
            let issued_at = {
                let mut issue_times = self.issue_times.lock().await;
                *issue_times
                    .entry(record.id.clone())
                    .or_insert(record.next_attempt_at)
            };
            let command = PlatformClaimedCommand::from_record(record, issued_at);
            let retrying =
                dispatch_platform_command(&self.store, &self.transport, &command, &mut result)
                    .await?;
            if !retrying {
                self.issue_times.lock().await.remove(&command.id);
            }
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct ClaimedCommand {
    id: String,
    device_id: String,
    method: String,
    params: String,
    mode: RpcMode,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl From<CommandOutboxRecord> for ClaimedCommand {
    fn from(command: CommandOutboxRecord) -> Self {
        Self {
            id: command.id,
            device_id: command.device_id,
            method: command.method,
            params: command.params,
            mode: command.mode,
            issued_at: command.next_attempt_at,
            expires_at: command.expires_at,
        }
    }
}

#[derive(Debug, Clone)]
struct PlatformClaimedCommand {
    id: String,
    device_id: String,
    method: String,
    params: String,
    mode: RpcMode,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl PlatformClaimedCommand {
    fn from_record(command: PlatformCommandOutboxRecord, issued_at: DateTime<Utc>) -> Self {
        Self {
            id: command.id,
            device_id: command.device_id,
            method: command.method,
            params: command.params,
            mode: command.mode,
            issued_at,
            expires_at: command.expires_at,
        }
    }
}

fn platform_command_request(
    command: &PlatformClaimedCommand,
) -> Result<TransportRpcPublishRequest, String> {
    let id = Uuid::parse_str(&command.id).map_err(|_| "command ID is not a UUID".to_owned())?;
    let params = serde_json::from_str::<Value>(&command.params)
        .map_err(|_| "command params are invalid JSON".to_owned())?;
    let request = RpcRequest::with_mode(
        id,
        &command.method,
        params,
        command.issued_at,
        command.expires_at,
        command.mode,
    )
    .map_err(|error| format!("invalid command request: {error}"))?;
    Ok(TransportRpcPublishRequest {
        device_id: command.device_id.clone(),
        id: request.id,
        method: request.method,
        params: request.params,
        mode: request.mode,
        issued_at: request.issued_at,
        expires_at: request.expires_at,
    })
}

async fn dispatch_platform_command<C>(
    store: &PlatformStore,
    transport: &C,
    command: &PlatformClaimedCommand,
    result: &mut CommandDispatchResult,
) -> Result<bool, CommandError>
where
    C: CommandTransport,
{
    if expire_platform_command_if_elapsed(store, command, Utc::now(), result).await? {
        return Ok(false);
    }

    let command_id = match Uuid::parse_str(&command.id) {
        Ok(command_id) => command_id,
        Err(_) => {
            mark_invalid_platform_command_failed(store, command, result).await?;
            return Ok(false);
        }
    };
    match platform_command_request(command) {
        Ok(request) => match transport.publish(request).await {
            Ok(()) => {
                let completed_at = Utc::now();
                if expire_platform_command_if_elapsed(store, command, completed_at, result).await? {
                    return Ok(false);
                }
                if CommandLifecycleRepository::mark_command_published(
                    store,
                    command_id,
                    completed_at,
                )
                .await?
                .is_some()
                {
                    result.published += 1;
                }
                Ok(false)
            }
            Err(error) => {
                if expire_platform_command_if_elapsed(store, command, Utc::now(), result).await? {
                    return Ok(false);
                }
                if matches!(
                    error,
                    CommandTransportError::Unavailable(_) | CommandTransportError::NoActiveSession
                ) {
                    let _ = CommandLifecycleRepository::release_command_for_retry(
                        store,
                        command_id,
                        &error.to_string(),
                        Utc::now() + COMMAND_RETRY_DELAY,
                    )
                    .await?;
                    return Ok(true);
                } else if CommandLifecycleRepository::mark_command_failed(
                    store,
                    command_id,
                    &error.to_string(),
                )
                .await?
                .is_some()
                {
                    result.failed += 1;
                }
                Ok(false)
            }
        },
        Err(error) => {
            if CommandLifecycleRepository::mark_command_failed(store, command_id, &error)
                .await?
                .is_some()
            {
                result.failed += 1;
            }
            Ok(false)
        }
    }
}

async fn mark_invalid_platform_command_failed(
    store: &PlatformStore,
    command: &PlatformClaimedCommand,
    result: &mut CommandDispatchResult,
) -> Result<(), CommandError> {
    const INVALID_COMMAND_ID_ERROR: &str = "command ID is not a UUID";

    // PostgreSQL enforces UUID command IDs; only legacy SQLite rows can violate this invariant.
    let marked = match store {
        PlatformStore::Sqlite(store) => store
            .mark_command_failed(&command.id, INVALID_COMMAND_ID_ERROR)
            .await
            .map_err(PlatformStoreError::from)?,
        PlatformStore::Timescale(_) => {
            return Err(CommandError::InvalidCommandId(command.id.clone()));
        }
    };
    if marked.is_some() {
        result.failed += 1;
    }
    Ok(())
}

async fn expire_platform_command_if_elapsed(
    store: &PlatformStore,
    command: &PlatformClaimedCommand,
    now: DateTime<Utc>,
    result: &mut CommandDispatchResult,
) -> Result<bool, CommandError> {
    if command.expires_at > now {
        return Ok(false);
    }

    result.expired += CommandLifecycleRepository::expire_commands(store, now)
        .await?
        .len();
    Ok(true)
}

fn command_request(command: &ClaimedCommand) -> Result<TransportRpcPublishRequest, String> {
    let id = Uuid::parse_str(&command.id).map_err(|_| "command ID is not a UUID".to_owned())?;
    let params = serde_json::from_str::<Value>(&command.params)
        .map_err(|_| "command params are invalid JSON".to_owned())?;
    let request = RpcRequest::with_mode(
        id,
        &command.method,
        params,
        command.issued_at,
        command.expires_at,
        command.mode,
    )
    .map_err(|error| format!("invalid command request: {error}"))?;
    Ok(TransportRpcPublishRequest {
        device_id: command.device_id.clone(),
        id: request.id,
        method: request.method,
        params: request.params,
        mode: request.mode,
        issued_at: request.issued_at,
        expires_at: request.expires_at,
    })
}

async fn dispatch_sqlite_command<C>(
    store: &CoreSqliteStore,
    transport: &C,
    command: &ClaimedCommand,
    result: &mut CommandDispatchResult,
) -> Result<(), CommandError>
where
    C: CommandTransport,
{
    if expire_sqlite_command_if_elapsed(store, command, Utc::now(), result).await? {
        return Ok(());
    }

    match command_request(command) {
        Ok(request) => match transport.publish(request).await {
            Ok(()) => {
                let completed_at = Utc::now();
                if expire_sqlite_command_if_elapsed(store, command, completed_at, result).await? {
                    return Ok(());
                }
                if store
                    .mark_command_published(&command.id, completed_at)
                    .await?
                    .is_some()
                {
                    result.published += 1;
                }
            }
            Err(error) => {
                if expire_sqlite_command_if_elapsed(store, command, Utc::now(), result).await? {
                    return Ok(());
                }
                if retryable_transport_error(&error) {
                    let _ = store
                        .release_command_for_retry(
                            &command.id,
                            &error.to_string(),
                            Utc::now() + COMMAND_RETRY_DELAY,
                        )
                        .await?;
                    return Ok(());
                }
                if store
                    .mark_command_failed(&command.id, &error.to_string())
                    .await?
                    .is_some()
                {
                    result.failed += 1;
                }
            }
        },
        Err(error) => {
            if store
                .mark_command_failed(&command.id, &error)
                .await?
                .is_some()
            {
                result.failed += 1;
            }
        }
    }
    Ok(())
}

async fn expire_sqlite_command_if_elapsed(
    store: &CoreSqliteStore,
    command: &ClaimedCommand,
    now: DateTime<Utc>,
    result: &mut CommandDispatchResult,
) -> Result<bool, CommandError> {
    if command.expires_at > now {
        return Ok(false);
    }

    result.expired += store.expire_commands(now).await?.len();
    Ok(true)
}

async fn dispatch_postgres_command<C>(
    pool: &PgPool,
    transport: &C,
    command: &ClaimedCommand,
    result: &mut CommandDispatchResult,
) -> Result<(), CommandError>
where
    C: CommandTransport,
{
    if expire_postgres_command_if_elapsed(pool, command, Utc::now(), result).await? {
        return Ok(());
    }

    match command_request(command) {
        Ok(request) => match transport.publish(request).await {
            Ok(()) => {
                let completed_at = Utc::now();
                if expire_postgres_command_if_elapsed(pool, command, completed_at, result).await? {
                    return Ok(());
                }
                if mark_postgres_command_published(pool, &command.id, completed_at).await? {
                    result.published += 1;
                }
            }
            Err(error) => {
                if expire_postgres_command_if_elapsed(pool, command, Utc::now(), result).await? {
                    return Ok(());
                }
                if retryable_transport_error(&error) {
                    let _ = release_postgres_command_for_retry(
                        pool,
                        &command.id,
                        &error.to_string(),
                        Utc::now() + COMMAND_RETRY_DELAY,
                    )
                    .await?;
                    return Ok(());
                }
                if mark_postgres_command_failed(pool, &command.id, &error.to_string()).await? {
                    result.failed += 1;
                }
            }
        },
        Err(error) => {
            if mark_postgres_command_failed(pool, &command.id, &error).await? {
                result.failed += 1;
            }
        }
    }
    Ok(())
}

async fn expire_postgres_command_if_elapsed(
    pool: &PgPool,
    command: &ClaimedCommand,
    now: DateTime<Utc>,
    result: &mut CommandDispatchResult,
) -> Result<bool, CommandError> {
    if command.expires_at > now {
        return Ok(false);
    }

    result.expired += expire_postgres_commands(pool, now).await? as usize;
    Ok(true)
}

async fn claim_postgres_commands(
    pool: &PgPool,
    now: DateTime<Utc>,
    lease_until: DateTime<Utc>,
    limit: u32,
) -> Result<Vec<ClaimedCommand>, sqlx::Error> {
    let rows = sqlx::query(
        "WITH due AS (
            SELECT id
            FROM command_outbox
            WHERE expires_at > $1
              AND (
                  (state = 'queued' AND next_attempt_at <= $1)
                  OR (state = 'leased' AND lease_until <= $1)
              )
            ORDER BY next_attempt_at, created_at, id
            LIMIT $2
            FOR UPDATE SKIP LOCKED
         )
         UPDATE command_outbox AS command
         SET state = 'leased',
             lease_until = $3,
             attempt_count = command.attempt_count + 1
         FROM due
         WHERE command.id = due.id
         RETURNING command.id, command.device_id, command.method, command.params,
                   command.mode, command.next_attempt_at, command.expires_at",
    )
    .bind(now)
    .bind(i64::from(limit))
    .bind(lease_until)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(ClaimedCommand {
                id: row.try_get::<Uuid, _>("id")?.to_string(),
                device_id: row.try_get("device_id")?,
                method: row.try_get("method")?,
                params: row
                    .try_get::<sqlx::types::Json<Value>, _>("params")?
                    .0
                    .to_string(),
                mode: postgres_rpc_mode(&row.try_get::<String, _>("mode")?)?,
                issued_at: row.try_get("next_attempt_at")?,
                expires_at: row.try_get("expires_at")?,
            })
        })
        .collect()
}

async fn expire_postgres_commands(pool: &PgPool, now: DateTime<Utc>) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "UPDATE command_outbox
         SET state = 'expired',
             lease_until = NULL
         WHERE (
                state IN ('queued', 'leased')
                OR (state = 'published_to_broker' AND mode = 'two_way')
               )
           AND expires_at <= $1",
    )
    .bind(now)
    .execute(pool)
    .await?
    .rows_affected())
}

async fn mark_postgres_command_published(
    pool: &PgPool,
    command_id: &str,
    published_at: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let command_id = Uuid::parse_str(command_id)
        .map_err(|_| sqlx::Error::Protocol("command ID is not a UUID".into()))?;
    Ok(sqlx::query(
        "UPDATE command_outbox
         SET state = 'published_to_broker',
             published_at = $1,
             lease_until = NULL
         WHERE id = $2
           AND state = 'leased'
           AND expires_at > $1",
    )
    .bind(published_at)
    .bind(command_id)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

async fn mark_postgres_command_failed(
    pool: &PgPool,
    command_id: &str,
    error: &str,
) -> Result<bool, sqlx::Error> {
    let command_id = Uuid::parse_str(command_id)
        .map_err(|_| sqlx::Error::Protocol("command ID is not a UUID".into()))?;
    Ok(sqlx::query(
        "UPDATE command_outbox
         SET state = 'failed',
             last_error = $1,
             lease_until = NULL
         WHERE id = $2
           AND state = 'leased'",
    )
    .bind(error)
    .bind(command_id)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

async fn release_postgres_command_for_retry(
    pool: &PgPool,
    command_id: &str,
    error: &str,
    next_attempt_at: DateTime<Utc>,
) -> Result<bool, sqlx::Error> {
    let command_id = Uuid::parse_str(command_id)
        .map_err(|_| sqlx::Error::Protocol("command ID is not a UUID".into()))?;
    Ok(sqlx::query(
        "UPDATE command_outbox
         SET state = 'queued',
             next_attempt_at = $1,
             last_error = $2,
             lease_until = NULL
         WHERE id = $3
           AND state = 'leased'
           AND expires_at > $1",
    )
    .bind(next_attempt_at)
    .bind(error)
    .bind(command_id)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

fn retryable_transport_error(error: &CommandTransportError) -> bool {
    matches!(error, CommandTransportError::NoActiveSession)
}

fn postgres_rpc_mode(value: &str) -> Result<RpcMode, sqlx::Error> {
    match value {
        "one_way" => Ok(RpcMode::OneWay),
        "two_way" => Ok(RpcMode::TwoWay),
        _ => Err(sqlx::Error::Protocol("command mode is invalid".into())),
    }
}

async fn read_http_status(stream: &mut TcpStream) -> Result<(), CommandTransportError> {
    let mut response = Vec::new();
    let mut buffer = [0_u8; 1_024];
    loop {
        if response.len() >= MAX_HTTP_RESPONSE_HEADER_BYTES {
            return Err(CommandTransportError::Unavailable(
                "transport response headers exceeded the limit".to_owned(),
            ));
        }
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|error| CommandTransportError::Unavailable(error.to_string()))?;
        if read == 0 {
            return Err(CommandTransportError::Unavailable(
                "transport closed the response before sending headers".to_owned(),
            ));
        }
        response.extend_from_slice(&buffer[..read]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let response = std::str::from_utf8(&response)
        .map_err(|_| CommandTransportError::Unavailable("invalid HTTP response".to_owned()))?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| CommandTransportError::Unavailable("invalid HTTP status line".to_owned()))?;
    match status {
        204 => Ok(()),
        503 => Err(CommandTransportError::NoActiveSession),
        status => Err(CommandTransportError::Rejected(format!(
            "transport returned HTTP status {status}"
        ))),
    }
}

fn transport_url_parts(base_url: &str) -> Result<(String, String), CommandTransportError> {
    let Some(rest) = base_url.strip_prefix("http://") else {
        if let Some((scheme, _)) = base_url.split_once("://") {
            return Err(CommandTransportError::Configuration(format!(
                "transport URL scheme {scheme:?} is unsupported"
            )));
        }
        return Err(CommandTransportError::Configuration(
            "expected an http:// URL".to_owned(),
        ));
    };
    if rest.is_empty()
        || rest
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || rest.contains(['?', '#', '@'])
    {
        return Err(CommandTransportError::Configuration(
            "host, port, and optional path are required without query or fragment".to_owned(),
        ));
    }
    let (authority, base_path) = rest.split_once('/').unwrap_or((rest, ""));
    if authority.is_empty() || !authority.contains(':') {
        return Err(CommandTransportError::Configuration(
            "host and port are required".to_owned(),
        ));
    }
    let base_path = base_path.trim_end_matches('/');
    let request_path = if base_path.is_empty() {
        TRANSPORT_RPC_PATH.to_owned()
    } else {
        format!("/{base_path}{TRANSPORT_RPC_PATH}")
    };
    Ok((authority.to_owned(), request_path))
}
