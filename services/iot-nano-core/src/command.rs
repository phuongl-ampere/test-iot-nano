#![forbid(unsafe_code)]

use std::{future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use iot_nano_foundation::{RpcMode, RpcRequest};
use iot_storage::{
    CommandLifecycleRepository, CommandOutboxRecord as PlatformCommandOutboxRecord, PlatformStore,
    PlatformStoreError,
};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

const COMMAND_LEASE_DURATION: Duration = Duration::seconds(30);
const COMMAND_RETRY_DELAY: Duration = Duration::seconds(1);

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TransportRpcPublishRequest {
    pub tenant_id: Uuid,
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
    Platform(#[from] PlatformStoreError),
    #[error("command ID is not a UUID: {0}")]
    InvalidCommandId(String),
}

#[derive(Clone)]
pub struct PlatformCommandDispatcher<C> {
    store: Arc<PlatformStore>,
    transport: C,
    batch_size: u32,
    tenant_cursor: Arc<Mutex<Option<Uuid>>>,
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
            tenant_cursor: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<CommandDispatchResult, CommandError> {
        let mut result = CommandDispatchResult::default();
        let tenant_ids = {
            let mut cursor = self.tenant_cursor.lock().await;
            let tenant_ids = self
                .store
                .ready_command_tenants(now, *cursor, self.batch_size)
                .await?;
            if let Some(tenant_id) = tenant_ids.last().copied() {
                *cursor = Some(tenant_id);
            }
            tenant_ids
        };
        if tenant_ids.is_empty() {
            return Ok(result);
        }

        let mut commands = Vec::with_capacity(tenant_ids.len());
        for tenant_id in &tenant_ids {
            let mut claimed = CommandLifecycleRepository::claim_commands(
                self.store.as_ref(),
                *tenant_id,
                now,
                now + COMMAND_LEASE_DURATION,
                1,
            )
            .await?;
            let Some(record) = claimed.pop() else {
                continue;
            };

            result.claimed += 1;
            commands.push(PlatformClaimedCommand::from_record(record));
        }

        // Reserve one expiry mutation for every claimed command. A command that expires while
        // its transport call is in flight must be expired by ID, not displaced by stale backlog.
        let mut expiry_budget = self.batch_size.saturating_sub(
            u32::try_from(commands.len()).expect("claimed command count is bounded"),
        );
        for tenant_id in tenant_ids {
            if expiry_budget == 0 {
                break;
            }
            let expired = CommandLifecycleRepository::expire_due_commands(
                self.store.as_ref(),
                tenant_id,
                now,
                expiry_budget,
            )
            .await?;
            result.expired += expired.len();
            expiry_budget = expiry_budget.saturating_sub(
                u32::try_from(expired.len())
                    .expect("expired command count is bounded by its limit"),
            );
        }

        for command in commands {
            dispatch_platform_command(&self.store, &self.transport, &command, &mut result).await?;
        }

        Ok(result)
    }
}

#[derive(Debug, Clone)]
struct PlatformClaimedCommand {
    id: String,
    tenant_id: Uuid,
    device_id: String,
    method: String,
    params: String,
    mode: RpcMode,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

impl PlatformClaimedCommand {
    fn from_record(command: PlatformCommandOutboxRecord) -> Self {
        Self {
            id: command.id,
            tenant_id: command.tenant_id,
            device_id: command.device_id,
            method: command.method,
            params: command.params,
            mode: command.mode,
            issued_at: command.created_at,
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
        tenant_id: command.tenant_id,
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

    let command_id = Uuid::parse_str(&command.id)
        .map_err(|_| CommandError::InvalidCommandId(command.id.clone()))?;
    match platform_command_request(command) {
        Ok(request) => match transport.publish(request).await {
            Ok(()) => {
                let completed_at = Utc::now();
                if expire_platform_command_if_elapsed(store, command, completed_at, result).await? {
                    return Ok(false);
                }
                if CommandLifecycleRepository::mark_command_published(
                    store,
                    command.tenant_id,
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
                        command.tenant_id,
                        command_id,
                        &error.to_string(),
                        Utc::now() + COMMAND_RETRY_DELAY,
                    )
                    .await?;
                    return Ok(true);
                } else if CommandLifecycleRepository::mark_command_failed(
                    store,
                    command.tenant_id,
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
            if CommandLifecycleRepository::mark_command_failed(
                store,
                command.tenant_id,
                command_id,
                &error,
            )
            .await?
            .is_some()
            {
                result.failed += 1;
            }
            Ok(false)
        }
    }
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

    let command_id = Uuid::parse_str(&command.id)
        .map_err(|_| CommandError::InvalidCommandId(command.id.clone()))?;
    if CommandLifecycleRepository::expire_command_if_elapsed(
        store,
        command.tenant_id,
        command_id,
        now,
    )
    .await?
    .is_some()
    {
        result.expired += 1;
    }
    Ok(true)
}
