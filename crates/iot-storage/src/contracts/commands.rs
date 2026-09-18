use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use iot_core::RpcMode;

use crate::{PlatformStoreError, SqliteStoreError};

pub trait CommandRepository: Send + Sync {
    fn enqueue_command<'a>(
        &'a self,
        command: NewCommandOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<CommandOutboxRecord, PlatformStoreError>> + Send + 'a>>;
}

pub trait NotificationRepository: Send + Sync {
    fn claim_notifications<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn mark_notification_sent<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        sent_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn release_notification_for_retry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        notification_id: uuid::Uuid,
        expected_lease_until: DateTime<Utc>,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<NotificationOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
}

pub trait CommandLifecycleRepository: Send + Sync {
    fn claim_commands<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    >;
    fn mark_command_published<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        published_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn mark_command_failed<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn release_command_for_retry<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        error: &'a str,
        next_attempt_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn expire_due_commands<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Pin<
        Box<dyn Future<Output = Result<Vec<CommandOutboxRecord>, PlatformStoreError>> + Send + 'a>,
    >;
    fn expire_command_if_elapsed<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        now: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
    fn mark_command_responded<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        command_id: uuid::Uuid,
        device_id: &'a str,
        token_id: uuid::Uuid,
        response: &'a str,
        responded_at: DateTime<Utc>,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<CommandOutboxRecord>, PlatformStoreError>>
                + Send
                + 'a,
        >,
    >;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandOutboxState {
    Queued,
    Leased,
    PublishedToBroker,
    Responded,
    Expired,
    Failed,
}

impl CommandOutboxState {
    pub(crate) fn from_database(value: &str) -> Result<Self, SqliteStoreError> {
        match value {
            "queued" => Ok(Self::Queued),
            "leased" => Ok(Self::Leased),
            "published_to_broker" => Ok(Self::PublishedToBroker),
            "responded" => Ok(Self::Responded),
            "expired" => Ok(Self::Expired),
            "failed" => Ok(Self::Failed),
            _ => Err(SqliteStoreError::InvalidCommandState(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewCommandOutboxEntry {
    pub id: String,
    pub tenant_id: uuid::Uuid,
    pub device_id: String,
    pub method: String,
    pub params: String,
    pub mode: RpcMode,
    pub expires_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutboxRecord {
    pub id: String,
    pub tenant_id: uuid::Uuid,
    pub device_id: String,
    pub method: String,
    pub params: String,
    pub mode: RpcMode,
    pub state: CommandOutboxState,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub response: Option<String>,
    pub responded_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    Opened,
    Resolved,
    Reminder,
}

impl NotificationKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::Resolved => "resolved",
            Self::Reminder => "reminder",
        }
    }

    pub(crate) fn from_database(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "opened" => Ok(Self::Opened),
            "resolved" => Ok(Self::Resolved),
            "reminder" => Ok(Self::Reminder),
            _ => Err(PlatformStoreError::InvalidNotificationKind(
                value.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationOutboxState {
    Pending,
    Leased,
    Sent,
}

impl NotificationOutboxState {
    pub(crate) fn from_database(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "sent" => Ok(Self::Sent),
            _ => Err(PlatformStoreError::InvalidNotificationState(
                value.to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationOutboxRecord {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub incident_id: uuid::Uuid,
    pub kind: NotificationKind,
    pub dedupe_key: String,
    pub subject: String,
    pub body: String,
    pub state: NotificationOutboxState,
    pub next_attempt_at: DateTime<Utc>,
    pub lease_until: Option<DateTime<Utc>>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub sent_at: Option<DateTime<Utc>>,
}
