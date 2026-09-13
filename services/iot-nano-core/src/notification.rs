use std::{
    env,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use iot_storage::{NotificationRepository, PlatformStore, PlatformStoreError};
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor, message::Mailbox,
    transport::smtp::authentication::Credentials,
};
use sqlx::{PgPool, Row, SqlitePool};
use thiserror::Error;
use tokio::time::timeout;
use uuid::Uuid;

const DEFAULT_LEASE_DURATION: Duration = Duration::seconds(30);
const SMTP_SEND_TIMEOUT: StdDuration = StdDuration::from_secs(15);

pub trait EmailSender: Send + Sync {
    fn send(
        &self,
        subject: String,
        body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>>;
}

#[derive(Debug, Error)]
pub enum NotificationError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    PlatformStorage(#[from] PlatformStoreError),
    #[error("{0}")]
    Send(String),
    #[error("SMTP configuration is incomplete: {0}")]
    Configuration(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SmtpConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub from: String,
    pub to: String,
    pub timeout: StdDuration,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SmtpConfigInput {
    pub host: Option<String>,
    pub port: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub timeout_seconds: Option<String>,
}

impl SmtpConfig {
    pub fn from_env() -> Result<Option<Self>, NotificationError> {
        Self::from_input(SmtpConfigInput {
            host: optional_env("SMTP_HOST"),
            port: optional_env("SMTP_PORT"),
            username: optional_env("SMTP_USERNAME"),
            password: optional_env("SMTP_PASSWORD"),
            from: optional_env("ALERT_EMAIL_FROM"),
            to: optional_env("ALERT_EMAIL_TO"),
            timeout_seconds: optional_env("SMTP_TIMEOUT_SECONDS"),
        })
    }

    pub fn from_file(path: &Path) -> Result<Option<Self>, NotificationError> {
        let source = std::fs::read_to_string(path).map_err(|error| {
            NotificationError::Configuration(format!("{}: {error}", path.display()))
        })?;
        let mut input = SmtpConfigInput::default();
        for (line_number, line) in source.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line.split_once('=').ok_or_else(|| {
                NotificationError::Configuration(format!(
                    "{}:{} is not an environment assignment",
                    path.display(),
                    line_number + 1
                ))
            })?;
            let value = parse_environment_value(value).map_err(NotificationError::Configuration)?;
            match key {
                "SMTP_HOST" => input.host = Some(value),
                "SMTP_PORT" => input.port = Some(value),
                "SMTP_USERNAME" => input.username = Some(value),
                "SMTP_PASSWORD" => input.password = Some(value),
                "ALERT_EMAIL_FROM" => input.from = Some(value),
                "ALERT_EMAIL_TO" => input.to = Some(value),
                "SMTP_TIMEOUT_SECONDS" => input.timeout_seconds = Some(value),
                _ => {}
            }
        }
        Self::from_input(input)
    }

    pub fn from_input(input: SmtpConfigInput) -> Result<Option<Self>, NotificationError> {
        let required = [
            ("SMTP_HOST", input.host),
            ("SMTP_USERNAME", input.username),
            ("SMTP_PASSWORD", input.password),
            ("ALERT_EMAIL_FROM", input.from),
            ("ALERT_EMAIL_TO", input.to),
        ];
        if required.iter().all(|(_, value)| value.is_none()) {
            return Ok(None);
        }
        let missing = required
            .iter()
            .filter_map(|(name, value)| value.is_none().then_some(*name))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(NotificationError::Configuration(format!(
                "missing {}",
                missing.join(", ")
            )));
        }

        let port = input
            .port
            .map(|value| {
                value.parse::<u16>().map_err(|_| {
                    NotificationError::Configuration("SMTP_PORT must be a u16".to_owned())
                })
            })
            .transpose()?
            .unwrap_or(465);
        let timeout_seconds = input
            .timeout_seconds
            .map(|value| {
                value.parse::<u64>().map_err(|_| {
                    NotificationError::Configuration(
                        "SMTP_TIMEOUT_SECONDS must be an unsigned integer".to_owned(),
                    )
                })
            })
            .transpose()?
            .unwrap_or(15);
        if timeout_seconds == 0 {
            return Err(NotificationError::Configuration(
                "SMTP_TIMEOUT_SECONDS must be greater than zero".to_owned(),
            ));
        }

        Ok(Some(Self {
            host: required[0].1.clone().expect("validated SMTP_HOST"),
            username: required[1].1.clone().expect("validated SMTP_USERNAME"),
            password: required[2].1.clone().expect("validated SMTP_PASSWORD"),
            from: required[3].1.clone().expect("validated ALERT_EMAIL_FROM"),
            to: required[4].1.clone().expect("validated ALERT_EMAIL_TO"),
            port,
            timeout: StdDuration::from_secs(timeout_seconds),
        }))
    }
}

pub fn load_live_smtp_config(
    path: &Path,
    fallback: Option<SmtpConfig>,
) -> Result<Option<SmtpConfig>, NotificationError> {
    if path.exists() {
        SmtpConfig::from_file(path)
    } else {
        Ok(fallback)
    }
}

#[derive(Clone)]
pub struct SmtpEmailSender {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    to: Mailbox,
}

impl SmtpEmailSender {
    pub fn new(config: SmtpConfig) -> Result<Self, NotificationError> {
        let from = config.from.parse::<Mailbox>().map_err(|error| {
            NotificationError::Configuration(format!("ALERT_EMAIL_FROM: {error}"))
        })?;
        let to = config.to.parse::<Mailbox>().map_err(|error| {
            NotificationError::Configuration(format!("ALERT_EMAIL_TO: {error}"))
        })?;
        let transport = AsyncSmtpTransport::<Tokio1Executor>::relay(&config.host)
            .map_err(|error| NotificationError::Configuration(format!("SMTP_HOST: {error}")))?
            .port(config.port)
            .credentials(Credentials::new(config.username, config.password))
            .build();

        Ok(Self {
            transport,
            from,
            to,
        })
    }
}

impl EmailSender for SmtpEmailSender {
    fn send(
        &self,
        subject: String,
        body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        let transport = self.transport.clone();
        let from = self.from.clone();
        let to = self.to.clone();
        Box::pin(async move {
            let message = Message::builder()
                .from(from)
                .to(to)
                .subject(subject)
                .body(body)
                .map_err(|error| NotificationError::Send(error.to_string()))?;
            transport
                .send(message)
                .await
                .map_err(|error| NotificationError::Send(error.to_string()))?;
            Ok(())
        })
    }
}

#[derive(Clone)]
pub struct ReloadingSmtpEmailSender {
    config_path: PathBuf,
    fallback: Option<SmtpConfig>,
}

impl ReloadingSmtpEmailSender {
    pub fn new(config_path: PathBuf, fallback: Option<SmtpConfig>) -> Self {
        Self {
            config_path,
            fallback,
        }
    }
}

impl EmailSender for ReloadingSmtpEmailSender {
    fn send(
        &self,
        subject: String,
        body: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), NotificationError>> + Send + '_>> {
        let config_path = self.config_path.clone();
        let fallback = self.fallback.clone();
        Box::pin(async move {
            let config = load_live_smtp_config(&config_path, fallback)?.ok_or_else(|| {
                NotificationError::Configuration("SMTP is not configured".to_owned())
            })?;
            let timeout_duration = config.timeout;
            match timeout(
                timeout_duration,
                SmtpEmailSender::new(config)?.send(subject, body),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(NotificationError::Send("SMTP send timed out".to_owned())),
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotificationDispatchResult {
    pub claimed: usize,
    pub sent: usize,
    pub retried: usize,
}

#[derive(Clone)]
pub struct PlatformNotificationDispatcher<S> {
    store: std::sync::Arc<PlatformStore>,
    sender: S,
    batch_size: usize,
    send_timeout: StdDuration,
    lease_duration: Duration,
    retry_base: Duration,
    retry_max: Duration,
}

#[derive(Debug, Clone)]
pub struct NotificationDispatcher<S> {
    pool: PgPool,
    sender: S,
    batch_size: usize,
    send_timeout: StdDuration,
    lease_duration: Duration,
    retry_base: Duration,
    retry_max: Duration,
}

#[derive(Debug)]
struct LeasedNotification {
    id: Uuid,
    subject: String,
    body: String,
    attempt_count: i32,
}

#[derive(Debug, Clone)]
pub struct SqliteNotificationDispatcher<S> {
    pool: SqlitePool,
    sender: S,
    batch_size: usize,
    send_timeout: StdDuration,
    lease_duration: Duration,
    retry_base: Duration,
    retry_max: Duration,
}

#[derive(Debug)]
struct SqliteLeasedNotification {
    id: String,
    subject: String,
    body: String,
    attempt_count: i64,
}

impl<S> PlatformNotificationDispatcher<S>
where
    S: EmailSender,
{
    pub fn new(store: std::sync::Arc<PlatformStore>, sender: S, batch_size: usize) -> Self {
        Self {
            store,
            sender,
            batch_size: batch_size.max(1),
            send_timeout: SMTP_SEND_TIMEOUT,
            lease_duration: DEFAULT_LEASE_DURATION,
            retry_base: Duration::seconds(1),
            retry_max: Duration::seconds(3_600),
        }
    }

    pub fn with_timeout(mut self, send_timeout: StdDuration) -> Self {
        self.send_timeout = send_timeout;
        self
    }

    pub fn with_delivery_policy(
        mut self,
        lease_duration: Duration,
        retry_base: Duration,
        retry_max: Duration,
    ) -> Self {
        self.lease_duration = lease_duration;
        self.retry_base = retry_base;
        self.retry_max = retry_max;
        self
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<NotificationDispatchResult, NotificationError> {
        let lease_until = now + self.lease_duration;
        let limit = u32::try_from(self.batch_size).unwrap_or(u32::MAX);
        let leased = NotificationRepository::claim_notifications(
            self.store.as_ref(),
            now,
            lease_until,
            limit,
        )
        .await?;
        let mut result = NotificationDispatchResult {
            claimed: leased.len(),
            sent: 0,
            retried: 0,
        };

        for notification in leased {
            let expected_lease_until = notification.lease_until.ok_or_else(|| {
                NotificationError::Configuration(format!(
                    "claimed notification {} has no lease",
                    notification.id
                ))
            })?;
            match timeout(
                self.send_timeout,
                self.sender
                    .send(notification.subject.clone(), notification.body.clone()),
            )
            .await
            {
                Ok(Ok(())) => {
                    if NotificationRepository::mark_notification_sent(
                        self.store.as_ref(),
                        notification.id,
                        expected_lease_until,
                        now,
                    )
                    .await?
                    .is_some()
                    {
                        result.sent += 1;
                    }
                }
                Ok(Err(error)) => {
                    self.release_for_retry(
                        notification.id,
                        notification.attempt_count,
                        expected_lease_until,
                        now,
                        error,
                    )
                    .await?;
                    result.retried += 1;
                }
                Err(_) => {
                    self.release_for_retry(
                        notification.id,
                        notification.attempt_count,
                        expected_lease_until,
                        now,
                        NotificationError::Send("SMTP send timed out".to_owned()),
                    )
                    .await?;
                    result.retried += 1;
                }
            }
        }

        Ok(result)
    }

    async fn release_for_retry(
        &self,
        id: Uuid,
        attempt_count: i64,
        expected_lease_until: DateTime<Utc>,
        now: DateTime<Utc>,
        error: NotificationError,
    ) -> Result<(), NotificationError> {
        let exponent = u32::try_from((attempt_count - 1).clamp(0, 11)).unwrap_or(0);
        let delay_seconds = self
            .retry_base
            .num_seconds()
            .max(1)
            .saturating_mul(2_i64.pow(exponent))
            .min(self.retry_max.num_seconds().max(1));
        NotificationRepository::release_notification_for_retry(
            self.store.as_ref(),
            id,
            expected_lease_until,
            &error.to_string(),
            now + Duration::seconds(delay_seconds),
        )
        .await?;
        Ok(())
    }
}

impl<S> SqliteNotificationDispatcher<S>
where
    S: EmailSender,
{
    pub fn new(pool: SqlitePool, sender: S, batch_size: usize) -> Self {
        Self {
            pool,
            sender,
            batch_size: batch_size.max(1),
            send_timeout: SMTP_SEND_TIMEOUT,
            lease_duration: DEFAULT_LEASE_DURATION,
            retry_base: Duration::seconds(1),
            retry_max: Duration::seconds(3_600),
        }
    }

    pub fn with_timeout(mut self, send_timeout: StdDuration) -> Self {
        self.send_timeout = send_timeout;
        self
    }

    pub fn with_delivery_policy(
        mut self,
        lease_duration: Duration,
        retry_base: Duration,
        retry_max: Duration,
    ) -> Self {
        self.lease_duration = lease_duration;
        self.retry_base = retry_base;
        self.retry_max = retry_max;
        self
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<NotificationDispatchResult, NotificationError> {
        let leased = self.claim_batch(now).await?;
        let mut result = NotificationDispatchResult {
            claimed: leased.len(),
            sent: 0,
            retried: 0,
        };

        for notification in leased {
            match timeout(
                self.send_timeout,
                self.sender
                    .send(notification.subject.clone(), notification.body.clone()),
            )
            .await
            {
                Ok(Ok(())) => {
                    self.mark_sent(&notification.id, now).await?;
                    result.sent += 1;
                }
                Ok(Err(error)) => {
                    self.release_for_retry(
                        &notification.id,
                        notification.attempt_count,
                        now,
                        error,
                    )
                    .await?;
                    result.retried += 1;
                }
                Err(_) => {
                    self.release_for_retry(
                        &notification.id,
                        notification.attempt_count,
                        now,
                        NotificationError::Send("SMTP send timed out".to_owned()),
                    )
                    .await?;
                    result.retried += 1;
                }
            }
        }

        Ok(result)
    }

    async fn claim_batch(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<SqliteLeasedNotification>, NotificationError> {
        let lease_until = (now + self.lease_duration).to_rfc3339();
        let now = now.to_rfc3339();
        let rows = sqlx::query(
            "UPDATE notification_outbox
             SET state = 'leased',
                 lease_until = ?,
                 attempt_count = attempt_count + 1
             WHERE id IN (
                 SELECT id
                 FROM notification_outbox
                 WHERE (state = 'pending'
                        AND next_attempt_at <= ?
                        AND (lease_until IS NULL OR lease_until <= ?))
                    OR (state = 'leased' AND lease_until <= ?)
                 ORDER BY next_attempt_at, created_at
                 LIMIT ?
             )
             RETURNING id, subject, body, attempt_count",
        )
        .bind(lease_until)
        .bind(&now)
        .bind(&now)
        .bind(now)
        .bind(i64::try_from(self.batch_size).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(SqliteLeasedNotification {
                    id: row.try_get("id")?,
                    subject: row.try_get("subject")?,
                    body: row.try_get("body")?,
                    attempt_count: row.try_get("attempt_count")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(NotificationError::from)
    }

    async fn mark_sent(&self, id: &str, now: DateTime<Utc>) -> Result<(), NotificationError> {
        sqlx::query(
            "UPDATE notification_outbox
             SET state = 'sent', sent_at = ?, lease_until = NULL, last_error = NULL
             WHERE id = ? AND state = 'leased'",
        )
        .bind(now.to_rfc3339())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release_for_retry(
        &self,
        id: &str,
        attempt_count: i64,
        now: DateTime<Utc>,
        error: NotificationError,
    ) -> Result<(), NotificationError> {
        let exponent = u32::try_from((attempt_count - 1).clamp(0, 11)).unwrap_or(0);
        let delay_seconds = self
            .retry_base
            .num_seconds()
            .max(1)
            .saturating_mul(2_i64.pow(exponent))
            .min(self.retry_max.num_seconds().max(1));
        sqlx::query(
            "UPDATE notification_outbox
             SET state = 'pending',
                 lease_until = NULL,
                 next_attempt_at = ?,
                 last_error = ?
             WHERE id = ? AND state = 'leased'",
        )
        .bind((now + Duration::seconds(delay_seconds)).to_rfc3339())
        .bind(error.to_string())
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

impl<S> NotificationDispatcher<S>
where
    S: EmailSender,
{
    pub fn new(pool: PgPool, sender: S, batch_size: usize) -> Self {
        Self {
            pool,
            sender,
            batch_size: batch_size.max(1),
            send_timeout: SMTP_SEND_TIMEOUT,
            lease_duration: DEFAULT_LEASE_DURATION,
            retry_base: Duration::seconds(1),
            retry_max: Duration::seconds(3_600),
        }
    }

    pub fn with_timeout(mut self, send_timeout: StdDuration) -> Self {
        self.send_timeout = send_timeout;
        self
    }

    pub fn with_delivery_policy(
        mut self,
        lease_duration: Duration,
        retry_base: Duration,
        retry_max: Duration,
    ) -> Self {
        self.lease_duration = lease_duration;
        self.retry_base = retry_base;
        self.retry_max = retry_max;
        self
    }

    pub async fn dispatch_once(
        &self,
        now: DateTime<Utc>,
    ) -> Result<NotificationDispatchResult, NotificationError> {
        let leased = self.claim_batch(now).await?;
        let mut result = NotificationDispatchResult {
            claimed: leased.len(),
            sent: 0,
            retried: 0,
        };

        for notification in leased {
            match timeout(
                self.send_timeout,
                self.sender
                    .send(notification.subject.clone(), notification.body.clone()),
            )
            .await
            {
                Ok(Ok(())) => {
                    self.mark_sent(notification.id, now).await?;
                    result.sent += 1;
                }
                Ok(Err(error)) => {
                    self.release_for_retry(notification.id, notification.attempt_count, now, error)
                        .await?;
                    result.retried += 1;
                }
                Err(_) => {
                    self.release_for_retry(
                        notification.id,
                        notification.attempt_count,
                        now,
                        NotificationError::Send("SMTP send timed out".to_owned()),
                    )
                    .await?;
                    result.retried += 1;
                }
            }
        }

        Ok(result)
    }

    async fn claim_batch(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<LeasedNotification>, NotificationError> {
        let mut transaction = self.pool.begin().await?;
        let lease_until = now + self.lease_duration;
        let rows = sqlx::query(
            "WITH candidates AS (
                SELECT id
                FROM notification_outbox
                WHERE state = 'pending'
                  AND next_attempt_at <= $1
                  AND (lease_until IS NULL OR lease_until <= $1)
                ORDER BY next_attempt_at, created_at
                LIMIT $2
                FOR UPDATE SKIP LOCKED
             )
             UPDATE notification_outbox AS outbox
             SET state = 'leased',
                 lease_until = $3,
                 attempt_count = outbox.attempt_count + 1
             FROM candidates
             WHERE outbox.id = candidates.id
             RETURNING outbox.id, outbox.subject, outbox.body, outbox.attempt_count",
        )
        .bind(now)
        .bind(i64::try_from(self.batch_size).unwrap_or(i64::MAX))
        .bind(lease_until)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;

        rows.into_iter()
            .map(|row| {
                Ok(LeasedNotification {
                    id: row.try_get("id")?,
                    subject: row.try_get("subject")?,
                    body: row.try_get("body")?,
                    attempt_count: row.try_get("attempt_count")?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(NotificationError::from)
    }

    async fn mark_sent(&self, id: Uuid, now: DateTime<Utc>) -> Result<(), NotificationError> {
        sqlx::query(
            "UPDATE notification_outbox
             SET state = 'sent', sent_at = $2, lease_until = NULL, last_error = NULL
             WHERE id = $1 AND state = 'leased'",
        )
        .bind(id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release_for_retry(
        &self,
        id: Uuid,
        attempt_count: i32,
        now: DateTime<Utc>,
        error: NotificationError,
    ) -> Result<(), NotificationError> {
        let exponent = u32::try_from((attempt_count - 1).clamp(0, 11)).unwrap_or(0);
        let delay_seconds = self
            .retry_base
            .num_seconds()
            .max(1)
            .saturating_mul(2_i64.pow(exponent))
            .min(self.retry_max.num_seconds().max(1));
        sqlx::query(
            "UPDATE notification_outbox
             SET state = 'pending',
                 lease_until = NULL,
                 next_attempt_at = $2,
                 last_error = $3
             WHERE id = $1 AND state = 'leased'",
        )
        .bind(id)
        .bind(now + Duration::seconds(delay_seconds))
        .bind(error.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn parse_environment_value(value: &str) -> Result<String, String> {
    let value = value.trim();
    if !value.starts_with('"') {
        return Ok(value.to_owned());
    }
    if value.len() < 2 || !value.ends_with('"') {
        return Err("unterminated quoted value".to_owned());
    }
    let mut result = String::new();
    let mut escaping = false;
    for character in value[1..value.len() - 1].chars() {
        if escaping {
            result.push(character);
            escaping = false;
        } else if character == '\\' {
            escaping = true;
        } else {
            result.push(character);
        }
    }
    if escaping {
        return Err("quoted value ends with escape".to_owned());
    }
    Ok(result)
}
