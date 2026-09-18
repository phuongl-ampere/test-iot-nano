use std::{future::Future, pin::Pin};

use chrono::{DateTime, Duration as ChronoDuration, Utc};

use crate::{NotificationKind, NotificationOutboxRecord, PlatformStoreError};

#[derive(Debug, Clone, PartialEq)]
pub struct AlertRule {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub name: String,
    pub enabled: bool,
    pub kind: AlertRuleKind,
    pub device_id: Option<String>,
    pub metric_key: String,
    pub comparison: AlertComparison,
    pub threshold: f64,
    pub window: Option<ChronoDuration>,
    pub for_duration: ChronoDuration,
    pub resolve_after: ChronoDuration,
    pub reopen_grace: ChronoDuration,
    pub hysteresis: Option<f64>,
    pub severity: AlertSeverity,
    pub reminder_interval: ChronoDuration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertRuleKind {
    EventThreshold,
    WindowAverage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertComparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertEvaluationEvent {
    pub event_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub tenant_id: uuid::Uuid,
    pub device_id: String,
    pub boot_id: uuid::Uuid,
    pub sequence: u64,
    pub measurements: serde_json::Map<String, serde_json::Value>,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlertEvaluationResult {
    pub evaluated: usize,
    pub opened: usize,
    pub resolved: usize,
    pub reminders: usize,
}

pub trait AlertEvaluationRepository: Send + Sync {
    fn evaluate_alert_events<'a>(
        &'a self,
        events: &'a [AlertEvaluationEvent],
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>;

    fn evaluate_alert_windows<'a>(
        &'a self,
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertIncidentStatus {
    Pending,
    Open,
    Resolved,
}

impl AlertIncidentStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Open => "open",
            Self::Resolved => "resolved",
        }
    }

    pub(crate) fn from_database(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "pending" => Ok(Self::Pending),
            "open" => Ok(Self::Open),
            "resolved" => Ok(Self::Resolved),
            _ => Err(PlatformStoreError::InvalidIncidentStatus(value.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewAlertIncident {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub rule_id: uuid::Uuid,
    pub device_id: String,
    pub status: AlertIncidentStatus,
    pub condition_started_at: DateTime<Utc>,
    pub last_value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertIncident {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub rule_id: uuid::Uuid,
    pub device_id: String,
    pub status: AlertIncidentStatus,
    pub condition_started_at: DateTime<Utc>,
    pub recovery_started_at: Option<DateTime<Utc>>,
    pub opened_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub last_value: Option<f64>,
    pub last_notified_at: Option<DateTime<Utc>>,
    pub last_reminder_at: Option<DateTime<Utc>>,
    pub state_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewNotificationOutboxEntry {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub kind: NotificationKind,
    pub dedupe_key: String,
    pub subject: String,
    pub body: String,
    pub next_attempt_at: DateTime<Utc>,
}

pub trait AlertIncidentRepository: Send + Sync {
    fn create_incident<'a>(
        &'a self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn update_incident_last_value<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn open_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn open_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn recover_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn resolve_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn resolve_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn remind_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn remind_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>;
    fn enqueue_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<
        Box<dyn Future<Output = Result<NotificationOutboxRecord, PlatformStoreError>> + Send + 'a>,
    >;
}
