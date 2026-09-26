use std::{future::Future, pin::Pin};

use chrono::{DateTime, Utc};
use sqlx::{Postgres, Row, Sqlite, Transaction, postgres::PgRow, sqlite::SqliteRow};
use thiserror::Error;
use uuid::Uuid;

use crate::{PlatformStore, PlatformStoreError};

use super::devices::validate_device_id;

pub const MANAGEMENT_ALERT_LIST_LIMIT: usize = 100;
pub const MANAGEMENT_ALERT_RULE_LIST_LIMIT: usize = 100;
pub const MANAGEMENT_ALERT_INCIDENT_LIST_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAlert {
    pub id: Uuid,
    pub rule_name: String,
    pub severity: String,
    pub device_id: String,
    pub status: String,
    pub last_value: Option<f64>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ManagementAlertError {
    #[error("stored management alert ID is invalid")]
    InvalidStoredAlertId,
    #[error("stored management alert timestamp is invalid")]
    InvalidStoredAlertTimestamp,
    #[error("management alert storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAlertError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAlertError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAlertRepository: Send + Sync {
    fn list_management_alerts<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAlert>, ManagementAlertError>> + Send + 'a>>;
}

impl ManagementAlertRepository for PlatformStore {
    fn list_management_alerts<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ManagementAlert>, ManagementAlertError>> + Send + 'a>>
    {
        Box::pin(async move { list_management_alerts(self, tenant_id).await })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAlertRule {
    pub id: Uuid,
    pub name: String,
    pub enabled: bool,
    pub device_id: Option<String>,
    pub metric_key: String,
    pub rule_type: String,
    pub comparison: String,
    pub threshold: f64,
    pub window_seconds: Option<u64>,
    pub for_seconds: u64,
    pub resolve_after_seconds: u64,
    pub reopen_grace_seconds: u64,
    pub hysteresis: Option<f64>,
    pub severity: String,
    pub reminder_interval_seconds: u64,
    pub archived_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateManagementAlertRule {
    pub name: String,
    pub enabled: bool,
    pub device_id: Option<String>,
    pub metric_key: String,
    pub rule_type: String,
    pub comparison: String,
    pub threshold: f64,
    pub window_seconds: Option<u64>,
    pub for_seconds: u64,
    pub resolve_after_seconds: u64,
    pub reopen_grace_seconds: u64,
    pub hysteresis: Option<f64>,
    pub severity: String,
    pub reminder_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateManagementAlertRule {
    pub name: String,
    pub enabled: bool,
    pub device_id: Option<String>,
    pub metric_key: String,
    pub rule_type: String,
    pub comparison: String,
    pub threshold: f64,
    pub window_seconds: Option<u64>,
    pub for_seconds: u64,
    pub resolve_after_seconds: u64,
    pub reopen_grace_seconds: u64,
    pub hysteresis: Option<f64>,
    pub severity: String,
    pub reminder_interval_seconds: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ManagementAlertIncident {
    pub id: Uuid,
    pub rule_id: Uuid,
    pub rule_name: String,
    pub severity: String,
    pub device_id: String,
    pub status: String,
    pub last_value: Option<f64>,
    pub condition_started_at: DateTime<Utc>,
    pub opened_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_by: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ManagementAlertRuleError {
    #[error("invalid alert rule name")]
    InvalidName,
    #[error("invalid alert metric key")]
    InvalidMetricKey,
    #[error("invalid alert rule type")]
    InvalidRuleType,
    #[error("invalid alert comparison")]
    InvalidComparison,
    #[error("invalid alert threshold")]
    InvalidThreshold,
    #[error("invalid alert window")]
    InvalidWindow,
    #[error("invalid alert duration")]
    InvalidDuration,
    #[error("invalid alert hysteresis")]
    InvalidHysteresis,
    #[error("invalid alert severity")]
    InvalidSeverity,
    #[error("alert rule device is unavailable: {0}")]
    DeviceUnavailable(String),
    #[error("management alert rule was not found")]
    RuleNotFound,
    #[error("management alert rule is archived")]
    RuleArchived,
    #[error("stored management alert rule is invalid")]
    InvalidStoredRule,
    #[error("management alert rule storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAlertRuleError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAlertRuleError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

#[derive(Debug, Error)]
pub enum ManagementAlertIncidentError {
    #[error("management alert incident was not found")]
    IncidentNotFound,
    #[error("stored management alert incident is invalid")]
    InvalidStoredIncident,
    #[error("management alert incident storage operation failed")]
    Storage {
        #[source]
        source: PlatformStoreError,
    },
}

impl From<PlatformStoreError> for ManagementAlertIncidentError {
    fn from(source: PlatformStoreError) -> Self {
        Self::Storage { source }
    }
}

impl From<sqlx::Error> for ManagementAlertIncidentError {
    fn from(source: sqlx::Error) -> Self {
        Self::from(PlatformStoreError::from(source))
    }
}

pub trait ManagementAlertRuleRepository: Send + Sync {
    fn list_management_alert_rules<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAlertRule>, ManagementAlertRuleError>>
                + Send
                + 'a,
        >,
    >;
    fn create_management_alert_rule<'a>(
        &'a self,
        tenant_id: Uuid,
        rule: CreateManagementAlertRule,
    ) -> Pin<
        Box<dyn Future<Output = Result<ManagementAlertRule, ManagementAlertRuleError>> + Send + 'a>,
    >;
    fn update_management_alert_rule<'a>(
        &'a self,
        tenant_id: Uuid,
        rule_id: Uuid,
        rule: UpdateManagementAlertRule,
    ) -> Pin<
        Box<dyn Future<Output = Result<ManagementAlertRule, ManagementAlertRuleError>> + Send + 'a>,
    >;
    fn archive_management_alert_rule<'a>(
        &'a self,
        tenant_id: Uuid,
        rule_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAlertRuleError>> + Send + 'a>>;
}

pub trait ManagementAlertIncidentRepository: Send + Sync {
    fn list_management_alert_incidents<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAlertIncident>, ManagementAlertIncidentError>>
                + Send
                + 'a,
        >,
    >;
    fn acknowledge_management_alert_incident<'a>(
        &'a self,
        tenant_id: Uuid,
        incident_id: Uuid,
        acknowledged_by: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAlertIncident, ManagementAlertIncidentError>>
                + Send
                + 'a,
        >,
    >;
    fn open_management_alert_incident_count<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<u64, ManagementAlertIncidentError>> + Send + 'a>>;
}

impl ManagementAlertRuleRepository for PlatformStore {
    fn list_management_alert_rules<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAlertRule>, ManagementAlertRuleError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_management_alert_rules(self, tenant_id).await })
    }

    fn create_management_alert_rule<'a>(
        &'a self,
        tenant_id: Uuid,
        rule: CreateManagementAlertRule,
    ) -> Pin<
        Box<dyn Future<Output = Result<ManagementAlertRule, ManagementAlertRuleError>> + Send + 'a>,
    > {
        Box::pin(async move { create_management_alert_rule(self, tenant_id, rule).await })
    }

    fn update_management_alert_rule<'a>(
        &'a self,
        tenant_id: Uuid,
        rule_id: Uuid,
        rule: UpdateManagementAlertRule,
    ) -> Pin<
        Box<dyn Future<Output = Result<ManagementAlertRule, ManagementAlertRuleError>> + Send + 'a>,
    > {
        Box::pin(async move { update_management_alert_rule(self, tenant_id, rule_id, rule).await })
    }

    fn archive_management_alert_rule<'a>(
        &'a self,
        tenant_id: Uuid,
        rule_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), ManagementAlertRuleError>> + Send + 'a>> {
        Box::pin(async move { archive_management_alert_rule(self, tenant_id, rule_id).await })
    }
}

impl ManagementAlertIncidentRepository for PlatformStore {
    fn list_management_alert_incidents<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Vec<ManagementAlertIncident>, ManagementAlertIncidentError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move { list_management_alert_incidents(self, tenant_id).await })
    }

    fn acknowledge_management_alert_incident<'a>(
        &'a self,
        tenant_id: Uuid,
        incident_id: Uuid,
        acknowledged_by: String,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<ManagementAlertIncident, ManagementAlertIncidentError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            acknowledge_management_alert_incident(self, tenant_id, incident_id, &acknowledged_by)
                .await
        })
    }

    fn open_management_alert_incident_count<'a>(
        &'a self,
        tenant_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<u64, ManagementAlertIncidentError>> + Send + 'a>> {
        Box::pin(async move { open_management_alert_incident_count(self, tenant_id).await })
    }
}

async fn list_management_alerts(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAlert>, ManagementAlertError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT incidents.id, rules.name AS rule_name, rules.severity, incidents.device_id,
                        incidents.status, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.tenant_id = ?
                 ORDER BY julianday(incidents.updated_at) DESC, incidents.id DESC
                 LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(MANAGEMENT_ALERT_LIST_LIMIT as i64)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_alert_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT incidents.id, rules.name AS rule_name, rules.severity, incidents.device_id,
                        incidents.status, incidents.last_value, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                    ON rules.id = incidents.rule_id
                   AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.tenant_id = $1
                 ORDER BY incidents.updated_at DESC, incidents.id DESC
                 LIMIT $2",
            )
            .bind(tenant_id)
            .bind(MANAGEMENT_ALERT_LIST_LIMIT as i64)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_alert_from_row)
                .collect()
        }
    }
}

struct ManagementAlertRuleInput {
    name: String,
    enabled: bool,
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    window_seconds: Option<u64>,
    for_seconds: u64,
    resolve_after_seconds: u64,
    reopen_grace_seconds: u64,
    hysteresis: Option<f64>,
    severity: String,
    reminder_interval_seconds: u64,
}

impl From<CreateManagementAlertRule> for ManagementAlertRuleInput {
    fn from(value: CreateManagementAlertRule) -> Self {
        Self {
            name: value.name,
            enabled: value.enabled,
            device_id: value.device_id,
            metric_key: value.metric_key,
            rule_type: value.rule_type,
            comparison: value.comparison,
            threshold: value.threshold,
            window_seconds: value.window_seconds,
            for_seconds: value.for_seconds,
            resolve_after_seconds: value.resolve_after_seconds,
            reopen_grace_seconds: value.reopen_grace_seconds,
            hysteresis: value.hysteresis,
            severity: value.severity,
            reminder_interval_seconds: value.reminder_interval_seconds,
        }
    }
}

impl From<UpdateManagementAlertRule> for ManagementAlertRuleInput {
    fn from(value: UpdateManagementAlertRule) -> Self {
        Self {
            name: value.name,
            enabled: value.enabled,
            device_id: value.device_id,
            metric_key: value.metric_key,
            rule_type: value.rule_type,
            comparison: value.comparison,
            threshold: value.threshold,
            window_seconds: value.window_seconds,
            for_seconds: value.for_seconds,
            resolve_after_seconds: value.resolve_after_seconds,
            reopen_grace_seconds: value.reopen_grace_seconds,
            hysteresis: value.hysteresis,
            severity: value.severity,
            reminder_interval_seconds: value.reminder_interval_seconds,
        }
    }
}

struct ValidatedManagementAlertRule {
    name: String,
    enabled: bool,
    device_id: Option<String>,
    metric_key: String,
    rule_type: String,
    comparison: String,
    threshold: f64,
    window_seconds: Option<i32>,
    for_seconds: i32,
    resolve_after_seconds: i32,
    reopen_grace_seconds: i32,
    hysteresis: Option<f64>,
    severity: String,
    reminder_interval_seconds: i32,
}

fn validate_management_alert_rule(
    input: ManagementAlertRuleInput,
) -> Result<ValidatedManagementAlertRule, ManagementAlertRuleError> {
    let name = input.name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err(ManagementAlertRuleError::InvalidName);
    }
    let metric_key = input.metric_key.trim();
    if metric_key.is_empty()
        || metric_key.len() > 128
        || !metric_key
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-' | b'.'))
    {
        return Err(ManagementAlertRuleError::InvalidMetricKey);
    }
    if !matches!(
        input.rule_type.as_str(),
        "event_threshold" | "window_average"
    ) {
        return Err(ManagementAlertRuleError::InvalidRuleType);
    }
    if !matches!(input.comparison.as_str(), "gt" | "gte" | "lt" | "lte") {
        return Err(ManagementAlertRuleError::InvalidComparison);
    }
    if !input.threshold.is_finite() {
        return Err(ManagementAlertRuleError::InvalidThreshold);
    }
    if !matches!(input.severity.as_str(), "info" | "warning" | "critical") {
        return Err(ManagementAlertRuleError::InvalidSeverity);
    }
    if input
        .hysteresis
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(ManagementAlertRuleError::InvalidHysteresis);
    }
    let window_seconds = match (input.rule_type.as_str(), input.window_seconds) {
        ("event_threshold", None) => None,
        ("event_threshold", Some(_)) => return Err(ManagementAlertRuleError::InvalidWindow),
        ("window_average", Some(seconds)) if seconds >= 60 => Some(seconds),
        ("window_average", _) => return Err(ManagementAlertRuleError::InvalidWindow),
        _ => return Err(ManagementAlertRuleError::InvalidRuleType),
    };
    if input.reminder_interval_seconds == 0 {
        return Err(ManagementAlertRuleError::InvalidDuration);
    }
    let to_i32 =
        |value: u64| i32::try_from(value).map_err(|_| ManagementAlertRuleError::InvalidDuration);
    let device_id = input.device_id.map(|value| value.trim().to_owned());
    if device_id
        .as_deref()
        .is_some_and(|value| value.is_empty() || validate_device_id(value).is_err())
    {
        return Err(ManagementAlertRuleError::DeviceUnavailable(
            device_id.clone().unwrap_or_default(),
        ));
    }

    Ok(ValidatedManagementAlertRule {
        name: name.to_owned(),
        enabled: input.enabled,
        device_id,
        metric_key: metric_key.to_owned(),
        rule_type: input.rule_type,
        comparison: input.comparison,
        threshold: input.threshold,
        window_seconds: window_seconds.map(to_i32).transpose()?,
        for_seconds: to_i32(input.for_seconds)?,
        resolve_after_seconds: to_i32(input.resolve_after_seconds)?,
        reopen_grace_seconds: to_i32(input.reopen_grace_seconds)?,
        hysteresis: input.hysteresis,
        severity: input.severity,
        reminder_interval_seconds: to_i32(input.reminder_interval_seconds)?,
    })
}

async fn list_management_alert_rules(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAlertRule>, ManagementAlertRuleError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                        window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                        hysteresis, severity, reminder_interval_seconds, archived_at, updated_at
                 FROM alert_rules
                 WHERE tenant_id = ? AND archived_at IS NULL
                 ORDER BY updated_at DESC, id DESC
                 LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(MANAGEMENT_ALERT_RULE_LIST_LIMIT as i64)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_alert_rule_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                        window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                        hysteresis, severity, reminder_interval_seconds, archived_at, updated_at
                 FROM alert_rules
                 WHERE tenant_id = $1 AND archived_at IS NULL
                 ORDER BY updated_at DESC, id DESC
                 LIMIT $2",
            )
            .bind(tenant_id)
            .bind(MANAGEMENT_ALERT_RULE_LIST_LIMIT as i64)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_alert_rule_from_row)
                .collect()
        }
    }
}

async fn management_alert_rule(
    store: &PlatformStore,
    tenant_id: Uuid,
    rule_id: Uuid,
) -> Result<ManagementAlertRule, ManagementAlertRuleError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                        window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                        hysteresis, severity, reminder_interval_seconds, archived_at, updated_at
                 FROM alert_rules
                 WHERE id = ? AND tenant_id = ? AND archived_at IS NULL",
            )
            .bind(rule_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementAlertRuleError::RuleNotFound)?;
            sqlite_management_alert_rule_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                        window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                        hysteresis, severity, reminder_interval_seconds, archived_at, updated_at
                 FROM alert_rules
                 WHERE id = $1 AND tenant_id = $2 AND archived_at IS NULL",
            )
            .bind(rule_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementAlertRuleError::RuleNotFound)?;
            timescale_management_alert_rule_from_row(row)
        }
    }
}

async fn create_management_alert_rule(
    store: &PlatformStore,
    tenant_id: Uuid,
    rule: CreateManagementAlertRule,
) -> Result<ManagementAlertRule, ManagementAlertRuleError> {
    let rule = validate_management_alert_rule(rule.into())?;
    let rule_id = Uuid::now_v7();
    match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            ensure_sqlite_management_alert_rule_device(
                &mut transaction,
                tenant_id,
                rule.device_id.as_deref(),
            )
            .await?;
            sqlx::query(
                "INSERT INTO alert_rules (
                     id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison,
                     threshold, window_seconds, for_seconds, resolve_after_seconds,
                     reopen_grace_seconds, hysteresis, severity, reminder_interval_seconds
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(rule_id.to_string())
            .bind(tenant_id.to_string())
            .bind(&rule.name)
            .bind(if rule.enabled { 1_i64 } else { 0_i64 })
            .bind(rule.device_id.as_deref())
            .bind(&rule.metric_key)
            .bind(&rule.rule_type)
            .bind(&rule.comparison)
            .bind(rule.threshold)
            .bind(rule.window_seconds.map(i64::from))
            .bind(rule.for_seconds)
            .bind(rule.resolve_after_seconds)
            .bind(rule.reopen_grace_seconds)
            .bind(rule.hysteresis)
            .bind(&rule.severity)
            .bind(rule.reminder_interval_seconds)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            ensure_timescale_management_alert_rule_device(
                &mut transaction,
                tenant_id,
                rule.device_id.as_deref(),
            )
            .await?;
            sqlx::query(
                "INSERT INTO alert_rules (
                     id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison,
                     threshold, window_seconds, for_seconds, resolve_after_seconds,
                     reopen_grace_seconds, hysteresis, severity, reminder_interval_seconds
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)",
            )
            .bind(rule_id)
            .bind(tenant_id)
            .bind(&rule.name)
            .bind(rule.enabled)
            .bind(rule.device_id.as_deref())
            .bind(&rule.metric_key)
            .bind(&rule.rule_type)
            .bind(&rule.comparison)
            .bind(rule.threshold)
            .bind(rule.window_seconds)
            .bind(rule.for_seconds)
            .bind(rule.resolve_after_seconds)
            .bind(rule.reopen_grace_seconds)
            .bind(rule.hysteresis)
            .bind(&rule.severity)
            .bind(rule.reminder_interval_seconds)
            .execute(&mut *transaction)
            .await?;
            transaction.commit().await?;
        }
    }
    management_alert_rule(store, tenant_id, rule_id).await
}

async fn update_management_alert_rule(
    store: &PlatformStore,
    tenant_id: Uuid,
    rule_id: Uuid,
    rule: UpdateManagementAlertRule,
) -> Result<ManagementAlertRule, ManagementAlertRuleError> {
    let rule = validate_management_alert_rule(rule.into())?;
    let updated = match store {
        PlatformStore::Sqlite(store) => {
            let mut transaction = store.pool().begin().await?;
            ensure_sqlite_management_alert_rule_device(
                &mut transaction,
                tenant_id,
                rule.device_id.as_deref(),
            )
            .await?;
            let updated = sqlx::query(
                "UPDATE alert_rules
                 SET name = ?, enabled = ?, device_id = ?, metric_key = ?, rule_type = ?,
                     comparison = ?, threshold = ?, window_seconds = ?, for_seconds = ?,
                     resolve_after_seconds = ?, reopen_grace_seconds = ?, hysteresis = ?,
                     severity = ?, reminder_interval_seconds = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ? AND archived_at IS NULL",
            )
            .bind(&rule.name)
            .bind(if rule.enabled { 1_i64 } else { 0_i64 })
            .bind(rule.device_id.as_deref())
            .bind(&rule.metric_key)
            .bind(&rule.rule_type)
            .bind(&rule.comparison)
            .bind(rule.threshold)
            .bind(rule.window_seconds.map(i64::from))
            .bind(rule.for_seconds)
            .bind(rule.resolve_after_seconds)
            .bind(rule.reopen_grace_seconds)
            .bind(rule.hysteresis)
            .bind(&rule.severity)
            .bind(rule.reminder_interval_seconds)
            .bind(Utc::now().to_rfc3339())
            .bind(rule_id.to_string())
            .bind(tenant_id.to_string())
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            transaction.commit().await?;
            updated
        }
        PlatformStore::Timescale(pool) => {
            let mut transaction = pool.begin().await?;
            ensure_timescale_management_alert_rule_device(
                &mut transaction,
                tenant_id,
                rule.device_id.as_deref(),
            )
            .await?;
            let updated = sqlx::query(
                "UPDATE alert_rules
                 SET name = $1, enabled = $2, device_id = $3, metric_key = $4, rule_type = $5,
                     comparison = $6, threshold = $7, window_seconds = $8, for_seconds = $9,
                     resolve_after_seconds = $10, reopen_grace_seconds = $11, hysteresis = $12,
                     severity = $13, reminder_interval_seconds = $14, updated_at = now()
                 WHERE id = $15 AND tenant_id = $16 AND archived_at IS NULL",
            )
            .bind(&rule.name)
            .bind(rule.enabled)
            .bind(rule.device_id.as_deref())
            .bind(&rule.metric_key)
            .bind(&rule.rule_type)
            .bind(&rule.comparison)
            .bind(rule.threshold)
            .bind(rule.window_seconds)
            .bind(rule.for_seconds)
            .bind(rule.resolve_after_seconds)
            .bind(rule.reopen_grace_seconds)
            .bind(rule.hysteresis)
            .bind(&rule.severity)
            .bind(rule.reminder_interval_seconds)
            .bind(rule_id)
            .bind(tenant_id)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
            transaction.commit().await?;
            updated
        }
    };
    if updated == 0 {
        return Err(ManagementAlertRuleError::RuleNotFound);
    }
    management_alert_rule(store, tenant_id, rule_id).await
}

async fn archive_management_alert_rule(
    store: &PlatformStore,
    tenant_id: Uuid,
    rule_id: Uuid,
) -> Result<(), ManagementAlertRuleError> {
    let archived = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE alert_rules
             SET enabled = 0, archived_at = ?, updated_at = ?
             WHERE id = ? AND tenant_id = ? AND archived_at IS NULL",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(Utc::now().to_rfc3339())
        .bind(rule_id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE alert_rules
             SET enabled = FALSE, archived_at = now(), updated_at = now()
             WHERE id = $1 AND tenant_id = $2 AND archived_at IS NULL",
        )
        .bind(rule_id)
        .bind(tenant_id)
        .execute(pool)
        .await?
        .rows_affected(),
    };
    if archived == 0 {
        return Err(ManagementAlertRuleError::RuleNotFound);
    }
    Ok(())
}

async fn ensure_sqlite_management_alert_rule_device(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: Uuid,
    device_id: Option<&str>,
) -> Result<(), ManagementAlertRuleError> {
    let Some(device_id) = device_id else {
        return Ok(());
    };
    let exists = sqlx::query_scalar::<_, i64>(
        "SELECT 1 FROM devices
         WHERE device_id = ? AND tenant_id = ? AND deleted_at IS NULL",
    )
    .bind(device_id)
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if exists {
        Ok(())
    } else {
        Err(ManagementAlertRuleError::DeviceUnavailable(
            device_id.to_owned(),
        ))
    }
}

async fn ensure_timescale_management_alert_rule_device(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: Uuid,
    device_id: Option<&str>,
) -> Result<(), ManagementAlertRuleError> {
    let Some(device_id) = device_id else {
        return Ok(());
    };
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
             SELECT 1 FROM devices
             WHERE device_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
         )",
    )
    .bind(device_id)
    .bind(tenant_id)
    .fetch_one(&mut **transaction)
    .await?;
    if exists {
        Ok(())
    } else {
        Err(ManagementAlertRuleError::DeviceUnavailable(
            device_id.to_owned(),
        ))
    }
}

fn management_alert_rule_seconds(value: i64) -> Result<u64, ManagementAlertRuleError> {
    u64::try_from(value).map_err(|_| ManagementAlertRuleError::InvalidStoredRule)
}

fn management_alert_rule_timestamp(value: &str) -> Result<DateTime<Utc>, ManagementAlertRuleError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc())
        })
        .map_err(|_| ManagementAlertRuleError::InvalidStoredRule)
}

fn sqlite_management_alert_rule_from_row(
    row: SqliteRow,
) -> Result<ManagementAlertRule, ManagementAlertRuleError> {
    let id = row.try_get::<String, _>("id")?;
    let archived_at = row
        .try_get::<Option<String>, _>("archived_at")?
        .as_deref()
        .map(management_alert_rule_timestamp)
        .transpose()?;
    Ok(ManagementAlertRule {
        id: Uuid::parse_str(&id).map_err(|_| ManagementAlertRuleError::InvalidStoredRule)?,
        name: row.try_get("name")?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        rule_type: row.try_get("rule_type")?,
        comparison: row.try_get("comparison")?,
        threshold: row.try_get("threshold")?,
        window_seconds: row
            .try_get::<Option<i64>, _>("window_seconds")?
            .map(management_alert_rule_seconds)
            .transpose()?,
        for_seconds: management_alert_rule_seconds(row.try_get("for_seconds")?)?,
        resolve_after_seconds: management_alert_rule_seconds(
            row.try_get("resolve_after_seconds")?,
        )?,
        reopen_grace_seconds: management_alert_rule_seconds(row.try_get("reopen_grace_seconds")?)?,
        hysteresis: row.try_get("hysteresis")?,
        severity: row.try_get("severity")?,
        reminder_interval_seconds: management_alert_rule_seconds(
            row.try_get("reminder_interval_seconds")?,
        )?,
        archived_at,
        updated_at: management_alert_rule_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn timescale_management_alert_rule_from_row(
    row: PgRow,
) -> Result<ManagementAlertRule, ManagementAlertRuleError> {
    let window_seconds = row
        .try_get::<Option<i32>, _>("window_seconds")?
        .map(i64::from)
        .map(management_alert_rule_seconds)
        .transpose()?;
    Ok(ManagementAlertRule {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        rule_type: row.try_get("rule_type")?,
        comparison: row.try_get("comparison")?,
        threshold: row.try_get("threshold")?,
        window_seconds,
        for_seconds: management_alert_rule_seconds(i64::from(
            row.try_get::<i32, _>("for_seconds")?,
        ))?,
        resolve_after_seconds: management_alert_rule_seconds(i64::from(
            row.try_get::<i32, _>("resolve_after_seconds")?,
        ))?,
        reopen_grace_seconds: management_alert_rule_seconds(i64::from(
            row.try_get::<i32, _>("reopen_grace_seconds")?,
        ))?,
        hysteresis: row.try_get("hysteresis")?,
        severity: row.try_get("severity")?,
        reminder_interval_seconds: management_alert_rule_seconds(i64::from(
            row.try_get::<i32, _>("reminder_interval_seconds")?,
        ))?,
        archived_at: row.try_get("archived_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

async fn list_management_alert_incidents(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<Vec<ManagementAlertIncident>, ManagementAlertIncidentError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let rows = sqlx::query(
                "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.last_value,
                        incidents.condition_started_at, incidents.opened_at, incidents.resolved_at,
                        incidents.acknowledged_at, incidents.acknowledged_by, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                   ON rules.id = incidents.rule_id AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.tenant_id = ?
                 ORDER BY julianday(incidents.updated_at) DESC, incidents.id DESC
                 LIMIT ?",
            )
            .bind(tenant_id.to_string())
            .bind(MANAGEMENT_ALERT_INCIDENT_LIST_LIMIT as i64)
            .fetch_all(store.pool())
            .await?;
            rows.into_iter()
                .map(sqlite_management_alert_incident_from_row)
                .collect()
        }
        PlatformStore::Timescale(pool) => {
            let rows = sqlx::query(
                "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.last_value,
                        incidents.condition_started_at, incidents.opened_at, incidents.resolved_at,
                        incidents.acknowledged_at, incidents.acknowledged_by, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                   ON rules.id = incidents.rule_id AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.tenant_id = $1
                 ORDER BY incidents.updated_at DESC, incidents.id DESC
                 LIMIT $2",
            )
            .bind(tenant_id)
            .bind(MANAGEMENT_ALERT_INCIDENT_LIST_LIMIT as i64)
            .fetch_all(pool)
            .await?;
            rows.into_iter()
                .map(timescale_management_alert_incident_from_row)
                .collect()
        }
    }
}

async fn management_alert_incident(
    store: &PlatformStore,
    tenant_id: Uuid,
    incident_id: Uuid,
) -> Result<ManagementAlertIncident, ManagementAlertIncidentError> {
    match store {
        PlatformStore::Sqlite(store) => {
            let row = sqlx::query(
                "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.last_value,
                        incidents.condition_started_at, incidents.opened_at, incidents.resolved_at,
                        incidents.acknowledged_at, incidents.acknowledged_by, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                   ON rules.id = incidents.rule_id AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.id = ? AND incidents.tenant_id = ?",
            )
            .bind(incident_id.to_string())
            .bind(tenant_id.to_string())
            .fetch_optional(store.pool())
            .await?
            .ok_or(ManagementAlertIncidentError::IncidentNotFound)?;
            sqlite_management_alert_incident_from_row(row)
        }
        PlatformStore::Timescale(pool) => {
            let row = sqlx::query(
                "SELECT incidents.id, incidents.rule_id, rules.name AS rule_name, rules.severity,
                        incidents.device_id, incidents.status, incidents.last_value,
                        incidents.condition_started_at, incidents.opened_at, incidents.resolved_at,
                        incidents.acknowledged_at, incidents.acknowledged_by, incidents.updated_at
                 FROM alert_incidents AS incidents
                 JOIN alert_rules AS rules
                   ON rules.id = incidents.rule_id AND rules.tenant_id = incidents.tenant_id
                 WHERE incidents.id = $1 AND incidents.tenant_id = $2",
            )
            .bind(incident_id)
            .bind(tenant_id)
            .fetch_optional(pool)
            .await?
            .ok_or(ManagementAlertIncidentError::IncidentNotFound)?;
            timescale_management_alert_incident_from_row(row)
        }
    }
}

async fn acknowledge_management_alert_incident(
    store: &PlatformStore,
    tenant_id: Uuid,
    incident_id: Uuid,
    acknowledged_by: &str,
) -> Result<ManagementAlertIncident, ManagementAlertIncidentError> {
    let updated = match store {
        PlatformStore::Sqlite(store) => sqlx::query(
            "UPDATE alert_incidents
             SET acknowledged_at = ?, acknowledged_by = ?, updated_at = ?
             WHERE id = ? AND tenant_id = ?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(acknowledged_by)
        .bind(Utc::now().to_rfc3339())
        .bind(incident_id.to_string())
        .bind(tenant_id.to_string())
        .execute(store.pool())
        .await?
        .rows_affected(),
        PlatformStore::Timescale(pool) => sqlx::query(
            "UPDATE alert_incidents
             SET acknowledged_at = now(), acknowledged_by = $1, updated_at = now()
             WHERE id = $2 AND tenant_id = $3",
        )
        .bind(acknowledged_by)
        .bind(incident_id)
        .bind(tenant_id)
        .execute(pool)
        .await?
        .rows_affected(),
    };
    if updated == 0 {
        return Err(ManagementAlertIncidentError::IncidentNotFound);
    }
    management_alert_incident(store, tenant_id, incident_id).await
}

async fn open_management_alert_incident_count(
    store: &PlatformStore,
    tenant_id: Uuid,
) -> Result<u64, ManagementAlertIncidentError> {
    let count: i64 = match store {
        PlatformStore::Sqlite(store) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM alert_incidents
             WHERE tenant_id = ? AND status IN ('pending', 'open')",
            )
            .bind(tenant_id.to_string())
            .fetch_one(store.pool())
            .await?
        }
        PlatformStore::Timescale(pool) => {
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM alert_incidents
             WHERE tenant_id = $1 AND status IN ('pending', 'open')",
            )
            .bind(tenant_id)
            .fetch_one(pool)
            .await?
        }
    };
    u64::try_from(count).map_err(|_| ManagementAlertIncidentError::InvalidStoredIncident)
}

fn management_alert_incident_timestamp(
    value: &str,
) -> Result<DateTime<Utc>, ManagementAlertIncidentError> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .map(|value| value.and_utc())
        })
        .map_err(|_| ManagementAlertIncidentError::InvalidStoredIncident)
}

fn sqlite_management_alert_incident_optional_timestamp(
    row: &SqliteRow,
    column: &str,
) -> Result<Option<DateTime<Utc>>, ManagementAlertIncidentError> {
    row.try_get::<Option<String>, _>(column)?
        .as_deref()
        .map(management_alert_incident_timestamp)
        .transpose()
}

fn sqlite_management_alert_incident_from_row(
    row: SqliteRow,
) -> Result<ManagementAlertIncident, ManagementAlertIncidentError> {
    let id = row.try_get::<String, _>("id")?;
    let rule_id = row.try_get::<String, _>("rule_id")?;
    Ok(ManagementAlertIncident {
        id: Uuid::parse_str(&id)
            .map_err(|_| ManagementAlertIncidentError::InvalidStoredIncident)?,
        rule_id: Uuid::parse_str(&rule_id)
            .map_err(|_| ManagementAlertIncidentError::InvalidStoredIncident)?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        last_value: row.try_get("last_value")?,
        condition_started_at: management_alert_incident_timestamp(
            &row.try_get::<String, _>("condition_started_at")?,
        )?,
        opened_at: sqlite_management_alert_incident_optional_timestamp(&row, "opened_at")?,
        resolved_at: sqlite_management_alert_incident_optional_timestamp(&row, "resolved_at")?,
        acknowledged_at: sqlite_management_alert_incident_optional_timestamp(
            &row,
            "acknowledged_at",
        )?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        updated_at: management_alert_incident_timestamp(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn timescale_management_alert_incident_from_row(
    row: PgRow,
) -> Result<ManagementAlertIncident, ManagementAlertIncidentError> {
    Ok(ManagementAlertIncident {
        id: row.try_get("id")?,
        rule_id: row.try_get("rule_id")?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        last_value: row.try_get("last_value")?,
        condition_started_at: row.try_get("condition_started_at")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        acknowledged_by: row.try_get("acknowledged_by")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn sqlite_management_alert_from_row(
    row: SqliteRow,
) -> Result<ManagementAlert, ManagementAlertError> {
    let id: String = row.try_get("id")?;
    let updated_at: String = row.try_get("updated_at")?;
    Ok(ManagementAlert {
        id: Uuid::parse_str(&id).map_err(|_| ManagementAlertError::InvalidStoredAlertId)?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        last_value: row.try_get("last_value")?,
        updated_at: DateTime::parse_from_rfc3339(&updated_at)
            .map(|value| value.with_timezone(&Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(&updated_at, "%Y-%m-%d %H:%M:%S")
                    .map(|value| value.and_utc())
            })
            .map_err(|_| ManagementAlertError::InvalidStoredAlertTimestamp)?,
    })
}

fn timescale_management_alert_from_row(
    row: PgRow,
) -> Result<ManagementAlert, ManagementAlertError> {
    Ok(ManagementAlert {
        id: row.try_get("id")?,
        rule_name: row.try_get("rule_name")?,
        severity: row.try_get("severity")?,
        device_id: row.try_get("device_id")?,
        status: row.try_get("status")?,
        last_value: row.try_get("last_value")?,
        updated_at: row.try_get("updated_at")?,
    })
}
