use chrono::{DateTime, Duration, Utc};
use iot_storage::SqliteStore;
use iot_stream::{StreamConsumer, StreamError, StreamRecord};
use sqlx::{PgPool, Postgres, Row, Sqlite, Transaction};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleKind {
    EventThreshold,
    WindowAverage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IncidentStatus {
    Pending,
    Open,
    Resolved,
}

#[derive(Debug, Clone)]
struct AlertRule {
    id: Uuid,
    name: String,
    device_id: Option<String>,
    metric_key: String,
    comparison: Comparison,
    threshold: f64,
    window_seconds: Option<i32>,
    for_seconds: i32,
    resolve_after_seconds: i32,
    reopen_grace_seconds: i32,
    hysteresis: Option<f64>,
    severity: Severity,
    reminder_interval_seconds: i32,
}

#[derive(Debug)]
struct AlertIncident {
    id: Uuid,
    status: IncidentStatus,
    condition_started_at: DateTime<Utc>,
    recovery_started_at: Option<DateTime<Utc>>,
    acknowledged_at: Option<DateTime<Utc>>,
    last_reminder_at: Option<DateTime<Utc>>,
    state_version: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Condition {
    Breaching,
    Normal,
    Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlertFlushResult {
    pub read: usize,
    pub evaluated: usize,
    pub opened: usize,
    pub resolved: usize,
    pub reminders: usize,
}

#[derive(Debug, Error)]
pub enum AlertError {
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("invalid stored alert rule: {0}")]
    InvalidRule(String),
}

#[derive(Debug, Clone)]
pub struct AlertEvaluator {
    pool: PgPool,
    batch_size: usize,
}

impl AlertEvaluator {
    pub fn new(pool: PgPool, batch_size: usize) -> Self {
        Self {
            pool,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_event_rules(
        &self,
        consumer: &mut StreamConsumer,
        now: DateTime<Utc>,
    ) -> Result<AlertFlushResult, AlertError> {
        let batch = consumer.poll(self.batch_size, now)?;
        if batch.records.is_empty() {
            return Ok(AlertFlushResult {
                read: 0,
                evaluated: 0,
                opened: 0,
                resolved: 0,
                reminders: 0,
            });
        }

        let mut transaction = self.pool.begin().await?;
        let rules = load_rules(&mut transaction, RuleKind::EventThreshold).await?;
        let mut result = AlertFlushResult {
            read: batch.records.len(),
            evaluated: 0,
            opened: 0,
            resolved: 0,
            reminders: 0,
        };

        for record in &batch.records {
            for rule in rules.iter().filter(|rule| rule_applies_to(rule, record)) {
                let Some(value) = measurement_value(record, &rule.metric_key) else {
                    continue;
                };
                result.evaluated += 1;
                let transition = evaluate_condition(
                    &mut transaction,
                    rule,
                    &record.message.event.device_id,
                    value,
                    record.message.received_at,
                )
                .await?;
                result.opened += usize::from(transition.opened);
                result.resolved += usize::from(transition.resolved);
                result.reminders += usize::from(transition.reminder);
            }
        }

        transaction.commit().await?;
        consumer.commit(batch, now)?;
        Ok(result)
    }

    pub async fn flush_window_rules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<AlertFlushResult, AlertError> {
        let mut transaction = self.pool.begin().await?;
        let rules = load_rules(&mut transaction, RuleKind::WindowAverage).await?;
        let mut result = AlertFlushResult {
            read: 0,
            evaluated: 0,
            opened: 0,
            resolved: 0,
            reminders: 0,
        };

        for rule in rules {
            let window_seconds = rule
                .window_seconds
                .ok_or_else(|| AlertError::InvalidRule("window rule has no window".to_owned()))?;
            let samples = sqlx::query(
                "SELECT device_id,
                        AVG((measurements ->> $1)::double precision) AS observed_value
                 FROM telemetry
                 WHERE event_at >= $2
                   AND measurements ? $1
                   AND ($3::TEXT IS NULL OR device_id = $3)
                 GROUP BY device_id",
            )
            .bind(&rule.metric_key)
            .bind(now - Duration::seconds(i64::from(window_seconds)))
            .bind(&rule.device_id)
            .fetch_all(&mut *transaction)
            .await?;

            for sample in samples {
                let device_id: String = sample.try_get("device_id")?;
                let value: Option<f64> = sample.try_get("observed_value")?;
                let Some(value) = value else {
                    continue;
                };
                result.evaluated += 1;
                let transition =
                    evaluate_condition(&mut transaction, &rule, &device_id, value, now).await?;
                result.opened += usize::from(transition.opened);
                result.resolved += usize::from(transition.resolved);
                result.reminders += usize::from(transition.reminder);
            }
        }

        transaction.commit().await?;
        Ok(result)
    }
}

#[derive(Clone)]
pub struct SqliteAlertEvaluator {
    store: SqliteStore,
    batch_size: usize,
}

impl SqliteAlertEvaluator {
    pub fn new(store: SqliteStore, batch_size: usize) -> Self {
        Self {
            store,
            batch_size: batch_size.max(1),
        }
    }

    pub async fn flush_event_rules(
        &self,
        consumer: &mut StreamConsumer,
        now: DateTime<Utc>,
    ) -> Result<AlertFlushResult, AlertError> {
        let batch = consumer.poll(self.batch_size, now)?;
        if batch.records.is_empty() {
            return Ok(AlertFlushResult {
                read: 0,
                evaluated: 0,
                opened: 0,
                resolved: 0,
                reminders: 0,
            });
        }

        let mut transaction = self.store.pool().begin().await?;
        let rules = load_sqlite_rules(&mut transaction, RuleKind::EventThreshold).await?;
        let mut result = AlertFlushResult {
            read: batch.records.len(),
            evaluated: 0,
            opened: 0,
            resolved: 0,
            reminders: 0,
        };

        for record in &batch.records {
            for rule in rules.iter().filter(|rule| rule_applies_to(rule, record)) {
                let Some(value) = measurement_value(record, &rule.metric_key) else {
                    continue;
                };
                if !sqlite_claim_event_evaluation(&mut transaction, rule.id, record).await? {
                    continue;
                }
                result.evaluated += 1;
                let transition = sqlite_evaluate_condition(
                    &mut transaction,
                    rule,
                    &record.message.event.device_id,
                    value,
                    record.message.received_at,
                )
                .await?;
                result.opened += usize::from(transition.opened);
                result.resolved += usize::from(transition.resolved);
                result.reminders += usize::from(transition.reminder);
            }
        }

        transaction.commit().await?;
        consumer.commit(batch, now)?;
        Ok(result)
    }

    pub async fn flush_window_rules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<AlertFlushResult, AlertError> {
        let mut transaction = self.store.pool().begin().await?;
        let rules = load_sqlite_rules(&mut transaction, RuleKind::WindowAverage).await?;
        let mut result = AlertFlushResult {
            read: 0,
            evaluated: 0,
            opened: 0,
            resolved: 0,
            reminders: 0,
        };

        for rule in rules {
            let window_seconds = rule
                .window_seconds
                .ok_or_else(|| AlertError::InvalidRule("window rule has no window".to_owned()))?;
            let json_path = format!("$.{}", rule.metric_key);
            let window_start = (now - Duration::seconds(i64::from(window_seconds))).to_rfc3339();
            let samples = sqlx::query(
                "SELECT device_id,
                        AVG(CAST(json_extract(measurements, ?) AS REAL)) AS observed_value
                 FROM telemetry
                 WHERE event_at >= ?
                   AND json_type(measurements, ?) IN ('integer', 'real')
                   AND (? IS NULL OR device_id = ?)
                 GROUP BY device_id",
            )
            .bind(&json_path)
            .bind(&window_start)
            .bind(&json_path)
            .bind(&rule.device_id)
            .bind(&rule.device_id)
            .fetch_all(&mut *transaction)
            .await?;

            for sample in samples {
                let device_id: String = sample.try_get("device_id")?;
                let value: Option<f64> = sample.try_get("observed_value")?;
                let Some(value) = value else {
                    continue;
                };
                result.evaluated += 1;
                let transition =
                    sqlite_evaluate_condition(&mut transaction, &rule, &device_id, value, now)
                        .await?;
                result.opened += usize::from(transition.opened);
                result.resolved += usize::from(transition.resolved);
                result.reminders += usize::from(transition.reminder);
            }
        }

        transaction.commit().await?;
        Ok(result)
    }
}

async fn sqlite_claim_event_evaluation(
    transaction: &mut Transaction<'_, Sqlite>,
    rule_id: Uuid,
    record: &StreamRecord,
) -> Result<bool, AlertError> {
    let event = &record.message.event;
    let result = sqlx::query(
        "INSERT INTO alert_rule_event_evaluations (
            rule_id, event_at, device_id, boot_id, sequence
         ) VALUES (?, ?, ?, ?, ?)
         ON CONFLICT (rule_id, event_at, device_id, boot_id, sequence) DO NOTHING",
    )
    .bind(rule_id.to_string())
    .bind(event.event_at.to_rfc3339())
    .bind(&event.device_id)
    .bind(event.boot_id.to_string())
    .bind(event.sequence.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

#[derive(Debug, Default)]
struct Transition {
    opened: bool,
    resolved: bool,
    reminder: bool,
}

async fn load_sqlite_rules(
    transaction: &mut Transaction<'_, Sqlite>,
    kind: RuleKind,
) -> Result<Vec<AlertRule>, AlertError> {
    let kind = match kind {
        RuleKind::EventThreshold => "event_threshold",
        RuleKind::WindowAverage => "window_average",
    };
    let rows = sqlx::query(
        "SELECT id, name, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules
         WHERE enabled = 1 AND archived_at IS NULL AND rule_type = ?
         ORDER BY created_at, id",
    )
    .bind(kind)
    .fetch_all(&mut **transaction)
    .await?;

    rows.into_iter().map(sqlite_rule_from_row).collect()
}

fn sqlite_rule_from_row(row: sqlx::sqlite::SqliteRow) -> Result<AlertRule, AlertError> {
    let id = row.try_get::<String, _>("id")?;
    let id = Uuid::parse_str(&id)
        .map_err(|error| AlertError::InvalidRule(format!("invalid rule id {id:?}: {error}")))?;
    let _kind = match row.try_get::<String, _>("rule_type")?.as_str() {
        "event_threshold" => RuleKind::EventThreshold,
        "window_average" => RuleKind::WindowAverage,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown rule_type {value:?}"
            )));
        }
    };
    let comparison = match row.try_get::<String, _>("comparison")?.as_str() {
        "gt" => Comparison::GreaterThan,
        "gte" => Comparison::GreaterThanOrEqual,
        "lt" => Comparison::LessThan,
        "lte" => Comparison::LessThanOrEqual,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown comparison {value:?}"
            )));
        }
    };
    let severity = match row.try_get::<String, _>("severity")?.as_str() {
        "info" => Severity::Info,
        "warning" => Severity::Warning,
        "critical" => Severity::Critical,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown severity {value:?}"
            )));
        }
    };

    Ok(AlertRule {
        id,
        name: row.try_get("name")?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        comparison,
        threshold: row.try_get("threshold")?,
        window_seconds: row.try_get("window_seconds")?,
        for_seconds: row.try_get("for_seconds")?,
        resolve_after_seconds: row.try_get("resolve_after_seconds")?,
        reopen_grace_seconds: row.try_get("reopen_grace_seconds")?,
        hysteresis: row.try_get("hysteresis")?,
        severity,
        reminder_interval_seconds: row.try_get("reminder_interval_seconds")?,
    })
}

async fn sqlite_open_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<bool, AlertError> {
    let id = Uuid::new_v4();
    let evaluated_at = evaluated_at.to_rfc3339();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at,
            last_value, last_notified_at, last_reminder_at, state_version, created_at, updated_at
         ) VALUES (?, ?, ?, 'open', ?, ?, ?, ?, ?, 1, ?, ?)",
    )
    .bind(id.to_string())
    .bind(rule.id.to_string())
    .bind(device_id)
    .bind(&evaluated_at)
    .bind(&evaluated_at)
    .bind(value)
    .bind(&evaluated_at)
    .bind(&evaluated_at)
    .bind(&evaluated_at)
    .bind(&evaluated_at)
    .execute(&mut **transaction)
    .await?;

    let severity = severity_name(rule.severity);
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, 'opened', ?, ?, ?, ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(id.to_string())
    .bind(format!("incident:{id}:opened:1"))
    .bind(format!("[{severity}] {} opened", rule.name))
    .bind(format!(
        "Rule: {}\nDevice: {device_id}\nMetric: {}\nValue: {value:.3}\nThreshold: {:.3}\nState: opened\n",
        rule.name, rule.metric_key, rule.threshold
    ))
    .bind(&evaluated_at)
    .bind(&evaluated_at)
    .execute(&mut **transaction)
    .await?;

    Ok(true)
}

#[derive(Debug)]
struct SqliteIncident {
    id: String,
    status: IncidentStatus,
    condition_started_at: DateTime<Utc>,
    recovery_started_at: Option<DateTime<Utc>>,
    acknowledged_at: Option<DateTime<Utc>>,
    last_reminder_at: Option<DateTime<Utc>>,
    state_version: i32,
}

async fn sqlite_evaluate_condition(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    let condition = classify(rule, value);
    let active = load_sqlite_active_incident(transaction, rule.id, device_id).await?;

    match (active, condition) {
        (None, Condition::Breaching) => {
            if let Some(resolved) = load_sqlite_recent_resolved_incident(
                transaction,
                rule.id,
                device_id,
                evaluated_at - Duration::seconds(i64::from(rule.reopen_grace_seconds)),
            )
            .await?
            {
                return sqlite_start_breach_on_existing(
                    transaction,
                    rule,
                    resolved,
                    device_id,
                    value,
                    evaluated_at,
                )
                .await;
            }
            sqlite_create_incident(transaction, rule, device_id, value, evaluated_at).await
        }
        (None, Condition::Normal | Condition::Indeterminate) => Ok(Transition::default()),
        (Some(incident), Condition::Breaching) => {
            sqlite_handle_breach(transaction, rule, incident, device_id, value, evaluated_at).await
        }
        (Some(incident), Condition::Normal) => {
            sqlite_handle_normal(transaction, rule, incident, device_id, value, evaluated_at).await
        }
        (Some(_), Condition::Indeterminate) => Ok(Transition::default()),
    }
}

async fn sqlite_create_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    if rule.for_seconds == 0 {
        sqlite_open_incident(transaction, rule, device_id, value, evaluated_at).await?;
        return Ok(Transition {
            opened: true,
            ..Transition::default()
        });
    }

    let id = Uuid::new_v4();
    let evaluated_at = evaluated_at.to_rfc3339();
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
         ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)",
    )
    .bind(id.to_string())
    .bind(rule.id.to_string())
    .bind(device_id)
    .bind(&evaluated_at)
    .bind(value)
    .bind(&evaluated_at)
    .bind(&evaluated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(Transition::default())
}

async fn sqlite_start_breach_on_existing(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident: SqliteIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    let evaluated_at = evaluated_at.to_rfc3339();
    if rule.for_seconds == 0 {
        let state_version = incident.state_version + 1;
        sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', condition_started_at = ?, recovery_started_at = NULL,
                 opened_at = ?, resolved_at = NULL, acknowledged_at = NULL,
                 acknowledged_by = NULL, last_value = ?, last_notified_at = ?,
                 last_reminder_at = ?, state_version = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(&evaluated_at)
        .bind(&evaluated_at)
        .bind(value)
        .bind(&evaluated_at)
        .bind(&evaluated_at)
        .bind(state_version)
        .bind(&evaluated_at)
        .bind(&incident.id)
        .execute(&mut **transaction)
        .await?;
        sqlite_enqueue_opened(
            transaction,
            rule,
            &incident.id,
            device_id,
            value,
            state_version,
            &evaluated_at,
        )
        .await?;
        return Ok(Transition {
            opened: true,
            ..Transition::default()
        });
    }

    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'pending', condition_started_at = ?, recovery_started_at = NULL,
             opened_at = NULL, resolved_at = NULL, acknowledged_at = NULL,
             acknowledged_by = NULL, last_value = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(&evaluated_at)
    .bind(value)
    .bind(&evaluated_at)
    .bind(&incident.id)
    .execute(&mut **transaction)
    .await?;
    Ok(Transition::default())
}

async fn sqlite_handle_breach(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident: SqliteIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    match incident.status {
        IncidentStatus::Pending => {
            if evaluated_at - incident.condition_started_at
                >= Duration::seconds(i64::from(rule.for_seconds))
            {
                let state_version = incident.state_version + 1;
                let evaluated_at_text = evaluated_at.to_rfc3339();
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'open', recovery_started_at = NULL, opened_at = ?,
                         last_value = ?, last_notified_at = ?, last_reminder_at = ?,
                         state_version = ?, updated_at = ?
                     WHERE id = ?",
                )
                .bind(&evaluated_at_text)
                .bind(value)
                .bind(&evaluated_at_text)
                .bind(&evaluated_at_text)
                .bind(state_version)
                .bind(&evaluated_at_text)
                .bind(&incident.id)
                .execute(&mut **transaction)
                .await?;
                sqlite_enqueue_opened(
                    transaction,
                    rule,
                    &incident.id,
                    device_id,
                    value,
                    state_version,
                    &evaluated_at_text,
                )
                .await?;
                return Ok(Transition {
                    opened: true,
                    ..Transition::default()
                });
            }
            sqlite_update_incident_value(transaction, &incident.id, value, evaluated_at).await?;
        }
        IncidentStatus::Open => {
            let evaluated_at_text = evaluated_at.to_rfc3339();
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = ?, updated_at = ?
                 WHERE id = ?",
            )
            .bind(value)
            .bind(&evaluated_at_text)
            .bind(&incident.id)
            .execute(&mut **transaction)
            .await?;

            let due = incident.acknowledged_at.is_none()
                && incident.last_reminder_at.is_none_or(|last_reminder_at| {
                    evaluated_at - last_reminder_at
                        >= Duration::seconds(i64::from(rule.reminder_interval_seconds))
                });
            if due {
                sqlx::query(
                    "UPDATE alert_incidents
                     SET last_reminder_at = ?, last_notified_at = ?, updated_at = ?
                     WHERE id = ?",
                )
                .bind(&evaluated_at_text)
                .bind(&evaluated_at_text)
                .bind(&evaluated_at_text)
                .bind(&incident.id)
                .execute(&mut **transaction)
                .await?;
                sqlite_enqueue_reminder(
                    transaction,
                    rule,
                    &incident.id,
                    device_id,
                    value,
                    incident.state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(Transition {
                    reminder: true,
                    ..Transition::default()
                });
            }
        }
        IncidentStatus::Resolved => {}
    }
    Ok(Transition::default())
}

async fn load_sqlite_active_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule_id: Uuid,
    device_id: &str,
) -> Result<Option<SqliteIncident>, AlertError> {
    let row = sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at,
                acknowledged_at, last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = ? AND device_id = ? AND status IN ('pending', 'open')",
    )
    .bind(rule_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(sqlite_incident_from_row).transpose()
}

async fn load_sqlite_recent_resolved_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule_id: Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<SqliteIncident>, AlertError> {
    let row = sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at,
                acknowledged_at, last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = ? AND device_id = ? AND status = 'resolved'
           AND resolved_at >= ?
         ORDER BY resolved_at DESC
         LIMIT 1",
    )
    .bind(rule_id.to_string())
    .bind(device_id)
    .bind(reopen_after.to_rfc3339())
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(sqlite_incident_from_row).transpose()
}

fn sqlite_incident_from_row(row: sqlx::sqlite::SqliteRow) -> Result<SqliteIncident, AlertError> {
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "pending" => IncidentStatus::Pending,
        "open" => IncidentStatus::Open,
        "resolved" => IncidentStatus::Resolved,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown incident status {value:?}"
            )));
        }
    };
    let condition_started_at = sqlite_timestamp(Some(row.try_get("condition_started_at")?))?
        .ok_or_else(|| {
            AlertError::InvalidRule("missing incident condition_started_at".to_owned())
        })?;
    Ok(SqliteIncident {
        id: row.try_get("id")?,
        status,
        condition_started_at,
        recovery_started_at: sqlite_timestamp(row.try_get("recovery_started_at")?)?,
        acknowledged_at: sqlite_timestamp(row.try_get("acknowledged_at")?)?,
        last_reminder_at: sqlite_timestamp(row.try_get("last_reminder_at")?)?,
        state_version: row.try_get("state_version")?,
    })
}

async fn sqlite_update_incident_value(
    transaction: &mut Transaction<'_, Sqlite>,
    id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<(), AlertError> {
    sqlx::query(
        "UPDATE alert_incidents
         SET last_value = ?, updated_at = ?
         WHERE id = ?",
    )
    .bind(value)
    .bind(evaluated_at.to_rfc3339())
    .bind(id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn sqlite_timestamp(value: Option<String>) -> Result<Option<DateTime<Utc>>, AlertError> {
    value
        .map(|value| {
            DateTime::parse_from_rfc3339(&value)
                .map(|timestamp| timestamp.with_timezone(&Utc))
                .map_err(|error| {
                    AlertError::InvalidRule(format!("invalid SQLite timestamp {value:?}: {error}"))
                })
        })
        .transpose()
}

async fn sqlite_enqueue_opened(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident_id: &str,
    device_id: &str,
    value: f64,
    state_version: i32,
    created_at: &str,
) -> Result<(), AlertError> {
    let severity = severity_name(rule.severity);
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, 'opened', ?, ?, ?, ?, ?)
         ON CONFLICT (dedupe_key) DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(incident_id)
    .bind(format!("incident:{incident_id}:opened:{state_version}"))
    .bind(format!("[{severity}] {} opened", rule.name))
    .bind(format!(
        "Rule: {}\nDevice: {device_id}\nMetric: {}\nValue: {value:.3}\nThreshold: {:.3}\nState: opened\n",
        rule.name, rule.metric_key, rule.threshold
    ))
    .bind(created_at)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn sqlite_enqueue_reminder(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident_id: &str,
    device_id: &str,
    value: f64,
    state_version: i32,
    created_at: DateTime<Utc>,
) -> Result<(), AlertError> {
    let interval = i64::from(rule.reminder_interval_seconds);
    let dedupe_key = format!(
        "incident:{incident_id}:reminder:{state_version}:{}",
        created_at.timestamp().div_euclid(interval)
    );
    let severity = severity_name(rule.severity);
    let created_at = created_at.to_rfc3339();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, 'reminder', ?, ?, ?, ?, ?)
         ON CONFLICT (dedupe_key) DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(incident_id)
    .bind(dedupe_key)
    .bind(format!("[{severity}] {} reminder", rule.name))
    .bind(format!(
        "Rule: {}\nDevice: {device_id}\nMetric: {}\nValue: {value:.3}\nThreshold: {:.3}\nState: reminder\n",
        rule.name, rule.metric_key, rule.threshold
    ))
    .bind(&created_at)
    .bind(&created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn sqlite_handle_normal(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident: SqliteIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    match incident.status {
        IncidentStatus::Pending => {
            sqlx::query("DELETE FROM alert_incidents WHERE id = ?")
                .bind(&incident.id)
                .execute(&mut **transaction)
                .await?;
        }
        IncidentStatus::Open => {
            let recovery_started_at = incident.recovery_started_at.unwrap_or(evaluated_at);
            if evaluated_at - recovery_started_at
                >= Duration::seconds(i64::from(rule.resolve_after_seconds))
            {
                let evaluated_at_text = evaluated_at.to_rfc3339();
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'resolved', recovery_started_at = ?, resolved_at = ?,
                         last_value = ?, last_notified_at = ?, state_version = ?, updated_at = ?
                     WHERE id = ?",
                )
                .bind(&evaluated_at_text)
                .bind(&evaluated_at_text)
                .bind(value)
                .bind(&evaluated_at_text)
                .bind(state_version)
                .bind(&evaluated_at_text)
                .bind(&incident.id)
                .execute(&mut **transaction)
                .await?;
                sqlite_enqueue_resolved(
                    transaction,
                    rule,
                    &incident.id,
                    device_id,
                    value,
                    state_version,
                    &evaluated_at_text,
                )
                .await?;
                return Ok(Transition {
                    resolved: true,
                    ..Transition::default()
                });
            }
            let recovery_started_at = recovery_started_at.to_rfc3339();
            let evaluated_at = evaluated_at.to_rfc3339();
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = ?, last_value = ?, updated_at = ?
                 WHERE id = ?",
            )
            .bind(recovery_started_at)
            .bind(value)
            .bind(evaluated_at)
            .bind(&incident.id)
            .execute(&mut **transaction)
            .await?;
        }
        IncidentStatus::Resolved => {}
    }
    Ok(Transition::default())
}

async fn sqlite_enqueue_resolved(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident_id: &str,
    device_id: &str,
    value: f64,
    state_version: i32,
    created_at: &str,
) -> Result<(), AlertError> {
    let severity = severity_name(rule.severity);
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, 'resolved', ?, ?, ?, ?, ?)
         ON CONFLICT (dedupe_key) DO NOTHING",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(incident_id)
    .bind(format!("incident:{incident_id}:resolved:{state_version}"))
    .bind(format!("[{severity}] {} resolved", rule.name))
    .bind(format!(
        "Rule: {}\nDevice: {device_id}\nMetric: {}\nValue: {value:.3}\nThreshold: {:.3}\nState: resolved\n",
        rule.name, rule.metric_key, rule.threshold
    ))
    .bind(created_at)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_rules(
    transaction: &mut Transaction<'_, Postgres>,
    kind: RuleKind,
) -> Result<Vec<AlertRule>, AlertError> {
    let kind = match kind {
        RuleKind::EventThreshold => "event_threshold",
        RuleKind::WindowAverage => "window_average",
    };
    let rows = sqlx::query(
        "SELECT id, name, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules
         WHERE enabled AND archived_at IS NULL AND rule_type = $1
         ORDER BY created_at, id
         FOR SHARE",
    )
    .bind(kind)
    .fetch_all(&mut **transaction)
    .await?;

    rows.into_iter().map(rule_from_row).collect()
}

fn rule_from_row(row: sqlx::postgres::PgRow) -> Result<AlertRule, AlertError> {
    let _kind = match row.try_get::<String, _>("rule_type")?.as_str() {
        "event_threshold" => RuleKind::EventThreshold,
        "window_average" => RuleKind::WindowAverage,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown rule_type {value:?}"
            )));
        }
    };
    let comparison = match row.try_get::<String, _>("comparison")?.as_str() {
        "gt" => Comparison::GreaterThan,
        "gte" => Comparison::GreaterThanOrEqual,
        "lt" => Comparison::LessThan,
        "lte" => Comparison::LessThanOrEqual,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown comparison {value:?}"
            )));
        }
    };
    let severity = match row.try_get::<String, _>("severity")?.as_str() {
        "info" => Severity::Info,
        "warning" => Severity::Warning,
        "critical" => Severity::Critical,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown severity {value:?}"
            )));
        }
    };

    Ok(AlertRule {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        comparison,
        threshold: row.try_get("threshold")?,
        window_seconds: row.try_get("window_seconds")?,
        for_seconds: row.try_get("for_seconds")?,
        resolve_after_seconds: row.try_get("resolve_after_seconds")?,
        reopen_grace_seconds: row.try_get("reopen_grace_seconds")?,
        hysteresis: row.try_get("hysteresis")?,
        severity,
        reminder_interval_seconds: row.try_get("reminder_interval_seconds")?,
    })
}

fn rule_applies_to(rule: &AlertRule, record: &StreamRecord) -> bool {
    rule.device_id
        .as_deref()
        .is_none_or(|device_id| device_id == record.message.event.device_id)
}

fn measurement_value(record: &StreamRecord, metric_key: &str) -> Option<f64> {
    record
        .message
        .event
        .measurements
        .get(metric_key)
        .and_then(serde_json::Value::as_f64)
}

async fn evaluate_condition(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    let condition = classify(rule, value);
    let active = load_active_incident(transaction, rule.id, device_id).await?;

    match (active, condition) {
        (None, Condition::Breaching) => {
            if let Some(resolved) = load_recent_resolved_incident(
                transaction,
                rule.id,
                device_id,
                evaluated_at - Duration::seconds(i64::from(rule.reopen_grace_seconds)),
            )
            .await?
            {
                return start_breach_on_existing(
                    transaction,
                    rule,
                    resolved,
                    device_id,
                    value,
                    evaluated_at,
                )
                .await;
            }
            create_incident(transaction, rule, device_id, value, evaluated_at).await
        }
        (None, Condition::Normal | Condition::Indeterminate) => Ok(Transition::default()),
        (Some(incident), Condition::Breaching) => {
            handle_breach(transaction, rule, incident, device_id, value, evaluated_at).await
        }
        (Some(incident), Condition::Normal) => {
            handle_normal(transaction, rule, incident, device_id, value, evaluated_at).await
        }
        (Some(_), Condition::Indeterminate) => Ok(Transition::default()),
    }
}

async fn create_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    let id = Uuid::new_v4();
    if rule.for_seconds == 0 {
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, rule_id, device_id, status, condition_started_at, opened_at,
                last_value, last_notified_at, last_reminder_at, state_version, created_at, updated_at
             ) VALUES ($1, $2, $3, 'open', $4, $4, $5, $4, $4, 1, $4, $4)",
        )
        .bind(id)
        .bind(rule.id)
        .bind(device_id)
        .bind(evaluated_at)
        .bind(value)
        .execute(&mut **transaction)
        .await?;
        enqueue_notification(
            transaction,
            rule,
            id,
            device_id,
            value,
            "opened",
            1,
            evaluated_at,
        )
        .await?;
        return Ok(Transition {
            opened: true,
            ..Transition::default()
        });
    }

    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
         ) VALUES ($1, $2, $3, 'pending', $4, $5, $4, $4)",
    )
    .bind(id)
    .bind(rule.id)
    .bind(device_id)
    .bind(evaluated_at)
    .bind(value)
    .execute(&mut **transaction)
    .await?;
    Ok(Transition::default())
}

async fn start_breach_on_existing(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident: AlertIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    if rule.for_seconds == 0 {
        let state_version = incident.state_version + 1;
        sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', condition_started_at = $2, recovery_started_at = NULL,
                 opened_at = $2, resolved_at = NULL, acknowledged_at = NULL,
                 acknowledged_by = NULL, last_value = $3, last_notified_at = $2,
                 last_reminder_at = $2, state_version = $4, updated_at = $2
             WHERE id = $1",
        )
        .bind(incident.id)
        .bind(evaluated_at)
        .bind(value)
        .bind(state_version)
        .execute(&mut **transaction)
        .await?;
        enqueue_notification(
            transaction,
            rule,
            incident.id,
            device_id,
            value,
            "opened",
            state_version,
            evaluated_at,
        )
        .await?;
        return Ok(Transition {
            opened: true,
            ..Transition::default()
        });
    }

    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'pending', condition_started_at = $2, recovery_started_at = NULL,
             opened_at = NULL, resolved_at = NULL, acknowledged_at = NULL,
             acknowledged_by = NULL, last_value = $3, updated_at = $2
         WHERE id = $1",
    )
    .bind(incident.id)
    .bind(evaluated_at)
    .bind(value)
    .execute(&mut **transaction)
    .await?;
    Ok(Transition::default())
}

async fn handle_breach(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident: AlertIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    match incident.status {
        IncidentStatus::Pending => {
            if evaluated_at - incident.condition_started_at
                >= Duration::seconds(i64::from(rule.for_seconds))
            {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'open', recovery_started_at = NULL, opened_at = $2,
                         last_value = $3, last_notified_at = $2, last_reminder_at = $2,
                         state_version = $4, updated_at = $2
                     WHERE id = $1",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(value)
                .bind(state_version)
                .execute(&mut **transaction)
                .await?;
                enqueue_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "opened",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(Transition {
                    opened: true,
                    ..Transition::default()
                });
            }
            update_incident_value(transaction, incident.id, value, evaluated_at).await?;
        }
        IncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = $2, updated_at = $3
                 WHERE id = $1",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .execute(&mut **transaction)
            .await?;

            if incident.acknowledged_at.is_none()
                && incident.last_reminder_at.is_none_or(|at| {
                    evaluated_at - at
                        >= Duration::seconds(i64::from(rule.reminder_interval_seconds))
                })
            {
                sqlx::query(
                    "UPDATE alert_incidents
                     SET last_reminder_at = $2, last_notified_at = $2, updated_at = $2
                     WHERE id = $1",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .execute(&mut **transaction)
                .await?;
                enqueue_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "reminder",
                    incident.state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(Transition {
                    reminder: true,
                    ..Transition::default()
                });
            }
        }
        IncidentStatus::Resolved => {}
    }
    Ok(Transition::default())
}

async fn handle_normal(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident: AlertIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<Transition, AlertError> {
    match incident.status {
        IncidentStatus::Pending => {
            sqlx::query("DELETE FROM alert_incidents WHERE id = $1")
                .bind(incident.id)
                .execute(&mut **transaction)
                .await?;
        }
        IncidentStatus::Open => {
            let recovery_started_at = incident.recovery_started_at.unwrap_or(evaluated_at);
            if evaluated_at - recovery_started_at
                >= Duration::seconds(i64::from(rule.resolve_after_seconds))
            {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'resolved', recovery_started_at = $2, resolved_at = $2,
                         last_value = $3, last_notified_at = $2, state_version = $4,
                         updated_at = $2
                     WHERE id = $1",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(value)
                .bind(state_version)
                .execute(&mut **transaction)
                .await?;
                enqueue_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "resolved",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(Transition {
                    resolved: true,
                    ..Transition::default()
                });
            }
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = $2, last_value = $3, updated_at = $2
                 WHERE id = $1",
            )
            .bind(incident.id)
            .bind(recovery_started_at)
            .bind(value)
            .execute(&mut **transaction)
            .await?;
        }
        IncidentStatus::Resolved => {}
    }
    Ok(Transition::default())
}

async fn update_incident_value(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<(), AlertError> {
    sqlx::query(
        "UPDATE alert_incidents
         SET last_value = $2, updated_at = $3
         WHERE id = $1",
    )
    .bind(id)
    .bind(value)
    .bind(evaluated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn load_active_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule_id: Uuid,
    device_id: &str,
) -> Result<Option<AlertIncident>, AlertError> {
    let row = sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at,
                acknowledged_at, last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = $1 AND device_id = $2 AND status IN ('pending', 'open')
         FOR UPDATE",
    )
    .bind(rule_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(incident_from_row).transpose()
}

async fn load_recent_resolved_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule_id: Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<AlertIncident>, AlertError> {
    let row = sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at,
                acknowledged_at, last_reminder_at, state_version
         FROM alert_incidents
         WHERE rule_id = $1 AND device_id = $2 AND status = 'resolved'
           AND resolved_at >= $3
         ORDER BY resolved_at DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(rule_id)
    .bind(device_id)
    .bind(reopen_after)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(incident_from_row).transpose()
}

fn incident_from_row(row: sqlx::postgres::PgRow) -> Result<AlertIncident, AlertError> {
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "pending" => IncidentStatus::Pending,
        "open" => IncidentStatus::Open,
        "resolved" => IncidentStatus::Resolved,
        value => {
            return Err(AlertError::InvalidRule(format!(
                "unknown incident status {value:?}"
            )));
        }
    };
    Ok(AlertIncident {
        id: row.try_get("id")?,
        status,
        condition_started_at: row.try_get("condition_started_at")?,
        recovery_started_at: row.try_get("recovery_started_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        last_reminder_at: row.try_get("last_reminder_at")?,
        state_version: row.try_get("state_version")?,
    })
}

async fn enqueue_notification(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident_id: Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i32,
    created_at: DateTime<Utc>,
) -> Result<(), AlertError> {
    let dedupe_key = match kind {
        "opened" | "resolved" => format!("incident:{incident_id}:{kind}:{state_version}"),
        "reminder" => {
            let interval = i64::from(rule.reminder_interval_seconds);
            format!(
                "incident:{incident_id}:reminder:{state_version}:{}",
                created_at.timestamp().div_euclid(interval)
            )
        }
        _ => {
            return Err(AlertError::InvalidRule(format!(
                "unknown notification kind {kind:?}"
            )));
        }
    };
    let severity = severity_name(rule.severity);
    let subject = format!("[{severity}] {} {kind}", rule.name);
    let body = format!(
        "Rule: {}\nDevice: {}\nMetric: {}\nValue: {:.3}\nThreshold: {:.3}\nState: {kind}\n",
        rule.name, device_id, rule.metric_key, value, rule.threshold
    );
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
         ON CONFLICT (dedupe_key) DO NOTHING",
    )
    .bind(Uuid::new_v4())
    .bind(incident_id)
    .bind(kind)
    .bind(dedupe_key)
    .bind(subject)
    .bind(body)
    .bind(created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn classify(rule: &AlertRule, value: f64) -> Condition {
    let hysteresis = rule.hysteresis.unwrap_or(0.0);
    match rule.comparison {
        Comparison::GreaterThan => {
            if value > rule.threshold {
                Condition::Breaching
            } else if value <= rule.threshold - hysteresis {
                Condition::Normal
            } else {
                Condition::Indeterminate
            }
        }
        Comparison::GreaterThanOrEqual => {
            if value >= rule.threshold {
                Condition::Breaching
            } else if value < rule.threshold - hysteresis {
                Condition::Normal
            } else {
                Condition::Indeterminate
            }
        }
        Comparison::LessThan => {
            if value < rule.threshold {
                Condition::Breaching
            } else if value >= rule.threshold + hysteresis {
                Condition::Normal
            } else {
                Condition::Indeterminate
            }
        }
        Comparison::LessThanOrEqual => {
            if value <= rule.threshold {
                Condition::Breaching
            } else if value > rule.threshold + hysteresis {
                Condition::Normal
            } else {
                Condition::Indeterminate
            }
        }
    }
}

fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Info => "INFO",
        Severity::Warning => "WARNING",
        Severity::Critical => "CRITICAL",
    }
}
