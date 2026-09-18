use std::{future::Future, pin::Pin};

use chrono::{DateTime, Duration as ChronoDuration, NaiveDateTime, Utc};
use sqlx::{PgPool, Postgres, Row, Sqlite, Transaction, postgres::PgRow, sqlite::SqliteRow};

use crate::{
    AlertComparison, AlertEvaluationEvent, AlertEvaluationRepository, AlertEvaluationResult,
    AlertIncident, AlertIncidentRepository, AlertIncidentStatus, AlertRule, AlertRuleKind,
    AlertSeverity, NewAlertIncident, NewNotificationOutboxEntry, NotificationOutboxRecord,
    PlatformStore, PlatformStoreError, SqliteStore, canonical_notification,
    canonical_postgres_timestamp,
};

impl PlatformStore {
    pub async fn evaluate_alert_events(
        &self,
        events: &[AlertEvaluationEvent],
        _evaluated_at: DateTime<Utc>,
    ) -> Result<AlertEvaluationResult, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => evaluate_sqlite_alert_events(store, events).await,
            Self::Timescale(pool) => evaluate_timescale_alert_events(pool, events).await,
        }
    }

    pub async fn evaluate_alert_windows(
        &self,
        evaluated_at: DateTime<Utc>,
    ) -> Result<AlertEvaluationResult, PlatformStoreError> {
        let evaluated_at = canonical_postgres_timestamp(evaluated_at);
        match self {
            Self::Sqlite(store) => evaluate_sqlite_alert_windows(store, evaluated_at).await,
            Self::Timescale(pool) => evaluate_timescale_alert_windows(pool, evaluated_at).await,
        }
    }
    pub async fn create_incident(
        &self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let incident = canonical_incident(incident);
        let opened_notification = opened_notification.map(canonical_notification);
        if opened_notification
            .as_ref()
            .is_some_and(|notification| notification.tenant_id != incident.tenant_id)
        {
            return Err(PlatformStoreError::NotificationTenantMismatch);
        }
        match self {
            Self::Sqlite(store) => Ok(store.create_incident(incident, opened_notification).await?),
            Self::Timescale(pool) => {
                create_timescale_incident(pool, incident, opened_notification).await
            }
        }
    }

    pub async fn update_incident_last_value(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_last_value(
                    tenant_id,
                    &incident_id.to_string(),
                    expected_version,
                    last_value,
                    canonical_postgres_timestamp(updated_at),
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_last_value(
                    pool,
                    tenant_id,
                    incident_id,
                    expected_version,
                    last_value,
                    canonical_postgres_timestamp(updated_at),
                )
                .await
            }
        }
    }

    pub async fn open_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Open(canonical_postgres_timestamp(opened_at)),
        )
        .await
    }

    pub async fn open_incident_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Open(canonical_postgres_timestamp(opened_at)),
            canonical_notification(notification),
        )
        .await
    }

    pub async fn recover_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Recover(canonical_postgres_timestamp(recovery_started_at)),
        )
        .await
    }

    pub async fn resolve_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Resolve(canonical_postgres_timestamp(resolved_at)),
        )
        .await
    }

    pub async fn resolve_incident_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Resolve(canonical_postgres_timestamp(resolved_at)),
            canonical_notification(notification),
        )
        .await
    }

    pub async fn remind_incident(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Remind(canonical_postgres_timestamp(reminded_at)),
        )
        .await
    }

    pub async fn remind_incident_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        self.update_incident_transition_with_notification(
            tenant_id,
            incident_id,
            expected_version,
            AlertIncidentTransition::Remind(canonical_postgres_timestamp(reminded_at)),
            canonical_notification(notification),
        )
        .await
    }

    async fn update_incident_transition(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        transition: AlertIncidentTransition,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_transition(
                    tenant_id,
                    &incident_id.to_string(),
                    expected_version,
                    transition,
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_transition(
                    pool,
                    tenant_id,
                    incident_id,
                    expected_version,
                    transition,
                )
                .await
            }
        }
    }

    async fn update_incident_transition_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        transition: AlertIncidentTransition,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        if notification.tenant_id != tenant_id {
            return Err(PlatformStoreError::NotificationTenantMismatch);
        }
        match self {
            Self::Sqlite(store) => Ok(store
                .update_incident_transition_with_notification(
                    tenant_id,
                    &incident_id.to_string(),
                    expected_version,
                    transition,
                    notification,
                )
                .await?),
            Self::Timescale(pool) => {
                update_timescale_incident_transition_with_notification(
                    pool,
                    tenant_id,
                    incident_id,
                    expected_version,
                    transition,
                    notification,
                )
                .await
            }
        }
    }
}

async fn create_timescale_incident(
    pool: &PgPool,
    incident: NewAlertIncident,
    opened_notification: Option<NewNotificationOutboxEntry>,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let inserted = sqlx::query(
        "INSERT INTO alert_incidents (
            id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, last_value
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT DO NOTHING",
    )
    .bind(incident.id)
    .bind(incident.tenant_id)
    .bind(incident.rule_id)
    .bind(&incident.device_id)
    .bind(incident.status.as_str())
    .bind(incident.condition_started_at)
    .bind((incident.status == AlertIncidentStatus::Open).then_some(incident.condition_started_at))
    .bind(incident.last_value)
    .execute(&mut *transaction)
    .await?;
    if inserted.rows_affected() == 0 {
        return Ok(None);
    }
    if let Some(notification) = opened_notification {
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(notification.id)
        .bind(incident.tenant_id)
        .bind(incident.id)
        .bind(notification.kind.as_str())
        .bind(notification.dedupe_key)
        .bind(notification.subject)
        .bind(notification.body)
        .bind(notification.next_attempt_at)
        .execute(&mut *transaction)
        .await?;
    }
    let row = sqlx::query(
        "SELECT id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                state_version
         FROM alert_incidents WHERE id = $1 AND tenant_id = $2",
    )
    .bind(incident.id)
    .bind(incident.tenant_id)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    postgres_alert_incident_record(row).map(Some)
}

async fn update_timescale_incident_last_value(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    expected_version: i64,
    last_value: Option<f64>,
    updated_at: DateTime<Utc>,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let row = sqlx::query(
        "UPDATE alert_incidents
         SET last_value = $1, state_version = state_version + 1, updated_at = $2
         WHERE id = $3 AND tenant_id = $5 AND state_version = $4 AND status IN ('pending', 'open')
         RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                   opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                   state_version",
    )
    .bind(last_value)
    .bind(updated_at)
    .bind(incident_id)
    .bind(expected_version)
    .bind(tenant_id)
    .fetch_optional(pool)
    .await?;
    row.map(postgres_alert_incident_record).transpose()
}

async fn update_timescale_incident_transition(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    expected_version: i64,
    transition: AlertIncidentTransition,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let query = match transition {
        AlertIncidentTransition::Open(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', opened_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'pending'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Recover(_) => sqlx::query(
            "UPDATE alert_incidents
             SET recovery_started_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
                   AND recovery_started_at IS NULL
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Resolve(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'resolved', resolved_at = $1,
                 recovery_started_at = COALESCE(recovery_started_at, $1), updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Remind(_) => sqlx::query(
            "UPDATE alert_incidents
             SET last_reminder_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
    };
    let row = query
        .bind(transition.timestamp())
        .bind(incident_id)
        .bind(expected_version)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?;
    row.map(postgres_alert_incident_record).transpose()
}

async fn update_timescale_incident_transition_with_notification(
    pool: &PgPool,
    tenant_id: uuid::Uuid,
    incident_id: uuid::Uuid,
    expected_version: i64,
    transition: AlertIncidentTransition,
    notification: NewNotificationOutboxEntry,
) -> Result<Option<AlertIncident>, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let query = match transition {
        AlertIncidentTransition::Open(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', opened_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'pending'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Resolve(_) => sqlx::query(
            "UPDATE alert_incidents
             SET status = 'resolved', resolved_at = $1,
                 recovery_started_at = COALESCE(recovery_started_at, $1), updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Remind(_) => sqlx::query(
            "UPDATE alert_incidents
             SET last_reminder_at = $1, updated_at = $1,
                 state_version = state_version + 1
             WHERE id = $2 AND tenant_id = $4 AND state_version = $3 AND status = 'open'
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        ),
        AlertIncidentTransition::Recover(_) => unreachable!(),
    };
    let row = query
        .bind(transition.timestamp())
        .bind(incident_id)
        .bind(expected_version)
        .bind(tenant_id)
        .fetch_optional(&mut *transaction)
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(notification.id)
    .bind(tenant_id)
    .bind(incident_id)
    .bind(notification.kind.as_str())
    .bind(notification.dedupe_key)
    .bind(notification.subject)
    .bind(notification.body)
    .bind(notification.next_attempt_at)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    postgres_alert_incident_record(row).map(Some)
}

fn event_condition(rule: &AlertRule, value: f64) -> Option<bool> {
    let hysteresis = rule.hysteresis.unwrap_or(0.0);
    Some(match rule.comparison {
        AlertComparison::GreaterThan if value > rule.threshold => true,
        AlertComparison::GreaterThan if value <= rule.threshold - hysteresis => false,
        AlertComparison::GreaterThanOrEqual if value >= rule.threshold => true,
        AlertComparison::GreaterThanOrEqual if value < rule.threshold - hysteresis => false,
        AlertComparison::LessThan if value < rule.threshold => true,
        AlertComparison::LessThan if value >= rule.threshold + hysteresis => false,
        AlertComparison::LessThanOrEqual if value <= rule.threshold => true,
        AlertComparison::LessThanOrEqual if value > rule.threshold + hysteresis => false,
        _ => return None,
    })
}

fn alert_severity_name(severity: AlertSeverity) -> &'static str {
    match severity {
        AlertSeverity::Info => "INFO",
        AlertSeverity::Warning => "WARNING",
        AlertSeverity::Critical => "CRITICAL",
    }
}

#[derive(Default)]
struct EventTransition {
    opened: bool,
    resolved: bool,
    reminder: bool,
}

struct EventIncident {
    id: uuid::Uuid,
    status: AlertIncidentStatus,
    condition_started_at: DateTime<Utc>,
    recovery_started_at: Option<DateTime<Utc>>,
    acknowledged_at: Option<DateTime<Utc>>,
    last_reminder_at: Option<DateTime<Utc>>,
    state_version: i64,
}

fn sqlite_event_incident(row: SqliteRow) -> Result<EventIncident, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    Ok(EventIncident {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidIncidentId(id))?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: incident_timestamp(&row, "condition_started_at")?,
        recovery_started_at: incident_optional_timestamp(&row, "recovery_started_at")?,
        acknowledged_at: incident_optional_timestamp(&row, "acknowledged_at")?,
        last_reminder_at: incident_optional_timestamp(&row, "last_reminder_at")?,
        state_version: row.try_get("state_version")?,
    })
}

fn postgres_event_incident(row: PgRow) -> Result<EventIncident, PlatformStoreError> {
    Ok(EventIncident {
        id: row.try_get("id")?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: row.try_get("condition_started_at")?,
        recovery_started_at: row.try_get("recovery_started_at")?,
        acknowledged_at: row.try_get("acknowledged_at")?,
        last_reminder_at: row.try_get("last_reminder_at")?,
        state_version: i64::from(row.try_get::<i32, _>("state_version")?),
    })
}

fn event_notification(
    rule: &AlertRule,
    incident_id: uuid::Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i64,
    created_at: DateTime<Utc>,
) -> (String, String, String) {
    let dedupe_key = match kind {
        "opened" | "resolved" => format!("incident:{incident_id}:{kind}:{state_version}"),
        "reminder" => format!(
            "incident:{incident_id}:reminder:{state_version}:{}",
            created_at
                .timestamp()
                .div_euclid(rule.reminder_interval.num_seconds())
        ),
        _ => unreachable!("event notifications have a known kind"),
    };
    let severity = alert_severity_name(rule.severity);
    (
        dedupe_key,
        format!("[{severity}] {} {kind}", rule.name),
        format!(
            "Rule: {}\nDevice: {device_id}\nMetric: {}\nValue: {value:.3}\nThreshold: {:.3}\nState: {kind}\n",
            rule.name, rule.metric_key, rule.threshold
        ),
    )
}

async fn insert_sqlite_event_notification(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident_id: uuid::Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i64,
    created_at: DateTime<Utc>,
) -> Result<(), PlatformStoreError> {
    let (dedupe_key, subject, body) = event_notification(
        rule,
        incident_id,
        device_id,
        value,
        kind,
        state_version,
        created_at,
    );
    let created_at = created_at.to_rfc3339();
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(rule.tenant_id.to_string())
    .bind(incident_id.to_string())
    .bind(kind)
    .bind(dedupe_key)
    .bind(subject)
    .bind(body)
    .bind(&created_at)
    .bind(&created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_timescale_event_notification(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident_id: uuid::Uuid,
    device_id: &str,
    value: f64,
    kind: &str,
    state_version: i64,
    created_at: DateTime<Utc>,
) -> Result<(), PlatformStoreError> {
    let (dedupe_key, subject, body) = event_notification(
        rule,
        incident_id,
        device_id,
        value,
        kind,
        state_version,
        created_at,
    );
    sqlx::query(
        "INSERT INTO notification_outbox (
            id, tenant_id, incident_id, kind, dedupe_key, subject, body, created_at, next_attempt_at
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(rule.tenant_id)
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

async fn load_sqlite_active_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = ? AND rule_id = ? AND device_id = ? AND status IN ('pending', 'open')",
    )
    .bind(tenant_id.to_string())
    .bind(rule_id.to_string())
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(sqlite_event_incident)
    .transpose()
}

async fn load_timescale_active_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = $1 AND rule_id = $2 AND device_id = $3 AND status IN ('pending', 'open')
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(rule_id)
    .bind(device_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_event_incident)
    .transpose()
}

async fn load_sqlite_recent_resolved_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = ? AND rule_id = ? AND device_id = ?
           AND status = 'resolved' AND resolved_at >= ?
         ORDER BY resolved_at DESC
         LIMIT 1",
    )
    .bind(tenant_id.to_string())
    .bind(rule_id.to_string())
    .bind(device_id)
    .bind(reopen_after.to_rfc3339())
    .fetch_optional(&mut **transaction)
    .await?
    .map(sqlite_event_incident)
    .transpose()
}

async fn load_timescale_recent_resolved_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: uuid::Uuid,
    rule_id: uuid::Uuid,
    device_id: &str,
    reopen_after: DateTime<Utc>,
) -> Result<Option<EventIncident>, PlatformStoreError> {
    sqlx::query(
        "SELECT id, status, condition_started_at, recovery_started_at, acknowledged_at,
                last_reminder_at, state_version
         FROM alert_incidents
         WHERE tenant_id = $1 AND rule_id = $2 AND device_id = $3
           AND status = 'resolved' AND resolved_at >= $4
         ORDER BY resolved_at DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(tenant_id)
    .bind(rule_id)
    .bind(device_id)
    .bind(reopen_after)
    .fetch_optional(&mut **transaction)
    .await?
    .map(postgres_event_incident)
    .transpose()
}

async fn reopen_sqlite_event_incident(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    incident: EventIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    let at = evaluated_at.to_rfc3339();
    if rule.for_duration == ChronoDuration::zero() {
        let state_version = incident.state_version + 1;
        sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', condition_started_at = ?, recovery_started_at = NULL,
                 opened_at = ?, resolved_at = NULL, acknowledged_at = NULL,
                 acknowledged_by = NULL, last_value = ?, last_notified_at = ?,
                 last_reminder_at = ?, state_version = ?, updated_at = ?
             WHERE id = ? AND tenant_id = ?",
        )
        .bind(&at)
        .bind(&at)
        .bind(value)
        .bind(&at)
        .bind(&at)
        .bind(state_version)
        .bind(&at)
        .bind(incident.id.to_string())
        .bind(rule.tenant_id.to_string())
        .execute(&mut **transaction)
        .await?;
        insert_sqlite_event_notification(
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
        return Ok(EventTransition {
            opened: true,
            ..EventTransition::default()
        });
    }
    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'pending', condition_started_at = ?, recovery_started_at = NULL,
             opened_at = NULL, resolved_at = NULL, acknowledged_at = NULL,
             acknowledged_by = NULL, last_value = ?, updated_at = ?
         WHERE id = ? AND tenant_id = ?",
    )
    .bind(&at)
    .bind(value)
    .bind(&at)
    .bind(incident.id.to_string())
    .bind(rule.tenant_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(EventTransition::default())
}

async fn reopen_timescale_event_incident(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    incident: EventIncident,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    if rule.for_duration == ChronoDuration::zero() {
        let state_version = incident.state_version + 1;
        sqlx::query(
            "UPDATE alert_incidents
             SET status = 'open', condition_started_at = $2, recovery_started_at = NULL,
                 opened_at = $2, resolved_at = NULL, acknowledged_at = NULL,
                 acknowledged_by = NULL, last_value = $3, last_notified_at = $2,
                 last_reminder_at = $2, state_version = $4, updated_at = $2
             WHERE id = $1 AND tenant_id = $5",
        )
        .bind(incident.id)
        .bind(evaluated_at)
        .bind(value)
        .bind(state_version as i32)
        .bind(rule.tenant_id)
        .execute(&mut **transaction)
        .await?;
        insert_timescale_event_notification(
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
        return Ok(EventTransition {
            opened: true,
            ..EventTransition::default()
        });
    }
    sqlx::query(
        "UPDATE alert_incidents
         SET status = 'pending', condition_started_at = $2, recovery_started_at = NULL,
             opened_at = NULL, resolved_at = NULL, acknowledged_at = NULL,
             acknowledged_by = NULL, last_value = $3, updated_at = $2
         WHERE id = $1 AND tenant_id = $4",
    )
    .bind(incident.id)
    .bind(evaluated_at)
    .bind(value)
    .bind(rule.tenant_id)
    .execute(&mut **transaction)
    .await?;
    Ok(EventTransition::default())
}

async fn evaluate_sqlite_event_transition(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    let condition = event_condition(rule, value);
    let at = evaluated_at.to_rfc3339();
    if condition != Some(true) {
        if condition == Some(false) {
            if let Some(incident) =
                load_sqlite_active_event_incident(transaction, rule.tenant_id, rule.id, device_id)
                    .await?
            {
                match incident.status {
                    AlertIncidentStatus::Pending => {
                        sqlx::query("DELETE FROM alert_incidents WHERE id = ? AND tenant_id = ?")
                            .bind(incident.id.to_string())
                            .bind(rule.tenant_id.to_string())
                            .execute(&mut **transaction)
                            .await?;
                    }
                    AlertIncidentStatus::Open => {
                        let recovery_started_at =
                            incident.recovery_started_at.unwrap_or(evaluated_at);
                        if evaluated_at - recovery_started_at >= rule.resolve_after {
                            let state_version = incident.state_version + 1;
                            sqlx::query(
                                "UPDATE alert_incidents
                                 SET status = 'resolved', recovery_started_at = ?, resolved_at = ?,
                                     last_value = ?, last_notified_at = ?, state_version = ?,
                                     updated_at = ?
                                 WHERE id = ? AND tenant_id = ?",
                            )
                            .bind(&at)
                            .bind(&at)
                            .bind(value)
                            .bind(&at)
                            .bind(state_version)
                            .bind(&at)
                            .bind(incident.id.to_string())
                            .bind(rule.tenant_id.to_string())
                            .execute(&mut **transaction)
                            .await?;
                            insert_sqlite_event_notification(
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
                            return Ok(EventTransition {
                                resolved: true,
                                ..EventTransition::default()
                            });
                        }
                        sqlx::query(
                            "UPDATE alert_incidents
                             SET recovery_started_at = ?, last_value = ?, updated_at = ?
                             WHERE id = ? AND tenant_id = ?",
                        )
                        .bind(recovery_started_at.to_rfc3339())
                        .bind(value)
                        .bind(&at)
                        .bind(incident.id.to_string())
                        .bind(rule.tenant_id.to_string())
                        .execute(&mut **transaction)
                        .await?;
                    }
                    AlertIncidentStatus::Resolved => {}
                }
            }
        }
        return Ok(EventTransition::default());
    }
    let Some(incident) =
        load_sqlite_active_event_incident(transaction, rule.tenant_id, rule.id, device_id).await?
    else {
        if let Some(resolved) = load_sqlite_recent_resolved_event_incident(
            transaction,
            rule.tenant_id,
            rule.id,
            device_id,
            evaluated_at - rule.reopen_grace,
        )
        .await?
        {
            return reopen_sqlite_event_incident(
                transaction,
                rule,
                resolved,
                device_id,
                value,
                evaluated_at,
            )
            .await;
        }
        let id = uuid::Uuid::new_v4();
        if rule.for_duration == ChronoDuration::zero() {
            sqlx::query(
                "INSERT INTO alert_incidents (id, tenant_id, rule_id, device_id, status, condition_started_at,
                    opened_at, last_value, last_notified_at, last_reminder_at, state_version,
                    created_at, updated_at)
                 VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, 1, ?, ?)",
            )
            .bind(id.to_string())
            .bind(rule.tenant_id.to_string())
            .bind(rule.id.to_string())
            .bind(device_id)
            .bind(&at)
            .bind(&at)
            .bind(value)
            .bind(&at)
            .bind(&at)
            .bind(&at)
            .bind(&at)
            .execute(&mut **transaction)
            .await?;
            insert_sqlite_event_notification(
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
            return Ok(EventTransition {
                opened: true,
                ..EventTransition::default()
            });
        }
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, tenant_id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
        )
        .bind(id.to_string())
        .bind(rule.tenant_id.to_string())
        .bind(rule.id.to_string())
        .bind(device_id)
        .bind(&at)
        .bind(value)
        .bind(&at)
        .bind(&at)
        .execute(&mut **transaction)
        .await?;
        return Ok(EventTransition::default());
    };

    match incident.status {
        AlertIncidentStatus::Pending => {
            if evaluated_at - incident.condition_started_at >= rule.for_duration {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'open', recovery_started_at = NULL, opened_at = ?,
                         last_value = ?, last_notified_at = ?, last_reminder_at = ?,
                         state_version = ?, updated_at = ?
                     WHERE id = ? AND tenant_id = ?",
                )
                .bind(&at)
                .bind(value)
                .bind(&at)
                .bind(&at)
                .bind(state_version)
                .bind(&at)
                .bind(incident.id.to_string())
                .bind(rule.tenant_id.to_string())
                .execute(&mut **transaction)
                .await?;
                insert_sqlite_event_notification(
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
                return Ok(EventTransition {
                    opened: true,
                    ..EventTransition::default()
                });
            }
            sqlx::query("UPDATE alert_incidents SET last_value = ?, updated_at = ? WHERE id = ? AND tenant_id = ?")
                .bind(value)
                .bind(&at)
                .bind(incident.id.to_string())
                .bind(rule.tenant_id.to_string())
                .execute(&mut **transaction)
                .await?;
        }
        AlertIncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = ?, updated_at = ?
                 WHERE id = ? AND tenant_id = ?",
            )
            .bind(value)
            .bind(&at)
            .bind(incident.id.to_string())
            .bind(rule.tenant_id.to_string())
            .execute(&mut **transaction)
            .await?;
            let due = incident.acknowledged_at.is_none()
                && incident.last_reminder_at.is_none_or(|last_reminder_at| {
                    evaluated_at - last_reminder_at >= rule.reminder_interval
                });
            if due {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET last_reminder_at = ?, last_notified_at = ?, state_version = ?,
                         updated_at = ?
                     WHERE id = ? AND tenant_id = ?",
                )
                .bind(&at)
                .bind(&at)
                .bind(state_version)
                .bind(&at)
                .bind(incident.id.to_string())
                .bind(rule.tenant_id.to_string())
                .execute(&mut **transaction)
                .await?;
                insert_sqlite_event_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "reminder",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(EventTransition {
                    reminder: true,
                    ..EventTransition::default()
                });
            }
        }
        AlertIncidentStatus::Resolved => {}
    }
    Ok(EventTransition::default())
}

async fn evaluate_timescale_event_transition(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    device_id: &str,
    value: f64,
    evaluated_at: DateTime<Utc>,
) -> Result<EventTransition, PlatformStoreError> {
    let condition = event_condition(rule, value);
    if condition != Some(true) {
        if condition == Some(false) {
            if let Some(incident) = load_timescale_active_event_incident(
                transaction,
                rule.tenant_id,
                rule.id,
                device_id,
            )
            .await?
            {
                match incident.status {
                    AlertIncidentStatus::Pending => {
                        sqlx::query("DELETE FROM alert_incidents WHERE id = $1 AND tenant_id = $2")
                            .bind(incident.id)
                            .bind(rule.tenant_id)
                            .execute(&mut **transaction)
                            .await?;
                    }
                    AlertIncidentStatus::Open => {
                        let recovery_started_at =
                            incident.recovery_started_at.unwrap_or(evaluated_at);
                        if evaluated_at - recovery_started_at >= rule.resolve_after {
                            let state_version = incident.state_version + 1;
                            sqlx::query(
                                "UPDATE alert_incidents
                                 SET status = 'resolved', recovery_started_at = $2, resolved_at = $2,
                                     last_value = $3, last_notified_at = $2, state_version = $4,
                                     updated_at = $2
                                 WHERE id = $1 AND tenant_id = $5",
                            )
                            .bind(incident.id)
                            .bind(evaluated_at)
                            .bind(value)
                            .bind(state_version as i32)
                            .bind(rule.tenant_id)
                            .execute(&mut **transaction)
                            .await?;
                            insert_timescale_event_notification(
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
                            return Ok(EventTransition {
                                resolved: true,
                                ..EventTransition::default()
                            });
                        }
                        sqlx::query(
                            "UPDATE alert_incidents
                             SET recovery_started_at = $2, last_value = $3, updated_at = $4
                             WHERE id = $1 AND tenant_id = $5",
                        )
                        .bind(incident.id)
                        .bind(recovery_started_at)
                        .bind(value)
                        .bind(evaluated_at)
                        .bind(rule.tenant_id)
                        .execute(&mut **transaction)
                        .await?;
                    }
                    AlertIncidentStatus::Resolved => {}
                }
            }
        }
        return Ok(EventTransition::default());
    }
    let Some(incident) =
        load_timescale_active_event_incident(transaction, rule.tenant_id, rule.id, device_id)
            .await?
    else {
        if let Some(resolved) = load_timescale_recent_resolved_event_incident(
            transaction,
            rule.tenant_id,
            rule.id,
            device_id,
            evaluated_at - rule.reopen_grace,
        )
        .await?
        {
            return reopen_timescale_event_incident(
                transaction,
                rule,
                resolved,
                device_id,
                value,
                evaluated_at,
            )
            .await;
        }
        let id = uuid::Uuid::new_v4();
        if rule.for_duration == ChronoDuration::zero() {
            sqlx::query(
                "INSERT INTO alert_incidents (id, tenant_id, rule_id, device_id, status, condition_started_at,
                    opened_at, last_value, last_notified_at, last_reminder_at, state_version,
                    created_at, updated_at)
                 VALUES ($1, $2, $3, $4, 'open', $5, $5, $6, $5, $5, 1, $5, $5)",
            )
            .bind(id)
            .bind(rule.tenant_id)
            .bind(rule.id)
            .bind(device_id)
            .bind(evaluated_at)
            .bind(value)
            .execute(&mut **transaction)
            .await?;
            insert_timescale_event_notification(
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
            return Ok(EventTransition {
                opened: true,
                ..EventTransition::default()
            });
        }
        sqlx::query(
            "INSERT INTO alert_incidents (
                id, tenant_id, rule_id, device_id, status, condition_started_at, last_value, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, 'pending', $5, $6, $5, $5)",
        )
        .bind(id)
        .bind(rule.tenant_id)
        .bind(rule.id)
        .bind(device_id)
        .bind(evaluated_at)
        .bind(value)
        .execute(&mut **transaction)
        .await?;
        return Ok(EventTransition::default());
    };

    match incident.status {
        AlertIncidentStatus::Pending => {
            if evaluated_at - incident.condition_started_at >= rule.for_duration {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET status = 'open', recovery_started_at = NULL, opened_at = $2,
                         last_value = $3, last_notified_at = $2, last_reminder_at = $2,
                         state_version = $4, updated_at = $2
                     WHERE id = $1 AND tenant_id = $5",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(value)
                .bind(state_version as i32)
                .bind(rule.tenant_id)
                .execute(&mut **transaction)
                .await?;
                insert_timescale_event_notification(
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
                return Ok(EventTransition {
                    opened: true,
                    ..EventTransition::default()
                });
            }
            sqlx::query(
                "UPDATE alert_incidents SET last_value = $2, updated_at = $3
                 WHERE id = $1 AND tenant_id = $4",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .bind(rule.tenant_id)
            .execute(&mut **transaction)
            .await?;
        }
        AlertIncidentStatus::Open => {
            sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = NULL, last_value = $2, updated_at = $3
                 WHERE id = $1 AND tenant_id = $4",
            )
            .bind(incident.id)
            .bind(value)
            .bind(evaluated_at)
            .bind(rule.tenant_id)
            .execute(&mut **transaction)
            .await?;
            let due = incident.acknowledged_at.is_none()
                && incident.last_reminder_at.is_none_or(|last_reminder_at| {
                    evaluated_at - last_reminder_at >= rule.reminder_interval
                });
            if due {
                let state_version = incident.state_version + 1;
                sqlx::query(
                    "UPDATE alert_incidents
                     SET last_reminder_at = $2, last_notified_at = $2, state_version = $3,
                         updated_at = $2
                     WHERE id = $1 AND tenant_id = $4",
                )
                .bind(incident.id)
                .bind(evaluated_at)
                .bind(state_version as i32)
                .bind(rule.tenant_id)
                .execute(&mut **transaction)
                .await?;
                insert_timescale_event_notification(
                    transaction,
                    rule,
                    incident.id,
                    device_id,
                    value,
                    "reminder",
                    state_version,
                    evaluated_at,
                )
                .await?;
                return Ok(EventTransition {
                    reminder: true,
                    ..EventTransition::default()
                });
            }
        }
        AlertIncidentStatus::Resolved => {}
    }
    Ok(EventTransition::default())
}

async fn evaluate_sqlite_alert_events(
    store: &SqliteStore,
    events: &[AlertEvaluationEvent],
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = store.pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled = 1 AND archived_at IS NULL
           AND rule_type = 'event_threshold' ORDER BY created_at, id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(sqlite_alert_rule_record)
        .collect::<Result<_, _>>()?;
    let mut result = AlertEvaluationResult::default();
    for event in events {
        for rule in &rules {
            if rule.tenant_id != event.tenant_id
                || rule
                    .device_id
                    .as_deref()
                    .is_some_and(|id| id != event.device_id)
            {
                continue;
            }
            let Some(value) = event
                .measurements
                .get(&rule.metric_key)
                .and_then(serde_json::Value::as_f64)
                .filter(|value| value.is_finite())
            else {
                continue;
            };
            let claim = sqlx::query(
                "INSERT INTO alert_rule_event_evaluations
                 (tenant_id, rule_id, event_at, device_id, boot_id, sequence)
                 VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT (tenant_id, rule_id, event_at, device_id, boot_id, sequence) DO NOTHING",
            )
            .bind(rule.tenant_id.to_string())
            .bind(rule.id.to_string())
            .bind(canonical_postgres_timestamp(event.event_at).to_rfc3339())
            .bind(&event.device_id)
            .bind(event.boot_id.to_string())
            .bind(event.sequence.to_string())
            .execute(&mut *transaction)
            .await?;
            if claim.rows_affected() == 0 {
                continue;
            }
            result.evaluated += 1;
            let transition = evaluate_sqlite_event_transition(
                &mut transaction,
                rule,
                &event.device_id,
                value,
                canonical_postgres_timestamp(event.received_at),
            )
            .await?;
            result.opened += usize::from(transition.opened);
            result.resolved += usize::from(transition.resolved);
            result.reminders += usize::from(transition.reminder);
        }
    }
    transaction.commit().await?;
    Ok(result)
}

async fn evaluate_timescale_alert_events(
    pool: &PgPool,
    events: &[AlertEvaluationEvent],
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled AND archived_at IS NULL
           AND rule_type = 'event_threshold' ORDER BY created_at, id FOR SHARE",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(postgres_alert_rule_record)
        .collect::<Result<_, _>>()?;

    // Lock every affected incident key in a stable order before any transition.
    // This prevents opposite event-batch orders from forming an advisory-lock cycle.
    let mut lock_keys = Vec::new();
    for event in events {
        for rule in &rules {
            if rule.tenant_id != event.tenant_id
                || rule
                    .device_id
                    .as_deref()
                    .is_some_and(|id| id != event.device_id)
            {
                continue;
            }
            if event
                .measurements
                .get(&rule.metric_key)
                .and_then(serde_json::Value::as_f64)
                .is_some_and(f64::is_finite)
            {
                lock_keys.push((rule.tenant_id, rule.id, event.device_id.clone()));
            }
        }
    }
    lock_keys.sort_unstable();
    lock_keys.dedup();
    for (tenant_id, rule_id, device_id) in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "iot_nano:alert-event:{tenant_id}:{rule_id}:{device_id}"
            ))
            .execute(&mut *transaction)
            .await?;
    }

    let mut result = AlertEvaluationResult::default();
    for event in events {
        for rule in &rules {
            if rule.tenant_id != event.tenant_id
                || rule
                    .device_id
                    .as_deref()
                    .is_some_and(|id| id != event.device_id)
            {
                continue;
            }
            let Some(value) = event
                .measurements
                .get(&rule.metric_key)
                .and_then(serde_json::Value::as_f64)
                .filter(|value| value.is_finite())
            else {
                continue;
            };
            let sequence = i64::try_from(event.sequence)
                .map_err(|_| PlatformStoreError::AlertRuleSequenceOverflow)?;
            let claim = sqlx::query("INSERT INTO alert_rule_event_evaluations (tenant_id, rule_id, event_at, device_id, boot_id, sequence) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (tenant_id, rule_id, event_at, device_id, boot_id, sequence) DO NOTHING")
                .bind(rule.tenant_id).bind(rule.id).bind(canonical_postgres_timestamp(event.event_at)).bind(&event.device_id).bind(event.boot_id).bind(sequence).execute(&mut *transaction).await?;
            if claim.rows_affected() == 0 {
                continue;
            }
            result.evaluated += 1;
            let transition = evaluate_timescale_event_transition(
                &mut transaction,
                rule,
                &event.device_id,
                value,
                canonical_postgres_timestamp(event.received_at),
            )
            .await?;
            result.opened += usize::from(transition.opened);
            result.resolved += usize::from(transition.resolved);
            result.reminders += usize::from(transition.reminder);
        }
    }
    transaction.commit().await?;
    Ok(result)
}

async fn sqlite_window_aggregates(
    transaction: &mut Transaction<'_, Sqlite>,
    rule: &AlertRule,
    evaluated_at: DateTime<Utc>,
) -> Result<Vec<(String, f64)>, PlatformStoreError> {
    let Some(window) = rule.window else {
        return Ok(Vec::new());
    };
    let from = evaluated_at - window;
    let path = format!("$.{}", rule.metric_key);
    let rows = sqlx::query(
        "WITH canonical_telemetry AS (
            SELECT tenant_id, device_id, measurements,
                   CAST(unixepoch(event_at) AS INTEGER) * 1000000
                   + CASE
                       WHEN instr(event_at, '.') = 0 THEN 0
                       ELSE CAST(
                           substr(
                               substr(
                                   event_at,
                                   instr(event_at, '.') + 1,
                                   CASE
                                       WHEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           'Z'
                                       ) > 0
                                       THEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           'Z'
                                       ) - 1
                                       WHEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           '+'
                                       ) > 0
                                       THEN instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           '+'
                                       ) - 1
                                       ELSE instr(
                                           substr(event_at, instr(event_at, '.') + 1),
                                           '-'
                                       ) - 1
                                   END
                               ) || '000000',
                               1,
                               6
                           ) AS INTEGER
                       )
                   END AS event_at_micros
            FROM telemetry
         ),
         finite_telemetry AS (
             SELECT device_id, json_extract(measurements, ?) AS finite_value
             FROM canonical_telemetry
             WHERE tenant_id = ?
               AND event_at_micros >= ?
               AND event_at_micros <= ?
               AND (? IS NULL OR device_id = ?)
               AND json_type(measurements, ?) IN ('integer', 'real')
               AND json_extract(measurements, ?) > -1.0e999
               AND json_extract(measurements, ?) < 1.0e999
         ),
         device_scales AS (
             SELECT device_id, MAX(ABS(1.0 * finite_value)) AS scale
             FROM finite_telemetry
             GROUP BY device_id
         )
         SELECT finite_telemetry.device_id,
                CASE
                    WHEN device_scales.scale = 0.0 THEN 0.0
                    ELSE AVG(
                        1.0 * finite_telemetry.finite_value / NULLIF(device_scales.scale, 0.0)
                    ) * device_scales.scale
                END AS average
         FROM finite_telemetry
         JOIN device_scales ON device_scales.device_id = finite_telemetry.device_id
         GROUP BY finite_telemetry.device_id, device_scales.scale
         ORDER BY finite_telemetry.device_id",
    )
    .bind(&path)
    .bind(rule.tenant_id.to_string())
    .bind(from.timestamp_micros())
    .bind(evaluated_at.timestamp_micros())
    .bind(rule.device_id.as_deref())
    .bind(rule.device_id.as_deref())
    .bind(&path)
    .bind(&path)
    .bind(&path)
    .fetch_all(&mut **transaction)
    .await?;
    let mut aggregates = Vec::with_capacity(rows.len());
    for row in rows {
        let average: f64 = row.try_get("average")?;
        if average.is_finite() {
            aggregates.push((row.try_get("device_id")?, average));
        }
    }
    Ok(aggregates)
}

async fn timescale_window_aggregates(
    transaction: &mut Transaction<'_, Postgres>,
    rule: &AlertRule,
    evaluated_at: DateTime<Utc>,
) -> Result<Vec<(String, f64)>, PlatformStoreError> {
    let Some(window) = rule.window else {
        return Ok(Vec::new());
    };
    let from = evaluated_at - window;
    let rows = sqlx::query(
        "WITH finite_telemetry AS (
             SELECT device_id,
                    CASE
                        WHEN jsonb_typeof(measurements -> $1) = 'number'
                        THEN CASE
                            WHEN (measurements ->> $1)::numeric BETWEEN
                                     '-1.7976931348623157e308'::numeric
                                 AND '1.7976931348623157e308'::numeric
                            THEN (measurements ->> $1)::numeric
                        END
                    END AS finite_value
             FROM telemetry
             WHERE tenant_id = $2
               AND event_at >= $3
               AND event_at <= $4
               AND ($5::text IS NULL OR device_id = $5)
         )
         SELECT device_id, (AVG(finite_value))::double precision AS average
         FROM finite_telemetry
         GROUP BY device_id
         HAVING COUNT(finite_value) > 0
         ORDER BY device_id",
    )
    .bind(&rule.metric_key)
    .bind(rule.tenant_id)
    .bind(from)
    .bind(evaluated_at)
    .bind(rule.device_id.as_deref())
    .fetch_all(&mut **transaction)
    .await?;
    let mut aggregates = Vec::with_capacity(rows.len());
    for row in rows {
        let average: f64 = row.try_get("average")?;
        if average.is_finite() {
            aggregates.push((row.try_get("device_id")?, average));
        }
    }
    Ok(aggregates)
}

async fn evaluate_sqlite_alert_windows(
    store: &SqliteStore,
    evaluated_at: DateTime<Utc>,
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = store.pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled = 1 AND archived_at IS NULL
           AND rule_type = 'window_average' ORDER BY created_at, id",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(sqlite_alert_rule_record)
        .collect::<Result<_, _>>()?;
    let mut evaluations = Vec::new();
    for rule in &rules {
        evaluations.extend(
            sqlite_window_aggregates(&mut transaction, rule, evaluated_at)
                .await?
                .into_iter()
                .map(|(device_id, value)| (rule.clone(), device_id, value)),
        );
    }

    let mut result = AlertEvaluationResult::default();
    for (rule, device_id, value) in evaluations {
        result.evaluated += 1;
        let transition = evaluate_sqlite_event_transition(
            &mut transaction,
            &rule,
            &device_id,
            value,
            evaluated_at,
        )
        .await?;
        result.opened += usize::from(transition.opened);
        result.resolved += usize::from(transition.resolved);
        result.reminders += usize::from(transition.reminder);
    }
    transaction.commit().await?;
    Ok(result)
}

async fn evaluate_timescale_alert_windows(
    pool: &PgPool,
    evaluated_at: DateTime<Utc>,
) -> Result<AlertEvaluationResult, PlatformStoreError> {
    let mut transaction = pool.begin().await?;
    let rows = sqlx::query(
        "SELECT id, tenant_id, name, enabled, device_id, metric_key, rule_type, comparison, threshold,
                window_seconds, for_seconds, resolve_after_seconds, reopen_grace_seconds,
                hysteresis, severity, reminder_interval_seconds
         FROM alert_rules WHERE enabled AND archived_at IS NULL
           AND rule_type = 'window_average' ORDER BY created_at, id FOR SHARE",
    )
    .fetch_all(&mut *transaction)
    .await?;
    let rules: Vec<_> = rows
        .into_iter()
        .map(postgres_alert_rule_record)
        .collect::<Result<_, _>>()?;
    let mut evaluations = Vec::new();
    for rule in &rules {
        evaluations.extend(
            timescale_window_aggregates(&mut transaction, rule, evaluated_at)
                .await?
                .into_iter()
                .map(|(device_id, value)| (rule.clone(), device_id, value)),
        );
    }

    // Lock every affected incident key in a stable order before any transition.
    // This prevents opposite window-batch orders from forming an advisory-lock cycle.
    let mut lock_keys: Vec<_> = evaluations
        .iter()
        .map(|(rule, device_id, _)| (rule.tenant_id, rule.id, device_id.clone()))
        .collect();
    lock_keys.sort_unstable();
    lock_keys.dedup();
    for (tenant_id, rule_id, device_id) in lock_keys {
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!(
                "iot_nano:alert-window:{tenant_id}:{rule_id}:{device_id}"
            ))
            .execute(&mut *transaction)
            .await?;
    }

    let mut result = AlertEvaluationResult::default();
    for (rule, device_id, value) in evaluations {
        result.evaluated += 1;
        let transition = evaluate_timescale_event_transition(
            &mut transaction,
            &rule,
            &device_id,
            value,
            evaluated_at,
        )
        .await?;
        result.opened += usize::from(transition.opened);
        result.resolved += usize::from(transition.resolved);
        result.reminders += usize::from(transition.reminder);
    }
    transaction.commit().await?;
    Ok(result)
}

fn canonical_incident(mut incident: NewAlertIncident) -> NewAlertIncident {
    incident.condition_started_at = canonical_postgres_timestamp(incident.condition_started_at);
    incident
}

enum AlertIncidentTransition {
    Open(DateTime<Utc>),
    Recover(DateTime<Utc>),
    Resolve(DateTime<Utc>),
    Remind(DateTime<Utc>),
}

impl AlertIncidentTransition {
    fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Open(timestamp)
            | Self::Recover(timestamp)
            | Self::Resolve(timestamp)
            | Self::Remind(timestamp) => *timestamp,
        }
    }
}

impl AlertEvaluationRepository for PlatformStore {
    fn evaluate_alert_events<'a>(
        &'a self,
        events: &'a [AlertEvaluationEvent],
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { self.evaluate_alert_events(events, evaluated_at).await })
    }

    fn evaluate_alert_windows<'a>(
        &'a self,
        evaluated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<AlertEvaluationResult, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { self.evaluate_alert_windows(evaluated_at).await })
    }
}

impl AlertIncidentRepository for PlatformStore {
    fn create_incident<'a>(
        &'a self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { self.create_incident(incident, opened_notification).await })
    }

    fn update_incident_last_value<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.update_incident_last_value(
                tenant_id,
                incident_id,
                expected_version,
                last_value,
                updated_at,
            )
            .await
        })
    }

    fn open_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.open_incident(tenant_id, incident_id, expected_version, opened_at)
                .await
        })
    }

    fn open_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        opened_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.open_incident_with_notification(
                tenant_id,
                incident_id,
                expected_version,
                opened_at,
                notification,
            )
            .await
        })
    }

    fn recover_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        recovery_started_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.recover_incident(
                tenant_id,
                incident_id,
                expected_version,
                recovery_started_at,
            )
            .await
        })
    }

    fn resolve_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.resolve_incident(tenant_id, incident_id, expected_version, resolved_at)
                .await
        })
    }

    fn resolve_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        resolved_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.resolve_incident_with_notification(
                tenant_id,
                incident_id,
                expected_version,
                resolved_at,
                notification,
            )
            .await
        })
    }

    fn remind_incident<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.remind_incident(tenant_id, incident_id, expected_version, reminded_at)
                .await
        })
    }

    fn remind_incident_with_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        expected_version: i64,
        reminded_at: DateTime<Utc>,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<Box<dyn Future<Output = Result<Option<AlertIncident>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.remind_incident_with_notification(
                tenant_id,
                incident_id,
                expected_version,
                reminded_at,
                notification,
            )
            .await
        })
    }

    fn enqueue_notification<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
        incident_id: uuid::Uuid,
        notification: NewNotificationOutboxEntry,
    ) -> Pin<
        Box<dyn Future<Output = Result<NotificationOutboxRecord, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move {
            self.enqueue_notification(tenant_id, incident_id, notification)
                .await
        })
    }
}

fn sqlite_alert_rule_record(row: SqliteRow) -> Result<AlertRule, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let rule = AlertRule {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidAlertRuleId(id))?,
        tenant_id: uuid::Uuid::parse_str(&tenant_id)
            .map_err(|_| PlatformStoreError::InvalidAlertRuleTenantId(tenant_id))?,
        name: row.try_get("name")?,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
        kind: alert_rule_kind(&row.try_get::<String, _>("rule_type")?)?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        comparison: alert_comparison(&row.try_get::<String, _>("comparison")?)?,
        threshold: row.try_get("threshold")?,
        window: row
            .try_get::<Option<i64>, _>("window_seconds")?
            .map(alert_duration)
            .transpose()?,
        for_duration: alert_duration(row.try_get("for_seconds")?)?,
        resolve_after: alert_duration(row.try_get("resolve_after_seconds")?)?,
        reopen_grace: alert_duration(row.try_get("reopen_grace_seconds")?)?,
        hysteresis: row.try_get("hysteresis")?,
        severity: alert_severity(&row.try_get::<String, _>("severity")?)?,
        reminder_interval: alert_positive_duration(
            row.try_get("reminder_interval_seconds")?,
            "reminder_interval_seconds",
        )?,
    };
    validate_alert_rule(&rule)
}

fn postgres_alert_rule_record(row: PgRow) -> Result<AlertRule, PlatformStoreError> {
    let rule = AlertRule {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        name: row.try_get("name")?,
        enabled: row.try_get("enabled")?,
        kind: alert_rule_kind(&row.try_get::<String, _>("rule_type")?)?,
        device_id: row.try_get("device_id")?,
        metric_key: row.try_get("metric_key")?,
        comparison: alert_comparison(&row.try_get::<String, _>("comparison")?)?,
        threshold: row.try_get("threshold")?,
        window: row
            .try_get::<Option<i32>, _>("window_seconds")?
            .map(i64::from)
            .map(alert_duration)
            .transpose()?,
        for_duration: alert_duration(i64::from(row.try_get::<i32, _>("for_seconds")?))?,
        resolve_after: alert_duration(i64::from(row.try_get::<i32, _>("resolve_after_seconds")?))?,
        reopen_grace: alert_duration(i64::from(row.try_get::<i32, _>("reopen_grace_seconds")?))?,
        hysteresis: row.try_get("hysteresis")?,
        severity: alert_severity(&row.try_get::<String, _>("severity")?)?,
        reminder_interval: alert_positive_duration(
            i64::from(row.try_get::<i32, _>("reminder_interval_seconds")?),
            "reminder_interval_seconds",
        )?,
    };
    validate_alert_rule(&rule)
}

fn alert_rule_kind(value: &str) -> Result<AlertRuleKind, PlatformStoreError> {
    match value {
        "event_threshold" => Ok(AlertRuleKind::EventThreshold),
        "window_average" => Ok(AlertRuleKind::WindowAverage),
        _ => Err(PlatformStoreError::InvalidAlertRuleKind(value.to_owned())),
    }
}

fn alert_comparison(value: &str) -> Result<AlertComparison, PlatformStoreError> {
    match value {
        "gt" => Ok(AlertComparison::GreaterThan),
        "gte" => Ok(AlertComparison::GreaterThanOrEqual),
        "lt" => Ok(AlertComparison::LessThan),
        "lte" => Ok(AlertComparison::LessThanOrEqual),
        _ => Err(PlatformStoreError::InvalidAlertRuleComparison(
            value.to_owned(),
        )),
    }
}

fn alert_severity(value: &str) -> Result<AlertSeverity, PlatformStoreError> {
    match value {
        "info" => Ok(AlertSeverity::Info),
        "warning" => Ok(AlertSeverity::Warning),
        "critical" => Ok(AlertSeverity::Critical),
        _ => Err(PlatformStoreError::InvalidAlertRuleSeverity(
            value.to_owned(),
        )),
    }
}

fn alert_duration(seconds: i64) -> Result<ChronoDuration, PlatformStoreError> {
    if seconds < 0 {
        return Err(PlatformStoreError::InvalidAlertRuleDuration {
            field: "duration",
            seconds,
        });
    }
    Ok(ChronoDuration::seconds(seconds))
}

fn alert_positive_duration(
    seconds: i64,
    field: &'static str,
) -> Result<ChronoDuration, PlatformStoreError> {
    if seconds <= 0 {
        return Err(PlatformStoreError::InvalidAlertRuleDuration { field, seconds });
    }
    Ok(ChronoDuration::seconds(seconds))
}

fn validate_alert_rule(rule: &AlertRule) -> Result<AlertRule, PlatformStoreError> {
    match (rule.kind, rule.window) {
        (AlertRuleKind::EventThreshold, Some(window)) => {
            return Err(PlatformStoreError::InvalidAlertRuleDuration {
                field: "window_seconds",
                seconds: window.num_seconds(),
            });
        }
        (AlertRuleKind::WindowAverage, None) => {
            return Err(PlatformStoreError::InvalidAlertRuleDuration {
                field: "window_seconds",
                seconds: 0,
            });
        }
        (AlertRuleKind::WindowAverage, Some(window)) if window < ChronoDuration::seconds(60) => {
            return Err(PlatformStoreError::InvalidAlertRuleDuration {
                field: "window_seconds",
                seconds: window.num_seconds(),
            });
        }
        _ => {}
    }
    if rule.threshold.is_nan() || rule.threshold.is_infinite() {
        return Err(PlatformStoreError::InvalidAlertRuleDuration {
            field: "threshold",
            seconds: 0,
        });
    }
    if rule
        .hysteresis
        .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(PlatformStoreError::InvalidAlertRuleDuration {
            field: "hysteresis",
            seconds: 0,
        });
    }
    Ok(rule.clone())
}

impl SqliteStore {
    async fn create_incident(
        &self,
        incident: NewAlertIncident,
        opened_notification: Option<NewNotificationOutboxEntry>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let mut transaction = self.pool.begin().await?;
        let inserted = sqlx::query(
            "INSERT INTO alert_incidents (
                id, tenant_id, rule_id, device_id, status, condition_started_at, opened_at, last_value
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT DO NOTHING",
        )
        .bind(incident.id.to_string())
        .bind(incident.tenant_id.to_string())
        .bind(incident.rule_id.to_string())
        .bind(&incident.device_id)
        .bind(incident.status.as_str())
        .bind(incident.condition_started_at.to_rfc3339())
        .bind(
            (incident.status == AlertIncidentStatus::Open)
                .then(|| incident.condition_started_at.to_rfc3339()),
        )
        .bind(incident.last_value)
        .execute(&mut *transaction)
        .await?;
        if inserted.rows_affected() == 0 {
            return Ok(None);
        }
        if let Some(notification) = opened_notification {
            sqlx::query(
                "INSERT INTO notification_outbox (
                    id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(notification.id.to_string())
            .bind(incident.tenant_id.to_string())
            .bind(incident.id.to_string())
            .bind(notification.kind.as_str())
            .bind(notification.dedupe_key)
            .bind(notification.subject)
            .bind(notification.body)
            .bind(notification.next_attempt_at.to_rfc3339())
            .execute(&mut *transaction)
            .await?;
        }
        let row = sqlx::query(
            "SELECT id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                    opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                    state_version
             FROM alert_incidents WHERE id = ? AND tenant_id = ?",
        )
        .bind(incident.id.to_string())
        .bind(incident.tenant_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
        transaction.commit().await?;
        sqlite_alert_incident_record(row).map(Some)
    }

    async fn update_incident_last_value(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: &str,
        expected_version: i64,
        last_value: Option<f64>,
        updated_at: DateTime<Utc>,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let row = sqlx::query(
            "UPDATE alert_incidents
             SET last_value = ?, state_version = state_version + 1, updated_at = ?
             WHERE id = ? AND tenant_id = ? AND state_version = ? AND status IN ('pending', 'open')
             RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                       opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                       state_version",
        )
        .bind(last_value)
        .bind(updated_at.to_rfc3339())
        .bind(incident_id)
        .bind(tenant_id.to_string())
        .bind(expected_version)
        .fetch_optional(&self.pool)
        .await?;
        row.map(sqlite_alert_incident_record).transpose()
    }

    async fn update_incident_transition(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: &str,
        expected_version: i64,
        transition: AlertIncidentTransition,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let timestamp = transition.timestamp().to_rfc3339();
        let query = match transition {
            AlertIncidentTransition::Open(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'open', opened_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'pending'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Recover(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET recovery_started_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                       AND recovery_started_at IS NULL
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Resolve(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'resolved', resolved_at = ?,
                     recovery_started_at = COALESCE(recovery_started_at, ?), updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Remind(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET last_reminder_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
        };
        let row = query
            .bind(incident_id)
            .bind(tenant_id.to_string())
            .bind(expected_version)
            .fetch_optional(&self.pool)
            .await?;
        row.map(sqlite_alert_incident_record).transpose()
    }

    async fn update_incident_transition_with_notification(
        &self,
        tenant_id: uuid::Uuid,
        incident_id: &str,
        expected_version: i64,
        transition: AlertIncidentTransition,
        notification: NewNotificationOutboxEntry,
    ) -> Result<Option<AlertIncident>, PlatformStoreError> {
        let timestamp = transition.timestamp().to_rfc3339();
        let mut transaction = self.pool.begin().await?;
        let query = match transition {
            AlertIncidentTransition::Open(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'open', opened_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'pending'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Resolve(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET status = 'resolved', resolved_at = ?,
                     recovery_started_at = COALESCE(recovery_started_at, ?), updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Remind(_) => sqlx::query(
                "UPDATE alert_incidents
                 SET last_reminder_at = ?, updated_at = ?,
                     state_version = state_version + 1
                 WHERE id = ? AND tenant_id = ? AND state_version = ? AND status = 'open'
                 RETURNING id, tenant_id, rule_id, device_id, status, condition_started_at, recovery_started_at,
                           opened_at, resolved_at, last_value, last_notified_at, last_reminder_at,
                           state_version",
            )
            .bind(&timestamp)
            .bind(&timestamp),
            AlertIncidentTransition::Recover(_) => unreachable!(),
        };
        let row = query
            .bind(incident_id)
            .bind(tenant_id.to_string())
            .bind(expected_version)
            .fetch_optional(&mut *transaction)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        sqlx::query(
            "INSERT INTO notification_outbox (
                id, tenant_id, incident_id, kind, dedupe_key, subject, body, next_attempt_at
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(notification.id.to_string())
        .bind(tenant_id.to_string())
        .bind(incident_id)
        .bind(notification.kind.as_str())
        .bind(notification.dedupe_key)
        .bind(notification.subject)
        .bind(notification.body)
        .bind(notification.next_attempt_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        sqlite_alert_incident_record(row).map(Some)
    }
}

fn sqlite_alert_incident_record(row: SqliteRow) -> Result<AlertIncident, PlatformStoreError> {
    let id: String = row.try_get("id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let rule_id: String = row.try_get("rule_id")?;
    Ok(AlertIncident {
        id: uuid::Uuid::parse_str(&id).map_err(|_| PlatformStoreError::InvalidIncidentId(id))?,
        tenant_id: uuid::Uuid::parse_str(&tenant_id)
            .map_err(|_| PlatformStoreError::InvalidIncidentTenantId(tenant_id))?,
        rule_id: uuid::Uuid::parse_str(&rule_id)
            .map_err(|_| PlatformStoreError::InvalidIncidentId(rule_id))?,
        device_id: row.try_get("device_id")?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: incident_timestamp(&row, "condition_started_at")?,
        recovery_started_at: incident_optional_timestamp(&row, "recovery_started_at")?,
        opened_at: incident_optional_timestamp(&row, "opened_at")?,
        resolved_at: incident_optional_timestamp(&row, "resolved_at")?,
        last_value: row.try_get("last_value")?,
        last_notified_at: incident_optional_timestamp(&row, "last_notified_at")?,
        last_reminder_at: incident_optional_timestamp(&row, "last_reminder_at")?,
        state_version: row.try_get("state_version")?,
    })
}

fn postgres_alert_incident_record(row: PgRow) -> Result<AlertIncident, PlatformStoreError> {
    Ok(AlertIncident {
        id: row.try_get("id")?,
        tenant_id: row.try_get("tenant_id")?,
        rule_id: row.try_get("rule_id")?,
        device_id: row.try_get("device_id")?,
        status: AlertIncidentStatus::from_database(&row.try_get::<String, _>("status")?)?,
        condition_started_at: row.try_get("condition_started_at")?,
        recovery_started_at: row.try_get("recovery_started_at")?,
        opened_at: row.try_get("opened_at")?,
        resolved_at: row.try_get("resolved_at")?,
        last_value: row.try_get("last_value")?,
        last_notified_at: row.try_get("last_notified_at")?,
        last_reminder_at: row.try_get("last_reminder_at")?,
        state_version: i64::from(row.try_get::<i32, _>("state_version")?),
    })
}

fn incident_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    let value: String = row.try_get(column)?;
    parse_incident_timestamp(value, column)
}

fn incident_optional_timestamp(
    row: &SqliteRow,
    column: &'static str,
) -> Result<Option<DateTime<Utc>>, PlatformStoreError> {
    row.try_get::<Option<String>, _>(column)?
        .map(|value| parse_incident_timestamp(value, column))
        .transpose()
}

fn parse_incident_timestamp(
    value: String,
    column: &'static str,
) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|timestamp| timestamp.and_utc())
        })
        .map_err(|source| PlatformStoreError::InvalidIncidentTimestamp {
            column,
            value,
            source,
        })
}
