use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

const SMTP_KEYS: &[&str] = &[
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USERNAME",
    "SMTP_PASSWORD",
    "ALERT_EMAIL_FROM",
    "ALERT_EMAIL_TO",
    "SMTP_TIMEOUT_SECONDS",
];
const MQTT_KEYS: &[&str] = &["MQTT_BROKER_HOST", "MQTT_BROKER_PORT"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SystemConfiguration {
    pub smtp: SmtpConfiguration,
    pub mqtt: MqttConfiguration,
    pub tuning: IngestTuning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SmtpConfiguration {
    pub enabled: bool,
    pub host: Option<String>,
    pub port: u16,
    pub username: Option<String>,
    pub password_configured: bool,
    pub from: Option<String>,
    pub to: Option<String>,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SmtpConfigurationUpdate {
    pub enabled: bool,
    pub host: Option<String>,
    pub port: u16,
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MqttConfiguration {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct MqttConfigurationUpdate {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SystemConfigurationUpdate {
    pub smtp: SmtpConfigurationUpdate,
    #[serde(default)]
    pub mqtt: Option<MqttConfigurationUpdate>,
    pub tuning: IngestTuning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct IngestTuning {
    pub retention_bytes: u64,
    pub retention_seconds: u64,
    pub segment_bytes: u64,
    pub max_record_bytes: u64,
    pub writer_batch_size: u64,
    pub alert_batch_size: u64,
    pub notification_batch_size: u64,
    pub writer_flush_seconds: u64,
    pub alert_event_interval_milliseconds: u64,
    pub alert_window_interval_seconds: u64,
    pub notification_interval_seconds: u64,
    pub retention_interval_seconds: u64,
    pub notification_lease_seconds: u64,
    pub notification_retry_base_seconds: u64,
    pub notification_retry_max_seconds: u64,
}

impl Default for IngestTuning {
    fn default() -> Self {
        Self {
            retention_bytes: 2 * 1024 * 1024 * 1024,
            retention_seconds: 24 * 60 * 60,
            segment_bytes: 128 * 1024 * 1024,
            max_record_bytes: 1024 * 1024,
            writer_batch_size: 1_000,
            alert_batch_size: 250,
            notification_batch_size: 10,
            writer_flush_seconds: 1,
            alert_event_interval_milliseconds: 250,
            alert_window_interval_seconds: 60,
            notification_interval_seconds: 1,
            retention_interval_seconds: 60,
            notification_lease_seconds: 30,
            notification_retry_base_seconds: 1,
            notification_retry_max_seconds: 3_600,
        }
    }
}

impl IngestTuning {
    pub fn validate(&self) -> Result<(), SystemConfigurationError> {
        validate_tuning(self)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SystemConfigurationError {
    #[error("SMTP configuration is invalid: {0}")]
    InvalidSmtp(String),
    #[error("ingest tuning is invalid: {0}")]
    InvalidTuning(String),
    #[error("MQTT configuration is invalid: {0}")]
    InvalidMqtt(String),
    #[error("environment value {key} is invalid")]
    InvalidEnvironment { key: &'static str },
}

pub fn managed_ingest_environment_keys() -> impl Iterator<Item = &'static str> {
    SMTP_KEYS
        .iter()
        .copied()
        .chain(MQTT_KEYS.iter().copied())
        .chain([
            "IOT_STREAM_RETENTION_BYTES",
            "IOT_STREAM_RETENTION_SECONDS",
            "IOT_STREAM_SEGMENT_BYTES",
            "IOT_STREAM_MAX_RECORD_BYTES",
            "IOT_WRITER_BATCH_SIZE",
            "IOT_ALERT_BATCH_SIZE",
            "IOT_NOTIFICATION_BATCH_SIZE",
            "IOT_WRITER_FLUSH_SECONDS",
            "IOT_ALERT_EVENT_INTERVAL_MILLISECONDS",
            "IOT_ALERT_WINDOW_INTERVAL_SECONDS",
            "IOT_NOTIFICATION_INTERVAL_SECONDS",
            "IOT_RETENTION_INTERVAL_SECONDS",
            "IOT_NOTIFICATION_LEASE_SECONDS",
            "IOT_NOTIFICATION_RETRY_BASE_SECONDS",
            "IOT_NOTIFICATION_RETRY_MAX_SECONDS",
        ])
}

pub fn read_system_configuration(
    values: &BTreeMap<String, String>,
) -> Result<SystemConfiguration, SystemConfigurationError> {
    let tuning = tuning_from_values(values)?;
    tuning.validate()?;
    Ok(SystemConfiguration {
        smtp: smtp_from_values(values)?,
        mqtt: mqtt_from_values(values)?,
        tuning,
    })
}

pub fn apply_system_configuration_update(
    values: &mut BTreeMap<String, String>,
    update: &SystemConfigurationUpdate,
) -> Result<SystemConfiguration, SystemConfigurationError> {
    update.tuning.validate()?;
    let mut next = values.clone();

    if update.smtp.enabled {
        let host = required_update_value("host", &update.smtp.host)?;
        let username = required_update_value("username", &update.smtp.username)?;
        let from = required_update_value("from", &update.smtp.from)?;
        let to = required_update_value("to", &update.smtp.to)?;
        if update.smtp.port == 0 {
            return Err(SystemConfigurationError::InvalidSmtp(
                "port must be greater than zero".to_owned(),
            ));
        }
        if update.smtp.timeout_seconds == 0 || update.smtp.timeout_seconds > 300 {
            return Err(SystemConfigurationError::InvalidSmtp(
                "timeout_seconds must be between 1 and 300".to_owned(),
            ));
        }
        let password = update
            .smtp
            .password
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .or_else(|| next.get("SMTP_PASSWORD").cloned())
            .ok_or_else(|| {
                SystemConfigurationError::InvalidSmtp(
                    "password is required until SMTP has an existing password".to_owned(),
                )
            })?;
        validate_env_value(&password).map_err(SystemConfigurationError::InvalidSmtp)?;

        next.insert("SMTP_HOST".to_owned(), host);
        next.insert("SMTP_PORT".to_owned(), update.smtp.port.to_string());
        next.insert("SMTP_USERNAME".to_owned(), username);
        next.insert("SMTP_PASSWORD".to_owned(), password);
        next.insert("ALERT_EMAIL_FROM".to_owned(), from);
        next.insert("ALERT_EMAIL_TO".to_owned(), to);
        next.insert(
            "SMTP_TIMEOUT_SECONDS".to_owned(),
            update.smtp.timeout_seconds.to_string(),
        );
    } else {
        for key in SMTP_KEYS {
            next.remove(*key);
        }
    }

    if let Some(mqtt) = &update.mqtt {
        let host = mqtt.host.trim();
        if host.is_empty() {
            return Err(SystemConfigurationError::InvalidMqtt(
                "host is required".to_owned(),
            ));
        }
        validate_env_value(host).map_err(SystemConfigurationError::InvalidMqtt)?;
        if mqtt.port == 0 {
            return Err(SystemConfigurationError::InvalidMqtt(
                "port must be greater than zero".to_owned(),
            ));
        }
        next.insert("MQTT_BROKER_HOST".to_owned(), host.to_owned());
        next.insert("MQTT_BROKER_PORT".to_owned(), mqtt.port.to_string());
    }

    write_tuning(&mut next, &update.tuning);
    let configuration = read_system_configuration(&next)?;
    *values = next;
    Ok(configuration)
}

fn mqtt_from_values(
    values: &BTreeMap<String, String>,
) -> Result<MqttConfiguration, SystemConfigurationError> {
    let host = values
        .get("MQTT_BROKER_HOST")
        .map(String::as_str)
        .unwrap_or("127.0.0.1")
        .trim();
    if host.is_empty() {
        return Err(SystemConfigurationError::InvalidMqtt(
            "host is required".to_owned(),
        ));
    }
    validate_env_value(host).map_err(SystemConfigurationError::InvalidMqtt)?;
    let port = parse_u16(values, "MQTT_BROKER_PORT", 1883)?;
    if port == 0 {
        return Err(SystemConfigurationError::InvalidMqtt(
            "port must be greater than zero".to_owned(),
        ));
    }
    Ok(MqttConfiguration {
        host: host.to_owned(),
        port,
    })
}

fn smtp_from_values(
    values: &BTreeMap<String, String>,
) -> Result<SmtpConfiguration, SystemConfigurationError> {
    if !SMTP_KEYS.iter().any(|key| values.contains_key(*key)) {
        return Ok(SmtpConfiguration {
            enabled: false,
            host: None,
            port: 465,
            username: None,
            password_configured: false,
            from: None,
            to: None,
            timeout_seconds: 15,
        });
    }

    let host = required_env_value(values, "SMTP_HOST")?;
    let username = required_env_value(values, "SMTP_USERNAME")?;
    let password = required_env_value(values, "SMTP_PASSWORD")?;
    let from = required_env_value(values, "ALERT_EMAIL_FROM")?;
    let to = required_env_value(values, "ALERT_EMAIL_TO")?;
    let port = parse_u16(values, "SMTP_PORT", 465)?;
    let timeout_seconds = parse_u64(values, "SMTP_TIMEOUT_SECONDS", 15)?;
    if port == 0 || timeout_seconds == 0 || timeout_seconds > 300 {
        return Err(SystemConfigurationError::InvalidSmtp(
            "port or timeout is outside the allowed range".to_owned(),
        ));
    }

    Ok(SmtpConfiguration {
        enabled: true,
        host: Some(host),
        port,
        username: Some(username),
        password_configured: !password.is_empty(),
        from: Some(from),
        to: Some(to),
        timeout_seconds,
    })
}

fn required_env_value(
    values: &BTreeMap<String, String>,
    key: &'static str,
) -> Result<String, SystemConfigurationError> {
    values
        .get(key)
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .ok_or_else(|| SystemConfigurationError::InvalidSmtp(format!("missing {key}")))
}

fn required_update_value(
    field: &str,
    value: &Option<String>,
) -> Result<String, SystemConfigurationError> {
    let value = value
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SystemConfigurationError::InvalidSmtp(format!("{field} is required")))?;
    validate_env_value(value).map_err(SystemConfigurationError::InvalidSmtp)?;
    Ok(value.to_owned())
}

fn validate_env_value(value: &str) -> Result<(), String> {
    if value.contains('\0') || value.contains('\n') || value.contains('\r') {
        return Err("values cannot contain line breaks or NUL".to_owned());
    }
    Ok(())
}

fn tuning_from_values(
    values: &BTreeMap<String, String>,
) -> Result<IngestTuning, SystemConfigurationError> {
    let defaults = IngestTuning::default();
    Ok(IngestTuning {
        retention_bytes: parse_u64(
            values,
            "IOT_STREAM_RETENTION_BYTES",
            defaults.retention_bytes,
        )?,
        retention_seconds: parse_u64(
            values,
            "IOT_STREAM_RETENTION_SECONDS",
            defaults.retention_seconds,
        )?,
        segment_bytes: parse_u64(values, "IOT_STREAM_SEGMENT_BYTES", defaults.segment_bytes)?,
        max_record_bytes: parse_u64(
            values,
            "IOT_STREAM_MAX_RECORD_BYTES",
            defaults.max_record_bytes,
        )?,
        writer_batch_size: parse_u64(values, "IOT_WRITER_BATCH_SIZE", defaults.writer_batch_size)?,
        alert_batch_size: parse_u64(values, "IOT_ALERT_BATCH_SIZE", defaults.alert_batch_size)?,
        notification_batch_size: parse_u64(
            values,
            "IOT_NOTIFICATION_BATCH_SIZE",
            defaults.notification_batch_size,
        )?,
        writer_flush_seconds: parse_u64(
            values,
            "IOT_WRITER_FLUSH_SECONDS",
            defaults.writer_flush_seconds,
        )?,
        alert_event_interval_milliseconds: parse_u64(
            values,
            "IOT_ALERT_EVENT_INTERVAL_MILLISECONDS",
            defaults.alert_event_interval_milliseconds,
        )?,
        alert_window_interval_seconds: parse_u64(
            values,
            "IOT_ALERT_WINDOW_INTERVAL_SECONDS",
            defaults.alert_window_interval_seconds,
        )?,
        notification_interval_seconds: parse_u64(
            values,
            "IOT_NOTIFICATION_INTERVAL_SECONDS",
            defaults.notification_interval_seconds,
        )?,
        retention_interval_seconds: parse_u64(
            values,
            "IOT_RETENTION_INTERVAL_SECONDS",
            defaults.retention_interval_seconds,
        )?,
        notification_lease_seconds: parse_u64(
            values,
            "IOT_NOTIFICATION_LEASE_SECONDS",
            defaults.notification_lease_seconds,
        )?,
        notification_retry_base_seconds: parse_u64(
            values,
            "IOT_NOTIFICATION_RETRY_BASE_SECONDS",
            defaults.notification_retry_base_seconds,
        )?,
        notification_retry_max_seconds: parse_u64(
            values,
            "IOT_NOTIFICATION_RETRY_MAX_SECONDS",
            defaults.notification_retry_max_seconds,
        )?,
    })
}

fn parse_u64(
    values: &BTreeMap<String, String>,
    key: &'static str,
    default: u64,
) -> Result<u64, SystemConfigurationError> {
    values
        .get(key)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| SystemConfigurationError::InvalidEnvironment { key })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_u16(
    values: &BTreeMap<String, String>,
    key: &'static str,
    default: u16,
) -> Result<u16, SystemConfigurationError> {
    values
        .get(key)
        .map(|value| {
            value
                .parse::<u16>()
                .map_err(|_| SystemConfigurationError::InvalidEnvironment { key })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn write_tuning(values: &mut BTreeMap<String, String>, tuning: &IngestTuning) {
    for (key, value) in [
        ("IOT_STREAM_RETENTION_BYTES", tuning.retention_bytes),
        ("IOT_STREAM_RETENTION_SECONDS", tuning.retention_seconds),
        ("IOT_STREAM_SEGMENT_BYTES", tuning.segment_bytes),
        ("IOT_STREAM_MAX_RECORD_BYTES", tuning.max_record_bytes),
        ("IOT_WRITER_BATCH_SIZE", tuning.writer_batch_size),
        ("IOT_ALERT_BATCH_SIZE", tuning.alert_batch_size),
        (
            "IOT_NOTIFICATION_BATCH_SIZE",
            tuning.notification_batch_size,
        ),
        ("IOT_WRITER_FLUSH_SECONDS", tuning.writer_flush_seconds),
        (
            "IOT_ALERT_EVENT_INTERVAL_MILLISECONDS",
            tuning.alert_event_interval_milliseconds,
        ),
        (
            "IOT_ALERT_WINDOW_INTERVAL_SECONDS",
            tuning.alert_window_interval_seconds,
        ),
        (
            "IOT_NOTIFICATION_INTERVAL_SECONDS",
            tuning.notification_interval_seconds,
        ),
        (
            "IOT_RETENTION_INTERVAL_SECONDS",
            tuning.retention_interval_seconds,
        ),
        (
            "IOT_NOTIFICATION_LEASE_SECONDS",
            tuning.notification_lease_seconds,
        ),
        (
            "IOT_NOTIFICATION_RETRY_BASE_SECONDS",
            tuning.notification_retry_base_seconds,
        ),
        (
            "IOT_NOTIFICATION_RETRY_MAX_SECONDS",
            tuning.notification_retry_max_seconds,
        ),
    ] {
        values.insert(key.to_owned(), value.to_string());
    }
}

fn validate_tuning(value: &IngestTuning) -> Result<(), SystemConfigurationError> {
    if value.retention_bytes < 1024 * 1024 || value.retention_bytes > 16 * 1024_u64.pow(4) {
        return Err(SystemConfigurationError::InvalidTuning(
            "retention_bytes must be between 1 MiB and 16 TiB".to_owned(),
        ));
    }
    if value.retention_seconds < 60 || value.retention_seconds > 365 * 24 * 60 * 60 {
        return Err(SystemConfigurationError::InvalidTuning(
            "retention_seconds must be between 60 and 31536000".to_owned(),
        ));
    }
    if value.segment_bytes < 1024 * 1024
        || value.segment_bytes > value.retention_bytes
        || value.max_record_bytes == 0
        || value.max_record_bytes > value.segment_bytes
    {
        return Err(SystemConfigurationError::InvalidTuning(
            "segment and record sizes are inconsistent".to_owned(),
        ));
    }
    if value.writer_batch_size == 0
        || value.writer_batch_size > 10_000
        || value.alert_batch_size == 0
        || value.alert_batch_size > 10_000
        || value.notification_batch_size == 0
        || value.notification_batch_size > 1_000
    {
        return Err(SystemConfigurationError::InvalidTuning(
            "batch sizes are outside the allowed range".to_owned(),
        ));
    }
    if value.writer_flush_seconds == 0
        || value.writer_flush_seconds > 3_600
        || value.alert_event_interval_milliseconds < 10
        || value.alert_event_interval_milliseconds > 60_000
        || value.alert_window_interval_seconds == 0
        || value.alert_window_interval_seconds > 3_600
        || value.notification_interval_seconds == 0
        || value.notification_interval_seconds > 3_600
        || value.retention_interval_seconds == 0
        || value.retention_interval_seconds > 3_600
        || value.notification_lease_seconds == 0
        || value.notification_lease_seconds > 3_600
        || value.notification_retry_base_seconds == 0
        || value.notification_retry_max_seconds < value.notification_retry_base_seconds
        || value.notification_retry_max_seconds > 86_400
    {
        return Err(SystemConfigurationError::InvalidTuning(
            "worker intervals or retry settings are outside the allowed range".to_owned(),
        ));
    }
    Ok(())
}
