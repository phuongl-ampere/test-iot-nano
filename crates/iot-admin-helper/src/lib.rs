#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

use iot_nano_foundation::{
    SystemConfiguration, SystemConfigurationError, SystemConfigurationUpdate,
    apply_system_configuration_update, read_system_configuration,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HelperError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Configuration(#[from] SystemConfigurationError),
    #[error("invalid environment file: {0}")]
    Environment(String),
}

pub fn read_config_file(path: &Path) -> Result<SystemConfiguration, HelperError> {
    let values = read_environment(path)?;
    Ok(read_system_configuration(&values)?)
}

pub fn apply_config_file(
    path: &Path,
    update: &SystemConfigurationUpdate,
) -> Result<SystemConfiguration, HelperError> {
    let mut values = read_environment(path)?;
    let configuration = apply_system_configuration_update(&mut values, update)?;
    write_environment_atomic(path, &values)?;
    Ok(configuration)
}

pub fn read_config_files(
    ingest_path: &Path,
    smtp_path: &Path,
) -> Result<SystemConfiguration, HelperError> {
    let values = merged_environment(ingest_path, smtp_path)?;
    Ok(read_system_configuration(&values)?)
}

pub fn read_config_files_with_api(
    ingest_path: &Path,
    smtp_path: &Path,
    api_path: &Path,
) -> Result<SystemConfiguration, HelperError> {
    let mut values = merged_environment(ingest_path, smtp_path)?;
    overlay_mqtt_values(&mut values, &read_environment(api_path)?);
    Ok(read_system_configuration(&values)?)
}

pub fn apply_config_files(
    ingest_path: &Path,
    smtp_path: &Path,
    update: &SystemConfigurationUpdate,
) -> Result<SystemConfiguration, HelperError> {
    let mut values = merged_environment(ingest_path, smtp_path)?;
    let configuration = apply_system_configuration_update(&mut values, update)?;
    let ingest_values = values
        .iter()
        .filter(|(key, _)| !is_smtp_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let smtp_values = values
        .iter()
        .filter(|(key, _)| is_smtp_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();

    write_environment_atomic(ingest_path, &ingest_values)?;
    write_environment_atomic_with_mode(smtp_path, &smtp_values, 0o640)?;
    Ok(configuration)
}

pub fn apply_config_files_with_api(
    ingest_path: &Path,
    smtp_path: &Path,
    api_path: &Path,
    update: &SystemConfigurationUpdate,
) -> Result<SystemConfiguration, HelperError> {
    let mut values = merged_environment(ingest_path, smtp_path)?;
    let mut api_values = read_environment(api_path)?;
    overlay_mqtt_values(&mut values, &api_values);
    let configuration = apply_system_configuration_update(&mut values, update)?;
    let ingest_values = values
        .iter()
        .filter(|(key, _)| !is_smtp_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let smtp_values = values
        .iter()
        .filter(|(key, _)| is_smtp_key(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    if update.mqtt.is_some() {
        overlay_mqtt_values(&mut api_values, &values);
    }

    write_environment_atomic(ingest_path, &ingest_values)?;
    write_environment_atomic_with_mode(smtp_path, &smtp_values, 0o640)?;
    write_environment_atomic_with_mode(api_path, &api_values, 0o640)?;
    Ok(configuration)
}

pub fn read_environment(path: &Path) -> Result<BTreeMap<String, String>, HelperError> {
    let source = fs::read_to_string(path)?;
    let mut values = BTreeMap::new();
    for (line_number, line) in source.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line.split_once('=').ok_or_else(|| {
            HelperError::Environment(format!("line {} has no '='", line_number + 1))
        })?;
        if !is_environment_key(key) {
            return Err(HelperError::Environment(format!(
                "line {} has invalid key",
                line_number + 1
            )));
        }
        values.insert(key.to_owned(), parse_environment_value(value)?);
    }
    Ok(values)
}

fn merged_environment(
    ingest_path: &Path,
    smtp_path: &Path,
) -> Result<BTreeMap<String, String>, HelperError> {
    let mut values = read_environment(ingest_path)?;
    match read_environment(smtp_path) {
        Ok(smtp_values) => {
            values.retain(|key, _| !is_smtp_key(key));
            values.extend(smtp_values);
        }
        Err(HelperError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(values)
}

fn overlay_mqtt_values(
    destination: &mut BTreeMap<String, String>,
    source: &BTreeMap<String, String>,
) {
    for key in ["MQTT_BROKER_HOST", "MQTT_BROKER_PORT"] {
        if let Some(value) = source.get(key) {
            destination.insert(key.to_owned(), value.clone());
        }
    }
}

pub fn write_environment_atomic(
    path: &Path,
    values: &BTreeMap<String, String>,
) -> Result<(), HelperError> {
    write_environment_atomic_with_mode(path, values, 0o600)
}

fn write_environment_atomic_with_mode(
    path: &Path,
    values: &BTreeMap<String, String>,
    mode: u32,
) -> Result<(), HelperError> {
    let parent = path
        .parent()
        .ok_or_else(|| HelperError::Environment("configuration path has no parent".to_owned()))?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| HelperError::Environment("invalid configuration filename".to_owned()))?,
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(mode)
        .open(&temporary)?;
    for (key, value) in values {
        writeln!(file, "{key}={}", render_environment_value(value))?;
    }
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn is_smtp_key(key: &str) -> bool {
    key.starts_with("SMTP_") || matches!(key, "ALERT_EMAIL_FROM" | "ALERT_EMAIL_TO")
}

fn is_environment_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn parse_environment_value(value: &str) -> Result<String, HelperError> {
    let value = value.trim();
    if value.starts_with('"') {
        if value.len() < 2 || !value.ends_with('"') {
            return Err(HelperError::Environment(
                "unterminated quoted environment value".to_owned(),
            ));
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
            return Err(HelperError::Environment(
                "quoted environment value ends with escape".to_owned(),
            ));
        }
        return Ok(result);
    }
    Ok(value.to_owned())
}

fn render_environment_value(value: &str) -> String {
    if !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'_' | b'-' | b'.' | b'/' | b':' | b'@' | b'+' | b',' | b'?' | b'='
                )
        })
    {
        return value.to_owned();
    }
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}
