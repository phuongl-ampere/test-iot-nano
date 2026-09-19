#![forbid(unsafe_code)]

use std::time::Duration;

use chrono::{DateTime, Utc};
use iot_nano_foundation::TelemetryEvent;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde_json::json;
use thiserror::Error;
use tokio::time::sleep;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationConfig {
    pub broker_host: String,
    pub broker_port: u16,
    pub devices: u32,
    pub messages_per_device: u64,
    pub interval: Duration,
}

impl SimulationConfig {
    pub fn validate(&self) -> Result<(), SimulatorError> {
        if self.devices == 0 {
            return Err(SimulatorError::ZeroDevices);
        }
        if self.messages_per_device == 0 {
            return Err(SimulatorError::ZeroMessages);
        }
        if self.interval.is_zero() {
            return Err(SimulatorError::ZeroInterval);
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum SimulatorError {
    #[error("devices must be greater than zero")]
    ZeroDevices,
    #[error("messages per device must be greater than zero")]
    ZeroMessages,
    #[error("interval must be greater than zero")]
    ZeroInterval,
    #[error(transparent)]
    Client(#[from] rumqttc::ClientError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

impl PartialEq for SimulatorError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::ZeroDevices, Self::ZeroDevices)
                | (Self::ZeroMessages, Self::ZeroMessages)
                | (Self::ZeroInterval, Self::ZeroInterval)
        )
    }
}

impl Eq for SimulatorError {}

pub fn device_id(device_number: u32) -> String {
    format!("esp-{device_number:06}")
}

pub fn telemetry_topic(device_id: &str) -> String {
    format!("iot/v1/devices/{device_id}/telemetry")
}

pub fn simulation_round(
    config: &SimulationConfig,
    sequence: u64,
    event_at: DateTime<Utc>,
) -> Result<Vec<(String, TelemetryEvent)>, SimulatorError> {
    config.validate()?;

    Ok((1..=config.devices)
        .map(|device_number| {
            let event = simulated_telemetry(device_number, sequence, event_at);
            let topic = telemetry_topic(&event.device_id);
            (topic, event)
        })
        .collect())
}

pub async fn publish_simulation(config: SimulationConfig) -> Result<(), SimulatorError> {
    config.validate()?;

    let client_id = format!("device-simulator-{}", Uuid::new_v4());
    let mut options = MqttOptions::new(client_id, &config.broker_host, config.broker_port);
    options.set_keep_alive(Duration::from_secs(5));
    let request_capacity = usize::try_from(config.devices)
        .unwrap_or(usize::MAX)
        .clamp(100, 10_000);
    let (client, mut event_loop) = AsyncClient::new(options, request_capacity);
    let driver = tokio::spawn(async move { while event_loop.poll().await.is_ok() {} });

    for sequence in 1..=config.messages_per_device {
        for (topic, event) in simulation_round(&config, sequence, Utc::now())? {
            client
                .publish(topic, QoS::AtLeastOnce, false, serde_json::to_vec(&event)?)
                .await?;
        }

        if sequence < config.messages_per_device {
            sleep(config.interval).await;
        }
    }

    client.disconnect().await?;
    sleep(Duration::from_millis(50)).await;
    driver.abort();

    Ok(())
}

pub fn simulated_telemetry(
    device_number: u32,
    sequence: u64,
    event_at: DateTime<Utc>,
) -> TelemetryEvent {
    let device_id = device_id(device_number);
    let boot_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, device_id.as_bytes());
    let mut measurements = serde_json::Map::new();
    measurements.insert(
        "temperature_c".to_owned(),
        json!(20.0 + f64::from(device_number % 100) / 10.0),
    );
    measurements.insert(
        "humidity_pct".to_owned(),
        json!(40.0 + f64::from(device_number % 60)),
    );

    TelemetryEvent {
        schema_version: 1,
        device_id,
        boot_id,
        sequence,
        event_at,
        measurements,
        gateway_device_id: None,
    }
}
