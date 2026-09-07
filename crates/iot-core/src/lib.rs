#![forbid(unsafe_code)]

mod device_token;
mod gateway;
mod storage;
mod system_config;
mod telemetry;

pub use device_token::{
    DEVICE_TELEMETRY_TOPIC, DEVICE_TOKEN_PREFIX_LENGTH, DeviceTelemetryPayload, DeviceTokenError,
    device_token_prefix, generate_device_token, hash_device_token, verify_device_token,
};
pub use gateway::{
    GATEWAY_CONNECT_TOPIC, GATEWAY_DISCONNECT_TOPIC, GATEWAY_TELEMETRY_TOPIC,
    GatewayChildLifecyclePayload, GatewayPayloadError, GatewayTelemetryPayload,
};
pub use storage::{DatabaseStorage, StorageConfiguration, StorageConfigurationError};
pub use system_config::{
    IngestTuning, MqttConfiguration, MqttConfigurationUpdate, SmtpConfiguration,
    SmtpConfigurationUpdate, SystemConfiguration, SystemConfigurationError,
    SystemConfigurationUpdate, apply_system_configuration_update, managed_ingest_environment_keys,
    read_system_configuration,
};
pub use telemetry::{TELEMETRY_TOPIC_PREFIX, TelemetryEvent, TelemetryValidationError};
