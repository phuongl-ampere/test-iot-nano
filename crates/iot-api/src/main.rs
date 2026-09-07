use std::{collections::BTreeMap, env, net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use iot_api::{
    ApiState, CommandMqttConfig, HelperSystemConfigurationService, SqliteApiState, TokenVault,
    bootstrap_users, bootstrap_users_sqlite, router, sqlite_router,
};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_ingest::migrate;
use iot_storage::SqliteStore;
use sqlx::PgPool;
use tower_http::cors::{Any, CorsLayer};

#[derive(Debug, Parser)]
#[command(about = "HTTP API for the IoT telemetry platform")]
struct Arguments {
    #[arg(long, env = "IOT_API_ADDRESS", default_value = "127.0.0.1:8080")]
    address: SocketAddr,
    #[arg(long, env = "MQTT_BROKER_HOST", default_value = "127.0.0.1")]
    broker_host: String,
    #[arg(long, env = "MQTT_BROKER_PORT", default_value_t = 1883)]
    broker_port: u16,
    #[arg(long, env = "MQTT_API_USERNAME")]
    mqtt_username: Option<String>,
    #[arg(long, env = "MQTT_API_PASSWORD")]
    mqtt_password: Option<String>,
    #[arg(long, env = "IOT_NANOMQ_AUTH_SECRET")]
    nanomq_auth_secret: String,
    #[arg(long, env = "IOT_DEVICE_TOKEN_VAULT_KEY")]
    device_token_vault_key: Option<String>,
    #[arg(
        long,
        env = "IOT_ADMIN_HELPER_PATH",
        default_value = "/usr/local/sbin/iot-admin-helper"
    )]
    admin_helper_path: PathBuf,
    #[arg(long, env = "IOT_SYSTEM_CONFIGURATION_DIRECT", default_value_t = false)]
    system_configuration_direct: bool,
}

fn validate_vault_key(value: &str) -> Result<(), ()> {
    if value.len() < 32 || !value.is_ascii() || value.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        Err(())
    } else {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let storage = StorageConfiguration::from_values(&BTreeMap::from_iter(env::vars()))
        .map_err(|error| error.to_string())?;
    if validate_vault_key(&arguments.nanomq_auth_secret).is_err() {
        return Err(
            "IOT_NANOMQ_AUTH_SECRET must be at least 32 ASCII non-whitespace characters".into(),
        );
    }
    if arguments
        .device_token_vault_key
        .as_deref()
        .is_some_and(|key| validate_vault_key(key).is_err())
    {
        return Err(
            "IOT_DEVICE_TOKEN_VAULT_KEY must be at least 32 ASCII non-whitespace characters".into(),
        );
    }
    let vault_key = arguments
        .device_token_vault_key
        .as_deref()
        .unwrap_or(&arguments.nanomq_auth_secret);
    let system_configuration = Arc::new(if arguments.system_configuration_direct {
        HelperSystemConfigurationService::direct(arguments.admin_helper_path)
    } else {
        HelperSystemConfigurationService::new(arguments.admin_helper_path)
    });
    let app = match storage.storage {
        DatabaseStorage::Timescale => {
            let pool = PgPool::connect(
                storage
                    .database_url
                    .as_deref()
                    .ok_or("DATABASE_URL is required for Timescale storage")?,
            )
            .await?;
            migrate(&pool).await?;
            bootstrap_users(&pool).await?;
            router(
                ApiState::with_command_mqtt(
                    pool,
                    CommandMqttConfig {
                        client_id: "iot-api".to_owned(),
                        broker_host: arguments.broker_host,
                        broker_port: arguments.broker_port,
                        username: arguments.mqtt_username,
                        password: arguments.mqtt_password,
                    },
                )
                .with_device_token_vault(TokenVault::from_key_material(vault_key))
                .with_nanomq_auth_secret(arguments.nanomq_auth_secret)
                .with_system_configuration(system_configuration),
            )
        }
        DatabaseStorage::Sqlite => {
            let store = SqliteStore::open(&storage).await?;
            bootstrap_users_sqlite(store.pool()).await?;
            sqlite_router(
                SqliteApiState::new(store)
                    .with_device_token_vault(TokenVault::from_key_material(vault_key))
                    .with_nanomq_auth_secret(arguments.nanomq_auth_secret)
                    .with_system_configuration(system_configuration),
            )
        }
    }
    .layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );
    let listener = tokio::net::TcpListener::bind(arguments.address).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_vault_key;

    #[test]
    fn vault_key_requires_at_least_32_ascii_non_whitespace_characters() {
        assert!(validate_vault_key("a".repeat(32).as_str()).is_ok());
        assert!(validate_vault_key("").is_err());
        assert!(validate_vault_key("short").is_err());
        assert!(validate_vault_key("a".repeat(31).as_str()).is_err());
        assert!(validate_vault_key("a".repeat(31).as_str()).is_err());
        assert!(validate_vault_key("a".repeat(32).replace('a', " ").as_str()).is_err());
    }
}
