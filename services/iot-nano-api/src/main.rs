use std::{collections::BTreeMap, env, net::SocketAddr, path::PathBuf, sync::Arc};

use clap::Parser;
use iot_api::{
    ApiState, CoreClient, HelperSystemConfigurationService, SqliteApiState, TokenVault,
    bootstrap_power_switcher_profile, bootstrap_power_switcher_profile_sqlite, bootstrap_users,
    bootstrap_users_sqlite, router, sqlite_router,
};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_nano_core::migrate;
use iot_storage::{PlatformStore, SqliteStore};
use sqlx::PgPool;
use tower_http::cors::{Any, CorsLayer};

#[derive(Debug, Parser)]
#[command(about = "HTTP API for the IoT telemetry platform")]
struct Arguments {
    #[arg(long, env = "IOT_API_ADDRESS", default_value = "127.0.0.1:8080")]
    address: SocketAddr,
    #[arg(long, env = "IOT_NANO_MQTTD_API_SECRET")]
    mqttd_device_transport_secret: String,
    #[arg(
        long,
        env = "IOT_NANO_MQTTD_INTERNAL_URL",
        default_value = "http://127.0.0.1:8083"
    )]
    mqttd_device_transport_control_url: String,
    #[arg(long, env = "IOT_NANO_API_MQTTD_SECRET")]
    api_mqttd_secret: String,
    #[arg(long, env = "IOT_DEVICE_TOKEN_VAULT_KEY")]
    device_token_vault_key: Option<String>,
    #[arg(long, env = "IOT_NANO_CORE_URL")]
    core_url: String,
    #[arg(long, env = "IOT_NANO_API_CORE_SECRET")]
    api_core_secret: String,
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

fn api_sqlite_storage_values(
    mut values: BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, String> {
    if values
        .get("IOT_DATABASE_STORAGE")
        .is_some_and(|storage| storage.eq_ignore_ascii_case("sqlite"))
    {
        let path = values.remove("IOT_NANO_API_SQLITE_PATH").ok_or_else(|| {
            "IOT_NANO_API_SQLITE_PATH is required when IOT_DATABASE_STORAGE=sqlite".to_owned()
        })?;
        values.insert("IOT_SQLITE_PATH".to_owned(), path);
    }
    Ok(values)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();
    let storage_values = api_sqlite_storage_values(BTreeMap::from_iter(env::vars()))
        .map_err(|error| error.to_string())?;
    let storage =
        StorageConfiguration::from_values(&storage_values).map_err(|error| error.to_string())?;
    if validate_vault_key(&arguments.mqttd_device_transport_secret).is_err() {
        return Err(
            "IOT_NANO_MQTTD_API_SECRET must be at least 32 ASCII non-whitespace characters".into(),
        );
    }
    if validate_vault_key(&arguments.api_mqttd_secret).is_err() {
        return Err(
            "IOT_NANO_API_MQTTD_SECRET must be at least 32 ASCII non-whitespace characters".into(),
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
        .unwrap_or(&arguments.mqttd_device_transport_secret);
    let core_client =
        CoreClient::new(&arguments.core_url, &arguments.api_core_secret).map_err(|error| {
            format!("IOT_NANO_CORE_URL and IOT_NANO_API_CORE_SECRET are required: {error}")
        })?;
    let system_configuration = Arc::new(if arguments.system_configuration_direct {
        HelperSystemConfigurationService::direct(arguments.admin_helper_path)
    } else {
        HelperSystemConfigurationService::new(arguments.admin_helper_path)
    });
    let app = match storage.storage {
        DatabaseStorage::Timescale => {
            let oauth_store = PlatformStore::open(&storage).await?;
            let pool = PgPool::connect(
                storage
                    .database_url
                    .as_deref()
                    .ok_or("DATABASE_URL is required for Timescale storage")?,
            )
            .await?;
            migrate(&pool).await?;
            bootstrap_users(&pool).await?;
            bootstrap_power_switcher_profile(&pool).await?;
            let mut state = ApiState::new(pool)
                .with_oauth_store(oauth_store)
                .with_device_token_vault(TokenVault::from_key_material(vault_key))
                .with_mqttd_device_transport_secret(arguments.mqttd_device_transport_secret)
                .with_api_mqttd_control(
                    arguments.mqttd_device_transport_control_url,
                    arguments.api_mqttd_secret,
                )
                .with_system_configuration(system_configuration);
            state = state.with_core_client(core_client.clone());
            router(state)
        }
        DatabaseStorage::Sqlite => {
            let oauth_store = PlatformStore::open(&storage).await?;
            let store = SqliteStore::open(&storage).await?;
            bootstrap_users_sqlite(store.pool()).await?;
            bootstrap_power_switcher_profile_sqlite(store.pool()).await?;
            let mut state = SqliteApiState::new(store)
                .with_oauth_store(oauth_store)
                .with_device_token_vault(TokenVault::from_key_material(vault_key))
                .with_mqttd_device_transport_secret(arguments.mqttd_device_transport_secret)
                .with_api_mqttd_control(
                    arguments.mqttd_device_transport_control_url,
                    arguments.api_mqttd_secret,
                )
                .with_system_configuration(system_configuration);
            state = state.with_core_client(core_client);
            sqlite_router(state)
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
    use std::collections::BTreeMap;

    use super::{api_sqlite_storage_values, validate_vault_key};

    #[test]
    fn vault_key_requires_at_least_32_ascii_non_whitespace_characters() {
        assert!(validate_vault_key("a".repeat(32).as_str()).is_ok());
        assert!(validate_vault_key("").is_err());
        assert!(validate_vault_key("short").is_err());
        assert!(validate_vault_key("a".repeat(31).as_str()).is_err());
        assert!(validate_vault_key("a".repeat(31).as_str()).is_err());
        assert!(validate_vault_key("a".repeat(32).replace('a', " ").as_str()).is_err());
    }

    #[test]
    fn sqlite_requires_an_api_owned_database_path() {
        let values = BTreeMap::from([("IOT_DATABASE_STORAGE".to_owned(), "sqlite".to_owned())]);
        assert!(api_sqlite_storage_values(values).is_err());

        let values = BTreeMap::from([
            ("IOT_DATABASE_STORAGE".to_owned(), "sqlite".to_owned()),
            (
                "IOT_NANO_API_SQLITE_PATH".to_owned(),
                "/var/lib/iot-nano-api/api.db".to_owned(),
            ),
        ]);
        let values = api_sqlite_storage_values(values).unwrap();
        assert_eq!(
            values.get("IOT_SQLITE_PATH").map(String::as_str),
            Some("/var/lib/iot-nano-api/api.db")
        );
    }
}
