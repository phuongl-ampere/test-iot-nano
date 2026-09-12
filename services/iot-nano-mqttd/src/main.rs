use std::{net::SocketAddr, path::PathBuf};

use axum::{Router, routing::get};
use clap::Parser;
use iot_nano_mqttd::{
    BrokerFileConfig, DeviceTransportEndpoints, HttpDeviceAuthenticator, HttpRpcResponseForwarder,
    HttpStreamUplinkForwarder, MqttdDeviceTransport, RuntimeAddressConfiguration, SqliteStorage,
    build_policy, load_tls_acceptor, management_router,
    management_router_with_config_and_runtime_and_policy, resolve_runtime_configuration,
    start_broker,
};

#[derive(Debug, Parser)]
#[command(about = "Rust MQTT broker and Rush IoT device transport")]
struct Arguments {
    #[arg(long, env = "IOT_MQTTD_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, env = "IOT_MQTTD_PLAIN_ADDRESS", default_value = "0.0.0.0:1883")]
    plaintext_address: SocketAddr,
    #[arg(long, env = "IOT_MQTTD_TLS_ADDRESS", default_value = "0.0.0.0:8883")]
    tls_address: SocketAddr,
    #[arg(long, env = "IOT_MQTTD_TLS_CERT_PATH")]
    tls_cert_path: Option<PathBuf>,
    #[arg(long, env = "IOT_MQTTD_TLS_KEY_PATH")]
    tls_key_path: Option<PathBuf>,
    #[arg(
        long,
        env = "IOT_MQTTD_MANAGEMENT_ADDRESS",
        default_value = "127.0.0.1:8082"
    )]
    management_address: SocketAddr,
    #[arg(
        long,
        env = "IOT_MQTTD_TRANSPORT_INTERNAL_ADDRESS",
        default_value = "127.0.0.1:8083"
    )]
    transport_internal_address: SocketAddr,
    #[arg(
        long,
        env = "IOT_MQTTD_INTERNAL_V311_ADDRESS",
        default_value = "127.0.0.1:18831"
    )]
    v311_backend_address: SocketAddr,
    #[arg(
        long,
        env = "IOT_MQTTD_INTERNAL_V5_ADDRESS",
        default_value = "127.0.0.1:18832"
    )]
    v5_backend_address: SocketAddr,
    #[arg(
        long,
        env = "IOT_MQTTD_DEVICE_BACKEND_ADDRESS",
        default_value = "127.0.0.1:18833"
    )]
    device_backend_address: SocketAddr,
    #[arg(
        long,
        env = "IOT_MQTTD_DEVICE_V5_BACKEND_ADDRESS",
        default_value = "127.0.0.1:18834"
    )]
    device_v5_backend_address: SocketAddr,
    #[arg(long, env = "IOT_MQTTD_MAX_CONNECTIONS", default_value_t = 10_000)]
    max_connections: usize,
    #[arg(long, env = "IOT_MQTTD_API_BASE_URL")]
    api_base_url: Option<String>,
    #[arg(long, env = "IOT_NANO_MQTTD_API_SECRET")]
    mqttd_api_secret: Option<String>,
    #[arg(long, env = "IOT_NANO_API_MQTTD_SECRET")]
    api_mqttd_secret: Option<String>,
    #[arg(long, env = "IOT_NANO_CORE_MQTTD_SECRET")]
    core_mqttd_secret: Option<String>,
    #[arg(long, env = "IOT_NANO_STREAM_URL")]
    stream_url: Option<String>,
    #[arg(long, env = "IOT_NANO_MQTTD_STREAM_SECRET")]
    stream_secret: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "Rustls crypto provider was already initialized incompatibly")?;
    let arguments = Arguments::parse();
    let file_config = arguments
        .config
        .as_ref()
        .map(BrokerFileConfig::from_path)
        .transpose()?;
    let tls_cert_path = arguments.tls_cert_path.clone().or_else(|| {
        file_config
            .as_ref()
            .map(|config| config.listeners.tls.certificate_path.clone())
    });
    let tls_key_path = arguments.tls_key_path.clone().or_else(|| {
        file_config
            .as_ref()
            .map(|config| config.listeners.tls.key_path.clone())
    });
    if file_config.is_none() && (tls_cert_path.is_none() || tls_key_path.is_none()) {
        return Err("TLS certificate and key paths are required without --config".into());
    }
    let mut resolved = resolve_runtime_configuration(
        file_config.as_ref(),
        RuntimeAddressConfiguration {
            plaintext_address: arguments.plaintext_address,
            tls_address: arguments.tls_address,
            v311_backend_address: arguments.v311_backend_address,
            v5_backend_address: arguments.v5_backend_address,
            device_backend_address: arguments.device_backend_address,
            device_v5_backend_address: arguments.device_v5_backend_address,
            tls_cert_path: tls_cert_path.unwrap_or_default(),
            tls_key_path: tls_key_path.unwrap_or_default(),
            max_connections: arguments.max_connections,
            ..RuntimeAddressConfiguration::default()
        },
    )?;
    let endpoints = DeviceTransportEndpoints {
        api_base_url: arguments.api_base_url.clone(),
        transport_secret: arguments.mqttd_api_secret.clone(),
    };
    let device_transport_enabled = resolved.device_transport_activation(&endpoints)?;
    resolved.device_transport_enabled = device_transport_enabled;
    resolved.set_device_transport_backends(device_transport_enabled);
    let policy_wired = file_config.as_ref().is_some_and(|config| {
        config
            .static_acl
            .as_ref()
            .is_some_and(|policy| policy.enabled)
            || config
                .http_authorization
                .as_ref()
                .is_some_and(|policy| policy.enabled)
    });
    if let Some(policy) = file_config.as_ref().map(build_policy).transpose()? {
        resolved.listener.auth_handler = policy.auth_handler;
        resolved.listener.authorization_handler = policy.authorization_handler;
    }
    let broker = match file_config.as_ref().map(|config| &config.storage) {
        Some(iot_nano_mqttd::StorageConfig::Sqlite { path, .. }) => {
            let storage = std::sync::Arc::new(SqliteStorage::open(path)?);
            let policy = file_config
                .as_ref()
                .expect("sqlite storage requires config")
                .storage
                .retention_policy();
            iot_nano_mqttd::start_broker_with_storage_and_policy(
                resolved.listener.clone(),
                storage,
                policy,
            )
            .await?
        }
        _ => start_broker(resolved.listener.clone()).await?,
    };
    if let Some(config) = &file_config {
        for rule in config.rules.iter().filter(|rule| rule.enabled) {
            broker.spawn_republish_rule_worker(rule.clone())?;
        }
    }
    if device_transport_enabled {
        let api_base_url = endpoints
            .api_base_url
            .as_deref()
            .expect("validated endpoint");
        let mqttd_api_secret = endpoints
            .transport_secret
            .as_deref()
            .expect("validated endpoint");
        let api_mqttd_secret = arguments
            .api_mqttd_secret
            .as_deref()
            .ok_or("IOT_NANO_API_MQTTD_SECRET is required for device transport")?;
        let core_mqttd_secret = arguments
            .core_mqttd_secret
            .as_deref()
            .ok_or("IOT_NANO_CORE_MQTTD_SECRET is required for device transport")?;
        let authenticator = HttpDeviceAuthenticator::new(api_base_url, mqttd_api_secret)?;
        let stream_uplink = match (&arguments.stream_url, &arguments.stream_secret) {
            (Some(url), Some(stream_secret)) => HttpStreamUplinkForwarder::new(url, stream_secret)?
                .with_gateway_authorization(api_base_url, mqttd_api_secret)?,
            (None, None) => {
                return Err(
                    "IOT_NANO_STREAM_URL and IOT_NANO_STREAM_SECRET are required for device transport"
                        .into(),
                );
            }
            _ => {
                return Err(
                    "IOT_NANO_STREAM_URL and IOT_NANO_STREAM_SECRET must be supplied together"
                        .into(),
                );
            }
        };
        let responses = HttpRpcResponseForwarder::new(api_base_url, mqttd_api_secret)?;
        let device_transport = MqttdDeviceTransport::new(authenticator, stream_uplink)
            .with_rpc_response_forwarder(responses);
        start_internal_server(
            arguments.transport_internal_address,
            device_transport.internal_router(api_mqttd_secret, core_mqttd_secret),
        )
        .await?;
        if let Some(address) = resolved.backends.device_v311 {
            let listener = tokio::net::TcpListener::bind(address).await?;
            let transport = device_transport.clone();
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let transport = transport.clone();
                            tokio::spawn(async move {
                                if let Err(error) = transport.serve_connection(stream).await {
                                    eprintln!("iot-mqttd device transport error: {error}");
                                }
                            });
                        }
                        Err(error) => {
                            eprintln!("iot-mqttd device backend accept error: {error}");
                            return;
                        }
                    }
                }
            });
        }
        if let Some(address) = resolved.backends.device_v5 {
            let listener = tokio::net::TcpListener::bind(address).await?;
            let transport = device_transport;
            tokio::spawn(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            let transport = transport.clone();
                            tokio::spawn(async move {
                                if let Err(error) = transport.serve_v5_connection(stream).await {
                                    eprintln!("iot-mqttd MQTT5 device transport error: {error}");
                                }
                            });
                        }
                        Err(error) => {
                            eprintln!("iot-mqttd MQTT5 device backend accept error: {error}");
                            return;
                        }
                    }
                }
            });
        }
    }
    let plaintext_listener = std::net::TcpListener::bind(resolved.listener.plaintext_address)?;
    let tls_listener = std::net::TcpListener::bind(resolved.listener.tls_address)?;
    let tls_acceptor = load_tls_acceptor(
        &resolved.listener.tls_cert_path,
        &resolved.listener.tls_key_path,
    )?;
    broker.spawn_public_plaintext_mux(
        plaintext_listener,
        resolved.backends,
        iot_nano_mqttd::MuxSettings::default(),
    )?;
    broker.spawn_public_tls_mux(
        tls_listener,
        tls_acceptor,
        resolved.backends,
        iot_nano_mqttd::MuxSettings::default(),
    )?;
    let management_address = file_config
        .as_ref()
        .map_or(arguments.management_address, |config| {
            config.management.address
        });
    let management = file_config.map_or_else(management_router, |config| {
        management_router_with_config_and_runtime_and_policy(
            config,
            device_transport_enabled,
            policy_wired,
        )
    });
    let listener = tokio::net::TcpListener::bind(management_address).await?;
    axum::serve(listener, management).await?;
    Ok(())
}

async fn start_internal_server(address: SocketAddr, app: Router) -> Result<(), std::io::Error> {
    let app = app.route("/healthz", get(|| async { "ok\n" }));
    let listener = tokio::net::TcpListener::bind(address).await?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("iot-mqttd internal transport server stopped: {error}");
        }
    });
    Ok(())
}
