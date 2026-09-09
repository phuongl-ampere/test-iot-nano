use std::{
    error::Error,
    fs::File,
    io::BufReader,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use axum::{Router, response::IntoResponse, routing::get};
use clap::Parser;
use iot_mqtt_transport::{
    HttpDeviceAuthenticator, HttpRpcResponseForwarder, HttpUplinkForwarder, MqttTransport,
};
use tokio::net::TcpListener;
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};

#[derive(Debug, Parser)]
#[command(about = "Token-aware MQTT transport with ThingsBoard-style virtual RPC topics")]
struct Arguments {
    #[arg(
        long,
        env = "IOT_MQTT_TRANSPORT_ADDRESS",
        default_value = "0.0.0.0:8883"
    )]
    address: SocketAddr,
    #[arg(
        long,
        env = "IOT_MQTT_TRANSPORT_INTERNAL_ADDRESS",
        default_value = "127.0.0.1:8083"
    )]
    internal_address: SocketAddr,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_TLS_CERT_PATH")]
    tls_cert: PathBuf,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_TLS_KEY_PATH")]
    tls_key: PathBuf,
    #[arg(
        long,
        env = "IOT_MQTT_TRANSPORT_API_BASE_URL",
        default_value = "http://127.0.0.1:8080"
    )]
    api_base_url: String,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_SECRET")]
    transport_secret: String,
    #[arg(
        long,
        env = "IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_URL",
        default_value = "http://127.0.0.1:8081/internal/mqtt-transport/telemetry"
    )]
    ingest_webhook_url: String,
    #[arg(long, env = "IOT_MQTT_TRANSPORT_INGEST_WEBHOOK_SECRET")]
    ingest_webhook_secret: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    tokio_rustls::rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "Rustls crypto provider was already initialized incompatibly")?;
    let arguments = Arguments::parse();
    validate_secret(&arguments.transport_secret, "IOT_MQTT_TRANSPORT_SECRET")?;
    validate_secret(
        &arguments.ingest_webhook_secret,
        "IOT_NANOMQ_WEBHOOK_SECRET",
    )?;

    let authenticator =
        HttpDeviceAuthenticator::new(&arguments.api_base_url, &arguments.transport_secret)?;
    let uplink = HttpUplinkForwarder::new(
        arguments.ingest_webhook_url,
        &arguments.ingest_webhook_secret,
    )?;
    let rpc_responses =
        HttpRpcResponseForwarder::new(&arguments.api_base_url, &arguments.transport_secret)?;
    let transport =
        MqttTransport::new(authenticator, uplink).with_rpc_response_forwarder(rpc_responses);
    start_internal_server(
        arguments.internal_address,
        transport.internal_router(&arguments.transport_secret),
    )
    .await?;

    let tls = TlsAcceptor::from(Arc::new(load_tls_config(
        &arguments.tls_cert,
        &arguments.tls_key,
    )?));
    let listener = TcpListener::bind(arguments.address).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let tls = tls.clone();
        let transport = transport.clone();
        tokio::spawn(async move {
            let stream = match tls.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("MQTT TLS handshake error: {error}");
                    return;
                }
            };
            if let Err(error) = transport.serve_connection(stream).await {
                eprintln!("MQTT transport connection error: {error}");
            }
        });
    }
}

async fn start_internal_server(address: SocketAddr, app: Router) -> Result<(), std::io::Error> {
    let app = app.route("/healthz", get(healthz));
    let listener = TcpListener::bind(address).await?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("MQTT transport internal server error: {error}");
        }
    });
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    "ok\n"
}

fn validate_secret(value: &str, name: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    if value.len() < 32 || !value.is_ascii() || value.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(format!("{name} must be at least 32 ASCII non-whitespace characters").into());
    }
    Ok(())
}

fn load_tls_config(
    certificate_path: &Path,
    private_key_path: &Path,
) -> Result<ServerConfig, Box<dyn Error + Send + Sync>> {
    let mut certificate = BufReader::new(File::open(certificate_path)?);
    let certificates = rustls_pemfile::certs(&mut certificate).collect::<Result<Vec<_>, _>>()?;
    let mut private_key = BufReader::new(File::open(private_key_path)?);
    let private_key = rustls_pemfile::private_key(&mut private_key)?
        .ok_or("IOT_MQTT_TRANSPORT_TLS_KEY contains no private key")?;
    Ok(ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)?)
}
