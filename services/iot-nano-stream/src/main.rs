use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::Parser;
use iot_nano_stream::{LocalStream, StreamConfig, http::StreamHttpState};
use tokio::time::{MissedTickBehavior, interval};

#[derive(Debug, Parser)]
#[command(about = "Rush IoT Nano single-node durable stream")]
struct Arguments {
    #[arg(
        long,
        env = "IOT_NANO_STREAM_ADDRESS",
        default_value = "127.0.0.1:8090"
    )]
    address: SocketAddr,
    #[arg(long, env = "IOT_NANO_STREAM_DIR")]
    stream_dir: PathBuf,
    #[arg(long, env = "IOT_NANO_MQTTD_STREAM_SECRET")]
    mqttd_secret: String,
    #[arg(long, env = "IOT_NANO_CORE_STREAM_SECRET")]
    core_secret: String,
    #[arg(long, env = "IOT_NANO_STREAM_PARTITIONS", default_value_t = 8)]
    partitions: u16,
    #[arg(
        long,
        env = "IOT_NANO_STREAM_SEGMENT_BYTES",
        default_value_t = 128 * 1024 * 1024
    )]
    segment_bytes: u64,
    #[arg(
        long,
        env = "IOT_NANO_STREAM_RETENTION_BYTES",
        default_value_t = 2 * 1024 * 1024 * 1024
    )]
    retention_bytes: u64,
    #[arg(
        long,
        env = "IOT_NANO_STREAM_RETENTION_SECONDS",
        default_value_t = 24 * 60 * 60
    )]
    retention_seconds: u64,
    #[arg(
        long,
        env = "IOT_NANO_STREAM_MAX_RECORD_BYTES",
        default_value_t = 1024 * 1024
    )]
    max_record_bytes: usize,
    #[arg(
        long,
        env = "IOT_NANO_STREAM_RETENTION_INTERVAL_SECONDS",
        default_value_t = 60
    )]
    retention_interval_seconds: u64,
}

impl Arguments {
    fn stream_config(&self) -> StreamConfig {
        StreamConfig {
            partition_count: self.partitions,
            segment_max_bytes: self.segment_bytes,
            retention_max_bytes: self.retention_bytes,
            retention_max_age: Duration::from_secs(self.retention_seconds),
            max_record_bytes: self.max_record_bytes,
            index_stride: 128,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = Arguments::parse();
    validate_secret(&arguments.mqttd_secret)?;
    validate_secret(&arguments.core_secret)?;
    if arguments.retention_interval_seconds == 0 {
        return Err("IOT_NANO_STREAM_RETENTION_INTERVAL_SECONDS must be positive".into());
    }
    let stream = LocalStream::open(&arguments.stream_dir, arguments.stream_config())?;
    spawn_retention_worker(
        stream.clone(),
        Duration::from_secs(arguments.retention_interval_seconds),
    );
    let listener = tokio::net::TcpListener::bind(arguments.address).await?;
    axum::serve(
        listener,
        iot_nano_stream::http::router(StreamHttpState::new(
            stream,
            arguments.mqttd_secret,
            arguments.core_secret,
        )),
    )
    .await?;
    Ok(())
}

fn spawn_retention_worker(stream: LocalStream, retention_interval: Duration) {
    tokio::spawn(async move {
        let mut tick = interval(retention_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            match iot_nano_stream::maintenance::enforce_retention(stream.clone()).await {
                Ok(result) if result.deleted_segments > 0 => {
                    eprintln!(
                        "stream retention deleted {} segments and {} bytes",
                        result.deleted_segments, result.deleted_bytes
                    );
                }
                Ok(_) => {}
                Err(error) => eprintln!("stream retention error: {error}"),
            }
        }
    });
}

fn validate_secret(secret: &str) -> Result<(), &'static str> {
    if secret.len() < 32
        || !secret.is_ascii()
        || secret.bytes().any(|value| value.is_ascii_whitespace())
    {
        return Err("IOT_NANO_STREAM_SECRET must be at least 32 ASCII non-whitespace characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, path::PathBuf};

    use super::Arguments;

    #[test]
    fn stream_configuration_uses_service_owned_retention_tuning() {
        let arguments = Arguments {
            address: "127.0.0.1:8090".parse::<SocketAddr>().unwrap(),
            stream_dir: PathBuf::from("/tmp/stream"),
            mqttd_secret: "mqttd-stream-secret-must-have-at-least-32".to_owned(),
            core_secret: "core-stream-secret-must-have-at-least-32-xx".to_owned(),
            retention_interval_seconds: 60,
            partitions: 2,
            segment_bytes: 512,
            retention_bytes: 2_048,
            retention_seconds: 120,
            max_record_bytes: 128,
        };

        let config = arguments.stream_config();

        assert_eq!(config.partition_count, 2);
        assert_eq!(config.segment_max_bytes, 512);
        assert_eq!(config.retention_max_bytes, 2_048);
        assert_eq!(
            config.retention_max_age,
            std::time::Duration::from_secs(120)
        );
        assert_eq!(config.max_record_bytes, 128);
    }
}
