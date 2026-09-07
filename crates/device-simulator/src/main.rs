use std::{process::ExitCode, time::Duration};

use clap::Parser;
use device_simulator::{SimulationConfig, publish_simulation};

#[derive(Debug, Parser)]
#[command(about = "Publish deterministic MQTT telemetry for local load testing")]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1")]
    broker_host: String,
    #[arg(long, default_value_t = 1883)]
    broker_port: u16,
    #[arg(long, default_value_t = 1_000)]
    devices: u32,
    #[arg(long, default_value_t = 10)]
    messages_per_device: u64,
    #[arg(long, default_value_t = 6_000)]
    interval_ms: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let arguments = Arguments::parse();
    let config = SimulationConfig {
        broker_host: arguments.broker_host,
        broker_port: arguments.broker_port,
        devices: arguments.devices,
        messages_per_device: arguments.messages_per_device,
        interval: Duration::from_millis(arguments.interval_ms),
    };

    match publish_simulation(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("device simulator failed: {error}");
            ExitCode::FAILURE
        }
    }
}
