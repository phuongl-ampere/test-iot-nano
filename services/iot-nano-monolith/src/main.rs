use std::io;

use clap::Parser;
use iot_nano_monolith::MonolithConfig;

#[derive(Debug, Parser)]
#[command(name = "iot-nano-monolith")]
struct Arguments {
    #[arg(long, conflicts_with = "migrate_only")]
    config_check: bool,
    #[arg(long)]
    migrate_only: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let arguments = Arguments::parse();
    let _configuration = MonolithConfig::from_env()?;

    if arguments.config_check {
        return Ok(());
    }

    if arguments.migrate_only {
        return Err(io::Error::other(
            "migrations are not available until platform storage is wired",
        )
        .into());
    }

    Err(io::Error::other("runtime is not available until monolith composition is wired").into())
}
