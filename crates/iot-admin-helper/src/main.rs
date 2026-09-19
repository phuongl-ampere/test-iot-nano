use std::{
    env,
    io::{self, Read},
    path::PathBuf,
    process::{Command, ExitCode},
};

use clap::{Parser, Subcommand};
use iot_admin_helper::{HelperError, apply_config_files_with_api, read_config_files_with_api};
use iot_nano_foundation::SystemConfigurationUpdate;

const DEFAULT_INGEST_ENV_PATH: &str = "/etc/rush-iot-nano/ingest.env";
const DEFAULT_SMTP_ENV_PATH: &str = "/etc/rush-iot-nano/smtp.env";
const DEFAULT_API_ENV_PATH: &str = "/etc/rush-iot-nano/api.env";

#[derive(Debug, Parser)]
#[command(about = "Root-owned Rush IoT Nano system configuration helper")]
struct Arguments {
    #[command(subcommand)]
    command: HelperCommand,
}

#[derive(Debug, Subcommand)]
enum HelperCommand {
    Read,
    Apply,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            match error {
                HelperError::Configuration(_) | HelperError::Environment(_) => ExitCode::from(2),
                HelperError::Io(_) => ExitCode::from(1),
            }
        }
    }
}

fn run() -> Result<(), HelperError> {
    match Arguments::parse().command {
        HelperCommand::Read => {
            let configuration =
                read_config_files_with_api(&ingest_env_path(), &smtp_env_path(), &api_env_path())?;
            println!(
                "{}",
                serde_json::to_string(&configuration)
                    .map_err(|error| HelperError::Environment(error.to_string()))?
            );
        }
        HelperCommand::Apply => {
            let mut input = String::new();
            io::stdin().read_to_string(&mut input)?;
            let update: SystemConfigurationUpdate = serde_json::from_str(&input)
                .map_err(|error| HelperError::Environment(error.to_string()))?;
            let smtp_path = smtp_env_path();
            let configuration = apply_config_files_with_api(
                &ingest_env_path(),
                &smtp_path,
                &api_env_path(),
                &update,
            )?;
            set_sensitive_file_owner(&smtp_path)?;
            set_sensitive_file_owner(&api_env_path())?;
            println!(
                "{}",
                serde_json::to_string(&configuration)
                    .map_err(|error| HelperError::Environment(error.to_string()))?
            );
        }
    }
    Ok(())
}

fn ingest_env_path() -> PathBuf {
    env::var_os("IOT_INGEST_ENV_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_INGEST_ENV_PATH))
}

fn smtp_env_path() -> PathBuf {
    env::var_os("IOT_SMTP_CONFIG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SMTP_ENV_PATH))
}

fn api_env_path() -> PathBuf {
    env::var_os("IOT_API_ENV_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_API_ENV_PATH))
}

fn set_sensitive_file_owner(path: &PathBuf) -> Result<(), HelperError> {
    if env::var_os("IOT_SYSTEM_CONFIGURATION_DIRECT").is_some() {
        return Ok(());
    }
    let status = Command::new("/usr/bin/chown")
        .arg("root:iot")
        .arg(path)
        .status()?;
    if !status.success() {
        return Err(HelperError::Environment(
            "cannot set smtp.env ownership to root:iot".to_owned(),
        ));
    }
    Ok(())
}
