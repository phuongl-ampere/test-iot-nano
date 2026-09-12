use serde_json::json;
use sqlx::{PgPool, SqlitePool};
use uuid::Uuid;

pub const POWER_SWITCHER_PROFILE_NAME: &str = "PowerSwitcher";

fn telemetry_schema() -> serde_json::Value {
    json!({
        "switch_state": { "type": "boolean" },
        "relay_state": { "type": "boolean" },
        "voltage_v": { "type": "number", "unit": "V" },
        "current_a": { "type": "number", "unit": "A" },
        "power_w": { "type": "number", "unit": "W" },
        "energy_kwh": { "type": "number", "unit": "kWh" }
    })
}

fn metric_mapping() -> serde_json::Value {
    json!({
        "state": "switch_state",
        "power": "power_w",
        "energy": "energy_kwh"
    })
}

fn reporting_settings() -> serde_json::Value {
    json!({
        "control": { "kind": "power_switcher" },
        "rpc": {
            "methods": ["switch_on", "switch_off", "set_power"],
            "two_way_supported": true
        }
    })
}

pub async fn bootstrap_power_switcher_profile(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO device_profiles (
            id, name, telemetry_schema, metric_mapping, reporting_settings
         ) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (name) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .bind(POWER_SWITCHER_PROFILE_NAME)
    .bind(sqlx::types::Json(telemetry_schema()))
    .bind(sqlx::types::Json(metric_mapping()))
    .bind(sqlx::types::Json(reporting_settings()))
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn bootstrap_power_switcher_profile_sqlite(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT OR IGNORE INTO device_profiles (
            id, name, telemetry_schema, metric_mapping, reporting_settings
         ) VALUES (?, ?, ?, ?, ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(POWER_SWITCHER_PROFILE_NAME)
    .bind(telemetry_schema().to_string())
    .bind(metric_mapping().to_string())
    .bind(reporting_settings().to_string())
    .execute(pool)
    .await?;
    Ok(())
}
