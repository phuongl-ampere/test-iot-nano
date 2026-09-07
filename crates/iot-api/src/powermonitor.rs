use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, ToSchema)]
pub enum PowerBucket {
    Raw,
    FiveMinutes,
    OneHour,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PowerSummary {
    pub device_count: i64,
    pub online_device_count: i64,
    pub asset_count: i64,
    pub total_power_w: f64,
    pub total_energy_kwh: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PowerAsset {
    pub id: Uuid,
    pub name: String,
    pub asset_profile_id: Option<Uuid>,
    pub parent_asset_id: Option<Uuid>,
    pub metadata: Value,
    pub device_count: i64,
    pub total_power_w: f64,
    pub total_energy_kwh: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PowerDevice {
    pub device_id: String,
    pub display_name: Option<String>,
    pub asset_id: Option<Uuid>,
    pub device_profile_id: Option<Uuid>,
    pub online: bool,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub is_gateway: bool,
    pub gateway_device_id: Option<String>,
    pub gateway_status: Option<String>,
    pub child_status: Option<String>,
    pub voltage_v: Option<f64>,
    pub current_a: Option<f64>,
    pub power_w: Option<f64>,
    pub energy_kwh: Option<f64>,
    pub frequency_hz: Option<f64>,
    pub power_factor: Option<f64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PowerTelemetryPoint {
    pub at: DateTime<Utc>,
    pub voltage_v: Option<f64>,
    pub current_a: Option<f64>,
    pub power_w: Option<f64>,
    pub energy_kwh: Option<f64>,
    pub frequency_hz: Option<f64>,
    pub power_factor: Option<f64>,
    pub event_count: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PowerTelemetryRecord {
    pub at: DateTime<Utc>,
    pub measurements: Value,
}

pub async fn summary(
    pool: &PgPool,
    online_after: DateTime<Utc>,
) -> Result<PowerSummary, sqlx::Error> {
    let row = sqlx::query(
        "WITH latest AS (
            SELECT DISTINCT ON (device_id) device_id, measurements
            FROM telemetry
            ORDER BY device_id, event_at DESC
         )
         SELECT
            COUNT(devices.device_id) AS device_count,
            COUNT(devices.device_id) FILTER (
                WHERE (
                    devices.is_gateway = TRUE
                    AND devices.last_seen_at >= $1
                ) OR (
                    devices.gateway_device_id IS NOT NULL
                    AND devices.gateway_read_quality IS DISTINCT FROM 'unavailable'
                    AND devices.gateway_last_read_at >= $1
                ) OR (
                    devices.is_gateway = FALSE
                    AND devices.gateway_device_id IS NULL
                    AND devices.last_seen_at >= $1
                )
            ) AS online_device_count,
            (SELECT COUNT(*) FROM assets) AS asset_count,
            COALESCE(SUM(
                CASE WHEN jsonb_typeof(latest.measurements -> 'power_w') = 'number'
                     THEN (latest.measurements ->> 'power_w')::double precision END
            ), 0) AS total_power_w,
            COALESCE(SUM(
                CASE WHEN jsonb_typeof(latest.measurements -> 'energy_kwh') = 'number'
                     THEN (latest.measurements ->> 'energy_kwh')::double precision END
            ), 0) AS total_energy_kwh
         FROM devices
         LEFT JOIN latest ON latest.device_id = devices.device_id
         WHERE devices.deleted_at IS NULL",
    )
    .bind(online_after)
    .fetch_one(pool)
    .await?;
    Ok(PowerSummary {
        device_count: row.try_get("device_count")?,
        online_device_count: row.try_get("online_device_count")?,
        asset_count: row.try_get("asset_count")?,
        total_power_w: row.try_get("total_power_w")?,
        total_energy_kwh: row.try_get("total_energy_kwh")?,
    })
}

pub async fn list_assets(pool: &PgPool) -> Result<Vec<PowerAsset>, sqlx::Error> {
    let rows = sqlx::query(
        "WITH RECURSIVE descendants(root_id, asset_id) AS (
            SELECT id, id
            FROM assets
            UNION
            SELECT descendants.root_id, children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.asset_id
         ),
         latest AS (
            SELECT DISTINCT ON (device_id) device_id, measurements
            FROM telemetry
            ORDER BY device_id, event_at DESC
         )
         SELECT
            assets.id, assets.name, assets.asset_profile_id, assets.parent_asset_id,
            assets.metadata,
            COUNT(devices.device_id) AS device_count,
            COALESCE(SUM(
                CASE WHEN jsonb_typeof(latest.measurements -> 'power_w') = 'number'
                     THEN (latest.measurements ->> 'power_w')::double precision END
            ), 0) AS total_power_w,
            COALESCE(SUM(
                CASE WHEN jsonb_typeof(latest.measurements -> 'energy_kwh') = 'number'
                     THEN (latest.measurements ->> 'energy_kwh')::double precision END
            ), 0) AS total_energy_kwh
         FROM assets
         LEFT JOIN descendants ON descendants.root_id = assets.id
         LEFT JOIN devices
           ON devices.asset_id = descendants.asset_id AND devices.deleted_at IS NULL
         LEFT JOIN latest ON latest.device_id = devices.device_id
         GROUP BY assets.id
         ORDER BY assets.name, assets.id",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(PowerAsset {
                id: row.try_get("id")?,
                name: row.try_get("name")?,
                asset_profile_id: row.try_get("asset_profile_id")?,
                parent_asset_id: row.try_get("parent_asset_id")?,
                metadata: row.try_get("metadata")?,
                device_count: row.try_get("device_count")?,
                total_power_w: row.try_get("total_power_w")?,
                total_energy_kwh: row.try_get("total_energy_kwh")?,
            })
        })
        .collect()
}

pub async fn list_devices(
    pool: &PgPool,
    online_after: DateTime<Utc>,
) -> Result<Vec<PowerDevice>, sqlx::Error> {
    let rows = sqlx::query(
        "WITH latest AS (
            SELECT DISTINCT ON (device_id) device_id, measurements
            FROM telemetry
            ORDER BY device_id, event_at DESC
         )
         SELECT
            devices.device_id, devices.display_name, devices.asset_id, devices.device_profile_id,
            devices.last_seen_at,
            devices.is_gateway, devices.gateway_device_id, devices.gateway_last_read_at,
            devices.gateway_read_quality,
            CASE WHEN jsonb_typeof(latest.measurements -> 'voltage_v') = 'number'
                 THEN (latest.measurements ->> 'voltage_v')::double precision END AS voltage_v,
            CASE WHEN jsonb_typeof(latest.measurements -> 'current_a') = 'number'
                 THEN (latest.measurements ->> 'current_a')::double precision END AS current_a,
            CASE WHEN jsonb_typeof(latest.measurements -> 'power_w') = 'number'
                 THEN (latest.measurements ->> 'power_w')::double precision END AS power_w,
            CASE WHEN jsonb_typeof(latest.measurements -> 'energy_kwh') = 'number'
                 THEN (latest.measurements ->> 'energy_kwh')::double precision END AS energy_kwh,
            CASE WHEN jsonb_typeof(latest.measurements -> 'frequency_hz') = 'number'
                 THEN (latest.measurements ->> 'frequency_hz')::double precision END AS frequency_hz,
            CASE WHEN jsonb_typeof(latest.measurements -> 'power_factor') = 'number'
                 THEN (latest.measurements ->> 'power_factor')::double precision END AS power_factor
         FROM devices
         LEFT JOIN latest ON latest.device_id = devices.device_id
         WHERE devices.deleted_at IS NULL
         ORDER BY devices.device_id",
    )
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            let last_seen_at = row.try_get::<Option<DateTime<Utc>>, _>("last_seen_at")?;
            let is_gateway = row.try_get::<bool, _>("is_gateway")?;
            let gateway_device_id = row.try_get::<Option<String>, _>("gateway_device_id")?;
            let gateway_last_read_at =
                row.try_get::<Option<DateTime<Utc>>, _>("gateway_last_read_at")?;
            let gateway_read_quality = row.try_get::<Option<String>, _>("gateway_read_quality")?;
            let (online, gateway_status, child_status, last_seen_at) = device_health(
                is_gateway,
                gateway_device_id.as_deref(),
                last_seen_at,
                gateway_last_read_at,
                gateway_read_quality.as_deref(),
                online_after,
            );
            Ok(PowerDevice {
                device_id: row.try_get("device_id")?,
                display_name: row.try_get("display_name")?,
                asset_id: row.try_get("asset_id")?,
                device_profile_id: row.try_get("device_profile_id")?,
                online,
                last_seen_at,
                is_gateway,
                gateway_device_id,
                gateway_status,
                child_status,
                voltage_v: row.try_get("voltage_v")?,
                current_a: row.try_get("current_a")?,
                power_w: row.try_get("power_w")?,
                energy_kwh: row.try_get("energy_kwh")?,
                frequency_hz: row.try_get("frequency_hz")?,
                power_factor: row.try_get("power_factor")?,
            })
        })
        .collect()
}

fn device_health(
    is_gateway: bool,
    gateway_device_id: Option<&str>,
    last_seen_at: Option<DateTime<Utc>>,
    gateway_last_read_at: Option<DateTime<Utc>>,
    gateway_read_quality: Option<&str>,
    fresh_after: DateTime<Utc>,
) -> (bool, Option<String>, Option<String>, Option<DateTime<Utc>>) {
    if is_gateway {
        let status = if last_seen_at.is_some_and(|seen| seen >= fresh_after) {
            "online"
        } else {
            "offline"
        };
        return (
            status == "online",
            Some(status.to_owned()),
            None,
            last_seen_at,
        );
    }
    if gateway_device_id.is_some() {
        let unavailable_after = fresh_after - chrono::Duration::minutes(10);
        let status = if gateway_read_quality == Some("unavailable") {
            "unavailable"
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= fresh_after) {
            "fresh"
        } else if gateway_last_read_at.is_some_and(|read_at| read_at >= unavailable_after) {
            "stale"
        } else {
            "unavailable"
        };
        return (
            status == "fresh",
            None,
            Some(status.to_owned()),
            gateway_last_read_at,
        );
    }
    (
        last_seen_at.is_some_and(|seen| seen >= fresh_after),
        None,
        None,
        last_seen_at,
    )
}

pub async fn device_telemetry(
    pool: &PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: PowerBucket,
) -> Result<Vec<PowerTelemetryPoint>, sqlx::Error> {
    match bucket {
        PowerBucket::Raw => raw_device_telemetry(pool, device_id, from, to).await,
        PowerBucket::FiveMinutes => {
            bucketed_device_telemetry(pool, device_id, from, to, "5 minutes").await
        }
        PowerBucket::OneHour => {
            bucketed_device_telemetry(pool, device_id, from, to, "1 hour").await
        }
    }
}

pub async fn device_telemetry_records(
    pool: &PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryRecord>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT event_at AS at, measurements
         FROM telemetry
         WHERE device_id = $1 AND event_at >= $2 AND event_at <= $3
         ORDER BY event_at DESC
         LIMIT 200",
    )
    .bind(device_id)
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(PowerTelemetryRecord {
                at: row.try_get("at")?,
                measurements: row.try_get("measurements")?,
            })
        })
        .collect()
}

pub async fn asset_telemetry(
    pool: &PgPool,
    asset_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    bucket: PowerBucket,
) -> Result<Vec<PowerTelemetryPoint>, sqlx::Error> {
    match bucket {
        PowerBucket::Raw => raw_asset_telemetry(pool, asset_id, from, to).await,
        PowerBucket::FiveMinutes => {
            bucketed_asset_telemetry(pool, asset_id, from, to, "5 minutes").await
        }
        PowerBucket::OneHour => bucketed_asset_telemetry(pool, asset_id, from, to, "1 hour").await,
    }
}

async fn raw_device_telemetry(
    pool: &PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryPoint>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT
            event_at AS at,
            CASE WHEN jsonb_typeof(measurements -> 'voltage_v') = 'number'
                 THEN (measurements ->> 'voltage_v')::double precision END AS voltage_v,
            CASE WHEN jsonb_typeof(measurements -> 'current_a') = 'number'
                 THEN (measurements ->> 'current_a')::double precision END AS current_a,
            CASE WHEN jsonb_typeof(measurements -> 'power_w') = 'number'
                 THEN (measurements ->> 'power_w')::double precision END AS power_w,
            CASE WHEN jsonb_typeof(measurements -> 'energy_kwh') = 'number'
                 THEN (measurements ->> 'energy_kwh')::double precision END AS energy_kwh,
            CASE WHEN jsonb_typeof(measurements -> 'frequency_hz') = 'number'
                 THEN (measurements ->> 'frequency_hz')::double precision END AS frequency_hz,
            CASE WHEN jsonb_typeof(measurements -> 'power_factor') = 'number'
                 THEN (measurements ->> 'power_factor')::double precision END AS power_factor,
            1::bigint AS event_count
         FROM telemetry
         WHERE device_id = $1
           AND event_at >= $2
           AND event_at <= $3
         ORDER BY event_at",
    )
    .bind(device_id)
    .bind(from)
    .bind(to)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(power_point_from_row).collect()
}

async fn bucketed_device_telemetry(
    pool: &PgPool,
    device_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    interval: &str,
) -> Result<Vec<PowerTelemetryPoint>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT
            time_bucket($4::interval, event_at) AS at,
            avg(CASE WHEN jsonb_typeof(measurements -> 'voltage_v') = 'number'
                     THEN (measurements ->> 'voltage_v')::double precision END) AS voltage_v,
            avg(CASE WHEN jsonb_typeof(measurements -> 'current_a') = 'number'
                     THEN (measurements ->> 'current_a')::double precision END) AS current_a,
            avg(CASE WHEN jsonb_typeof(measurements -> 'power_w') = 'number'
                     THEN (measurements ->> 'power_w')::double precision END) AS power_w,
            max(CASE WHEN jsonb_typeof(measurements -> 'energy_kwh') = 'number'
                     THEN (measurements ->> 'energy_kwh')::double precision END) AS energy_kwh,
            avg(CASE WHEN jsonb_typeof(measurements -> 'frequency_hz') = 'number'
                     THEN (measurements ->> 'frequency_hz')::double precision END) AS frequency_hz,
            avg(CASE WHEN jsonb_typeof(measurements -> 'power_factor') = 'number'
                     THEN (measurements ->> 'power_factor')::double precision END) AS power_factor,
            COUNT(*) AS event_count
         FROM telemetry
         WHERE device_id = $1 AND event_at >= $2 AND event_at <= $3
         GROUP BY 1
         ORDER BY 1",
    )
    .bind(device_id)
    .bind(from)
    .bind(to)
    .bind(interval)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(power_point_from_row).collect()
}

async fn raw_asset_telemetry(
    pool: &PgPool,
    asset_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<PowerTelemetryPoint>, sqlx::Error> {
    bucketed_asset_telemetry(pool, asset_id, from, to, "1 minute").await
}

async fn bucketed_asset_telemetry(
    pool: &PgPool,
    asset_id: Uuid,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    interval: &str,
) -> Result<Vec<PowerTelemetryPoint>, sqlx::Error> {
    let rows = sqlx::query(
        "WITH RECURSIVE descendants(id) AS (
            SELECT id FROM assets WHERE id = $1
            UNION
            SELECT children.id
            FROM descendants
            JOIN assets AS children ON children.parent_asset_id = descendants.id
         ),
         per_device AS (
            SELECT
                time_bucket($4::interval, event_at) AS at,
                device_id,
                avg(CASE WHEN jsonb_typeof(measurements -> 'voltage_v') = 'number'
                         THEN (measurements ->> 'voltage_v')::double precision END) AS voltage_v,
                avg(CASE WHEN jsonb_typeof(measurements -> 'current_a') = 'number'
                         THEN (measurements ->> 'current_a')::double precision END) AS current_a,
                avg(CASE WHEN jsonb_typeof(measurements -> 'power_w') = 'number'
                         THEN (measurements ->> 'power_w')::double precision END) AS power_w,
                max(CASE WHEN jsonb_typeof(measurements -> 'energy_kwh') = 'number'
                         THEN (measurements ->> 'energy_kwh')::double precision END) AS energy_kwh,
                avg(CASE WHEN jsonb_typeof(measurements -> 'frequency_hz') = 'number'
                         THEN (measurements ->> 'frequency_hz')::double precision END) AS frequency_hz,
                avg(CASE WHEN jsonb_typeof(measurements -> 'power_factor') = 'number'
                         THEN (measurements ->> 'power_factor')::double precision END) AS power_factor,
                COUNT(*) AS event_count
            FROM telemetry
            WHERE device_id IN (
                SELECT device_id
                FROM devices
                WHERE asset_id IN (SELECT id FROM descendants)
                  AND deleted_at IS NULL
            ) AND event_at >= $2 AND event_at <= $3
            GROUP BY 1, device_id
         )
         SELECT
            at,
            avg(voltage_v) AS voltage_v,
            avg(current_a) AS current_a,
            sum(power_w) AS power_w,
            sum(energy_kwh) AS energy_kwh,
            avg(frequency_hz) AS frequency_hz,
            avg(power_factor) AS power_factor,
            sum(event_count)::bigint AS event_count
         FROM per_device
         GROUP BY at
         ORDER BY at",
    )
    .bind(asset_id)
    .bind(from)
    .bind(to)
    .bind(interval)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(power_point_from_row).collect()
}

fn power_point_from_row(row: sqlx::postgres::PgRow) -> Result<PowerTelemetryPoint, sqlx::Error> {
    Ok(PowerTelemetryPoint {
        at: row.try_get("at")?,
        voltage_v: row.try_get("voltage_v")?,
        current_a: row.try_get("current_a")?,
        power_w: row.try_get("power_w")?,
        energy_kwh: row.try_get("energy_kwh")?,
        frequency_hz: row.try_get("frequency_hz")?,
        power_factor: row.try_get("power_factor")?,
        event_count: row.try_get("event_count")?,
    })
}
