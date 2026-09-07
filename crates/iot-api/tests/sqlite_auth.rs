use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use chrono::{TimeZone, Utc};
use iot_api::{Role, SqliteApiState, bootstrap_users_sqlite, sqlite_router};
use iot_core::{DatabaseStorage, StorageConfiguration, TelemetryEvent};
use iot_storage::SqliteStore;
use serde_json::json;
use tower::ServiceExt;

#[tokio::test]
async fn sqlite_bootstrap_creates_the_default_admin_and_viewer_accounts() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();

    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT username, role FROM users ORDER BY username")
            .fetch_all(store.pool())
            .await
            .unwrap();

    assert_eq!(
        rows,
        [
            ("admin".to_owned(), Role::Admin.as_str().to_owned()),
            ("viewer".to_owned(), Role::Viewer.as_str().to_owned())
        ]
    );
}

#[tokio::test]
async fn sqlite_router_authenticates_the_bootstrapped_admin() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let session: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(session["role"], "admin");
    assert!(session["session_id"].as_str().is_some());
}

#[tokio::test]
async fn sqlite_router_lists_power_monitor_devices_from_sqlite_telemetry() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let event = TelemetryEvent {
        schema_version: 1,
        device_id: "sqlite-meter".to_owned(),
        boot_id: uuid::Uuid::new_v4(),
        sequence: 1,
        event_at: Utc.with_ymd_and_hms(2026, 9, 7, 1, 0, 0).unwrap(),
        measurements: serde_json::Map::from_iter([
            ("voltage_v".to_owned(), json!(230.4)),
            ("power_w".to_owned(), json!(529.9)),
        ]),
        gateway_device_id: None,
    };
    store
        .write_telemetry(&event, event.event_at, "topic")
        .await
        .unwrap();
    let app = sqlite_router(SqliteApiState::new(store));

    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let login_body = to_bytes(login.into_body(), usize::MAX).await.unwrap();
    let session: serde_json::Value = serde_json::from_slice(&login_body).unwrap();
    let session_id = session["session_id"].as_str().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/apps/powermonitor/devices")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let devices: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert_eq!(devices[0]["device_id"], "sqlite-meter");
    assert_eq!(devices[0]["voltage_v"], 230.4);
    assert_eq!(devices[0]["power_w"], 529.9);
}

#[tokio::test]
async fn sqlite_dashboard_lists_devices_and_returns_raw_and_rollup_telemetry() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let first_at = Utc.with_ymd_and_hms(2026, 9, 7, 1, 1, 0).unwrap();
    let second_at = Utc.with_ymd_and_hms(2026, 9, 7, 1, 3, 0).unwrap();
    for (sequence, event_at, temperature_c, humidity_pct) in
        [(1, first_at, 40.0, 50.0), (2, second_at, 42.0, 52.0)]
    {
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: "sqlite-meter".to_owned(),
            boot_id: uuid::Uuid::new_v4(),
            sequence,
            event_at,
            measurements: serde_json::Map::from_iter([
                ("temperature_c".to_owned(), json!(temperature_c)),
                ("humidity_pct".to_owned(), json!(humidity_pct)),
            ]),
            gateway_device_id: None,
        };
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap();
    }
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let devices = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/devices")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(devices.0, StatusCode::OK);
    assert_eq!(devices.1[0]["device_id"], "sqlite-meter");
    assert_eq!(devices.1[0]["display_name"], serde_json::Value::Null);
    assert!(devices.1[0]["last_seen_at"].as_str().is_some());

    for (bucket, expected_count) in [("raw", 2), ("5m", 1), ("1h", 1)] {
        let telemetry = sqlite_json_response(
            &app,
            Request::builder()
                .uri(format!(
                    "/api/devices/sqlite-meter/telemetry?from=2026-09-07T00:59:00Z&to=2026-09-07T01:05:00Z&bucket={bucket}"
                ))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;

        assert_eq!(telemetry.0, StatusCode::OK, "bucket={bucket}");
        assert_eq!(telemetry.1.as_array().unwrap().len(), expected_count);
    }

    let raw = sqlite_json_response(
        &app,
        Request::builder()
            .uri(
                "/api/devices/sqlite-meter/telemetry?from=2026-09-07T00:59:00Z&to=2026-09-07T01:05:00Z&bucket=raw",
            )
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(raw.1[0]["temperature_c"], 40.0);
    assert_eq!(raw.1[0]["humidity_pct"], 50.0);
    assert_eq!(raw.1[0]["event_count"], 1);

    for bucket in ["5m", "1h"] {
        let rollup = sqlite_json_response(
            &app,
            Request::builder()
                .uri(format!(
                    "/api/devices/sqlite-meter/telemetry?from=2026-09-07T00:59:00Z&to=2026-09-07T01:05:00Z&bucket={bucket}"
                ))
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(rollup.1[0]["temperature_c"], 41.0, "bucket={bucket}");
        assert_eq!(rollup.1[0]["humidity_pct"], 51.0, "bucket={bucket}");
        assert_eq!(rollup.1[0]["event_count"], 2, "bucket={bucket}");
    }
}

#[tokio::test]
async fn sqlite_powermonitor_routes_return_summary_telemetry_and_records() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let site_id = uuid::Uuid::now_v7();
    let panel_id = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO assets (id, name) VALUES (?, ?)")
        .bind(site_id.to_string())
        .bind("Factory")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO assets (id, name, parent_asset_id) VALUES (?, ?, ?)")
        .bind(panel_id.to_string())
        .bind("Main panel")
        .bind(site_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let now = Utc::now();
    let bucket_start = Utc
        .timestamp_opt(now.timestamp().div_euclid(300) * 300, 0)
        .single()
        .unwrap();
    let first_at = bucket_start + chrono::Duration::seconds(30);
    let second_at = first_at + chrono::Duration::minutes(1);
    for (device_id, sequence, event_at, power_w, energy_kwh) in [
        ("sqlite-power-meter", 1, first_at, 100.0, 2.0),
        ("sqlite-power-meter", 2, second_at, 120.0, 2.5),
        ("sqlite-aux-meter", 1, first_at, 30.0, 1.0),
    ] {
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: device_id.to_owned(),
            boot_id: uuid::Uuid::new_v4(),
            sequence,
            event_at,
            measurements: serde_json::Map::from_iter([
                ("voltage_v".to_owned(), json!(230.0)),
                ("current_a".to_owned(), json!(2.0)),
                ("power_w".to_owned(), json!(power_w)),
                ("energy_kwh".to_owned(), json!(energy_kwh)),
                ("frequency_hz".to_owned(), json!(50.0)),
                ("power_factor".to_owned(), json!(0.98)),
            ]),
            gateway_device_id: None,
        };
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap();
        sqlx::query("UPDATE devices SET asset_id = ? WHERE device_id = ?")
            .bind(panel_id.to_string())
            .bind(device_id)
            .execute(store.pool())
            .await
            .unwrap();
    }

    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let from = (first_at - chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let to = (second_at + chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let summary = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/summary")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(summary.0, StatusCode::OK, "{:?}", summary.1);
    assert_eq!(summary.1["device_count"], 2);
    assert_eq!(summary.1["asset_count"], 2);
    assert_eq!(summary.1["total_power_w"], 150.0);
    assert_eq!(summary.1["total_energy_kwh"], 3.5);

    let device_raw = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(device_raw.0, StatusCode::OK, "{:?}", device_raw.1);
    assert_eq!(device_raw.1.as_array().unwrap().len(), 2);
    assert_eq!(device_raw.1[1]["power_w"], 120.0);
    assert_eq!(device_raw.1[1]["event_count"], 1);

    let device_rollup = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry?from={from}&to={to}&bucket=5m"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(device_rollup.0, StatusCode::OK, "{:?}", device_rollup.1);
    assert_eq!(device_rollup.1.as_array().unwrap().len(), 1);
    assert_eq!(device_rollup.1[0]["power_w"], 110.0);
    assert_eq!(device_rollup.1[0]["event_count"], 2);

    let records = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry/records?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(records.0, StatusCode::OK, "{:?}", records.1);
    assert_eq!(records.1.as_array().unwrap().len(), 2);
    assert_eq!(records.1[0]["measurements"]["power_w"], 120.0);

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/assets/{site_id}/telemetry?from={from}&to={to}&bucket=5m"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::OK, "{:?}", asset.1);
    assert_eq!(asset.1.as_array().unwrap().len(), 1);
    assert_eq!(asset.1[0]["power_w"], 140.0);
    assert_eq!(asset.1[0]["event_count"], 3);

    let invalid_device_id = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite.meter/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(invalid_device_id.0, StatusCode::BAD_REQUEST);

    let invalid_range = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-power-meter/telemetry?from={to}&to={from}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(invalid_range.0, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sqlite_powermonitor_raw_routes_reject_ranges_over_24_hours() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let to = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let from = (Utc::now() - chrono::Duration::hours(24) - chrono::Duration::seconds(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let asset_id = uuid::Uuid::now_v7();

    for uri in [
        format!(
            "/api/apps/powermonitor/devices/sqlite-meter/telemetry?from={from}&to={to}&bucket=raw"
        ),
        format!(
            "/api/apps/powermonitor/devices/sqlite-meter/telemetry/records?from={from}&to={to}&bucket=raw"
        ),
        format!(
            "/api/apps/powermonitor/assets/{asset_id}/telemetry?from={from}&to={to}&bucket=raw"
        ),
    ] {
        let response = sqlite_json_response(
            &app,
            Request::builder()
                .uri(uri)
                .header("authorization", format!("Session {session_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.0, StatusCode::BAD_REQUEST, "{:?}", response.1);
    }

    let rollup = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/sqlite-meter/telemetry?from={from}&to={to}&bucket=5m"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(rollup.0, StatusCode::OK, "{:?}", rollup.1);
}

#[tokio::test]
async fn sqlite_powermonitor_raw_responses_are_capped_at_10_000_rows() {
    const RAW_ROW_LIMIT: i64 = 10_000;
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let device_id = "sqlite-high-frequency-meter";
    let asset_id = uuid::Uuid::now_v7();
    let event_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    sqlx::query("INSERT INTO assets (id, name) VALUES (?, ?)")
        .bind(asset_id.to_string())
        .bind("High-frequency panel")
        .execute(store.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO devices (device_id, asset_id) VALUES (?, ?)")
        .bind(device_id)
        .bind(asset_id.to_string())
        .execute(store.pool())
        .await
        .unwrap();

    let received_at = event_at.clone();
    let boot_id = uuid::Uuid::new_v4().to_string();
    let measurements = json!({ "power_w": 1.0 }).to_string();
    let mut transaction = store.pool().begin().await.unwrap();
    for start in (0..=RAW_ROW_LIMIT).step_by(1_000) {
        let end = (start + 1_000).min(RAW_ROW_LIMIT + 1);
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "INSERT INTO telemetry (
                event_at, received_at, device_id, boot_id, sequence, measurements, topic
             ) ",
        );
        query.push_values(start..end, |mut values, sequence| {
            values
                .push_bind(&event_at)
                .push_bind(&received_at)
                .push_bind(device_id)
                .push_bind(&boot_id)
                .push_bind(sequence)
                .push_bind(&measurements)
                .push_bind("test");
        });
        query.build().execute(&mut *transaction).await.unwrap();
    }
    transaction.commit().await.unwrap();

    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let from = (Utc::now() - chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let to = (Utc::now() + chrono::Duration::minutes(1))
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let telemetry = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/{device_id}/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(telemetry.0, StatusCode::OK, "{:?}", telemetry.1);
    assert_eq!(
        telemetry.1.as_array().unwrap().len(),
        RAW_ROW_LIMIT as usize
    );

    let records = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/devices/{device_id}/telemetry/records?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(records.0, StatusCode::OK, "{:?}", records.1);
    assert_eq!(records.1.as_array().unwrap().len(), RAW_ROW_LIMIT as usize);

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .uri(format!(
                "/api/apps/powermonitor/assets/{asset_id}/telemetry?from={from}&to={to}&bucket=raw"
            ))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::OK, "{:?}", asset.1);
    assert_eq!(asset.1.as_array().unwrap().len(), 1);
    assert_eq!(asset.1[0]["event_count"], RAW_ROW_LIMIT + 1);
}

#[tokio::test]
async fn sqlite_powermonitor_summary_uses_one_latest_row_for_tied_timestamps() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();

    let event_at = Utc::now();
    for (sequence, power_w, energy_kwh) in [(1, 100.0, 2.0), (2, 200.0, 3.0)] {
        let event = TelemetryEvent {
            schema_version: 1,
            device_id: "sqlite-tied-meter".to_owned(),
            boot_id: uuid::Uuid::new_v4(),
            sequence,
            event_at,
            measurements: serde_json::Map::from_iter([
                ("power_w".to_owned(), json!(power_w)),
                ("energy_kwh".to_owned(), json!(energy_kwh)),
            ]),
            gateway_device_id: None,
        };
        store
            .write_telemetry(&event, event.event_at, "topic")
            .await
            .unwrap();
    }

    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let summary = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/apps/powermonitor/summary")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(summary.0, StatusCode::OK, "{:?}", summary.1);
    assert_eq!(summary.1["device_count"], 1);
    assert_eq!(summary.1["total_power_w"], 200.0);
    assert_eq!(summary.1["total_energy_kwh"], 3.0);
}

#[tokio::test]
async fn sqlite_dashboard_admin_manages_the_alert_rule_and_incident_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;
    let rule = json!({
        "name": "High temperature",
        "device_id": "sqlite-meter",
        "metric_key": "temperature_c",
        "rule_type": "event_threshold",
        "comparison": "gt",
        "threshold": 40.0,
        "for_seconds": 0,
        "severity": "warning"
    });

    let created = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/alert-rules")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(rule.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(created.0, StatusCode::CREATED);
    let rule_id = created.1["id"].as_str().unwrap().to_owned();

    let updated_rule = json!({
        "name": "Critical temperature",
        "device_id": "sqlite-meter",
        "metric_key": "temperature_c",
        "rule_type": "event_threshold",
        "comparison": "gte",
        "threshold": 42.0,
        "for_seconds": 0,
        "severity": "critical"
    });
    let updated = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/alert-rules/{rule_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(updated_rule.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(updated.0, StatusCode::OK);
    assert_eq!(updated.1["name"], "Critical temperature");
    assert_eq!(updated.1["severity"], "critical");

    let toggled = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/api/alert-rules/{rule_id}/toggle"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"enabled":false}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(toggled.0, StatusCode::OK);
    assert_eq!(toggled.1["enabled"], false);

    let rules = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/alert-rules")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(rules.0, StatusCode::OK);
    assert_eq!(rules.1[0]["id"], rule_id);

    let incident_id = uuid::Uuid::new_v4().to_string();
    let occurred_at = "2026-09-07T01:00:00+00:00";
    sqlx::query(
        "INSERT INTO alert_incidents (
            id, rule_id, device_id, status, condition_started_at, opened_at,
            last_value, state_version, created_at, updated_at
         ) VALUES (?, ?, 'sqlite-meter', 'open', ?, ?, 42.5, 1, ?, ?)",
    )
    .bind(&incident_id)
    .bind(&rule_id)
    .bind(occurred_at)
    .bind(occurred_at)
    .bind(occurred_at)
    .bind(occurred_at)
    .execute(store.pool())
    .await
    .unwrap();

    let incidents = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/alert-incidents")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(incidents.0, StatusCode::OK);
    assert_eq!(incidents.1[0]["id"], incident_id);
    assert_eq!(incidents.1[0]["rule_name"], "Critical temperature");

    let acknowledged = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri(format!("/api/alert-incidents/{incident_id}/acknowledge"))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(acknowledged.0, StatusCode::OK);
    assert_eq!(acknowledged.1["acknowledged_by"], "dashboard");

    let archived = sqlite_json_response(
        &app,
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/alert-rules/{rule_id}"))
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(archived.0, StatusCode::NO_CONTENT);

    let active_rules = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/alert-rules")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(active_rules.0, StatusCode::OK);
    assert!(active_rules.1.as_array().unwrap().is_empty());
    let incident_status: String =
        sqlx::query_scalar("SELECT status FROM alert_incidents WHERE id = ?")
            .bind(&incident_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(incident_status, "resolved");
}

#[tokio::test]
async fn sqlite_dashboard_alert_writes_require_an_admin_session() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "viewer", "NanoView@1234").await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/alert-rules")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::from(
                    json!({
                        "name": "High temperature",
                        "metric_key": "temperature_c",
                        "rule_type": "event_threshold",
                        "comparison": "gt",
                        "threshold": 40.0
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn sqlite_management_admin_crud_covers_profiles_assets_devices_and_users() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let device_profile = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/profiles/device-profiles")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Power meter",
                    "telemetry_schema": { "power_w": "number" },
                    "metric_mapping": { "power": "power_w" },
                    "reporting_settings": { "interval_seconds": 60 }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(device_profile.0, StatusCode::CREATED);
    let device_profile_id = device_profile.1["id"].as_str().unwrap().to_owned();

    let asset_profile = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/profiles/asset-profiles")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Electrical panel",
                    "fields": { "floor": "string" },
                    "dashboard_defaults": { "layout": "grid" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(asset_profile.0, StatusCode::CREATED);
    let asset_profile_id = asset_profile.1["id"].as_str().unwrap().to_owned();

    let asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/assets")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Main panel",
                    "asset_profile_id": asset_profile_id,
                    "attributes": { "floor": "1" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(asset.0, StatusCode::CREATED);
    let asset_id = asset.1["id"].as_str().unwrap().to_owned();

    let provisioned = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Meter 1"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(provisioned.0, StatusCode::CREATED);
    let device_id = provisioned.1["device_id"].as_str().unwrap().to_owned();
    assert!(uuid::Uuid::parse_str(&device_id).is_ok());

    let device = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{device_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "display_name": "Meter 1 updated",
                    "asset_id": asset_id,
                    "device_profile_id": device_profile_id,
                    "attributes": { "phase": "A" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(device.0, StatusCode::OK);
    assert_eq!(device.1["display_name"], "Meter 1 updated");
    assert_eq!(device.1["attributes"]["phase"], "A");

    let users = sqlite_json_response(
        &app,
        Request::builder()
            .uri("/api/management/users")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(users.0, StatusCode::OK);
    assert_eq!(users.1.as_array().unwrap().len(), 2);

    let user = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri("/api/management/users/viewer")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                r#"{"default_app":"/apps/powermonitor","granted_apps":["powermonitor"],"role":"viewer"}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(user.0, StatusCode::OK);
    assert_eq!(user.1["role"], "viewer");
    assert_eq!(user.1["granted_apps"], json!(["powermonitor"]));

    let renamed_asset = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/assets/{asset_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "name": "Main panel updated",
                    "asset_profile_id": asset_profile_id,
                    "attributes": { "floor": "2" }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(renamed_asset.0, StatusCode::OK);
    assert_eq!(renamed_asset.1["attributes"]["floor"], "2");

    for uri in [
        format!("/api/management/devices/{device_id}"),
        format!("/api/management/assets/{asset_id}"),
        format!("/api/management/profiles/device-profiles/{device_profile_id}"),
        format!("/api/management/profiles/asset-profiles/{asset_profile_id}"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(uri)
                    .header("authorization", format!("Session {session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    let deleted_at: Option<String> =
        sqlx::query_scalar("SELECT deleted_at FROM devices WHERE device_id = ?")
            .bind(device_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(deleted_at.is_some());
}

#[tokio::test]
async fn sqlite_management_writes_require_an_admin_session() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store));
    let session_id = sqlite_session_id(&app, "viewer", "NanoView@1234").await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/management/assets")
                .header(header::CONTENT_TYPE, "application/json")
                .header("authorization", format!("Session {session_id}"))
                .body(Body::from(r#"{"name":"Viewer cannot create this"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn sqlite_management_gateway_child_assignment_revokes_the_child_token() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("rush.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let app = sqlite_router(SqliteApiState::new(store.clone()));
    let session_id = sqlite_session_id(&app, "admin", "NanoAdmin@1234").await;

    let gateway = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Gateway"}"#))
            .unwrap(),
    )
    .await;
    let gateway_id = gateway.1["device_id"].as_str().unwrap().to_owned();
    let child = sqlite_json_response(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/management/devices")
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(r#"{"display_name":"Child meter"}"#))
            .unwrap(),
    )
    .await;
    let child_id = child.1["device_id"].as_str().unwrap().to_owned();

    let gateway_update = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{gateway_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                r#"{"display_name":"Gateway","topology":{"is_gateway":true}}"#,
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(gateway_update.0, StatusCode::OK);

    let child_update = sqlite_json_response(
        &app,
        Request::builder()
            .method("PUT")
            .uri(format!("/api/management/devices/{child_id}"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("authorization", format!("Session {session_id}"))
            .body(Body::from(
                json!({
                    "display_name": "Child meter",
                    "topology": {
                        "is_gateway": false,
                        "gateway_device_id": gateway_id
                    }
                })
                .to_string(),
            ))
            .unwrap(),
    )
    .await;
    assert_eq!(child_update.0, StatusCode::OK);
    assert_eq!(child_update.1["gateway_device_id"], gateway_id);

    let active_token_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM device_tokens WHERE device_id = ? AND revoked_at IS NULL",
    )
    .bind(child_id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(active_token_count, 0);
}

async fn sqlite_session_id(app: &axum::Router, username: &str, password: &str) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    json!({ "username": username, "password": password }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice::<serde_json::Value>(&body).unwrap()["session_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn sqlite_json_response(
    app: &axum::Router,
    request: Request<Body>,
) -> (StatusCode, serde_json::Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&body).unwrap()
    };
    (status, body)
}
