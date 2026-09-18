mod common;

use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
};

use sqlx::{Connection, PgConnection};

const SHARED_LOCK_KEY: &str = "iot_nano:platform-storage-test";
const SHARED_SCHEMA_HELPER_MARKERS: &[&str] = &[
    "common::reset_timescale_schema",
    "common::lock_timescale_schema",
];
const SHARED_SCHEMA_TEST_FILES: &[&str] = &[
    "alert_evaluation.rs",
    "alert_incident.rs",
    "application_registry.rs",
    "audit_events.rs",
    "backend_contract.rs",
    "command_outbox.rs",
    "device_authorization.rs",
    "device_tokens.rs",
    "gateway_ingest.rs",
    "identity.rs",
    "management_alerts.rs",
    "management_assets.rs",
    "management_devices.rs",
    "management_profiles.rs",
    "management_users.rs",
    "migration_safety.rs",
    "notification_outbox.rs",
    "oauth_persistence.rs",
    "public_api.rs",
    "resource_authorization.rs",
    "telemetry_aggregate.rs",
];

const RESETTERS: &[(&str, &str)] = &[
    ("alert_evaluation.rs", "timescale_store"),
    ("alert_incident.rs", "timescale_store"),
    (
        "application_registry.rs",
        "timescale_application_registry_matches_sqlite_contract",
    ),
    (
        "audit_events.rs",
        "timescale_audit_events_cover_permission_relation_and_containment_mutations",
    ),
    ("backend_contract.rs", "timescale_test_store"),
    ("command_outbox.rs", "timescale_store"),
    ("device_authorization.rs", "timescale_store"),
    ("device_tokens.rs", "timescale_store"),
    ("gateway_ingest.rs", "timescale_test_store"),
    ("identity.rs", "timescale_store"),
    (
        "management_alerts.rs",
        "timescale_management_alert_list_is_tenant_scoped_newest_first_and_capped",
    ),
    ("management_assets.rs", "timescale_store"),
    ("management_devices.rs", "timescale_store"),
    ("management_profiles.rs", "timescale_store"),
    ("management_users.rs", "timescale_store"),
    (
        "migration_safety.rs",
        "timescale_open_rejects_pre_tenant_platform_schema_without_partial_migration",
    ),
    (
        "migration_safety.rs",
        "timescale_open_rejects_pre_tenant_asset_schema_without_mutating_data",
    ),
    ("notification_outbox.rs", "timescale_notification_store"),
    (
        "oauth_persistence.rs",
        "timescale_oauth_repository_matches_sqlite_contract",
    ),
    ("public_api.rs", "timescale_store"),
    (
        "public_api.rs",
        "timescale_public_repository_creates_and_reads_an_asset",
    ),
    (
        "public_api.rs",
        "timescale_public_device_repository_matches_sqlite_mutation_contract",
    ),
    (
        "public_api.rs",
        "timescale_public_device_asset_assignment_requires_asset_manager_permission",
    ),
    (
        "public_api.rs",
        "timescale_public_device_create_rejects_an_unavailable_profile_atomically",
    ),
    (
        "public_api.rs",
        "timescale_public_device_assignment_serializes_with_management_asset_deletion",
    ),
    (
        "public_api.rs",
        "timescale_public_device_assignment_waits_for_device_profile_lock",
    ),
    ("resource_authorization.rs", "timescale_store"),
    ("telemetry_aggregate.rs", "timescale_store"),
];

const CONSUMERS: &[(&str, &str)] = &[];

fn tests_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests")
}

fn read_test_source(file_name: &str) -> String {
    fs::read_to_string(tests_dir().join(file_name))
        .unwrap_or_else(|error| panic!("failed to read {file_name}: {error}"))
}

fn is_shared_schema_source(source: &str) -> bool {
    SHARED_SCHEMA_HELPER_MARKERS
        .iter()
        .any(|marker| source.contains(marker))
}

fn function_body<'a>(source: &'a str, function_name: &str) -> &'a str {
    let marker = format!("fn {function_name}");
    let function_start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("missing function {function_name}"));
    let body_start = function_start
        + source[function_start..]
            .find('{')
            .expect("function has a body");
    let mut depth = 0;
    for (offset, character) in source[body_start..].char_indices() {
        match character {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    let end = body_start + offset + character.len_utf8();
                    return &source[body_start..end];
                }
            }
            _ => {}
        }
    }
    panic!("unclosed body for function {function_name}");
}

#[test]
fn isolated_timescale_source_is_not_shared_by_backend_markers_alone() {
    let isolated_source = r#"
        let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL").unwrap();
        let storage = DatabaseStorage::Timescale;
    "#;

    assert!(!is_shared_schema_source(isolated_source));
}

#[test]
fn shared_schema_inventory_is_explicit_and_excludes_isolated_tests() {
    let actual: BTreeSet<String> = fs::read_dir(tests_dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("rs"))
        .filter(|path| {
            path.file_name().and_then(|name| name.to_str())
                != Some("schema_reset_lock_invariant.rs")
        })
        .filter(|path| {
            let source = fs::read_to_string(path).unwrap();
            is_shared_schema_source(&source)
        })
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    let expected: BTreeSet<String> = SHARED_SCHEMA_TEST_FILES
        .iter()
        .map(|file_name| (*file_name).to_owned())
        .collect();

    assert_eq!(actual, expected);
}

#[test]
fn common_reset_helper_locks_before_dropping_the_shared_schema() {
    let source = read_test_source("common/mod.rs");
    let lock_body = function_body(&source, "lock_timescale_schema");
    assert!(lock_body.contains("pg_advisory_lock"));
    assert!(lock_body.contains(SHARED_LOCK_KEY));
    assert!(lock_body.contains("execute(&mut *connection)"));

    let reset_body = function_body(&source, "reset_timescale_schema");
    let lock_call = reset_body
        .find("lock_timescale_schema(connection).await?")
        .expect("reset helper must call the canonical lock helper");
    let destructive_reset = reset_body
        .find("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .expect("reset helper must contain the shared schema reset");
    assert!(
        lock_call < destructive_reset,
        "session lock must be acquired before the destructive reset"
    );
}

#[test]
fn every_shared_schema_resetter_routes_through_the_common_reset_helper() {
    assert_eq!(RESETTERS.len(), 28);
    for (file_name, function_name) in RESETTERS {
        let source = read_test_source(file_name);
        let body = function_body(&source, function_name);
        assert!(
            body.contains("common::reset_timescale_schema(&mut connection)"),
            "{file_name}::{function_name} must use the common reset helper"
        );
        assert!(
            !body.contains("DROP SCHEMA IF EXISTS iot_nano CASCADE"),
            "{file_name}::{function_name} must not reset the schema directly"
        );
    }
}

#[test]
fn every_non_reset_shared_schema_consumer_routes_through_the_common_lock_helper() {
    assert_eq!(CONSUMERS.len(), 0);
    for (file_name, function_name) in CONSUMERS {
        let source = read_test_source(file_name);
        let body = function_body(&source, function_name);
        let lock_call = body
            .find("common::lock_timescale_schema(&mut connection)")
            .unwrap_or_else(|| panic!("{file_name}::{function_name} must acquire the common lock"));
        let store_open = body
            .find("PlatformStore::open")
            .expect("Timescale consumer must open a store");
        assert!(
            lock_call < store_open,
            "{file_name}::{function_name} must lock before opening the Timescale store"
        );
        assert!(
            body.contains("current_database()") && body.contains("iot_nano_test_"),
            "{file_name}::{function_name} must retain the disposable database safety gate"
        );
        assert!(
            body.contains("let mut connection"),
            "{file_name}::{function_name} must retain the lock-owning connection"
        );
    }
}

#[test]
fn no_test_file_resets_the_shared_schema_directly() {
    for entry in fs::read_dir(tests_dir()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("schema_reset_lock_invariant.rs")
        {
            continue;
        }
        let source = fs::read_to_string(&path).unwrap();
        assert!(
            !source.contains("DROP SCHEMA IF EXISTS iot_nano CASCADE"),
            "{} must route destructive resets through common/mod.rs",
            path.display()
        );
    }
}

#[tokio::test]
async fn live_shared_lock_is_session_held_when_configured() {
    let Ok(database_url) = env::var("IOT_NANO_TIMESCALE_TEST_URL") else {
        return;
    };

    let mut owner = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut owner)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to probe non-test database {database_name:?}"
    );
    common::lock_timescale_schema(&mut owner).await.unwrap();

    let mut contender = PgConnection::connect(&database_url).await.unwrap();
    let acquired: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_lock(hashtext('iot_nano:platform-storage-test'))",
    )
    .fetch_one(&mut contender)
    .await
    .unwrap();
    assert!(
        !acquired,
        "a second session acquired the shared lock while the owner was alive"
    );
}
