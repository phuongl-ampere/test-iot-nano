use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRecord, ApplicationRepository, NewApplication, PlatformStore,
    PlatformStoreError,
};
use sqlx::{Connection, PgConnection};

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("platform.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    (directory, store)
}

fn application(enabled: bool) -> NewApplication {
    NewApplication {
        app_id: "power-monitor".parse().unwrap(),
        kind: ApplicationKind::FullStack,
        launch_url: "https://apps.example.test/power".to_owned(),
        client_id: "client-power-monitor".parse().unwrap(),
        redirect_uris: vec![
            "https://apps.example.test/callback/z".parse().unwrap(),
            "https://apps.example.test/callback/a".parse().unwrap(),
        ],
        allowed_scopes: vec!["telemetry:read".to_owned(), "devices:read".to_owned()],
        enabled,
    }
}

fn assert_application_shape(application: ApplicationRecord, enabled: bool) {
    assert_eq!(application.app_id.as_str(), "power-monitor");
    assert_eq!(application.kind, ApplicationKind::FullStack);
    assert_eq!(application.launch_url, "https://apps.example.test/power");
    assert_eq!(application.client_id.as_str(), "client-power-monitor");
    assert_eq!(
        application
            .redirect_uris
            .iter()
            .map(|uri| uri.as_str())
            .collect::<Vec<_>>(),
        [
            "https://apps.example.test/callback/a",
            "https://apps.example.test/callback/z",
        ]
    );
    assert_eq!(
        application.allowed_scopes,
        ["devices:read", "telemetry:read"]
    );
    assert_eq!(application.enabled, enabled);
}

#[tokio::test]
async fn sqlite_application_registry_validates_canonicalizes_and_upserts() {
    let (_directory, store) = sqlite_store().await;

    let created = ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    assert_application_shape(created, true);

    let by_app_id = ApplicationRepository::find_application_by_app_id(&store, "power-monitor")
        .await
        .unwrap()
        .unwrap();
    assert_application_shape(by_app_id, true);

    let by_client_id =
        ApplicationRepository::find_application_by_client_id(&store, "client-power-monitor")
            .await
            .unwrap()
            .unwrap();
    assert_application_shape(by_client_id, true);

    let mut updated = application(false);
    updated.launch_url = "https://apps.example.test/power-v2".to_owned();
    let updated = ApplicationRepository::upsert_application(&store, updated)
        .await
        .unwrap();
    assert_eq!(updated.launch_url, "https://apps.example.test/power-v2");
    assert!(!updated.enabled);

    let disabled =
        ApplicationRepository::find_application_by_client_id(&store, "client-power-monitor").await;
    assert!(matches!(
        disabled,
        Err(PlatformStoreError::ApplicationDisabled(ref app_id)) if app_id.as_str() == "power-monitor"
    ));
}

#[tokio::test]
async fn sqlite_application_registry_rejects_domain_invalid_values() {
    let (_directory, store) = sqlite_store().await;

    assert!(matches!(
        "Power Monitor".parse::<iot_storage::ApplicationId>(),
        Err(PlatformStoreError::InvalidApplicationId(_))
    ));
    assert!(matches!(
        "".parse::<iot_storage::ClientId>(),
        Err(PlatformStoreError::EmptyApplicationClientId)
    ));
    assert!(matches!(
        "".parse::<iot_storage::RedirectUri>(),
        Err(PlatformStoreError::EmptyApplicationRedirectUri)
    ));
    assert!(matches!(
        ApplicationRepository::upsert_application(
            &store,
            NewApplication {
                launch_url: String::new(),
                ..application(true)
            }
        )
        .await,
        Err(PlatformStoreError::EmptyApplicationLaunchUrl)
    ));

    let mut duplicate = application(true);
    duplicate
        .redirect_uris
        .push(duplicate.redirect_uris[0].clone());
    assert!(matches!(
        ApplicationRepository::upsert_application(&store, duplicate).await,
        Err(PlatformStoreError::DuplicateApplicationRedirectUri(_))
    ));
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL"]
async fn timescale_application_registry_matches_sqlite_contract() {
    let database_url = std::env::var("IOT_NANO_TIMESCALE_TEST_URL")
        .expect("IOT_NANO_TIMESCALE_TEST_URL must be set when running ignored Timescale tests");
    let mut connection = PgConnection::connect(&database_url).await.unwrap();
    let database_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert!(
        database_name.starts_with("iot_nano_test_"),
        "refusing to reset non-test database {database_name:?}"
    );
    sqlx::query("SELECT pg_advisory_lock(hashtext('iot_nano:platform-storage-test'))")
        .execute(&mut connection)
        .await
        .unwrap();
    sqlx::query("DROP SCHEMA IF EXISTS iot_nano CASCADE")
        .execute(&mut connection)
        .await
        .unwrap();

    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Timescale,
        database_url: Some(database_url),
        sqlite_path: None,
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let record = ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    assert_application_shape(record, true);

    sqlx::raw_sql(
        "CREATE FUNCTION reject_application_redirect() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             RAISE EXCEPTION 'redirect write rejected';
         END;
         $$;
         CREATE TRIGGER reject_application_redirect_trigger
         BEFORE INSERT ON application_redirect_uris
         FOR EACH ROW EXECUTE FUNCTION reject_application_redirect();",
    )
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let mut replacement = application(true);
    replacement.launch_url = "https://apps.example.test/replacement".to_owned();
    assert!(
        ApplicationRepository::upsert_application(&store, replacement)
            .await
            .is_err()
    );
    let unchanged = ApplicationRepository::find_application_by_app_id(&store, "power-monitor")
        .await
        .unwrap()
        .unwrap();
    assert_application_shape(unchanged, true);
}
