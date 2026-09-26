use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationDomainProfileRepository, ApplicationDomainResourceKind, ApplicationKind,
    ApplicationRepository, ClientId, CreateApplicationAssetProfileRelation,
    CreateApplicationDomainProfile, NewApplication, PlatformStore, RedirectUri,
};
use serde_json::json;
use uuid::Uuid;

async fn sqlite_store() -> (tempfile::TempDir, PlatformStore, Uuid) {
    let directory = tempfile::tempdir().unwrap();
    let store = PlatformStore::open(&StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("application-domain.sqlite")),
        sqlite_busy_timeout_ms: 5_000,
    })
    .await
    .unwrap();
    let tenant_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'application-domain', 'active')",
    )
    .bind(tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    (directory, store, tenant_id)
}

async fn create_application(store: &PlatformStore, tenant_id: Uuid, app_id: &str) {
    ApplicationRepository::upsert_application(
        store,
        NewApplication {
            app_id: app_id.parse().unwrap(),
            tenant_id,
            kind: ApplicationKind::Frontend,
            launch_url: format!("http://{app_id}.example.test"),
            client_id: format!("{app_id}-client").parse::<ClientId>().unwrap(),
            redirect_uris: vec![
                format!("http://{app_id}.example.test/callback")
                    .parse::<RedirectUri>()
                    .unwrap(),
            ],
            allowed_scopes: vec!["assets:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
}

fn domain_profile(
    app_id: &str,
    resource_kind: ApplicationDomainResourceKind,
    name: &str,
) -> CreateApplicationDomainProfile {
    CreateApplicationDomainProfile {
        app_id: app_id.parse().unwrap(),
        resource_kind,
        name: name.to_owned(),
        definition: json!({"schema": {"power_w": {"type": "number"}}}),
        live_view: json!({
            "live_charts": [{
                "metric": "power_w",
                "label": "Active power",
                "unit": "W",
                "aggregation": "last"
            }]
        }),
    }
}

#[tokio::test]
async fn sqlite_application_domain_profiles_are_isolated_by_application_and_resource_kind() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    create_application(&store, tenant_id, "powermonitor").await;
    create_application(&store, tenant_id, "fleetmonitor").await;

    let meter = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Device,
            "Power Meter",
        ),
    )
    .await
    .unwrap();
    ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Asset,
            "Power Farm",
        ),
    )
    .await
    .unwrap();
    ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "fleetmonitor",
            ApplicationDomainResourceKind::Device,
            "Tracker",
        ),
    )
    .await
    .unwrap();

    let profiles = ApplicationDomainProfileRepository::list_application_domain_profiles(
        &store,
        tenant_id,
        "powermonitor",
        Some(ApplicationDomainResourceKind::Device),
    )
    .await
    .unwrap();

    assert_eq!(profiles, vec![meter]);
}

#[tokio::test]
async fn sqlite_application_domain_profile_assignment_replaces_and_clears_only_that_app_assignment()
{
    let (_directory, store, tenant_id) = sqlite_store().await;
    create_application(&store, tenant_id, "powermonitor").await;
    create_application(&store, tenant_id, "fleetmonitor").await;
    sqlx::query("INSERT INTO devices (device_id, tenant_id) VALUES ('meter-a', ?)")
        .bind(tenant_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    let first = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Device,
            "Power Meter",
        ),
    )
    .await
    .unwrap();
    let replacement = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Device,
            "Inverter",
        ),
    )
    .await
    .unwrap();
    let fleet_profile = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "fleetmonitor",
            ApplicationDomainResourceKind::Device,
            "Tracker",
        ),
    )
    .await
    .unwrap();

    ApplicationDomainProfileRepository::assign_application_domain_profile(
        &store,
        tenant_id,
        "powermonitor",
        ApplicationDomainResourceKind::Device,
        "meter-a",
        Some(first.id),
    )
    .await
    .unwrap();
    ApplicationDomainProfileRepository::assign_application_domain_profile(
        &store,
        tenant_id,
        "powermonitor",
        ApplicationDomainResourceKind::Device,
        "meter-a",
        Some(replacement.id),
    )
    .await
    .unwrap();
    ApplicationDomainProfileRepository::assign_application_domain_profile(
        &store,
        tenant_id,
        "fleetmonitor",
        ApplicationDomainResourceKind::Device,
        "meter-a",
        Some(fleet_profile.id),
    )
    .await
    .unwrap();

    assert_eq!(
        ApplicationDomainProfileRepository::application_domain_profile_assignment(
            &store,
            tenant_id,
            "powermonitor",
            ApplicationDomainResourceKind::Device,
            "meter-a",
        )
        .await
        .unwrap(),
        Some(replacement),
    );
    ApplicationDomainProfileRepository::assign_application_domain_profile(
        &store,
        tenant_id,
        "powermonitor",
        ApplicationDomainResourceKind::Device,
        "meter-a",
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        ApplicationDomainProfileRepository::application_domain_profile_assignment(
            &store,
            tenant_id,
            "powermonitor",
            ApplicationDomainResourceKind::Device,
            "meter-a",
        )
        .await
        .unwrap(),
        None,
    );
    assert_eq!(
        ApplicationDomainProfileRepository::application_domain_profile_assignment(
            &store,
            tenant_id,
            "fleetmonitor",
            ApplicationDomainResourceKind::Device,
            "meter-a",
        )
        .await
        .unwrap(),
        Some(fleet_profile),
    );
}

#[tokio::test]
async fn sqlite_application_domain_allows_only_asset_contains_relations() {
    let (_directory, store, tenant_id) = sqlite_store().await;
    create_application(&store, tenant_id, "powermonitor").await;
    let farm = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Asset,
            "Power Farm",
        ),
    )
    .await
    .unwrap();
    let zone = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Asset,
            "Power Zone",
        ),
    )
    .await
    .unwrap();
    let meter = ApplicationDomainProfileRepository::create_application_domain_profile(
        &store,
        tenant_id,
        domain_profile(
            "powermonitor",
            ApplicationDomainResourceKind::Device,
            "Power Meter",
        ),
    )
    .await
    .unwrap();

    let relation = ApplicationDomainProfileRepository::create_application_asset_profile_relation(
        &store,
        tenant_id,
        CreateApplicationAssetProfileRelation {
            app_id: "powermonitor".parse().unwrap(),
            parent_profile_id: farm.id,
            child_profile_id: zone.id,
        },
    )
    .await
    .unwrap();
    assert_eq!(relation.parent_profile_id, farm.id);
    assert_eq!(relation.child_profile_id, zone.id);

    let error = ApplicationDomainProfileRepository::create_application_asset_profile_relation(
        &store,
        tenant_id,
        CreateApplicationAssetProfileRelation {
            app_id: "powermonitor".parse().unwrap(),
            parent_profile_id: farm.id,
            child_profile_id: meter.id,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("asset profiles"), "{error}");
}
