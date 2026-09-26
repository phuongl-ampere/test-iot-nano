use std::{fs, path::Path};

#[test]
fn api_crate_exposes_only_the_monolith_library_boundary() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));

    for retired_artifact in [
        "src/routes.rs",
        "src/storage.rs",
        "src/api_sqlite_schema.sql",
        "src/power_switcher.rs",
        "src/powermonitor.rs",
        "src/resource_authorization.rs",
        "src/system_config.rs",
        "migrations/0001_api.sql",
    ] {
        assert!(
            !manifest_dir.join(retired_artifact).exists(),
            "retired direct API artifact remains: {retired_artifact}"
        );
    }

    let library = fs::read_to_string(manifest_dir.join("src/lib.rs"))
        .expect("API library root must be readable");

    for retired_export in [
        "mod routes;",
        "mod storage;",
        "ApiState",
        "SqliteApiState",
        "ApiRouters",
        "SqliteApiRouters",
        "connect_api_database",
        "migrate_api",
        "ApiSqliteStore",
        "bootstrap_users,",
    ] {
        assert!(
            !library.contains(retired_export),
            "retired direct API export remains: {retired_export}"
        );
    }

    for required_export in [
        "OAuthBrowserSessionVerifier",
        "public_oauth_router",
        "public_oauth_router_with_browser_session_verifier",
        "pub use public_v1::public_v1_router;",
        "CoreFacade",
        "TokenVault",
        "authenticate_platform_account",
        "create_platform_device_token",
        "provision_platform_device_token",
    ] {
        assert!(
            library.contains(required_export),
            "monolith library export is missing: {required_export}"
        );
    }
}
