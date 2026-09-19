use chrono::{Duration, TimeZone, Utc};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthAuthorizationCode,
    NewOAuthClientSecret, OAuthAuthorizationCodeExchange, OAuthClientCredentialsToken,
    OAuthRepository, PlatformStore, PlatformStoreError,
};
use sqlx::{Connection, PgConnection, Row, SqlitePool};
use uuid::Uuid;

mod common;

const S256_CODE_VERIFIER: &str = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
const S256_CODE_CHALLENGE: &str = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

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
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'oauth-persistence', 'active')")
        .bind(test_tenant_id().to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    (directory, store)
}

fn test_tenant_id() -> Uuid {
    Uuid::from_u128(10_003)
}

async fn sqlite_columns(pool: &SqlitePool, table: &str) -> Vec<String> {
    let query = match table {
        "oauth_client_secrets" => "PRAGMA table_info(oauth_client_secrets)",
        "oauth_authorization_codes" => "PRAGMA table_info(oauth_authorization_codes)",
        "oauth_access_tokens" => "PRAGMA table_info(oauth_access_tokens)",
        _ => unreachable!("test only requests known OAuth tables"),
    };
    sqlx::query(query)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.try_get("name").unwrap())
        .collect()
}

fn application(enabled: bool) -> NewApplication {
    NewApplication {
        app_id: "power-monitor".parse().unwrap(),
        tenant_id: test_tenant_id(),
        kind: ApplicationKind::FullStack,
        launch_url: "https://apps.example.test/power".to_owned(),
        client_id: "client-power-monitor".parse().unwrap(),
        redirect_uris: vec!["https://apps.example.test/callback".parse().unwrap()],
        allowed_scopes: vec!["devices:read".to_owned(), "telemetry:read".to_owned()],
        enabled,
    }
}

fn other_application(enabled: bool) -> NewApplication {
    NewApplication {
        app_id: "other-monitor".parse().unwrap(),
        tenant_id: test_tenant_id(),
        kind: ApplicationKind::FullStack,
        launch_url: "https://apps.example.test/other".to_owned(),
        client_id: "client-other-monitor".parse().unwrap(),
        redirect_uris: vec!["https://apps.example.test/callback".parse().unwrap()],
        allowed_scopes: vec!["devices:read".to_owned()],
        enabled,
    }
}

async fn seed_user(store: &PlatformStore, user_id: Uuid) {
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, ?, 'unused', 'viewer', 'user')",
    )
    .bind(user_id.to_string())
    .bind(test_tenant_id().to_string())
    .bind(format!("oauth-user-{user_id}"))
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
}

async fn seed_timescale_user(store: &PlatformStore, user_id: Uuid) {
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES ($1, $2, $3, 'unused', 'viewer', 'user')",
    )
    .bind(user_id)
    .bind(test_tenant_id())
    .bind(format!("oauth-user-{user_id}"))
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
}

#[tokio::test]
async fn sqlite_oauth_schema_normalizes_secret_code_and_token_records() {
    let (_directory, store) = sqlite_store().await;
    let pool = store.sqlite_pool().unwrap();

    assert_eq!(
        sqlite_columns(pool, "oauth_client_secrets").await,
        ["app_id", "tenant_id", "secret_hash", "created_at"]
    );
    assert_eq!(
        sqlite_columns(pool, "oauth_authorization_codes").await,
        [
            "code_hash",
            "app_id",
            "tenant_id",
            "user_id",
            "redirect_uri",
            "code_challenge",
            "scopes_json",
            "issued_at",
            "expires_at",
            "consumed_at",
        ]
    );
    assert_eq!(
        sqlite_columns(pool, "oauth_access_tokens").await,
        [
            "token_hash",
            "app_id",
            "tenant_id",
            "user_id",
            "scopes_json",
            "issued_at",
            "expires_at",
        ]
    );
}

#[tokio::test]
async fn sqlite_oauth_authorization_codes_require_matching_user_and_application_tenants() {
    let (_directory, store) = sqlite_store().await;
    let foreign_tenant_id = Uuid::now_v7();
    let foreign_user_id = Uuid::now_v7();
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'oauth-foreign', 'active')")
        .bind(foreign_tenant_id.to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class)
         VALUES (?, ?, 'oauth-foreign-user', 'unused', 'viewer', 'user')",
    )
    .bind(foreign_user_id.to_string())
    .bind(foreign_tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();

    let now = Utc.with_ymd_and_hms(2026, 9, 13, 1, 2, 3).single().unwrap();
    let result = OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-cross-tenant-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id: foreign_user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: now,
            expires_at: now + Duration::minutes(10),
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let codes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_authorization_codes")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(codes, 0);
}

#[tokio::test]
async fn sqlite_oauth_issues_digest_only_client_secret_and_authorization_code() {
    let (_directory, store) = sqlite_store().await;
    let now = Utc.with_ymd_and_hms(2026, 9, 13, 1, 2, 3).single().unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();

    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            client_secret: "oauth-client-secret-for-test".to_owned(),
        },
    )
    .await
    .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-authorization-code-for-test".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: now,
            expires_at: now + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let secret_hash: String = sqlx::query_scalar(
        "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = 'power-monitor'",
    )
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        secret_hash,
        "2a906b5405ec88ccba222cde447f95aa5a1b25003db13d76536b10b691859f60"
    );

    let code = sqlx::query(
        "SELECT code_hash, app_id, user_id, redirect_uri, code_challenge, scopes_json,
                issued_at, expires_at, consumed_at
         FROM oauth_authorization_codes",
    )
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        code.try_get::<String, _>("code_hash").unwrap(),
        "c59b6c0d76fd980d301a0d737387e44ee7c8ee03b69e89eb032e8b8caf1990b7"
    );
    assert_eq!(
        code.try_get::<String, _>("app_id").unwrap(),
        "power-monitor"
    );
    assert_eq!(
        code.try_get::<String, _>("user_id").unwrap(),
        user_id.to_string()
    );
    assert_eq!(
        code.try_get::<String, _>("redirect_uri").unwrap(),
        "https://apps.example.test/callback"
    );
    assert_eq!(
        code.try_get::<String, _>("code_challenge").unwrap(),
        S256_CODE_CHALLENGE
    );
    assert_eq!(
        code.try_get::<String, _>("scopes_json").unwrap(),
        r#"["devices:read"]"#
    );
    assert_eq!(
        code.try_get::<String, _>("issued_at").unwrap(),
        now.to_rfc3339()
    );
    assert_eq!(
        code.try_get::<String, _>("expires_at").unwrap(),
        (now + Duration::minutes(10)).to_rfc3339()
    );
    assert!(
        code.try_get::<Option<String>, _>("consumed_at")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn sqlite_oauth_consumes_code_once_and_issues_a_digest_only_access_token() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc.with_ymd_and_hms(2026, 9, 13, 2, 3, 4).single().unwrap();
    let exchange_at = code_issued_at + Duration::minutes(1);
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-code-for-exchange".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at: code_issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let issued = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-code-for-exchange".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-access-token-for-test".to_owned(),
            issued_at: exchange_at,
            expires_at: exchange_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(issued.app_id.as_str(), "power-monitor");
    assert_eq!(issued.user_id, Some(user_id));
    assert_eq!(issued.scopes, ["devices:read"]);
    assert_eq!(issued.issued_at, exchange_at);
    assert_eq!(issued.expires_at, exchange_at + Duration::hours(1));

    let code_consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_eq!(
        code_consumed_at.as_deref(),
        Some(exchange_at.to_rfc3339().as_str())
    );

    let token = sqlx::query(
        "SELECT token_hash, app_id, user_id, scopes_json, issued_at, expires_at
         FROM oauth_access_tokens",
    )
    .fetch_one(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    assert_eq!(
        token.try_get::<String, _>("token_hash").unwrap(),
        "c0a634cf2b527b6ff9d22da5809f685deac0f77cec6d5d7e6557aabfc0a7a90d"
    );
    assert_eq!(
        token.try_get::<String, _>("app_id").unwrap(),
        "power-monitor"
    );
    assert_eq!(
        token.try_get::<String, _>("user_id").unwrap(),
        user_id.to_string()
    );
    assert_eq!(
        token.try_get::<String, _>("scopes_json").unwrap(),
        r#"["devices:read"]"#
    );
    assert_eq!(
        token.try_get::<String, _>("issued_at").unwrap(),
        exchange_at.to_rfc3339()
    );
    assert_eq!(
        token.try_get::<String, _>("expires_at").unwrap(),
        (exchange_at + Duration::hours(1)).to_rfc3339()
    );

    let reused = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-code-for-exchange".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-second-access-token".to_owned(),
            issued_at: exchange_at + Duration::seconds(1),
            expires_at: exchange_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        reused,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 1);
}

#[tokio::test]
async fn sqlite_oauth_denies_an_expired_code_without_consuming_or_issuing_a_token() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc.with_ymd_and_hms(2026, 9, 13, 3, 4, 5).single().unwrap();
    let expires_at = code_issued_at + Duration::minutes(1);
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-expired-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at,
        },
    )
    .await
    .unwrap();

    let exchange = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-expired-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-token-for-expired-code".to_owned(),
            issued_at: expires_at,
            expires_at: expires_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        exchange,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
}

#[tokio::test]
async fn sqlite_oauth_denies_a_code_exchange_with_a_different_redirect_uri() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc.with_ymd_and_hms(2026, 9, 13, 4, 5, 6).single().unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-redirect-bound-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at: code_issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let exchange = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-redirect-bound-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/different-callback"
                .parse()
                .unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-token-for-redirect-mismatch".to_owned(),
            issued_at: code_issued_at + Duration::minutes(1),
            expires_at: code_issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        exchange,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
}

#[tokio::test]
async fn sqlite_oauth_denies_a_code_exchange_through_a_different_client_application() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc.with_ymd_and_hms(2026, 9, 13, 5, 6, 7).single().unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    ApplicationRepository::upsert_application(&store, other_application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-cross-app-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at: code_issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let exchange = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-cross-app-code".to_owned(),
            client_id: "client-other-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-token-for-cross-app-code".to_owned(),
            issued_at: code_issued_at + Duration::minutes(1),
            expires_at: code_issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        exchange,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
}

#[tokio::test]
async fn sqlite_oauth_denies_a_code_exchange_after_the_application_is_disabled() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc.with_ymd_and_hms(2026, 9, 13, 6, 7, 8).single().unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-disabled-app-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at: code_issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();
    ApplicationRepository::upsert_application(&store, application(false))
        .await
        .unwrap();

    let exchange = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-disabled-app-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-token-for-disabled-app".to_owned(),
            issued_at: code_issued_at + Duration::minutes(1),
            expires_at: code_issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        exchange,
        Err(PlatformStoreError::ApplicationDisabled(ref app_id))
            if app_id.as_str() == "power-monitor"
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
}

#[tokio::test]
async fn sqlite_oauth_denies_an_incorrect_confidential_client_secret() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc.with_ymd_and_hms(2026, 9, 13, 7, 8, 9).single().unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-confidential-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at: code_issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let exchange = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-confidential-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: Some("incorrect-confidential-client-secret".to_owned()),
            access_token: "oauth-token-for-bad-client-secret".to_owned(),
            issued_at: code_issued_at + Duration::minutes(1),
            expires_at: code_issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        exchange,
        Err(PlatformStoreError::OAuthClientAuthenticationDenied)
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
}

#[tokio::test]
async fn sqlite_oauth_client_credentials_preserves_requested_scopes_and_denies_expansion() {
    let (_directory, store) = sqlite_store().await;
    let issued_at = Utc
        .with_ymd_and_hms(2026, 9, 13, 8, 9, 10)
        .single()
        .unwrap();
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            client_secret: "client-credentials-secret".to_owned(),
        },
    )
    .await
    .unwrap();

    let issued = OAuthRepository::issue_client_credentials_access_token(
        &store,
        OAuthClientCredentialsToken {
            client_id: "client-power-monitor".parse().unwrap(),
            client_secret: "client-credentials-secret".to_owned(),
            access_token: "oauth-client-credentials-token".to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at,
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(issued.app_id.as_str(), "power-monitor");
    assert_eq!(issued.user_id, None);
    assert_eq!(issued.scopes, ["devices:read"]);

    let expanded = OAuthRepository::issue_client_credentials_access_token(
        &store,
        OAuthClientCredentialsToken {
            client_id: "client-power-monitor".parse().unwrap(),
            client_secret: "client-credentials-secret".to_owned(),
            access_token: "oauth-client-credentials-expansion-token".to_owned(),
            scopes: vec!["alerts:read".to_owned(), "devices:read".to_owned()],
            issued_at: issued_at + Duration::minutes(1),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        expanded,
        Err(PlatformStoreError::OAuthScopeDenied)
    ));
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 1);
}

#[tokio::test]
async fn sqlite_oauth_resolves_exact_access_token_scopes_and_denies_expiry() {
    let (_directory, store) = sqlite_store().await;
    let issued_at = Utc
        .with_ymd_and_hms(2026, 9, 13, 9, 10, 11)
        .single()
        .unwrap();
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            client_secret: "access-token-resolution-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    OAuthRepository::issue_client_credentials_access_token(
        &store,
        OAuthClientCredentialsToken {
            client_id: "client-power-monitor".parse().unwrap(),
            client_secret: "access-token-resolution-secret".to_owned(),
            access_token: "oauth-access-token-to-resolve".to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at,
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();

    let resolved = OAuthRepository::resolve_access_token(
        &store,
        "oauth-access-token-to-resolve",
        issued_at + Duration::minutes(30),
    )
    .await
    .unwrap();
    assert_eq!(resolved.app_id.as_str(), "power-monitor");
    assert_eq!(resolved.user_id, None);
    assert_eq!(resolved.scopes, ["devices:read"]);
    assert_eq!(resolved.expires_at, issued_at + Duration::hours(1));

    let expired = OAuthRepository::resolve_access_token(
        &store,
        "oauth-access-token-to-resolve",
        issued_at + Duration::hours(1),
    )
    .await;
    assert!(matches!(
        expired,
        Err(PlatformStoreError::OAuthAccessTokenDenied)
    ));
}

#[tokio::test]
async fn sqlite_oauth_rejects_a_nonfuture_access_token_expiry_before_consuming_a_code() {
    let (_directory, store) = sqlite_store().await;
    let code_issued_at = Utc
        .with_ymd_and_hms(2026, 9, 13, 10, 11, 12)
        .single()
        .unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-token-expiry-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: code_issued_at,
            expires_at: code_issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let exchange_at = code_issued_at + Duration::minutes(1);
    let exchange = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-token-expiry-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-token-with-invalid-expiry".to_owned(),
            issued_at: exchange_at,
            expires_at: exchange_at,
        },
    )
    .await;
    assert!(matches!(
        exchange,
        Err(PlatformStoreError::InvalidOAuthAccessTokenExpiry)
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
}

#[tokio::test]
async fn sqlite_oauth_denies_an_incorrect_s256_verifier_without_consuming_the_code() {
    let (_directory, store) = sqlite_store().await;
    let issued_at = Utc
        .with_ymd_and_hms(2026, 9, 13, 11, 12, 13)
        .single()
        .unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-pkce-retry-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at,
            expires_at: issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let mismatch = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-pkce-retry-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: "wrong-pkce-verifier".to_owned(),
            client_secret: None,
            access_token: "oauth-pkce-mismatch-token".to_owned(),
            issued_at: issued_at + Duration::minutes(1),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        mismatch,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);

    let retried = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-pkce-retry-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-pkce-correct-retry-token".to_owned(),
            issued_at: issued_at + Duration::minutes(2),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(retried.user_id, Some(user_id));
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 1);

    let replay = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-pkce-retry-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-pkce-replay-token".to_owned(),
            issued_at: issued_at + Duration::minutes(3),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        replay,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 1);
}

#[tokio::test]
async fn sqlite_oauth_exchanges_a_code_with_the_matching_s256_verifier() {
    let (_directory, store) = sqlite_store().await;
    let issued_at = Utc
        .with_ymd_and_hms(2026, 9, 13, 12, 13, 14)
        .single()
        .unwrap();
    let user_id = Uuid::new_v4();
    seed_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "oauth-pkce-success-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at,
            expires_at: issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let exchanged = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "oauth-pkce-success-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: None,
            access_token: "oauth-pkce-success-token".to_owned(),
            issued_at: issued_at + Duration::minutes(1),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(exchanged.user_id, Some(user_id));
    assert_eq!(exchanged.scopes, ["devices:read"]);
}

#[tokio::test]
#[ignore = "requires IOT_NANO_TIMESCALE_TEST_URL"]
async fn timescale_oauth_repository_matches_sqlite_contract() {
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
    common::reset_timescale_schema(&mut connection)
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
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES ($1, 'oauth-persistence', 'active')",
    )
    .bind(test_tenant_id())
    .execute(store.timescale_pool().unwrap())
    .await
    .unwrap();
    let issued_at = Utc
        .with_ymd_and_hms(2026, 9, 13, 10, 11, 12)
        .single()
        .unwrap();
    let user_id = Uuid::new_v4();
    seed_timescale_user(&store, user_id).await;
    ApplicationRepository::upsert_application(&store, application(true))
        .await
        .unwrap();
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            client_secret: "timescale-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    OAuthRepository::issue_authorization_code(
        &store,
        NewOAuthAuthorizationCode {
            code: "timescale-authorization-code".to_owned(),
            app_id: "power-monitor".parse().unwrap(),
            tenant_id: test_tenant_id(),
            user_id,
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_challenge: S256_CODE_CHALLENGE.to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at,
            expires_at: issued_at + Duration::minutes(10),
        },
    )
    .await
    .unwrap();
    let mismatch = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "timescale-authorization-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: "timescale-wrong-pkce-verifier".to_owned(),
            client_secret: Some("timescale-client-secret".to_owned()),
            access_token: "timescale-pkce-mismatch-token".to_owned(),
            issued_at: issued_at + Duration::minutes(1),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        mismatch,
        Err(PlatformStoreError::OAuthAuthorizationCodeDenied)
    ));
    let consumed_at: Option<chrono::DateTime<Utc>> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.timescale_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
    let token_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_access_tokens")
        .fetch_one(store.timescale_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(token_count, 0);
    let code_token = OAuthRepository::consume_authorization_code_and_issue_access_token(
        &store,
        OAuthAuthorizationCodeExchange {
            code: "timescale-authorization-code".to_owned(),
            client_id: "client-power-monitor".parse().unwrap(),
            redirect_uri: "https://apps.example.test/callback".parse().unwrap(),
            code_verifier: S256_CODE_VERIFIER.to_owned(),
            client_secret: Some("timescale-client-secret".to_owned()),
            access_token: "timescale-code-access-token".to_owned(),
            issued_at: issued_at + Duration::minutes(1),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(code_token.user_id, Some(user_id));
    assert_eq!(code_token.scopes, ["devices:read"]);

    let client_token = OAuthRepository::issue_client_credentials_access_token(
        &store,
        OAuthClientCredentialsToken {
            client_id: "client-power-monitor".parse().unwrap(),
            client_secret: "timescale-client-secret".to_owned(),
            access_token: "timescale-client-credentials-token".to_owned(),
            scopes: vec!["devices:read".to_owned()],
            issued_at: issued_at + Duration::minutes(2),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await
    .unwrap();
    assert_eq!(client_token.user_id, None);
    let expanded = OAuthRepository::issue_client_credentials_access_token(
        &store,
        OAuthClientCredentialsToken {
            client_id: "client-power-monitor".parse().unwrap(),
            client_secret: "timescale-client-secret".to_owned(),
            access_token: "timescale-expanded-scope-token".to_owned(),
            scopes: vec!["alerts:read".to_owned(), "devices:read".to_owned()],
            issued_at: issued_at + Duration::minutes(3),
            expires_at: issued_at + Duration::hours(1),
        },
    )
    .await;
    assert!(matches!(
        expanded,
        Err(PlatformStoreError::OAuthScopeDenied)
    ));

    let resolved = OAuthRepository::resolve_access_token(
        &store,
        "timescale-client-credentials-token",
        issued_at + Duration::minutes(30),
    )
    .await
    .unwrap();
    assert_eq!(resolved.app_id.as_str(), "power-monitor");
    assert_eq!(resolved.user_id, None);
    assert_eq!(resolved.scopes, ["devices:read"]);
}
