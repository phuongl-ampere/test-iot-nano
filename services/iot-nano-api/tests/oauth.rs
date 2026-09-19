use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{
        HeaderMap, Request, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, PRAGMA},
    },
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration, Utc};
use iot_api::{
    OAuthBrowserSessionVerifier, public_oauth_router,
    public_oauth_router_with_browser_session_verifier,
};
use iot_nano_foundation::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret, OAuthRepository,
    PlatformStore,
};
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use uuid::Uuid;

const APP_ID: &str = "public-oauth-router-app";
const CLIENT_ID: &str = "public-oauth-router-client";
const CLIENT_SECRET: &str = "public-oauth-router-client-secret";
const TRUSTED_SESSION_HEADER: &str = "x-test-oauth-session";

fn tenant_id() -> Uuid {
    Uuid::from_u128(10_005)
}

async fn public_oauth_store() -> (tempfile::TempDir, Arc<PlatformStore>) {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(
        PlatformStore::open(&StorageConfiguration {
            storage: DatabaseStorage::Sqlite,
            database_url: None,
            sqlite_path: Some(directory.path().join("public-oauth-router.sqlite")),
            sqlite_busy_timeout_ms: 5_000,
        })
        .await
        .unwrap(),
    );
    sqlx::query("INSERT INTO tenants (id, slug, status) VALUES (?, 'public-oauth', 'active')")
        .bind(tenant_id().to_string())
        .execute(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    ApplicationRepository::upsert_application(
        store.as_ref(),
        NewApplication {
            app_id: APP_ID.parse().unwrap(),
            tenant_id: tenant_id(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec!["https://client.example.test/callback".parse().unwrap()],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    OAuthRepository::register_client_secret(
        store.as_ref(),
        NewOAuthClientSecret {
            app_id: APP_ID.parse().unwrap(),
            tenant_id: tenant_id(),
            client_secret: CLIENT_SECRET.to_owned(),
        },
    )
    .await
    .unwrap();

    (directory, store)
}

async fn public_oauth_app() -> (tempfile::TempDir, axum::Router) {
    let (directory, store) = public_oauth_store().await;
    (directory, public_oauth_router::<()>(store))
}

struct FixedSessionVerifier {
    user_id: Uuid,
}

impl OAuthBrowserSessionVerifier for FixedSessionVerifier {
    fn authenticated_user_id(&self, headers: &HeaderMap) -> Option<Uuid> {
        headers
            .get(TRUSTED_SESSION_HEADER)
            .filter(|value| value.as_bytes() == b"trusted")
            .map(|_| self.user_id)
    }
}

#[tokio::test]
async fn exported_public_oauth_router_uses_platform_store_and_fails_closed_for_code_flow() {
    let (_directory, app) = public_oauth_app().await;

    let client_credentials = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=client_credentials&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&scope=devices%3Aread"
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(client_credentials.status(), StatusCode::OK);
    assert_eq!(client_credentials.headers()[CACHE_CONTROL], "no-store");
    assert_eq!(client_credentials.headers()[PRAGMA], "no-cache");
    let payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(client_credentials.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload["token_type"], "Bearer");
    assert_eq!(payload["scope"], "devices:read");

    let authorize = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={}&code_challenge_method=S256",
                    URL_SAFE_NO_PAD.encode(Sha256::digest(b"missing-session-pkce-verifier-with-at-least-forty-three-characters")),
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(authorize.status(), StatusCode::UNAUTHORIZED);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(authorize.into_body(), usize::MAX).await.unwrap())
            .unwrap();
    assert_eq!(payload["error"], "access_denied");

    let authorization_code = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code=untrusted-code&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&client_id={CLIENT_ID}&code_verifier=untrusted-verifier"
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(authorization_code.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(authorization_code.into_body(), usize::MAX)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload["error"], "invalid_grant");
}

#[tokio::test]
async fn exported_public_oauth_router_issues_and_exchanges_pkce_codes_from_a_trusted_session() {
    let (directory, store) = public_oauth_store().await;
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class, default_app)
         VALUES (?, ?, 'oauth-browser-user', 'unused', 'admin', 'admin', '/apps/powermonitor')",
    )
    .bind(user_id.to_string())
    .bind(tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let app = public_oauth_router_with_browser_session_verifier::<()>(
        Arc::clone(&store),
        Arc::new(FixedSessionVerifier { user_id }),
    );
    let verifier = "public-router-pkce-verifier-with-at-least-forty-three-characters";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));

    let authorize = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
                ))
                .header(TRUSTED_SESSION_HEADER, "trusted")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(authorize.status(), StatusCode::FOUND);
    let location = authorize.headers()["location"].to_str().unwrap();
    let code = location
        .split_once('?')
        .unwrap()
        .1
        .split('&')
        .find_map(|part| part.strip_prefix("code="))
        .unwrap();
    let (issued_at, expires_at): (String, String) =
        sqlx::query_as("SELECT issued_at, expires_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    let issued_at = DateTime::parse_from_rfc3339(&issued_at)
        .unwrap()
        .with_timezone(&Utc);
    let expires_at = DateTime::parse_from_rfc3339(&expires_at)
        .unwrap()
        .with_timezone(&Utc);
    assert_eq!(expires_at - issued_at, Duration::minutes(5));
    let exchange = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&code_verifier={verifier}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exchange.status(), StatusCode::OK);

    let replay = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&code_verifier={verifier}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(replay.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_grant");

    drop(directory);
}

#[tokio::test]
async fn exported_public_oauth_router_denies_a_trusted_user_from_another_tenant() {
    let (_directory, store) = public_oauth_store().await;
    let foreign_tenant_id = Uuid::now_v7();
    let foreign_user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO tenants (id, slug, status) VALUES (?, 'public-oauth-foreign', 'active')",
    )
    .bind(foreign_tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class, default_app)
         VALUES (?, ?, 'oauth-foreign-user', 'unused', 'admin', 'admin', '/apps/powermonitor')",
    )
    .bind(foreign_user_id.to_string())
    .bind(foreign_tenant_id.to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let app = public_oauth_router_with_browser_session_verifier::<()>(
        Arc::clone(&store),
        Arc::new(FixedSessionVerifier {
            user_id: foreign_user_id,
        }),
    );
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(
        b"public-router-cross-tenant-pkce-verifier-with-at-least-forty-three-characters",
    ));

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
                ))
                .header(TRUSTED_SESSION_HEADER, "trusted")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "access_denied");
    let codes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_authorization_codes")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_eq!(codes, 0);
}

#[tokio::test]
async fn exported_public_oauth_router_rejects_a_short_pkce_verifier_even_when_its_hash_matches() {
    let (directory, store) = public_oauth_store().await;
    let user_id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO users (id, tenant_id, username, password_hash, role, account_class, default_app)
         VALUES (?, ?, 'oauth-short-verifier-user', 'unused', 'admin', 'admin', '/apps/powermonitor')",
    )
    .bind(user_id.to_string())
    .bind(tenant_id().to_string())
    .execute(store.sqlite_pool().unwrap())
    .await
    .unwrap();
    let app = public_oauth_router_with_browser_session_verifier::<()>(
        store,
        Arc::new(FixedSessionVerifier { user_id }),
    );
    let verifier = "short";
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let authorize = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
                ))
                .header(TRUSTED_SESSION_HEADER, "trusted")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let code = authorize.headers()["location"]
        .to_str()
        .unwrap()
        .split_once('?')
        .unwrap()
        .1
        .split('&')
        .find_map(|part| part.strip_prefix("code="))
        .unwrap();

    let exchange = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&code_verifier={verifier}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(exchange.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(exchange.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_grant");

    drop(directory);
}
