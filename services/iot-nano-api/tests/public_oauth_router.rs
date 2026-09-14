use std::sync::Arc;

use axum::{
    body::{Body, to_bytes},
    http::{
        Request, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, PRAGMA},
    },
};
use iot_api::public_oauth_router;
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret, OAuthRepository,
    PlatformStore,
};
use tower::ServiceExt;

const APP_ID: &str = "public-oauth-router-app";
const CLIENT_ID: &str = "public-oauth-router-client";
const CLIENT_SECRET: &str = "public-oauth-router-client-secret";

async fn public_oauth_app() -> (tempfile::TempDir, axum::Router) {
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
    ApplicationRepository::upsert_application(
        store.as_ref(),
        NewApplication {
            app_id: APP_ID.parse().unwrap(),
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
            client_secret: CLIENT_SECRET.to_owned(),
        },
    )
    .await
    .unwrap();

    (directory, public_oauth_router::<()>(store))
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
                .uri("/oauth/authorize")
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
    assert_eq!(payload["error"], "unsupported_grant_type");
}
