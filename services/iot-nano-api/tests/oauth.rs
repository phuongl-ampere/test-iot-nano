use axum::{
    body::{Body, to_bytes},
    http::{
        HeaderMap, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, COOKIE, LOCATION},
    },
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use iot_api::{
    ApiState, BearerAccessTokenError, Role, SqliteApiState, bootstrap_users_sqlite, routers,
    sqlite_router, validate_bearer_access_token,
};
use iot_core::{DatabaseStorage, StorageConfiguration};
use iot_storage::{
    ApplicationKind, ApplicationRepository, NewApplication, NewOAuthClientSecret, OAuthRepository,
    PlatformStore, SqliteStore,
};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;
use uuid::Uuid;

const BROWSER_SESSION: &str = "oauth-browser-session";
const CLIENT_ID: &str = "oauth-test-client";
const REDIRECT_URI: &str = "https://client.example.test/callback";

async fn oauth_test_state(enabled: bool) -> (tempfile::TempDir, PlatformStore, ApiState) {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("oauth.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = PlatformStore::open(&configuration).await.unwrap();
    let pool = store.sqlite_pool().unwrap();
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, role, account_class, default_app)
         VALUES (?, 'browser-user', 'unused', 'admin', 'admin', '/apps/powermonitor')",
    )
    .bind(Uuid::nil().to_string())
    .execute(pool)
    .await
    .unwrap();
    ApplicationRepository::upsert_application(
        &store,
        NewApplication {
            app_id: "oauth-test-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled,
        },
    )
    .await
    .unwrap();
    let api_pool = PgPoolOptions::new()
        .connect_lazy("postgres://oauth:oauth@localhost/oauth")
        .unwrap();
    let state = ApiState::new(api_pool)
        .with_session(BROWSER_SESSION, "browser-user", Role::Admin)
        .with_oauth_store(store.clone());

    (directory, store, state)
}

fn s256_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

async fn authorize_code(state: ApiState, verifier: &str) -> String {
    let challenge = s256_challenge(verifier);
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();
    let response = routers(state).public.oneshot(request).await.unwrap();
    let location = response.headers().get(LOCATION).unwrap().to_str().unwrap();
    location
        .split_once('?')
        .unwrap()
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix("code="))
        .unwrap()
        .to_owned()
}

fn authorization_code_token_request(code: &str, verifier: &str) -> Request<Body> {
    authorization_code_token_request_for(
        code,
        verifier,
        CLIENT_ID,
        "https%3A%2F%2Fclient.example.test%2Fcallback",
    )
}

fn authorization_code_token_request_for(
    code: &str,
    verifier: &str,
    client_id: &str,
    encoded_redirect_uri: &str,
) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/oauth/token")
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(format!(
            "grant_type=authorization_code&code={code}&redirect_uri={encoded_redirect_uri}&client_id={client_id}&code_verifier={verifier}"
        )))
        .unwrap()
}

fn client_credentials_token_request(client_secret: &str, scope: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/oauth/token")
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(format!(
            "grant_type=client_credentials&client_id={CLIENT_ID}&client_secret={client_secret}&scope={scope}"
        )))
        .unwrap()
}

#[tokio::test]
async fn authorization_endpoint_issues_an_s256_bound_code_on_the_public_router() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let challenge = s256_challenge(verifier);
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["pragma"], "no-cache");
    let location = response.headers().get(LOCATION).unwrap().to_str().unwrap();
    assert!(location.starts_with("https://client.example.test/callback?"));
    assert!(location.ends_with("&state=carry-me"));
    let code = location
        .split_once('?')
        .unwrap()
        .1
        .split('&')
        .find_map(|pair| pair.strip_prefix("code="))
        .unwrap();
    assert_eq!(code.len(), 43);
    assert!(
        code.bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    );

    let (code_hash, code_challenge): (String, String) =
        sqlx::query_as("SELECT code_hash, code_challenge FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_ne!(code_hash, code);
    assert_eq!(code_hash.len(), 64);
    assert_eq!(code_challenge, challenge);
}

#[tokio::test]
async fn authorization_endpoint_requires_a_nonempty_state_before_issuing_a_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");

    for state_parameter in ["", "&state="] {
        let response = routers(state.clone())
            .public
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread{state_parameter}&code_challenge={challenge}&code_challenge_method=S256"
                    ))
                    .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["error"], "invalid_request");
    }

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn authorization_endpoint_requires_a_nonempty_scope_before_issuing_a_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");

    for scope_parameter in ["", "&scope="] {
        let response = routers(state.clone())
            .public
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback{scope_parameter}&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
                    ))
                    .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["error"], "invalid_request");
    }

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn token_endpoint_exchanges_a_code_with_its_original_s256_verifier() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let code = authorize_code(state.clone(), verifier).await;
    let request = authorization_code_token_request(&code, verifier);

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["pragma"], "no-cache");
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let access_token = payload["access_token"].as_str().unwrap();
    assert_eq!(access_token.len(), 43);
    assert_eq!(payload["token_type"], "Bearer");
    assert_eq!(payload["scope"], "devices:read");
    assert_eq!(payload["expires_in"], 3_600);

    let token_hash: String = sqlx::query_scalar("SELECT token_hash FROM oauth_access_tokens")
        .fetch_one(store.sqlite_pool().unwrap())
        .await
        .unwrap();
    assert_ne!(token_hash, access_token);
    assert_eq!(token_hash.len(), 64);
}

#[tokio::test]
async fn token_endpoint_rejects_a_code_verifier_that_does_not_match_s256() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let code = authorize_code(
        state.clone(),
        "correct-pkce-verifier-with-at-least-forty-three-characters",
    )
    .await;
    let response = routers(state)
        .public
        .oneshot(authorization_code_token_request(
            &code,
            "incorrect-pkce-verifier-with-at-least-forty-characters",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_grant");
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
}

#[tokio::test]
async fn token_endpoint_issues_client_credentials_for_the_exact_requested_scope() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();

    let response = routers(state)
        .public
        .oneshot(client_credentials_token_request(
            "correct-confidential-client-secret",
            "devices%3Aread",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let access_token = payload["access_token"].as_str().unwrap();
    assert_eq!(access_token.len(), 43);
    assert_eq!(payload["scope"], "devices:read");
    let (token_hash, user_id): (String, Option<String>) =
        sqlx::query_as("SELECT token_hash, user_id FROM oauth_access_tokens")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert_ne!(token_hash, access_token);
    assert!(user_id.is_none());
}

#[tokio::test]
async fn token_endpoint_requires_a_nonempty_client_credentials_scope() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();

    for body in [
        format!(
            "grant_type=client_credentials&client_id={CLIENT_ID}&client_secret=correct-confidential-client-secret"
        ),
        format!(
            "grant_type=client_credentials&client_id={CLIENT_ID}&client_secret=correct-confidential-client-secret&scope="
        ),
    ] {
        let response = routers(state.clone())
            .public
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/token")
                    .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let payload: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(payload["error"], "invalid_request");
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_access_tokens")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn token_endpoint_authenticates_a_confidential_client_with_http_basic() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    let basic = STANDARD.encode(format!("{CLIENT_ID}:correct-confidential-client-secret"));
    let response = routers(state)
        .public
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(AUTHORIZATION, format!("Basic {basic}"))
                .body(Body::from(
                    "grant_type=client_credentials&scope=devices%3Aread",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["scope"], "devices:read");
}

#[tokio::test]
async fn token_endpoint_never_uses_a_browser_session_as_client_authentication() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let response = routers(state)
        .public
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(AUTHORIZATION, format!("Bearer {BROWSER_SESSION}"))
                .body(Body::from(
                    "grant_type=client_credentials&scope=devices%3Aread",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"], "invalid_client");
    assert!(!String::from_utf8_lossy(&body).contains(BROWSER_SESSION));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_access_tokens")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn browser_login_sets_the_cookie_used_by_oauth_authorization() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("browser-login.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(store.pool()).await.unwrap();
    let response = sqlite_router(SqliteApiState::new(store))
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .to_owned();
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let session_id = payload["session_id"].as_str().unwrap();
    assert!(cookie.starts_with(format!("iot_nano_session={session_id};").as_str()));
    assert!(cookie.contains("HttpOnly"));
    assert!(cookie.contains("Secure"));
    assert!(cookie.contains("SameSite=Lax"));
    assert!(cookie.contains("Path=/"));
}

#[tokio::test]
async fn sqlite_public_router_mounts_the_oauth_authorization_endpoint() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = StorageConfiguration {
        storage: DatabaseStorage::Sqlite,
        database_url: None,
        sqlite_path: Some(directory.path().join("sqlite-oauth.db")),
        sqlite_busy_timeout_ms: 5_000,
    };
    let oauth_store = PlatformStore::open(&configuration).await.unwrap();
    let api_store = SqliteStore::open(&configuration).await.unwrap();
    bootstrap_users_sqlite(api_store.pool()).await.unwrap();
    ApplicationRepository::upsert_application(
        &oauth_store,
        NewApplication {
            app_id: "oauth-test-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://client.example.test".to_owned(),
            client_id: CLIENT_ID.parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let app = sqlite_router(SqliteApiState::new(api_store).with_oauth_store(oauth_store));
    let login = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"username":"admin","password":"NanoAdmin@1234"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = login.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
                ))
                .header(COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FOUND);
}

#[tokio::test]
async fn token_endpoint_hides_a_confidential_client_secret_mismatch() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();

    let response = routers(state)
        .public
        .oneshot(client_credentials_token_request(
            "incorrect-confidential-client-secret",
            "devices%3Aread",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_client");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_access_tokens")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn token_endpoint_hides_a_confidential_secret_mismatch_for_authorization_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let code = authorize_code(state.clone(), verifier).await;
    let response = routers(state)
        .public
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "grant_type=authorization_code&code={code}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&client_id={CLIENT_ID}&client_secret=incorrect-confidential-client-secret&code_verifier={verifier}"
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_client");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_access_tokens")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn token_endpoint_does_not_expand_client_credentials_scopes() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();

    let response = routers(state)
        .public
        .oneshot(client_credentials_token_request(
            "correct-confidential-client-secret",
            "alerts%3Aread+devices%3Aread",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(response.headers()["pragma"], "no-cache");
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_scope");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_access_tokens")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn bearer_validation_resolves_an_oauth_access_token_with_its_exact_scope() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    let response = routers(state)
        .public
        .oneshot(client_credentials_token_request(
            "correct-confidential-client-secret",
            "devices%3Aread",
        ))
        .await
        .unwrap();
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        format!("Bearer {}", payload["access_token"].as_str().unwrap())
            .parse()
            .unwrap(),
    );

    let context = validate_bearer_access_token(&store, &headers, chrono::Utc::now())
        .await
        .unwrap();

    assert_eq!(context.app_id, "oauth-test-app");
    assert_eq!(context.user_id, None);
    assert_eq!(context.scopes, ["devices:read"]);
    assert!(context.allows_scope("devices:read"));
    assert!(!context.allows_scope("alerts:read"));
}

#[tokio::test]
async fn bearer_validation_denies_an_expired_oauth_access_token() {
    let (_directory, store, state) = oauth_test_state(true).await;
    OAuthRepository::register_client_secret(
        &store,
        NewOAuthClientSecret {
            app_id: "oauth-test-app".parse().unwrap(),
            client_secret: "correct-confidential-client-secret".to_owned(),
        },
    )
    .await
    .unwrap();
    let response = routers(state)
        .public
        .oneshot(client_credentials_token_request(
            "correct-confidential-client-secret",
            "devices%3Aread",
        ))
        .await
        .unwrap();
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        format!("Bearer {}", payload["access_token"].as_str().unwrap())
            .parse()
            .unwrap(),
    );

    assert_eq!(
        validate_bearer_access_token(
            &store,
            &headers,
            chrono::Utc::now() + chrono::Duration::hours(2),
        )
        .await,
        Err(BearerAccessTokenError::Denied)
    );
}

#[tokio::test]
async fn authorization_endpoint_rejects_an_unregistered_redirect_uri_without_issuing_a_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Funregistered.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_request");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn authorization_endpoint_rejects_a_fragment_redirect_uri_before_issuing_a_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback%23fragment&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn authorization_endpoint_denies_a_disabled_application_without_issuing_a_code() {
    let (_directory, store, state) = oauth_test_state(false).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "unauthorized_client");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn authorization_endpoint_does_not_issue_a_code_for_an_unallowed_scope() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=alerts%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_scope");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn authorization_endpoint_denies_an_unknown_client_without_issuing_a_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id=unknown-client&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(COOKIE, format!("iot_nano_session={BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "unauthorized_client");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn token_endpoint_rejects_an_authorization_code_after_it_is_consumed() {
    let (_directory, _store, state) = oauth_test_state(true).await;
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let code = authorize_code(state.clone(), verifier).await;
    let first = routers(state.clone())
        .public
        .oneshot(authorization_code_token_request(&code, verifier))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let response = routers(state)
        .public
        .oneshot(authorization_code_token_request(&code, verifier))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"], "invalid_grant");
    assert!(!String::from_utf8_lossy(&body).contains(&code));
}

#[tokio::test]
async fn token_endpoint_rejects_an_expired_authorization_code() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let issued_at = chrono::Utc::now() - chrono::Duration::minutes(20);
    OAuthRepository::issue_authorization_code(
        &store,
        iot_storage::NewOAuthAuthorizationCode {
            code: "expired-authorization-code".to_owned(),
            app_id: "oauth-test-app".parse().unwrap(),
            user_id: Uuid::nil(),
            redirect_uri: REDIRECT_URI.parse().unwrap(),
            code_challenge: s256_challenge(verifier),
            scopes: vec!["devices:read".to_owned()],
            issued_at,
            expires_at: issued_at + chrono::Duration::minutes(10),
        },
    )
    .await
    .unwrap();

    let response = routers(state)
        .public
        .oneshot(authorization_code_token_request(
            "expired-authorization-code",
            verifier,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_grant");
}

#[tokio::test]
async fn token_endpoint_preserves_a_code_redirect_uri_binding() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let code = authorize_code(state.clone(), verifier).await;

    let response = routers(state)
        .public
        .oneshot(authorization_code_token_request_for(
            &code,
            verifier,
            CLIENT_ID,
            "https%3A%2F%2Fclient.example.test%2Fdifferent-callback",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_grant");
    let consumed_at: Option<String> =
        sqlx::query_scalar("SELECT consumed_at FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap();
    assert!(consumed_at.is_none());
}

#[tokio::test]
async fn token_endpoint_preserves_a_code_client_binding() {
    let (_directory, store, state) = oauth_test_state(true).await;
    ApplicationRepository::upsert_application(
        &store,
        NewApplication {
            app_id: "oauth-other-app".parse().unwrap(),
            kind: ApplicationKind::FullStack,
            launch_url: "https://other.example.test".to_owned(),
            client_id: "oauth-other-client".parse().unwrap(),
            redirect_uris: vec![REDIRECT_URI.parse().unwrap()],
            allowed_scopes: vec!["devices:read".to_owned()],
            enabled: true,
        },
    )
    .await
    .unwrap();
    let verifier = "correct-pkce-verifier-with-at-least-forty-three-characters";
    let code = authorize_code(state.clone(), verifier).await;

    let response = routers(state)
        .public
        .oneshot(authorization_code_token_request_for(
            &code,
            verifier,
            "oauth-other-client",
            "https%3A%2F%2Fclient.example.test%2Fcallback",
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_grant");
}

#[tokio::test]
async fn oauth_authorization_uses_a_browser_cookie_not_a_bearer_session() {
    let (_directory, store, state) = oauth_test_state(true).await;
    let challenge = s256_challenge("correct-pkce-verifier-with-at-least-forty-three-characters");
    let request = Request::builder()
        .uri(format!(
            "/oauth/authorize?response_type=code&client_id={CLIENT_ID}&redirect_uri=https%3A%2F%2Fclient.example.test%2Fcallback&scope=devices%3Aread&state=carry-me&code_challenge={challenge}&code_challenge_method=S256"
        ))
        .header(AUTHORIZATION, format!("Bearer {BROWSER_SESSION}"))
        .body(Body::empty())
        .unwrap();

    let response = routers(state).public.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"], "access_denied");
    assert!(!String::from_utf8_lossy(&body).contains(BROWSER_SESSION));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(store.sqlite_pool().unwrap())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn oauth_routes_are_available_only_from_the_public_router() {
    let (_directory, _store, state) = oauth_test_state(true).await;
    let public = routers(state.clone())
        .public
        .oneshot(
            Request::builder()
                .uri("/oauth/authorize")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let management = routers(state)
        .management
        .oneshot(
            Request::builder()
                .uri("/oauth/authorize")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(public.status(), StatusCode::BAD_REQUEST);
    assert_eq!(management.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn token_endpoint_returns_an_oauth_envelope_for_an_invalid_form_content_type() {
    let (_directory, _store, state) = oauth_test_state(true).await;
    let response = routers(state)
        .public
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/token")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"grant_type":"client_credentials"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(payload["error"], "invalid_request");
}
