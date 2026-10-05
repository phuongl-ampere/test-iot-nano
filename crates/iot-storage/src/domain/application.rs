use std::{future::Future, pin::Pin};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Row, error::DatabaseError, postgres::PgRow, sqlite::SqliteRow, types::Json};
use subtle::ConstantTimeEq;

use crate::{
    ApplicationId, ApplicationKind, ApplicationRecord, ApplicationRepository, ClientId,
    NewApplication, NewOAuthAuthorizationCode, NewOAuthClientSecret, OAuthAccessTokenRecord,
    OAuthAuthorizationCodeExchange, OAuthClientCredentialsToken, OAuthRepository, PlatformStore,
    PlatformStoreError,
};

impl PlatformStore {
    pub async fn upsert_application(
        &self,
        mut application: NewApplication,
    ) -> Result<ApplicationRecord, PlatformStoreError> {
        validate_application(&mut application)?;
        if let Some(existing) = self
            .find_application_by_app_id(application.app_id.as_str())
            .await?
            && existing.tenant_id != application.tenant_id
        {
            return Err(PlatformStoreError::ApplicationTenantConflict(
                application.app_id.clone(),
            ));
        }
        if self
            .list_applications_for_tenant(application.tenant_id)
            .await?
            .into_iter()
            .any(|existing| existing.app_id != application.app_id)
        {
            return Err(PlatformStoreError::TenantApplicationLimit(
                application.tenant_id,
            ));
        }
        match self {
            Self::Sqlite(store) => {
                let conflicting_app_id = sqlx::query_scalar::<_, String>(
                    "SELECT app_id FROM applications
                     WHERE client_id = ? AND app_id <> ?",
                )
                .bind(application.client_id.as_str())
                .bind(application.app_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                if conflicting_app_id.is_some() {
                    return Err(PlatformStoreError::ApplicationClientIdConflict(
                        application.client_id.as_str().to_owned(),
                    ));
                }
            }
            Self::Timescale(pool) => {
                let conflicting_app_id = sqlx::query_scalar::<_, String>(
                    "SELECT app_id FROM applications
                     WHERE client_id = $1 AND app_id <> $2",
                )
                .bind(application.client_id.as_str())
                .bind(application.app_id.as_str())
                .fetch_optional(pool)
                .await?;
                if conflicting_app_id.is_some() {
                    return Err(PlatformStoreError::ApplicationClientIdConflict(
                        application.client_id.as_str().to_owned(),
                    ));
                }
            }
        }
        let scopes = serde_json::to_string(&application.allowed_scopes)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?;
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                let result = sqlx::query(
                    "INSERT INTO applications (
                        app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (app_id) DO UPDATE SET
                        kind = excluded.kind,
                        launch_url = excluded.launch_url,
                        client_id = excluded.client_id,
                        allowed_scopes_json = excluded.allowed_scopes_json,
                        enabled = excluded.enabled
                     WHERE applications.tenant_id = excluded.tenant_id",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .bind(application.kind.as_str())
                .bind(&application.launch_url)
                .bind(application.client_id.as_str())
                .bind(&scopes)
                .bind(application.enabled)
                .execute(&mut *transaction)
                .await
                .map_err(|error| {
                    map_application_write_conflict(
                        error,
                        application.client_id.as_str(),
                        application.tenant_id,
                    )
                })?;
                if result.rows_affected() != 1 {
                    return Err(PlatformStoreError::ApplicationTenantConflict(
                        application.app_id.clone(),
                    ));
                }
                sqlx::query(
                    "DELETE FROM application_redirect_uris WHERE app_id = ? AND tenant_id = ?",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .execute(&mut *transaction)
                .await?;
                for redirect_uri in &application.redirect_uris {
                    sqlx::query(
                        "INSERT INTO application_redirect_uris (app_id, tenant_id, redirect_uri)
                         VALUES (?, ?, ?)",
                    )
                    .bind(application.app_id.as_str())
                    .bind(application.tenant_id.to_string())
                    .bind(redirect_uri.as_str())
                    .execute(&mut *transaction)
                    .await?;
                }
                transaction.commit().await?;
                self.find_application_by_app_id(application.app_id.as_str())
                    .await?
                    .ok_or_else(|| {
                        PlatformStoreError::InvalidApplicationId(
                            application.app_id.as_str().to_owned(),
                        )
                    })
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let result = sqlx::query(
                    "INSERT INTO applications (
                        app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                     ON CONFLICT (app_id) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        launch_url = EXCLUDED.launch_url,
                        client_id = EXCLUDED.client_id,
                        allowed_scopes_json = EXCLUDED.allowed_scopes_json,
                        enabled = EXCLUDED.enabled
                     WHERE applications.tenant_id = EXCLUDED.tenant_id",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .bind(application.kind.as_str())
                .bind(&application.launch_url)
                .bind(application.client_id.as_str())
                .bind(&scopes)
                .bind(application.enabled)
                .execute(&mut *transaction)
                .await
                .map_err(|error| {
                    map_application_write_conflict(
                        error,
                        application.client_id.as_str(),
                        application.tenant_id,
                    )
                })?;
                if result.rows_affected() != 1 {
                    return Err(PlatformStoreError::ApplicationTenantConflict(
                        application.app_id.clone(),
                    ));
                }
                sqlx::query(
                    "DELETE FROM application_redirect_uris WHERE app_id = $1 AND tenant_id = $2",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .execute(&mut *transaction)
                .await?;
                for redirect_uri in &application.redirect_uris {
                    sqlx::query(
                        "INSERT INTO application_redirect_uris (app_id, tenant_id, redirect_uri)
                         VALUES ($1, $2, $3)",
                    )
                    .bind(application.app_id.as_str())
                    .bind(application.tenant_id)
                    .bind(redirect_uri.as_str())
                    .execute(&mut *transaction)
                    .await?;
                }
                transaction.commit().await?;
                self.find_application_by_app_id(application.app_id.as_str())
                    .await?
                    .ok_or_else(|| {
                        PlatformStoreError::InvalidApplicationId(
                            application.app_id.as_str().to_owned(),
                        )
                    })
            }
        }
    }

    pub async fn find_application_by_app_id(
        &self,
        app_id: &str,
    ) -> Result<Option<ApplicationRecord>, PlatformStoreError> {
        let app_id = app_id.parse::<ApplicationId>()?;
        match self {
            Self::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE app_id = ?",
                )
                .bind(app_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let tenant_id: String = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = ? AND tenant_id = ? ORDER BY redirect_uri",
                )
                .bind(app_id.as_str())
                .bind(tenant_id)
                .fetch_all(store.pool())
                .await?;
                sqlite_application_record(row, redirects).map(Some)
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE app_id = $1",
                )
                .bind(app_id.as_str())
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = $1 AND tenant_id = $2 ORDER BY redirect_uri",
                )
                .bind(app_id.as_str())
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                postgres_application_record(row, redirects).map(Some)
            }
        }
    }

    pub async fn list_applications_for_tenant(
        &self,
        tenant_id: uuid::Uuid,
    ) -> Result<Vec<ApplicationRecord>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE tenant_id = ? ORDER BY app_id",
                )
                .bind(tenant_id.to_string())
                .fetch_all(store.pool())
                .await?;
                let mut applications = Vec::with_capacity(rows.len());
                for row in rows {
                    let app_id: String = row.try_get("app_id")?;
                    let redirects = sqlx::query_scalar(
                        "SELECT redirect_uri FROM application_redirect_uris
                         WHERE app_id = ? AND tenant_id = ? ORDER BY redirect_uri",
                    )
                    .bind(&app_id)
                    .bind(tenant_id.to_string())
                    .fetch_all(store.pool())
                    .await?;
                    applications.push(sqlite_application_record(row, redirects)?);
                }
                Ok(applications)
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE tenant_id = $1 ORDER BY app_id",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                let mut applications = Vec::with_capacity(rows.len());
                for row in rows {
                    let app_id: String = row.try_get("app_id")?;
                    let redirects = sqlx::query_scalar(
                        "SELECT redirect_uri FROM application_redirect_uris
                         WHERE app_id = $1 AND tenant_id = $2 ORDER BY redirect_uri",
                    )
                    .bind(&app_id)
                    .bind(tenant_id)
                    .fetch_all(pool)
                    .await?;
                    applications.push(postgres_application_record(row, redirects)?);
                }
                Ok(applications)
            }
        }
    }

    pub async fn find_application_by_client_id(
        &self,
        client_id: &str,
    ) -> Result<Option<ApplicationRecord>, PlatformStoreError> {
        let client_id = client_id.parse::<ClientId>()?;
        let application = match self {
            Self::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE client_id = ?",
                )
                .bind(client_id.as_str())
                .fetch_optional(store.pool())
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: String = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = ? AND tenant_id = ? ORDER BY redirect_uri",
                )
                .bind(&app_id)
                .bind(tenant_id)
                .fetch_all(store.pool())
                .await?;
                sqlite_application_record(row, redirects)?
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, kind, launch_url, client_id, allowed_scopes_json, enabled
                     FROM applications WHERE client_id = $1",
                )
                .bind(client_id.as_str())
                .fetch_optional(pool)
                .await?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
                let redirects = sqlx::query_scalar(
                    "SELECT redirect_uri FROM application_redirect_uris
                     WHERE app_id = $1 AND tenant_id = $2 ORDER BY redirect_uri",
                )
                .bind(&app_id)
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                postgres_application_record(row, redirects)?
            }
        };
        if !application.enabled {
            return Err(PlatformStoreError::ApplicationDisabled(application.app_id));
        }
        Ok(Some(application))
    }

    pub async fn register_client_secret(
        &self,
        secret: NewOAuthClientSecret,
    ) -> Result<(), PlatformStoreError> {
        if secret.client_secret.is_empty() {
            return Err(PlatformStoreError::EmptyOAuthClientSecret);
        }
        let application = self
            .find_application_by_app_id(secret.app_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthApplicationNotFound)?;
        if application.tenant_id != secret.tenant_id {
            return Err(PlatformStoreError::OAuthApplicationNotFound);
        }
        if !application.enabled {
            return Err(PlatformStoreError::ApplicationDisabled(application.app_id));
        }
        let secret_hash = sha256_hex(&secret.client_secret);
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO oauth_client_secrets (app_id, tenant_id, secret_hash)
                     VALUES (?, ?, ?)
                     ON CONFLICT DO NOTHING",
                )
                .bind(secret.app_id.as_str())
                .bind(secret.tenant_id.to_string())
                .bind(secret_hash)
                .execute(store.pool())
                .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO oauth_client_secrets (app_id, tenant_id, secret_hash)
                     VALUES ($1, $2, $3)
                     ON CONFLICT DO NOTHING",
                )
                .bind(secret.app_id.as_str())
                .bind(secret.tenant_id)
                .bind(secret_hash)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn issue_authorization_code(
        &self,
        code: NewOAuthAuthorizationCode,
    ) -> Result<(), PlatformStoreError> {
        if code.code.is_empty() {
            return Err(PlatformStoreError::EmptyOAuthAuthorizationCode);
        }
        if code.code_challenge.is_empty() {
            return Err(PlatformStoreError::EmptyOAuthCodeChallenge);
        }
        if code.expires_at <= code.issued_at {
            return Err(PlatformStoreError::InvalidOAuthAuthorizationCodeExpiry);
        }
        let scopes = canonical_application_scopes(code.scopes)?;
        let application = self
            .find_application_by_app_id(code.app_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthApplicationNotFound)?;
        if application.tenant_id != code.tenant_id
            || !oauth_user_belongs_to_tenant(self, code.user_id, code.tenant_id).await?
        {
            return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
        }
        if !application.enabled {
            return Err(PlatformStoreError::ApplicationDisabled(application.app_id));
        }
        if !application
            .redirect_uris
            .iter()
            .any(|redirect_uri| redirect_uri == &code.redirect_uri)
        {
            return Err(PlatformStoreError::OAuthRedirectUriDenied);
        }
        let code_hash = sha256_hex(&code.code);
        let scopes_json = serde_json::to_string(&scopes)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?;
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO oauth_authorization_codes (
                        code_hash, app_id, tenant_id, user_id, redirect_uri, code_challenge, scopes_json,
                        issued_at, expires_at, consumed_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                )
                .bind(code_hash)
                .bind(code.app_id.as_str())
                .bind(code.tenant_id.to_string())
                .bind(code.user_id.to_string())
                .bind(code.redirect_uri.as_str())
                .bind(code.code_challenge)
                .bind(scopes_json)
                .bind(code.issued_at.to_rfc3339())
                .bind(code.expires_at.to_rfc3339())
                .execute(store.pool())
                .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO oauth_authorization_codes (
                        code_hash, app_id, tenant_id, user_id, redirect_uri, code_challenge, scopes_json,
                        issued_at, expires_at, consumed_at
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, NULL)",
                )
                .bind(code_hash)
                .bind(code.app_id.as_str())
                .bind(code.tenant_id)
                .bind(code.user_id)
                .bind(code.redirect_uri.as_str())
                .bind(code.code_challenge)
                .bind(scopes_json)
                .bind(code.issued_at)
                .bind(code.expires_at)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn consume_authorization_code_and_issue_access_token(
        &self,
        exchange: OAuthAuthorizationCodeExchange,
    ) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
        validate_oauth_access_token_expiry(exchange.issued_at, exchange.expires_at)?;
        let application = self
            .find_application_by_client_id(exchange.client_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthAuthorizationCodeDenied)?;
        let code_hash = sha256_hex(&exchange.code);
        let code_challenge = s256_code_challenge(&exchange.code_verifier);
        let token_hash = sha256_hex(&exchange.access_token);
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, user_id, scopes_json
                     FROM oauth_authorization_codes
                     WHERE code_hash = ? AND redirect_uri = ?
                       AND code_challenge = ?
                       AND app_id = ? AND tenant_id = ?
                       AND consumed_at IS NULL AND expires_at > ?",
                )
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .bind(exchange.issued_at.to_rfc3339())
                .fetch_optional(&mut *transaction)
                .await?;
                let Some(row) = row else {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: String = row.try_get("tenant_id")?;
                let user_id: String = row.try_get("user_id")?;
                let scopes_json: String = row.try_get("scopes_json")?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = ? AND tenant_id = ?",
                )
                .bind(&app_id)
                .bind(&tenant_id)
                .fetch_all(&mut *transaction)
                .await?;
                if !oauth_client_secret_matches(&secret_hashes, exchange.client_secret.as_deref()) {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let record = oauth_access_token_record(
                    app_id,
                    tenant_id,
                    user_id,
                    &scopes_json,
                    exchange.issued_at,
                    exchange.expires_at,
                )?;
                let consumed = sqlx::query(
                    "UPDATE oauth_authorization_codes
                       SET consumed_at = ?
                     WHERE code_hash = ? AND redirect_uri = ?
                       AND code_challenge = ?
                       AND app_id = ? AND tenant_id = ?
                       AND consumed_at IS NULL AND expires_at > ?",
                )
                .bind(exchange.issued_at.to_rfc3339())
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id.to_string())
                .bind(exchange.issued_at.to_rfc3339())
                .execute(&mut *transaction)
                .await?;
                if consumed.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                }
                sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     ) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(token_hash)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id.to_string())
                .bind(
                    record
                        .user_id
                        .expect("authorization code user ID is present")
                        .to_string(),
                )
                .bind(scopes_json)
                .bind(exchange.issued_at.to_rfc3339())
                .bind(exchange.expires_at.to_rfc3339())
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(record)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let row = sqlx::query(
                    "SELECT app_id, tenant_id, user_id, scopes_json::text AS scopes_json
                     FROM oauth_authorization_codes
                     WHERE code_hash = $1 AND redirect_uri = $2
                       AND code_challenge = $3
                       AND app_id = $4 AND tenant_id = $5
                       AND consumed_at IS NULL AND expires_at > $6
                     FOR UPDATE",
                )
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .bind(exchange.issued_at)
                .fetch_optional(&mut *transaction)
                .await?;
                let Some(row) = row else {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                };
                let app_id: String = row.try_get("app_id")?;
                let tenant_id: uuid::Uuid = row.try_get("tenant_id")?;
                let user_id: uuid::Uuid = row.try_get("user_id")?;
                let scopes_json: String = row.try_get("scopes_json")?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = $1 AND tenant_id = $2",
                )
                .bind(&app_id)
                .bind(tenant_id)
                .fetch_all(&mut *transaction)
                .await?;
                if !oauth_client_secret_matches(&secret_hashes, exchange.client_secret.as_deref()) {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let record = oauth_access_token_record(
                    app_id,
                    tenant_id.to_string(),
                    user_id.to_string(),
                    &scopes_json,
                    exchange.issued_at,
                    exchange.expires_at,
                )?;
                let consumed = sqlx::query(
                    "UPDATE oauth_authorization_codes
                     SET consumed_at = $1
                     WHERE code_hash = $2 AND redirect_uri = $3
                       AND code_challenge = $4
                       AND app_id = $5 AND tenant_id = $6
                       AND consumed_at IS NULL AND expires_at > $7",
                )
                .bind(exchange.issued_at)
                .bind(&code_hash)
                .bind(exchange.redirect_uri.as_str())
                .bind(&code_challenge)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id)
                .bind(exchange.issued_at)
                .execute(&mut *transaction)
                .await?;
                if consumed.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthAuthorizationCodeDenied);
                }
                sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)",
                )
                .bind(token_hash)
                .bind(record.app_id.as_str())
                .bind(record.tenant_id)
                .bind(
                    record
                        .user_id
                        .expect("authorization code user ID is present"),
                )
                .bind(scopes_json)
                .bind(exchange.issued_at)
                .bind(exchange.expires_at)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                Ok(record)
            }
        }
    }

    pub async fn issue_client_credentials_access_token(
        &self,
        request: OAuthClientCredentialsToken,
    ) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
        validate_oauth_access_token_expiry(request.issued_at, request.expires_at)?;
        let application = self
            .find_application_by_client_id(request.client_id.as_str())
            .await?
            .ok_or(PlatformStoreError::OAuthClientAuthenticationDenied)?;
        let scopes = canonical_application_scopes(request.scopes)?;
        let token_hash = sha256_hex(&request.access_token);
        let scopes_json = serde_json::to_string(&scopes)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?;
        let record = OAuthAccessTokenRecord {
            app_id: application.app_id.clone(),
            tenant_id: application.tenant_id,
            user_id: None,
            scopes,
            issued_at: request.issued_at,
            expires_at: request.expires_at,
        };
        match self {
            Self::Sqlite(store) => {
                let mut transaction = store.pool().begin().await?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = ? AND tenant_id = ?",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .fetch_all(&mut *transaction)
                .await?;
                if secret_hashes.is_empty()
                    || !oauth_client_secret_matches(&secret_hashes, Some(&request.client_secret))
                {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let inserted = sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     )
                     SELECT ?, app_id, tenant_id, NULL, ?, ?, ?
                     FROM applications
                     WHERE app_id = ? AND tenant_id = ? AND client_id = ? AND enabled = 1",
                )
                .bind(token_hash)
                .bind(scopes_json)
                .bind(request.issued_at.to_rfc3339())
                .bind(request.expires_at.to_rfc3339())
                .bind(application.app_id.as_str())
                .bind(application.tenant_id.to_string())
                .bind(request.client_id.as_str())
                .execute(&mut *transaction)
                .await?;
                if inserted.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                transaction.commit().await?;
                Ok(record)
            }
            Self::Timescale(pool) => {
                let mut transaction = pool.begin().await?;
                let secret_hashes = sqlx::query_scalar(
                    "SELECT secret_hash FROM oauth_client_secrets WHERE app_id = $1 AND tenant_id = $2",
                )
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .fetch_all(&mut *transaction)
                .await?;
                if secret_hashes.is_empty()
                    || !oauth_client_secret_matches(&secret_hashes, Some(&request.client_secret))
                {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                let inserted = sqlx::query(
                    "INSERT INTO oauth_access_tokens (
                        token_hash, app_id, tenant_id, user_id, scopes_json, issued_at, expires_at
                     )
                     SELECT $1, app_id, tenant_id, NULL, $2::jsonb, $3, $4
                     FROM applications
                     WHERE app_id = $5 AND tenant_id = $6 AND client_id = $7 AND enabled = TRUE",
                )
                .bind(token_hash)
                .bind(scopes_json)
                .bind(request.issued_at)
                .bind(request.expires_at)
                .bind(application.app_id.as_str())
                .bind(application.tenant_id)
                .bind(request.client_id.as_str())
                .execute(&mut *transaction)
                .await?;
                if inserted.rows_affected() != 1 {
                    return Err(PlatformStoreError::OAuthClientAuthenticationDenied);
                }
                transaction.commit().await?;
                Ok(record)
            }
        }
    }

    pub async fn resolve_access_token(
        &self,
        access_token: &str,
        now: DateTime<Utc>,
    ) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
        let token_hash = sha256_hex(access_token);
        match self {
            Self::Sqlite(store) => {
                let row = sqlx::query(
                    "SELECT token.app_id, token.tenant_id, token.user_id, token.scopes_json,
                            token.issued_at, token.expires_at
                     FROM oauth_access_tokens AS token
                     JOIN applications AS app
                       ON app.app_id = token.app_id AND app.tenant_id = token.tenant_id
                     WHERE token.token_hash = ? AND token.expires_at > ? AND app.enabled = 1",
                )
                .bind(token_hash)
                .bind(now.to_rfc3339())
                .fetch_optional(store.pool())
                .await?
                .ok_or(PlatformStoreError::OAuthAccessTokenDenied)?;
                oauth_resolved_access_token_record(
                    row.try_get("app_id")?,
                    row.try_get("tenant_id")?,
                    row.try_get("user_id")?,
                    &row.try_get::<String, _>("scopes_json")?,
                    parse_oauth_timestamp(&row.try_get::<String, _>("issued_at")?)?,
                    parse_oauth_timestamp(&row.try_get::<String, _>("expires_at")?)?,
                )
            }
            Self::Timescale(pool) => {
                let row = sqlx::query(
                    "SELECT token.app_id, token.tenant_id, token.user_id,
                            token.scopes_json::text AS scopes_json,
                            token.issued_at, token.expires_at
                     FROM oauth_access_tokens AS token
                     JOIN applications AS app
                       ON app.app_id = token.app_id AND app.tenant_id = token.tenant_id
                     WHERE token.token_hash = $1 AND token.expires_at > $2 AND app.enabled = TRUE",
                )
                .bind(token_hash)
                .bind(now)
                .fetch_optional(pool)
                .await?
                .ok_or(PlatformStoreError::OAuthAccessTokenDenied)?;
                oauth_resolved_access_token_record(
                    row.try_get("app_id")?,
                    row.try_get::<uuid::Uuid, _>("tenant_id")?.to_string(),
                    row.try_get::<Option<uuid::Uuid>, _>("user_id")?
                        .map(|user_id| user_id.to_string()),
                    &row.try_get::<String, _>("scopes_json")?,
                    row.try_get("issued_at")?,
                    row.try_get("expires_at")?,
                )
            }
        }
    }
}

impl ApplicationRepository for PlatformStore {
    fn upsert_application<'a>(
        &'a self,
        application: NewApplication,
    ) -> Pin<Box<dyn Future<Output = Result<ApplicationRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::upsert_application(self, application).await })
    }

    fn list_applications_for_tenant<'a>(
        &'a self,
        tenant_id: uuid::Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ApplicationRecord>, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::list_applications_for_tenant(self, tenant_id).await })
    }

    fn find_application_by_app_id<'a>(
        &'a self,
        app_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::find_application_by_app_id(self, app_id).await })
    }

    fn find_application_by_client_id<'a>(
        &'a self,
        client_id: &'a str,
    ) -> Pin<
        Box<dyn Future<Output = Result<Option<ApplicationRecord>, PlatformStoreError>> + Send + 'a>,
    > {
        Box::pin(async move { PlatformStore::find_application_by_client_id(self, client_id).await })
    }
}

impl OAuthRepository for PlatformStore {
    fn register_client_secret<'a>(
        &'a self,
        secret: NewOAuthClientSecret,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { PlatformStore::register_client_secret(self, secret).await })
    }

    fn issue_authorization_code<'a>(
        &'a self,
        code: NewOAuthAuthorizationCode,
    ) -> Pin<Box<dyn Future<Output = Result<(), PlatformStoreError>> + Send + 'a>> {
        Box::pin(async move { PlatformStore::issue_authorization_code(self, code).await })
    }

    fn consume_authorization_code_and_issue_access_token<'a>(
        &'a self,
        exchange: OAuthAuthorizationCodeExchange,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            PlatformStore::consume_authorization_code_and_issue_access_token(self, exchange).await
        })
    }

    fn issue_client_credentials_access_token<'a>(
        &'a self,
        request: OAuthClientCredentialsToken,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move {
            PlatformStore::issue_client_credentials_access_token(self, request).await
        })
    }

    fn resolve_access_token<'a>(
        &'a self,
        access_token: &'a str,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccessTokenRecord, PlatformStoreError>> + Send + 'a>>
    {
        Box::pin(async move { PlatformStore::resolve_access_token(self, access_token, now).await })
    }
}

fn validate_application(application: &mut NewApplication) -> Result<(), PlatformStoreError> {
    if application.launch_url.trim().is_empty() {
        return Err(PlatformStoreError::EmptyApplicationLaunchUrl);
    }
    let mut redirect_uris =
        std::collections::HashSet::with_capacity(application.redirect_uris.len());
    for redirect_uri in &application.redirect_uris {
        if !redirect_uris.insert(redirect_uri.as_str()) {
            return Err(PlatformStoreError::DuplicateApplicationRedirectUri(
                redirect_uri.as_str().to_owned(),
            ));
        }
    }
    for scope in &application.allowed_scopes {
        if scope.trim().is_empty() {
            return Err(PlatformStoreError::EmptyApplicationScope);
        }
    }
    application.allowed_scopes.sort();
    application.allowed_scopes.dedup();
    Ok(())
}

fn map_application_write_conflict(
    error: sqlx::Error,
    client_id: &str,
    tenant_id: uuid::Uuid,
) -> PlatformStoreError {
    if error
        .as_database_error()
        .is_some_and(is_application_tenant_unique_violation)
    {
        PlatformStoreError::TenantApplicationLimit(tenant_id)
    } else if error
        .as_database_error()
        .is_some_and(is_application_client_id_unique_violation)
    {
        PlatformStoreError::ApplicationClientIdConflict(client_id.to_owned())
    } else {
        PlatformStoreError::Database(error)
    }
}

fn is_application_tenant_unique_violation(database_error: &(dyn DatabaseError + 'static)) -> bool {
    match database_error.code().as_deref() {
        Some("23505") => database_error.constraint() == Some("applications_tenant_id_key"),
        Some("19") | Some("2067") => database_error
            .message()
            .contains("UNIQUE constraint failed: applications.tenant_id"),
        _ => false,
    }
}

fn is_application_client_id_unique_violation(
    database_error: &(dyn DatabaseError + 'static),
) -> bool {
    let code = database_error.code();
    match code.as_deref() {
        Some("23505") => {
            database_error.constraint() == Some("applications_client_id_key")
                || database_error
                    .message()
                    .contains("applications_client_id_key")
        }
        Some("19") | Some("2067") => database_error
            .message()
            .contains("UNIQUE constraint failed: applications.client_id"),
        _ => false,
    }
}

fn canonical_application_scopes(scopes: Vec<String>) -> Result<Vec<String>, PlatformStoreError> {
    if scopes.iter().any(|scope| scope.trim().is_empty()) {
        return Err(PlatformStoreError::InvalidApplicationScopes);
    }
    let mut scopes = scopes;
    scopes.sort();
    scopes.dedup();
    Ok(scopes)
}

fn sha256_hex(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn s256_code_challenge(code_verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()))
}

fn validate_oauth_access_token_expiry(
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<(), PlatformStoreError> {
    if expires_at <= issued_at {
        return Err(PlatformStoreError::InvalidOAuthAccessTokenExpiry);
    }
    Ok(())
}

async fn oauth_user_belongs_to_tenant(
    store: &PlatformStore,
    user_id: uuid::Uuid,
    tenant_id: uuid::Uuid,
) -> Result<bool, PlatformStoreError> {
    match store {
        PlatformStore::Sqlite(store) => Ok(sqlx::query_scalar::<_, i64>(
            "SELECT 1 FROM users WHERE id = ? AND tenant_id = ?",
        )
        .bind(user_id.to_string())
        .bind(tenant_id.to_string())
        .fetch_optional(store.pool())
        .await?
        .is_some()),
        PlatformStore::Timescale(pool) => Ok(sqlx::query_scalar::<_, i32>(
            "SELECT 1 FROM users WHERE id = $1 AND tenant_id = $2",
        )
        .bind(user_id)
        .bind(tenant_id)
        .fetch_optional(pool)
        .await?
        .is_some()),
    }
}

fn oauth_client_secret_matches(stored_hashes: &[String], supplied_secret: Option<&str>) -> bool {
    if stored_hashes.is_empty() {
        return true;
    }
    let Some(supplied_secret) = supplied_secret else {
        return false;
    };
    let supplied_hash = sha256_hex(supplied_secret);
    let mut matched = 0_u8;
    for stored_hash in stored_hashes {
        matched |= supplied_hash
            .as_bytes()
            .ct_eq(stored_hash.as_bytes())
            .unwrap_u8();
    }
    matched != 0
}

fn oauth_access_token_record(
    app_id: String,
    tenant_id: String,
    user_id: String,
    scopes_json: &str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
    let app_id = app_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let tenant_id = tenant_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let user_id = user_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let scopes = serde_json::from_str(scopes_json)
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    let scopes = canonical_application_scopes(scopes)
        .map_err(|_| PlatformStoreError::OAuthAuthorizationCodeDenied)?;
    Ok(OAuthAccessTokenRecord {
        app_id,
        tenant_id,
        user_id: Some(user_id),
        scopes,
        issued_at,
        expires_at,
    })
}

fn parse_oauth_timestamp(value: &str) -> Result<DateTime<Utc>, PlatformStoreError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)
}

fn oauth_resolved_access_token_record(
    app_id: String,
    tenant_id: String,
    user_id: Option<String>,
    scopes_json: &str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<OAuthAccessTokenRecord, PlatformStoreError> {
    let app_id = app_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let tenant_id = tenant_id
        .parse()
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let user_id = user_id
        .map(|user_id| user_id.parse())
        .transpose()
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let scopes = serde_json::from_str(scopes_json)
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    let scopes = canonical_application_scopes(scopes)
        .map_err(|_| PlatformStoreError::OAuthAccessTokenDenied)?;
    Ok(OAuthAccessTokenRecord {
        app_id,
        tenant_id,
        user_id,
        scopes,
        issued_at,
        expires_at,
    })
}

fn sqlite_application_record(
    row: SqliteRow,
    redirects: Vec<String>,
) -> Result<ApplicationRecord, PlatformStoreError> {
    let scopes = canonical_application_scopes(
        serde_json::from_str(&row.try_get::<String, _>("allowed_scopes_json")?)
            .map_err(|_| PlatformStoreError::InvalidApplicationScopes)?,
    )?;
    Ok(ApplicationRecord {
        app_id: row.try_get::<String, _>("app_id")?.parse()?,
        tenant_id: row
            .try_get::<String, _>("tenant_id")?
            .parse()
            .map_err(|_| PlatformStoreError::OAuthApplicationNotFound)?,
        kind: ApplicationKind::parse(&row.try_get::<String, _>("kind")?)?,
        launch_url: row.try_get("launch_url")?,
        client_id: row.try_get::<String, _>("client_id")?.parse()?,
        redirect_uris: redirects
            .into_iter()
            .map(|uri| uri.parse())
            .collect::<Result<Vec<_>, _>>()?,
        allowed_scopes: scopes,
        enabled: row.try_get::<i64, _>("enabled")? != 0,
    })
}

fn postgres_application_record(
    row: PgRow,
    redirects: Vec<String>,
) -> Result<ApplicationRecord, PlatformStoreError> {
    let scopes = canonical_application_scopes(
        row.try_get::<Json<Vec<String>>, _>("allowed_scopes_json")?
            .0,
    )?;
    Ok(ApplicationRecord {
        app_id: row.try_get::<String, _>("app_id")?.parse()?,
        tenant_id: row.try_get("tenant_id")?,
        kind: ApplicationKind::parse(&row.try_get::<String, _>("kind")?)?,
        launch_url: row.try_get("launch_url")?,
        client_id: row.try_get::<String, _>("client_id")?.parse()?,
        redirect_uris: redirects
            .into_iter()
            .map(|uri| uri.parse())
            .collect::<Result<Vec<_>, _>>()?,
        allowed_scopes: scopes,
        enabled: row.try_get("enabled")?,
    })
}
