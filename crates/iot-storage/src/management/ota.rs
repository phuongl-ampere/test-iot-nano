use uuid::Uuid;

use crate::{PlatformStore, PlatformStoreError};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OtaPolicy {
    pub require_matching_device_profile: bool,
    pub require_newer_version: bool,
}

impl Default for OtaPolicy {
    fn default() -> Self {
        Self {
            require_matching_device_profile: true,
            require_newer_version: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtaArtifact {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub device_profile_id: Uuid,
    pub version: String,
    pub filename: String,
    pub storage_path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OtaDeploymentStatus {
    Started,
    Succeeded,
    Failed,
}

impl OtaDeploymentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOtaDeployment {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub device_id: String,
    pub artifact_id: Uuid,
    pub from_version: Option<String>,
    pub target_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtaDeployment {
    pub id: Uuid,
    pub tenant_id: Uuid,
    pub device_id: String,
    pub artifact_id: Uuid,
    pub from_version: Option<String>,
    pub target_version: String,
    pub status: OtaDeploymentStatus,
    pub error_message: Option<String>,
    pub started_at: String,
    pub completed_at: Option<String>,
}

impl PlatformStore {
    pub async fn create_ota_deployment(
        &self,
        deployment: NewOtaDeployment,
    ) -> Result<(), PlatformStoreError> {
        if deployment.device_id.is_empty() || !is_semver(&deployment.target_version) {
            return Err(PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid OTA deployment".to_owned(),
            )));
        }
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                    "INSERT INTO ota_deployments (id, tenant_id, device_id, artifact_id, from_version, target_version, status)
                     VALUES (?, ?, ?, ?, ?, ?, 'started')",
                )
                .bind(deployment.id.to_string())
                .bind(deployment.tenant_id.to_string())
                .bind(deployment.device_id)
                .bind(deployment.artifact_id.to_string())
                .bind(deployment.from_version)
                .bind(deployment.target_version)
                .execute(store.pool())
                .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                    "INSERT INTO ota_deployments (id, tenant_id, device_id, artifact_id, from_version, target_version, status)
                     VALUES ($1, $2, $3, $4, $5, $6, 'started')",
                )
                .bind(deployment.id)
                .bind(deployment.tenant_id)
                .bind(deployment.device_id)
                .bind(deployment.artifact_id)
                .bind(deployment.from_version)
                .bind(deployment.target_version)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    pub async fn report_ota_deployment(
        &self,
        tenant_id: Uuid,
        device_id: &str,
        deployment_id: Uuid,
        status: OtaDeploymentStatus,
        error_message: Option<String>,
    ) -> Result<(), PlatformStoreError> {
        if status == OtaDeploymentStatus::Started {
            return Err(PlatformStoreError::Database(sqlx::Error::Protocol(
                "OTA deployment result must be succeeded or failed".to_owned(),
            )));
        }
        match self {
            Self::Sqlite(store) => {
                let result = sqlx::query(
                    "UPDATE ota_deployments
                     SET status = ?, error_message = ?, completed_at = CURRENT_TIMESTAMP
                     WHERE id = ? AND tenant_id = ? AND device_id = ? AND status = 'started'",
                )
                .bind(status.as_str())
                .bind(error_message)
                .bind(deployment_id.to_string())
                .bind(tenant_id.to_string())
                .bind(device_id)
                .execute(store.pool())
                .await?;
                if result.rows_affected() != 1 {
                    return Err(PlatformStoreError::Database(sqlx::Error::Protocol(
                        "OTA deployment is unavailable for reporting".to_owned(),
                    )));
                }
            }
            Self::Timescale(pool) => {
                let result = sqlx::query(
                    "UPDATE ota_deployments
                     SET status = $1, error_message = $2, completed_at = now()
                     WHERE id = $3 AND tenant_id = $4 AND device_id = $5 AND status = 'started'",
                )
                .bind(status.as_str())
                .bind(error_message)
                .bind(deployment_id)
                .bind(tenant_id)
                .bind(device_id)
                .execute(pool)
                .await?;
                if result.rows_affected() != 1 {
                    return Err(PlatformStoreError::Database(sqlx::Error::Protocol(
                        "OTA deployment is unavailable for reporting".to_owned(),
                    )));
                }
            }
        }
        Ok(())
    }

    pub async fn list_ota_deployments(
        &self,
        tenant_id: Uuid,
        limit: u32,
    ) -> Result<Vec<OtaDeployment>, PlatformStoreError> {
        let limit = i64::from(limit.clamp(1, 200));
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query_as::<_, (String, String, String, String, Option<String>, String, String, Option<String>, String, Option<String>)>(
                    "SELECT id, tenant_id, device_id, artifact_id, from_version, target_version, status, error_message, started_at, completed_at
                     FROM ota_deployments WHERE tenant_id = ? ORDER BY started_at DESC, id DESC LIMIT ?",
                )
                .bind(tenant_id.to_string())
                .bind(limit)
                .fetch_all(store.pool())
                .await?;
                rows.into_iter().map(sqlite_deployment).collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query_as::<_, (Uuid, Uuid, String, Uuid, Option<String>, String, String, Option<String>, String, Option<String>)>(
                    "SELECT id, tenant_id, device_id, artifact_id, from_version, target_version, status, error_message,
                            started_at::TEXT, completed_at::TEXT
                     FROM ota_deployments WHERE tenant_id = $1 ORDER BY started_at DESC, id DESC LIMIT $2",
                )
                .bind(tenant_id)
                .bind(limit)
                .fetch_all(pool)
                .await?;
                rows.into_iter().map(postgres_deployment).collect()
            }
        }
    }

    pub async fn list_ota_artifacts(
        &self,
        tenant_id: Uuid,
    ) -> Result<Vec<OtaArtifact>, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let rows = sqlx::query_as::<_, (String, String, String, String, String, String, String, i64)>(
                    "SELECT id, tenant_id, device_profile_id, version, filename, storage_path, sha256, size_bytes
                     FROM ota_artifacts WHERE tenant_id = ? ORDER BY created_at DESC",
                ).bind(tenant_id.to_string()).fetch_all(store.pool()).await?;
                rows.into_iter().map(sqlite_artifact).collect()
            }
            Self::Timescale(pool) => {
                let rows = sqlx::query_as::<_, (Uuid, Uuid, Uuid, String, String, String, String, i64)>(
                    "SELECT id, tenant_id, device_profile_id, version, filename, storage_path, sha256, size_bytes
                     FROM ota_artifacts WHERE tenant_id = $1 ORDER BY created_at DESC",
                ).bind(tenant_id).fetch_all(pool).await?;
                rows.into_iter()
                    .map(
                        |(
                            id,
                            tenant_id,
                            device_profile_id,
                            version,
                            filename,
                            storage_path,
                            sha256,
                            size_bytes,
                        )| {
                            Ok(OtaArtifact {
                                id,
                                tenant_id,
                                device_profile_id,
                                version,
                                filename,
                                storage_path,
                                sha256,
                                size_bytes: size_bytes.try_into().map_err(|_| {
                                    PlatformStoreError::Database(sqlx::Error::Protocol(
                                        "invalid OTA size".to_owned(),
                                    ))
                                })?,
                            })
                        },
                    )
                    .collect()
            }
        }
    }
    pub async fn set_ota_policy(
        &self,
        tenant_id: Uuid,
        policy: OtaPolicy,
    ) -> Result<(), PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                "INSERT INTO tenant_ota_policies (tenant_id, require_matching_device_profile, require_newer_version)
                 VALUES (?, ?, ?)
                 ON CONFLICT(tenant_id) DO UPDATE SET
                   require_matching_device_profile = excluded.require_matching_device_profile,
                   require_newer_version = excluded.require_newer_version,
                   updated_at = CURRENT_TIMESTAMP",
            )
            .bind(tenant_id.to_string())
            .bind(policy.require_matching_device_profile)
            .bind(policy.require_newer_version)
            .execute(store.pool())
            .await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                "INSERT INTO tenant_ota_policies (tenant_id, require_matching_device_profile, require_newer_version)
                 VALUES ($1, $2, $3)
                 ON CONFLICT(tenant_id) DO UPDATE SET
                   require_matching_device_profile = excluded.require_matching_device_profile,
                   require_newer_version = excluded.require_newer_version,
                   updated_at = now()",
            )
            .bind(tenant_id)
            .bind(policy.require_matching_device_profile)
            .bind(policy.require_newer_version)
            .execute(pool)
            .await?;
            }
        };
        Ok(())
    }

    pub async fn create_ota_artifact(
        &self,
        artifact: &OtaArtifact,
    ) -> Result<(), PlatformStoreError> {
        if !is_semver(&artifact.version)
            || artifact.filename.is_empty()
            || artifact.filename.contains('/')
            || artifact.sha256.len() != 64
            || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(PlatformStoreError::Database(sqlx::Error::Protocol(
                "invalid OTA artifact metadata".to_owned(),
            )));
        }
        match self {
            Self::Sqlite(store) => {
                sqlx::query(
                "INSERT INTO ota_artifacts (id, tenant_id, device_profile_id, version, filename, storage_path, sha256, size_bytes)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(artifact.id.to_string()).bind(artifact.tenant_id.to_string())
            .bind(artifact.device_profile_id.to_string()).bind(&artifact.version)
            .bind(&artifact.filename).bind(&artifact.storage_path).bind(&artifact.sha256)
            .bind(i64::try_from(artifact.size_bytes).unwrap_or(i64::MAX))
            .execute(store.pool()).await?;
            }
            Self::Timescale(pool) => {
                sqlx::query(
                "INSERT INTO ota_artifacts (id, tenant_id, device_profile_id, version, filename, storage_path, sha256, size_bytes)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            )
            .bind(artifact.id).bind(artifact.tenant_id).bind(artifact.device_profile_id)
            .bind(&artifact.version).bind(&artifact.filename).bind(&artifact.storage_path).bind(&artifact.sha256)
            .bind(i64::try_from(artifact.size_bytes).unwrap_or(i64::MAX))
            .execute(pool).await?;
            }
        };
        Ok(())
    }

    pub async fn ota_policy(&self, tenant_id: Uuid) -> Result<OtaPolicy, PlatformStoreError> {
        match self {
            Self::Sqlite(store) => {
                let row = sqlx::query_as::<_, (i64, i64)>(
                    "SELECT require_matching_device_profile, require_newer_version
                     FROM tenant_ota_policies WHERE tenant_id = ?",
                )
                .bind(tenant_id.to_string())
                .fetch_optional(store.pool())
                .await?;
                Ok(
                    row.map_or_else(OtaPolicy::default, |(profile, version)| OtaPolicy {
                        require_matching_device_profile: profile != 0,
                        require_newer_version: version != 0,
                    }),
                )
            }
            Self::Timescale(pool) => {
                let row = sqlx::query_as::<_, (bool, bool)>(
                    "SELECT require_matching_device_profile, require_newer_version
                     FROM tenant_ota_policies WHERE tenant_id = $1",
                )
                .bind(tenant_id)
                .fetch_optional(pool)
                .await?;
                Ok(
                    row.map_or_else(OtaPolicy::default, |(profile, version)| OtaPolicy {
                        require_matching_device_profile: profile,
                        require_newer_version: version,
                    }),
                )
            }
        }
    }
}

fn is_semver(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 3
        && parts
            .into_iter()
            .all(|part| !part.is_empty() && part.parse::<u64>().is_ok())
}

fn sqlite_artifact(
    row: (String, String, String, String, String, String, String, i64),
) -> Result<OtaArtifact, PlatformStoreError> {
    let (id, tenant_id, device_profile_id, version, filename, storage_path, sha256, size_bytes) =
        row;
    Ok(OtaArtifact {
        id: Uuid::parse_str(&id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid OTA id".to_owned()))
        })?,
        tenant_id: Uuid::parse_str(&tenant_id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid OTA tenant".to_owned()))
        })?,
        device_profile_id: Uuid::parse_str(&device_profile_id).map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid OTA profile".to_owned()))
        })?,
        version,
        filename,
        storage_path,
        sha256,
        size_bytes: size_bytes.try_into().map_err(|_| {
            PlatformStoreError::Database(sqlx::Error::Protocol("invalid OTA size".to_owned()))
        })?,
    })
}

fn sqlite_deployment(
    row: (
        String,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
        String,
        Option<String>,
    ),
) -> Result<OtaDeployment, PlatformStoreError> {
    let (
        id,
        tenant_id,
        device_id,
        artifact_id,
        from_version,
        target_version,
        status,
        error_message,
        started_at,
        completed_at,
    ) = row;
    Ok(OtaDeployment {
        id: parse_uuid(&id, "deployment id")?,
        tenant_id: parse_uuid(&tenant_id, "deployment tenant")?,
        device_id,
        artifact_id: parse_uuid(&artifact_id, "deployment artifact")?,
        from_version,
        target_version,
        status: ota_deployment_status(&status)?,
        error_message,
        started_at,
        completed_at,
    })
}

fn postgres_deployment(
    row: (
        Uuid,
        Uuid,
        String,
        Uuid,
        Option<String>,
        String,
        String,
        Option<String>,
        String,
        Option<String>,
    ),
) -> Result<OtaDeployment, PlatformStoreError> {
    let (
        id,
        tenant_id,
        device_id,
        artifact_id,
        from_version,
        target_version,
        status,
        error_message,
        started_at,
        completed_at,
    ) = row;
    Ok(OtaDeployment {
        id,
        tenant_id,
        device_id,
        artifact_id,
        from_version,
        target_version,
        status: ota_deployment_status(&status)?,
        error_message,
        started_at,
        completed_at,
    })
}

fn parse_uuid(value: &str, field: &str) -> Result<Uuid, PlatformStoreError> {
    Uuid::parse_str(value).map_err(|_| {
        PlatformStoreError::Database(sqlx::Error::Protocol(format!("invalid OTA {field}")))
    })
}

fn ota_deployment_status(value: &str) -> Result<OtaDeploymentStatus, PlatformStoreError> {
    match value {
        "started" => Ok(OtaDeploymentStatus::Started),
        "succeeded" => Ok(OtaDeploymentStatus::Succeeded),
        "failed" => Ok(OtaDeploymentStatus::Failed),
        _ => Err(PlatformStoreError::Database(sqlx::Error::Protocol(
            "invalid OTA deployment status".to_owned(),
        ))),
    }
}
