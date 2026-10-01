use super::*;

pub(super) fn management_openapi() -> Value {
    let mut paths = Map::new();

    documented_path(
        &mut paths,
        "/api/auth/login",
        vec![(
            "post",
            documented_operation(
                "Start a platform session",
                None,
                Some(("application/json", "LoginRequest")),
                (
                    "200",
                    "Platform session started",
                    Some("PlatformLoginResponse"),
                ),
                &[
                    ("401", "Invalid credentials"),
                    ("429", "Too many attempts"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/auth/logout",
        vec![(
            "post",
            documented_operation(
                "End the current management session",
                None,
                None,
                ("204", "Management session ended", None),
                &[],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/auth/me",
        vec![(
            "get",
            documented_operation(
                "Get the current management session",
                Some("managementSession"),
                None,
                ("200", "Current session", Some("SessionResponse")),
                &[("401", "No active management session")],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/applications",
        vec![(
            "post",
            management_operation(
                "Create or update an OAuth application",
                "ApplicationRequest",
                "201",
                "ApplicationResponse",
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/profile",
        vec![
            (
                "get",
                documented_operation(
                    "Export the current tenant profile configuration",
                    Some("managementSession"),
                    None,
                    (
                        "200",
                        "Tenant profile configuration",
                        Some("TenantProfileConfiguration"),
                    ),
                    &tenant_management_errors(),
                ),
            ),
            (
                "put",
                documented_operation(
                    "Replace the current tenant profile configuration",
                    Some("managementSession"),
                    Some(("application/json", "TenantProfileConfiguration")),
                    ("204", "Tenant profile configuration replaced", None),
                    &tenant_management_errors(),
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profile/export",
        vec![(
            "get",
            documented_operation(
                "Export the current tenant profile configuration",
                Some("managementSession"),
                None,
                (
                    "200",
                    "Tenant profile configuration",
                    Some("TenantProfileConfiguration"),
                ),
                &tenant_management_errors(),
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/profile/import",
        vec![(
            "put",
            documented_operation(
                "Validate and atomically replace the tenant profile configuration",
                Some("managementSession"),
                Some(("application/json", "TenantProfileConfiguration")),
                ("204", "Tenant profile configuration replaced", None),
                &tenant_management_errors(),
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/ota/artifacts",
        vec![
            (
                "get",
                management_list_operation("List OTA firmware artifacts", "OtaArtifactList"),
            ),
            ("post", ota_upload_operation()),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/ota/policy",
        vec![
            (
                "get",
                management_list_operation("Get OTA delivery policy", "OtaPolicy"),
            ),
            (
                "put",
                documented_operation(
                    "Update OTA delivery policy",
                    Some("managementSession"),
                    Some(("application/json", "OtaPolicy")),
                    ("204", "OTA delivery policy updated", None),
                    &management_errors(),
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/alerts",
        vec![(
            "get",
            tenant_management_list_operation("List tenant alerts", "ManagementAlertList"),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/alerts/summary",
        vec![(
            "get",
            management_list_operation("Get tenant alert summary", "ManagementAlertSummary"),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/alert-rules",
        vec![
            (
                "get",
                management_list_operation("List tenant alert rules", "ManagementAlertRuleList"),
            ),
            (
                "post",
                management_operation(
                    "Create a tenant alert rule",
                    "ManagementAlertRuleRequest",
                    "201",
                    "ManagementAlertRule",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/alert-rules/{rule_id}",
        vec![(
            "put",
            management_operation(
                "Update a tenant alert rule",
                "ManagementAlertRuleRequest",
                "200",
                "ManagementAlertRule",
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/alert-rules/{rule_id}/archive",
        vec![(
            "post",
            management_no_content_operation("Archive a tenant alert rule"),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/alert-incidents",
        vec![(
            "get",
            management_list_operation("List tenant alert incidents", "ManagementAlertIncidentList"),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/alert-incidents/{incident_id}/acknowledge",
        vec![(
            "post",
            documented_operation(
                "Acknowledge a tenant alert incident",
                Some("managementSession"),
                None,
                (
                    "200",
                    "Alert incident acknowledged",
                    Some("ManagementAlertIncident"),
                ),
                &[
                    ("401", "No active management session"),
                    ("403", "Tenant account required"),
                    ("404", "Alert incident not found"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/audit",
        vec![(
            "get",
            tenant_audit_list_operation("List tenant audit events", "ManagementAuditEventPage"),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/users",
        vec![
            (
                "get",
                management_list_operation("List management users", "ManagementUserList"),
            ),
            (
                "post",
                management_operation(
                    "Create a management user",
                    "ManagementUserCreateRequest",
                    "201",
                    "ManagementUser",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/users/{username}",
        vec![(
            "put",
            management_operation(
                "Update a management user",
                "ManagementUserUpdateRequest",
                "200",
                "ManagementUser",
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/users/{username}/capabilities",
        vec![(
            "put",
            management_operation(
                "Replace a User's elevated capabilities",
                "ManagementUserCapabilitiesRequest",
                "200",
                "ManagementUser",
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/device-profiles",
        vec![
            (
                "get",
                management_list_operation("List device profiles", "DeviceProfileList"),
            ),
            (
                "post",
                management_operation(
                    "Create a device profile",
                    "DeviceProfileRequest",
                    "201",
                    "DeviceProfile",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/device-profiles/{profile_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update a device profile",
                    "DeviceProfileRequest",
                    "200",
                    "DeviceProfile",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete a device profile"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/asset-profiles",
        vec![
            (
                "get",
                management_list_operation("List asset profiles", "AssetProfileList"),
            ),
            (
                "post",
                management_operation(
                    "Create an asset profile",
                    "AssetProfileRequest",
                    "201",
                    "AssetProfile",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/profiles/asset-profiles/{profile_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update an asset profile",
                    "AssetProfileRequest",
                    "200",
                    "AssetProfile",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete an asset profile"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/devices",
        vec![
            (
                "get",
                management_list_operation("List management devices", "ManagementDeviceList"),
            ),
            (
                "post",
                management_operation(
                    "Provision a device",
                    "DeviceProvisionRequest",
                    "201",
                    "DeviceToken",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update a management device",
                    "ManagementDeviceUpdateRequest",
                    "200",
                    "ManagementDevice",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete a management device"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/owner",
        vec![(
            "put",
            documented_operation(
                "Assign or clear a device owner",
                Some("managementSession"),
                Some(("application/json", "ManagementResourceOwnerRequest")),
                ("204", "Device owner updated", None),
                &tenant_management_errors(),
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/telemetry",
        vec![(
            "get",
            documented_operation(
                "List recent raw telemetry for a tenant device",
                Some("managementSession"),
                None,
                (
                    "200",
                    "Recent device telemetry",
                    Some("ManagementDeviceTelemetryPage"),
                ),
                &tenant_management_errors(),
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/claim-code",
        vec![(
            "post",
            documented_operation(
                "Issue or replace a one-time device claim code from the tenant console",
                Some("managementSession"),
                None,
                (
                    "201",
                    "Plaintext claim code, shown only in this response",
                    Some("ManagementDeviceClaimCode"),
                ),
                &[
                    ("401", "No active management session"),
                    ("403", "Tenant account required"),
                    ("409", "Pairing is disabled or the device cannot be claimed"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/tokens",
        vec![(
            "post",
            documented_operation(
                "Issue a device token",
                Some("managementSession"),
                None,
                ("201", "Device token issued", Some("DeviceToken")),
                &[
                    ("400", "Invalid device identifier"),
                    ("401", "No active management session"),
                    ("403", "Administrator role required"),
                    ("404", "Device not found"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/token",
        vec![(
            "get",
            documented_operation(
                "Reveal the active device token",
                Some("managementSession"),
                None,
                ("200", "Active device token", Some("DeviceToken")),
                &[
                    ("401", "No active management session"),
                    ("403", "Tenant account required"),
                    ("404", "Active device token not found"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/devices/{device_id}/tokens/{token_id}/rotate",
        vec![(
            "post",
            documented_operation(
                "Atomically rotate a device token",
                Some("managementSession"),
                None,
                (
                    "201",
                    "Replacement device token issued",
                    Some("DeviceToken"),
                ),
                &[
                    ("400", "Invalid token identifier"),
                    ("401", "No active management session"),
                    ("403", "Tenant account required"),
                    ("404", "Active device token not found"),
                    ("409", "Device token cannot be rotated"),
                    ("503", "Service unavailable"),
                ],
            ),
        )],
    );
    documented_path(
        &mut paths,
        "/api/management/assets",
        vec![
            (
                "get",
                management_list_operation("List management assets", "ManagementAssetList"),
            ),
            (
                "post",
                management_operation(
                    "Create a management asset",
                    "ManagementAssetRequest",
                    "201",
                    "ManagementAsset",
                ),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/assets/{asset_id}",
        vec![
            (
                "put",
                management_operation(
                    "Update a management asset",
                    "ManagementAssetRequest",
                    "200",
                    "ManagementAsset",
                ),
            ),
            (
                "delete",
                management_no_content_operation("Delete a management asset"),
            ),
        ],
    );
    documented_path(
        &mut paths,
        "/api/management/assets/{asset_id}/owner",
        vec![(
            "put",
            documented_operation(
                "Assign or clear an asset owner",
                Some("managementSession"),
                Some(("application/json", "ManagementResourceOwnerRequest")),
                ("204", "Asset owner updated", None),
                &tenant_management_errors(),
            ),
        )],
    );

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "IoT Nano management API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Management and authentication operations."
        },
        "paths": paths,
        "components": {
            "securitySchemes": {
                "managementSession": {
                    "type": "apiKey",
                    "in": "cookie",
                    "name": SESSION_COOKIE
                }
            },
            "schemas": management_openapi_schemas()
        }
    })
}

fn documented_path(paths: &mut Map<String, Value>, path: &str, operations: Vec<(&str, Value)>) {
    let mut item = Map::new();
    for (method, operation) in operations {
        item.insert(method.to_owned(), operation);
    }
    let parameters = path_parameters(path);
    if !parameters.is_empty() {
        item.insert("parameters".to_owned(), Value::Array(parameters));
    }
    paths.insert(path.to_owned(), Value::Object(item));
}

fn path_parameters(path: &str) -> Vec<Value> {
    path.split('/')
        .filter_map(|segment| {
            segment
                .strip_prefix('{')
                .and_then(|segment| segment.strip_suffix('}'))
        })
        .map(|name| {
            json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            })
        })
        .collect()
}

fn management_operation(
    summary: &str,
    request_schema: &str,
    success_status: &str,
    response_schema: &str,
) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        Some(("application/json", request_schema)),
        (success_status, "Request completed", Some(response_schema)),
        &management_errors(),
    )
}

fn management_list_operation(summary: &str, response_schema: &str) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        None,
        ("200", "Request completed", Some(response_schema)),
        &management_errors(),
    )
}

fn ota_upload_operation() -> Value {
    let mut operation = documented_operation(
        "Upload an OTA firmware artifact",
        Some("managementSession"),
        Some(("application/octet-stream", "OtaArtifactUpload")),
        ("201", "Firmware artifact stored", Some("OtaArtifact")),
        &management_errors(),
    );
    operation
        .as_object_mut()
        .expect("documented operation is an object")
        .insert(
            "parameters".to_owned(),
            json!([
                {"name": "x-ota-device-profile-id", "in": "header", "required": true, "schema": {"type": "string", "format": "uuid"}},
                {"name": "x-ota-version", "in": "header", "required": true, "schema": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"}},
                {"name": "x-ota-filename", "in": "header", "required": true, "schema": {"type": "string"}}
            ]),
        );
    operation
}

fn tenant_management_list_operation(summary: &str, response_schema: &str) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        None,
        ("200", "Request completed", Some(response_schema)),
        &tenant_management_errors(),
    )
}

fn tenant_audit_list_operation(summary: &str, response_schema: &str) -> Value {
    let mut operation = tenant_management_list_operation(summary, response_schema);
    operation
        .as_object_mut()
        .expect("documented operation is an object")
        .insert(
            "parameters".to_owned(),
            json!([
                {
                    "name": "after",
                    "in": "query",
                    "required": false,
                    "description": "Opaque keyset cursor for older audit events.",
                    "schema": {"type": "string"}
                },
                {
                    "name": "limit",
                    "in": "query",
                    "required": false,
                    "description": "Maximum number of audit events to return.",
                    "schema": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_TENANT_AUDIT_LIMIT
                    }
                }
            ]),
        );
    operation
}

fn management_no_content_operation(summary: &str) -> Value {
    documented_operation(
        summary,
        Some("managementSession"),
        None,
        ("204", "Request completed", None),
        &management_errors(),
    )
}

fn management_errors() -> [(&'static str, &'static str); 6] {
    [
        ("400", "Invalid request"),
        ("401", "No active management session"),
        ("403", "Administrator role required"),
        ("404", "Resource not found"),
        ("409", "Conflicting request"),
        ("503", "Service unavailable"),
    ]
}

fn tenant_management_errors() -> [(&'static str, &'static str); 6] {
    [
        ("400", "Invalid request"),
        ("401", "No active management session"),
        ("403", "Tenant Account required"),
        ("404", "Resource not found"),
        ("409", "Conflicting request"),
        ("503", "Service unavailable"),
    ]
}

fn documented_operation(
    summary: &str,
    security: Option<&str>,
    request: Option<(&str, &str)>,
    success: (&str, &str, Option<&str>),
    errors: &[(&str, &str)],
) -> Value {
    let mut operation = Map::new();
    operation.insert("summary".to_owned(), Value::String(summary.to_owned()));
    if let Some((content_type, schema)) = request {
        operation.insert(
            "requestBody".to_owned(),
            json!({
                "required": true,
                "content": {content_type: {"schema": schema_reference(schema)}}
            }),
        );
    }
    if let Some(scheme) = security {
        let mut requirement = Map::new();
        requirement.insert(scheme.to_owned(), json!([]));
        operation.insert(
            "security".to_owned(),
            Value::Array(vec![Value::Object(requirement)]),
        );
    }
    let mut responses = Map::new();
    responses.insert(success.0.to_owned(), response_value(success.1, success.2));
    for (status, description) in errors {
        responses.insert(
            (*status).to_owned(),
            response_value(description, Some("Error")),
        );
    }
    operation.insert("responses".to_owned(), Value::Object(responses));
    Value::Object(operation)
}

fn response_value(description: &str, schema: Option<&str>) -> Value {
    let mut response = Map::new();
    response.insert(
        "description".to_owned(),
        Value::String(description.to_owned()),
    );
    if let Some(schema) = schema {
        response.insert(
            "content".to_owned(),
            json!({"application/json": {"schema": schema_reference(schema)}}),
        );
    }
    Value::Object(response)
}

fn schema_reference(schema: &str) -> Value {
    json!({"$ref": format!("#/components/schemas/{schema}")})
}

fn management_openapi_schemas() -> Value {
    let mut schemas = Map::new();
    schemas.insert(
        "Error".to_owned(),
        object_schema(json!({"error": {"type": "string"}}), &["error"]),
    );
    schemas.insert(
        "OtaPolicy".to_owned(),
        object_schema(
            json!({
                "require_matching_device_profile": {"type": "boolean"},
                "require_newer_version": {"type": "boolean"}
            }),
            &["require_matching_device_profile", "require_newer_version"],
        ),
    );
    schemas.insert(
        "OtaArtifact".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "device_profile_id": uuid_schema_non_null(),
                "version": {"type": "string"},
                "filename": {"type": "string"},
                "sha256": {"type": "string"},
                "size_bytes": {"type": "integer", "minimum": 0}
            }),
            &[
                "id",
                "device_profile_id",
                "version",
                "filename",
                "sha256",
                "size_bytes",
            ],
        ),
    );
    schemas.insert(
        "OtaArtifactUpload".to_owned(),
        json!({"type": "string", "format": "binary"}),
    );
    schemas.insert(
        "LoginRequest".to_owned(),
        object_schema(
            json!({
                "username": {"type": "string"},
                "password": {"type": "string", "format": "password"}
            }),
            &["username", "password"],
        ),
    );
    schemas.insert(
        "PlatformLoginResponse".to_owned(),
        object_schema(
            json!({
                "principal_kind": {"type": "string"},
                "principal_id": uuid_schema_non_null(),
                "tenant_id": uuid_schema(),
            }),
            &["principal_kind", "principal_id", "tenant_id"],
        ),
    );
    schemas.insert(
        "SessionResponse".to_owned(),
        object_schema(json!({"user_id": uuid_schema_non_null()}), &["user_id"]),
    );
    schemas.insert(
        "ApplicationRequest".to_owned(),
        object_schema(
            json!({
                "app_id": {"type": "string"},
                "kind": {"type": "string"},
                "launch_url": {"type": "string", "format": "uri"},
                "client_id": {"type": "string"},
                "redirect_uris": {"type": "array", "items": {"type": "string", "format": "uri"}},
                "allowed_scopes": {"type": "array", "items": {"type": "string"}},
                "enabled": {"type": "boolean"}
            }),
            &[
                "app_id",
                "kind",
                "launch_url",
                "client_id",
                "redirect_uris",
                "allowed_scopes",
                "enabled",
            ],
        ),
    );
    schemas.insert(
        "ApplicationResponse".to_owned(),
        object_schema(
            json!({"app_id": {"type": "string"}, "client_id": {"type": "string"}}),
            &["app_id", "client_id"],
        ),
    );
    schemas.insert(
        "TenantProfileConfiguration".to_owned(),
        object_schema(
            json!({
                "version": {"type": "integer", "minimum": 1},
                "profiles": {"type": "array", "items": schema_reference("TenantProfileDefinition")},
                "containment_rules": {"type": "array", "items": schema_reference("TenantProfileContainmentRule")},
                "permission_definitions": {"type": "object"}
            }),
            &["version", "profiles", "containment_rules", "permission_definitions"],
        ),
    );
    schemas.insert(
        "TenantProfileDefinition".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "resource_kind": {"type": "string", "enum": ["asset", "device"]},
                "name": {"type": "string"},
                "definition": {"type": "object"},
                "live_view": {"type": "object"}
            }),
            &["id", "resource_kind", "name", "definition", "live_view"],
        ),
    );
    schemas.insert(
        "TenantProfileContainmentRule".to_owned(),
        object_schema(
            json!({
                "parent_profile_id": uuid_schema_non_null(),
                "child_profile_id": uuid_schema_non_null()
            }),
            &["parent_profile_id", "child_profile_id"],
        ),
    );
    schemas.insert(
        "ManagementUserCreateRequest".to_owned(),
        object_schema(
            json!({
                "username": {"type": "string"},
                "password": {"type": "string", "format": "password"}
            }),
            &["username", "password"],
        ),
    );
    schemas.insert(
        "ManagementUserUpdateRequest".to_owned(),
        object_schema(
            json!({
                "role": {"type": "string"}
            }),
            &[],
        ),
    );
    schemas.insert(
        "ManagementUserCapabilitiesRequest".to_owned(),
        object_schema(
            json!({
                "capabilities": {"type": "array", "items": {"type": "string"}}
            }),
            &["capabilities"],
        ),
    );
    schemas.insert(
        "ManagementUser".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "username": {"type": "string"},
                "role": {"type": "string"},
                "account_class": {"type": "string"},
                "capabilities": {"type": "array", "items": {"type": "string"}}
            }),
            &["id", "username", "role", "account_class", "capabilities"],
        ),
    );
    schemas.insert(
        "ManagementAlert".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "rule_name": {"type": "string"},
                "severity": {"type": "string"},
                "device_id": {"type": "string"},
                "status": {"type": "string"},
                "last_value": {"type": ["number", "null"]},
                "updated_at": {"type": "string", "format": "date-time"}
            }),
            &[
                "id",
                "rule_name",
                "severity",
                "device_id",
                "status",
                "updated_at",
            ],
        ),
    );
    schemas.insert(
        "ManagementAlertSummary".to_owned(),
        object_schema(
            json!({"open_incident_count": {"type": "integer", "minimum": 0}}),
            &["open_incident_count"],
        ),
    );
    schemas.insert(
        "ManagementAlertRuleRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "enabled": {"type": "boolean"},
                "device_id": {"type": ["string", "null"]},
                "metric_key": {"type": "string"},
                "rule_type": {"type": "string", "enum": ["event_threshold", "window_average"]},
                "comparison": {"type": "string", "enum": ["gt", "gte", "lt", "lte"]},
                "threshold": {"type": "number"},
                "window_seconds": {"type": ["integer", "null"], "minimum": 60},
                "for_seconds": {"type": "integer", "minimum": 0},
                "resolve_after_seconds": {"type": "integer", "minimum": 0},
                "reopen_grace_seconds": {"type": "integer", "minimum": 0},
                "hysteresis": {"type": ["number", "null"], "minimum": 0},
                "severity": {"type": "string", "enum": ["info", "warning", "critical"]},
                "reminder_interval_seconds": {"type": "integer", "minimum": 1}
            }),
            &[
                "name",
                "metric_key",
                "rule_type",
                "comparison",
                "threshold",
                "severity",
            ],
        ),
    );
    schemas.insert(
        "ManagementAlertRule".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "name": {"type": "string"},
                "enabled": {"type": "boolean"},
                "device_id": {"type": ["string", "null"]},
                "metric_key": {"type": "string"},
                "rule_type": {"type": "string"},
                "comparison": {"type": "string"},
                "threshold": {"type": "number"},
                "window_seconds": {"type": ["integer", "null"]},
                "for_seconds": {"type": "integer"},
                "resolve_after_seconds": {"type": "integer"},
                "reopen_grace_seconds": {"type": "integer"},
                "hysteresis": {"type": ["number", "null"]},
                "severity": {"type": "string"},
                "reminder_interval_seconds": {"type": "integer"},
                "archived_at": {"type": ["string", "null"], "format": "date-time"},
                "updated_at": {"type": "string", "format": "date-time"}
            }),
            &[
                "id",
                "name",
                "enabled",
                "metric_key",
                "rule_type",
                "comparison",
                "threshold",
                "for_seconds",
                "resolve_after_seconds",
                "reopen_grace_seconds",
                "severity",
                "reminder_interval_seconds",
                "updated_at",
            ],
        ),
    );
    schemas.insert(
        "ManagementAlertIncident".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "rule_id": uuid_schema_non_null(),
                "rule_name": {"type": "string"},
                "severity": {"type": "string"},
                "device_id": {"type": "string"},
                "status": {"type": "string"},
                "last_value": {"type": ["number", "null"]},
                "condition_started_at": {"type": "string", "format": "date-time"},
                "opened_at": {"type": ["string", "null"], "format": "date-time"},
                "resolved_at": {"type": ["string", "null"], "format": "date-time"},
                "acknowledged_at": {"type": ["string", "null"], "format": "date-time"},
                "acknowledged_by": {"type": ["string", "null"]},
                "updated_at": {"type": "string", "format": "date-time"}
            }),
            &[
                "id",
                "rule_id",
                "rule_name",
                "severity",
                "device_id",
                "status",
                "condition_started_at",
                "updated_at",
            ],
        ),
    );
    schemas.insert(
        "ManagementAuditEvent".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "occurred_at": {"type": "string", "format": "date-time"},
                "actor_kind": {"type": "string"},
                "actor_id": uuid_schema_non_null(),
                "action": {"type": "string"},
                "target_type": {"type": "string"},
                "target_id": {"type": "string"},
                "changes": json_object_schema()
            }),
            &[
                "id",
                "occurred_at",
                "actor_kind",
                "actor_id",
                "action",
                "target_type",
                "target_id",
                "changes",
            ],
        ),
    );
    schemas.insert(
        "ManagementAuditEventPage".to_owned(),
        object_schema(
            json!({
                "items": array_schema("ManagementAuditEvent"),
                "next_cursor": {"type": ["string", "null"]},
                "has_more": {"type": "boolean"}
            }),
            &["items", "next_cursor", "has_more"],
        ),
    );
    schemas.insert(
        "DeviceProfileRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "telemetry_schema": json_object_schema(),
                "metric_mapping": json_object_schema(),
                "reporting_settings": json_object_schema()
            }),
            &[
                "name",
                "telemetry_schema",
                "metric_mapping",
                "reporting_settings",
            ],
        ),
    );
    schemas.insert(
        "DeviceProfile".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "name": {"type": "string"},
                "telemetry_schema": json_object_schema(),
                "metric_mapping": json_object_schema(),
                "reporting_settings": json_object_schema()
            }),
            &[
                "id",
                "name",
                "telemetry_schema",
                "metric_mapping",
                "reporting_settings",
            ],
        ),
    );
    schemas.insert(
        "AssetProfileRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "fields": json_object_schema(),
                "dashboard_defaults": json_object_schema()
            }),
            &["name", "fields", "dashboard_defaults"],
        ),
    );
    schemas.insert(
        "AssetProfile".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "name": {"type": "string"},
                "fields": json_object_schema(),
                "dashboard_defaults": json_object_schema()
            }),
            &["id", "name", "fields", "dashboard_defaults"],
        ),
    );
    schemas.insert(
        "DeviceProvisionRequest".to_owned(),
        object_schema(
            json!({
                "display_name": {"type": "string"},
                "asset_id": uuid_schema(),
                "device_profile_id": uuid_schema(),
                "attributes": json_object_schema()
            }),
            &["display_name"],
        ),
    );
    schemas.insert(
        "ManagementDeviceUpdateRequest".to_owned(),
        object_schema(
            json!({
                "display_name": {"type": "string"},
                "asset_id": uuid_schema(),
                "device_profile_id": uuid_schema(),
                "attributes": json_object_schema(),
                "topology": json_object_schema()
            }),
            &["display_name"],
        ),
    );
    schemas.insert(
        "ManagementDevice".to_owned(),
        object_schema(
            json!({
                "device_id": {"type": "string"},
                "display_name": {"type": ["string", "null"]},
                "owner_user_id": uuid_schema(),
                "asset_id": uuid_schema(),
                "device_profile_id": uuid_schema(),
                "attributes": json_object_schema(),
                "online": {"type": "boolean"},
                "last_seen_at": {"type": ["string", "null"], "format": "date-time"},
                "is_gateway": {"type": "boolean"},
                "gateway_device_id": {"type": ["string", "null"]},
                "gateway_status": {"type": ["string", "null"]},
                "child_status": {"type": ["string", "null"]}
            }),
            &["device_id", "attributes", "online", "is_gateway"],
        ),
    );
    schemas.insert(
        "ManagementDeviceTelemetry".to_owned(),
        object_schema(
            json!({
                "event_at": {"type": "string", "format": "date-time"},
                "received_at": {"type": "string", "format": "date-time"},
                "device_id": {"type": "string"},
                "boot_id": {"type": "string"},
                "sequence": {"type": "integer"},
                "measurements": json_object_schema(),
                "topic": {"type": "string"}
            }),
            &[
                "event_at",
                "received_at",
                "device_id",
                "boot_id",
                "sequence",
                "measurements",
                "topic",
            ],
        ),
    );
    schemas.insert(
        "ManagementDeviceTelemetryPage".to_owned(),
        object_schema(
            json!({"items": array_schema("ManagementDeviceTelemetry")}),
            &["items"],
        ),
    );
    schemas.insert(
        "ManagementDeviceClaimCode".to_owned(),
        object_schema(
            json!({
                "device_id": {"type": "string"},
                "code": {"type": "string"},
                "expires_at": {"type": "string", "format": "date-time"}
            }),
            &["device_id", "code", "expires_at"],
        ),
    );
    schemas.insert(
        "DeviceToken".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "device_id": {"type": "string"},
                "token_prefix": {"type": "string"},
                "created_at": {"type": "string", "format": "date-time"},
                "last_used_at": {"type": ["string", "null"], "format": "date-time"},
                "revoked_at": {"type": ["string", "null"], "format": "date-time"},
                "token": {"type": "string"}
            }),
            &[
                "id",
                "device_id",
                "token_prefix",
                "created_at",
                "last_used_at",
                "revoked_at",
            ],
        ),
    );
    schemas.insert(
        "ManagementAssetRequest".to_owned(),
        object_schema(
            json!({
                "name": {"type": "string"},
                "asset_profile_id": uuid_schema(),
                "parent_asset_id": uuid_schema(),
                "metadata": json_object_schema(),
                "attributes": json_object_schema()
            }),
            &["name", "metadata"],
        ),
    );
    schemas.insert(
        "ManagementResourceOwnerRequest".to_owned(),
        object_schema(json!({"user_id": uuid_schema()}), &[]),
    );
    schemas.insert(
        "ManagementAsset".to_owned(),
        object_schema(
            json!({
                "id": uuid_schema_non_null(),
                "name": {"type": "string"},
                "owner_user_id": uuid_schema(),
                "asset_profile_id": uuid_schema(),
                "parent_asset_id": uuid_schema(),
                "metadata": json_object_schema(),
                "attributes": json_object_schema()
            }),
            &["id", "name", "metadata", "attributes"],
        ),
    );

    for (name, item) in [
        ("ManagementUserList", "ManagementUser"),
        ("ManagementAlertList", "ManagementAlert"),
        ("ManagementAlertRuleList", "ManagementAlertRule"),
        ("ManagementAlertIncidentList", "ManagementAlertIncident"),
        ("DeviceProfileList", "DeviceProfile"),
        ("AssetProfileList", "AssetProfile"),
        ("ManagementDeviceList", "ManagementDevice"),
        ("ManagementAssetList", "ManagementAsset"),
        ("OtaArtifactList", "OtaArtifact"),
    ] {
        schemas.insert(name.to_owned(), array_schema(item));
    }

    Value::Object(schemas)
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn array_schema(item: &str) -> Value {
    json!({"type": "array", "items": schema_reference(item)})
}

fn json_object_schema() -> Value {
    json!({"type": "object", "additionalProperties": true})
}

fn uuid_schema() -> Value {
    json!({"type": ["string", "null"], "format": "uuid"})
}

fn uuid_schema_non_null() -> Value {
    json!({"type": "string", "format": "uuid"})
}
