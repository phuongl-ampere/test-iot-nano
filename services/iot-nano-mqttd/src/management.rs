use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;

use crate::{BrokerFileConfig, ManagementConfig};

#[derive(Clone)]
struct ManagementState {
    config: Arc<BrokerFileConfig>,
    native_device_transport_enabled: bool,
    policy_wired: bool,
}

pub fn management_router() -> Router {
    public_router()
}

pub fn management_router_with_config(config: BrokerFileConfig) -> Router {
    management_router_with_config_and_runtime_and_policy(config, false, false)
}

pub fn management_router_with_config_and_runtime(
    config: BrokerFileConfig,
    native_device_transport_enabled: bool,
) -> Router {
    management_router_with_config_and_runtime_and_policy(
        config,
        native_device_transport_enabled,
        false,
    )
}

pub fn management_router_with_config_and_runtime_and_policy(
    config: BrokerFileConfig,
    native_device_transport_enabled: bool,
    policy_wired: bool,
) -> Router {
    let state = ManagementState {
        config: Arc::new(config),
        native_device_transport_enabled,
        policy_wired,
    };
    let mut router = public_router();
    if state.config.management.username.is_some() {
        let protected = Router::new()
            .route("/api/v1/broker/status", get(status))
            .route("/api/v1/broker/config", get(effective_config))
            .layer(middleware::from_fn_with_state(state.clone(), basic_auth))
            .with_state(state);
        router = router.merge(protected);
    }
    router
}

fn public_router() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
}

async fn basic_auth(
    State(state): State<ManagementState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let unauthorized = || {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"iot-mqttd\"")],
            "unauthorized\n",
        )
            .into_response()
    };
    let Some(value) = request.headers().get(header::AUTHORIZATION) else {
        return unauthorized();
    };
    let Ok(value) = value.to_str() else {
        return unauthorized();
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return unauthorized();
    };
    let Ok(decoded) = STANDARD.decode(encoded) else {
        return unauthorized();
    };
    let Ok(credentials) = String::from_utf8(decoded) else {
        return unauthorized();
    };
    let Some((username, password)) = credentials.split_once(':') else {
        return unauthorized();
    };
    let expected_username = state
        .config
        .management
        .username
        .as_deref()
        .unwrap_or_default();
    let expected_password = state
        .config
        .management
        .password
        .as_deref()
        .unwrap_or_default();
    if username != expected_username || password != expected_password {
        return unauthorized();
    }
    next.run(request).await
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn readyz() -> impl IntoResponse {
    (StatusCode::OK, "ready\n")
}

async fn metrics() -> impl IntoResponse {
    (StatusCode::OK, "iot_mqttd_up 1\n")
}

async fn status(State(state): State<ManagementState>) -> impl IntoResponse {
    let mut capabilities = BrokerFileConfig::supported_capabilities();
    if !state.native_device_transport_enabled {
        capabilities.retain(|capability| {
            *capability != crate::Capability::Mqtt311NativeDeviceTransport
                && *capability != crate::Capability::Mqtt5NativeDeviceTransport
        });
    }
    if state.config.listeners.websocket.enabled {
        capabilities.push(if state.config.listeners.websocket.tls {
            crate::Capability::Wss
        } else {
            crate::Capability::WebSocket
        });
    }
    if state.config.bridges.iter().any(|bridge| bridge.enabled) {
        capabilities.push(crate::Capability::Bridge);
    }
    if state.config.rules.iter().any(|rule| rule.enabled) {
        capabilities.push(crate::Capability::Rules);
    }
    if state.policy_wired {
        if state
            .config
            .static_acl
            .as_ref()
            .is_some_and(|acl| acl.enabled)
        {
            capabilities.push(crate::Capability::StaticAcl);
        }
    }
    axum::Json(json!({
        "status": "ok",
        "capabilities": capabilities,
        "metrics": {
            "process_up": 1
        },
        "management_address": state.config.management.address,
    }))
}

async fn effective_config(State(state): State<ManagementState>) -> impl IntoResponse {
    axum::Json(state.config.redacted_json())
}

#[allow(dead_code)]
fn _management_config_is_used(_: &ManagementConfig) {}
