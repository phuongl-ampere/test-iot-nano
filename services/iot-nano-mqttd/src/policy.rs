use std::{collections::HashMap, sync::Arc};

use rumqttd::{AuthHandler, AuthorizationAction, AuthorizationHandler, AuthorizationRequest};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{AclRule, BrokerFileConfig, ConfigError, StaticAclConfig};

const DUMMY_STATIC_PASSWORD: &str = "iot-mqttd-static-password-dummy";

#[derive(Clone)]
pub struct PolicyAdapters {
    pub auth_handler: Option<AuthHandler>,
    pub authorization_handler: Option<AuthorizationHandler>,
}

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("invalid policy configuration: {0}")]
    Config(#[from] ConfigError),
}

pub fn build_policy(config: &BrokerFileConfig) -> Result<PolicyAdapters, PolicyError> {
    config.validate()?;

    if let Some(static_acl) = config.static_acl.as_ref().filter(|acl| acl.enabled) {
        return Ok(static_policy(static_acl));
    }

    Ok(PolicyAdapters {
        auth_handler: None,
        authorization_handler: None,
    })
}

fn static_policy(config: &StaticAclConfig) -> PolicyAdapters {
    let credentials = config
        .users
        .iter()
        .map(|user| (user.username.clone(), user.password.clone()))
        .collect::<HashMap<_, _>>();
    let rules = config.rules.clone();
    let auth_handler: AuthHandler = Arc::new(move |_client_id, username, password| {
        let expected = credentials
            .get(&username)
            .map(String::as_str)
            .unwrap_or(DUMMY_STATIC_PASSWORD);
        let password_matches: bool = expected.as_bytes().ct_eq(password.as_bytes()).into();
        let known_user = credentials.contains_key(&username);
        let allowed = known_user & password_matches;
        Box::pin(async move { allowed })
    });

    let authorization_rules = rules;
    let authorization_handler: AuthorizationHandler = Arc::new(move |request| {
        let allowed = static_authorized(&authorization_rules, &request);
        Box::pin(async move { allowed })
    });

    PolicyAdapters {
        auth_handler: Some(auth_handler),
        authorization_handler: Some(authorization_handler),
    }
}

fn static_authorized(rules: &[AclRule], request: &AuthorizationRequest) -> bool {
    let Some(identity) = request.principal.as_deref() else {
        return false;
    };

    match request.action {
        AuthorizationAction::Connect => true,
        AuthorizationAction::Publish => request.topic.as_deref().is_some_and(|topic| {
            rumqttd::protocol::valid_topic(topic)
                && rules.iter().any(|rule| {
                    rule.publish
                        && rule.identity == identity
                        && mqtt_filter_matches_topic(&rule.topic, topic)
                })
        }),
        AuthorizationAction::Subscribe => request.topic_filter.as_deref().is_some_and(|filter| {
            rumqttd::protocol::valid_filter(filter)
                && rules.iter().any(|rule| {
                    rule.subscribe
                        && rule.identity == identity
                        && mqtt_filter_covers_filter(&rule.topic, filter)
                })
        }),
    }
}

fn mqtt_filter_matches_topic(filter: &str, topic: &str) -> bool {
    let filter_levels = filter.split('/').collect::<Vec<_>>();
    let topic_levels = topic.split('/').collect::<Vec<_>>();
    let mut index = 0;
    while index < filter_levels.len() {
        let level = filter_levels[index];
        if level == "#" {
            return index + 1 == filter_levels.len();
        }
        if index >= topic_levels.len() || (level != "+" && level != topic_levels[index]) {
            return false;
        }
        index += 1;
    }
    index == topic_levels.len()
}

fn mqtt_filter_covers_filter(allowed: &str, requested: &str) -> bool {
    let allowed = allowed.split('/').collect::<Vec<_>>();
    let requested = requested.split('/').collect::<Vec<_>>();
    let mut index = 0;
    while index < allowed.len() {
        let allowed_level = allowed[index];
        if allowed_level == "#" {
            return index + 1 == allowed.len();
        }
        if index >= requested.len() || requested[index] == "#" {
            return false;
        }
        if allowed_level != "+" && allowed_level != requested[index] {
            return false;
        }
        if allowed_level == "+" && requested[index].is_empty() {
            return false;
        }
        index += 1;
    }
    index == requested.len()
}

#[cfg(test)]
mod tests {
    use super::{mqtt_filter_covers_filter, mqtt_filter_matches_topic};

    #[test]
    fn mqtt_filters_match_topics_with_plus_and_hash() {
        assert!(mqtt_filter_matches_topic(
            "sensor/+/temperature",
            "sensor/a/temperature"
        ));
        assert!(mqtt_filter_matches_topic(
            "sensor/#",
            "sensor/a/temperature"
        ));
        assert!(!mqtt_filter_matches_topic(
            "sensor/+/temperature",
            "sensor/a/humidity"
        ));
    }

    #[test]
    fn allowed_filter_covers_requested_subscription_filter() {
        assert!(mqtt_filter_covers_filter("sensor/#", "sensor/+"));
        assert!(mqtt_filter_covers_filter("sensor/+", "sensor/temperature"));
        assert!(!mqtt_filter_covers_filter("sensor/temperature", "sensor/+"));
    }
}
