use std::{
    collections::HashMap,
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rumqttd::{AuthHandler, AuthorizationAction, AuthorizationHandler, AuthorizationRequest};
use serde::Serialize;
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{AclRule, BrokerFileConfig, ConfigError, HttpAuthorizationConfig, StaticAclConfig};

const HTTP_AUTHORIZATION_SECRET_HEADER: &str = "x-iot-mqttd-authorization-secret";
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
    #[error("HTTP authorization timeout must be finite and greater than zero")]
    InvalidHttpTimeout,
    #[error("HTTP authorization client setup failed")]
    HttpClient(#[source] reqwest::Error),
}

pub fn build_policy(config: &BrokerFileConfig) -> Result<PolicyAdapters, PolicyError> {
    config.validate()?;

    if let Some(static_acl) = config.static_acl.as_ref().filter(|acl| acl.enabled) {
        return Ok(static_policy(static_acl));
    }

    if let Some(http) = config
        .http_authorization
        .as_ref()
        .filter(|authorization| authorization.enabled)
    {
        return http_policy(http);
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

#[derive(Clone)]
struct HttpPolicy {
    client: reqwest::Client,
    url: String,
    secret: String,
    cache_ttl: Duration,
    cache: Arc<Mutex<CacheStore>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    client_id: String,
    principal: Option<String>,
    username: Option<String>,
    action: AuthorizationActionKey,
    topic: Option<String>,
    topic_filter: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AuthorizationActionKey {
    Publish,
    Subscribe,
}

#[derive(Debug, Clone, Copy)]
struct CachedDecision {
    allowed: bool,
    expires_at: Instant,
}

struct CacheStore {
    entries: HashMap<CacheKey, CachedDecision>,
    order: VecDeque<CacheKey>,
    capacity: usize,
}

#[derive(Debug, Serialize)]
struct HttpPolicyRequest {
    client_id: String,
    username: Option<String>,
    action: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_filter: Option<String>,
}

fn http_policy(config: &HttpAuthorizationConfig) -> Result<PolicyAdapters, PolicyError> {
    if config.timeout_ms == 0 {
        return Err(PolicyError::InvalidHttpTimeout);
    }
    let timeout = Duration::from_millis(config.timeout_ms);
    let client = reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(PolicyError::HttpClient)?;
    let policy = HttpPolicy {
        client,
        url: config.url.clone(),
        secret: config.secret.clone(),
        cache_ttl: Duration::from_secs(config.cache_ttl_seconds),
        cache: Arc::new(Mutex::new(CacheStore {
            entries: HashMap::new(),
            order: VecDeque::new(),
            capacity: config.cache_capacity,
        })),
    };

    let authenticator_policy = policy.clone();
    let auth_handler: AuthHandler = Arc::new(move |client_id, username, password| {
        let policy = authenticator_policy.clone();
        Box::pin(async move {
            policy
                .request(HttpPolicyRequest {
                    client_id,
                    username: Some(username),
                    action: "connect",
                    password: Some(password),
                    topic: None,
                    topic_filter: None,
                })
                .await
        })
    });

    let authorization_policy = policy;
    let authorization_handler: AuthorizationHandler = Arc::new(move |request| {
        let policy = authorization_policy.clone();
        Box::pin(async move { policy.authorize(request).await })
    });

    Ok(PolicyAdapters {
        auth_handler: Some(auth_handler),
        authorization_handler: Some(authorization_handler),
    })
}

impl HttpPolicy {
    async fn authorize(&self, request: AuthorizationRequest) -> bool {
        if request.action == AuthorizationAction::Connect {
            return request.principal.is_some();
        }

        if request.principal.is_none() {
            return false;
        }
        let Some((action, topic)) = topic_key(&request) else {
            return false;
        };
        if action == AuthorizationActionKey::Subscribe && !rumqttd::protocol::valid_filter(topic) {
            return false;
        }
        if action == AuthorizationActionKey::Publish && !rumqttd::protocol::valid_topic(topic) {
            return false;
        }
        let key = CacheKey {
            client_id: request.client_id.clone(),
            principal: request.principal.clone(),
            username: request.username.clone(),
            action,
            topic: request.topic.clone(),
            topic_filter: request.topic_filter.clone(),
        };

        if self.cache_ttl != Duration::ZERO {
            if let Ok(mut cache) = self.cache.lock() {
                if let Some(allowed) = cache.get(&key) {
                    return allowed;
                }
            }
        }

        let allowed = self
            .request(HttpPolicyRequest {
                client_id: request.client_id,
                username: request.username,
                action: action.as_str(),
                password: None,
                topic: request.topic.clone(),
                topic_filter: request.topic_filter.clone(),
            })
            .await;

        if self.cache_ttl != Duration::ZERO {
            if let Ok(mut cache) = self.cache.lock() {
                if let Some(expires_at) = Instant::now().checked_add(self.cache_ttl) {
                    cache.insert(
                        key,
                        CachedDecision {
                            allowed,
                            expires_at,
                        },
                    );
                }
            }
        }
        allowed
    }

    async fn request(&self, request: HttpPolicyRequest) -> bool {
        self.client
            .post(&self.url)
            .header(HTTP_AUTHORIZATION_SECRET_HEADER, &self.secret)
            .json(&request)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }
}

impl CacheStore {
    fn get(&mut self, key: &CacheKey) -> Option<bool> {
        let now = Instant::now();
        let expired = self
            .entries
            .iter()
            .filter(|(_, decision)| decision.expires_at <= now)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            self.remove(&key);
        }
        self.entries.get(key).map(|decision| decision.allowed)
    }

    fn insert(&mut self, key: CacheKey, decision: CachedDecision) {
        if self.capacity == 0 {
            return;
        }
        self.remove(&key);
        while self.entries.len() >= self.capacity {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
        self.order.push_back(key.clone());
        self.entries.insert(key, decision);
    }

    fn remove(&mut self, key: &CacheKey) {
        self.entries.remove(key);
        self.order.retain(|entry| entry != key);
    }
}

fn topic_key(request: &AuthorizationRequest) -> Option<(AuthorizationActionKey, &str)> {
    match request.action {
        AuthorizationAction::Publish => request
            .topic
            .as_deref()
            .map(|topic| (AuthorizationActionKey::Publish, topic)),
        AuthorizationAction::Subscribe => request
            .topic_filter
            .as_deref()
            .map(|filter| (AuthorizationActionKey::Subscribe, filter)),
        AuthorizationAction::Connect => None,
    }
}

impl AuthorizationActionKey {
    fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
        }
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
    use std::time::{Duration, Instant};

    use super::{
        AuthorizationActionKey, CacheKey, CacheStore, CachedDecision, mqtt_filter_covers_filter,
        mqtt_filter_matches_topic,
    };

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

    #[test]
    fn cache_evicts_oldest_entry_when_capacity_is_reached() {
        let mut cache = CacheStore {
            entries: Default::default(),
            order: Default::default(),
            capacity: 2,
        };
        let decision = CachedDecision {
            allowed: true,
            expires_at: Instant::now() + Duration::from_secs(60),
        };
        let key = |client_id: &str| CacheKey {
            client_id: client_id.into(),
            principal: Some("alice".into()),
            username: Some("alice".into()),
            action: AuthorizationActionKey::Publish,
            topic: Some("task5/topic".into()),
            topic_filter: None,
        };
        cache.insert(key("first"), decision);
        cache.insert(key("second"), decision);
        cache.insert(key("third"), decision);

        assert_eq!(cache.get(&key("first")), None);
        assert_eq!(cache.get(&key("second")), Some(true));
        assert_eq!(cache.get(&key("third")), Some(true));
    }
}
