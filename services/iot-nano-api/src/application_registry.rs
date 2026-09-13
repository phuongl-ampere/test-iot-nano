use std::{str::FromStr, sync::Arc};

use iot_storage::{
    ApplicationRecord, ApplicationRepository, ClientId, PlatformStore, PlatformStoreError,
    RedirectUri,
};

#[derive(Clone)]
pub(crate) struct ApplicationRegistry {
    store: Arc<PlatformStore>,
}

#[derive(Debug)]
pub(crate) enum ApplicationRegistryError {
    UnknownClient,
    Disabled,
    RedirectDenied,
    ScopeDenied,
    Unavailable,
}

impl ApplicationRegistry {
    pub(crate) fn new(store: Arc<PlatformStore>) -> Self {
        Self { store }
    }

    pub(crate) async fn validate_authorization_request(
        &self,
        client_id: &str,
        redirect_uri: &str,
        scopes: &[String],
    ) -> Result<ApplicationRecord, ApplicationRegistryError> {
        let client_id =
            ClientId::from_str(client_id).map_err(|_| ApplicationRegistryError::UnknownClient)?;
        let redirect_uri = RedirectUri::from_str(redirect_uri)
            .map_err(|_| ApplicationRegistryError::RedirectDenied)?;
        let application = ApplicationRepository::find_application_by_client_id(
            self.store.as_ref(),
            client_id.as_str(),
        )
        .await
        .map_err(|error| match error {
            PlatformStoreError::ApplicationDisabled(_) => ApplicationRegistryError::Disabled,
            _ => ApplicationRegistryError::Unavailable,
        })?
        .ok_or(ApplicationRegistryError::UnknownClient)?;
        if !application.enabled {
            return Err(ApplicationRegistryError::Disabled);
        }
        if !application
            .redirect_uris
            .iter()
            .any(|registered| registered == &redirect_uri)
        {
            return Err(ApplicationRegistryError::RedirectDenied);
        }
        if !scopes
            .iter()
            .all(|scope| application.allowed_scopes.binary_search(scope).is_ok())
        {
            return Err(ApplicationRegistryError::ScopeDenied);
        }

        Ok(application)
    }
}
