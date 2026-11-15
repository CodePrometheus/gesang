//! One authenticated connection pool serves a single configured deployment.

use std::sync::Arc;

use gesang_api::{ApiProblem, Grant, GrantSelection, SessionSnapshot};
use reqwest::{Client as HttpClient, Method, StatusCode, Url, header};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{ClientError, GrantView, Permission, ReceiptView, Scope, SessionView};

/// A deployment-scoped connection pool; dropping it does not stop cloud execution.
#[derive(uniffi::Object)]
pub struct Client {
    pub(crate) http: HttpClient,
    pub(crate) endpoint: Url,
    authorization: Option<header::HeaderValue>,
    pub(crate) tls: Arc<rustls::ClientConfig>,
    pub(crate) allow_insecure_local: bool,
    pub(crate) goals: Arc<crate::goals::GoalCache>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    /// Register the configured Firebase installation after explicit device consent.
    pub async fn register_device_push(
        &self,
        device_id: String,
        project_id: String,
        installation_id: String,
    ) -> Result<(), ClientError> {
        let id = checked_id(&device_id, "deviceId")?;
        if project_id.is_empty() || installation_id.is_empty() {
            return Err(ClientError::InvalidInput {
                field: "pushTarget".into(),
            });
        }
        let request = gesang_api::PushRegistration {
            provider: gesang_api::PushProvider::Fcm,
            project_id,
            target: gesang_api::PushTarget::FirebaseInstallation {
                id: installation_id,
            },
        };
        let registered: gesang_api::DevicePushRegistration = self
            .send_json(
                Method::PUT,
                &["v1", "devices", &device_id, "push"],
                &request,
            )
            .await?;
        if registered.device_id != id
            || registered.status != gesang_api::PushRegistrationStatus::Registered
        {
            return Err(ClientError::InvalidResponse);
        }
        Ok(())
    }

    /// Revoke the server destination once; a lost response must remain visibly uncertain.
    pub async fn revoke_device_push(&self, device_id: String) -> Result<(), ClientError> {
        checked_id(&device_id, "deviceId")?;
        let response = self
            .request(
                Method::DELETE,
                self.url(&["v1", "devices", &device_id, "push"])?,
            )
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        let response = check_response(response).await?;
        if response.status() != StatusCode::NO_CONTENT {
            return Err(ClientError::InvalidResponse);
        }
        Ok(())
    }

    #[uniffi::constructor]
    /// Connect to an HTTPS deployment; plaintext is restricted to explicit local development.
    pub fn new(
        endpoint: String,
        access_token: Option<String>,
        allow_insecure_local: bool,
    ) -> Result<Arc<Self>, ClientError> {
        let endpoint = validated_endpoint(&endpoint, allow_insecure_local)?;
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = Arc::new(
            rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| ClientError::InvalidResponse)?
            .with_root_certificates(roots)
            .with_no_client_auth(),
        );
        let authorization = if let Some(token) = access_token {
            let mut value =
                header::HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                    ClientError::InvalidInput {
                        field: "accessToken".into(),
                    }
                })?;
            value.set_sensitive(true);
            Some(value)
        } else {
            None
        };
        let http = HttpClient::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .tls_backend_preconfigured(tls.as_ref().clone())
            .build()
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        Ok(Arc::new(Self {
            http,
            endpoint,
            authorization,
            tls,
            allow_insecure_local,
            goals: Default::default(),
        }))
    }

    /// Fetch the authenticated owner and the server's current grant.
    pub async fn session(&self) -> Result<SessionView, ClientError> {
        let session: SessionSnapshot = self.get(&["v1", "self"]).await?;
        Ok(session.into())
    }

    /// Persist an explicit authority choice and return its acknowledged grant.
    pub async fn select_permission(
        &self,
        agent_id: String,
        permission: Permission,
        scopes: Vec<Scope>,
    ) -> Result<GrantView, ClientError> {
        checked_id(&agent_id, "agentId")?;
        let selection = GrantSelection {
            mode: permission.into(),
            scopes: scopes.into_iter().map(Into::into).collect(),
        };
        let grant: Grant = self
            .send_json(
                Method::PUT,
                &["v1", "agents", &agent_id, "grant"],
                &selection,
            )
            .await?;
        Ok(grant.into())
    }

    /// Revoke new admissions using a grant; in-flight effects remain independently observable.
    pub async fn revoke_grant(
        &self,
        agent_id: String,
        grant_id: String,
    ) -> Result<(), ClientError> {
        checked_id(&agent_id, "agentId")?;
        checked_id(&grant_id, "grantId")?;
        let response = self
            .request(
                Method::DELETE,
                self.url(&["v1", "agents", &agent_id, "grant", &grant_id])?,
            )
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        check_response(response).await?;
        Ok(())
    }

    /// Query the original receipt without creating another execution request.
    pub async fn request_status(&self, request_id: String) -> Result<ReceiptView, ClientError> {
        checked_id(&request_id, "requestId")?;
        let receipt: gesang_api::CommandReceipt =
            self.get(&["v1", "requests", &request_id]).await?;
        receipt.try_into()
    }
}

impl Client {
    pub(crate) async fn engine(
        &self,
        agent_id: &str,
    ) -> Result<chopin_protocol::ChordClient, ClientError> {
        checked_id(agent_id, "agentId")?;
        let authorization = self
            .authorization
            .as_ref()
            .ok_or(ClientError::AuthenticationRequired)?;
        let token = authorization
            .to_str()
            .map_err(|_| ClientError::InvalidResponse)?
            .strip_prefix("Bearer ")
            .ok_or(ClientError::InvalidResponse)?;
        let mut url = self.url(&["v1", "agents", agent_id, "connect"])?;
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .map_err(|_| ClientError::InvalidResponse)?;
        chopin_protocol::ChordClient::connect_with(
            url.as_str(),
            token,
            chopin_protocol::TlsConnector::Rustls(self.tls.clone()),
            chopin_protocol::ConnectionOptions::default(),
        )
        .await
        .map_err(crate::observation::protocol_error)
    }

    pub(crate) fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        let request = self.http.request(method, url);
        match &self.authorization {
            Some(authorization) => request.header(header::AUTHORIZATION, authorization.clone()),
            None => request,
        }
    }

    pub(crate) fn url(&self, segments: &[&str]) -> Result<Url, ClientError> {
        let mut url = self.endpoint.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| ClientError::InvalidResponse)?;
        path.pop_if_empty();
        path.extend(segments);
        drop(path);
        Ok(url)
    }

    pub(crate) async fn get<T: DeserializeOwned>(
        &self,
        segments: &[&str],
    ) -> Result<T, ClientError> {
        let response = self
            .request(Method::GET, self.url(segments)?)
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        decode_response(response).await
    }

    pub(crate) async fn send_json<T: DeserializeOwned>(
        &self,
        method: Method,
        segments: &[&str],
        body: &impl Serialize,
    ) -> Result<T, ClientError> {
        let response = self
            .request(method, self.url(segments)?)
            .json(body)
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        decode_response(response).await
    }
}

pub(crate) fn checked_id(value: &str, field: &str) -> Result<Uuid, ClientError> {
    Uuid::parse_str(value).map_err(|_| ClientError::InvalidInput {
        field: field.into(),
    })
}

pub(crate) async fn decode_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, ClientError> {
    check_response(response)
        .await?
        .json()
        .await
        .map_err(|_| ClientError::InvalidResponse)
}

async fn check_response(response: reqwest::Response) -> Result<reqwest::Response, ClientError> {
    match response.status() {
        status if status.is_success() => Ok(response),
        StatusCode::UNAUTHORIZED => Err(ClientError::AuthenticationRequired),
        StatusCode::FORBIDDEN => Err(ClientError::PermissionDenied),
        StatusCode::NOT_FOUND => Err(ClientError::NotFound),
        StatusCode::CONFLICT => Err(ClientError::Conflict),
        _ => {
            let problem = response
                .json::<ApiProblem>()
                .await
                .map_err(|_| ClientError::InvalidResponse)?;
            Err(ClientError::Rejected { code: problem.code })
        }
    }
}

fn validated_endpoint(value: &str, allow_insecure_local: bool) -> Result<Url, ClientError> {
    let mut endpoint = Url::parse(value).map_err(|_| ClientError::InvalidAddress {
        reason: "Use a complete HTTPS address.".into(),
    })?;
    let local = matches!(
        endpoint.host_str(),
        Some("localhost" | "127.0.0.1" | "[::1]" | "::1" | "10.0.2.2")
    );
    if endpoint.scheme() != "https"
        && !(allow_insecure_local && local && endpoint.scheme() == "http")
    {
        return Err(ClientError::InvalidAddress {
            reason: "HTTPS is required.".into(),
        });
    }
    if !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.host_str().is_none()
    {
        return Err(ClientError::InvalidAddress {
            reason: "Remove credentials, query parameters and fragments.".into(),
        });
    }
    if !endpoint.path().ends_with('/') {
        endpoint.set_path(&format!("{}/", endpoint.path()));
    }
    Ok(endpoint)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_keeps_host_prefix_and_encodes_opaque_identifiers() {
        let client = Client::new("https://agent.example/service".into(), None, false).unwrap();
        assert_eq!(
            client.url(&["v1", "goals", "a/b ?"]).unwrap().as_str(),
            "https://agent.example/service/v1/goals/a%2Fb%20%3F"
        );
    }

    #[test]
    fn release_endpoint_rejects_plaintext_and_embedded_secrets() {
        for value in [
            "http://localhost",
            "https://u:p@agent.example",
            "https://agent.example?token=x",
            "https://agent.example/#token",
        ] {
            assert!(validated_endpoint(value, false).is_err(), "{value}");
        }
        assert!(validated_endpoint("http://10.0.2.2:8080", true).is_ok());
        assert!(validated_endpoint("http://agent.example", true).is_err());
    }

    #[test]
    fn tokens_cannot_inject_http_headers() {
        assert!(
            Client::new(
                "https://agent.example".into(),
                Some("bad\r\nvalue".into()),
                false
            )
            .is_err()
        );
    }
}
