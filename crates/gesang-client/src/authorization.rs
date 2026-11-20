//! Browser authorization uses a one-use proof key and deployment discovery.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::{Client, ClientError};

const REDIRECT_URI: &str = "dev.gesang.app:/oauth/callback";

/// Public discovery fields projected from the shared platform contract.
#[derive(Clone, Debug, Deserialize, Serialize, uniffi::Record)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfiguration {
    /// Platform contract version supported by this deployment.
    pub api_version: u32,
    /// Identity issuer independently verified by the server.
    pub issuer: String,
    /// External-browser authorization endpoint.
    pub authorization_endpoint: String,
    /// Standard authorization-code and refresh endpoint.
    pub token_endpoint: String,
    /// Public native client identifier, never a client secret.
    pub client_id: String,
    /// Exact registered callback implemented by this application.
    pub redirect_uri: String,
    /// Deployment-selected scopes, including offline access when configured.
    pub scopes: Vec<String>,
}

/// Device credentials for platform-keystore storage; debug output redacts their contents.
#[derive(Clone, uniffi::Record)]
pub struct TokenSet {
    /// Bearer credential used only for the paired deployment's private API.
    pub access_token: String,
    /// Optional rotating credential bound to the discovered identity registration.
    pub refresh_token: Option<String>,
    /// Absolute expiration in Unix milliseconds, absent when the issuer omits a lifetime.
    pub expires_at_ms: Option<i64>,
}

impl std::fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenSet")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    token_type: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct StoredAuthorization {
    version: u8,
    configuration: ServerConfiguration,
    verifier: String,
    state: String,
}

impl TokenResponse {
    fn into_tokens(self, previous_refresh: Option<String>) -> Result<TokenSet, ClientError> {
        if !self.token_type.eq_ignore_ascii_case("bearer") || self.access_token.is_empty() {
            return Err(ClientError::InvalidResponse);
        }
        let expires_at_ms = self
            .expires_in
            .map(|seconds| {
                i64::try_from(seconds)
                    .ok()
                    .and_then(|s| s.checked_mul(1000))
                    .and_then(|ms| crate::unix_milliseconds(jiff::Timestamp::now()).checked_add(ms))
                    .ok_or(ClientError::InvalidResponse)
            })
            .transpose()?;
        Ok(TokenSet {
            access_token: self.access_token,
            refresh_token: self.refresh_token.or(previous_refresh),
            expires_at_ms,
        })
    }
}

/// One browser authorization attempt with a fresh proof key and one-use callback.
#[derive(uniffi::Object)]
pub struct AuthorizationAttempt {
    http: reqwest::Client,
    configuration: ServerConfiguration,
    authorization_url: String,
    verifier: Zeroizing<String>,
    state: Zeroizing<String>,
    consumed: AtomicBool,
}

impl std::fmt::Debug for AuthorizationAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizationAttempt")
            .finish_non_exhaustive()
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl Client {
    /// Fetch and validate public discovery without forwarding a device bearer token.
    pub async fn configuration(&self) -> Result<ServerConfiguration, ClientError> {
        let response = self
            .http
            .get(self.url(&[".well-known", "gesang-configuration"])?)
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        let source: gesang_api::ServerConfiguration =
            crate::transport::decode_response(response).await?;
        let configuration = ServerConfiguration::from(source);
        configuration.validate(self.allow_insecure_local)?;
        Ok(configuration)
    }

    /// Prepare an S256 proof-key request for an external system browser.
    pub async fn begin_authorization(&self) -> Result<Arc<AuthorizationAttempt>, ClientError> {
        let configuration = self.configuration().await?;
        let verifier = Zeroizing::new(random_url_token()?);
        let state = Zeroizing::new(random_url_token()?);
        let authorization_url = authorization_url(&configuration, &verifier, &state)?;
        Ok(Arc::new(AuthorizationAttempt {
            http: self.http.clone(),
            configuration,
            authorization_url,
            verifier,
            state,
            consumed: AtomicBool::new(false),
        }))
    }

    /// Restore a pending proof key from encrypted device storage after process recreation.
    pub fn restore_authorization(
        &self,
        encoded: Vec<u8>,
    ) -> Result<Arc<AuthorizationAttempt>, ClientError> {
        let encoded = Zeroizing::new(encoded);
        let stored: StoredAuthorization =
            serde_json::from_slice(&encoded).map_err(|_| ClientError::InvalidResponse)?;
        stored.configuration.validate(self.allow_insecure_local)?;
        if stored.version != 1 || !valid_proof(&stored.verifier) || !valid_proof(&stored.state) {
            return Err(ClientError::InvalidResponse);
        }
        let authorization_url =
            authorization_url(&stored.configuration, &stored.verifier, &stored.state)?;
        Ok(Arc::new(AuthorizationAttempt {
            http: self.http.clone(),
            configuration: stored.configuration,
            authorization_url,
            verifier: Zeroizing::new(stored.verifier),
            state: Zeroizing::new(stored.state),
            consumed: AtomicBool::new(false),
        }))
    }

    /// Refresh against the saved registration and preserve an unrotated refresh credential.
    pub async fn refresh_tokens(
        &self,
        configuration: ServerConfiguration,
        refresh_token: String,
    ) -> Result<TokenSet, ClientError> {
        configuration.validate(self.allow_insecure_local)?;
        let response = self
            .http
            .post(&configuration.token_endpoint)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &configuration.client_id),
                ("refresh_token", &refresh_token),
            ])
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        parse_tokens(response)
            .await?
            .into_tokens(Some(refresh_token))
    }
}

#[uniffi::export(async_runtime = "tokio")]
impl AuthorizationAttempt {
    /// Encode the pending proof key for encrypted device storage; never log these bytes.
    pub fn encode(&self) -> Result<Vec<u8>, ClientError> {
        if self.consumed.load(Ordering::Acquire) {
            return Err(ClientError::InvalidInput {
                field: "authorizationAlreadyUsed".into(),
            });
        }
        serde_json::to_vec(&StoredAuthorization {
            version: 1,
            configuration: self.configuration.clone(),
            verifier: self.verifier.to_string(),
            state: self.state.to_string(),
        })
        .map_err(|_| ClientError::InvalidResponse)
    }

    /// Return the browser destination, including this attempt's proof-key challenge.
    pub fn authorization_url(&self) -> String {
        self.authorization_url.clone()
    }

    /// Return the registration to bind encrypted credentials to their issuer.
    pub fn configuration(&self) -> ServerConfiguration {
        self.configuration.clone()
    }

    /// Validate a callback before removing its pending encrypted proof-key record.
    pub fn validate_callback(&self, callback_url: String) -> Result<(), ClientError> {
        self.callback_code(&callback_url).map(|_| ())
    }

    /// Validate callback destination and state, then exchange its one-use code.
    pub async fn complete(&self, callback_url: String) -> Result<TokenSet, ClientError> {
        let code = self.callback_code(&callback_url)?;
        if self.consumed.swap(true, Ordering::AcqRel) {
            return Err(ClientError::InvalidInput {
                field: "authorizationAlreadyUsed".into(),
            });
        }
        let response = self
            .http
            .post(&self.configuration.token_endpoint)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", &self.configuration.client_id),
                ("redirect_uri", &self.configuration.redirect_uri),
                ("code", &code),
                ("code_verifier", self.verifier.as_str()),
            ])
            .send()
            .await
            .map_err(|_| ClientError::ConnectionUnavailable)?;
        parse_tokens(response).await?.into_tokens(None)
    }
}

impl AuthorizationAttempt {
    fn callback_code(&self, callback_url: &str) -> Result<String, ClientError> {
        let callback = Url::parse(callback_url).map_err(|_| ClientError::InvalidResponse)?;
        let mut destination = callback.clone();
        destination.set_query(None);
        destination.set_fragment(None);
        if destination.as_str() != self.configuration.redirect_uri || callback.fragment().is_some()
        {
            return Err(ClientError::InvalidInput {
                field: "redirectUri".into(),
            });
        }
        let state = single_parameter(&callback, "state")?.ok_or(ClientError::InvalidResponse)?;
        if state != self.state.as_str() {
            return Err(ClientError::InvalidInput {
                field: "state".into(),
            });
        }
        if let Some(error) = single_parameter(&callback, "error")? {
            let code = if error
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
                && error.len() <= 64
            {
                error
            } else {
                "authorization_error".into()
            };
            return Err(ClientError::Rejected { code });
        }
        single_parameter(&callback, "code")?
            .filter(|code| !code.is_empty())
            .ok_or(ClientError::InvalidResponse)
    }
}

fn authorization_url(
    configuration: &ServerConfiguration,
    verifier: &str,
    state: &str,
) -> Result<String, ClientError> {
    let mut url = Url::parse(&configuration.authorization_endpoint)
        .map_err(|_| ClientError::InvalidResponse)?;
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    url.query_pairs_mut()
        .append_pair("client_id", &configuration.client_id)
        .append_pair("redirect_uri", &configuration.redirect_uri)
        .append_pair("response_type", "code")
        .append_pair("scope", &configuration.scopes.join(" "))
        .append_pair("state", state)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256");
    Ok(url.into())
}

fn valid_proof(value: &str) -> bool {
    value.len() == 43
        && URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|bytes| bytes.len() == 32)
}

impl ServerConfiguration {
    fn validate(&self, allow_insecure_local: bool) -> Result<(), ClientError> {
        if self.api_version != 1 || self.client_id.is_empty() || self.redirect_uri != REDIRECT_URI {
            return Err(ClientError::InvalidResponse);
        }
        for value in [
            &self.issuer,
            &self.authorization_endpoint,
            &self.token_endpoint,
        ] {
            let url = Url::parse(value).map_err(|_| ClientError::InvalidResponse)?;
            let local = matches!(
                url.host_str(),
                Some("localhost" | "127.0.0.1" | "[::1]" | "::1" | "10.0.2.2")
            );
            if (url.scheme() != "https"
                && !(allow_insecure_local && local && url.scheme() == "http"))
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.fragment().is_some()
            {
                return Err(ClientError::InvalidResponse);
            }
        }
        Ok(())
    }
}

impl From<gesang_api::ServerConfiguration> for ServerConfiguration {
    fn from(value: gesang_api::ServerConfiguration) -> Self {
        Self {
            api_version: value.api_version,
            issuer: value.issuer,
            authorization_endpoint: value.authorization_endpoint,
            token_endpoint: value.token_endpoint,
            client_id: value.client_id,
            redirect_uri: value.redirect_uri,
            scopes: value.scopes,
        }
    }
}

fn random_url_token() -> Result<String, ClientError> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| ClientError::InvalidResponse)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn single_parameter(url: &Url, key: &str) -> Result<Option<String>, ClientError> {
    let mut values = url
        .query_pairs()
        .filter(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned());
    let value = values.next();
    if values.next().is_some() {
        return Err(ClientError::InvalidResponse);
    }
    Ok(value)
}

async fn parse_tokens(response: reqwest::Response) -> Result<TokenResponse, ClientError> {
    if response.status() == reqwest::StatusCode::BAD_REQUEST
        || response.status() == reqwest::StatusCode::UNAUTHORIZED
    {
        return Err(ClientError::AuthenticationRequired);
    }
    crate::transport::decode_response(response).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn stored_proof_restores_the_same_browser_request_and_callback_binding() {
        let client = Client::new("https://gesang.example".into(), None, false).unwrap();
        let configuration = ServerConfiguration {
            api_version: 1,
            issuer: "https://identity.example".into(),
            authorization_endpoint: "https://identity.example/authorize".into(),
            token_endpoint: "https://identity.example/token".into(),
            client_id: "native-client".into(),
            redirect_uri: REDIRECT_URI.into(),
            scopes: vec!["openid".into()],
        };
        let verifier = Zeroizing::new(random_url_token().unwrap());
        let state = Zeroizing::new(random_url_token().unwrap());
        let authorization_url = authorization_url(&configuration, &verifier, &state).unwrap();
        let attempt = AuthorizationAttempt {
            http: client.http.clone(),
            configuration,
            authorization_url,
            verifier,
            state,
            consumed: AtomicBool::new(false),
        };
        let restored = client
            .restore_authorization(attempt.encode().unwrap())
            .unwrap();
        assert_eq!(restored.authorization_url(), attempt.authorization_url());
        let callback = format!(
            "{REDIRECT_URI}?state={}&code=one-use-code",
            attempt.state.as_str()
        );
        assert!(restored.validate_callback(callback).is_ok());
        assert!(
            restored
                .validate_callback(format!(
                    "{REDIRECT_URI}?state=another-attempt&code=one-use-code"
                ))
                .is_err()
        );
        assert!(
            restored
                .validate_callback(format!(
                    "dev.gesang.app:/other?state={}&code=one-use-code",
                    attempt.state.as_str()
                ))
                .is_err()
        );
        restored.consumed.store(true, Ordering::Release);
        assert!(restored.encode().is_err());
    }

    #[test]
    fn duplicate_callback_parameters_are_rejected() {
        let callback = Url::parse("dev.gesang.app:/oauth/callback?state=one&state=two").unwrap();
        assert!(single_parameter(&callback, "state").is_err());
    }

    #[test]
    fn refresh_rotation_preserves_the_previous_token_when_not_rotated() {
        let tokens = TokenResponse {
            access_token: "new-access".into(),
            token_type: "Bearer".into(),
            refresh_token: None,
            expires_in: None,
        }
        .into_tokens(Some("previous-refresh".into()))
        .unwrap();
        assert_eq!(tokens.refresh_token.as_deref(), Some("previous-refresh"));
        assert!(tokens.expires_at_ms.is_none());
    }

    #[test]
    fn proof_keys_have_required_entropy_and_url_encoding() {
        let first = random_url_token().unwrap();
        let second = random_url_token().unwrap();
        assert_eq!(first.len(), 43);
        assert!(!first.contains(['+', '/', '=']));
        assert_ne!(first, second);
    }
}
