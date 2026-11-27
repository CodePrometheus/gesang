//! Host-bound authentication and shared HTTP error responses.

use std::sync::Arc;

use axum::{
    Json,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use gesang_api::ApiProblem;
use gesang_core::{ConfigurationAccess, Owner, PluginCatalog};
use gesang_plugins::{Authority, Graph, ResourceScope};
use gesang_services::{Authenticate, Caller, authentication_interface};
use zeroize::Zeroizing;

#[derive(Clone, Debug)]
pub(super) struct HostAccess {
    pub(super) catalog: PluginCatalog,
    pub(super) graph: Graph,
    pub(super) configuration: ConfigurationAccess,
    host: Authority,
}

impl HostAccess {
    pub(super) fn new(
        catalog: PluginCatalog,
        graph: Graph,
        configuration: ConfigurationAccess,
        host: Authority,
    ) -> gesang_services::Result<Self> {
        if host.owner_scope().is_some() {
            return Err(gesang_services::Error::Invalid(
                "a host service identity is required".into(),
            ));
        }
        Ok(Self {
            catalog,
            graph,
            configuration,
            host,
        })
    }

    pub(super) async fn caller(&self, headers: &HeaderMap) -> Result<Caller, ApiFailure> {
        let mut credentials = headers.get_all(header::AUTHORIZATION).iter();
        let value = credentials.next().ok_or_else(ApiFailure::unauthorized)?;
        if credentials.next().is_some() {
            return Err(ApiFailure::unauthorized());
        }
        let (scheme, value) = value
            .to_str()
            .map_err(|_| ApiFailure::unauthorized())?
            .split_once(' ')
            .ok_or_else(ApiFailure::unauthorized)?;
        if !scheme.eq_ignore_ascii_case("Bearer") || value.is_empty() {
            return Err(ApiFailure::unauthorized());
        }
        let credential = Zeroizing::new(value.to_owned());
        let interface = authentication_interface();
        let endpoint = self
            .catalog
            .selected(&self.configuration, self.host.binding(), &interface)
            .await
            .map_err(|_| ApiFailure::unavailable())?;
        let service = self
            .catalog
            .service::<Arc<dyn Authenticate>>(
                &self.configuration,
                &self.graph,
                &endpoint,
                &interface,
            )
            .await
            .map_err(|_| ApiFailure::unavailable())?;
        let caller = service
            .call(&self.host, move |service, _| {
                Box::pin(async move { service.authenticate(&credential).await })
            })
            .await
            .map_err(|_| ApiFailure::unavailable())??;
        if caller.authority().binding() != self.host.binding() {
            return Err(ApiFailure::forbidden());
        }
        Ok(caller)
    }

    pub(super) async fn owner(&self, headers: &HeaderMap) -> Result<(Owner, Caller), ApiFailure> {
        let caller = self.caller(headers).await?;
        let Some(ResourceScope::Owner { owner_id }) = caller.authority().owner_scope() else {
            return Err(ApiFailure::forbidden());
        };
        Ok((Owner { id: *owner_id }, caller))
    }
}

pub(super) async fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

pub(super) struct ApiFailure {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiFailure {
    pub(super) fn invalid() -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: "The request is invalid.",
        }
    }

    pub(super) fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "Authentication is required.",
        }
    }
    pub(super) fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: "The operation is not permitted.",
        }
    }
    pub(super) fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: "The resource or operation was not found.",
        }
    }
    pub(super) fn conflict() -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "request_conflict",
            message: "The request conflicts with its original content.",
        }
    }
    pub(super) fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "unavailable",
            message: "The service is not available.",
        }
    }

    pub(super) fn artifact_too_large() -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "artifact_too_large",
            message: "The artifact exceeds the download limit.",
        }
    }
}

impl From<gesang_services::Error> for ApiFailure {
    fn from(error: gesang_services::Error) -> Self {
        match error {
            gesang_services::Error::Unauthorized => Self::unauthorized(),
            gesang_services::Error::Forbidden => Self::forbidden(),
            gesang_services::Error::NotFound => Self::not_found(),
            gesang_services::Error::Conflict => Self::conflict(),
            gesang_services::Error::Platform(error) => error.into(),
            gesang_services::Error::Invalid(_) => Self::invalid(),
            _ => Self::unavailable(),
        }
    }
}

impl From<gesang_core::Error> for ApiFailure {
    fn from(error: gesang_core::Error) -> Self {
        match error {
            gesang_core::Error::Forbidden => Self::forbidden(),
            gesang_core::Error::NotFound => Self::not_found(),
            gesang_core::Error::RequestConflict | gesang_core::Error::VersionConflict => {
                Self::conflict()
            }
            _ => Self::unavailable(),
        }
    }
}

impl IntoResponse for ApiFailure {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(ApiProblem {
                code: self.code.into(),
                message: self.message.into(),
            }),
        )
            .into_response();
        if self.status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}
