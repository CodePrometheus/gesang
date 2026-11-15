use std::{num::NonZeroUsize, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, OriginalUri, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use gesang_api::ApiProblem;
use gesang_core::{ConfigurationAccess, PluginCatalog, ResourceBinding, Store};
use gesang_plugins::{Authority, Graph};
use gesang_services::{
    Authenticate, Caller, CreateSandbox, OperationStatus, Sandbox, SandboxOperation,
    authentication_interface, sandbox_interface,
};
use serde::Deserialize;
use uuid::Uuid;
use zeroize::Zeroizing;

/// Authenticated resource routes shared by standalone and embedded hosts.
///
/// Each call resolves the configured authenticator. New resource requests use
/// the current sandbox selection; existing resources use their original binding.
/// Provider endpoints and caller identities never come from request bodies.
#[derive(Clone, Debug)]
pub struct SandboxGateway {
    platform: Store,
    catalog: PluginCatalog,
    graph: Graph,
    configuration: ConfigurationAccess,
    host: Authority,
}

impl SandboxGateway {
    /// Binds routes to one trusted host identity and business scope.
    ///
    /// `configuration` is host administration authority, never user input.
    /// `host` must be a service identity so authentication can run before users
    /// exist. Its binding is also the exact binding accepted from authenticated callers.
    pub fn new(
        platform: Store,
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
            platform,
            catalog,
            graph,
            configuration,
            host,
        })
    }

    /// Builds resource and receipt routes with an explicit request-size policy.
    ///
    /// The embedding host supplies TLS and owns each provider's recovery runner.
    /// Successful admission returns an operation location, not a claim that a
    /// sandbox has finished provisioning.
    pub fn router(self, max_body_bytes: NonZeroUsize) -> Router {
        Router::new()
            .route("/v1/sandboxes", post(create))
            .route("/v1/sandboxes/{id}", get(observe))
            .route("/v1/sandboxes/{id}/stop", post(stop))
            .route("/v1/sandboxes/{id}/operations/{request_id}", get(operation))
            .route("/v1/sandbox-requests/{request_id}", get(request))
            .layer(DefaultBodyLimit::max(max_body_bytes.get()))
            .layer(middleware::map_response(no_store))
            .with_state(self)
    }

    async fn caller(&self, headers: &HeaderMap) -> Result<Caller, ApiFailure> {
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

    async fn service(
        &self,
        binding: &ResourceBinding,
    ) -> Result<gesang_plugins::ServiceHandle<Arc<dyn Sandbox>>, ApiFailure> {
        self.catalog
            .service(
                &self.configuration,
                &self.graph,
                &binding.endpoint,
                &sandbox_interface(),
            )
            .await
            .map_err(|_| ApiFailure::unavailable())
    }
}

async fn create(
    State(gateway): State<SandboxGateway>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Json(request): Json<CreateSandbox>,
) -> Result<Response, ApiFailure> {
    let caller = gateway.caller(&headers).await?;
    let binding = gateway
        .catalog
        .reserve(
            &gateway.configuration,
            caller.authority(),
            &sandbox_interface(),
            request.request_id,
            request.digest(),
        )
        .await
        .map_err(|error| match error {
            gesang_core::CatalogError::Configuration(gesang_core::Error::RequestConflict) => {
                ApiFailure::conflict()
            }
            _ => ApiFailure::unavailable(),
        })?;
    let service = gateway.service(&binding).await?;
    let authority = caller.authority().clone();
    let result = service
        .call(&authority, move |service, _| {
            Box::pin(async move { service.create(&caller, &binding, &request).await })
        })
        .await
        .map_err(|_| ApiFailure::unavailable())??;
    let location = format!(
        "{}/{}/operations/{}",
        uri.path(),
        result.resource_id,
        result.request_id
    );
    receipt(result, location)
}

async fn observe(
    State(gateway): State<SandboxGateway>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Response, ApiFailure> {
    let caller = gateway.caller(&headers).await?;
    let binding = gateway
        .platform
        .resource_binding(caller.authority(), id)
        .await?;
    let service = gateway.service(&binding).await?;
    let authority = caller.authority().clone();
    let result = service
        .call(&authority, move |service, _| {
            Box::pin(async move { service.observe(&caller, &binding).await })
        })
        .await
        .map_err(|_| ApiFailure::unavailable())??;
    Ok(Json(result).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StopRequest {
    request_id: Uuid,
}

async fn stop(
    State(gateway): State<SandboxGateway>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    OriginalUri(uri): OriginalUri,
    Json(request): Json<StopRequest>,
) -> Result<Response, ApiFailure> {
    let caller = gateway.caller(&headers).await?;
    let binding = gateway
        .platform
        .resource_binding(caller.authority(), id)
        .await?;
    let service = gateway.service(&binding).await?;
    let authority = caller.authority().clone();
    let result = service
        .call(&authority, move |service, _| {
            Box::pin(async move { service.stop(&caller, &binding, request.request_id).await })
        })
        .await
        .map_err(|_| ApiFailure::unavailable())??;
    let (base, _) = uri
        .path()
        .rsplit_once('/')
        .ok_or_else(ApiFailure::unavailable)?;
    receipt(result, format!("{base}/operations/{}", request.request_id))
}

async fn operation(
    State(gateway): State<SandboxGateway>,
    headers: HeaderMap,
    Path((id, request_id)): Path<(Uuid, Uuid)>,
) -> Result<Response, ApiFailure> {
    let caller = gateway.caller(&headers).await?;
    let binding = gateway
        .platform
        .resource_binding(caller.authority(), id)
        .await?;
    read_operation(&gateway, caller, binding, request_id).await
}

async fn request(
    State(gateway): State<SandboxGateway>,
    headers: HeaderMap,
    Path(request_id): Path<Uuid>,
) -> Result<Response, ApiFailure> {
    let caller = gateway.caller(&headers).await?;
    let binding = gateway
        .platform
        .resource_request(caller.authority(), &sandbox_interface(), request_id)
        .await?
        .ok_or_else(ApiFailure::not_found)?;
    read_operation(&gateway, caller, binding, request_id).await
}

async fn read_operation(
    gateway: &SandboxGateway,
    caller: Caller,
    binding: ResourceBinding,
    request_id: Uuid,
) -> Result<Response, ApiFailure> {
    let service = gateway.service(&binding).await?;
    let authority = caller.authority().clone();
    let result = service
        .call(&authority, move |service, _| {
            Box::pin(async move { service.operation(&caller, &binding, request_id).await })
        })
        .await
        .map_err(|_| ApiFailure::unavailable())??;
    Ok(Json(result).into_response())
}

fn receipt(operation: SandboxOperation, location: String) -> Result<Response, ApiFailure> {
    let status = match operation.status {
        OperationStatus::Accepted | OperationStatus::Unknown => StatusCode::ACCEPTED,
        OperationStatus::Succeeded | OperationStatus::Failed => StatusCode::OK,
    };
    let mut response = (status, Json(operation)).into_response();
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&location).map_err(|_| ApiFailure::unavailable())?,
    );
    Ok(response)
}

async fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

struct ApiFailure {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiFailure {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "Authentication is required.",
        }
    }
    fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: "The operation is not permitted.",
        }
    }
    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: "The resource or operation was not found.",
        }
    }
    fn conflict() -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "request_conflict",
            message: "The request conflicts with its original content.",
        }
    }
    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "unavailable",
            message: "The service is not available.",
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
            gesang_services::Error::Invalid(_) => Self {
                status: StatusCode::BAD_REQUEST,
                code: "invalid_request",
                message: "The request is invalid.",
            },
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
