/// Service failures preserve permission denials and uncertain external outcomes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The request has no valid current credential.
    #[error("service authentication failed")]
    Unauthorized,
    /// The caller's current grant does not allow this operation.
    #[error("service operation is not permitted")]
    Forbidden,
    /// No matching record exists within the verified caller's scope.
    #[error("service resource not found")]
    NotFound,
    /// One original request was reused with different immutable content.
    #[error("service request conflicts with its original content")]
    Conflict,
    /// A request violates the service contract before external effects.
    #[error("invalid service request: {0}")]
    Invalid(String),
    /// External work may have occurred; the original operation requires reconciliation.
    #[error("the external operation outcome is not yet known")]
    Uncertain,
    /// Secure credential generation failed.
    #[error("secure randomness is unavailable")]
    Randomness,
    /// A shared platform transaction or invariant failed.
    #[error(transparent)]
    Platform(#[from] gesang_core::Error),
    /// A service-owned transactional operation failed.
    #[error("service store operation failed: {0}")]
    Store(#[from] sqlx::Error),
    /// A numbered service migration failed.
    #[error("service migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// Persisted typed metadata could not be decoded.
    #[error("service metadata could not be decoded")]
    Metadata(#[from] serde_json::Error),
    /// The provider transport failed without proving the operation's outcome.
    #[error("resource provider transport failed")]
    Transport(#[from] reqwest::Error),
    /// The protected client's cryptographic configuration could not be constructed.
    #[error("resource client cryptographic configuration failed")]
    Tls(#[from] rustls::Error),
    /// The provider returned a status outside the operation's documented contract.
    #[error("resource provider returned HTTP {status}")]
    ProviderStatus {
        /// Actual response status, without response bodies or credentials.
        status: reqwest::StatusCode,
    },
    /// The endpoint no longer identifies the originally configured provider.
    #[error("resource provider identity changed")]
    ProviderIdentity,
    /// A physical instance does not match the original resource and configuration.
    #[error("physical resource identity does not match its binding")]
    ResourceIdentity,
    /// Provider data cannot establish the required typed observation.
    #[error("resource provider returned invalid data")]
    ProviderData,
    /// Persisted service state violates its versioned transition contract.
    #[error("stored sandbox state is invalid")]
    InvalidState,
}

/// A service result that distinguishes rejection from uncertainty.
pub type Result<T> = std::result::Result<T, Error>;
