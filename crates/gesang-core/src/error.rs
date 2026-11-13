use thiserror::Error;

/// Kernel operations preserve authorization, persistence, and uncertain-effect failures.
pub type Result<T> = std::result::Result<T, Error>;

/// Typed kernel failures; external outcomes must not be inferred from transport failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// No resource exists within the authenticated ownership scope.
    #[error("resource not found")]
    NotFound,
    /// A delegation, owner scope, or required capability does not permit the action.
    #[error("permission denied")]
    Forbidden,
    /// One stable request identifier was reused with different immutable content.
    #[error("request identifier already has different content")]
    RequestConflict,
    /// Another operation superseded the expected resource revision.
    #[error("resource version has changed")]
    VersionConflict,
    /// Independent proof that the prior writer stopped or lost I/O access is missing.
    #[error("exclusive writer admission has not been verified")]
    FencingRequired,
    /// The runtime identity, controller lease, or fencing generation is no longer current.
    #[error("runtime generation is no longer current")]
    StaleGeneration,
    /// The current writer has not reached an execution-ready phase.
    #[error("runtime preparation is not complete")]
    RuntimeNotReady,
    /// Local state cannot account for an externally confirmed commit.
    #[error("confirmed storage history is missing or differs from the trusted anchor")]
    FreshnessMismatch,
    /// Database configuration does not provide the required transaction guarantees.
    #[error("durable read-committed database transactions are required")]
    DurabilityRequired,
    /// A command violates its public contract before execution admission.
    #[error("invalid command: {0}")]
    InvalidCommand(String),
    /// Protected payload integrity, binding, or key validation failed.
    #[error("protected data could not be authenticated")]
    InvalidCiphertext,
    /// The host could not obtain cryptographically secure random bytes.
    #[error("secure randomness unavailable")]
    Randomness,
    /// Observation or transport failed without proving an external operation's result.
    #[error("execution result is uncertain: {0}")]
    Uncertain(String),
    /// A transactional platform operation failed.
    #[error("store error: {0}")]
    Store(#[from] sqlx::Error),
    /// An embedded immutable database migration failed.
    #[error("store migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    /// A validated platform or upstream payload could not be encoded or decoded.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
