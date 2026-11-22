use std::{
    collections::HashMap,
    fmt::Write as _,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chopin_protocol::{
    ChordClient, ConversationState, ConversationSummary, CorrectionKind, EngineCommand,
    GoalCommand, GoalPage, GoalSnapshot, HeldChange, LoopChange, LoopSettings, LoopStanding,
    ModelChoices, ModelSettingsSnapshot, ProtocolError, PublishedArtifact, StateObserver,
    TranscriptPage,
};
use gesang_api::{ApiProblem, CommandAuthorization};
use gesang_core::{
    CommandRequirements, Dispatch, EngineBridge, EnginePreparation, Error, ExecutionResponse,
    InstancePhase, InstancePurpose, RequiredAuthority, Result, Runtime, Store, Wake,
};
use jiff::Timestamp;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as AsyncMutex, RwLock, watch};
use uuid::Uuid;

use crate::{PersonalCredentialChange, PersonalCredentialClaims, SubscriptionChallenge};

mod observation;
pub use observation::EngineObservation;

/// The checked bytes from one original publication.
#[derive(Debug)]
pub struct ArtifactDownload {
    /// Source metadata for the exact publication.
    pub metadata: PublishedArtifact,
    /// Complete, validated file bytes.
    pub bytes: Vec<u8>,
    /// Verified binary SHA-256.
    pub digest: [u8; 32],
}

/// Outcome of checking an original publication against the host's download policy.
#[derive(Debug)]
pub enum ArtifactRead {
    /// The publication does not belong to this goal or has not finished.
    Missing,
    /// The announced file exceeded the host's limit before any page was read.
    TooLarge,
    /// The complete file has been validated.
    Complete(ArtifactDownload),
}

/// Credential-free commands for the owner's original subscription service.
#[derive(Debug)]
pub enum SubscriptionChange {
    /// Starts a connection or prepares a separate switch candidate.
    Start {
        /// Provider selected by the owner.
        provider: String,
        /// Whether to keep the active account until the replacement is confirmed.
        switch_account: bool,
    },
    /// Cancels just the selected sign-in attempt.
    Cancel {
        /// The original attempt identity.
        attempt: Uuid,
    },
    /// Disconnects the selected provider's expected account.
    Disconnect {
        /// Provider selected by the owner.
        provider: String,
        /// Stable identity of the account to disconnect.
        expected_account_id: Uuid,
    },
}

/// Explicit host configuration for sequential execution RPC connections.
#[derive(Clone, Debug)]
pub struct EngineConnectionPolicy {
    /// Whole-operation deadline, including queueing, connection setup, and a response.
    pub operation_timeout: Duration,
    /// Idle duration after which an unused cached connection is retired.
    pub idle_timeout: Duration,
}

/// Host routing, protected credential lookup, and custody unlock for an exact writer.
#[async_trait]
pub trait EngineConnector: Send + Sync {
    /// Independently verifies the locator and returns an authenticated, unlocked client.
    async fn connect(&self, runtime: &Runtime) -> Result<ChordClient>;
}

struct Connection {
    runtime: Runtime,
    client: AsyncMutex<Option<ChordClient>>,
    active: watch::Sender<bool>,
}

struct CachedConnection {
    last_used: Instant,
    connection: Arc<Connection>,
}

struct PreparingConnection {
    runtime: Runtime,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl Drop for PreparingConnection {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Shared protocol bridge with explicit reconciliation and cancellation-safe connection reuse.
pub struct EngineAdapter {
    store: Store,
    connector: Arc<dyn EngineConnector>,
    policy: EngineConnectionPolicy,
    connections: Mutex<HashMap<Uuid, CachedConnection>>,
    preparations: Mutex<HashMap<Uuid, PreparingConnection>>,
    accepting: Arc<AtomicBool>,
    calls: Arc<RwLock<()>>,
}

impl std::fmt::Debug for EngineAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineAdapter")
            .field("policy", &self.policy)
            .field("accepting", &self.accepting.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Drop for EngineAdapter {
    fn drop(&mut self) {
        self.accepting.store(false, Ordering::Release);
        let connections = self
            .connections
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        for connection in connections.values() {
            connection.connection.active.send_replace(false);
        }
    }
}

impl EngineAdapter {
    /// Builds the shared adapter without supplying hidden connection or retry limits.
    pub fn new(
        store: Store,
        connector: Arc<dyn EngineConnector>,
        policy: EngineConnectionPolicy,
    ) -> Result<Self> {
        if policy.operation_timeout.is_zero() || policy.idle_timeout.is_zero() {
            return Err(Error::InvalidCommand(
                "connection durations must be positive".into(),
            ));
        }
        Ok(Self {
            store,
            connector,
            policy,
            connections: Mutex::new(HashMap::new()),
            preparations: Mutex::new(HashMap::new()),
            accepting: Arc::new(AtomicBool::new(true)),
            calls: Arc::new(RwLock::new(())),
        })
    }

    /// Retires unused idle sockets; an in-flight RPC keeps its original connection alive.
    pub fn reap_idle(&self) -> Result<()> {
        self.connections
            .lock()
            .map_err(|_| pool_unavailable())?
            .retain(|_, cached| {
                cached.last_used.elapsed() < self.policy.idle_timeout
                    || Arc::strong_count(&cached.connection) > 1
            });
        Ok(())
    }

    /// Reads one authoritative goal through the same fenced connection used for delivery.
    pub async fn goal(&self, runtime: &Runtime, goal_id: &str) -> Result<GoalSnapshot> {
        let goal_id = goal_id.to_owned();
        self.call(runtime, |client| {
            Box::pin(async move { client.snapshot(&goal_id).await })
        })
        .await
        .map_err(Error::from)
    }

    /// Lists immutable publications using the current fenced owner connection.
    pub async fn goal_artifacts(
        &self,
        runtime: &Runtime,
        goal_id: &str,
    ) -> Result<Vec<PublishedArtifact>> {
        let goal_id = goal_id.to_owned();
        self.call(runtime, |client| {
            Box::pin(async move { client.goal_artifacts(&goal_id).await })
        })
        .await
        .map_err(Error::from)
    }

    /// Reads and verifies every page of the exact original publication before returning bytes.
    pub async fn goal_artifact(
        &self,
        runtime: &Runtime,
        goal_id: &str,
        artifact_id: Uuid,
        max_file_bytes: NonZeroUsize,
    ) -> Result<ArtifactRead> {
        let goal_id = goal_id.to_owned();
        let artifact = self
            .call(runtime, |client| {
                Box::pin(async move {
                    let Some(metadata) = client
                        .goal_artifacts(&goal_id)
                        .await?
                        .into_iter()
                        .find(|entry| entry.artifact_id == artifact_id)
                    else {
                        return Ok(ArtifactRead::Missing);
                    };
                    let Ok(size) = usize::try_from(metadata.size_bytes) else {
                        return Ok(ArtifactRead::TooLarge);
                    };
                    if size > max_file_bytes.get() {
                        return Ok(ArtifactRead::TooLarge);
                    }
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve_exact(size)
                        .map_err(|_| ProtocolError::MessageTooLarge)?;
                    let mut offset = 0;
                    let mut digest = Sha256::new();
                    loop {
                        let page = client
                            .goal_artifact_page(&goal_id, artifact_id, offset)
                            .await?
                            .ok_or(ProtocolError::InvalidState)?;
                        let remaining = size - bytes.len();
                        if page.data.len()
                            > base64::encoded_len(remaining, false).unwrap_or(usize::MAX)
                        {
                            return Err(ProtocolError::InvalidState);
                        }
                        let decoded = URL_SAFE_NO_PAD
                            .decode(&page.data)
                            .map_err(|_| ProtocolError::InvalidState)?;
                        let end = offset
                            .checked_add(decoded.len() as u64)
                            .ok_or(ProtocolError::InvalidState)?;
                        if end > metadata.size_bytes
                            || page.next_offset != (end < metadata.size_bytes).then_some(end)
                            || (decoded.is_empty() && metadata.size_bytes != 0)
                        {
                            return Err(ProtocolError::InvalidState);
                        }
                        digest.update(&decoded);
                        bytes.extend_from_slice(&decoded);
                        let Some(next) = page.next_offset else { break };
                        offset = next;
                    }
                    let digest = digest.finalize();
                    let mut sha256 = String::with_capacity(digest.len() * 2);
                    for byte in digest.as_slice() {
                        let _ = write!(sha256, "{byte:02x}");
                    }
                    if sha256 != metadata.sha256 {
                        return Err(ProtocolError::InvalidState);
                    }
                    Ok(ArtifactRead::Complete(ArtifactDownload {
                        metadata,
                        bytes,
                        digest: digest.into(),
                    }))
                })
            })
            .await
            .map_err(Error::from)?;
        self.store.verify_generation(runtime).await?;
        Ok(artifact)
    }

    /// Reads exactly one source page; hosts preserve the cursor and supply their configured page size.
    pub async fn goals(
        &self,
        runtime: &Runtime,
        limit: std::num::NonZeroU64,
        cursor: Option<Value>,
    ) -> Result<GoalPage> {
        self.call(runtime, |client| {
            Box::pin(async move { client.goals(limit.get(), cursor).await })
        })
        .await
        .map_err(Error::from)
    }

    /// Reads the producer's bounded current conversation window and releases its observation.
    pub async fn conversation(
        &self,
        runtime: &Runtime,
        conversation_id: u64,
    ) -> Result<ConversationState> {
        self.call(runtime, |client| {
            Box::pin(async move {
                let state = client
                    .state_snapshot(
                        "chopin.conversation",
                        serde_json::json!({"conversation":conversation_id}),
                    )
                    .await?;
                Ok(serde_json::from_value(state)?)
            })
        })
        .await
        .map_err(Error::from)
    }

    /// Preserves existing conversations and their actual sidebar metadata.
    pub async fn summaries(&self, runtime: &Runtime) -> Result<Vec<ConversationSummary>> {
        self.call(runtime, |client| Box::pin(client.summaries()))
            .await
            .map_err(Error::from)
    }

    /// Resolves the exact execution-owned conversation binding; only an explicit absent goal returns `None`.
    pub async fn goal_for_conversation(
        &self,
        runtime: &Runtime,
        conversation_id: u64,
    ) -> Result<Option<GoalSnapshot>> {
        self.call(runtime, |client| {
            Box::pin(async move { client.goal_for_conversation(conversation_id).await })
        })
        .await
        .map_err(Error::from)
    }

    /// Reads available models and the service's actual default selection.
    pub async fn models(&self, runtime: &Runtime) -> Result<ModelChoices> {
        self.call(runtime, |client| Box::pin(client.models()))
            .await
            .map_err(Error::from)
    }

    /// Reads one authoritative personal model settings snapshot from the current writer.
    pub async fn model_settings(&self, runtime: &Runtime) -> Result<ModelSettingsSnapshot> {
        let snapshot = self
            .call(runtime, |client| {
                Box::pin(async move {
                    let value = client
                        .invoke(
                            "chopin.credentials",
                            serde_json::json!({}),
                            "snapshot",
                            None,
                        )
                        .await?;
                    serde_json::from_value(value).map_err(ProtocolError::from)
                })
            })
            .await
            .map_err(model_settings_error)?;
        self.store.verify_generation(runtime).await?;
        Ok(snapshot)
    }

    /// Stages a single private claim and waits for the current writer's durable confirmation.
    ///
    /// Dropping or timing out the call discards an unclaimed change. A claimed change's result
    /// remains uncertain until an explicit confirmation arrives.
    pub async fn apply_personal_credential_change(
        &self,
        runtime: &Runtime,
        claims: &PersonalCredentialClaims,
        change: PersonalCredentialChange,
    ) -> Result<()> {
        let claim = claims.stage(runtime.clone(), self.policy.operation_timeout, change)?;
        let operation_id = claim.id;
        let result = self
            .call(runtime, move |client| {
                Box::pin(async move {
                    let confirmation = client
                        .invoke(
                            "chopin.credentials",
                            serde_json::json!({}),
                            "applyPersonalCredentialChange",
                            Some(serde_json::json!({"operationId": operation_id})),
                        )
                        .await?;
                    if confirmation.is_null() {
                        Ok(())
                    } else {
                        Err(ProtocolError::InvalidFrame)
                    }
                })
            })
            .await
            .map_err(model_settings_error);
        drop(claim);
        result
    }

    /// Reads the execution owner's public subscription status, if the producer supports it.
    pub async fn subscription_snapshot(&self, runtime: &Runtime) -> Result<Option<Value>> {
        match self
            .subscription_rpc(runtime, "subscriptionSnapshot", None)
            .await
        {
            Ok(value) => {
                self.store.verify_generation(runtime).await?;
                Ok(Some(value))
            }
            Err(CallError::Protocol(ProtocolError::UnknownMember)) => Ok(None),
            Err(error) if plugin_code(&error) == Some("credential_refused") => Ok(None),
            Err(error) => Err(subscription_error(error)),
        }
    }

    /// Starts, cancels, or disconnects an owner subscription without sending credentials.
    pub async fn subscription_change(
        &self,
        runtime: &Runtime,
        method: SubscriptionChange,
    ) -> Result<()> {
        let starts_attempt = matches!(&method, SubscriptionChange::Start { .. });
        let (member, args) = match method {
            SubscriptionChange::Start {
                provider,
                switch_account,
            } => (
                "startSubscriptionSignIn",
                serde_json::json!({"provider": provider, "mode": if switch_account { "switch_account" } else { "connect" }}),
            ),
            SubscriptionChange::Cancel { attempt } => (
                "cancelSubscriptionSignIn",
                serde_json::json!({"attemptId": attempt}),
            ),
            SubscriptionChange::Disconnect {
                provider,
                expected_account_id,
            } => (
                "disconnectSubscription",
                serde_json::json!({"provider": provider, "expectedAccountId": expected_account_id}),
            ),
        };
        let response = self
            .subscription_rpc(runtime, member, Some(args))
            .await
            .map_err(subscription_error)?;
        let acknowledged = if starts_attempt {
            response["attemptId"]
                .as_str()
                .is_some_and(|id| Uuid::parse_str(id).is_ok())
        } else {
            response.is_null()
        };
        if !acknowledged {
            return Err(Error::Uncertain(
                "the subscription operation was not acknowledged".into(),
            ));
        }
        self.store.verify_generation(runtime).await
    }

    /// Accepts a challenge only from the exact authenticated writer over the private callback.
    pub async fn subscription_challenge(
        &self,
        runtime: &Runtime,
        claims: &PersonalCredentialClaims,
        attempt: Uuid,
    ) -> Result<SubscriptionChallenge> {
        let guard =
            claims
                .challenges
                .reserve(runtime.clone(), attempt, self.policy.operation_timeout)?;
        let response = self
            .subscription_rpc(
                runtime,
                "deliverSubscriptionChallenge",
                Some(serde_json::json!({"attemptId": attempt, "operationId": guard.id})),
            )
            .await
            .map_err(subscription_error)?;
        if !response.is_null() {
            return Err(Error::Uncertain(
                "the subscription challenge delivery was not acknowledged".into(),
            ));
        }
        self.store.verify_generation(runtime).await?;
        guard.take()
    }

    async fn subscription_rpc(
        &self,
        runtime: &Runtime,
        member: &'static str,
        args: Option<Value>,
    ) -> std::result::Result<Value, CallError> {
        self.call(runtime, move |client| {
            Box::pin(async move {
                client
                    .invoke("chopin.subscriptions", serde_json::json!({}), member, args)
                    .await
            })
        })
        .await
    }

    /// Reads an authoritative history page without collecting or truncating the remaining history.
    pub async fn entries(
        &self,
        runtime: &Runtime,
        conversation_id: u64,
        limit: std::num::NonZeroU64,
        cursor: Option<Value>,
        at_or_before: Option<u64>,
    ) -> Result<TranscriptPage> {
        self.call(runtime, |client| {
            Box::pin(async move {
                client
                    .entries(conversation_id, limit.get(), cursor, at_or_before)
                    .await
            })
        })
        .await
        .map_err(Error::from)
    }

    /// Reads the execution-owned improvement summary without changing its budget or scheduler.
    pub async fn loop_standing(&self, runtime: &Runtime) -> Result<LoopStanding> {
        self.call(runtime, |client| Box::pin(client.loop_standing()))
            .await
            .map_err(Error::from)
    }

    /// Reads evaluated changes from the execution authority.
    pub async fn loop_changes(&self, runtime: &Runtime) -> Result<Vec<LoopChange>> {
        self.call(runtime, |client| Box::pin(client.loop_changes()))
            .await
            .map_err(Error::from)
    }

    /// Reads recurring corrections requiring an owner decision.
    pub async fn loop_corrections(&self, runtime: &Runtime) -> Result<Vec<CorrectionKind>> {
        self.call(runtime, |client| Box::pin(client.loop_corrections()))
            .await
            .map_err(Error::from)
    }

    /// Reads held evaluation candidates without approving or publishing them.
    pub async fn loop_held(&self, runtime: &Runtime) -> Result<Vec<HeldChange>> {
        self.call(runtime, |client| Box::pin(client.loop_held()))
            .await
            .map_err(Error::from)
    }

    /// Reads actual improvement settings from the execution service.
    pub async fn loop_settings(&self, runtime: &Runtime) -> Result<LoopSettings> {
        self.call(runtime, |client| Box::pin(client.loop_settings()))
            .await
            .map_err(Error::from)
    }

    /// Removes one exact writer's cached connection without affecting its replacement.
    pub async fn retire(&self, runtime: &Runtime) -> Result<()> {
        let task = {
            let mut preparations = self.preparations.lock().map_err(|_| pool_unavailable())?;
            if preparations
                .get(&runtime.agent_id)
                .is_some_and(|preparing| preparing.runtime == *runtime)
            {
                preparations
                    .remove(&runtime.agent_id)
                    .and_then(|mut preparing| preparing.task.take())
            } else {
                None
            }
        };
        if let Some(task) = task {
            task.abort();
            let _cancelled = task.await;
        }
        let connection = {
            let mut connections = self.connections.lock().map_err(|_| pool_unavailable())?;
            if connections
                .get(&runtime.agent_id)
                .is_some_and(|cached| cached.connection.runtime == *runtime)
            {
                connections
                    .remove(&runtime.agent_id)
                    .map(|cached| cached.connection)
            } else {
                None
            }
        };
        if let Some(connection) = connection {
            connection.active.send_replace(false);
            let close = async {
                connection.client.lock().await.take();
            };
            tokio::time::timeout(self.policy.operation_timeout, close)
                .await
                .map_err(|_| Error::Uncertain("engine connection is still draining".into()))?;
        }
        Ok(())
    }

    /// Stops new calls, drains admitted operations, and drops all remaining sockets.
    pub async fn shutdown(&self) -> Result<()> {
        self.accepting.store(false, Ordering::Release);
        let preparations = {
            let mut preparations = self.preparations.lock().map_err(|_| pool_unavailable())?;
            preparations
                .drain()
                .map(|(_, mut preparing)| preparing.task.take())
                .collect::<Vec<_>>()
        };
        for task in preparations.into_iter().flatten() {
            task.abort();
            let _cancelled = task.await;
        }
        let _drained = self.calls.write().await;
        let mut connections = self.connections.lock().map_err(|_| pool_unavailable())?;
        for connection in connections.values() {
            connection.connection.active.send_replace(false);
        }
        connections.clear();
        Ok(())
    }

    fn connection(&self, runtime: &Runtime) -> Result<Arc<Connection>> {
        self.reap_idle()?;
        let mut connections = self.connections.lock().map_err(|_| pool_unavailable())?;
        let cached = connections
            .entry(runtime.agent_id)
            .or_insert_with(|| CachedConnection {
                last_used: Instant::now(),
                connection: Arc::new(Connection {
                    runtime: runtime.clone(),
                    client: AsyncMutex::new(None),
                    active: watch::channel(true).0,
                }),
            });
        if cached.connection.runtime != *runtime {
            cached.connection.active.send_replace(false);
            cached.connection = Arc::new(Connection {
                runtime: runtime.clone(),
                client: AsyncMutex::new(None),
                active: watch::channel(true).0,
            });
        }
        cached.last_used = Instant::now();
        Ok(cached.connection.clone())
    }

    /// Opens the execution-owned goal state with producer deltas merged by the shared protocol client.
    pub async fn observe_goal(
        &self,
        runtime: &Runtime,
        goal_id: &str,
    ) -> Result<EngineObservation> {
        self.observe(runtime, |client| async move {
            client.observe_goal(goal_id).await
        })
        .await
    }

    /// Opens the bounded live conversation state; older entries use the paged history method.
    pub async fn observe_conversation(
        &self,
        runtime: &Runtime,
        conversation_id: u64,
    ) -> Result<EngineObservation> {
        self.observe(runtime, |client| {
            client.observe_conversation(conversation_id)
        })
        .await
    }

    async fn observe<F>(
        &self,
        runtime: &Runtime,
        operation: impl FnOnce(ChordClient) -> F,
    ) -> Result<EngineObservation>
    where
        F: Future<Output = chopin_protocol::Result<StateObserver>>,
    {
        let open = async {
            let _admitted = self.calls.read().await;
            if !self.accepting.load(Ordering::Acquire) {
                return Err(Error::Uncertain("engine adapter is draining".into()));
            }
            self.store.verify_execution(runtime).await?;
            let connection = self.connection(runtime)?;
            let client = self.connector.connect(runtime).await?;
            self.store.verify_execution(runtime).await?;
            let observer = operation(client)
                .await
                .map_err(|_| Error::Uncertain("engine observation could not be opened".into()))?;
            let observation = EngineObservation::new(
                self.store.clone(),
                connection,
                observer,
                self.policy.operation_timeout,
            );
            observation.verify().await?;
            Ok(observation)
        };
        tokio::time::timeout(self.policy.operation_timeout, open)
            .await
            .map_err(|_| Error::Uncertain("engine observation open deadline exceeded".into()))?
    }

    async fn call<T: Send>(
        &self,
        runtime: &Runtime,
        operation: impl for<'a> FnOnce(&'a mut ChordClient) -> RpcFuture<'a, T> + Send,
    ) -> std::result::Result<T, CallError> {
        let call = async {
            let _admitted = self.calls.read().await;
            if !self.accepting.load(Ordering::Acquire) {
                return Err(CallError::Kernel(Error::Uncertain(
                    "engine adapter is draining".into(),
                )));
            }
            self.store.verify_generation(runtime).await?;
            let connection = self.connection(runtime)?;
            let mut cached = connection.client.lock().await;
            self.store.verify_generation(runtime).await?;
            // Moving the client out makes cancellation discard any partially consumed wire state.
            let mut client = match cached.take() {
                Some(client) => client,
                None => self.connector.connect(runtime).await?,
            };
            self.store.verify_generation(runtime).await?;
            let response = operation(&mut client).await?;
            *cached = Some(client);
            Ok(response)
        };
        tokio::time::timeout(self.policy.operation_timeout, call)
            .await
            .map_err(|_| {
                CallError::Kernel(Error::Uncertain(
                    "engine operation deadline exceeded".into(),
                ))
            })?
    }
}

type RpcFuture<'a, T> =
    Pin<Box<dyn Future<Output = std::result::Result<T, ProtocolError>> + Send + 'a>>;

enum CallError {
    Kernel(Error),
    Protocol(ProtocolError),
}

impl From<Error> for CallError {
    fn from(error: Error) -> Self {
        Self::Kernel(error)
    }
}

impl From<ProtocolError> for CallError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<CallError> for Error {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Kernel(error) => error,
            CallError::Protocol(_) => {
                Self::Uncertain("engine response could not be established".into())
            }
        }
    }
}

fn model_settings_error(error: CallError) -> Error {
    match plugin_code(&error) {
        Some("forbidden") => Error::Forbidden,
        Some("credential_refused") => {
            Error::InvalidCommand("model credential operation was refused".into())
        }
        _ => error.into(),
    }
}

fn subscription_error(error: CallError) -> Error {
    match plugin_code(&error) {
        Some("subscription_account_mismatch") => Error::RequestConflict,
        _ => model_settings_error(error),
    }
}

fn plugin_code(error: &CallError) -> Option<&str> {
    match error {
        CallError::Protocol(ProtocolError::Method { error: problem, .. })
            if problem["kind"] == "plugin" =>
        {
            problem["code"].as_str()
        }
        _ => None,
    }
}

fn pool_unavailable() -> Error {
    Error::Uncertain("engine connection coordination is unavailable".into())
}

fn command(value: &Value) -> Result<GoalCommand> {
    let command: GoalCommand = serde_json::from_value(value.clone())?;
    command
        .validate()
        .map_err(|_| Error::InvalidCommand("execution command violates its contract".into()))?;
    Ok(command)
}

fn authorize_command(command: &GoalCommand, authorization: &CommandAuthorization) -> Result<()> {
    match (authorization, command) {
        (
            CommandAuthorization::Owner,
            GoalCommand::Input { grant: None, .. } | GoalCommand::Stop { .. },
        ) => Ok(()),
        (
            CommandAuthorization::Grant { id, version },
            GoalCommand::Submit { grant, .. }
            | GoalCommand::Input {
                grant: Some(grant), ..
            },
        ) if *version > 0 && grant.version == *version as u64 && grant.id == id.to_string() => {
            Ok(())
        }
        _ => Err(Error::Forbidden),
    }
}

#[async_trait]
impl EngineBridge for EngineAdapter {
    async fn prepare(&self, runtime: &Runtime) -> Result<EnginePreparation> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(Error::Uncertain("engine adapter is draining".into()));
        }
        if self.store.instance_phase(runtime).await?
            != (InstancePurpose::Execution, InstancePhase::Unlocking)
        {
            return Err(Error::RuntimeNotReady);
        }
        let completed = {
            let mut preparations = self.preparations.lock().map_err(|_| pool_unavailable())?;
            if !self.accepting.load(Ordering::Acquire) {
                return Err(Error::Uncertain("engine adapter is draining".into()));
            }
            if preparations
                .get(&runtime.agent_id)
                .is_some_and(|preparing| preparing.runtime != *runtime)
            {
                preparations.remove(&runtime.agent_id);
            }
            if let Some(preparing) = preparations.get(&runtime.agent_id) {
                if preparing
                    .task
                    .as_ref()
                    .is_some_and(|task| task.is_finished())
                {
                    preparations
                        .remove(&runtime.agent_id)
                        .and_then(|mut preparing| preparing.task.take())
                } else {
                    None
                }
            } else {
                let connection = self.connection(runtime)?;
                let task = tokio::spawn(prepare_connection(
                    self.store.clone(),
                    self.connector.clone(),
                    connection,
                    self.policy.operation_timeout,
                    self.accepting.clone(),
                    self.calls.clone(),
                ));
                preparations.insert(
                    runtime.agent_id,
                    PreparingConnection {
                        runtime: runtime.clone(),
                        task: Some(task),
                    },
                );
                None
            }
        };
        let Some(completed) = completed else {
            return Ok(EnginePreparation::Pending);
        };
        completed
            .await
            .map_err(|_| Error::Uncertain("engine preparation was interrupted".into()))??;
        self.store.verify_writer(runtime).await?;
        Ok(EnginePreparation::Ready)
    }

    fn requirements(&self, value: &Value) -> Result<CommandRequirements> {
        let authority = match command(value)? {
            GoalCommand::Submit { .. } | GoalCommand::Input { grant: Some(_), .. } => {
                RequiredAuthority::TaskGrant
            }
            GoalCommand::Input { grant: None, .. } | GoalCommand::Stop { .. } => {
                RequiredAuthority::OwnerControl
            }
        };
        Ok(CommandRequirements {
            authority,
            scopes: vec![],
        })
    }

    fn authorize_command(
        &self,
        value: &Value,
        authorization: &CommandAuthorization,
    ) -> Result<CommandRequirements> {
        authorize_command(&command(value)?, authorization)?;
        self.requirements(value)
    }

    async fn request(&self, runtime: &Runtime, request_id: Uuid) -> Result<Option<Value>> {
        let expected_id = request_id.to_string();
        let request_id = expected_id.clone();
        let receipt = self
            .call(runtime, |client| {
                Box::pin(async move { client.request(&request_id).await })
            })
            .await
            .map_err(Error::from)?;
        if receipt
            .as_ref()
            .is_some_and(|receipt| receipt.request_id != expected_id)
        {
            return Err(Error::Uncertain(
                "engine receipt names a different request".into(),
            ));
        }
        receipt
            .map(serde_json::to_value)
            .transpose()
            .map_err(Error::from)
    }

    async fn execute(&self, dispatch: &Dispatch) -> Result<ExecutionResponse> {
        let command = command(&dispatch.command)?;
        authorize_command(&command, &dispatch.authorization)?;
        let envelope = EngineCommand::new(dispatch.request_id.to_string(), command);
        let response = self
            .call(&dispatch.runtime, |client| {
                Box::pin(async move { client.execute(&envelope).await })
            })
            .await;
        let receipt = match response {
            Ok(receipt) => receipt,
            Err(CallError::Protocol(error)) => {
                if let Some(rejection) = error.command_rejection() {
                    return Ok(ExecutionResponse::Rejected(ApiProblem {
                        code: rejection.as_str().into(),
                        message: "Execution service rejected this command".into(),
                    }));
                }
                return Err(Error::from(CallError::Protocol(error)));
            }
            Err(error) => return Err(Error::from(error)),
        };
        if receipt.request_id != dispatch.request_id.to_string() {
            return Err(Error::Uncertain(
                "engine receipt names a different request".into(),
            ));
        }
        Ok(ExecutionResponse::Accepted(serde_json::to_value(receipt)?))
    }

    async fn wake(&self, runtime: &Runtime) -> Result<Wake> {
        let wake = self
            .call(runtime, |client| Box::pin(client.wake()))
            .await
            .map_err(Error::from)?;
        let at = wake
            .at
            .map(|at| {
                if !at.is_finite() || at.fract() != 0.0 || at.abs() > ((1u64 << 53) - 1) as f64 {
                    return Err(Error::Uncertain(
                        "engine wake timestamp is not an exact integer".into(),
                    ));
                }
                Timestamp::from_millisecond(at as i64)
                    .map_err(|_| Error::Uncertain("engine wake timestamp is out of range".into()))
            })
            .transpose()?;
        Ok(Wake {
            active: wake.active,
            at,
        })
    }

    async fn resume(&self, runtime: &Runtime) -> Result<()> {
        self.call(runtime, |client| Box::pin(client.resume()))
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn snapshot(&self, runtime: &Runtime, goal_id: &str) -> Result<Value> {
        let snapshot = self.goal(runtime, goal_id).await?;
        Ok(serde_json::to_value(snapshot)?)
    }
}

async fn prepare_connection(
    store: Store,
    connector: Arc<dyn EngineConnector>,
    connection: Arc<Connection>,
    timeout: Duration,
    accepting: Arc<AtomicBool>,
    calls: Arc<RwLock<()>>,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        let _admitted = calls.read().await;
        if !accepting.load(Ordering::Acquire) {
            return Err(Error::Uncertain("engine adapter is draining".into()));
        }
        store.verify_writer(&connection.runtime).await?;
        let mut cached = connection.client.lock().await;
        let client = match cached.take() {
            Some(client) => client,
            None => connector.connect(&connection.runtime).await?,
        };
        store.verify_writer(&connection.runtime).await?;
        *cached = Some(client);
        Ok(())
    })
    .await
    .map_err(|_| Error::Uncertain("engine preparation deadline exceeded".into()))?
}
