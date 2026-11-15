//! A trusted service graph with revocable generations and dependency-ordered cleanup.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::{
    Authority, AuthorityError, PackageVersion, PinStore, PinSubject, ServiceScope, VersionPin,
};

/// A service's stable name and resource lifetime within a graph.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceId {
    /// Name assigned by the trusted composition root.
    pub name: String,
    /// Ownership and lifetime of the service's resources.
    pub scope: ServiceScope,
}

/// A service identity carrying its Rust interface type.
pub struct ServiceKey<T> {
    id: ServiceId,
    marker: PhantomData<fn() -> T>,
}

impl<T> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for ServiceKey<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ServiceKey").field(&self.id).finish()
    }
}

impl<T: Send + Sync + 'static> ServiceKey<T> {
    /// Declares a typed identity; registration diagnoses name/type collisions.
    #[must_use]
    pub fn new(name: impl Into<String>, scope: ServiceScope) -> Self {
        Self {
            id: ServiceId {
                name: name.into(),
                scope,
            },
            marker: PhantomData,
        }
    }

    /// Returns the stable identity without erasing the key's type.
    #[must_use]
    pub fn id(&self) -> &ServiceId {
        &self.id
    }

    /// Declares a required dependency with the same type as this key.
    #[must_use]
    pub fn dependency(&self) -> Dependency {
        Dependency {
            id: self.id.clone(),
            service_type: TypeId::of::<T>(),
        }
    }
}

/// A declared typed dependency, resolved only after the provider has started.
#[derive(Clone, Debug)]
pub struct Dependency {
    id: ServiceId,
    service_type: TypeId,
}

impl Dependency {
    /// Returns the exact provider identity for validation against an installed manifest.
    #[must_use]
    pub fn id(&self) -> &ServiceId {
        &self.id
    }
}

/// The installer-approved package and dependencies used by one service activation.
#[derive(Clone, Debug)]
pub struct ServiceSpec {
    /// Immutable package content selected by the trusted installer.
    pub package: PackageVersion,
    /// Persistent consumer reference; survives resource shutdown.
    pub pin_reference: Uuid,
    /// All services the factory may resolve.
    pub dependencies: Vec<Dependency>,
}

/// The stage at which a service failed; absence of a status is never readiness.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureStage {
    /// A required provider is unavailable.
    Dependency,
    /// The persistent version reference was not accepted.
    Pin,
    /// The service factory failed or panicked.
    Start,
    /// Cleanup failed, panicked, or is blocked by a consumer's failed cleanup.
    Shutdown,
}

/// Observable lifecycle state; failed cleanup retains its revoked resource for an explicit retry.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServiceState {
    /// Registered but never started.
    Registered,
    /// The pin was accepted and the factory is running.
    Starting,
    /// This generation accepts new calls.
    Ready,
    /// New calls are refused while accepted calls drain and cleanup runs.
    Stopping,
    /// Resources have been explicitly closed.
    Stopped,
    /// The failure remains queryable until a successful transition replaces it.
    Failed {
        /// Transition stage that did not complete.
        stage: FailureStage,
        /// Detail from the failed transition.
        message: String,
    },
}

/// A service's state without its value or credentials.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceSnapshot {
    /// Stable service identity.
    pub service: ServiceId,
    /// Package selected for the next or current activation.
    pub package: PackageVersion,
    /// Most recently attempted generation, monotonically increasing within this graph.
    pub generation: u64,
    /// Whether resources still require cleanup, including failed cleanup.
    pub has_resources: bool,
    /// Current observable lifecycle state.
    pub state: ServiceState,
    /// Most recent transition failure, including a refused replacement of a ready service.
    pub last_failure: Option<ServiceFailure>,
}

/// A failure attached to its service and lifecycle stage.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceFailure {
    /// Service that could not complete the requested transition.
    pub service: ServiceId,
    /// Transition stage that failed.
    pub stage: FailureStage,
    /// Failure detail from the service or platform store.
    pub message: String,
}

/// The complete outcome of a graph transition, including independent failures.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChangeReport {
    /// Successfully started generations, in dependency order.
    pub started: Vec<ServiceId>,
    /// Successfully closed resources, in consumer-first order.
    pub stopped: Vec<ServiceId>,
    /// All services that could not complete the transition.
    pub failures: Vec<ServiceFailure>,
}

impl ChangeReport {
    /// Whether every selected service completed the requested transition.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// A graph definition, authority, lifecycle, or typed lookup was invalid.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum GraphError {
    /// The same identity was registered twice.
    #[error("service is already registered: {0:?}")]
    Duplicate(ServiceId),
    /// The requested service has not been registered.
    #[error("unknown service: {0:?}")]
    Unknown(ServiceId),
    /// A stable identity was used with incompatible Rust types.
    #[error("service type does not match its key: {0:?}")]
    TypeMismatch(ServiceId),
    /// A declared provider is absent.
    #[error("service {service:?} requires missing dependency {dependency:?}")]
    MissingDependency {
        /// Consumer requiring the absent provider.
        service: ServiceId,
        /// Provider that has not been registered.
        dependency: Box<ServiceId>,
    },
    /// The path closes a dependency cycle, with the repeated first member at its end.
    #[error("service dependency cycle: {0:?}")]
    Cycle(Vec<ServiceId>),
    /// A provider's lifetime is shorter than its consumer's, or belongs to another owner.
    #[error("dependency {dependency:?} does not contain consumer scope {service:?}")]
    DependencyScope {
        /// Consumer whose lifetime or owner is incompatible.
        service: ServiceId,
        /// Provider whose scope does not contain the consumer.
        dependency: Box<ServiceId>,
    },
    /// A factory attempted to resolve an undeclared dependency.
    #[error("factory did not declare dependency: {0:?}")]
    UndeclaredDependency(ServiceId),
    /// The requested service has no ready generation.
    #[error("service is not ready: {0:?}")]
    Unavailable(ServiceId),
    /// The handle's generation no longer accepts new calls.
    #[error("service handle has been revoked: {0:?}")]
    StaleHandle(ServiceId),
    /// A trusted composition supplied inconsistent scope or empty identity fields.
    #[error("invalid service definition: {0}")]
    InvalidDefinition(String),
    /// The requested handle or delegated authority exceeds its parent.
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    /// An internal lock was poisoned by a panic.
    #[error("service graph state lock is poisoned")]
    Poisoned,
    /// A lifecycle coordination task could not finish.
    #[error("service graph coordinator failed: {0}")]
    Coordinator(String),
}

/// A boxed service call borrowing its service and non-expanding authority.
pub type CallFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Explicit asynchronous cleanup, retried only after a caller requests another transition.
#[async_trait]
pub trait Shutdown: Send + Sync + 'static {
    /// Releases resources; must tolerate retry after a previous unknown or failed outcome.
    async fn close(&self) -> Result<(), String>;
}

#[async_trait]
impl<F, Fut> Shutdown for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send,
{
    async fn close(&self) -> Result<(), String> {
        self().await
    }
}

/// A successfully started service and the cleanup that owns its resources.
pub struct Started<T> {
    value: Arc<T>,
    shutdown: Arc<dyn Shutdown>,
}

impl<T> std::fmt::Debug for Started<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Started").finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> Started<T> {
    /// Transfers a value and its cleanup to the graph.
    #[must_use]
    pub fn new(value: T, shutdown: impl Shutdown) -> Self {
        Self {
            value: Arc::new(value),
            shutdown: Arc::new(shutdown),
        }
    }
}

/// A trusted factory; partial startup failures must clean up their own unreturned resources.
#[async_trait]
pub trait Factory<T>: Send + Sync + 'static {
    /// Acquires resources after every declared dependency and the durable pin are ready.
    async fn start(&self, context: ServiceContext) -> Result<Started<T>, String>;
}

#[async_trait]
impl<T, F, Fut> Factory<T> for F
where
    T: Send + Sync + 'static,
    F: Fn(ServiceContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Started<T>, String>> + Send,
{
    async fn start(&self, context: ServiceContext) -> Result<Started<T>, String> {
        self(context).await
    }
}

struct ErasedStarted {
    value: Arc<dyn Any + Send + Sync>,
    shutdown: Arc<dyn Shutdown>,
}

#[async_trait]
trait ErasedFactory: Send + Sync {
    async fn start(&self, context: ServiceContext) -> Result<ErasedStarted, String>;
}

struct TypedFactory<T, F>(F, PhantomData<fn() -> T>);

#[async_trait]
impl<T: Send + Sync + 'static, F: Factory<T>> ErasedFactory for TypedFactory<T, F> {
    async fn start(&self, context: ServiceContext) -> Result<ErasedStarted, String> {
        let started = self.0.start(context).await?;
        Ok(ErasedStarted {
            value: started.value,
            shutdown: started.shutdown,
        })
    }
}

struct GateState {
    accepting: bool,
    calls: usize,
}

struct CallGate {
    state: Mutex<GateState>,
    drained: Notify,
}

impl CallGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                accepting: true,
                calls: 0,
            }),
            drained: Notify::new(),
        }
    }

    fn enter(self: &Arc<Self>, id: &ServiceId) -> Result<CallPermit, GraphError> {
        let mut state = self.state.lock().map_err(|_| GraphError::Poisoned)?;
        if !state.accepting {
            return Err(GraphError::StaleHandle(id.clone()));
        }
        state.calls = state
            .calls
            .checked_add(1)
            .ok_or_else(|| GraphError::Coordinator("active call counter overflow".into()))?;
        Ok(CallPermit(self.clone()))
    }

    fn revoke(&self) -> Result<(), GraphError> {
        self.state
            .lock()
            .map_err(|_| GraphError::Poisoned)?
            .accepting = false;
        Ok(())
    }

    async fn wait_drained(&self) -> Result<(), GraphError> {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().map_err(|_| GraphError::Poisoned)?.calls == 0 {
                return Ok(());
            }
            notified.await;
        }
    }
}

struct CallPermit(Arc<CallGate>);

impl Drop for CallPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.calls -= 1;
            if state.calls == 0 {
                self.0.drained.notify_waiters();
            }
        }
    }
}

struct Live {
    value: Arc<dyn Any + Send + Sync>,
    shutdown: Arc<dyn Shutdown>,
    gate: Arc<CallGate>,
}

/// A typed generation reference; every new call checks revocation before borrowing its value.
pub struct ServiceHandle<T> {
    id: ServiceId,
    generation: u64,
    value: Arc<T>,
    live: Arc<Live>,
}

impl<T> Clone for ServiceHandle<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            generation: self.generation,
            value: self.value.clone(),
            live: self.live.clone(),
        }
    }
}

impl<T> std::fmt::Debug for ServiceHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceHandle")
            .field("service", &self.id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl<T: Send + Sync + 'static> ServiceHandle<T> {
    /// Executes one call with the caller's authority; accepted calls drain before cleanup.
    pub async fn call<R: Send>(
        &self,
        authority: &Authority,
        operation: impl for<'a> FnOnce(&'a T, &'a Authority) -> CallFuture<'a, R> + Send,
    ) -> Result<R, GraphError> {
        if !self.id.scope.accepts(authority) {
            return Err(AuthorityError::ScopeExpansion.into());
        }
        let _permit = self.live.gate.enter(&self.id)?;
        Ok(operation(&self.value, authority).await)
    }

    /// Returns this handle's immutable generation number.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Factory access to declared dependencies and its service's resource lifetime.
#[derive(Clone)]
pub struct ServiceContext {
    inner: Arc<Inner>,
    scope: ServiceScope,
    dependencies: BTreeMap<ServiceId, TypeId>,
}

impl std::fmt::Debug for ServiceContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceContext")
            .field("scope", &self.scope)
            .field("dependencies", &self.dependencies.keys())
            .finish()
    }
}

impl ServiceContext {
    /// Returns construction scope; it is not permission to access any user's data.
    #[must_use]
    pub fn scope(&self) -> &ServiceScope {
        &self.scope
    }

    /// Resolves a declared provider without capturing a caller; every call supplies authority.
    pub fn service<T: Send + Sync + 'static>(
        &self,
        key: &ServiceKey<T>,
    ) -> Result<ServiceHandle<T>, GraphError> {
        match self.dependencies.get(&key.id) {
            None => return Err(GraphError::UndeclaredDependency(key.id.clone())),
            Some(kind) if *kind != TypeId::of::<T>() => {
                return Err(GraphError::TypeMismatch(key.id.clone()));
            }
            Some(_) => {}
        }
        lookup(&self.inner, key)
    }
}

struct Node {
    service_type: TypeId,
    spec: ServiceSpec,
    factory: Arc<dyn ErasedFactory>,
    generation: u64,
    state: ServiceState,
    live: Option<Arc<Live>>,
    last_failure: Option<ServiceFailure>,
}

#[derive(Clone)]
struct Definition {
    service_type: TypeId,
    spec: ServiceSpec,
}

#[derive(Default)]
struct State {
    nodes: BTreeMap<ServiceId, Node>,
}

struct Inner {
    state: Mutex<State>,
    transition: tokio::sync::Mutex<()>,
    pins: Arc<dyn PinStore>,
}

impl Inner {
    fn state(&self) -> Result<MutexGuard<'_, State>, GraphError> {
        self.state.lock().map_err(|_| GraphError::Poisoned)
    }
}

/// Shared lifecycle coordination used by both embedded and standalone hosts.
///
/// Transitions are serialized and continue if their requesting future is dropped. Hosts must
/// await explicit scope release before stopping the runtime. Cleanup hooks release resources;
/// they cannot use revoked handles to submit new service work.
#[derive(Clone)]
pub struct Graph {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph").finish_non_exhaustive()
    }
}

impl Graph {
    /// Creates a resource graph using the platform's authoritative pin store.
    #[must_use]
    pub fn new(pins: Arc<dyn PinStore>) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::default(),
                transition: tokio::sync::Mutex::new(()),
                pins,
            }),
        }
    }

    /// Registers a factory without acquiring resources; forward dependencies are allowed.
    pub async fn register<T: Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        spec: ServiceSpec,
        factory: impl Factory<T>,
    ) -> Result<(), GraphError> {
        check_spec(&key.id, &spec)?;
        let _transition = self.inner.transition.lock().await;
        let mut state = self.inner.state()?;
        if let Some(existing) = state.nodes.get(&key.id) {
            return Err(if existing.service_type == TypeId::of::<T>() {
                GraphError::Duplicate(key.id)
            } else {
                GraphError::TypeMismatch(key.id)
            });
        }
        state.nodes.insert(
            key.id,
            Node {
                service_type: TypeId::of::<T>(),
                spec,
                factory: Arc::new(TypedFactory::<T, _>(factory, PhantomData)),
                generation: 0,
                state: ServiceState::Registered,
                live: None,
                last_failure: None,
            },
        );
        Ok(())
    }

    /// Validates every required type, dependency lifetime, missing provider and cycle.
    pub fn validate(&self) -> Result<Vec<ServiceId>, GraphError> {
        validate(&definitions(&*self.inner.state()?))
    }

    /// Reads lifecycle state without exposing service values or credentials.
    pub fn snapshots(&self) -> Result<Vec<ServiceSnapshot>, GraphError> {
        Ok(self
            .inner
            .state()?
            .nodes
            .iter()
            .map(|(id, node)| ServiceSnapshot {
                service: id.clone(),
                package: node.spec.package.clone(),
                generation: node.generation,
                has_resources: node.live.is_some(),
                state: node.state.clone(),
                last_failure: node.last_failure.clone(),
            })
            .collect())
    }

    /// Returns immediate consumers, derived from the same declared dependency graph.
    pub fn dependents(&self, key: &ServiceId) -> Result<Vec<ServiceId>, GraphError> {
        Ok(self
            .inner
            .state()?
            .nodes
            .iter()
            .filter(|(_, node)| {
                node.spec
                    .dependencies
                    .iter()
                    .any(|dependency| &dependency.id == key)
            })
            .map(|(id, _)| id.clone())
            .collect())
    }

    /// Obtains a typed generation; every invocation separately supplies host-issued authority.
    pub fn service<T: Send + Sync + 'static>(
        &self,
        key: &ServiceKey<T>,
    ) -> Result<ServiceHandle<T>, GraphError> {
        lookup(&self.inner, key)
    }

    /// Starts registered or stopped services in dependency order, reporting each failure.
    pub async fn activate(&self) -> Result<ChangeReport, GraphError> {
        let graph = self.clone();
        coordinated(async move {
            let _transition = graph.inner.transition.lock().await;
            let order = graph.validate()?;
            graph.start_order(&order).await
        })
        .await
    }

    /// Revokes selected generations, drains accepted calls and closes consumers first.
    pub async fn release(&self, scope: ServiceScope) -> Result<ChangeReport, GraphError> {
        let graph = self.clone();
        coordinated(async move {
            let _transition = graph.inner.transition.lock().await;
            let defs = active_definitions(&*graph.inner.state()?);
            let order = validate(&defs)?;
            let selected = defs
                .keys()
                .filter(|id| scope.contains(&id.scope))
                .cloned()
                .collect();
            graph
                .stop_order(&order, &dependent_closure(&defs, selected))
                .await
        })
        .await
    }

    /// Replaces a provider and restarts its transitive consumers with fresh generations.
    ///
    /// A failed cleanup blocks replacement and preserves the revoked resources for retry.
    /// Previous durable version pins remain until their consumers explicitly release them.
    pub async fn replace<T: Send + Sync + 'static>(
        &self,
        key: ServiceKey<T>,
        spec: ServiceSpec,
        factory: impl Factory<T>,
    ) -> Result<ChangeReport, GraphError> {
        check_spec(&key.id, &spec)?;
        let graph = self.clone();
        let factory: Arc<dyn ErasedFactory> = Arc::new(TypedFactory::<T, _>(factory, PhantomData));
        coordinated(async move {
            let _transition = graph.inner.transition.lock().await;
            let mut candidate = definitions(&*graph.inner.state()?);
            let existing = candidate
                .get_mut(&key.id)
                .ok_or_else(|| GraphError::Unknown(key.id.clone()))?;
            if existing.service_type != TypeId::of::<T>() {
                return Err(GraphError::TypeMismatch(key.id));
            }
            existing.spec = spec.clone();
            let next_order = validate(&candidate)?;
            if let Err(error) = graph.inner.pins.pin(version_pin(&key.id, &spec)).await {
                let failure = ServiceFailure {
                    service: key.id.clone(),
                    stage: FailureStage::Pin,
                    message: error.to_string(),
                };
                graph
                    .inner
                    .state()?
                    .nodes
                    .get_mut(&key.id)
                    .ok_or_else(|| GraphError::Unknown(key.id.clone()))?
                    .last_failure = Some(failure.clone());
                return Ok(ChangeReport {
                    failures: vec![failure],
                    ..ChangeReport::default()
                });
            }
            let current = definitions(&*graph.inner.state()?);
            let order = validate(&active_definitions(&*graph.inner.state()?))?;
            let selected = dependent_closure(&current, BTreeSet::from([key.id.clone()]));
            let mut report = graph.stop_order(&order, &selected).await?;
            if !report.is_success() {
                return Ok(report);
            }
            {
                let mut state = graph.inner.state()?;
                let node = state
                    .nodes
                    .get_mut(&key.id)
                    .ok_or_else(|| GraphError::Unknown(key.id.clone()))?;
                node.spec = spec;
                node.factory = factory;
                node.state = ServiceState::Stopped;
            }
            let needed = dependency_closure(&candidate, selected);
            let order = next_order
                .into_iter()
                .filter(|id| needed.contains(id))
                .collect::<Vec<_>>();
            let started = graph.start_order(&order).await?;
            report.started.extend(started.started);
            report.failures.extend(started.failures);
            Ok(report)
        })
        .await
    }

    async fn start_order(&self, order: &[ServiceId]) -> Result<ChangeReport, GraphError> {
        let mut report = ChangeReport::default();
        for id in order {
            let (spec, factory, blocked) = {
                let state = self.inner.state()?;
                let node = &state.nodes[id];
                if matches!(node.state, ServiceState::Ready) {
                    continue;
                }
                let blocked = if node.live.is_some() {
                    Some((
                        FailureStage::Shutdown,
                        "previous resources still require cleanup".to_owned(),
                    ))
                } else {
                    node.spec
                        .dependencies
                        .iter()
                        .find(|dep| !matches!(state.nodes[&dep.id].state, ServiceState::Ready))
                        .map(|dep| {
                            (
                                FailureStage::Dependency,
                                format!("dependency {} is not ready", dep.id.name),
                            )
                        })
                };
                (node.spec.clone(), node.factory.clone(), blocked)
            };
            if let Some((stage, message)) = blocked {
                self.fail(id, stage, message, &mut report)?;
                continue;
            }
            let pin = version_pin(id, &spec);
            if let Err(error) = self.inner.pins.pin(pin).await {
                self.fail(id, FailureStage::Pin, error.to_string(), &mut report)?;
                continue;
            }
            {
                let mut state = self.inner.state()?;
                let node = state
                    .nodes
                    .get_mut(id)
                    .ok_or_else(|| GraphError::Unknown(id.clone()))?;
                node.generation = node
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| GraphError::Coordinator("service generation overflow".into()))?;
                node.state = ServiceState::Starting;
            }
            let context = ServiceContext {
                inner: self.inner.clone(),
                scope: id.scope.clone(),
                dependencies: spec
                    .dependencies
                    .into_iter()
                    .map(|dep| (dep.id, dep.service_type))
                    .collect(),
            };
            let started = tokio::spawn(async move { factory.start(context).await }).await;
            match started {
                Ok(Ok(started)) => {
                    let mut state = self.inner.state()?;
                    let node = state
                        .nodes
                        .get_mut(id)
                        .ok_or_else(|| GraphError::Unknown(id.clone()))?;
                    node.live = Some(Arc::new(Live {
                        value: started.value,
                        shutdown: started.shutdown,
                        gate: Arc::new(CallGate::new()),
                    }));
                    node.state = ServiceState::Ready;
                    node.last_failure = None;
                    report.started.push(id.clone());
                }
                Ok(Err(error)) => self.fail(id, FailureStage::Start, error, &mut report)?,
                Err(error) => self.fail(id, FailureStage::Start, error.to_string(), &mut report)?,
            }
        }
        Ok(report)
    }

    async fn stop_order(
        &self,
        order: &[ServiceId],
        selected: &BTreeSet<ServiceId>,
    ) -> Result<ChangeReport, GraphError> {
        let mut report = ChangeReport::default();
        {
            let mut state = self.inner.state()?;
            for id in selected {
                let node = state
                    .nodes
                    .get_mut(id)
                    .ok_or_else(|| GraphError::Unknown(id.clone()))?;
                if let Some(live) = &node.live {
                    live.gate.revoke()?;
                    node.state = ServiceState::Stopping;
                }
            }
        }
        for id in order.iter().rev().filter(|id| selected.contains(*id)) {
            let (live, blocked) = {
                let state = self.inner.state()?;
                let live = state.nodes[id].live.clone();
                let blocked = state
                    .nodes
                    .iter()
                    .find(|(consumer, node)| {
                        selected.contains(*consumer)
                            && node.live.is_some()
                            && node.spec.dependencies.iter().any(|dep| &dep.id == id)
                    })
                    .map(|(consumer, _)| {
                        format!("consumer {} still requires cleanup", consumer.name)
                    });
                (live, blocked)
            };
            let Some(live) = live else {
                continue;
            };
            if let Some(message) = blocked {
                self.fail(id, FailureStage::Shutdown, message, &mut report)?;
                continue;
            }
            live.gate.wait_drained().await?;
            let closed = tokio::spawn(async move { live.shutdown.close().await }).await;
            match closed {
                Ok(Ok(())) => {
                    let mut state = self.inner.state()?;
                    let node = state
                        .nodes
                        .get_mut(id)
                        .ok_or_else(|| GraphError::Unknown(id.clone()))?;
                    node.live = None;
                    node.state = ServiceState::Stopped;
                    node.last_failure = None;
                    report.stopped.push(id.clone());
                }
                Ok(Err(error)) => self.fail(id, FailureStage::Shutdown, error, &mut report)?,
                Err(error) => {
                    self.fail(id, FailureStage::Shutdown, error.to_string(), &mut report)?
                }
            }
        }
        Ok(report)
    }

    fn fail(
        &self,
        id: &ServiceId,
        stage: FailureStage,
        message: String,
        report: &mut ChangeReport,
    ) -> Result<(), GraphError> {
        let failure = ServiceFailure {
            service: id.clone(),
            stage,
            message: message.clone(),
        };
        let mut state = self.inner.state()?;
        let node = state
            .nodes
            .get_mut(id)
            .ok_or_else(|| GraphError::Unknown(id.clone()))?;
        node.state = ServiceState::Failed { stage, message };
        node.last_failure = Some(failure.clone());
        report.failures.push(failure);
        Ok(())
    }
}

async fn coordinated(
    transition: impl Future<Output = Result<ChangeReport, GraphError>> + Send + 'static,
) -> Result<ChangeReport, GraphError> {
    tokio::spawn(transition)
        .await
        .map_err(|error| GraphError::Coordinator(error.to_string()))?
}

fn lookup<T: Send + Sync + 'static>(
    inner: &Inner,
    key: &ServiceKey<T>,
) -> Result<ServiceHandle<T>, GraphError> {
    let state = inner.state()?;
    let node = state
        .nodes
        .get(&key.id)
        .ok_or_else(|| GraphError::Unknown(key.id.clone()))?;
    if node.service_type != TypeId::of::<T>() {
        return Err(GraphError::TypeMismatch(key.id.clone()));
    }
    if !matches!(node.state, ServiceState::Ready) {
        return Err(GraphError::Unavailable(key.id.clone()));
    }
    let live = node
        .live
        .clone()
        .ok_or_else(|| GraphError::Unavailable(key.id.clone()))?;
    let value = live
        .value
        .clone()
        .downcast::<T>()
        .map_err(|_| GraphError::TypeMismatch(key.id.clone()))?;
    Ok(ServiceHandle {
        id: key.id.clone(),
        generation: node.generation,
        value,
        live,
    })
}

fn check_spec(id: &ServiceId, spec: &ServiceSpec) -> Result<(), GraphError> {
    if id.name.is_empty()
        || spec.package.package.is_empty()
        || spec.package.version.is_empty()
        || spec.package.digest.is_empty()
    {
        return Err(GraphError::InvalidDefinition(
            "service name, package, version and digest must be nonempty".into(),
        ));
    }
    let mut dependencies = BTreeSet::new();
    for dep in &spec.dependencies {
        if !dependencies.insert(&dep.id) {
            return Err(GraphError::InvalidDefinition(format!(
                "dependency {} is declared twice",
                dep.id.name
            )));
        }
    }
    Ok(())
}

fn definitions(state: &State) -> BTreeMap<ServiceId, Definition> {
    state
        .nodes
        .iter()
        .map(|(id, node)| {
            (
                id.clone(),
                Definition {
                    service_type: node.service_type,
                    spec: node.spec.clone(),
                },
            )
        })
        .collect()
}

fn active_definitions(state: &State) -> BTreeMap<ServiceId, Definition> {
    let mut definitions = definitions(state);
    definitions.retain(|id, _| state.nodes[id].live.is_some());
    definitions
}

fn version_pin(id: &ServiceId, spec: &ServiceSpec) -> VersionPin {
    VersionPin {
        subject: id.scope.owner_scope().map_or(
            PinSubject::Installation {
                binding: id.scope.binding(),
            },
            |scope| PinSubject::Owner {
                owner_id: scope.owner_id(),
            },
        ),
        reference_id: spec.pin_reference,
        package: spec.package.package.clone(),
        version: spec.package.version.clone(),
        digest: spec.package.digest.clone(),
    }
}

fn validate(defs: &BTreeMap<ServiceId, Definition>) -> Result<Vec<ServiceId>, GraphError> {
    for (id, definition) in defs {
        for dep in &definition.spec.dependencies {
            let provider = defs
                .get(&dep.id)
                .ok_or_else(|| GraphError::MissingDependency {
                    service: id.clone(),
                    dependency: Box::new(dep.id.clone()),
                })?;
            if provider.service_type != dep.service_type {
                return Err(GraphError::TypeMismatch(dep.id.clone()));
            }
            if !dep.id.scope.contains(&id.scope) {
                return Err(GraphError::DependencyScope {
                    service: id.clone(),
                    dependency: Box::new(dep.id.clone()),
                });
            }
        }
    }
    fn visit(
        id: &ServiceId,
        defs: &BTreeMap<ServiceId, Definition>,
        path: &mut Vec<ServiceId>,
        complete: &mut BTreeSet<ServiceId>,
        order: &mut Vec<ServiceId>,
    ) -> Result<(), GraphError> {
        if complete.contains(id) {
            return Ok(());
        }
        if let Some(start) = path.iter().position(|member| member == id) {
            let mut cycle = path[start..].to_vec();
            cycle.push(id.clone());
            return Err(GraphError::Cycle(cycle));
        }
        path.push(id.clone());
        for dep in &defs[id].spec.dependencies {
            visit(&dep.id, defs, path, complete, order)?;
        }
        path.pop();
        complete.insert(id.clone());
        order.push(id.clone());
        Ok(())
    }
    let mut order = Vec::new();
    let mut complete = BTreeSet::new();
    for id in defs.keys() {
        visit(id, defs, &mut Vec::new(), &mut complete, &mut order)?;
    }
    Ok(order)
}

fn dependent_closure(
    defs: &BTreeMap<ServiceId, Definition>,
    mut selected: BTreeSet<ServiceId>,
) -> BTreeSet<ServiceId> {
    loop {
        let added = defs
            .iter()
            .filter(|(id, def)| {
                !selected.contains(*id)
                    && def
                        .spec
                        .dependencies
                        .iter()
                        .any(|dep| selected.contains(&dep.id))
            })
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if added.is_empty() {
            return selected;
        }
        selected.extend(added);
    }
}

fn dependency_closure(
    defs: &BTreeMap<ServiceId, Definition>,
    mut selected: BTreeSet<ServiceId>,
) -> BTreeSet<ServiceId> {
    loop {
        let added = selected
            .iter()
            .flat_map(|id| defs[id].spec.dependencies.iter().map(|dep| dep.id.clone()))
            .filter(|id| !selected.contains(id))
            .collect::<Vec<_>>();
        if added.is_empty() {
            return selected;
        }
        selected.extend(added);
    }
}
