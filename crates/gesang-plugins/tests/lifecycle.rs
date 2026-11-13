//! Behavioral coverage for scoped service dependencies, replacement, and resource shutdown.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use gesang_api::CapabilityScope;
use gesang_plugins::{
    Authority, AuthorityError, FailureStage, Graph, GraphError, PackageVersion, PinError, PinStore,
    ResourceScope, ServiceContext, ServiceHandle, ServiceKey, ServiceSpec, ServiceState, Started,
    VersionPin,
};
use tokio::sync::Notify;
use uuid::Uuid;

type PinKey = (Uuid, Uuid, String);

#[derive(Debug, Default)]
struct MemoryPins(Mutex<BTreeMap<PinKey, VersionPin>>);

#[async_trait]
impl PinStore for MemoryPins {
    async fn pin(&self, pin: VersionPin) -> Result<VersionPin, PinError> {
        let mut pins = self.0.lock().unwrap();
        let key = (pin.owner_id, pin.reference_id, pin.package.clone());
        match pins.get(&key) {
            Some(existing) if existing != &pin => Err(PinError::Conflict {
                existing: Box::new(existing.clone()),
                requested: Box::new(pin),
            }),
            Some(existing) => Ok(existing.clone()),
            None => {
                pins.insert(key, pin.clone());
                Ok(pin)
            }
        }
    }

    async fn get(
        &self,
        owner: Uuid,
        reference: Uuid,
        package: &str,
    ) -> Result<Option<VersionPin>, PinError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(&(owner, reference, package.to_owned()))
            .cloned())
    }

    async fn list(&self, owner: Uuid, reference: Uuid) -> Result<Vec<VersionPin>, PinError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .values()
            .filter(|pin| pin.owner_id == owner && pin.reference_id == reference)
            .cloned()
            .collect())
    }

    async fn unpin(&self, owner: Uuid, reference: Uuid, package: &str) -> Result<(), PinError> {
        self.0
            .lock()
            .unwrap()
            .remove(&(owner, reference, package.to_owned()));
        Ok(())
    }
}

fn scope() -> ResourceScope {
    ResourceScope::Owner {
        owner_id: Uuid::new_v4(),
    }
}

fn authority(scope: &ResourceScope) -> Authority {
    Authority::issue(scope.clone(), [])
}

fn spec<T: Send + Sync + 'static>(key: &ServiceKey<T>) -> ServiceSpec {
    ServiceSpec {
        package: PackageVersion {
            package: key.id().name.clone(),
            version: "1".into(),
            digest: "digest-1".into(),
        },
        pin_reference: Uuid::new_v4(),
        authority: authority(&key.id().scope),
        dependencies: vec![],
    }
}

fn constant(value: u32) -> impl gesang_plugins::Factory<u32> {
    move |_context: ServiceContext| async move { Ok(Started::new(value, || async { Ok(()) })) }
}

async fn panicking_factory(_context: ServiceContext) -> Result<Started<u32>, String> {
    panic!("factory panic")
}

async fn panicking_cleanup() -> Result<(), String> {
    panic!("cleanup panic")
}

#[test]
fn delegation_cannot_add_capabilities_change_owner_or_lengthen_its_lifetime() {
    let owner = Uuid::new_v4();
    let instance = Uuid::new_v4();
    let parent = ResourceScope::Owner { owner_id: owner };
    let child = ResourceScope::Instance {
        owner_id: owner,
        instance_id: instance,
    };
    let activation = ResourceScope::Activation {
        owner_id: owner,
        instance_id: instance,
        activation_id: Uuid::new_v4(),
    };
    let read = CapabilityScope {
        capability: "files.read".into(),
        resource: "workspace".into(),
    };
    let write = CapabilityScope {
        capability: "files.write".into(),
        resource: "workspace".into(),
    };
    let root = Authority::issue(parent.clone(), [read.clone()]);
    let delegated = root.narrow(child.clone(), [read.clone()]).unwrap();
    assert_eq!(
        delegated.narrow(parent, []),
        Err(AuthorityError::ScopeExpansion)
    );
    assert_eq!(
        delegated.narrow(child.clone(), [write]),
        Err(AuthorityError::CapabilityExpansion)
    );
    assert_eq!(
        delegated.narrow(scope(), []),
        Err(AuthorityError::ScopeExpansion)
    );
    assert!(delegated.narrow(activation.clone(), [read]).is_ok());
    assert!(!child.contains(&ResourceScope::Instance {
        owner_id: owner,
        instance_id: Uuid::new_v4()
    }));
    assert!(!activation.contains(&child));
}

#[tokio::test]
async fn missing_dependencies_and_cycles_fail_before_any_factory_runs() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let first = ServiceKey::<u32>::new("first", owner.clone());
    let second = ServiceKey::<u32>::new("second", owner);
    let mut first_spec = spec(&first);
    first_spec.dependencies = vec![second.dependency()];
    graph
        .register(first.clone(), first_spec, constant(1))
        .await
        .unwrap();
    assert_eq!(
        graph.activate().await.unwrap_err(),
        GraphError::MissingDependency {
            service: first.id().clone(),
            dependency: Box::new(second.id().clone()),
        }
    );
    let mut second_spec = spec(&second);
    second_spec.dependencies = vec![first.dependency()];
    graph
        .register(second.clone(), second_spec, constant(2))
        .await
        .unwrap();
    let GraphError::Cycle(path) = graph.validate().unwrap_err() else {
        panic!("expected cycle diagnostic")
    };
    assert_eq!(path.first(), path.last());
    assert!(path.contains(first.id()) && path.contains(second.id()));
    assert!(
        graph
            .snapshots()
            .unwrap()
            .iter()
            .all(|snapshot| snapshot.generation == 0)
    );
}

#[tokio::test]
async fn dependencies_cannot_cross_owners_or_capture_shorter_lived_resources() {
    let owner = scope();
    let instance = ResourceScope::Instance {
        owner_id: owner.owner_id(),
        instance_id: Uuid::new_v4(),
    };
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let provider = ServiceKey::<u32>::new("provider", instance);
    let consumer = ServiceKey::<u32>::new("consumer", owner);
    graph
        .register(provider.clone(), spec(&provider), constant(1))
        .await
        .unwrap();
    let mut consumer_spec = spec(&consumer);
    consumer_spec.dependencies.push(provider.dependency());
    graph
        .register(consumer.clone(), consumer_spec, constant(2))
        .await
        .unwrap();
    assert_eq!(
        graph.validate().unwrap_err(),
        GraphError::DependencyScope {
            service: consumer.id().clone(),
            dependency: Box::new(provider.id().clone()),
        }
    );
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let foreign = ServiceKey::<u32>::new("foreign", scope());
    graph
        .register(foreign.clone(), spec(&foreign), constant(1))
        .await
        .unwrap();
    let mut definition = spec(&consumer);
    definition.dependencies.push(foreign.dependency());
    graph
        .register(consumer.clone(), definition, constant(2))
        .await
        .unwrap();
    assert_eq!(
        graph.validate().unwrap_err(),
        GraphError::DependencyScope {
            service: consumer.id().clone(),
            dependency: Box::new(foreign.id().clone()),
        }
    );
}

#[tokio::test]
async fn factories_can_resolve_only_declared_types_and_keep_consumer_authority() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let provider = ServiceKey::<u32>::new("provider", owner.clone());
    let wrong = ServiceKey::<String>::new("provider", owner.clone());
    graph
        .register(provider.clone(), spec(&provider), constant(7))
        .await
        .unwrap();
    let consumer = ServiceKey::<u32>::new("consumer", owner.clone());
    let provider_copy = provider.clone();
    let wrong_copy = wrong.clone();
    graph
        .register(
            consumer.clone(),
            spec(&consumer),
            move |context: ServiceContext| {
                let provider = provider_copy.clone();
                let wrong = wrong_copy.clone();
                async move {
                    assert!(matches!(
                        context.service(&provider),
                        Err(GraphError::UndeclaredDependency(_))
                    ));
                    assert!(matches!(
                        context.service(&wrong),
                        Err(GraphError::UndeclaredDependency(_))
                    ));
                    Ok(Started::new(3, || async { Ok(()) }))
                }
            },
        )
        .await
        .unwrap();
    assert!(graph.activate().await.unwrap().is_success());
    assert!(matches!(
        graph.service(&wrong, &authority(&owner)),
        Err(GraphError::TypeMismatch(_))
    ));
    assert!(matches!(
        graph.service(&provider, &authority(&scope())),
        Err(GraphError::Authority(_))
    ));
    let read = CapabilityScope {
        capability: "files.read".into(),
        resource: "a".into(),
    };
    let client = Authority::issue(owner.clone(), [read.clone()]);
    let handle = graph.service(&provider, &client).unwrap();
    assert_eq!(
        handle
            .call(|value, granted| Box::pin(async move { (*value, granted.capabilities().len()) }))
            .await
            .unwrap(),
        (7, 1)
    );
    assert!(
        handle
            .narrow(
                owner,
                [CapabilityScope {
                    capability: "files.write".into(),
                    resource: "a".into()
                }]
            )
            .is_err()
    );
}

#[derive(Debug)]
struct Consumer(ServiceHandle<u32>);

#[tokio::test]
async fn replacing_a_provider_revokes_old_handles_drains_calls_and_rebuilds_consumers() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let provider = ServiceKey::<u32>::new("provider", owner.clone());
    let consumer = ServiceKey::<Consumer>::new("consumer", owner.clone());
    let events_start = events.clone();
    graph
        .register(
            provider.clone(),
            spec(&provider),
            move |_context: ServiceContext| {
                let events = events_start.clone();
                async move {
                    Ok(Started::new(1, move || {
                        let events = events.clone();
                        async move {
                            events.lock().unwrap().push("provider".into());
                            Ok(())
                        }
                    }))
                }
            },
        )
        .await
        .unwrap();
    let provider_copy = provider.clone();
    let mut consumer_spec = spec(&consumer);
    consumer_spec.dependencies.push(provider.dependency());
    let events_start = events.clone();
    graph
        .register(
            consumer.clone(),
            consumer_spec,
            move |context: ServiceContext| {
                let provider = provider_copy.clone();
                let events = events_start.clone();
                async move {
                    let handle = context
                        .service(&provider)
                        .map_err(|error| error.to_string())?;
                    Ok(Started::new(Consumer(handle), move || {
                        let events = events.clone();
                        async move {
                            events.lock().unwrap().push("consumer".into());
                            Ok(())
                        }
                    }))
                }
            },
        )
        .await
        .unwrap();
    assert_eq!(
        graph.activate().await.unwrap().started,
        vec![provider.id().clone(), consumer.id().clone()]
    );
    assert_eq!(
        graph.dependents(provider.id()).unwrap(),
        vec![consumer.id().clone()]
    );
    let old = graph.service(&provider, &authority(&owner)).unwrap();
    let old_consumer = graph.service(&consumer, &authority(&owner)).unwrap();
    let entered = Arc::new(Notify::new());
    let leave = Arc::new(Notify::new());
    let call = tokio::spawn({
        let handle = old.clone();
        let entered = entered.clone();
        let leave = leave.clone();
        async move {
            handle
                .call(move |value, _| {
                    Box::pin(async move {
                        entered.notify_one();
                        leave.notified().await;
                        *value
                    })
                })
                .await
        }
    });
    entered.notified().await;
    let replacing = tokio::spawn({
        let graph = graph.clone();
        let provider = provider.clone();
        async move {
            graph
                .replace(provider.clone(), spec(&provider), constant(2))
                .await
        }
    });
    loop {
        if graph.snapshots().unwrap().iter().any(|snapshot| {
            snapshot.service == *provider.id() && snapshot.state == ServiceState::Stopping
        }) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        old.call(|value, _| Box::pin(async move { *value })).await,
        Err(GraphError::StaleHandle(_))
    ));
    assert!(matches!(
        old_consumer.call(|_, _| Box::pin(async {})).await,
        Err(GraphError::StaleHandle(_))
    ));
    assert!(
        !events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "provider")
    );
    leave.notify_one();
    assert_eq!(call.await.unwrap().unwrap(), 1);
    let report = replacing.await.unwrap().unwrap();
    assert!(report.is_success());
    assert_eq!(*events.lock().unwrap(), ["consumer", "provider"]);
    let fresh = graph.service(&consumer, &authority(&owner)).unwrap();
    assert!(fresh.generation() > old_consumer.generation());
    let observed = fresh
        .call(|value, _| {
            Box::pin(async move {
                value
                    .0
                    .call(|number, _| Box::pin(async move { *number }))
                    .await
                    .unwrap()
            })
        })
        .await
        .unwrap();
    assert_eq!(observed, 2);
}

#[tokio::test]
async fn a_failed_cleanup_keeps_dependencies_alive_but_refuses_new_calls_until_retry() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let provider = ServiceKey::<u32>::new("provider", owner.clone());
    let consumer = ServiceKey::<u32>::new("consumer", owner.clone());
    let events_start = events.clone();
    graph
        .register(provider.clone(), spec(&provider), move |_| {
            let events = events_start.clone();
            async move {
                Ok(Started::new(1, move || {
                    let events = events.clone();
                    async move {
                        events.lock().unwrap().push("provider".into());
                        Ok(())
                    }
                }))
            }
        })
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_start = attempts.clone();
    let events_start = events.clone();
    let mut definition = spec(&consumer);
    definition.dependencies.push(provider.dependency());
    graph
        .register(consumer.clone(), definition, move |_| {
            let attempts = attempts_start.clone();
            let events = events_start.clone();
            async move {
                Ok(Started::new(2, move || {
                    let attempts = attempts.clone();
                    let events = events.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            return Err("cleanup refused".into());
                        }
                        events.lock().unwrap().push("consumer".into());
                        Ok(())
                    }
                }))
            }
        })
        .await
        .unwrap();
    graph.activate().await.unwrap();
    let old = graph.service(&provider, &authority(&owner)).unwrap();
    let report = graph.release(owner.clone()).await.unwrap();
    assert_eq!(report.failures.len(), 2);
    assert!(events.lock().unwrap().is_empty());
    assert!(
        graph
            .snapshots()
            .unwrap()
            .iter()
            .all(|snapshot| snapshot.has_resources
                && matches!(
                    snapshot.state,
                    ServiceState::Failed {
                        stage: FailureStage::Shutdown,
                        ..
                    }
                ))
    );
    assert!(matches!(
        old.call(|number, _| Box::pin(async move { *number })).await,
        Err(GraphError::StaleHandle(_))
    ));
    assert!(graph.release(owner).await.unwrap().is_success());
    assert_eq!(*events.lock().unwrap(), ["consumer", "provider"]);
}

#[tokio::test]
async fn startup_failure_is_queryable_and_never_starts_its_consumers() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let provider = ServiceKey::<u32>::new("provider", owner.clone());
    let consumer = ServiceKey::<u32>::new("consumer", owner);
    let failing = Arc::new(AtomicBool::new(true));
    let failing_start = failing.clone();
    graph
        .register(provider.clone(), spec(&provider), move |_| {
            let failing = failing_start.clone();
            async move {
                if failing.load(Ordering::SeqCst) {
                    Err("provider not available".into())
                } else {
                    Ok(Started::new(1, || async { Ok(()) }))
                }
            }
        })
        .await
        .unwrap();
    let invoked = Arc::new(AtomicUsize::new(0));
    let invoked_start = invoked.clone();
    let mut definition = spec(&consumer);
    definition.dependencies.push(provider.dependency());
    graph
        .register(consumer.clone(), definition, move |_| {
            let invoked = invoked_start.clone();
            async move {
                invoked.fetch_add(1, Ordering::SeqCst);
                Ok(Started::new(2, || async { Ok(()) }))
            }
        })
        .await
        .unwrap();
    let report = graph.activate().await.unwrap();
    assert_eq!(report.failures.len(), 2);
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert!(
        graph
            .snapshots()
            .unwrap()
            .iter()
            .any(|snapshot| snapshot.service == *provider.id()
                && matches!(
                    snapshot.state,
                    ServiceState::Failed {
                        stage: FailureStage::Start,
                        ..
                    }
                ))
    );
    failing.store(false, Ordering::SeqCst);
    assert!(graph.activate().await.unwrap().is_success());
    assert_eq!(invoked.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn durable_references_outlive_activation_release_and_conflicts_precede_startup() {
    let owner = scope();
    let pins = Arc::new(MemoryPins::default());
    let graph = Graph::new(pins.clone());
    let key = ServiceKey::<u32>::new("provider", owner.clone());
    let definition = spec(&key);
    let reference = definition.pin_reference;
    graph
        .register(key.clone(), definition.clone(), constant(1))
        .await
        .unwrap();
    graph.activate().await.unwrap();
    graph.release(owner.clone()).await.unwrap();
    assert_eq!(
        pins.list(owner.owner_id(), reference).await.unwrap().len(),
        1
    );
    let restored_graph = Graph::new(pins.clone());
    let invoked = Arc::new(AtomicUsize::new(0));
    let invoked_start = invoked.clone();
    let mut conflicting = definition;
    conflicting.package.version = "2".into();
    restored_graph
        .register(key.clone(), conflicting, move |_| {
            let invoked = invoked_start.clone();
            async move {
                invoked.fetch_add(1, Ordering::SeqCst);
                Ok(Started::new(2, || async { Ok(()) }))
            }
        })
        .await
        .unwrap();
    let report = restored_graph.activate().await.unwrap();
    assert_eq!(report.failures[0].stage, FailureStage::Pin);
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    pins.unpin(owner.owner_id(), reference, "provider")
        .await
        .unwrap();
    assert!(restored_graph.activate().await.unwrap().is_success());
}

#[tokio::test]
async fn releasing_one_activation_does_not_release_owner_instance_or_sibling_resources() {
    let owner = scope();
    let instance = ResourceScope::Instance {
        owner_id: owner.owner_id(),
        instance_id: Uuid::new_v4(),
    };
    let ResourceScope::Instance { instance_id, .. } = instance else {
        unreachable!()
    };
    let activation = ResourceScope::Activation {
        owner_id: owner.owner_id(),
        instance_id,
        activation_id: Uuid::new_v4(),
    };
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let owner_key = ServiceKey::<u32>::new("owner", owner.clone());
    let instance_key = ServiceKey::<u32>::new("instance", instance.clone());
    let activation_key = ServiceKey::<u32>::new("activation", activation.clone());
    let sibling_key = ServiceKey::<u32>::new("sibling", scope());
    graph
        .register(owner_key.clone(), spec(&owner_key), constant(1))
        .await
        .unwrap();
    let mut definition = spec(&instance_key);
    definition.dependencies.push(owner_key.dependency());
    graph
        .register(instance_key.clone(), definition, constant(2))
        .await
        .unwrap();
    let mut definition = spec(&activation_key);
    definition.dependencies.push(instance_key.dependency());
    graph
        .register(activation_key.clone(), definition, constant(3))
        .await
        .unwrap();
    graph
        .register(sibling_key.clone(), spec(&sibling_key), constant(4))
        .await
        .unwrap();
    graph.activate().await.unwrap();
    assert_eq!(
        graph.release(activation).await.unwrap().stopped,
        vec![activation_key.id().clone()]
    );
    for key in [&owner_key, &instance_key, &sibling_key] {
        assert!(graph.service(key, &authority(&key.id().scope)).is_ok());
    }
    assert!(matches!(
        graph.service(&activation_key, &authority(&activation_key.id().scope)),
        Err(GraphError::Unavailable(_))
    ));
}

#[tokio::test]
async fn conflicting_replacement_keeps_the_ready_generation_and_records_the_refusal() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let key = ServiceKey::<u32>::new("provider", owner.clone());
    let original = spec(&key);
    let closed = Arc::new(AtomicBool::new(false));
    let closed_start = closed.clone();
    graph
        .register(key.clone(), original.clone(), move |_| {
            let closed = closed_start.clone();
            async move {
                Ok(Started::new(1, move || {
                    let closed = closed.clone();
                    async move {
                        closed.store(true, Ordering::SeqCst);
                        Ok(())
                    }
                }))
            }
        })
        .await
        .unwrap();
    graph.activate().await.unwrap();
    let mut replacement = original;
    replacement.package.version = "2".into();
    let report = graph
        .replace(key.clone(), replacement, constant(2))
        .await
        .unwrap();
    assert_eq!(report.failures[0].stage, FailureStage::Pin);
    assert!(!closed.load(Ordering::SeqCst));
    let handle = graph.service(&key, &authority(&owner)).unwrap();
    assert_eq!(
        handle
            .call(|value, _| Box::pin(async move { *value }))
            .await
            .unwrap(),
        1
    );
    let snapshot = graph.snapshots().unwrap().pop().unwrap();
    assert_eq!(snapshot.state, ServiceState::Ready);
    assert_eq!(snapshot.last_failure.unwrap().stage, FailureStage::Pin);
}

#[tokio::test]
async fn cancelling_a_transition_caller_does_not_drop_partially_started_resources() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let key = ServiceKey::<u32>::new("provider", owner.clone());
    let entered = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let closed = Arc::new(AtomicBool::new(false));
    graph
        .register(key.clone(), spec(&key), {
            let entered = entered.clone();
            let resume = resume.clone();
            let closed = closed.clone();
            move |_| {
                let entered = entered.clone();
                let resume = resume.clone();
                let closed = closed.clone();
                async move {
                    entered.notify_one();
                    resume.notified().await;
                    Ok(Started::new(1, move || {
                        let closed = closed.clone();
                        async move {
                            closed.store(true, Ordering::SeqCst);
                            Ok(())
                        }
                    }))
                }
            }
        })
        .await
        .unwrap();
    let caller = tokio::spawn({
        let graph = graph.clone();
        async move { graph.activate().await }
    });
    entered.notified().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    resume.notify_one();
    let released = graph.release(owner).await.unwrap();
    assert_eq!(released.stopped, vec![key.id().clone()]);
    assert!(closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn factory_and_cleanup_panics_become_queryable_failures() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let failed = ServiceKey::<u32>::new("failed", owner.clone());
    graph
        .register(failed.clone(), spec(&failed), panicking_factory)
        .await
        .unwrap();
    let cleanup = ServiceKey::<u32>::new("cleanup", owner.clone());
    graph
        .register(cleanup.clone(), spec(&cleanup), |_| async {
            Ok(Started::new(1, panicking_cleanup))
        })
        .await
        .unwrap();
    let report = graph.activate().await.unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].stage, FailureStage::Start);
    let report = graph.release(owner).await.unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].stage, FailureStage::Shutdown);
    assert!(
        graph
            .snapshots()
            .unwrap()
            .iter()
            .any(|snapshot| snapshot.service == *cleanup.id()
                && snapshot.has_resources
                && matches!(
                    snapshot.state,
                    ServiceState::Failed {
                        stage: FailureStage::Shutdown,
                        ..
                    }
                ))
    );
}

#[tokio::test]
async fn an_incomplete_inactive_registration_cannot_block_existing_resource_release() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let existing = ServiceKey::<u32>::new("existing", owner.clone());
    graph
        .register(existing.clone(), spec(&existing), constant(1))
        .await
        .unwrap();
    graph.activate().await.unwrap();
    let incomplete = ServiceKey::<u32>::new("incomplete", owner.clone());
    let missing = ServiceKey::<u32>::new("missing", owner.clone());
    let mut definition = spec(&incomplete);
    definition.dependencies.push(missing.dependency());
    graph
        .register(incomplete.clone(), definition, constant(2))
        .await
        .unwrap();
    assert!(matches!(
        graph.validate(),
        Err(GraphError::MissingDependency { .. })
    ));
    assert_eq!(
        graph.release(owner).await.unwrap().stopped,
        vec![existing.id().clone()]
    );
}
