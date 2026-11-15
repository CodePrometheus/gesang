//! Behavioral coverage for scoped service dependencies, replacement, and resource shutdown.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use gesang_api::CapabilityScope;
use gesang_plugins::{
    Authority, AuthorityError, BindingScope, FailureStage, Graph, GraphError, PackageVersion,
    PinError, PinStore, PinSubject, ResourceScope, ServiceContext, ServiceHandle, ServiceKey,
    ServiceScope, ServiceSpec, ServiceState, Started, VersionPin,
};
use tokio::sync::Notify;
use uuid::Uuid;

type PinKey = (PinSubject, Uuid, String);

#[derive(Debug, Default)]
struct MemoryPins(Mutex<BTreeMap<PinKey, VersionPin>>);

#[async_trait]
impl PinStore for MemoryPins {
    async fn pin(&self, pin: VersionPin) -> Result<VersionPin, PinError> {
        let mut pins = self.0.lock().unwrap();
        let key = (pin.subject.clone(), pin.reference_id, pin.package.clone());
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
        subject: &PinSubject,
        reference: Uuid,
        package: &str,
    ) -> Result<Option<VersionPin>, PinError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(&(subject.clone(), reference, package.to_owned()))
            .cloned())
    }

    async fn list(
        &self,
        subject: &PinSubject,
        reference: Uuid,
    ) -> Result<Vec<VersionPin>, PinError> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .values()
            .filter(|pin| &pin.subject == subject && pin.reference_id == reference)
            .cloned()
            .collect())
    }

    async fn unpin(
        &self,
        subject: &PinSubject,
        reference: Uuid,
        package: &str,
    ) -> Result<(), PinError> {
        self.0
            .lock()
            .unwrap()
            .remove(&(subject.clone(), reference, package.to_owned()));
        Ok(())
    }
}

fn binding() -> BindingScope {
    BindingScope::Deployment {
        deployment_id: Uuid::from_u128(1),
    }
}

fn service_scope(scope: &ResourceScope) -> ServiceScope {
    ServiceScope::owned(binding(), scope.clone())
}

fn key<T: Send + Sync + 'static>(name: &str, scope: ResourceScope) -> ServiceKey<T> {
    ServiceKey::new(name, service_scope(&scope))
}

fn scope() -> ResourceScope {
    ResourceScope::Owner {
        owner_id: Uuid::new_v4(),
    }
}

fn authority(scope: &ResourceScope) -> Authority {
    Authority::owner(binding(), scope.clone(), [])
}

fn spec<T: Send + Sync + 'static>(key: &ServiceKey<T>) -> ServiceSpec {
    ServiceSpec {
        package: PackageVersion {
            package: key.id().name.clone(),
            version: "1".into(),
            digest: "digest-1".into(),
        },
        pin_reference: Uuid::new_v4(),
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
fn delegation_bounds() {
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
    let root = Authority::owner(binding(), parent.clone(), [read.clone()]);
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
async fn dependency_validation() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let first = key::<u32>("first", owner.clone());
    let second = key::<u32>("second", owner);
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
async fn dependency_scopes() {
    let owner = scope();
    let instance = ResourceScope::Instance {
        owner_id: owner.owner_id(),
        instance_id: Uuid::new_v4(),
    };
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let provider = key::<u32>("provider", instance);
    let consumer = key::<u32>("consumer", owner);
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
    let foreign = key::<u32>("foreign", scope());
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
async fn declared_dependencies() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let provider = key::<u32>("provider", owner.clone());
    let wrong = key::<String>("provider", owner.clone());
    graph
        .register(provider.clone(), spec(&provider), constant(7))
        .await
        .unwrap();
    let consumer = key::<u32>("consumer", owner.clone());
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
        graph.service(&wrong),
        Err(GraphError::TypeMismatch(_))
    ));
    assert!(matches!(
        graph
            .service(&provider)
            .unwrap()
            .call(&authority(&scope()), |_, _| Box::pin(async {}))
            .await,
        Err(GraphError::Authority(_))
    ));
    let read = CapabilityScope {
        capability: "files.read".into(),
        resource: "a".into(),
    };
    let client = Authority::owner(binding(), owner.clone(), [read.clone()]);
    let handle = graph.service(&provider).unwrap();
    assert_eq!(
        handle
            .call(&client, |value, granted| Box::pin(async move {
                (*value, granted.capabilities().len())
            }))
            .await
            .unwrap(),
        (7, 1)
    );
    assert!(
        client
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
async fn provider_replacement() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let provider = key::<u32>("provider", owner.clone());
    let consumer = key::<Consumer>("consumer", owner.clone());
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
    let old = graph.service(&provider).unwrap();
    let old_consumer = graph.service(&consumer).unwrap();
    let entered = Arc::new(Notify::new());
    let leave = Arc::new(Notify::new());
    let call = tokio::spawn({
        let handle = old.clone();
        let client = authority(&owner);
        let entered = entered.clone();
        let leave = leave.clone();
        async move {
            handle
                .call(&client, move |value, _| {
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
        old.call(&authority(&owner), |value, _| Box::pin(
            async move { *value }
        ))
        .await,
        Err(GraphError::StaleHandle(_))
    ));
    assert!(matches!(
        old_consumer
            .call(&authority(&owner), |_, _| Box::pin(async {}))
            .await,
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
    let fresh = graph.service(&consumer).unwrap();
    assert!(fresh.generation() > old_consumer.generation());
    let observed = fresh
        .call(&authority(&owner), |value, authority| {
            Box::pin(async move {
                value
                    .0
                    .call(authority, |number, _| Box::pin(async move { *number }))
                    .await
                    .unwrap()
            })
        })
        .await
        .unwrap();
    assert_eq!(observed, 2);
}

#[tokio::test]
async fn cleanup_recovery() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let events = Arc::new(Mutex::new(Vec::<String>::new()));
    let provider = key::<u32>("provider", owner.clone());
    let consumer = key::<u32>("consumer", owner.clone());
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
    let old = graph.service(&provider).unwrap();
    let report = graph.release(service_scope(&owner)).await.unwrap();
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
        old.call(&authority(&owner), |number, _| Box::pin(
            async move { *number }
        ))
        .await,
        Err(GraphError::StaleHandle(_))
    ));
    assert!(
        graph
            .release(service_scope(&owner))
            .await
            .unwrap()
            .is_success()
    );
    assert_eq!(*events.lock().unwrap(), ["consumer", "provider"]);
}

#[tokio::test]
async fn startup_failure() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let provider = key::<u32>("provider", owner.clone());
    let consumer = key::<u32>("consumer", owner);
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
async fn retained_references() {
    let owner = scope();
    let pins = Arc::new(MemoryPins::default());
    let graph = Graph::new(pins.clone());
    let key = key::<u32>("provider", owner.clone());
    let definition = spec(&key);
    let reference = definition.pin_reference;
    graph
        .register(key.clone(), definition.clone(), constant(1))
        .await
        .unwrap();
    graph.activate().await.unwrap();
    graph.release(service_scope(&owner)).await.unwrap();
    assert_eq!(
        pins.list(
            &PinSubject::Owner {
                owner_id: owner.owner_id()
            },
            reference
        )
        .await
        .unwrap()
        .len(),
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
    pins.unpin(
        &PinSubject::Owner {
            owner_id: owner.owner_id(),
        },
        reference,
        "provider",
    )
    .await
    .unwrap();
    assert!(restored_graph.activate().await.unwrap().is_success());
}

#[tokio::test]
async fn activation_release() {
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
    let owner_key = key::<u32>("owner", owner.clone());
    let instance_key = key::<u32>("instance", instance.clone());
    let activation_key = key::<u32>("activation", activation.clone());
    let sibling_key = key::<u32>("sibling", scope());
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
        graph
            .release(service_scope(&activation))
            .await
            .unwrap()
            .stopped,
        vec![activation_key.id().clone()]
    );
    for key in [&owner_key, &instance_key, &sibling_key] {
        assert!(graph.service(key).is_ok());
    }
    assert!(matches!(
        graph.service(&activation_key),
        Err(GraphError::Unavailable(_))
    ));
}

#[tokio::test]
async fn replacement_conflict() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let key = key::<u32>("provider", owner.clone());
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
    let handle = graph.service(&key).unwrap();
    assert_eq!(
        handle
            .call(&authority(&owner), |value, _| Box::pin(
                async move { *value }
            ))
            .await
            .unwrap(),
        1
    );
    let snapshot = graph.snapshots().unwrap().pop().unwrap();
    assert_eq!(snapshot.state, ServiceState::Ready);
    assert_eq!(snapshot.last_failure.unwrap().stage, FailureStage::Pin);
}

#[tokio::test]
async fn transition_cancellation() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let key = key::<u32>("provider", owner.clone());
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
    let released = graph.release(service_scope(&owner)).await.unwrap();
    assert_eq!(released.stopped, vec![key.id().clone()]);
    assert!(closed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn transition_panics() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let failed = key::<u32>("failed", owner.clone());
    graph
        .register(failed.clone(), spec(&failed), panicking_factory)
        .await
        .unwrap();
    let cleanup = key::<u32>("cleanup", owner.clone());
    graph
        .register(cleanup.clone(), spec(&cleanup), |_| async {
            Ok(Started::new(1, panicking_cleanup))
        })
        .await
        .unwrap();
    let report = graph.activate().await.unwrap();
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].stage, FailureStage::Start);
    let report = graph.release(service_scope(&owner)).await.unwrap();
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
async fn inactive_release() {
    let owner = scope();
    let graph = Graph::new(Arc::new(MemoryPins::default()));
    let existing = key::<u32>("existing", owner.clone());
    graph
        .register(existing.clone(), spec(&existing), constant(1))
        .await
        .unwrap();
    graph.activate().await.unwrap();
    let incomplete = key::<u32>("incomplete", owner.clone());
    let missing = key::<u32>("missing", owner.clone());
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
        graph.release(service_scope(&owner)).await.unwrap().stopped,
        vec![existing.id().clone()]
    );
}
