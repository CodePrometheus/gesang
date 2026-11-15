//! Real transactional references and trusted service lifetimes; caller scopes are test inputs.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use gesang_api::CapabilityScope;
use gesang_core::{PayloadKey, Store, VerifiedIdentity};
use gesang_plugins::{
    Authority, BindingScope, CallSubject, FailureStage, Graph, GraphError, PackageVersion,
    PinError, PinStore, PinSubject, ResourceScope, ServiceContext, ServiceHandle, ServiceKey,
    ServiceScope, ServiceSpec, Started, VersionPin,
};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::sync::Barrier;
use uuid::Uuid;

async fn database() -> Store {
    let url = std::env::var("GESANG_TEST_DATABASE_URL")
        .expect("a dedicated integration database is required");
    let schema = format!("test_{}", Uuid::new_v4().simple());
    let bootstrap = PgPoolOptions::new().connect(&url).await.unwrap();
    bootstrap
        .execute(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .connect_with(
            PgConnectOptions::from_str(&url)
                .unwrap()
                .options([("search_path", schema.as_str())]),
        )
        .await
        .unwrap();
    bootstrap.close().await;
    Store::new(pool, PayloadKey::new([42; 32]))
}

async fn store() -> Store {
    let store = database().await;
    store.migrate().await.unwrap();
    store
}

fn binding() -> BindingScope {
    BindingScope::Integration {
        deployment_id: Uuid::new_v4(),
        integration_id: Uuid::new_v4(),
    }
}

fn spec(reference: Uuid) -> ServiceSpec {
    ServiceSpec {
        package: PackageVersion {
            package: "test-provider".into(),
            version: "1".into(),
            digest: "content-a".into(),
        },
        pin_reference: reference,
        dependencies: vec![],
    }
}

fn pin(subject: PinSubject, reference_id: Uuid) -> VersionPin {
    VersionPin {
        subject,
        reference_id,
        package: "test-provider".into(),
        version: "1".into(),
        digest: "content-a".into(),
    }
}

#[test]
fn binding_boundaries() {
    let deployment_id = Uuid::new_v4();
    let deployment = BindingScope::Deployment { deployment_id };
    let integration = BindingScope::Integration {
        deployment_id,
        integration_id: Uuid::new_v4(),
    };
    let other = BindingScope::Integration {
        deployment_id,
        integration_id: Uuid::new_v4(),
    };
    let owner = ResourceScope::Owner {
        owner_id: Uuid::new_v4(),
    };
    let shared = ServiceScope::shared(deployment);
    let connected = ServiceScope::shared(integration);
    let private = ServiceScope::owned(integration, owner.clone());
    assert!(shared.contains(&connected));
    assert!(connected.contains(&private));
    assert!(!private.contains(&connected));
    assert!(!connected.contains(&shared));
    assert!(!connected.contains(&ServiceScope::shared(other)));
    let authority = Authority::service(integration, Uuid::new_v4(), []);
    assert!(shared.accepts(&authority));
    assert!(!private.accepts(&authority));
    assert!(authority.narrow(owner, []).is_err());
    let restricted = authority.restrict([]).unwrap();
    assert_eq!(restricted.binding(), integration);
    assert_eq!(restricted.subject(), authority.subject());
    let missing_owner = serde_json::json!({"kind":"owned","binding":integration});
    assert!(serde_json::from_value::<ServiceScope>(missing_owner).is_err());
    let implicit_shared = serde_json::json!({"binding":integration});
    assert!(serde_json::from_value::<ServiceScope>(implicit_shared).is_err());
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn shared_calls() {
    let store = store().await;
    let binding = binding();
    let scope = ServiceScope::shared(binding);
    let reference = Uuid::new_v4();
    let key = ServiceKey::<Mutex<Vec<CallSubject>>>::new("shared", scope.clone());
    let graph = Graph::new(Arc::new(store.clone()));
    let starts = Arc::new(AtomicUsize::new(0));
    let count = starts.clone();
    graph
        .register(
            key.clone(),
            spec(reference),
            move |context: gesang_plugins::ServiceContext| {
                let count = count.clone();
                async move {
                    assert!(context.scope().owner_scope().is_none());
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(Started::new(Mutex::new(Vec::new()), || async { Ok(()) }))
                }
            },
        )
        .await
        .unwrap();
    assert!(graph.activate().await.unwrap().is_success());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_owners")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
    let handle = graph.service(&key).unwrap();
    let service = Authority::service(binding, Uuid::new_v4(), []);
    handle
        .call(&service, |seen, authority| {
            Box::pin(async move {
                seen.lock().unwrap().push(authority.subject().clone());
            })
        })
        .await
        .unwrap();

    let first = store
        .bind_owner(&VerifiedIdentity {
            issuer: "service-test".into(),
            subject: "first".into(),
        })
        .await
        .unwrap();
    let second = store
        .bind_owner(&VerifiedIdentity {
            issuer: "service-test".into(),
            subject: "second".into(),
        })
        .await
        .unwrap();
    let authorities = [first.id, second.id]
        .map(|owner_id| Authority::owner(binding, ResourceScope::Owner { owner_id }, []));
    let barrier = Arc::new(Barrier::new(authorities.len()));
    let mut calls = Vec::new();
    for authority in authorities.clone() {
        let handle = handle.clone();
        let barrier = barrier.clone();
        calls.push(tokio::spawn(async move {
            handle
                .call(&authority, move |seen, actual| {
                    Box::pin(async move {
                        barrier.wait().await;
                        seen.lock().unwrap().push(actual.subject().clone());
                        actual.subject().clone()
                    })
                })
                .await
                .unwrap()
        }));
    }
    for (call, authority) in calls.into_iter().zip(&authorities) {
        assert_eq!(&call.await.unwrap(), authority.subject());
    }
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    let BindingScope::Integration { deployment_id, .. } = binding else {
        panic!("an integration binding is required by this scenario");
    };
    for denied in [
        super_binding(binding),
        BindingScope::Integration {
            deployment_id,
            integration_id: Uuid::new_v4(),
        },
        self::binding(),
    ] {
        assert!(matches!(
            handle
                .call(&Authority::service(denied, Uuid::new_v4(), []), |_, _| {
                    Box::pin(async { panic!("foreign binding reached the service") })
                })
                .await,
            Err(GraphError::Authority(_))
        ));
    }

    let private = ServiceKey::<u32>::new(
        "private",
        ServiceScope::owned(binding, ResourceScope::Owner { owner_id: first.id }),
    );
    graph
        .register(private.clone(), spec(Uuid::new_v4()), |_| async {
            Ok(Started::new(7, || async { Ok(()) }))
        })
        .await
        .unwrap();
    assert!(graph.activate().await.unwrap().is_success());
    let private_handle = graph.service(&private).unwrap();
    for denied in [&service, &authorities[1]] {
        assert!(matches!(
            private_handle
                .call(denied, |_, _| Box::pin(async {
                    panic!("foreign caller reached private resources")
                }))
                .await,
            Err(GraphError::Authority(_))
        ));
    }
    assert_eq!(
        private_handle
            .call(&authorities[0], |value, _| Box::pin(async move { *value }))
            .await
            .unwrap(),
        7
    );
    assert!(graph.release(scope).await.unwrap().is_success());
    assert!(matches!(
        handle.call(&service, |_, _| Box::pin(async {})).await,
        Err(GraphError::StaleHandle(_))
    ));
    assert_eq!(
        store
            .list(&PinSubject::Installation { binding }, reference)
            .await
            .unwrap()
            .len(),
        1
    );
}

fn super_binding(binding: BindingScope) -> BindingScope {
    match binding {
        BindingScope::Integration { deployment_id, .. } => {
            BindingScope::Deployment { deployment_id }
        }
        _ => panic!("an integration binding is required by this scenario"),
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn dependency_calls() {
    let store = store().await;
    let binding = binding();
    let graph = Graph::new(Arc::new(store.clone()));
    let provider = ServiceKey::<()>::new("provider", ServiceScope::shared(super_binding(binding)));
    graph
        .register(provider.clone(), spec(Uuid::new_v4()), |_| async {
            Ok(Started::new((), || async { Ok(()) }))
        })
        .await
        .unwrap();
    let consumer = ServiceKey::<ServiceHandle<()>>::new("consumer", ServiceScope::shared(binding));
    let mut definition = spec(Uuid::new_v4());
    definition.dependencies.push(provider.dependency());
    graph
        .register(
            consumer.clone(),
            definition,
            move |context: ServiceContext| {
                let provider = provider.clone();
                async move {
                    let handle = context
                        .service(&provider)
                        .map_err(|error| error.to_string())?;
                    Ok(Started::new(handle, || async { Ok(()) }))
                }
            },
        )
        .await
        .unwrap();
    assert!(graph.activate().await.unwrap().is_success());
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "dependency-test".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let read = CapabilityScope {
        capability: "records.read".into(),
        resource: "selected-record".into(),
    };
    let handle = graph.service(&consumer).unwrap();
    for authority in [
        Authority::service(binding, Uuid::new_v4(), []),
        Authority::owner(binding, ResourceScope::Owner { owner_id: owner.id }, [read]),
    ] {
        let observed = handle
            .call(&authority, |provider, actual| {
                Box::pin(async move {
                    provider
                        .call(actual, |_, forwarded| {
                            Box::pin(async move { forwarded.clone() })
                        })
                        .await
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed, authority);
    }
    assert!(
        graph
            .release(ServiceScope::shared(super_binding(binding)))
            .await
            .unwrap()
            .is_success()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn shared_dependencies() {
    let store = store().await;
    let binding = binding();
    let graph = Graph::new(Arc::new(store.clone()));
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "dependency-test".into(),
            subject: "private-provider".into(),
        })
        .await
        .unwrap();
    let provider = ServiceKey::<()>::new(
        "private",
        ServiceScope::owned(binding, ResourceScope::Owner { owner_id: owner.id }),
    );
    let consumer = ServiceKey::<()>::new("shared", ServiceScope::shared(binding));
    let starts = Arc::new(AtomicUsize::new(0));
    for key in [&provider, &consumer] {
        let mut definition = spec(Uuid::new_v4());
        if key.id() == consumer.id() {
            definition.dependencies.push(provider.dependency());
        }
        let starts = starts.clone();
        graph
            .register(key.clone(), definition, move |_| {
                let starts = starts.clone();
                async move {
                    starts.fetch_add(1, Ordering::SeqCst);
                    Ok(Started::new((), || async { Ok(()) }))
                }
            })
            .await
            .unwrap();
    }
    assert_eq!(
        graph.activate().await.unwrap_err(),
        GraphError::DependencyScope {
            service: consumer.id().clone(),
            dependency: Box::new(provider.id().clone()),
        }
    );
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_package_pins")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pin_failure() {
    let store = store().await;
    let scope = ServiceScope::shared(binding());
    let key = ServiceKey::<()>::new("unavailable", scope);
    let graph = Graph::new(Arc::new(store.clone()));
    let starts = Arc::new(AtomicUsize::new(0));
    let count = starts.clone();
    graph
        .register(key.clone(), spec(Uuid::new_v4()), move |_| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(Started::new((), || async { Ok(()) }))
            }
        })
        .await
        .unwrap();
    store.pool().close().await;
    let report = graph.activate().await.unwrap();
    assert!(!report.is_success());
    assert!(report.started.is_empty());
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].stage, FailureStage::Pin);
    assert_eq!(report.failures[0].service, *key.id());
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(graph.service(&key).is_err());
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn reference_domains() {
    let store = store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "pin-test".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let binding = binding();
    let reference = Uuid::new_v4();
    let subjects = [
        PinSubject::Owner { owner_id: owner.id },
        PinSubject::Installation { binding },
        PinSubject::Resource {
            binding,
            resource_id: reference,
        },
    ];
    let mut expected = Vec::new();
    for (index, subject) in subjects.iter().enumerate() {
        let mut pin = pin(subject.clone(), reference);
        pin.version = index.to_string();
        expected.push(store.pin(pin).await.unwrap());
    }
    let options = store.pool().connect_options().as_ref().clone();
    store.pool().close().await;
    let reopened = Store::new(
        PgPoolOptions::new().connect_with(options).await.unwrap(),
        PayloadKey::new([42; 32]),
    );
    for value in &expected {
        assert_eq!(
            reopened
                .get(&value.subject, reference, &value.package)
                .await
                .unwrap()
                .as_ref(),
            Some(value)
        );
    }
    reopened
        .unpin(&subjects[1], reference, "test-provider")
        .await
        .unwrap();
    assert!(
        reopened
            .list(&subjects[1], reference)
            .await
            .unwrap()
            .is_empty()
    );
    for subject in [&subjects[0], &subjects[2]] {
        assert_eq!(reopened.list(subject, reference).await.unwrap().len(), 1);
    }
    assert!(
        reopened
            .get(
                &PinSubject::Resource {
                    binding: self::binding(),
                    resource_id: reference
                },
                reference,
                "test-provider"
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pin_conflicts() {
    let store = store().await;
    let original = pin(
        PinSubject::Installation { binding: binding() },
        Uuid::new_v4(),
    );
    let changed = VersionPin {
        digest: "content-b".into(),
        ..original.clone()
    };
    let (left, right) = tokio::join!(store.pin(original.clone()), store.pin(changed.clone()));
    let winner = match (left, right) {
        (Ok(value), Err(PinError::Conflict { .. }))
        | (Err(PinError::Conflict { .. }), Ok(value)) => value,
        other => {
            panic!("conflicting concurrent pins did not select exactly one version: {other:?}")
        }
    };
    assert_eq!(
        store
            .get(&original.subject, original.reference_id, &original.package)
            .await
            .unwrap(),
        Some(winner)
    );
    let missing_owner = pin(
        PinSubject::Owner {
            owner_id: Uuid::new_v4(),
        },
        Uuid::new_v4(),
    );
    assert!(store.pin(missing_owner).await.is_err());
    store.pool().close().await;
    assert!(
        store
            .get(&original.subject, original.reference_id, &original.package)
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pin_migration() {
    let store = database().await;
    let migrations = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let legacy = std::env::temp_dir().join(format!("pin-migrations-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&legacy).unwrap();
    for entry in std::fs::read_dir(&migrations).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let version: u64 = name
            .to_str()
            .unwrap()
            .split('_')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        if version >= 11 {
            continue;
        }
        std::fs::copy(entry.path(), legacy.join(name)).unwrap();
    }
    sqlx::migrate::Migrator::new(legacy.as_path())
        .await
        .unwrap()
        .run(store.pool())
        .await
        .unwrap();
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "migration-test".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let expected = pin(PinSubject::Owner { owner_id: owner.id }, Uuid::new_v4());
    sqlx::query("INSERT INTO gesang_version_pins(owner_id,reference_id,package,version,digest) VALUES($1,$2,$3,$4,$5)")
        .bind(owner.id).bind(expected.reference_id).bind(&expected.package).bind(&expected.version).bind(&expected.digest).execute(store.pool()).await.unwrap();
    store.migrate().await.unwrap();
    assert_eq!(
        store
            .get(&expected.subject, expected.reference_id, &expected.package)
            .await
            .unwrap(),
        Some(expected.clone())
    );
    for query in [
        "UPDATE gesang_version_pins SET version='changed'",
        "DELETE FROM gesang_version_pins",
        "INSERT INTO gesang_version_pins SELECT * FROM gesang_version_pins",
    ] {
        let error = sqlx::query(query).execute(store.pool()).await.unwrap_err();
        assert_eq!(
            error
                .as_database_error()
                .and_then(|error| error.code())
                .as_deref(),
            Some("55000")
        );
    }
    store
        .unpin(&expected.subject, expected.reference_id, &expected.package)
        .await
        .unwrap();
    assert!(
        store
            .get(&expected.subject, expected.reference_id, &expected.package)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_version_pins")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT digest FROM gesang_version_pins")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        expected.digest
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn pin_commit_policy() {
    let store = store().await;
    sqlx::raw_sql("CREATE FUNCTION check_pin_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit') NOT IN ('on','remote_apply') THEN RAISE EXCEPTION 'unsafe commit'; END IF; IF TG_OP='DELETE' THEN RETURN OLD; END IF; RETURN NEW; END $$; CREATE TRIGGER check_pin_commit BEFORE INSERT OR UPDATE OR DELETE ON gesang_package_pins FOR EACH ROW EXECUTE FUNCTION check_pin_commit();")
        .execute(store.pool()).await.unwrap();
    let pool = PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit='off'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(store.pool().connect_options().as_ref().clone())
        .await
        .unwrap();
    let unsafe_defaults = Store::new(pool, PayloadKey::new([42; 32]));
    for subject in [
        PinSubject::Installation { binding: binding() },
        PinSubject::Resource {
            binding: binding(),
            resource_id: Uuid::new_v4(),
        },
    ] {
        let pin = pin(subject, Uuid::new_v4());
        unsafe_defaults.pin(pin.clone()).await.unwrap();
        unsafe_defaults.pin(pin.clone()).await.unwrap();
        unsafe_defaults
            .unpin(&pin.subject, pin.reference_id, &pin.package)
            .await
            .unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW synchronous_commit")
            .fetch_one(unsafe_defaults.pool())
            .await
            .unwrap(),
        "off"
    );
}
