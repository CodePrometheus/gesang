//! Actual credential possession, grant changes, and transactional revocation ordering.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{TimeDelta, Utc};
use gesang_api::CapabilityScope;
use gesang_core::{PayloadKey, Store, VerifiedIdentity};
use gesang_plugins::{Authority, BindingScope, PrincipalId, ResourceScope};
use gesang_services::{Caller, Credential, CredentialGrant, Error, ServiceAccess};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{collections::BTreeSet, str::FromStr};
use uuid::Uuid;

async fn store() -> Store {
    let url =
        std::env::var("GESANG_TEST_DATABASE_URL").expect("dedicated integration database required");
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
    let store = Store::new(pool, PayloadKey::new([42; 32]));
    store.migrate().await.unwrap();
    ServiceAccess::new(store.clone()).migrate().await.unwrap();
    store
}

fn scope(action: &str) -> CapabilityScope {
    CapabilityScope {
        capability: format!("sandbox.{action}"),
        resource: "template-a".into(),
    }
}

fn grant() -> CredentialGrant {
    CredentialGrant {
        binding: BindingScope::Integration {
            deployment_id: Uuid::new_v4(),
            integration_id: Uuid::new_v4(),
        },
        principal: PrincipalId::Service {
            service_id: Uuid::new_v4(),
        },
        scopes: BTreeSet::from([scope("create"), scope("observe"), scope("stop")]),
        expires_at: Utc::now() + TimeDelta::hours(1),
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn bearer_authentication() {
    let store = store().await;
    let access = ServiceAccess::new(store.clone());
    let grant = grant();
    let (reference, secret) = access.issue(&grant).await.unwrap();
    let parsed = Credential::parse(secret.expose().into()).unwrap();
    let caller = access.authenticate(&parsed).await.unwrap();
    assert_eq!(caller.authorization(), &reference);
    assert_eq!(caller.authority().principal(), grant.principal);
    assert_eq!(caller.authority().binding(), grant.binding);
    assert_eq!(caller.authority().capabilities(), &grant.scopes);
    assert_eq!(format!("{secret:?}"), "Credential([redacted])");
    let stored = sqlx::query_scalar::<_, String>(
        "SELECT row_to_json(c)::text FROM gesang_service_credentials c WHERE id=$1",
    )
    .bind(reference.id)
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert!(!stored.contains(secret.expose()));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_owners")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
    let (_, other) = access.issue(&grant).await.unwrap();
    assert!(secret.expose() != other.expose());
    let mut changed = secret.expose().as_bytes().to_vec();
    changed[3] = if changed[3] == b'A' { b'B' } else { b'A' };
    let unknown = Credential::parse(String::from_utf8(changed).unwrap()).unwrap();
    assert!(matches!(
        access.authenticate(&unknown).await,
        Err(Error::Unauthorized)
    ));
    for invalid in ["", "s1.", "Bearer s1.invalid", "s1.invalid"] {
        assert!(matches!(
            Credential::parse(invalid.into()),
            Err(Error::Unauthorized)
        ));
    }
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn scoped_authority() {
    let store = store().await;
    let access = ServiceAccess::new(store.clone());
    let original = grant();
    let (reference, secret) = access.issue(&original).await.unwrap();
    let caller = access.authenticate(&secret).await.unwrap();
    let mut tx = store.begin().await.unwrap();
    let actual = access
        .authorize_in(&mut tx, &caller, &scope("observe"))
        .await
        .unwrap();
    assert_eq!(actual.capabilities(), &BTreeSet::from([scope("observe")]));
    let narrowed = Caller::verified(
        caller.authority().restrict([scope("observe")]).unwrap(),
        reference.clone(),
    );
    assert!(matches!(
        access
            .authorize_in(&mut tx, &narrowed, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    let forged = Caller::verified(
        Authority::service(grant().binding, Uuid::new_v4(), original.scopes.clone()),
        reference.clone(),
    );
    assert!(matches!(
        access
            .authorize_in(&mut tx, &forged, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    let principal = match original.principal {
        PrincipalId::Service { service_id } => service_id,
        _ => unreachable!(),
    };
    let other_binding = Caller::verified(
        Authority::service(grant().binding, principal, original.scopes.clone()),
        reference.clone(),
    );
    assert!(matches!(
        access
            .authorize_in(&mut tx, &other_binding, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    let other_principal = Caller::verified(
        Authority::service(original.binding, Uuid::new_v4(), original.scopes.clone()),
        reference.clone(),
    );
    assert!(matches!(
        access
            .authorize_in(&mut tx, &other_principal, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    let mut unknown = reference;
    unknown.issuer = "foreign.authority".into();
    let unknown = Caller::verified(caller.authority().clone(), unknown);
    assert!(matches!(
        access
            .authorize_in(&mut tx, &unknown, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    tx.commit().await.unwrap();
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "access-test".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let mut owner_grant = original;
    owner_grant.principal = PrincipalId::Owner { owner_id: owner.id };
    let (reference, secret) = access.issue(&owner_grant).await.unwrap();
    let caller = access.authenticate(&secret).await.unwrap();
    let instance = ResourceScope::Instance {
        owner_id: owner.id,
        instance_id: Uuid::new_v4(),
    };
    let narrowed = Caller::verified(
        caller
            .authority()
            .narrow(instance.clone(), [scope("observe")])
            .unwrap(),
        reference,
    );
    let mut tx = store.begin().await.unwrap();
    let result = access
        .authorize_in(&mut tx, &narrowed, &scope("observe"))
        .await
        .unwrap();
    assert_eq!(result.owner_scope(), Some(&instance));
    tx.commit().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn grant_changes() {
    let store = store().await;
    let access = ServiceAccess::new(store.clone());
    let (reference, secret) = access.issue(&grant()).await.unwrap();
    let previous = access.authenticate(&secret).await.unwrap();
    let retained = BTreeSet::from([scope("observe"), scope("stop")]);
    let revised = access.replace(&reference, &retained).await.unwrap();
    assert!(revised.revision > reference.revision);
    assert!(matches!(
        access.replace(&reference, &retained).await,
        Err(Error::Conflict)
    ));
    let current = access.authenticate(&secret).await.unwrap();
    let mut tx = store.begin().await.unwrap();
    assert!(matches!(
        access
            .authorize_in(&mut tx, &previous, &scope("observe"))
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        access
            .authorize_in(&mut tx, &current, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    access
        .authorize_in(&mut tx, &current, &scope("stop"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    access.revoke(reference.id).await.unwrap();
    access.revoke(reference.id).await.unwrap();
    assert!(matches!(
        access.authenticate(&secret).await,
        Err(Error::Unauthorized)
    ));
    let mut tx = store.begin().await.unwrap();
    assert!(matches!(
        access.authorize_in(&mut tx, &current, &scope("stop")).await,
        Err(Error::Forbidden)
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn revocation_order() {
    let store = store().await;
    let access = ServiceAccess::new(store.clone());
    let (reference, secret) = access.issue(&grant()).await.unwrap();
    let caller = access.authenticate(&secret).await.unwrap();
    let mut tx = store.begin().await.unwrap();
    let holder = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    access
        .authorize_in(&mut tx, &caller, &scope("create"))
        .await
        .unwrap();
    let revoker = access.clone();
    let mut revoke = tokio::spawn(async move { revoker.revoke(reference.id).await });
    loop {
        tokio::select! {
            result=&mut revoke=>panic!("revocation escaped the admission lock: {result:?}"),
            blocked=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))")
                .bind(holder).fetch_one(store.pool())=>if blocked.unwrap() {break;},
        }
    }
    tx.commit().await.unwrap();
    revoke.await.unwrap().unwrap();
    let mut tx = store.begin().await.unwrap();
    assert!(matches!(
        access
            .authorize_in(&mut tx, &caller, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn credential_expiry() {
    let store = store().await;
    let access = ServiceAccess::new(store.clone());
    let mut grant = grant();
    grant.expires_at = Utc::now() - TimeDelta::seconds(1);
    assert!(matches!(access.issue(&grant).await, Err(Error::Invalid(_))));
    // This short lifetime is explicit test input, not a production default or assertion timeout.
    grant.expires_at = Utc::now() + TimeDelta::seconds(1);
    let (reference, secret) = access.issue(&grant).await.unwrap();
    while !sqlx::query_scalar::<_, bool>(
        "SELECT expires_at<=clock_timestamp() FROM gesang_service_credentials WHERE id=$1",
    )
    .bind(reference.id)
    .fetch_one(store.pool())
    .await
    .unwrap()
    {
        tokio::task::yield_now().await;
    }
    assert!(matches!(
        access.authenticate(&secret).await,
        Err(Error::Unauthorized)
    ));
    let authority = Authority::service(
        grant.binding,
        match grant.principal {
            PrincipalId::Service { service_id } => service_id,
            _ => unreachable!(),
        },
        grant.scopes,
    );
    let previous = Caller::verified(authority, reference);
    let mut tx = store.begin().await.unwrap();
    assert!(matches!(
        access
            .authorize_in(&mut tx, &previous, &scope("create"))
            .await,
        Err(Error::Forbidden)
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn store_failure() {
    let store = store().await;
    let access = ServiceAccess::new(store.clone());
    let (_, secret) = access.issue(&grant()).await.unwrap();
    access.authenticate(&secret).await.unwrap();
    store.pool().close().await;
    assert!(matches!(
        access.authenticate(&secret).await,
        Err(Error::Store(sqlx::Error::PoolClosed))
    ));
}
