//! Real transactional checks for identity, delivery, revocation, wakes, and fencing metadata.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "../test-support.rs"]
mod support;
use support::ReadyFixture;

use std::{str::FromStr, sync::Arc};

use chrono::{TimeDelta, Utc};
use gesang_api::{
    CommandAuthorization, CommandSubmission, GrantPurpose, GrantSelection, PermissionMode,
};
use gesang_core::{
    Admission, CommandRequirements, Error, GrantRequirement, PayloadKey, PreviousWriter,
    RequiredAuthority, Store, VerifiedIdentity,
};
use serde_json::json;
use sqlx::{
    Executor, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use uuid::Uuid;

fn execution_requirements() -> CommandRequirements {
    CommandRequirements {
        authority: RequiredAuthority::TaskGrant,
        scopes: vec![],
    }
}

async fn store() -> Store {
    let url = std::env::var("GESANG_TEST_DATABASE_URL")
        .expect("set GESANG_TEST_DATABASE_URL to a dedicated integration database");
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
    let store = Store::new(pool, PayloadKey::new([42; 32]));
    store.migrate().await.unwrap();
    bootstrap.close().await;
    store
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn host_transaction_holds_authority_until_commit_and_preserves_control_after_revocation() {
    let store = store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "transaction-host".into(),
            subject: "owner".into(),
        })
        .await
        .unwrap();
    let agent = store.ensure_agent(&owner, "engine", "local").await.unwrap();
    let claim = store
        .claim_runtime(agent.id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    let runtime = store
        .admit_ready_fixture(
            &claim,
            &Admission {
                runtime_id: Uuid::new_v4(),
                locator: "protected-runtime".into(),
                engine_owner: "local".into(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await
        .unwrap();
    let grant = store
        .select_grant(
            &owner,
            agent.id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    let requirement = GrantRequirement {
        id: grant.id,
        version: grant.version,
        purpose: GrantPurpose::Task,
        scopes: &[],
    };
    let mut tx = store.pool().begin().await.unwrap();
    assert_eq!(
        store
            .authorize_runtime_in(&mut tx, &runtime, &requirement)
            .await
            .unwrap(),
        grant
    );
    let blocked = sqlx::query("SELECT id FROM gesang_agents WHERE id=$1 FOR UPDATE NOWAIT")
        .bind(agent.id)
        .fetch_one(store.pool())
        .await
        .unwrap_err();
    assert_eq!(
        blocked.as_database_error().unwrap().code().as_deref(),
        Some("55P03")
    );
    tx.commit().await.unwrap();
    sqlx::query("SELECT id FROM gesang_agents WHERE id=$1 FOR UPDATE NOWAIT")
        .bind(agent.id)
        .fetch_one(store.pool())
        .await
        .unwrap();

    let mut wrong_purpose = store.pool().begin().await.unwrap();
    assert!(matches!(
        store
            .authorize_runtime_in(
                &mut wrong_purpose,
                &runtime,
                &GrantRequirement {
                    purpose: GrantPurpose::Improvement,
                    ..requirement
                },
            )
            .await,
        Err(Error::Forbidden)
    ));
    wrong_purpose.rollback().await.unwrap();

    store
        .revoke_grant(&owner, agent.id, grant.id)
        .await
        .unwrap();
    let mut revoked = store.pool().begin().await.unwrap();
    assert!(matches!(
        store
            .authorize_runtime_in(&mut revoked, &runtime, &requirement)
            .await,
        Err(Error::Forbidden)
    ));
    store
        .verify_writer_in(&mut revoked, &runtime)
        .await
        .unwrap();
    revoked.commit().await.unwrap();

    for changed in [
        gesang_core::Runtime {
            generation: runtime.generation + 1,
            ..runtime.clone()
        },
        gesang_core::Runtime {
            engine_owner: "foreign-owner".into(),
            ..runtime.clone()
        },
    ] {
        let mut tx = store.pool().begin().await.unwrap();
        assert!(matches!(
            store.verify_writer_in(&mut tx, &changed).await,
            Err(Error::StaleGeneration)
        ));
        tx.rollback().await.unwrap();
    }

    let mut snapshot = store.pool().begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *snapshot)
        .await
        .unwrap();
    assert!(matches!(
        store
            .authorize_runtime_in(&mut snapshot, &runtime, &requirement)
            .await,
        Err(Error::DurabilityRequired)
    ));
    assert!(matches!(
        store.verify_writer_in(&mut snapshot, &runtime).await,
        Err(Error::DurabilityRequired)
    ));
    snapshot.rollback().await.unwrap();
    store.pool().close().await;
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn identity_authorization_and_deduplicated_delivery_survive_reopening() {
    let store = store().await;
    let identity = VerifiedIdentity {
        issuer: "issuer-a".into(),
        subject: "person-a".into(),
    };
    assert_eq!(store.find_owner(&identity).await.unwrap(), None);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM gesang_owners")
            .fetch_one(store.pool())
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        store
            .find_owner(&VerifiedIdentity {
                issuer: String::new(),
                subject: "person-a".into()
            })
            .await,
        Err(Error::Forbidden)
    ));
    let owner = store.bind_owner(&identity).await.unwrap();
    assert_eq!(
        store.find_owner(&identity).await.unwrap(),
        Some(owner.clone())
    );
    assert_eq!(
        store
            .find_owner(&VerifiedIdentity {
                issuer: "issuer-b".into(),
                subject: "person-a".into()
            })
            .await
            .unwrap(),
        None
    );
    assert_eq!(owner, store.bind_owner(&identity).await.unwrap());
    let other = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer-b".into(),
            subject: "person-a".into(),
        })
        .await
        .unwrap();
    assert_ne!(owner, other);
    let (a, b) = tokio::join!(
        store.ensure_agent(&owner, "execution-v1", "local"),
        store.ensure_agent(&owner, "execution-v1", "local")
    );
    let agent = a.unwrap();
    assert_eq!(agent, b.unwrap());
    assert!(store.session(&owner).await.unwrap().grant.is_none());
    assert!(matches!(
        store.agent(&other, agent.id).await,
        Err(Error::NotFound)
    ));
    let grant = store
        .select_grant(
            &owner,
            agent.id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    let submission = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization: CommandAuthorization::Grant {
            id: grant.id,
            version: grant.version,
        },
        command: json!({"kind":"goal.submit","objective":"confidential goal","completionCriteria":[]}),
    };
    let requirements = execution_requirements();
    let (a, b) = tokio::join!(
        store.enqueue(&owner, agent.id, &submission, &requirements),
        store.enqueue(&owner, agent.id, &submission, &requirements)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM gesang_commands")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
    let mut changed = submission.clone();
    changed.command["objective"] = json!("different goal");
    assert!(matches!(
        store
            .enqueue(&owner, agent.id, &changed, &execution_requirements())
            .await,
        Err(Error::RequestConflict)
    ));
    assert!(matches!(
        store.receipt(&other, submission.request_id).await,
        Err(Error::NotFound)
    ));
    let sealed: Vec<u8> =
        sqlx::query_scalar("SELECT sealed_command FROM gesang_commands WHERE request_id=$1")
            .bind(submission.request_id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(
        !sealed
            .windows(b"confidential goal".len())
            .any(|window| window == b"confidential goal")
    );
    let claimant = Uuid::new_v4();
    let pending = store
        .claim_command(claimant, TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.command, submission.command);
    assert!(
        store
            .claim_command(Uuid::new_v4(), TimeDelta::minutes(1))
            .await
            .unwrap()
            .is_none()
    );
    let receipt = json!({"requestId":submission.request_id,"goalId":"goal-a","acceptedVersion":1,"acceptedAt":0});
    store.accept_command(&pending, &receipt).await.unwrap();
    let accepted_request_id = submission.request_id;
    let reopened = Store::new(store.pool().clone(), PayloadKey::new([42; 32]));
    assert_eq!(
        reopened
            .receipt(&owner, submission.request_id)
            .await
            .unwrap()
            .engine_receipt,
        Some(receipt)
    );
    store
        .revoke_grant(&owner, agent.id, grant.id)
        .await
        .unwrap();
    assert!(matches!(
        store
            .authorize(agent.id, grant.id, grant.version, &[])
            .await,
        Err(Error::Forbidden)
    ));
    assert_eq!(
        store
            .enqueue(&owner, agent.id, &submission, &execution_requirements())
            .await
            .unwrap(),
        reopened
            .receipt(&owner, submission.request_id)
            .await
            .unwrap()
    );
    let mut new_request = submission;
    new_request.request_id = Uuid::new_v4();
    assert!(matches!(
        store
            .enqueue(&owner, agent.id, &new_request, &execution_requirements())
            .await,
        Err(Error::Forbidden)
    ));
    let wrong_key = Store::new(store.pool().clone(), PayloadKey::new([41; 32]));
    assert!(matches!(
        wrong_key.receipt(&owner, accepted_request_id).await,
        Err(Error::InvalidCiphertext)
    ));
    let stop = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization: CommandAuthorization::Owner,
        command: json!({"kind":"goal.stop","goalId":"goal-a","expectedVersion":1}),
    };
    let control_requirements = CommandRequirements {
        authority: RequiredAuthority::OwnerControl,
        scopes: vec![],
    };
    store
        .enqueue(&owner, agent.id, &stop, &control_requirements)
        .await
        .unwrap();
    assert!(matches!(
        store
            .enqueue(&other, agent.id, &stop, &control_requirements)
            .await,
        Err(Error::NotFound)
    ));
    let mut forged = stop;
    forged.request_id = Uuid::new_v4();
    forged.command = json!({"kind":"goal.submit","objective":"bypass"});
    assert!(matches!(
        store
            .enqueue(&owner, agent.id, &forged, &execution_requirements())
            .await,
        Err(Error::Forbidden)
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn expired_wake_attempt_cannot_settle_a_new_claim_even_if_claimant_is_reused() {
    let store = store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let agent = store
        .ensure_agent(&owner, "execution-v1", "local")
        .await
        .unwrap();
    store.schedule(agent.id, Utc::now()).await.unwrap();
    let claimant = Uuid::new_v4();
    let old = store
        .claim_wake(claimant, TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE gesang_wakes SET claimed_until=clock_timestamp()-interval '1 second' WHERE agent_id=$1")
        .bind(agent.id).execute(store.pool()).await.unwrap();
    let current = store
        .claim_wake(claimant, TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert!(current.revision > old.revision);
    store.settle_wake(&old, None).await.unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM gesang_wakes WHERE agent_id=$1 AND revision=$2")
            .bind(agent.id)
            .bind(current.revision)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(remaining, 1);
    store.settle_wake(&current, None).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn anchor_reconciles_prepared_commits_without_accepting_old_or_unexplained_history() {
    use gesang_core::{AnchorHead, PendingAnchor};
    let store = store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let agent = store
        .ensure_agent(&owner, "execution-v1", "local")
        .await
        .unwrap();
    let claim = store
        .claim_runtime(agent.id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    let runtime = store
        .admit_ready_fixture(
            &claim,
            &Admission {
                runtime_id: Uuid::new_v4(),
                locator: "protected-writer".into(),
                engine_owner: "local".into(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await
        .unwrap();
    let genesis = store.initialize_anchor(&runtime, &[0; 32]).await.unwrap();
    let prepared = PendingAnchor {
        operation_id: Uuid::new_v4(),
        generation: runtime.generation,
        next: AnchorHead {
            sequence: 1,
            digest: vec![1; 32],
        },
    };
    store
        .prepare_anchor(&runtime, &genesis.head, &prepared)
        .await
        .unwrap();
    let reopened = Store::new(store.pool().clone(), PayloadKey::new([42; 32]));
    assert_eq!(
        reopened.anchor(&runtime).await.unwrap().pending.as_ref(),
        Some(&prepared)
    );
    assert!(matches!(
        reopened
            .confirm_anchor(&runtime, &prepared, &genesis.head)
            .await,
        Err(Error::FreshnessMismatch)
    ));
    let confirmed = reopened
        .confirm_anchor(&runtime, &prepared, &prepared.next)
        .await
        .unwrap();
    assert_eq!(confirmed.head, prepared.next);
    assert!(confirmed.pending.is_none());
    assert_eq!(
        reopened
            .confirm_anchor(&runtime, &prepared, &prepared.next)
            .await
            .unwrap(),
        confirmed
    );
    assert!(matches!(
        reopened
            .prepare_anchor(&runtime, &genesis.head, &prepared)
            .await,
        Err(Error::FreshnessMismatch)
    ));
    let pending = PendingAnchor {
        operation_id: Uuid::new_v4(),
        generation: runtime.generation,
        next: AnchorHead {
            sequence: 2,
            digest: vec![2; 32],
        },
    };
    reopened
        .prepare_anchor(&runtime, &confirmed.head, &pending)
        .await
        .unwrap();
    assert!(matches!(
        reopened
            .abort_uncommitted_anchor(&runtime, &pending, &pending.next)
            .await,
        Err(Error::FreshnessMismatch)
    ));
    assert_eq!(
        reopened
            .abort_uncommitted_anchor(&runtime, &pending, &confirmed.head)
            .await
            .unwrap(),
        confirmed
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn version_pins_survive_reopening_and_reject_conflicting_content() {
    use gesang_plugins::{PinError, PinStore, PinSubject, VersionPin};
    let store = store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let pin = VersionPin {
        subject: PinSubject::Owner { owner_id: owner.id },
        reference_id: Uuid::new_v4(),
        package: "package-a".into(),
        version: "release-a".into(),
        digest: "digest-a".into(),
    };
    let (left, right) = tokio::join!(store.pin(pin.clone()), store.pin(pin.clone()));
    assert_eq!(left.unwrap(), right.unwrap());
    let reopened = Store::new(store.pool().clone(), PayloadKey::new([42; 32]));
    assert_eq!(
        reopened
            .get(&pin.subject, pin.reference_id, &pin.package)
            .await
            .unwrap(),
        Some(pin.clone())
    );
    let changed = VersionPin {
        digest: "different-content".into(),
        ..pin.clone()
    };
    assert!(matches!(
        reopened.pin(changed).await,
        Err(PinError::Conflict { .. })
    ));
    assert_eq!(
        reopened.list(&pin.subject, pin.reference_id).await.unwrap(),
        vec![pin.clone()]
    );
    reopened
        .unpin(&pin.subject, pin.reference_id, &pin.package)
        .await
        .unwrap();
    assert!(
        reopened
            .get(&pin.subject, pin.reference_id, &pin.package)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn every_acknowledged_mutation_forces_sync_commit_despite_unsafe_session_defaults() {
    let seeded = store().await;
    sqlx::raw_sql("CREATE FUNCTION require_sync_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF current_setting('synchronous_commit') NOT IN ('on','remote_apply') THEN RAISE EXCEPTION 'mutation attempted without synchronous commit'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_owners FOR EACH ROW EXECUTE FUNCTION require_sync_commit();
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_agents FOR EACH ROW EXECUTE FUNCTION require_sync_commit();
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_grants FOR EACH ROW EXECUTE FUNCTION require_sync_commit();
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_commands FOR EACH ROW EXECUTE FUNCTION require_sync_commit();
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_wakes FOR EACH ROW EXECUTE FUNCTION require_sync_commit();
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_anchors FOR EACH ROW EXECUTE FUNCTION require_sync_commit();
        CREATE TRIGGER require_sync BEFORE INSERT OR UPDATE ON gesang_package_pins FOR EACH ROW EXECUTE FUNCTION require_sync_commit();")
        .execute(seeded.pool()).await.unwrap();
    let pool = PgPoolOptions::new()
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit='off'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect_with(seeded.pool().connect_options().as_ref().clone())
        .await
        .unwrap();
    let store = Store::new(pool, PayloadKey::new([42; 32]));
    assert!(matches!(
        store.validate_durability().await,
        Err(Error::DurabilityRequired)
    ));
    let identity = VerifiedIdentity {
        issuer: "issuer".into(),
        subject: "person".into(),
    };
    let owner = store.bind_owner(&identity).await.unwrap();
    let agent = store
        .ensure_agent(&owner, "execution-v1", "local")
        .await
        .unwrap();
    store
        .bind_placement(
            &owner,
            agent.id,
            &["deployment/persistent-volume".into()],
            "preserved-volume",
        )
        .await
        .unwrap();
    let grant = store
        .select_grant(
            &owner,
            agent.id,
            &GrantSelection {
                mode: PermissionMode::FullAccess,
                scopes: vec![],
            },
        )
        .await
        .unwrap();
    let submission = CommandSubmission {
        request_id: Uuid::new_v4(),
        authorization: CommandAuthorization::Grant {
            id: grant.id,
            version: grant.version,
        },
        command: json!({"kind":"goal.submit","objective":"durability"}),
    };
    store
        .enqueue(&owner, agent.id, &submission, &execution_requirements())
        .await
        .unwrap();
    let pending = store
        .claim_command(Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    store
        .accept_command(
            &pending,
            &json!({"requestId":pending.request_id,"goalId":"goal"}),
        )
        .await
        .unwrap();
    let wake = store
        .claim_wake(Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    store.settle_wake(&wake, Some(Utc::now())).await.unwrap();
    let claim = store
        .claim_runtime(agent.id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    let runtime = store
        .admit_ready_fixture(
            &claim,
            &Admission {
                runtime_id: Uuid::new_v4(),
                locator: "protected-instance".into(),
                engine_owner: "local".into(),
                previous_writer: PreviousWriter::Absent,
            },
        )
        .await
        .unwrap();
    store.initialize_anchor(&runtime, &[0; 32]).await.unwrap();
    store
        .revoke_grant(&owner, agent.id, grant.id)
        .await
        .unwrap();
    let direct = sqlx::query(
        "INSERT INTO gesang_owners(id,issuer,subject) VALUES($1,'unchecked','unchecked')",
    )
    .bind(Uuid::new_v4())
    .execute(store.pool())
    .await;
    assert!(direct.is_err());
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn stale_controller_cannot_admit_without_independent_fencing_evidence() {
    let store = store().await;
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let agent = store
        .ensure_agent(&owner, "execution-v1", "local")
        .await
        .unwrap();
    let first = store
        .claim_runtime(agent.id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    store.verify_runtime_claim(&first).await.unwrap();
    let mut forged = first.clone();
    forged.engine_owner = "another".into();
    assert!(matches!(
        store.verify_runtime_claim(&forged).await,
        Err(Error::StaleGeneration)
    ));
    let mut expired = first.clone();
    expired.expires_at = Utc::now() - TimeDelta::seconds(1);
    assert!(matches!(
        store.verify_runtime_claim(&expired).await,
        Err(Error::StaleGeneration)
    ));
    let admission = Admission {
        runtime_id: Uuid::new_v4(),
        locator: "protected-runtime-a".into(),
        engine_owner: "local".into(),
        previous_writer: PreviousWriter::Absent,
    };
    let runtime = store.admit_ready_fixture(&first, &admission).await.unwrap();
    for forged in [
        gesang_core::Runtime {
            locator: "another-service".into(),
            ..runtime.clone()
        },
        gesang_core::Runtime {
            engine_owner: "another-owner".into(),
            ..runtime.clone()
        },
    ] {
        assert!(matches!(
            store.verify_generation(&forged).await,
            Err(Error::StaleGeneration)
        ));
        assert!(matches!(
            store.initialize_anchor(&forged, &[0; 32]).await,
            Err(Error::StaleGeneration)
        ));
    }
    assert!(matches!(
        store
            .claim_runtime(agent.id, Uuid::new_v4(), TimeDelta::minutes(1))
            .await,
        Err(Error::StaleGeneration)
    ));
    sqlx::query(
        "UPDATE gesang_agents SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(agent.id)
    .execute(store.pool())
    .await
    .unwrap();
    let next = store
        .claim_runtime(agent.id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    assert!(next.generation > first.generation);
    assert!(matches!(
        store.verify_runtime_claim(&first).await,
        Err(Error::StaleGeneration)
    ));
    store.verify_runtime_claim(&next).await.unwrap();
    assert!(matches!(
        store.verify_generation(&runtime).await,
        Err(Error::StaleGeneration)
    ));
    assert!(matches!(
        store.admit_ready_fixture(&next, &admission).await,
        Err(Error::FencingRequired)
    ));
    let next_admission = Admission {
        runtime_id: Uuid::new_v4(),
        previous_writer: PreviousWriter::Terminated(runtime.id),
        ..admission
    };
    assert_eq!(
        store
            .admit_ready_fixture(&next, &next_admission)
            .await
            .unwrap()
            .generation,
        next.generation
    );
    assert!(matches!(
        store.admit_ready_fixture(&first, &next_admission).await,
        Err(Error::FencingRequired) | Err(Error::StaleGeneration)
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn overlapping_physical_workspaces_cannot_be_bound_across_owners() {
    let store = store().await;
    let first_owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer-a".into(),
            subject: "same-email".into(),
        })
        .await
        .unwrap();
    let second_owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer-b".into(),
            subject: "same-email".into(),
        })
        .await
        .unwrap();
    let first = store
        .ensure_agent(&first_owner, "execution-v1", "local")
        .await
        .unwrap();
    let second = store
        .ensure_agent(&second_owner, "execution-v1", "local")
        .await
        .unwrap();
    let claim = store
        .claim_runtime(first.id, Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap();
    let first_keys = vec!["deployment/pvc-a".into(), "deployment/pvc-b".into()];
    store
        .bind_placement(&first_owner, first.id, &first_keys, "workspace-a")
        .await
        .unwrap();
    let binding = store.agent_binding(first.id).await.unwrap();
    assert_eq!(binding.agent, first);
    assert_eq!(binding.engine_owner, "local");
    assert_eq!(binding.workspace_keys, first_keys);
    assert!(matches!(
        store.verify_runtime_claim(&claim).await,
        Err(Error::StaleGeneration)
    ));
    assert!(matches!(
        store
            .admit_ready_fixture(
                &claim,
                &Admission {
                    runtime_id: Uuid::new_v4(),
                    locator: "stale-placement".into(),
                    engine_owner: "local".into(),
                    previous_writer: PreviousWriter::Absent,
                }
            )
            .await,
        Err(Error::StaleGeneration)
    ));
    let reversed = vec![first_keys[1].clone(), first_keys[0].clone()];
    store
        .bind_placement(&first_owner, first.id, &reversed, "workspace-a")
        .await
        .unwrap();
    assert!(matches!(
        store
            .bind_placement(&first_owner, first.id, &first_keys, "changed-locator")
            .await,
        Err(Error::VersionConflict)
    ));
    let overlap = vec!["deployment/pvc-0".into(), "deployment/pvc-b".into()];
    assert!(matches!(
        store
            .bind_placement(&second_owner, second.id, &overlap, "different-json")
            .await,
        Err(Error::VersionConflict)
    ));
    let unaffected = store.agent_binding(second.id).await.unwrap();
    assert!(unaffected.workspace_keys.is_empty());
    assert!(unaffected.placement.is_none());
    let other_keys = vec!["deployment/pvc-0".into(), "deployment/pvc-d".into()];
    store
        .bind_placement(&second_owner, second.id, &other_keys, "workspace-b")
        .await
        .unwrap();
    assert_eq!(
        store.agent_binding(second.id).await.unwrap().workspace_keys,
        other_keys
    );
    assert!(matches!(
        store
            .bind_placement(&second_owner, first.id, &first_keys, "workspace-a")
            .await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        store
            .bind_placement(&first_owner, first.id, &other_keys, "workspace-a")
            .await,
        Err(Error::VersionConflict)
    ));
    let mut current = store
        .claim_runtime(first.id, claim.controller_id, TimeDelta::minutes(1))
        .await
        .unwrap();
    store.verify_runtime_claim(&current).await.unwrap();
    current.workspace_keys.clear();
    assert!(matches!(
        store.verify_runtime_claim(&current).await,
        Err(Error::StaleGeneration)
    ));
}

#[tokio::test]
#[ignore = "requires a real isolated transactional database"]
async fn concurrent_new_work_cannot_be_erased_by_an_old_wake_receipt() {
    let store = Arc::new(store().await);
    let owner = store
        .bind_owner(&VerifiedIdentity {
            issuer: "issuer".into(),
            subject: "person".into(),
        })
        .await
        .unwrap();
    let agent = store
        .ensure_agent(&owner, "execution-v1", "local")
        .await
        .unwrap();
    store.schedule(agent.id, Utc::now()).await.unwrap();
    let claim = store
        .claim_wake(Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    store.schedule(agent.id, Utc::now()).await.unwrap();
    store.settle_wake(&claim, None).await.unwrap();
    let current = sqlx::query("SELECT revision FROM gesang_wakes WHERE agent_id=$1")
        .bind(agent.id)
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert!(current.get::<i64, _>("revision") > claim.revision);
    let retry = store
        .claim_wake(Uuid::new_v4(), TimeDelta::minutes(1))
        .await
        .unwrap()
        .unwrap();
    store.settle_wake(&retry, None).await.unwrap();
    assert!(
        store
            .claim_wake(Uuid::new_v4(), TimeDelta::minutes(1))
            .await
            .unwrap()
            .is_none()
    );
}
