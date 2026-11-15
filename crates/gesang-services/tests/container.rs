//! Actual local allocation, process start, identity checks, and recorded compute removal.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(unix)]

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::Duration,
};

use gesang_plugins::{BindingScope, InstallationId, InstalledVersion, ServiceEndpoint};
use gesang_services::{
    ContainerConnection, ContainerProfile, ContainerRuntime, ContainerState, Error, Termination,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

#[derive(Deserialize)]
struct Fixture {
    connection: ContainerConnection,
    profile: ContainerProfile,
}

fn record(path: &std::path::Path, value: serde_json::Value) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    writeln!(file, "{}", value).unwrap();
    file.sync_all().unwrap();
}

#[tokio::test]
#[ignore = "requires the explicitly configured owned local container runtime"]
async fn lifecycle() {
    let config = PathBuf::from(
        std::env::var("GESANG_TEST_CONTAINER_CONFIG").expect("runtime fixture required"),
    );
    let fixture: Fixture = serde_json::from_slice(&fs::read(config).unwrap()).unwrap();
    let artifacts = PathBuf::from(
        std::env::var("GESANG_TEST_ARTIFACT_DIR").expect("evidence directory required"),
    );
    fs::create_dir_all(&artifacts).unwrap();
    let resource = Uuid::new_v4();
    let log = artifacts.join(format!("container-{resource}.jsonl"));
    let endpoint = ServiceEndpoint {
        provider: InstalledVersion {
            installation: InstallationId {
                binding: BindingScope::Deployment {
                    deployment_id: Uuid::new_v4(),
                },
                id: Uuid::new_v4(),
            },
            revision: 1,
        },
        service: "sandbox".into(),
    };
    record(
        &log,
        json!({"action":"allocate_intent","resource":resource,"endpoint":endpoint,"profile":fixture.profile}),
    );
    let runtime = ContainerRuntime::connect(fixture.connection.clone(), endpoint.clone())
        .await
        .unwrap();
    runtime.validate_profile(&fixture.profile).await.unwrap();
    let allocated = runtime.allocate(resource, &fixture.profile).await.unwrap();
    record(
        &log,
        json!({"action":"allocated","resource":resource,"physicalId":allocated.id}),
    );
    assert_eq!(allocated.state, ContainerState::Created);
    assert!(allocated.started_at.is_none());
    assert_eq!(
        runtime.find(resource, &fixture.profile).await.unwrap(),
        Some(allocated.clone())
    );
    record(
        &log,
        json!({"action":"start_intent","physicalId":allocated.id}),
    );
    let running = runtime
        .start(&allocated.id, resource, &fixture.profile)
        .await
        .unwrap();
    assert_eq!(running.state, ContainerState::Running);
    assert!(running.started_at.is_some());
    let foreign = Uuid::new_v4();
    assert!(matches!(
        runtime
            .terminate(&allocated.id, foreign, &fixture.profile)
            .await,
        Err(Error::ResourceIdentity)
    ));
    assert_eq!(
        runtime
            .observe(&allocated.id, resource, &fixture.profile)
            .await
            .unwrap()
            .unwrap()
            .state,
        ContainerState::Running
    );
    let mut wrong = fixture.connection.clone();
    wrong.engine_id = Uuid::new_v4().to_string();
    assert!(matches!(
        ContainerRuntime::connect(wrong, endpoint).await,
        Err(Error::ProviderIdentity)
    ));
    record(
        &log,
        json!({"action":"remove_compute_intent","physicalId":allocated.id,"deleteVolumes":false}),
    );
    assert_eq!(
        runtime
            .terminate(&allocated.id, resource, &fixture.profile)
            .await
            .unwrap(),
        Termination::Confirmed
    );
    record(&log, json!({"action":"removed","physicalId":allocated.id}));
    tokio::time::timeout(
        Duration::from_millis(fixture.connection.request_timeout_ms.get()),
        async {
            while runtime
                .terminated(&allocated.id, resource, &fixture.profile)
                .await
                .unwrap()
                == Termination::Unknown
            {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .unwrap();
    assert_eq!(
        runtime
            .terminated(&allocated.id, foreign, &fixture.profile)
            .await
            .unwrap(),
        Termination::Unknown
    );
    assert!(
        runtime
            .observe(&allocated.id, resource, &fixture.profile)
            .await
            .unwrap()
            .is_none()
    );
    record(
        &log,
        json!({"action":"verified","destructionEvent":true,"foreignIdentityRejected":true}),
    );
}
