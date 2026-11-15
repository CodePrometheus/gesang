//! Runs one installed sandbox provider independently of HTTP request lifetimes.

use std::{
    env, fs,
    num::{NonZeroU32, NonZeroU64},
    path::PathBuf,
    time::Duration,
};

use gesang_core::{
    ConfigurationAccess, ConfigurationKey, PayloadKey, PluginCatalog, Store, VerifiedIdentity,
};
use gesang_plugins::ServiceEndpoint;
use gesang_services::ContainerSandbox;
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use zeroize::Zeroize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Configuration {
    database_url: String,
    payload_key: [u8; 32],
    configuration_key: [u8; 32],
    endpoint: ServiceEndpoint,
    recovery_interval_ms: NonZeroU64,
    batch_size: NonZeroU32,
    ready_file: PathBuf,
}

impl Drop for Configuration {
    fn drop(&mut self) {
        self.database_url.zeroize();
        self.payload_key.zeroize();
        self.configuration_key.zeroize();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let path = arguments
        .next()
        .ok_or("a protected configuration file is required")?;
    if arguments.next().is_some() {
        return Err("unexpected argument".into());
    }
    let configuration: Configuration = serde_json::from_slice(&fs::read(path)?)?;
    let pool = PgPoolOptions::new()
        .connect(&configuration.database_url)
        .await?;
    let store = Store::new(pool, PayloadKey::new(configuration.payload_key));
    let catalog = PluginCatalog::new(
        store.clone(),
        ConfigurationKey::new(configuration.configuration_key)?,
    );
    let access = ConfigurationAccess::Administrator(VerifiedIdentity {
        issuer: "local-host".into(),
        subject: "sandbox-controller".into(),
    });
    let installed = catalog
        .load(&access, configuration.endpoint.provider)
        .await?;
    let sandbox =
        ContainerSandbox::open(store, &installed, &configuration.endpoint.service).await?;
    fs::write(&configuration.ready_file, std::process::id().to_string())?;
    let (shutdown, receiver) = watch::channel(false);
    let run = sandbox.run(
        Duration::from_millis(configuration.recovery_interval_ms.get()),
        configuration.batch_size,
        receiver,
    );
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => result?,
        result = tokio::signal::ctrl_c() => {
            result?;
            shutdown.send(true)?;
            run.await?;
        }
    }
    Ok(())
}
