use std::{sync::Arc, time::Duration};

use chopin_protocol::{StateObserver, StateUpdate};
use gesang_core::{Error, Result, Store};
use serde_json::Value;

use super::Connection;

/// A dedicated upstream observation invalidated by writer replacement or adapter retirement.
pub struct EngineObservation {
    store: Store,
    connection: Arc<Connection>,
    observer: Option<StateObserver>,
    timeout: Duration,
}

impl std::fmt::Debug for EngineObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineObservation")
            .field("runtime_id", &self.connection.runtime.id)
            .finish_non_exhaustive()
    }
}

impl EngineObservation {
    pub(super) fn new(
        store: Store,
        connection: Arc<Connection>,
        observer: StateObserver,
        timeout: Duration,
    ) -> Self {
        Self {
            store,
            connection,
            observer: Some(observer),
            timeout,
        }
    }

    pub(super) async fn verify(&self) -> Result<()> {
        if !*self.connection.active.borrow() {
            return Err(Error::StaleGeneration);
        }
        self.store
            .verify_execution(&self.connection.runtime)
            .await?;
        if !*self.connection.active.borrow() {
            return Err(Error::StaleGeneration);
        }
        Ok(())
    }

    /// Borrows the current producer-merged state after checking its writer identity.
    pub async fn state(&self) -> Result<&Value> {
        self.verify().await?;
        self.observer
            .as_ref()
            .ok_or_else(|| Error::Uncertain("engine observation is closed".into()))?
            .state()
            .map_err(|_| Error::Uncertain("engine observation is closed".into()))
    }

    /// Waits for an actual producer change; cancellation requires a fresh observation and snapshot.
    pub async fn next(&mut self) -> Result<StateUpdate> {
        self.verify().await?;
        let mut observer = self
            .observer
            .take()
            .ok_or_else(|| Error::Uncertain("engine observation is closed".into()))?;
        let mut active = self.connection.active.subscribe();
        if !*active.borrow() {
            return Err(Error::StaleGeneration);
        }
        let update = tokio::select! {
            biased;
            _ = active.changed() => return Err(Error::StaleGeneration),
            update = observer.next() => update.map_err(|_| Error::Uncertain("engine observation must be reopened".into()))?,
        };
        self.verify().await?;
        self.observer = Some(observer);
        Ok(update)
    }

    /// Releases the upstream state reference and its dedicated connection.
    pub async fn close(mut self) -> Result<()> {
        if let Some(observer) = self.observer.take() {
            tokio::time::timeout(self.timeout, observer.close())
                .await
                .map_err(|_| Error::Uncertain("engine observation close deadline exceeded".into()))?
                .map_err(|_| Error::Uncertain("engine observation could not be closed".into()))?;
        }
        Ok(())
    }
}
