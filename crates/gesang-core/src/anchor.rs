use crate::CoordinationTransaction;
use crate::store::database::{Row, query, query_scalar, statement};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, Result, Runtime, Store};

/// A confirmed execution commit sequence and cryptographic content digest.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnchorHead {
    /// Monotonic local commit sequence, atomically stored with execution data.
    pub sequence: i64,
    /// Cryptographic digest binding the complete commit content and object references.
    pub digest: Vec<u8>,
}

/// One prepared commit whose local durable outcome must be reconciled before confirmation.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PendingAnchor {
    /// Stable commit operation identity, reused after an uncertain database response.
    pub operation_id: Uuid,
    /// Writer generation under which the operation was prepared.
    pub generation: i64,
    /// Exact intended successor sequence and digest.
    pub next: AnchorHead,
}

/// Trusted freshness state held outside execution-volume backups.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Anchor {
    /// Last commit whose durable confirmation could have been acknowledged to execution.
    pub head: AnchorHead,
    /// At most one prepared operation requiring local-state reconciliation.
    pub pending: Option<PendingAnchor>,
}

impl Store {
    /// Registers the genesis digest without replacing an existing confirmed history.
    pub async fn initialize_anchor(
        &self,
        runtime: &Runtime,
        genesis_digest: &[u8],
    ) -> Result<Anchor> {
        if genesis_digest.is_empty() {
            return Err(Error::FreshnessMismatch);
        }
        let mut tx = self.database.begin().await?;
        verify_writer(&mut tx.coordination(), runtime).await?;
        query(tx.executor(), "INSERT INTO gesang_anchors(agent_id,digest) VALUES($1,$2) ON CONFLICT(agent_id) DO NOTHING")
    .bind(runtime.agent_id)
    .bind(genesis_digest)
    .execute().await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_anchors WHERE agent_id=$1",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(runtime.agent_id)
        .fetch_one()
        .await?;
        let anchor = read_anchor(&row)?;
        if anchor.head.sequence == 0 && anchor.head.digest != genesis_digest {
            return Err(Error::FreshnessMismatch);
        }
        tx.commit().await?;
        Ok(anchor)
    }

    /// Reads trusted freshness only for the admitted current writer.
    pub async fn anchor(&self, runtime: &Runtime) -> Result<Anchor> {
        let mut tx = self.database.begin_read().await?;
        verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(
            tx.executor(),
            "SELECT * FROM gesang_anchors WHERE agent_id=$1",
        )
        .bind(runtime.agent_id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let anchor = read_anchor(&row)?;
        tx.commit().await?;
        Ok(anchor)
    }

    /// Durably prepares one exact successor under current writer admission and head equality.
    pub async fn prepare_anchor(
        &self,
        runtime: &Runtime,
        expected: &AnchorHead,
        pending: &PendingAnchor,
    ) -> Result<Anchor> {
        if pending.generation != runtime.generation
            || expected.sequence.checked_add(1) != Some(pending.next.sequence)
            || pending.next.digest.is_empty()
        {
            return Err(Error::FreshnessMismatch);
        }
        let mut tx = self.database.begin().await?;
        verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_anchors WHERE agent_id=$1",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(runtime.agent_id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let mut anchor = read_anchor(&row)?;
        if anchor.head != *expected {
            return Err(Error::FreshnessMismatch);
        }
        if let Some(existing) = &anchor.pending {
            if existing != pending {
                return Err(Error::VersionConflict);
            }
        } else {
            query(tx.executor(), "UPDATE gesang_anchors SET pending_id=$1,pending_generation=$2,pending_sequence=$3,pending_digest=$4 WHERE agent_id=$5")
    .bind(pending.operation_id)
    .bind(pending.generation)
    .bind(pending.next.sequence)
    .bind(&pending.next.digest)
    .bind(runtime.agent_id)
    .execute().await?;
            anchor.pending = Some(pending.clone());
        }
        tx.commit().await?;
        Ok(anchor)
    }

    /// Confirms only the matching prepared operation after trusted storage verified local durability.
    pub async fn confirm_anchor(
        &self,
        runtime: &Runtime,
        pending: &PendingAnchor,
        local: &AnchorHead,
    ) -> Result<Anchor> {
        if local != &pending.next {
            return Err(Error::FreshnessMismatch);
        }
        let mut tx = self.database.begin().await?;
        verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_anchors WHERE agent_id=$1",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(runtime.agent_id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let anchor = read_anchor(&row)?;
        if anchor.pending.is_none() && anchor.head == *local {
            tx.commit().await?;
            return Ok(anchor);
        }
        if anchor.pending.as_ref() != Some(pending) {
            return Err(Error::FreshnessMismatch);
        }
        query(tx.executor(), "UPDATE gesang_anchors SET sequence=$1,digest=$2,pending_id=NULL,pending_generation=NULL,pending_sequence=NULL,pending_digest=NULL WHERE agent_id=$3")
    .bind(local.sequence)
    .bind(&local.digest)
    .bind(runtime.agent_id)
    .execute().await?;
        tx.commit().await?;
        Ok(Anchor {
            head: local.clone(),
            pending: None,
        })
    }

    /// Aborts a prepared operation only after trusted storage verified that local state stayed old.
    pub async fn abort_uncommitted_anchor(
        &self,
        runtime: &Runtime,
        pending: &PendingAnchor,
        verified_local: &AnchorHead,
    ) -> Result<Anchor> {
        let mut tx = self.database.begin().await?;
        verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_anchors WHERE agent_id=$1",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(runtime.agent_id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let mut anchor = read_anchor(&row)?;
        if anchor.pending.as_ref() != Some(pending) || anchor.head != *verified_local {
            return Err(Error::FreshnessMismatch);
        }
        query(tx.executor(), "UPDATE gesang_anchors SET pending_id=NULL,pending_generation=NULL,pending_sequence=NULL,pending_digest=NULL WHERE agent_id=$1")
    .bind(runtime.agent_id)
    .execute().await?;
        anchor.pending = None;
        tx.commit().await?;
        Ok(anchor)
    }
}

pub(crate) async fn verify_writer(
    tx: &mut CoordinationTransaction<'_, '_>,
    runtime: &Runtime,
) -> Result<()> {
    let id=query_scalar::<Uuid>(tx.executor(), statement!("SELECT a.id FROM gesang_agents a JOIN gesang_instance_intents i ON i.id=a.runtime_id AND i.agent_id=a.id AND i.generation=a.generation AND i.locator=a.runtime_locator WHERE a.id=$1 AND a.runtime_id=$2 AND a.generation=$3 AND a.runtime_generation=$3 AND a.engine_owner=$4 AND a.runtime_locator=$5 AND a.lease_until>", clock, " AND i.phase NOT IN ('allocating','stopped')", [" FOR SHARE OF a,i", ""]))
    .bind(runtime.agent_id)
    .bind(runtime.id)
    .bind(runtime.generation)
    .bind(&runtime.engine_owner)
    .bind(&runtime.locator)
    .fetch_optional().await?;
    id.ok_or(Error::StaleGeneration)?;
    Ok(())
}

pub(crate) async fn verify_execution(
    tx: &mut CoordinationTransaction<'_, '_>,
    runtime: &Runtime,
) -> Result<()> {
    verify_writer(tx, runtime).await?;
    let row = query(
        tx.executor(),
        "SELECT plan->>'purpose' AS purpose,phase FROM gesang_instance_intents WHERE id=$1",
    )
    .bind(runtime.id)
    .fetch_one()
    .await?;
    if row.try_get::<&str, _>("purpose")? != "execution" {
        return Err(Error::Forbidden);
    }
    if row.try_get::<&str, _>("phase")? != "ready" {
        return Err(Error::RuntimeNotReady);
    }
    Ok(())
}

fn read_anchor(row: &Row) -> Result<Anchor> {
    let head = AnchorHead {
        sequence: row.try_get("sequence")?,
        digest: row.try_get("digest")?,
    };
    let pending = row
        .try_get::<Option<Uuid>, _>("pending_id")?
        .map(|operation_id| -> Result<_> {
            Ok(PendingAnchor {
                operation_id,
                generation: row.try_get("pending_generation")?,
                next: AnchorHead {
                    sequence: row.try_get("pending_sequence")?,
                    digest: row.try_get("pending_digest")?,
                },
            })
        })
        .transpose()?;
    Ok(Anchor { head, pending })
}
