use crate::CoordinationTransaction;
use crate::store::database::{Executor, Row, query, query_scalar, statement};
use chrono::TimeDelta;
use uuid::Uuid;

use super::{
    Custody, CustodyAction, CustodyEnrollment, CustodyReceipt, CustodyStatus, RuntimeRetirement,
    ServiceKeyLease, ServiceKeyVerifier,
};
use crate::{AgentBinding, Error, Owner, Result, Runtime, Store};

mod initialization;

impl Custody {
    /// Validates explicit owner enrollment before publishing a wrapped derived key.
    pub async fn enroll(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        enrollment: &CustodyEnrollment,
        verifier: &dyn ServiceKeyVerifier,
    ) -> Result<CustodyReceipt> {
        validate_revision(enrollment.expected_revision)?;
        self.store.agent(owner, agent_id).await?;
        let binding = self.store.agent_binding(agent_id).await?;
        if initialization::pending(self.store.database.executor(), agent_id).await? {
            return Err(Error::Forbidden);
        }
        if binding.placement.is_none() || binding.workspace_keys.is_empty() {
            return Err(Error::FencingRequired);
        }
        let digest = self.key.digest(&[
            b"enroll",
            owner.id.as_bytes(),
            agent_id.as_bytes(),
            binding.engine_owner.as_bytes(),
            enrollment.operation_id.as_bytes(),
            &enrollment.expected_revision.to_be_bytes(),
            enrollment.service_key.expose(),
        ])?;
        if let Some(row) = operation(
            self.store.database.executor(),
            agent_id,
            enrollment.operation_id,
        )
        .await?
        {
            return existing_receipt(&row, &digest);
        }
        verifier.verify(&binding, &enrollment.service_key).await?;
        let mut tx = self.store.database.begin().await?;
        let agent = lock_agent(&mut tx.coordination(), owner, agent_id).await?;
        if initialization::pending(tx.executor(), agent_id).await? {
            return Err(Error::Forbidden);
        }
        if crate::store::read_binding(&mut tx.coordination(), &agent).await? != binding {
            return Err(Error::StaleGeneration);
        }
        query(
            tx.executor(),
            "INSERT INTO gesang_custody(agent_id) VALUES($1) ON CONFLICT(agent_id) DO NOTHING",
        )
        .bind(agent_id)
        .execute()
        .await?;
        if let Some(row) = operation(tx.executor(), agent_id, enrollment.operation_id).await? {
            return existing_receipt(&row, &digest);
        }
        let state = state(&mut tx.coordination(), agent_id).await?;
        if state.try_get::<i64, _>("revision")? != enrollment.expected_revision {
            return Err(Error::VersionConflict);
        }
        if state.try_get::<Option<i64>, _>("active_version")?.is_some() {
            return Err(Error::InvalidCommand("custody is already enrolled; vault key rotation requires a separate verified operation".into()));
        }
        if retirement_pending(&mut tx.coordination(), agent_id).await? {
            return Err(Error::FencingRequired);
        }
        let revision = next_revision(enrollment.expected_revision)?;
        let sealed = self
            .key
            .seal(&aad(&binding, revision)?, &enrollment.service_key)?;
        let row = query(tx.executor(), "INSERT INTO gesang_custody_operations(agent_id,revision,operation_id,expected_revision,kind,owner_id,engine_owner,input_digest,sealed_key) VALUES($1,$2,$3,$4,'enrolled',$5,$6,$7,$8) RETURNING *")
    .bind(agent_id)
    .bind(revision)
    .bind(enrollment.operation_id)
    .bind(enrollment.expected_revision)
    .bind(owner.id)
    .bind(&binding.engine_owner)
    .bind(digest)
    .bind(sealed)
    .fetch_one().await?;
        query(
            tx.executor(),
            "UPDATE gesang_custody SET revision=$2,active_version=$2 WHERE agent_id=$1",
        )
        .bind(agent_id)
        .bind(revision)
        .execute()
        .await?;
        let receipt = receipt(&row)?;
        tx.commit().await?;
        Ok(receipt)
    }

    /// Reads current availability without exposing protected key material.
    pub async fn status(&self, owner: &Owner, agent_id: Uuid) -> Result<CustodyStatus> {
        self.store.agent(owner, agent_id).await?;
        let row = query(self.store.database.executor(), "SELECT c.revision,c.active_version,EXISTS(SELECT 1 FROM gesang_runtime_retirements r WHERE r.agent_id=a.id AND r.finished_at IS NULL) AS retirement_pending FROM gesang_agents a LEFT JOIN gesang_custody c ON c.agent_id=a.id WHERE a.id=$1 AND a.owner_id=$2")
    .bind(agent_id)
    .bind(owner.id)
    .fetch_one().await?;
        Ok(CustodyStatus {
            revision: row.try_get::<Option<i64>, _>("revision")?.unwrap_or(0),
            active_version: row.try_get("active_version")?,
            retirement_pending: row.try_get("retirement_pending")?,
        })
    }

    /// Reads the exact original custody operation without releasing key material or changing state.
    ///
    /// A receipt confirms that operation's durable result. It does not imply that its key is
    /// still active, a runtime is ready, or physical retirement has finished. A missing operation
    /// returns `None` only after authenticating ownership of the requested agent.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] for an unknown agent or a different owner. Database failures
    /// remain errors and must not be interpreted as an absent operation.
    pub async fn receipt(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        operation_id: Uuid,
    ) -> Result<Option<CustodyReceipt>> {
        self.store.agent(owner, agent_id).await?;
        query(self.store.database.executor(), "SELECT operation_id,revision,kind,created_at FROM gesang_custody_operations WHERE agent_id=$1 AND owner_id=$2 AND operation_id=$3")
    .bind(agent_id)
    .bind(owner.id)
    .bind(operation_id)
    .fetch_optional()
            .await?
            .map(|row| receipt(&row))
            .transpose()
    }

    /// Revokes future admissions and atomically records physical retirement before acknowledging.
    pub async fn revoke(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        operation_id: Uuid,
        expected_revision: i64,
    ) -> Result<CustodyReceipt> {
        validate_revision(expected_revision)?;
        let digest = self.key.digest(&[
            b"revoke",
            owner.id.as_bytes(),
            agent_id.as_bytes(),
            operation_id.as_bytes(),
            &expected_revision.to_be_bytes(),
        ])?;
        let mut tx = self.store.database.begin().await?;
        let agent = lock_agent(&mut tx.coordination(), owner, agent_id).await?;
        if let Some(row) = operation(tx.executor(), agent_id, operation_id).await? {
            return existing_receipt(&row, &digest);
        }
        let state = state(&mut tx.coordination(), agent_id).await?;
        if state.try_get::<i64, _>("revision")? != expected_revision {
            return Err(Error::VersionConflict);
        }
        if state.try_get::<Option<i64>, _>("active_version")?.is_none()
            && !initialization::pending(tx.executor(), agent_id).await?
        {
            return Err(Error::Forbidden);
        }
        let revision = next_revision(expected_revision)?;
        let previous = crate::store::read_runtime(&agent)?;
        let generation: i64 = agent.try_get("generation")?;
        let next_generation = generation.checked_add(1).ok_or(Error::VersionConflict)?;
        let engine_owner: String = agent.try_get("engine_owner")?;
        let row = query(tx.executor(), "INSERT INTO gesang_custody_operations(agent_id,revision,operation_id,expected_revision,kind,owner_id,engine_owner,input_digest) VALUES($1,$2,$3,$4,'revoked',$5,$6,$7) RETURNING *")
    .bind(agent_id)
    .bind(revision)
    .bind(operation_id)
    .bind(expected_revision)
    .bind(owner.id)
    .bind(engine_owner)
    .bind(digest)
    .fetch_one().await?;
        query(
            tx.executor(),
            "UPDATE gesang_custody SET revision=$2,active_version=NULL WHERE agent_id=$1",
        )
        .bind(agent_id)
        .bind(revision)
        .execute()
        .await?;
        query(tx.executor(), "UPDATE gesang_initializations SET state='revoked' WHERE agent_id=$1 AND state='pending'")
    .bind(agent_id)
    .execute().await?;
        query(tx.executor(), "UPDATE gesang_agents SET generation=$2,controller_id=NULL,lease_until=NULL WHERE id=$1")
    .bind(agent_id)
    .bind(next_generation)
    .execute().await?;
        query(tx.executor(), "INSERT INTO gesang_runtime_retirements(id,agent_id,generation,previous_runtime,custody_revision) VALUES($1,$2,$3,$4,$5)")
    .bind(Uuid::new_v4())
    .bind(agent_id)
    .bind(generation)
    .bind(previous.map(serde_json::to_value).transpose()?)
    .bind(revision)
    .execute().await?;
        let receipt = receipt(&row)?;
        tx.commit().await?;
        Ok(receipt)
    }

    /// Releases only the current owner's key to an exact, currently admitted runtime.
    pub async fn release(&self, owner_id: Uuid, runtime: &Runtime) -> Result<ServiceKeyLease> {
        let mut tx = self.store.database.begin().await?;
        crate::anchor::verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(tx.executor(), statement!("SELECT o.* FROM gesang_custody c JOIN gesang_custody_operations o ON o.agent_id=c.agent_id AND o.revision=c.active_version JOIN gesang_agents a ON a.id=c.agent_id WHERE c.agent_id=$1 AND a.owner_id=$2 AND o.owner_id=a.owner_id AND o.engine_owner=a.engine_owner AND o.kind='enrolled'", [" FOR SHARE OF c", ""]))
    .bind(runtime.agent_id)
    .bind(owner_id)
    .fetch_optional().await?.ok_or(Error::Forbidden)?;
        let revision: i64 = row.try_get("revision")?;
        let key = self.key.open(
            &key_aad(owner_id, runtime.agent_id, &runtime.engine_owner, revision)?,
            &row.try_get::<Vec<u8>, _>("sealed_key")?,
        )?;
        query(tx.executor(), "INSERT INTO gesang_custody_releases(agent_id,revision,runtime_id,generation) VALUES($1,$2,$3,$4)")
    .bind(runtime.agent_id)
    .bind(revision)
    .bind(runtime.id)
    .bind(runtime.generation)
    .execute().await?;
        tx.commit().await?;
        Ok(ServiceKeyLease { revision, key })
    }
}

impl Store {
    pub(crate) async fn claim_retirement(
        &self,
        claimant: Uuid,
        duration: TimeDelta,
    ) -> Result<Option<RuntimeRetirement>> {
        let millis = crate::store::positive_duration(duration)?;
        let mut tx = self.database.begin().await?;
        let row = query(tx.executor(), statement!("UPDATE gesang_runtime_retirements SET claimed_by=$1,claimed_until=", (lease 2), " WHERE id=(SELECT id FROM gesang_runtime_retirements WHERE finished_at IS NULL AND (claimed_until IS NULL OR claimed_until<=", clock, ") ORDER BY requested_at,id", [" FOR UPDATE SKIP LOCKED", ""], " LIMIT 1) RETURNING *"))
    .bind(claimant)
    .bind(millis)
    .fetch_optional().await?;
        tx.commit().await?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(RuntimeRetirement {
            id: row.try_get("id")?,
            binding: self.agent_binding(row.try_get("agent_id")?).await?,
            generation: row.try_get("generation")?,
            previous: row
                .try_get::<Option<serde_json::Value>, _>("previous_runtime")?
                .map(serde_json::from_value)
                .transpose()?,
            claimant,
            expires_at: row.timestamp("claimed_until")?,
        }))
    }

    /// Verifies the exact retirement attempt before a provider changes external resources.
    pub async fn verify_retirement(&self, retirement: &RuntimeRetirement) -> Result<()> {
        let row = query(self.database.executor(), statement!("SELECT * FROM gesang_runtime_retirements WHERE id=$1 AND agent_id=$2 AND generation=$3 AND claimed_by=$4 AND claimed_until=$5 AND claimed_until>", clock, " AND finished_at IS NULL"))
    .bind(retirement.id)
    .bind(retirement.binding.agent.id)
    .bind(retirement.generation)
    .bind(retirement.claimant)
    .bind_timestamp(retirement.expires_at)
    .fetch_optional().await?.ok_or(Error::StaleGeneration)?;
        let previous = row
            .try_get::<Option<serde_json::Value>, _>("previous_runtime")?
            .map(serde_json::from_value)
            .transpose()?;
        if retirement.previous != previous
            || self.agent_binding(retirement.binding.agent.id).await? != retirement.binding
        {
            return Err(Error::StaleGeneration);
        }
        Ok(())
    }

    pub(crate) async fn finish_retirement(&self, retirement: &RuntimeRetirement) -> Result<()> {
        let mut tx = self.database.begin().await?;
        let changed = query(tx.executor(), statement!("UPDATE gesang_runtime_retirements SET finished_at=", clock, ",claimed_by=NULL,claimed_until=NULL WHERE id=$1 AND claimed_by=$2 AND claimed_until=$3 AND claimed_until>", clock, " AND finished_at IS NULL"))
    .bind(retirement.id)
    .bind(retirement.claimant)
    .bind_timestamp(retirement.expires_at)
    .execute().await?;
        if changed != 1 {
            return Err(Error::VersionConflict);
        }
        tx.commit().await?;
        Ok(())
    }
}

async fn lock_agent(
    connection: &mut CoordinationTransaction<'_, '_>,
    owner: &Owner,
    agent_id: Uuid,
) -> Result<Row> {
    query(
        connection.executor(),
        statement!(
            "SELECT * FROM gesang_agents WHERE id=$1 AND owner_id=$2",
            [" FOR UPDATE", ""]
        ),
    )
    .bind(agent_id)
    .bind(owner.id)
    .fetch_optional()
    .await?
    .ok_or(Error::NotFound)
}

async fn state(connection: &mut CoordinationTransaction<'_, '_>, agent_id: Uuid) -> Result<Row> {
    query(
        connection.executor(),
        statement!(
            "SELECT * FROM gesang_custody WHERE agent_id=$1",
            [" FOR UPDATE", ""]
        ),
    )
    .bind(agent_id)
    .fetch_optional()
    .await?
    .ok_or(Error::NotFound)
}

async fn retirement_pending(
    connection: &mut CoordinationTransaction<'_, '_>,
    agent_id: Uuid,
) -> Result<bool> {
    Ok(query_scalar(connection.executor(), "SELECT EXISTS(SELECT 1 FROM gesang_runtime_retirements WHERE agent_id=$1 AND finished_at IS NULL)")
    .bind(agent_id)
    .fetch_one().await?)
}

async fn operation(
    executor: Executor<'_>,
    agent_id: Uuid,
    operation_id: Uuid,
) -> Result<Option<Row>> {
    Ok(query(
        executor,
        "SELECT * FROM gesang_custody_operations WHERE agent_id=$1 AND operation_id=$2",
    )
    .bind(agent_id)
    .bind(operation_id)
    .fetch_optional()
    .await?)
}

fn existing_receipt(row: &Row, digest: &[u8]) -> Result<CustodyReceipt> {
    if row.try_get::<Vec<u8>, _>("input_digest")? != digest {
        return Err(Error::RequestConflict);
    }
    receipt(row)
}

fn receipt(row: &Row) -> Result<CustodyReceipt> {
    Ok(CustodyReceipt {
        operation_id: row.try_get("operation_id")?,
        revision: row.try_get("revision")?,
        action: match row.try_get::<&str, _>("kind")? {
            "enrolled" => CustodyAction::Enrolled,
            "revoked" => CustodyAction::Revoked,
            _ => return Err(Error::InvalidCiphertext),
        },
        created_at: row.timestamp("created_at")?,
    })
}

fn aad(binding: &AgentBinding, revision: i64) -> Result<Vec<u8>> {
    key_aad(
        binding.agent.owner_id,
        binding.agent.id,
        &binding.engine_owner,
        revision,
    )
}

fn key_aad(owner_id: Uuid, agent_id: Uuid, engine_owner: &str, revision: i64) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        "gesang:custody:service-key:v1",
        owner_id,
        agent_id,
        engine_owner,
        revision,
    ))?)
}

fn validate_revision(revision: i64) -> Result<()> {
    if revision < 0 {
        return Err(Error::InvalidCommand(
            "custody revision cannot be negative".into(),
        ));
    }
    Ok(())
}

fn next_revision(revision: i64) -> Result<i64> {
    revision.checked_add(1).ok_or(Error::VersionConflict)
}
