use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    AnchorHead, Custody, CustodyReceipt, Error, Initialization, InitializationKeyLease,
    InitializationRequest, InitializationState, Owner, Result, Runtime, ServiceKey,
};

impl Custody {
    /// Persists a first-store key before provisioning; only an entirely unbound agent may begin.
    pub async fn begin_initialization(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        request: &InitializationRequest,
    ) -> Result<Initialization> {
        if request.operation_id.is_nil() {
            return Err(Error::InvalidCommand(
                "initialization operation is required".into(),
            ));
        }
        let mut tx = self.store.begin().await?;
        let agent = super::lock_agent(&mut tx, owner, agent_id).await?;
        if let Some(row) = record(&mut tx, agent_id).await? {
            let original = read_record(&mut tx, &row).await?;
            if original.operation_id != request.operation_id
                || original.plan_digest != request.plan_digest
            {
                return Err(Error::RequestConflict);
            }
            tx.commit().await?;
            return Ok(original);
        }
        let binding = crate::store::read_binding(&mut tx, &agent).await?;
        let prior: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM gesang_custody WHERE agent_id=$1) OR EXISTS(SELECT 1 FROM gesang_instance_intents WHERE agent_id=$1) OR EXISTS(SELECT 1 FROM gesang_anchors WHERE agent_id=$1)")
            .bind(agent_id).fetch_one(&mut *tx).await?;
        if binding.placement.is_some()
            || !binding.workspace_keys.is_empty()
            || agent.try_get::<i64, _>("generation")? != 0
            || crate::store::read_runtime(&agent)?.is_some()
            || prior
        {
            return Err(Error::Forbidden);
        }
        let mut bytes = Zeroizing::new([0; 32]);
        getrandom::fill(bytes.as_mut()).map_err(|_| Error::Randomness)?;
        let key = ServiceKey::new(*bytes);
        let sealed = self.key.seal(
            &initialization_aad(owner.id, agent_id, &binding.engine_owner, request)?,
            &key,
        )?;
        sqlx::query("INSERT INTO gesang_custody(agent_id) VALUES($1)")
            .bind(agent_id)
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query("INSERT INTO gesang_initializations(agent_id,operation_id,owner_id,engine_owner,plan_digest,sealed_key) VALUES($1,$2,$3,$4,$5,$6) RETURNING *")
            .bind(agent_id).bind(request.operation_id).bind(owner.id).bind(&binding.engine_owner)
            .bind(request.plan_digest.as_slice()).bind(sealed).fetch_one(&mut *tx).await?;
        let initialization = read_record(&mut tx, &row).await?;
        tx.commit().await?;
        Ok(initialization)
    }

    /// Reads the original first-store operation without exposing its pending key or admitting effects.
    pub async fn initialization(
        &self,
        owner: &Owner,
        agent_id: Uuid,
    ) -> Result<Option<Initialization>> {
        self.store.agent(owner, agent_id).await?;
        let mut tx = self.store.pool().begin().await?;
        let result = match record(&mut tx, agent_id).await? {
            Some(row) => Some(read_record(&mut tx, &row).await?),
            None => None,
        };
        tx.commit().await?;
        Ok(result)
    }

    /// Releases the original key only to a current admitted initializer of the owner's bound resources.
    ///
    /// Completion permits only the same admitted initializer to reconcile its existing store.
    /// The original custody revision must remain active. The host verifies resource identity.
    pub async fn release_initialization_key(
        &self,
        owner: &Owner,
        runtime: &Runtime,
        operation_id: Uuid,
    ) -> Result<InitializationKeyLease> {
        let mut tx = self.store.begin().await?;
        let row = initializer_key(&mut tx, owner, runtime, operation_id, true).await?;
        let request = request(&row)?;
        let key = self.key.open(
            &initialization_aad(owner.id, runtime.agent_id, &runtime.engine_owner, &request)?,
            &row.try_get::<Vec<u8>, _>("sealed_key")?,
        )?;
        sqlx::query("INSERT INTO gesang_initialization_releases(agent_id,operation_id,runtime_id,generation) VALUES($1,$2,$3,$4)")
            .bind(runtime.agent_id).bind(operation_id).bind(runtime.id).bind(runtime.generation)
            .execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(InitializationKeyLease { operation_id, key })
    }

    /// Publishes ordinary custody only after the exact initializer closed at its confirmed storage head.
    ///
    /// Completion is idempotent and never grants execution or proves that the initializer has stopped.
    pub async fn finish_initialization(
        &self,
        owner: &Owner,
        runtime: &Runtime,
        operation_id: Uuid,
        head: &AnchorHead,
    ) -> Result<CustodyReceipt> {
        let mut tx = self.store.begin().await?;
        super::lock_agent(&mut tx, owner, runtime.agent_id).await?;
        let recorded = record(&mut tx, runtime.agent_id)
            .await?
            .ok_or(Error::NotFound)?;
        let original = read_record(&mut tx, &recorded).await?;
        if original.operation_id != operation_id {
            return Err(Error::RequestConflict);
        }
        if let InitializationState::Completed {
            runtime: previous,
            head: previous_head,
            receipt,
        } = original.state
        {
            if previous != *runtime || previous_head != *head {
                return Err(Error::RequestConflict);
            }
            tx.commit().await?;
            return Ok(receipt);
        }
        let row = initializer_key(&mut tx, owner, runtime, operation_id, false).await?;
        let released: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM gesang_initialization_releases WHERE agent_id=$1 AND operation_id=$2 AND runtime_id=$3 AND generation=$4)")
            .bind(runtime.agent_id).bind(operation_id).bind(runtime.id).bind(runtime.generation)
            .fetch_one(&mut *tx).await?;
        if !released || head.sequence < 0 || head.digest.len() != 32 {
            return Err(Error::FreshnessMismatch);
        }
        let anchor = sqlx::query(
            "SELECT sequence,digest,pending_id FROM gesang_anchors WHERE agent_id=$1 FOR SHARE",
        )
        .bind(runtime.agent_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::FreshnessMismatch)?;
        if anchor.try_get::<i64, _>("sequence")? != head.sequence
            || anchor.try_get::<Vec<u8>, _>("digest")? != head.digest
            || anchor.try_get::<Option<Uuid>, _>("pending_id")?.is_some()
        {
            return Err(Error::FreshnessMismatch);
        }
        let request = request(&row)?;
        let key = self.key.open(
            &initialization_aad(owner.id, runtime.agent_id, &runtime.engine_owner, &request)?,
            &row.try_get::<Vec<u8>, _>("sealed_key")?,
        )?;
        let revision = super::next_revision(row.try_get("custody_revision")?)?;
        let sealed = self.key.seal(
            &super::key_aad(owner.id, runtime.agent_id, &runtime.engine_owner, revision)?,
            &key,
        )?;
        let digest = self.key.digest(&[
            b"initialize",
            owner.id.as_bytes(),
            runtime.agent_id.as_bytes(),
            runtime.engine_owner.as_bytes(),
            operation_id.as_bytes(),
            &request.plan_digest,
        ])?;
        let receipt_row = sqlx::query("INSERT INTO gesang_custody_operations(agent_id,revision,operation_id,expected_revision,kind,owner_id,engine_owner,input_digest,sealed_key) VALUES($1,$2,$3,$4,'enrolled',$5,$6,$7,$8) RETURNING operation_id,revision,kind,created_at")
            .bind(runtime.agent_id).bind(revision).bind(operation_id).bind(revision - 1)
            .bind(owner.id).bind(&runtime.engine_owner).bind(digest).bind(sealed).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE gesang_custody SET revision=$2,active_version=$2 WHERE agent_id=$1")
            .bind(runtime.agent_id)
            .bind(revision)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE gesang_initializations SET state='completed',completed_runtime=$2,completed_head=$3,receipt_revision=$4 WHERE agent_id=$1")
            .bind(runtime.agent_id).bind(serde_json::to_value(runtime)?).bind(serde_json::to_value(head)?).bind(revision).execute(&mut *tx).await?;
        let receipt = super::receipt(&receipt_row)?;
        tx.commit().await?;
        Ok(receipt)
    }
}

pub(super) async fn pending<'e, E>(executor: E, agent_id: Uuid) -> Result<bool>
where
    E: sqlx::Executor<'e, Database = Postgres>,
{
    Ok(sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM gesang_initializations WHERE agent_id=$1 AND state='pending')",
    )
    .bind(agent_id)
    .fetch_one(executor)
    .await?)
}

async fn initializer_key(
    tx: &mut Transaction<'_, Postgres>,
    owner: &Owner,
    runtime: &Runtime,
    operation_id: Uuid,
    allow_completed: bool,
) -> Result<PgRow> {
    crate::anchor::verify_writer(tx, runtime).await?;
    sqlx::query("SELECT i.*,c.revision AS custody_revision FROM gesang_initializations i JOIN gesang_custody c ON c.agent_id=i.agent_id JOIN gesang_agents a ON a.id=i.agent_id JOIN gesang_instance_intents w ON w.id=a.runtime_id WHERE i.agent_id=$1 AND i.operation_id=$2 AND i.owner_id=$3 AND a.owner_id=i.owner_id AND a.engine_owner=i.engine_owner AND ((i.state='pending' AND c.revision=0 AND c.active_version IS NULL) OR ($4 AND i.state='completed' AND i.completed_runtime=$5 AND c.active_version=i.receipt_revision AND c.revision=i.receipt_revision)) AND a.legacy_reference IS NOT NULL AND EXISTS(SELECT 1 FROM gesang_workspace_resources r WHERE r.agent_id=a.id) AND w.plan->>'purpose'='initialization' AND w.phase='initializing' FOR SHARE OF i,c")
        .bind(runtime.agent_id).bind(operation_id).bind(owner.id).bind(allow_completed).bind(serde_json::to_value(runtime)?)
        .fetch_optional(&mut **tx).await?.ok_or(Error::Forbidden)
}

async fn record(tx: &mut Transaction<'_, Postgres>, agent_id: Uuid) -> Result<Option<PgRow>> {
    Ok(sqlx::query("SELECT agent_id,operation_id,plan_digest,state,completed_runtime,completed_head,receipt_revision,created_at FROM gesang_initializations WHERE agent_id=$1")
        .bind(agent_id).fetch_optional(&mut **tx).await?)
}

fn request(row: &PgRow) -> Result<InitializationRequest> {
    Ok(InitializationRequest {
        operation_id: row.try_get("operation_id")?,
        plan_digest: row
            .try_get::<Vec<u8>, _>("plan_digest")?
            .try_into()
            .map_err(|_| Error::InvalidCiphertext)?,
    })
}

async fn read_record(tx: &mut Transaction<'_, Postgres>, row: &PgRow) -> Result<Initialization> {
    let request = request(row)?;
    let agent_id = row.try_get("agent_id")?;
    let state = match row.try_get::<&str, _>("state")? {
        "pending" => InitializationState::Pending,
        "revoked" => InitializationState::Revoked,
        "completed" => {
            let receipt = sqlx::query("SELECT operation_id,revision,kind,created_at FROM gesang_custody_operations WHERE agent_id=$1 AND revision=$2 AND operation_id=$3")
                .bind(agent_id).bind(row.try_get::<i64, _>("receipt_revision")?).bind(request.operation_id)
                .fetch_one(&mut **tx).await?;
            InitializationState::Completed {
                runtime: serde_json::from_value(row.try_get("completed_runtime")?)?,
                head: serde_json::from_value(row.try_get("completed_head")?)?,
                receipt: super::receipt(&receipt)?,
            }
        }
        _ => return Err(Error::InvalidCiphertext),
    };
    Ok(Initialization {
        agent_id,
        operation_id: request.operation_id,
        plan_digest: request.plan_digest,
        state,
        created_at: row.try_get("created_at")?,
    })
}

fn initialization_aad(
    owner_id: Uuid,
    agent_id: Uuid,
    engine_owner: &str,
    request: &InitializationRequest,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&(
        "gesang:custody:initialization:v1",
        owner_id,
        agent_id,
        engine_owner,
        request.operation_id,
        request.plan_digest,
    ))?)
}
