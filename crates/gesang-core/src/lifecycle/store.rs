use crate::CoordinationTransaction;
use crate::store::database::{Row, query, query_scalar, statement};
use serde_json::json;
use uuid::Uuid;

use super::{
    CompletedMigration, InstanceIntent, InstancePhase, InstancePlan, InstancePurpose,
    InstanceRecord,
};
use crate::{Admission, AnchorHead, Error, PreviousWriter, Result, Runtime, RuntimeClaim, Store};

impl Store {
    /// Reads an original admitted allocation without renewing its lease or execution authority.
    ///
    /// Use the authoritative database and a read-committed transaction. Historical
    /// records permit reconciliation only; new effects still need current admission.
    pub async fn recorded_instance_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        agent_id: Uuid,
        runtime_id: Uuid,
    ) -> Result<InstanceRecord> {
        self.recorded_instance_coordination_in(&mut tx.into(), agent_id, runtime_id)
            .await
    }

    /// Runs the same check in the host's active coordination transaction.
    pub async fn recorded_instance_coordination_in(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        agent_id: Uuid,
        instance_id: Uuid,
    ) -> Result<InstanceRecord> {
        self.database.verify_transaction(tx).await?;
        let row = query(tx.executor(), statement!("SELECT a.*,i.id AS recorded_id,i.generation AS recorded_generation,i.locator AS recorded_locator,i.plan AS recorded_plan,i.phase AS recorded_phase FROM gesang_agents a JOIN gesang_instance_intents i ON i.agent_id=a.id WHERE a.id=$1 AND i.id=$2 AND i.locator IS NOT NULL", [" FOR SHARE OF a,i", ""]))
    .bind(agent_id)
    .bind(instance_id)
    .fetch_optional().await?.ok_or(Error::NotFound)?;
        let binding = crate::store::read_binding(tx, &row).await?;
        Ok(InstanceRecord {
            runtime: Runtime {
                id: row.try_get("recorded_id")?,
                agent_id,
                generation: row.try_get("recorded_generation")?,
                locator: row.try_get("recorded_locator")?,
                engine_owner: binding.engine_owner.clone(),
            },
            binding,
            plan: serde_json::from_value(row.try_get("recorded_plan")?)?,
            phase: InstancePhase::from_str(row.try_get("recorded_phase")?)?,
        })
    }

    /// Reads the most recent successful migration under the current ownership and resource claim.
    pub async fn latest_completed_migration(
        &self,
        claim: &RuntimeClaim,
    ) -> Result<Option<CompletedMigration>> {
        let mut tx = self.database.begin_read().await?;
        verify_claim(&mut tx.coordination(), claim).await?;
        let row = query(tx.executor(), "SELECT id,generation,locator,plan,migration_head FROM gesang_instance_intents WHERE agent_id=$1 AND generation<=$2 AND plan->>'purpose'='migration' AND phase='stopped' AND migration_head IS NOT NULL AND problem IS NULL ORDER BY generation DESC LIMIT 1")
    .bind(claim.agent.id)
    .bind(claim.generation)
    .fetch_optional().await?;
        let completed = row
            .map(|row| -> Result<CompletedMigration> {
                Ok(CompletedMigration {
                    runtime: Runtime {
                        id: row.try_get("id")?,
                        agent_id: claim.agent.id,
                        generation: row.try_get("generation")?,
                        locator: row.try_get("locator")?,
                        engine_owner: claim.engine_owner.clone(),
                    },
                    plan: serde_json::from_value(row.try_get("plan")?)?,
                    head: serde_json::from_value(row.try_get("migration_head")?)?,
                })
            })
            .transpose()?;
        tx.commit().await?;
        Ok(completed)
    }

    /// Reads a persisted attempt under the current controller claim without replanning it.
    pub async fn instance_intent(&self, claim: &RuntimeClaim) -> Result<Option<InstanceIntent>> {
        let mut tx = self.database.begin_read().await?;
        verify_claim(&mut tx.coordination(), claim).await?;
        let row = query(
            tx.executor(),
            "SELECT * FROM gesang_instance_intents WHERE agent_id=$1 AND generation=$2",
        )
        .bind(claim.agent.id)
        .bind(claim.generation)
        .fetch_optional()
        .await?;
        let intent = row
            .as_ref()
            .map(|row| read_intent(row, claim))
            .transpose()?;
        tx.commit().await?;
        Ok(intent)
    }

    /// Persists a stable runtime identity and immutable provider definition before allocation.
    pub async fn begin_instance(
        &self,
        claim: &RuntimeClaim,
        plan: &InstancePlan,
    ) -> Result<InstanceIntent> {
        plan.validate()?;
        let mut tx = self.database.begin().await?;
        let agent = verify_claim(&mut tx.coordination(), claim).await?;
        if crate::store::read_runtime(&agent)? != claim.previous {
            return Err(Error::StaleGeneration);
        }
        let row = query(tx.executor(), "INSERT INTO gesang_instance_intents(id,agent_id,generation,plan,phase) VALUES($1,$2,$3,$4,'allocating') ON CONFLICT(agent_id,generation) DO NOTHING RETURNING *")
    .bind(Uuid::new_v4())
    .bind(claim.agent.id)
    .bind(claim.generation)
    .bind(serde_json::to_value(plan)?)
    .fetch_optional().await?;
        let intent = match row {
            Some(row) => {
                let pending_initialization: bool = query_scalar(tx.executor(), "SELECT EXISTS(SELECT 1 FROM gesang_initializations WHERE agent_id=$1 AND state='pending')")
    .bind(claim.agent.id)
    .fetch_one().await?;
                if pending_initialization != (plan.purpose == InstancePurpose::Initialization) {
                    return Err(Error::Forbidden);
                }
                let intent = read_intent(&row, claim)?;
                event(&mut tx.coordination(), &intent, json!({})).await?;
                intent
            }
            None => {
                let row = query(
                    tx.executor(),
                    "SELECT * FROM gesang_instance_intents WHERE agent_id=$1 AND generation=$2",
                )
                .bind(claim.agent.id)
                .bind(claim.generation)
                .fetch_one()
                .await?;
                let intent = read_intent(&row, claim)?;
                if intent.plan != *plan {
                    return Err(Error::RequestConflict);
                }
                intent
            }
        };
        crate::store::schedule(&mut tx.coordination(), claim.agent.id, None).await?;
        tx.commit().await?;
        Ok(intent)
    }

    /// Checks an exact persisted preparation attempt before a provider changes external resources.
    pub async fn verify_instance_intent(&self, intent: &InstanceIntent) -> Result<()> {
        let mut tx = self.database.begin_read().await?;
        let agent = verify_claim(&mut tx.coordination(), &intent.claim).await?;
        check_intent(&mut tx.coordination(), intent).await?;
        let expected = intent.runtime.as_ref().or(intent.claim.previous.as_ref());
        if crate::store::read_runtime(&agent)?.as_ref() != expected {
            return Err(Error::StaleGeneration);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Admits an actual gated allocation before the provider releases its startup barrier.
    pub async fn admit_instance(
        &self,
        intent: &InstanceIntent,
        admission: &Admission,
    ) -> Result<InstanceIntent> {
        if intent.phase != InstancePhase::Allocating
            || intent.id != admission.runtime_id
            || admission.locator.is_empty()
            || admission.engine_owner != intent.claim.engine_owner
        {
            return Err(Error::FencingRequired);
        }
        match (&intent.claim.previous, &admission.previous_writer) {
            (None, PreviousWriter::Absent) => {}
            (
                Some(previous),
                PreviousWriter::Terminated(id) | PreviousWriter::StorageRevoked(id),
            ) if previous.id == *id && previous.id != intent.id => {}
            _ => return Err(Error::FencingRequired),
        }
        let mut tx = self.database.begin().await?;
        let agent = verify_claim(&mut tx.coordination(), &intent.claim).await?;
        check_intent(&mut tx.coordination(), intent).await?;
        if crate::store::read_runtime(&agent)? != intent.claim.previous {
            return Err(Error::StaleGeneration);
        }
        query(tx.executor(), "UPDATE gesang_agents SET runtime_id=$2,runtime_generation=$3,runtime_locator=$4 WHERE id=$1")
    .bind(intent.claim.agent.id)
    .bind(intent.id)
    .bind(intent.claim.generation)
    .bind(&admission.locator)
    .execute().await?;
        let phase = match intent.plan.purpose {
            InstancePurpose::Execution => InstancePhase::Starting,
            InstancePurpose::Initialization => InstancePhase::Initializing,
            InstancePurpose::Migration => InstancePhase::Migrating,
        };
        let row = query(tx.executor(), statement!("UPDATE gesang_instance_intents SET phase=$2,locator=$3,revision=revision+1,updated_at=", clock, " WHERE id=$1 RETURNING *"))
    .bind(intent.id)
    .bind(phase.as_str())
    .bind(&admission.locator)
    .fetch_one().await?;
        let updated = read_intent(&row, &intent.claim)?;
        event(
            &mut tx.coordination(),
            &updated,
            json!({"writerAdmitted":true}),
        )
        .await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Advances one allowed preparation boundary; terminal states require independent stop evidence.
    pub async fn transition_instance(
        &self,
        intent: &InstanceIntent,
        next: InstancePhase,
    ) -> Result<InstanceIntent> {
        let allowed = matches!(
            (intent.plan.purpose, intent.phase, next),
            (
                InstancePurpose::Execution,
                InstancePhase::Starting,
                InstancePhase::Unlocking
            ) | (
                InstancePurpose::Execution,
                InstancePhase::Unlocking,
                InstancePhase::Ready
            ) | (
                _,
                InstancePhase::Starting
                    | InstancePhase::Unlocking
                    | InstancePhase::Initializing
                    | InstancePhase::Migrating
                    | InstancePhase::Ready,
                InstancePhase::Draining
            )
        );
        if !allowed {
            return Err(Error::InvalidCommand(
                "invalid instance lifecycle transition".into(),
            ));
        }
        let mut tx = self.database.begin().await?;
        verify_claim(&mut tx.coordination(), &intent.claim).await?;
        check_intent(&mut tx.coordination(), intent).await?;
        let runtime = intent.runtime.as_ref().ok_or(Error::FencingRequired)?;
        crate::anchor::verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(
            tx.executor(),
            statement!(
                "UPDATE gesang_instance_intents SET phase=$2,revision=revision+1,updated_at=",
                clock,
                " WHERE id=$1 RETURNING *"
            ),
        )
        .bind(intent.id)
        .bind(next.as_str())
        .fetch_one()
        .await?;
        let updated = read_intent(&row, &intent.claim)?;
        event(&mut tx.coordination(), &updated, json!({})).await?;
        if next == InstancePhase::Ready {
            crate::store::schedule(&mut tx.coordination(), runtime.agent_id, None).await?;
        }
        tx.commit().await?;
        Ok(updated)
    }

    /// Records actual process termination; successful storage preparation requires its confirmed anchor.
    pub async fn finish_instance(
        &self,
        intent: &InstanceIntent,
        proof: &PreviousWriter,
        completed_head: Option<&AnchorHead>,
        problem: Option<&gesang_api::ApiProblem>,
    ) -> Result<InstanceIntent> {
        if *proof != PreviousWriter::Terminated(intent.id) || intent.phase == InstancePhase::Stopped
        {
            return Err(Error::FencingRequired);
        }
        if completed_head.is_some()
            && (!matches!(
                (intent.plan.purpose, intent.phase),
                (InstancePurpose::Migration, InstancePhase::Migrating)
                    | (InstancePurpose::Initialization, InstancePhase::Initializing)
            ) || problem.is_some())
        {
            return Err(Error::InvalidCommand(
                "storage completion requires its active preparation writer".into(),
            ));
        }
        let mut tx = self.database.begin().await?;
        verify_claim(&mut tx.coordination(), &intent.claim).await?;
        check_intent(&mut tx.coordination(), intent).await?;
        if let Some(head) = completed_head {
            let row = query(
                tx.executor(),
                statement!(
                    "SELECT sequence,digest,pending_id FROM gesang_anchors WHERE agent_id=$1",
                    [" FOR SHARE", ""]
                ),
            )
            .bind(intent.claim.agent.id)
            .fetch_optional()
            .await?
            .ok_or(Error::FreshnessMismatch)?;
            if row.try_get::<i64, _>("sequence")? != head.sequence
                || row.try_get::<Vec<u8>, _>("digest")? != head.digest
                || row.try_get::<Option<Uuid>, _>("pending_id")?.is_some()
            {
                return Err(Error::FreshnessMismatch);
            }
            if intent.plan.purpose == InstancePurpose::Initialization {
                let completed: bool = query_scalar(tx.executor(), "SELECT EXISTS(SELECT 1 FROM gesang_initializations WHERE agent_id=$1 AND state='completed' AND completed_runtime=$2 AND completed_head=$3)")
    .bind(intent.claim.agent.id)
    .bind(serde_json::to_value(intent.runtime.as_ref().ok_or(Error::FencingRequired)?)?)
    .bind(serde_json::to_value(head)?)
    .fetch_one().await?;
                if !completed {
                    return Err(Error::FreshnessMismatch);
                }
            }
        }
        let migration_head =
            completed_head.filter(|_| intent.plan.purpose == InstancePurpose::Migration);
        let row = query(tx.executor(), statement!("UPDATE gesang_instance_intents SET phase='stopped',migration_head=$2,problem=$3,revision=revision+1,updated_at=", clock, " WHERE id=$1 RETURNING *"))
    .bind(intent.id)
    .bind(migration_head.map(serde_json::to_value).transpose()?)
    .bind(problem.map(serde_json::to_value).transpose()?)
    .fetch_one().await?;
        let updated = read_intent(&row, &intent.claim)?;
        event(
            &mut tx.coordination(),
            &updated,
            json!({"migrationHead":migration_head,"initializationHead":completed_head.filter(|_| intent.plan.purpose == InstancePurpose::Initialization),"problem":problem}),
        )
        .await?;
        tx.commit().await?;
        Ok(updated)
    }

    /// Reads admitted writer identity even while execution remains paused for preparation.
    pub async fn writer(&self, agent_id: Uuid) -> Result<Runtime> {
        let row = query(self.database.executor(), statement!("SELECT a.* FROM gesang_agents a JOIN gesang_instance_intents i ON i.id=a.runtime_id AND i.agent_id=a.id AND i.generation=a.generation AND i.locator=a.runtime_locator WHERE a.id=$1 AND a.runtime_generation=a.generation AND a.lease_until>", clock, " AND i.phase NOT IN ('allocating','stopped')"))
    .bind(agent_id)
    .fetch_optional().await?.ok_or(Error::StaleGeneration)?;
        crate::store::read_runtime(&row)?.ok_or(Error::FencingRequired)
    }

    /// Verifies a current admitted writer without granting execution-ready routing.
    pub async fn verify_writer(&self, runtime: &Runtime) -> Result<()> {
        let mut tx = self.database.begin_read().await?;
        self.verify_writer_coordination_in(&mut tx.coordination(), runtime)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Holds current writer admission through a caller-owned metadata transaction.
    ///
    /// The transaction must use this store's authoritative database and read-committed
    /// isolation. This permits observation and control, never new execution by itself.
    pub async fn verify_writer_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        runtime: &Runtime,
    ) -> Result<()> {
        self.verify_writer_coordination_in(&mut tx.into(), runtime)
            .await
    }

    /// Runs the same check in the host's active coordination transaction.
    pub async fn verify_writer_coordination_in(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        runtime: &Runtime,
    ) -> Result<()> {
        self.database.verify_transaction(tx).await?;
        crate::anchor::verify_writer(tx, runtime).await
    }

    /// Requires current execution readiness without accepting a migration-purpose credential.
    pub async fn verify_execution(&self, runtime: &Runtime) -> Result<()> {
        self.under_current_execution(runtime, || Ok(())).await
    }

    /// Consumes a synchronous host capability while the exact ready writer remains current.
    ///
    /// The callback must not perform I/O. A failed transaction discards its returned value.
    pub async fn under_current_execution<T: Send>(
        &self,
        runtime: &Runtime,
        consume: impl FnOnce() -> Result<T> + Send,
    ) -> Result<T> {
        let mut tx = self.database.begin_read().await?;
        crate::anchor::verify_execution(&mut tx.coordination(), runtime).await?;
        let value = consume()?;
        tx.commit().await?;
        Ok(value)
    }

    /// Returns the current writer's fixed purpose and preparation stage.
    pub async fn instance_phase(
        &self,
        runtime: &Runtime,
    ) -> Result<(InstancePurpose, InstancePhase)> {
        let mut tx = self.database.begin_read().await?;
        crate::anchor::verify_writer(&mut tx.coordination(), runtime).await?;
        let row = query(
            tx.executor(),
            "SELECT plan,phase FROM gesang_instance_intents WHERE id=$1",
        )
        .bind(runtime.id)
        .fetch_one()
        .await?;
        let plan: InstancePlan = serde_json::from_value(row.try_get("plan")?)?;
        let phase = InstancePhase::from_str(row.try_get("phase")?)?;
        tx.commit().await?;
        Ok((plan.purpose, phase))
    }
}

async fn verify_claim(
    connection: &mut CoordinationTransaction<'_, '_>,
    claim: &RuntimeClaim,
) -> Result<Row> {
    let row = query(connection.executor(), statement!("SELECT * FROM gesang_agents WHERE id=$1 AND controller_id=$2 AND generation=$3 AND lease_until >= $4 AND $4>", clock, [" FOR UPDATE", ""]))
    .bind(claim.agent.id)
    .bind(claim.controller_id)
    .bind(claim.generation)
    .bind_timestamp(claim.expires_at)
    .fetch_optional().await?.ok_or(Error::StaleGeneration)?;
    let binding = crate::store::read_binding(connection, &row).await?;
    if binding.agent != claim.agent
        || binding.engine_owner != claim.engine_owner
        || binding.placement != claim.placement
        || binding.workspace_keys != claim.workspace_keys
    {
        return Err(Error::StaleGeneration);
    }
    Ok(row)
}

async fn check_intent(
    connection: &mut CoordinationTransaction<'_, '_>,
    intent: &InstanceIntent,
) -> Result<()> {
    let row = query(
        connection.executor(),
        statement!(
            "SELECT * FROM gesang_instance_intents WHERE id=$1 AND agent_id=$2 AND generation=$3",
            [" FOR UPDATE", ""]
        ),
    )
    .bind(intent.id)
    .bind(intent.claim.agent.id)
    .bind(intent.claim.generation)
    .fetch_optional()
    .await?
    .ok_or(Error::StaleGeneration)?;
    let actual = read_intent(&row, &intent.claim)?;
    if actual.plan != intent.plan
        || actual.phase != intent.phase
        || actual.revision != intent.revision
        || actual.runtime != intent.runtime
        || actual.migration_head != intent.migration_head
        || actual.problem != intent.problem
    {
        return Err(Error::VersionConflict);
    }
    Ok(())
}

fn read_intent(row: &Row, claim: &RuntimeClaim) -> Result<InstanceIntent> {
    let id = row.try_get("id")?;
    let locator: Option<String> = row.try_get("locator")?;
    Ok(InstanceIntent {
        id,
        claim: claim.clone(),
        plan: serde_json::from_value(row.try_get("plan")?)?,
        phase: InstancePhase::from_str(row.try_get("phase")?)?,
        revision: row.try_get("revision")?,
        runtime: locator.map(|locator| Runtime {
            id,
            agent_id: claim.agent.id,
            generation: claim.generation,
            locator,
            engine_owner: claim.engine_owner.clone(),
        }),
        migration_head: row
            .try_get::<Option<serde_json::Value>, _>("migration_head")?
            .map(serde_json::from_value)
            .transpose()?,
        problem: row
            .try_get::<Option<serde_json::Value>, _>("problem")?
            .map(serde_json::from_value)
            .transpose()?,
    })
}

async fn event(
    connection: &mut CoordinationTransaction<'_, '_>,
    intent: &InstanceIntent,
    detail: serde_json::Value,
) -> Result<()> {
    query(
        connection.executor(),
        "INSERT INTO gesang_instance_events(instance_id,revision,phase,detail) VALUES($1,$2,$3,$4)",
    )
    .bind(intent.id)
    .bind(intent.revision)
    .bind(intent.phase.as_str())
    .bind(detail)
    .execute()
    .await?;
    Ok(())
}
