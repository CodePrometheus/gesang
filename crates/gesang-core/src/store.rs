pub(crate) mod database;

use std::{path::Path, sync::Arc};

use gesang_api::{
    Agent, ApiProblem, CapabilityScope, CommandAuthorization, CommandReceipt, CommandSubmission,
    DeliveryState, Grant, GrantPurpose, GrantSelection, PermissionMode, SessionSnapshot,
};
use jiff::{SignedDuration, Timestamp};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, SqlitePool};
use uuid::Uuid;

use database::{Database, Row, Statement, query, query_scalar, statement};

pub use database::CoordinationTransaction;

use crate::{
    AgentBinding, CommandRequirements, Error, Owner, PayloadKey, RequiredAuthority, Result,
    Runtime, RuntimeClaim, VerifiedIdentity, Wake,
};

/// Transactional platform metadata and encrypted command delivery, without execution journal state.
#[derive(Clone, Debug)]
pub struct Store {
    pub(crate) database: Database,
    key: Arc<PayloadKey>,
}

/// Exact delegation required by a trusted host for one execution admission.
#[derive(Clone, Copy, Debug)]
pub struct GrantRequirement<'a> {
    /// Persisted delegation identity.
    pub id: Uuid,
    /// Exact revision; a revoked or replaced grant cannot satisfy it.
    pub version: i64,
    /// Independent execution purpose selected by the host.
    pub purpose: GrantPurpose,
    /// Resource requirements derived from the actual admitted action.
    pub scopes: &'a [CapabilityScope],
}

/// One claimed delivery attempt; execution reconciliation uses the original request identity.
#[derive(Clone, Debug)]
pub struct PendingCommand {
    /// Stable request identity shared with the execution service.
    pub request_id: Uuid,
    /// Logical agent owning the command.
    pub agent_id: Uuid,
    /// Exact owner-control or task-delegation requirement bound to the immutable command.
    pub authorization: CommandAuthorization,
    /// Original authenticated command content.
    pub command: Value,
    /// Admission requirements derived by the trusted execution adapter.
    pub requirements: Vec<CapabilityScope>,
    /// Unique delivery attempt claimant, preventing stale acknowledgements.
    pub delivery_owner: Uuid,
}

/// A versioned wake claim; an obsolete claimant cannot erase new work.
#[derive(Clone, Debug)]
pub struct WakeClaim {
    /// Agent whose recovery work was claimed.
    pub agent_id: Uuid,
    /// Revision incremented for new work and lease takeover.
    pub revision: i64,
    /// Identity of this specific attempt, separate from controller identity.
    pub claimant: Uuid,
}

impl Store {
    /// Starts a short host transaction with synchronous commit enforced locally.
    /// Service adapters may join their own records to kernel authorization checks;
    /// they must not query private kernel tables or hold the transaction across network calls.
    pub async fn begin(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>> {
        database::begin_postgres(self.postgres_pool().ok_or(Error::StorageBackend)?).await
    }

    /// Uses a host-configured connection pool and protected payload encryption key.
    pub fn new(pool: PgPool, key: PayloadKey) -> Self {
        Self {
            database: Database::Postgres(pool),
            key: Arc::new(key),
        }
    }

    /// Opens and migrates the durable coordination file for one trusted host.
    ///
    /// The same pool must be supplied to the host's command ledger. Keep the file
    /// and its journal on local durable storage with a single trusted writer process.
    pub async fn open_sqlite(path: &Path, key: PayloadKey) -> Result<Self> {
        let store = Self {
            database: Database::open_sqlite(path).await?,
            key: Arc::new(key),
        };
        store.migrate().await?;
        Ok(store)
    }

    /// Exposes the pool for PostgreSQL-only host adapters.
    ///
    /// # Panics
    ///
    /// Panics if the store was constructed for another backend. Adapters accepting
    /// both backends should use the explicit pool accessors.
    pub fn pool(&self) -> &PgPool {
        #[expect(
            clippy::expect_used,
            reason = "legacy accessor for PostgreSQL-only host adapters"
        )]
        self.postgres_pool().expect("PostgreSQL store required")
    }

    /// Returns the host-configured PostgreSQL pool when this store uses it.
    pub fn postgres_pool(&self) -> Option<&PgPool> {
        match &self.database {
            Database::Postgres(pool) => Some(pool),
            Database::Sqlite(_) => None,
        }
    }

    /// Returns the coordination file's pool for host-owned metadata in the same database.
    pub fn sqlite_pool(&self) -> Option<&SqlitePool> {
        match &self.database {
            Database::Postgres(_) => None,
            Database::Sqlite(pool) => Some(pool),
        }
    }

    /// Checks required durability settings and applies immutable embedded migrations.
    pub async fn migrate(&self) -> Result<()> {
        self.database.migrate().await
    }

    /// Rejects database settings that cannot support durable coordination.
    pub async fn validate_durability(&self) -> Result<()> {
        self.database.validate_durability().await
    }

    /// Looks up a verified identity without enrolling an owner or merging issuers.
    ///
    /// A missing owner is distinct from an unavailable database. Hosts must preserve
    /// lookup failures rather than interpreting them as permission to use a legacy path.
    pub async fn find_owner(&self, identity: &VerifiedIdentity) -> Result<Option<Owner>> {
        if identity.issuer.is_empty() || identity.subject.is_empty() {
            return Err(Error::Forbidden);
        }
        let id = query_scalar(
            self.database.executor(),
            "SELECT id FROM gesang_owners WHERE issuer=$1 AND subject=$2",
        )
        .bind(&identity.issuer)
        .bind(&identity.subject)
        .fetch_optional()
        .await?;
        Ok(id.map(|id| Owner { id }))
    }

    /// Atomically binds a previously verified issuer and subject without cross-issuer merging.
    pub async fn bind_owner(&self, identity: &VerifiedIdentity) -> Result<Owner> {
        if identity.issuer.is_empty() || identity.subject.is_empty() {
            return Err(Error::Forbidden);
        }
        let mut tx = self.database.begin().await?;
        let id = query_scalar(
            tx.executor(),
            "INSERT INTO gesang_owners(id,issuer,subject) VALUES ($1,$2,$3) \
             ON CONFLICT(issuer,subject) DO UPDATE SET subject=EXCLUDED.subject RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(&identity.issuer)
        .bind(&identity.subject)
        .fetch_one()
        .await?;
        tx.commit().await?;
        Ok(Owner { id })
    }

    /// Creates one logical agent or verifies the existing execution adapter and owner binding.
    pub async fn ensure_agent(
        &self,
        owner: &Owner,
        engine: &str,
        engine_owner: &str,
    ) -> Result<Agent> {
        let mut tx = self.database.begin().await?;
        let row = query(
            tx.executor(),
            "INSERT INTO gesang_agents(id,owner_id,engine,engine_owner) VALUES($1,$2,$3,$4) \
             ON CONFLICT(owner_id) DO UPDATE SET owner_id=EXCLUDED.owner_id RETURNING *",
        )
        .bind(Uuid::new_v4())
        .bind(owner.id)
        .bind(engine)
        .bind(engine_owner)
        .fetch_one()
        .await?;
        let agent = read_agent(&row)?;
        if agent.engine != engine || row.try_get::<String, _>("engine_owner")? != engine_owner {
            return Err(Error::VersionConflict);
        }
        tx.commit().await?;
        Ok(agent)
    }

    /// Persists an immutable host placement before adopting an existing workspace.
    /// First-store operations must use the custody-bound initialization entry point.
    pub async fn bind_placement(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        workspace_keys: &[String],
        placement: &str,
    ) -> Result<()> {
        let mut tx = self.database.begin().await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_agents WHERE id=$1 AND owner_id=$2",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(agent_id)
        .bind(owner.id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let initializing: bool = query_scalar(
            tx.executor(),
            "SELECT EXISTS(SELECT 1 FROM gesang_initializations WHERE agent_id=$1)",
        )
        .bind(agent_id)
        .fetch_one()
        .await?;
        if initializing {
            return Err(Error::Forbidden);
        }
        Self::bind_placement_in(
            &mut tx.coordination(),
            &row,
            agent_id,
            workspace_keys,
            placement,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn bind_placement_in(
        connection: &mut CoordinationTransaction<'_, '_>,
        agent: &Row,
        agent_id: Uuid,
        workspace_keys: &[String],
        placement: &str,
    ) -> Result<()> {
        let mut keys = workspace_keys.to_vec();
        keys.sort();
        keys.dedup();
        if placement.is_empty()
            || keys.is_empty()
            || keys.iter().any(String::is_empty)
            || keys.len() != workspace_keys.len()
        {
            return Err(Error::InvalidCommand(
                "placement requires distinct physical resource identities".into(),
            ));
        }
        let existing = workspace_resources(connection, agent_id).await?;
        let prior_placement: Option<String> = agent.try_get("legacy_reference")?;
        if prior_placement
            .as_deref()
            .is_some_and(|prior| prior != placement)
            || (!existing.is_empty() && existing != keys)
            || (existing.is_empty() && agent.try_get::<Option<Uuid>, _>("runtime_id")?.is_some())
        {
            return Err(Error::VersionConflict);
        }
        let bound = query_scalar::<Uuid>(
            connection.executor(),
            Statement::new(
                "INSERT INTO gesang_workspace_resources(resource_key,agent_id) \
             SELECT key,$1 FROM jsonb_array_elements_text($2) AS keys(key) ORDER BY key \
             ON CONFLICT(resource_key) DO UPDATE SET agent_id=EXCLUDED.agent_id \
             WHERE gesang_workspace_resources.agent_id=EXCLUDED.agent_id RETURNING agent_id",
                "INSERT INTO gesang_workspace_resources(resource_key,agent_id) \
             SELECT value,$1 FROM json_each($2) WHERE true ORDER BY value \
             ON CONFLICT(resource_key) DO UPDATE SET agent_id=EXCLUDED.agent_id \
             WHERE gesang_workspace_resources.agent_id=EXCLUDED.agent_id RETURNING agent_id",
            ),
        )
        .bind(agent_id)
        .bind(serde_json::to_value(&keys)?)
        .fetch_all()
        .await?;
        if bound.len() != keys.len() {
            return Err(Error::VersionConflict);
        }
        query(
            connection.executor(),
            "UPDATE gesang_agents SET legacy_reference=$1 WHERE id=$2",
        )
        .bind(placement)
        .bind(agent_id)
        .execute()
        .await?;
        Ok(())
    }

    /// Resolves an agent only within its authenticated owner's boundary.
    pub async fn agent(&self, owner: &Owner, id: Uuid) -> Result<Agent> {
        let row = query(
            self.database.executor(),
            "SELECT * FROM gesang_agents WHERE id=$1 AND owner_id=$2",
        )
        .bind(id)
        .bind(owner.id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        read_agent(&row)
    }

    /// Reads immutable placement for a trusted host; this is not a public owner lookup.
    pub async fn agent_binding(&self, agent_id: Uuid) -> Result<AgentBinding> {
        let mut tx = self.database.begin_read().await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_agents WHERE id=$1",
                [" FOR SHARE", ""]
            ),
        )
        .bind(agent_id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let binding = read_binding(&mut tx.coordination(), &row).await?;
        tx.commit().await?;
        Ok(binding)
    }

    /// Reads the owner's agent and explicitly selected active delegation.
    pub async fn session(&self, owner: &Owner) -> Result<SessionSnapshot> {
        let row = query(
            self.database.executor(),
            "SELECT * FROM gesang_agents WHERE owner_id=$1",
        )
        .bind(owner.id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let agent = read_agent(&row)?;
        let grant =
            query(self.database.executor(), "SELECT * FROM gesang_grants WHERE agent_id=$1 AND purpose='task' AND revoked_at IS NULL")
    .bind(agent.id)
    .fetch_optional()
                .await?
                .as_ref()
                .map(read_grant)
                .transpose()?;
        Ok(SessionSnapshot {
            owner_id: owner.id,
            agent,
            grant,
        })
    }

    /// Atomically revokes the prior delegation and creates the explicitly chosen replacement.
    pub async fn select_grant(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        selection: &GrantSelection,
    ) -> Result<Grant> {
        self.select_grant_for(owner, agent_id, GrantPurpose::Task, selection)
            .await
    }

    /// Replaces only the selected execution purpose; other delegations remain independent.
    pub async fn select_grant_for(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        purpose: GrantPurpose,
        selection: &GrantSelection,
    ) -> Result<Grant> {
        let mut tx = self.database.begin().await?;
        let found = query_scalar::<Uuid>(
            tx.executor(),
            statement!(
                "SELECT id FROM gesang_agents WHERE id=$1 AND owner_id=$2",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(agent_id)
        .bind(owner.id)
        .fetch_optional()
        .await?;
        found.ok_or(Error::NotFound)?;
        query(
            tx.executor(),
            statement!(
                "UPDATE gesang_grants SET revoked_at=",
                clock,
                ",version=version+1 WHERE agent_id=$1 AND purpose=$2 AND revoked_at IS NULL"
            ),
        )
        .bind(agent_id)
        .bind(purpose_str(purpose))
        .execute()
        .await?;
        let row = query(tx.executor(), "INSERT INTO gesang_grants(id,agent_id,version,mode,scopes,purpose) VALUES($1,$2,1,$3,$4,$5) RETURNING *")
    .bind(Uuid::new_v4())
    .bind(agent_id)
    .bind(mode_str(selection.mode))
    .bind(serde_json::to_value(&selection.scopes)?)
    .bind(purpose_str(purpose))
    .fetch_one().await?;
        let grant = read_grant(&row)?;
        tx.commit().await?;
        Ok(grant)
    }

    /// Idempotently revokes this owner's delegation without claiming to undo prior effects.
    pub async fn revoke_grant(&self, owner: &Owner, agent_id: Uuid, grant_id: Uuid) -> Result<()> {
        let mut tx = self.database.begin().await?;
        query_scalar::<Uuid>(
            tx.executor(),
            statement!(
                "SELECT id FROM gesang_agents WHERE id=$1 AND owner_id=$2",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(agent_id)
        .bind(owner.id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let count = query(tx.executor(), statement!("UPDATE gesang_grants SET revoked_at=COALESCE(revoked_at,", clock, "),version=CASE WHEN revoked_at IS NULL THEN version+1 ELSE version END WHERE id=$1 AND agent_id=$2"))
    .bind(grant_id)
    .bind(agent_id)
    .execute().await?;
        if count == 0 {
            return Err(Error::NotFound);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Checks a current delegation and exact scopes; host resource policy remains mandatory.
    pub async fn authorize(
        &self,
        agent_id: Uuid,
        grant_id: Uuid,
        version: i64,
        scopes: &[CapabilityScope],
    ) -> Result<Grant> {
        let row = query(self.database.executor(), "SELECT * FROM gesang_grants WHERE id=$1 AND agent_id=$2 AND version=$3 AND purpose='task' AND revoked_at IS NULL")
    .bind(grant_id)
    .bind(agent_id)
    .bind(version)
    .fetch_optional().await?.ok_or(Error::Forbidden)?;
        let grant = read_grant(&row)?;
        check_scopes(&grant, scopes)?;
        Ok(grant)
    }

    /// Linearizes current writer admission and delegation against revocation and takeover.
    pub async fn authorize_runtime(
        &self,
        runtime: &Runtime,
        grant_id: Uuid,
        version: i64,
        scopes: &[CapabilityScope],
    ) -> Result<Grant> {
        self.authorize_runtime_for(runtime, grant_id, version, GrantPurpose::Task, scopes)
            .await
    }

    /// Admits only the required purpose while linearizing against writer and grant revocation.
    pub async fn authorize_runtime_for(
        &self,
        runtime: &Runtime,
        grant_id: Uuid,
        version: i64,
        purpose: GrantPurpose,
        scopes: &[CapabilityScope],
    ) -> Result<Grant> {
        let mut tx = self.database.begin_read().await?;
        let grant = self
            .authorize_runtime_coordination_in(
                &mut tx.coordination(),
                runtime,
                &GrantRequirement {
                    id: grant_id,
                    version,
                    purpose,
                    scopes,
                },
            )
            .await?;
        tx.commit().await?;
        Ok(grant)
    }

    /// Joins host-owned acceptance to current writer and delegation checks in one transaction.
    ///
    /// Use this store's authoritative database and a read-committed transaction.
    /// Locks remain held until the caller commits or rolls back. Commit acceptance
    /// before issuing an external effect; authorization does not prove execution.
    pub async fn authorize_runtime_in(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        runtime: &Runtime,
        required: &GrantRequirement<'_>,
    ) -> Result<Grant> {
        self.authorize_runtime_coordination_in(&mut tx.into(), runtime, required)
            .await
    }

    /// Runs the same check in the host's active coordination transaction.
    pub async fn authorize_runtime_coordination_in(
        &self,
        tx: &mut CoordinationTransaction<'_, '_>,
        runtime: &Runtime,
        required: &GrantRequirement<'_>,
    ) -> Result<Grant> {
        self.database.verify_transaction(tx).await?;
        crate::anchor::verify_execution(tx, runtime).await?;
        let row = query(tx.executor(), "SELECT * FROM gesang_grants WHERE id=$1 AND agent_id=$2 AND version=$3 AND purpose=$4 AND revoked_at IS NULL")
    .bind(required.id)
    .bind(runtime.agent_id)
    .bind(required.version)
    .bind(purpose_str(required.purpose))
    .fetch_optional().await?.ok_or(Error::Forbidden)?;
        let grant = read_grant(&row)?;
        check_scopes(&grant, required.scopes)?;
        Ok(grant)
    }

    /// Persists immutable encrypted delivery and a recovery wake in the same durable transaction.
    pub async fn enqueue(
        &self,
        owner: &Owner,
        agent_id: Uuid,
        submission: &CommandSubmission,
        requirements: &CommandRequirements,
    ) -> Result<CommandReceipt> {
        let content = serde_json::to_vec(submission)?;
        let digest = Sha256::digest(&content).to_vec();
        let aad = command_aad(agent_id, submission.request_id);
        let sealed = self.key.seal(&aad, &content)?;
        let mut tx = self.database.begin().await?;
        query_scalar::<Uuid>(
            tx.executor(),
            statement!(
                "SELECT id FROM gesang_agents WHERE id=$1 AND owner_id=$2",
                [" FOR SHARE", ""]
            ),
        )
        .bind(agent_id)
        .bind(owner.id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        if let Some(row) = query(
            tx.executor(),
            "SELECT * FROM gesang_commands WHERE request_id=$1",
        )
        .bind(submission.request_id)
        .fetch_optional()
        .await?
        {
            if row.try_get::<Uuid, _>("agent_id")? != agent_id
                || row.try_get::<Vec<u8>, _>("content_digest")? != digest
            {
                return Err(Error::RequestConflict);
            }
            let receipt = self.read_receipt(&row)?;
            tx.commit().await?;
            return Ok(receipt);
        }
        let (authority, grant_id, grant_version) = match (
            &submission.authorization,
            requirements.authority,
        ) {
            (CommandAuthorization::Owner, RequiredAuthority::OwnerControl)
                if requirements.scopes.is_empty() =>
            {
                ("owner", None, None)
            }
            (CommandAuthorization::Grant { id, version }, RequiredAuthority::TaskGrant) => {
                let row=query(tx.executor(), statement!("SELECT * FROM gesang_grants WHERE id=$1 AND agent_id=$2 AND version=$3 AND purpose='task' AND revoked_at IS NULL", [" FOR SHARE", ""]))
    .bind(id)
    .bind(agent_id)
    .bind(version)
    .fetch_optional().await?.ok_or(Error::Forbidden)?;
                check_scopes(&read_grant(&row)?, &requirements.scopes)?;
                ("grant", Some(*id), Some(*version))
            }
            _ => return Err(Error::Forbidden),
        };
        let inserted = query(tx.executor(), "INSERT INTO gesang_commands(request_id,agent_id,grant_id,grant_version,content_digest,sealed_command,requirements,authority) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(request_id) DO NOTHING RETURNING *")
    .bind(submission.request_id)
    .bind(agent_id)
    .bind(grant_id)
    .bind(grant_version)
    .bind(&digest)
    .bind(sealed)
    .bind(serde_json::to_value(&requirements.scopes)?)
    .bind(authority)
    .fetch_optional().await?;
        let row = match inserted {
            Some(row) => row,
            None => {
                query(
                    tx.executor(),
                    "SELECT * FROM gesang_commands WHERE request_id=$1",
                )
                .bind(submission.request_id)
                .fetch_one()
                .await?
            }
        };
        if row.try_get::<Uuid, _>("agent_id")? != agent_id
            || row.try_get::<Vec<u8>, _>("content_digest")? != digest
        {
            return Err(Error::RequestConflict);
        }
        schedule(&mut tx.coordination(), agent_id, None).await?;
        let receipt = self.read_receipt(&row)?;
        tx.commit().await?;
        Ok(receipt)
    }

    /// Retrieves only this owner's original delivery acknowledgement and rejection metadata.
    pub async fn receipt(&self, owner: &Owner, request_id: Uuid) -> Result<CommandReceipt> {
        let row = query(self.database.executor(), "SELECT c.request_id,c.agent_id,c.state,c.sealed_receipt,c.problem FROM gesang_commands c JOIN gesang_agents a ON a.id=c.agent_id WHERE c.request_id=$1 AND a.owner_id=$2")
    .bind(request_id)
    .bind(owner.id)
    .fetch_optional().await?.ok_or(Error::NotFound)?;
        self.read_receipt(&row)
    }

    /// Claims one pending command without blocking other claimants on locked rows.
    pub async fn claim_command(
        &self,
        claimant: Uuid,
        duration: SignedDuration,
    ) -> Result<Option<PendingCommand>> {
        let duration_ms = positive_duration(duration)?;
        let mut tx = self.database.begin().await?;
        let row = query(tx.executor(), statement!("UPDATE gesang_commands SET delivery_owner=$1,delivery_until=", (lease 2), " WHERE request_id=(SELECT request_id FROM gesang_commands WHERE state='pending' AND (delivery_until IS NULL OR delivery_until<=", clock, ") ORDER BY created_at,request_id", [" FOR UPDATE SKIP LOCKED", ""], " LIMIT 1) RETURNING *"))
    .bind(claimant)
    .bind(duration_ms)
    .fetch_optional().await?;
        let command = row
            .map(|row| {
                let agent_id = row.try_get("agent_id")?;
                let request_id = row.try_get("request_id")?;
                let content = self.key.open(
                    &command_aad(agent_id, request_id),
                    &row.try_get::<Vec<u8>, _>("sealed_command")?,
                )?;
                let submission: CommandSubmission = serde_json::from_slice(&content)?;
                let stored_authorization = match row.try_get::<&str, _>("authority")? {
                    "owner" => CommandAuthorization::Owner,
                    "grant" => CommandAuthorization::Grant {
                        id: row.try_get("grant_id")?,
                        version: row.try_get("grant_version")?,
                    },
                    _ => return Err(Error::InvalidCiphertext),
                };
                if submission.request_id != request_id
                    || submission.authorization != stored_authorization
                {
                    return Err(Error::InvalidCiphertext);
                }
                Ok(PendingCommand {
                    request_id,
                    agent_id,
                    authorization: submission.authorization,
                    command: submission.command,
                    requirements: serde_json::from_value(row.try_get("requirements")?)?,
                    delivery_owner: claimant,
                })
            })
            .transpose()?;
        tx.commit().await?;
        Ok(command)
    }

    /// Records the execution authority's durable receipt for the current attempt.
    pub async fn accept_command(&self, command: &PendingCommand, receipt: &Value) -> Result<()> {
        let sealed = self.key.seal(
            &receipt_aad(command.agent_id, command.request_id),
            &serde_json::to_vec(receipt)?,
        )?;
        let mut tx = self.database.begin().await?;
        let changed = query(tx.executor(), statement!("UPDATE gesang_commands SET state='accepted',sealed_receipt=$1,acknowledged_at=", clock, ",delivery_owner=NULL,delivery_until=NULL WHERE request_id=$2 AND state='pending' AND delivery_owner=$3"))
    .bind(sealed)
    .bind(command.request_id)
    .bind(command.delivery_owner)
    .execute().await?;
        if changed == 0 {
            return Err(Error::VersionConflict);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Records a definitive rejection; transport uncertainty is not a rejection.
    pub async fn reject_command(
        &self,
        command: &PendingCommand,
        problem: &ApiProblem,
    ) -> Result<()> {
        let mut tx = self.database.begin().await?;
        let changed = query(tx.executor(), statement!("UPDATE gesang_commands SET state='rejected',problem=$1,acknowledged_at=", clock, ",delivery_owner=NULL,delivery_until=NULL WHERE request_id=$2 AND state='pending' AND delivery_owner=$3"))
    .bind(serde_json::to_value(problem)?)
    .bind(command.request_id)
    .bind(command.delivery_owner)
    .execute().await?;
        if changed == 0 {
            return Err(Error::VersionConflict);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Retains original command identity and schedules reconciliation after an uncertain attempt.
    pub async fn defer_command(&self, command: &PendingCommand, due_at: Timestamp) -> Result<()> {
        let mut tx = self.database.begin().await?;
        query(tx.executor(), "UPDATE gesang_commands SET delivery_owner=NULL,delivery_until=$1 WHERE request_id=$2 AND state='pending' AND delivery_owner=$3")
    .bind_timestamp(due_at)
    .bind(command.request_id)
    .bind(command.delivery_owner)
    .execute().await?;
        schedule(&mut tx.coordination(), command.agent_id, Some(due_at)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Persists the earliest recovery deadline and invalidates stale wake receipts.
    pub async fn schedule(&self, agent_id: Uuid, due_at: Timestamp) -> Result<()> {
        let mut tx = self.database.begin().await?;
        schedule(&mut tx.coordination(), agent_id, Some(due_at)).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Persists a verified writer's recovery signal without erasing concurrent wake work.
    pub async fn report_wake(
        &self,
        runtime: &Runtime,
        wake: &Wake,
        recovery_interval: SignedDuration,
    ) -> Result<()> {
        positive_duration(recovery_interval)?;
        let mut tx = self.database.begin().await?;
        crate::anchor::verify_execution(&mut tx.coordination(), runtime).await?;
        let now = query(tx.executor(), statement!("SELECT ", clock, " AS now"))
            .fetch_one()
            .await?
            .timestamp("now")?;
        let due = if wake.active {
            let recovery = now.checked_add(recovery_interval).map_err(|_| {
                Error::InvalidCommand("recovery deadline exceeds the clock range".into())
            })?;
            Some(wake.at.map_or(recovery, |at| at.min(recovery)))
        } else {
            wake.at
        };
        if let Some(due) = due {
            schedule(&mut tx.coordination(), runtime.agent_id, Some(due)).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Claims one due recovery record with a new revision on every takeover.
    pub async fn claim_wake(
        &self,
        claimant: Uuid,
        duration: SignedDuration,
    ) -> Result<Option<WakeClaim>> {
        let mut tx = self.database.begin().await?;
        let row = query(tx.executor(), statement!("UPDATE gesang_wakes SET claimed_by=$1,claimed_until=", (lease 2), ",revision=revision+1 WHERE agent_id=(SELECT agent_id FROM gesang_wakes WHERE due_at<=", clock, " AND (claimed_until IS NULL OR claimed_until<=", clock, ") ORDER BY due_at,agent_id", [" FOR UPDATE SKIP LOCKED", ""], " LIMIT 1) RETURNING agent_id,revision"))
    .bind(claimant)
    .bind(positive_duration(duration)?)
    .fetch_optional().await?;
        let claim = row
            .map(|row| -> Result<WakeClaim> {
                Ok(WakeClaim {
                    agent_id: row.try_get("agent_id")?,
                    revision: row.try_get("revision")?,
                    claimant,
                })
            })
            .transpose()?;
        tx.commit().await?;
        Ok(claim)
    }

    /// Updates or consumes a wake only if no newer work or claimant superseded it.
    pub async fn settle_wake(&self, claim: &WakeClaim, next: Option<Timestamp>) -> Result<()> {
        let mut tx = self.database.begin().await?;
        match next {
            Some(next) => {
                query(tx.executor(), "UPDATE gesang_wakes SET due_at=$1,revision=revision+1,claimed_by=NULL,claimed_until=NULL WHERE agent_id=$2 AND revision=$3 AND claimed_by=$4")
    .bind_timestamp(next)
    .bind(claim.agent_id)
    .bind(claim.revision)
    .bind(claim.claimant)
    .execute().await?;
            }
            None => {
                query(
                    tx.executor(),
                    "DELETE FROM gesang_wakes WHERE agent_id=$1 AND revision=$2 AND claimed_by=$3",
                )
                .bind(claim.agent_id)
                .bind(claim.revision)
                .bind(claim.claimant)
                .execute()
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Allocates or renews controller intent without claiming that an old writer is fenced.
    pub async fn claim_runtime(
        &self,
        agent_id: Uuid,
        controller_id: Uuid,
        duration: SignedDuration,
    ) -> Result<RuntimeClaim> {
        let millis = positive_duration(duration)?;
        let mut tx = self.database.begin().await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT *,",
                clock,
                " AS observed_now FROM gesang_agents WHERE id=$1",
                [" FOR UPDATE", ""]
            ),
        )
        .bind(agent_id)
        .fetch_optional()
        .await?
        .ok_or(Error::NotFound)?;
        let observed_now: Timestamp = row.timestamp("observed_now")?;
        let revoked: bool = query_scalar(tx.executor(), "SELECT EXISTS(SELECT 1 FROM gesang_custody c WHERE c.agent_id=$1 AND c.active_version IS NULL AND NOT (c.revision=0 AND EXISTS(SELECT 1 FROM gesang_initializations i WHERE i.agent_id=c.agent_id AND i.state='pending')))")
    .bind(agent_id)
    .fetch_one().await?;
        if revoked {
            return Err(Error::Forbidden);
        }
        let active = row
            .optional_timestamp("lease_until")?
            .is_some_and(|until| until > observed_now);
        if active && row.try_get::<Option<Uuid>, _>("controller_id")? != Some(controller_id) {
            return Err(Error::StaleGeneration);
        }
        let previous = read_runtime(&row)?;
        let row = query(tx.executor(), statement!("UPDATE gesang_agents SET generation=CASE WHEN controller_id=$2 AND lease_until>", clock, " THEN generation ELSE generation+1 END,controller_id=$2,lease_until=", (lease 3), " WHERE id=$1 RETURNING *"))
    .bind(agent_id)
    .bind(controller_id)
    .bind(millis)
    .fetch_one().await?;
        let claim = RuntimeClaim {
            agent: read_agent(&row)?,
            engine_owner: row.try_get("engine_owner")?,
            placement: row.try_get("legacy_reference")?,
            workspace_keys: workspace_resources(&mut tx.coordination(), agent_id).await?,
            controller_id,
            generation: row.try_get("generation")?,
            expires_at: row.timestamp("lease_until")?,
            previous,
        };
        tx.commit().await?;
        Ok(claim)
    }

    /// Checks admission intent before host effects; providers must still fence actual writers.
    pub async fn verify_runtime_claim(&self, claim: &RuntimeClaim) -> Result<()> {
        let mut tx = self.database.begin_read().await?;
        let row = query(
            tx.executor(),
            statement!(
                "SELECT * FROM gesang_agents WHERE id=$1 AND controller_id=$2 AND generation=$3 \
             AND lease_until>= $4 AND $4>",
                clock,
                [" FOR SHARE", ""]
            ),
        )
        .bind(claim.agent.id)
        .bind(claim.controller_id)
        .bind(claim.generation)
        .bind_timestamp(claim.expires_at)
        .fetch_optional()
        .await?
        .ok_or(Error::StaleGeneration)?;
        if read_agent(&row)? != claim.agent
            || row.try_get::<String, _>("engine_owner")? != claim.engine_owner
            || row.try_get::<Option<String>, _>("legacy_reference")? != claim.placement
            || workspace_resources(&mut tx.coordination(), claim.agent.id).await?
                != claim.workspace_keys
        {
            return Err(Error::StaleGeneration);
        }
        tx.commit().await?;
        Ok(())
    }

    /// Advances an exact claim after the provider fenced all launches, including unadmitted ones.
    pub async fn advance_fenced_generation(&self, claim: &RuntimeClaim) -> Result<RuntimeClaim> {
        let mut tx = self.database.begin().await?;
        let row=query(tx.executor(), statement!("UPDATE gesang_agents SET generation=generation+1 WHERE id=$1 AND generation=$2 AND controller_id=$3 AND lease_until >= $4 AND $4>", clock, " AND runtime_id IS NOT DISTINCT FROM $5 AND runtime_generation IS NOT DISTINCT FROM $6 AND runtime_locator IS NOT DISTINCT FROM $7 AND owner_id=$8 AND engine_owner=$9 AND legacy_reference IS NOT DISTINCT FROM $10 RETURNING *"))
    .bind(claim.agent.id)
    .bind(claim.generation)
    .bind(claim.controller_id)
    .bind_timestamp(claim.expires_at)
    .bind(claim.previous.as_ref().map(|old| old.id))
    .bind(claim.previous.as_ref().map(|old| old.generation))
    .bind(claim.previous.as_ref().map(|old| old.locator.as_str()))
    .bind(claim.agent.owner_id)
    .bind(&claim.engine_owner)
    .bind(&claim.placement)
    .fetch_optional().await?.ok_or(Error::StaleGeneration)?;
        if read_agent(&row)? != claim.agent
            || workspace_resources(&mut tx.coordination(), claim.agent.id).await?
                != claim.workspace_keys
            || claim.previous.as_ref().is_some_and(|old| {
                old.agent_id != claim.agent.id || old.engine_owner != claim.engine_owner
            })
        {
            return Err(Error::StaleGeneration);
        }
        let claim = RuntimeClaim {
            agent: read_agent(&row)?,
            engine_owner: row.try_get("engine_owner")?,
            placement: row.try_get("legacy_reference")?,
            workspace_keys: workspace_resources(&mut tx.coordination(), claim.agent.id).await?,
            controller_id: claim.controller_id,
            generation: row.try_get("generation")?,
            expires_at: row.timestamp("lease_until")?,
            previous: read_runtime(&row)?,
        };
        tx.commit().await?;
        Ok(claim)
    }

    /// Reads an admitted runtime only while its generation and controller lease remain current.
    pub async fn runtime(&self, agent_id: Uuid) -> Result<Runtime> {
        let row = query(self.database.executor(), statement!("SELECT a.*,i.phase,i.plan->>'purpose' AS purpose FROM gesang_agents a JOIN gesang_instance_intents i ON i.id=a.runtime_id AND i.agent_id=a.id AND i.generation=a.generation AND i.locator=a.runtime_locator WHERE a.id=$1 AND a.lease_until>", clock, " AND a.runtime_generation=a.generation AND i.phase NOT IN ('allocating','stopped')"))
    .bind(agent_id)
    .fetch_optional().await?.ok_or(Error::StaleGeneration)?;
        if row.try_get::<&str, _>("phase")? != "ready"
            || row.try_get::<&str, _>("purpose")? != "execution"
        {
            return Err(Error::RuntimeNotReady);
        }
        read_runtime(&row)?.ok_or(Error::FencingRequired)
    }

    /// Rejects stale instance identities before admitting a new external action.
    pub async fn verify_generation(&self, runtime: &Runtime) -> Result<()> {
        let current = self.runtime(runtime.agent_id).await?;
        if current != *runtime {
            return Err(Error::StaleGeneration);
        }
        Ok(())
    }

    fn read_receipt(&self, row: &Row) -> Result<CommandReceipt> {
        let request_id = row.try_get("request_id")?;
        let agent_id = row.try_get("agent_id")?;
        let state = match row.try_get::<&str, _>("state")? {
            "pending" => DeliveryState::Pending,
            "accepted" => DeliveryState::Accepted,
            "rejected" => DeliveryState::Rejected,
            _ => return Err(Error::InvalidCiphertext),
        };
        let engine_receipt = row
            .try_get::<Option<Vec<u8>>, _>("sealed_receipt")?
            .map(|sealed| {
                self.key
                    .open(&receipt_aad(agent_id, request_id), &sealed)
                    .and_then(|plain| Ok(serde_json::from_slice(&plain)?))
            })
            .transpose()?;
        Ok(CommandReceipt {
            request_id,
            agent_id,
            state,
            engine_receipt,
            problem: row
                .try_get::<Option<Value>, _>("problem")?
                .map(serde_json::from_value)
                .transpose()?,
        })
    }
}

async fn workspace_resources(
    connection: &mut CoordinationTransaction<'_, '_>,
    agent_id: Uuid,
) -> Result<Vec<String>> {
    Ok(query_scalar(connection.executor(), statement!("SELECT resource_key FROM gesang_workspace_resources WHERE agent_id=$1 ORDER BY resource_key ", ["COLLATE \"C\"", "COLLATE BINARY"]))
    .bind(agent_id)
    .fetch_all().await?)
}

pub(crate) async fn read_binding(
    connection: &mut CoordinationTransaction<'_, '_>,
    row: &Row,
) -> Result<AgentBinding> {
    let agent = read_agent(row)?;
    let workspace_keys = workspace_resources(connection, agent.id).await?;
    Ok(AgentBinding {
        agent,
        engine_owner: row.try_get("engine_owner")?,
        placement: row.try_get("legacy_reference")?,
        workspace_keys,
    })
}

pub(crate) async fn schedule(
    tx: &mut CoordinationTransaction<'_, '_>,
    agent_id: Uuid,
    due_at: Option<Timestamp>,
) -> Result<()> {
    query(
        tx.executor(),
        statement!(
            "INSERT INTO gesang_wakes(agent_id,due_at) VALUES($1,COALESCE($2,",
            clock,
            ")) ON CONFLICT(agent_id) DO UPDATE SET due_at=",
            ["LEAST", "min"],
            "(gesang_wakes.due_at,EXCLUDED.due_at),revision=gesang_wakes.revision+1,
             claimed_by=NULL,claimed_until=NULL",
        ),
    )
    .bind(agent_id)
    .bind_optional_timestamp(due_at)
    .execute()
    .await?;
    Ok(())
}

fn read_agent(row: &Row) -> Result<Agent> {
    Ok(Agent {
        id: row.try_get("id")?,
        owner_id: row.try_get("owner_id")?,
        engine: row.try_get("engine")?,
        created_at: row.timestamp("created_at")?,
    })
}

pub(crate) fn read_runtime(row: &Row) -> Result<Option<Runtime>> {
    let Some(id) = row.try_get::<Option<Uuid>, _>("runtime_id")? else {
        return Ok(None);
    };
    Ok(Some(Runtime {
        id,
        agent_id: row.try_get("id")?,
        generation: row.try_get("runtime_generation")?,
        locator: row.try_get("runtime_locator")?,
        engine_owner: row.try_get("engine_owner")?,
    }))
}

fn read_grant(row: &Row) -> Result<Grant> {
    let mode = match row.try_get::<&str, _>("mode")? {
        "full_access" => PermissionMode::FullAccess,
        "scoped" => PermissionMode::Scoped,
        "ask" => PermissionMode::Ask,
        _ => return Err(Error::Forbidden),
    };
    Ok(Grant {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        purpose: match row.try_get::<&str, _>("purpose")? {
            "task" => GrantPurpose::Task,
            "improvement" => GrantPurpose::Improvement,
            _ => return Err(Error::Forbidden),
        },
        version: row.try_get("version")?,
        mode,
        scopes: serde_json::from_value(row.try_get("scopes")?)?,
        revoked_at: row.optional_timestamp("revoked_at")?,
    })
}

fn check_scopes(grant: &Grant, scopes: &[CapabilityScope]) -> Result<()> {
    if grant.mode == PermissionMode::FullAccess
        || scopes
            .iter()
            .all(|required| grant.scopes.contains(required))
    {
        return Ok(());
    }
    Err(Error::Forbidden)
}

fn mode_str(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::FullAccess => "full_access",
        PermissionMode::Scoped => "scoped",
        PermissionMode::Ask => "ask",
    }
}

fn purpose_str(purpose: GrantPurpose) -> &'static str {
    match purpose {
        GrantPurpose::Task => "task",
        GrantPurpose::Improvement => "improvement",
    }
}

pub(crate) fn positive_duration(duration: SignedDuration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .ok()
        .filter(|&millis| millis > 0)
        .ok_or_else(|| {
            Error::InvalidCommand(
                "lease duration must be positive and fit in i64 milliseconds".into(),
            )
        })
}

fn command_aad(agent_id: Uuid, request_id: Uuid) -> Vec<u8> {
    [
        b"gesang:command:v1".as_slice(),
        agent_id.as_bytes(),
        request_id.as_bytes(),
    ]
    .concat()
}

fn receipt_aad(agent_id: Uuid, request_id: Uuid) -> Vec<u8> {
    [
        b"gesang:receipt:v1".as_slice(),
        agent_id.as_bytes(),
        request_id.as_bytes(),
    ]
    .concat()
}
