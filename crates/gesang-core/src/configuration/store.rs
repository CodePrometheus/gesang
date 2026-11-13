use std::{collections::BTreeMap, sync::Arc};

use hmac::Mac;
use serde_json::{Value, json};
use sqlx::{PgConnection, Row, postgres::PgRow};
use uuid::Uuid;
use zeroize::Zeroizing;

use super::{
    AdoptionBoundary, Configuration, ConfigurationAccess, ConfigurationChange, ConfigurationKey,
    ConfigurationRevision, ConfigurationScope, SecretInput, SecretReference, key::add_part,
};
use crate::{Error, Result, Store};

/// Durable typed configuration with atomic activation and independently revocable secret versions.
#[derive(Clone, Debug)]
pub struct ConfigurationCenter {
    store: Store,
    key: Arc<ConfigurationKey>,
}

impl ConfigurationCenter {
    /// Uses the platform store and a host-owned key dedicated to configuration custody.
    pub fn new(store: Store, key: ConfigurationKey) -> Self {
        Self {
            store,
            key: Arc::new(key),
        }
    }

    /// Prepares immutable validated content and encrypted secrets without changing active settings.
    pub async fn prepare<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        change: &ConfigurationChange<T>,
    ) -> Result<ConfigurationRevision<T>> {
        self.prepare_inner(access, scope, change, None).await
    }

    async fn prepare_inner<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        change: &ConfigurationChange<T>,
        rollback_of: Option<i64>,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        if T::COMPONENT.is_empty()
            || T::SCHEMA_VERSION == 0
            || change
                .expected_revision
                .is_some_and(|revision| revision <= 0)
            || change.secrets.keys().any(String::is_empty)
        {
            return Err(Error::InvalidCommand(
                "invalid configuration identity".into(),
            ));
        }
        change
            .values
            .validate(scope, &change.secrets.keys().cloned().collect())?;
        let mut values = serde_json::to_value(&change.values)?;
        values.sort_all_objects();
        let actor = access.audit();
        let mut metadata = json!({"scope":scope.key(),"component":T::COMPONENT,"schemaVersion":T::SCHEMA_VERSION,
            "boundary":T::ADOPTION_BOUNDARY,"expectedRevision":change.expected_revision,"values":values,"rollbackOf":rollback_of,"actor":actor});
        metadata.sort_all_objects();
        let mut fingerprint = self.key.mac()?;
        add_part(&mut fingerprint, b"gesang:configuration-request:v1");
        add_part(&mut fingerprint, &serde_json::to_vec(&metadata)?);
        for (name, secret) in &change.secrets {
            add_part(&mut fingerprint, name.as_bytes());
            match secret {
                SecretInput::Keep(reference) => {
                    add_part(&mut fingerprint, b"keep");
                    add_part(&mut fingerprint, reference.id.as_bytes());
                }
                SecretInput::Set(value) => {
                    add_part(&mut fingerprint, b"set");
                    add_part(&mut fingerprint, value);
                }
            }
        }
        let mut tx = self.store.begin().await?;
        sqlx::query("INSERT INTO gesang_configuration_streams(id,scope_key,owner_id,component) VALUES($1,$2,$3,$4) ON CONFLICT(scope_key,component) DO NOTHING")
            .bind(Uuid::new_v4()).bind(scope.key()).bind(scope.owner()).bind(T::COMPONENT).execute(&mut *tx).await?;
        let stream = stream::<T>(&mut tx, scope, true).await?;
        let stream_id: Uuid = stream.try_get("id")?;
        if let Some(row) = sqlx::query(
            "SELECT * FROM gesang_configuration_revisions WHERE stream_id=$1 AND operation_id=$2",
        )
        .bind(stream_id)
        .bind(change.operation_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            if fingerprint
                .verify_slice(&row.try_get::<Vec<u8>, _>("input_digest")?)
                .is_err()
            {
                return Err(Error::RequestConflict);
            }
            let revision = revision::<T>(&row, scope)?;
            tx.commit().await?;
            return Ok(revision);
        }
        if stream.try_get::<Option<i64>, _>("active_revision")? != change.expected_revision {
            return Err(Error::VersionConflict);
        }
        let number: i64 = stream.try_get("next_revision")?;
        let successor = number
            .checked_add(1)
            .ok_or_else(|| Error::InvalidCommand("configuration revision exhausted".into()))?;
        let mut references = BTreeMap::new();
        for (name, input) in &change.secrets {
            let reference = match input {
                SecretInput::Keep(reference) => {
                    self.secret(&mut tx, stream_id, name, reference).await?;
                    *reference
                }
                SecretInput::Set(value) => {
                    let reference = SecretReference { id: Uuid::new_v4() };
                    let sealed = self
                        .key
                        .seal(&secret_aad(stream_id, name, reference.id), value)?;
                    sqlx::query("INSERT INTO gesang_configuration_secrets(id,stream_id,name,sealed_value) VALUES($1,$2,$3,$4)")
                        .bind(reference.id).bind(stream_id).bind(name).bind(sealed).execute(&mut *tx).await?;
                    reference
                }
            };
            references.insert(name.clone(), reference);
        }
        let row = sqlx::query("INSERT INTO gesang_configuration_revisions(stream_id,revision,operation_id,expected_revision,schema_version,adoption_boundary,input_digest,values,secret_references,rollback_of,prepared_by) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *")
            .bind(stream_id).bind(number).bind(change.operation_id).bind(change.expected_revision)
            .bind(i64::from(T::SCHEMA_VERSION)).bind(T::ADOPTION_BOUNDARY.as_str()).bind(fingerprint.finalize().into_bytes().to_vec())
            .bind(values).bind(serde_json::to_value(references)?).bind(rollback_of).bind(&actor).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE gesang_configuration_streams SET next_revision=$1 WHERE id=$2")
            .bind(successor)
            .bind(stream_id)
            .execute(&mut *tx)
            .await?;
        event(
            &mut tx,
            stream_id,
            "prepared",
            &actor,
            json!({"revision":number,"operationId":change.operation_id,"rollbackOf":rollback_of}),
        )
        .await?;
        let revision = revision::<T>(&row, scope)?;
        tx.commit().await?;
        Ok(revision)
    }

    /// Activates one prepared revision only if its expected published predecessor remains current.
    pub async fn publish<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        operation_id: Uuid,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        let mut tx = self.store.begin().await?;
        let stream = stream::<T>(&mut tx, scope, true).await?;
        let stream_id = stream.try_get("id")?;
        let row = sqlx::query(
            "SELECT * FROM gesang_configuration_revisions WHERE stream_id=$1 AND operation_id=$2",
        )
        .bind(stream_id)
        .bind(operation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(Error::NotFound)?;
        let prepared = revision::<T>(&row, scope)?;
        if prepared.published_at.is_some() {
            tx.commit().await?;
            return Ok(prepared);
        }
        if prepared.expected_revision != stream.try_get::<Option<i64>, _>("active_revision")? {
            return Err(Error::VersionConflict);
        }
        self.check_secrets(&mut tx, stream_id, &prepared.secrets)
            .await?;
        let actor = access.audit();
        let row = sqlx::query("UPDATE gesang_configuration_revisions SET published_by=$1,published_at=clock_timestamp() WHERE stream_id=$2 AND revision=$3 RETURNING *")
            .bind(&actor).bind(stream_id).bind(prepared.revision).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE gesang_configuration_streams SET active_revision=$1 WHERE id=$2")
            .bind(prepared.revision)
            .bind(stream_id)
            .execute(&mut *tx)
            .await?;
        event(&mut tx, stream_id, "published", &actor, json!({"revision":prepared.revision,"operationId":operation_id,"previous":prepared.expected_revision})).await?;
        let published = revision::<T>(&row, scope)?;
        tx.commit().await?;
        Ok(published)
    }

    /// Reads the current published configuration; prepared drafts never become implicit defaults.
    pub async fn current<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        let mut tx = self.store.pool().begin().await?;
        let stream = stream::<T>(&mut tx, scope, false).await?;
        let number = stream
            .try_get::<Option<i64>, _>("active_revision")?
            .ok_or(Error::NotFound)?;
        let current = published::<T>(&mut tx, stream.try_get("id")?, number, scope).await?;
        tx.commit().await?;
        Ok(current)
    }

    /// Adopts current live settings atomically without serializing unrelated consumers on publication locks.
    pub async fn current_for_consumer<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        consumer_id: Uuid,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        if T::ADOPTION_BOUNDARY != AdoptionBoundary::Live {
            return Err(Error::InvalidCommand(
                "automatic adoption requires a live configuration boundary".into(),
            ));
        }
        let mut tx = self.store.begin().await?;
        let stream = stream::<T>(&mut tx, scope, false).await?;
        let stream_id: Uuid = stream.try_get("id")?;
        let number = stream
            .try_get::<Option<i64>, _>("active_revision")?
            .ok_or(Error::NotFound)?;
        let value = published::<T>(&mut tx, stream_id, number, scope).await?;
        self.check_secrets(&mut tx, stream_id, &value.secrets)
            .await?;
        let previous = sqlx::query_scalar::<_, i64>("SELECT revision FROM gesang_configuration_adoptions WHERE stream_id=$1 AND consumer_id=$2 FOR UPDATE")
            .bind(stream_id).bind(consumer_id).fetch_optional(&mut *tx).await?;
        if previous != Some(number) {
            let actor = access.audit();
            let changed = sqlx::query("INSERT INTO gesang_configuration_adoptions(stream_id,consumer_id,revision,adopted_by) VALUES($1,$2,$3,$4) ON CONFLICT(stream_id,consumer_id) DO UPDATE SET revision=EXCLUDED.revision,adopted_by=EXCLUDED.adopted_by,adopted_at=clock_timestamp() WHERE gesang_configuration_adoptions.revision<>EXCLUDED.revision")
                .bind(stream_id).bind(consumer_id).bind(number).bind(&actor).execute(&mut *tx).await?.rows_affected();
            if changed != 0 {
                event(&mut tx, stream_id, "adopted", &actor, json!({"consumerId":consumer_id,"revision":number,"previous":previous,"boundary":T::ADOPTION_BOUNDARY})).await?;
            }
        }
        tx.commit().await?;
        Ok(value)
    }

    /// Reads a specific published revision without treating it as the current active settings.
    pub async fn history<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        number: i64,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        let mut tx = self.store.pool().begin().await?;
        let stream = stream::<T>(&mut tx, scope, false).await?;
        let value = published::<T>(&mut tx, stream.try_get("id")?, number, scope).await?;
        tx.commit().await?;
        Ok(value)
    }

    /// Prepares rollback as a new revision; active pointers and original history never move backward.
    pub async fn prepare_rollback<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        operation_id: Uuid,
        expected_revision: i64,
        source_revision: i64,
    ) -> Result<ConfigurationRevision<T>> {
        let source = self.history::<T>(access, scope, source_revision).await?;
        let change = ConfigurationChange {
            operation_id,
            expected_revision: Some(expected_revision),
            values: source.values,
            secrets: source
                .secrets
                .into_iter()
                .map(|(name, reference)| (name, SecretInput::Keep(reference)))
                .collect(),
        };
        self.prepare_inner(access, scope, &change, Some(source_revision))
            .await
    }

    /// Resolves a published secret under current access and revocation, including for pinned work.
    pub async fn resolve_secret<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        number: i64,
        name: &str,
    ) -> Result<Zeroizing<Vec<u8>>> {
        access.check(scope)?;
        let mut tx = self.store.pool().begin().await?;
        let stream = stream::<T>(&mut tx, scope, false).await?;
        if T::ADOPTION_BOUNDARY == AdoptionBoundary::Live
            && stream.try_get::<Option<i64>, _>("active_revision")? != Some(number)
        {
            return Err(Error::VersionConflict);
        }
        let stream_id = stream.try_get("id")?;
        let value = published::<T>(&mut tx, stream_id, number, scope).await?;
        let reference = value.secrets.get(name).ok_or(Error::NotFound)?;
        let secret = self.secret(&mut tx, stream_id, name, reference).await?;
        tx.commit().await?;
        Ok(secret)
    }

    /// Revokes one exact secret version immediately without rewriting published configuration.
    pub async fn revoke_secret<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        name: &str,
        reference: SecretReference,
    ) -> Result<()> {
        access.check(scope)?;
        let mut tx = self.store.begin().await?;
        let stream = stream::<T>(&mut tx, scope, true).await?;
        let stream_id: Uuid = stream.try_get("id")?;
        let row = sqlx::query("SELECT revoked_at FROM gesang_configuration_secrets WHERE id=$1 AND stream_id=$2 AND name=$3")
            .bind(reference.id).bind(stream_id).bind(name).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
        if row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("revoked_at")?
            .is_none()
        {
            sqlx::query(
                "UPDATE gesang_configuration_secrets SET revoked_at=clock_timestamp() WHERE id=$1",
            )
            .bind(reference.id)
            .execute(&mut *tx)
            .await?;
            event(
                &mut tx,
                stream_id,
                "secret_revoked",
                &access.audit(),
                json!({"secretId":reference.id,"name":name}),
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Records successful application at the declared boundary, using a compare-and-set consumer revision.
    pub async fn adopt<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        consumer_id: Uuid,
        number: i64,
        expected_adoption: Option<i64>,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        let mut tx = self.store.begin().await?;
        let stream = stream::<T>(&mut tx, scope, true).await?;
        let stream_id: Uuid = stream.try_get("id")?;
        let current = sqlx::query_scalar::<_, i64>("SELECT revision FROM gesang_configuration_adoptions WHERE stream_id=$1 AND consumer_id=$2")
            .bind(stream_id).bind(consumer_id).fetch_optional(&mut *tx).await?;
        let value = published::<T>(&mut tx, stream_id, number, scope).await?;
        let active: Option<i64> = stream.try_get("active_revision")?;
        if active != Some(number)
            && (current != Some(number) || T::ADOPTION_BOUNDARY == AdoptionBoundary::Live)
        {
            return Err(Error::VersionConflict);
        }
        if current != Some(number) && current != expected_adoption {
            return Err(Error::VersionConflict);
        }
        self.check_secrets(&mut tx, stream_id, &value.secrets)
            .await?;
        if current != Some(number) {
            let actor = access.audit();
            sqlx::query("INSERT INTO gesang_configuration_adoptions(stream_id,consumer_id,revision,adopted_by) VALUES($1,$2,$3,$4) ON CONFLICT(stream_id,consumer_id) DO UPDATE SET revision=EXCLUDED.revision,adopted_by=EXCLUDED.adopted_by,adopted_at=clock_timestamp()")
                .bind(stream_id).bind(consumer_id).bind(number).bind(&actor).execute(&mut *tx).await?;
            event(&mut tx, stream_id, "adopted", &actor, json!({"consumerId":consumer_id,"revision":number,"previous":current,"boundary":T::ADOPTION_BOUNDARY})).await?;
        }
        tx.commit().await?;
        Ok(value)
    }

    /// Reads the last recorded revision for compare-and-set adoption, including superseded pins.
    pub async fn adoption_revision<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        consumer_id: Uuid,
    ) -> Result<Option<i64>> {
        access.check(scope)?;
        let mut tx = self.store.pool().begin().await?;
        let stream = stream::<T>(&mut tx, scope, false).await?;
        let number = sqlx::query_scalar("SELECT revision FROM gesang_configuration_adoptions WHERE stream_id=$1 AND consumer_id=$2")
            .bind(stream.try_get::<Uuid, _>("id")?).bind(consumer_id).fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(number)
    }

    /// Reads an existing consumer pin; live settings reject pins superseded by publication.
    pub async fn adopted<T: Configuration>(
        &self,
        access: &ConfigurationAccess,
        scope: &ConfigurationScope,
        consumer_id: Uuid,
    ) -> Result<ConfigurationRevision<T>> {
        access.check(scope)?;
        let mut tx = self.store.pool().begin().await?;
        let stream = stream::<T>(&mut tx, scope, false).await?;
        let stream_id: Uuid = stream.try_get("id")?;
        let number = sqlx::query_scalar::<_, i64>("SELECT revision FROM gesang_configuration_adoptions WHERE stream_id=$1 AND consumer_id=$2")
            .bind(stream_id).bind(consumer_id).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
        if T::ADOPTION_BOUNDARY == AdoptionBoundary::Live
            && stream.try_get::<Option<i64>, _>("active_revision")? != Some(number)
        {
            return Err(Error::VersionConflict);
        }
        let value = published::<T>(&mut tx, stream_id, number, scope).await?;
        self.check_secrets(&mut tx, stream_id, &value.secrets)
            .await?;
        tx.commit().await?;
        Ok(value)
    }

    async fn check_secrets(
        &self,
        connection: &mut PgConnection,
        stream_id: Uuid,
        references: &BTreeMap<String, SecretReference>,
    ) -> Result<()> {
        for (name, reference) in references {
            self.secret(connection, stream_id, name, reference).await?;
        }
        Ok(())
    }

    async fn secret(
        &self,
        connection: &mut PgConnection,
        stream_id: Uuid,
        name: &str,
        reference: &SecretReference,
    ) -> Result<Zeroizing<Vec<u8>>> {
        let sealed = sqlx::query_scalar::<_, Vec<u8>>("SELECT sealed_value FROM gesang_configuration_secrets WHERE id=$1 AND stream_id=$2 AND name=$3 AND revoked_at IS NULL")
            .bind(reference.id).bind(stream_id).bind(name).fetch_optional(connection).await?.ok_or(Error::Forbidden)?;
        self.key
            .open(&secret_aad(stream_id, name, reference.id), &sealed)
    }
}

async fn stream<T: Configuration>(
    connection: &mut PgConnection,
    scope: &ConfigurationScope,
    exclusive: bool,
) -> Result<PgRow> {
    let query = if exclusive {
        "SELECT * FROM gesang_configuration_streams WHERE scope_key=$1 AND component=$2 FOR UPDATE"
    } else {
        "SELECT * FROM gesang_configuration_streams WHERE scope_key=$1 AND component=$2 FOR SHARE"
    };
    sqlx::query(query)
        .bind(scope.key())
        .bind(T::COMPONENT)
        .fetch_optional(connection)
        .await?
        .ok_or(Error::NotFound)
}

async fn published<T: Configuration>(
    connection: &mut PgConnection,
    stream_id: Uuid,
    number: i64,
    scope: &ConfigurationScope,
) -> Result<ConfigurationRevision<T>> {
    let row = sqlx::query("SELECT * FROM gesang_configuration_revisions WHERE stream_id=$1 AND revision=$2 AND published_at IS NOT NULL")
        .bind(stream_id).bind(number).fetch_optional(connection).await?.ok_or(Error::NotFound)?;
    revision::<T>(&row, scope)
}

fn revision<T: Configuration>(
    row: &PgRow,
    scope: &ConfigurationScope,
) -> Result<ConfigurationRevision<T>> {
    if row.try_get::<i64, _>("schema_version")? != i64::from(T::SCHEMA_VERSION)
        || row.try_get::<&str, _>("adoption_boundary")? != T::ADOPTION_BOUNDARY.as_str()
    {
        return Err(Error::VersionConflict);
    }
    let value: T = serde_json::from_value(row.try_get("values")?)?;
    let secrets: BTreeMap<String, SecretReference> =
        serde_json::from_value(row.try_get("secret_references")?)?;
    value.validate(scope, &secrets.keys().cloned().collect())?;
    Ok(ConfigurationRevision {
        revision: row.try_get("revision")?,
        operation_id: row.try_get("operation_id")?,
        expected_revision: row.try_get("expected_revision")?,
        values: value,
        secrets,
        rollback_of: row.try_get("rollback_of")?,
        prepared_at: row.try_get("prepared_at")?,
        published_at: row.try_get("published_at")?,
    })
}

async fn event(
    connection: &mut PgConnection,
    stream_id: Uuid,
    kind: &str,
    actor: &Value,
    detail: Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO gesang_configuration_events(stream_id,kind,actor,detail) VALUES($1,$2,$3,$4)",
    )
    .bind(stream_id)
    .bind(kind)
    .bind(actor)
    .bind(detail)
    .execute(connection)
    .await?;
    Ok(())
}

fn secret_aad(stream_id: Uuid, name: &str, id: Uuid) -> Vec<u8> {
    [
        b"gesang:configuration-secret:v1\0".as_slice(),
        stream_id.as_bytes(),
        id.as_bytes(),
        name.as_bytes(),
    ]
    .concat()
}
