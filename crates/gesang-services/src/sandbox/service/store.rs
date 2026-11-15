use std::num::NonZeroU32;

use gesang_core::ResourceBinding;
use gesang_plugins::ServiceEndpoint;
use sqlx::{PgPool, Postgres, Transaction, types::Json};
use uuid::Uuid;

use crate::{Error, Result, access::StoredCaller};

use super::{
    super::OperationStatus,
    state::{Action, Instance, InstanceRow, OperationRow},
};

pub(super) async fn insert(
    transaction: &mut Transaction<'_, Postgres>,
    binding: &ResourceBinding,
    profile: &str,
    creator: &StoredCaller,
) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO gesang_sandbox_instances (id, endpoint, profile, creator, launch)
        VALUES ($1, $2, $3, $4, 'queued')
        ON CONFLICT (id) DO NOTHING
        "#,
        binding.id,
        Json(&binding.endpoint) as _,
        profile,
        Json(creator) as _,
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) async fn read(pool: &PgPool, id: Uuid) -> Result<Instance> {
    sqlx::query_as!(
        InstanceRow,
        r#"
        SELECT id, endpoint AS "endpoint: _", profile, creator AS "creator: _",
               launch, physical_id, stop_requested, removal, stopper AS "stopper: _"
        FROM gesang_sandbox_instances
        WHERE id = $1
        "#,
        id,
    )
    .fetch_optional(pool)
    .await?
    .ok_or(Error::NotFound)?
    .try_into()
}

pub(super) async fn lock(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> Result<Instance> {
    sqlx::query_as!(
        InstanceRow,
        r#"
        SELECT id, endpoint AS "endpoint: _", profile, creator AS "creator: _",
               launch, physical_id, stop_requested, removal, stopper AS "stopper: _"
        FROM gesang_sandbox_instances
        WHERE id = $1
        FOR UPDATE
        "#,
        id,
    )
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(Error::NotFound)?
    .try_into()
}

pub(super) async fn save(
    transaction: &mut Transaction<'_, Postgres>,
    instance: &Instance,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE gesang_sandbox_instances
        SET launch = $2,
            physical_id = $3,
            stop_requested = $4,
            removal = $5,
            stopper = $6,
            updated_at = clock_timestamp()
        WHERE id = $1
        "#,
        instance.id,
        instance.launch.name(),
        instance.physical_id.as_ref().map(|id| id.as_str()),
        instance.stop_requested,
        instance.removal.name(),
        instance.stopper.as_ref().map(Json) as _,
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) async fn admit(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    request_id: Uuid,
    action: Action,
    caller: &StoredCaller,
) -> Result<OperationRow> {
    let row = sqlx::query_as!(
        OperationRow,
        r#"
        INSERT INTO gesang_sandbox_operations (
            resource_id, request_id, action, status, caller
        )
        VALUES ($1, $2, $3, 'accepted', $4)
        ON CONFLICT (resource_id, request_id)
        DO UPDATE SET resource_id = gesang_sandbox_operations.resource_id
        RETURNING resource_id, request_id, action, status, problem, caller AS "caller: _"
        "#,
        resource_id,
        request_id,
        action.name(),
        Json(caller) as _,
    )
    .fetch_one(&mut **transaction)
    .await?;
    if row.action != action.name() {
        return Err(Error::Conflict);
    }
    Ok(row)
}

pub(super) async fn operation(
    pool: &PgPool,
    resource_id: Uuid,
    request_id: Uuid,
) -> Result<OperationRow> {
    sqlx::query_as!(
        OperationRow,
        r#"
        SELECT resource_id, request_id, action, status, problem, caller AS "caller: _"
        FROM gesang_sandbox_operations
        WHERE resource_id = $1 AND request_id = $2
        "#,
        resource_id,
        request_id,
    )
    .fetch_optional(pool)
    .await?
    .ok_or(Error::NotFound)
}

pub(super) async fn next_stop(
    pool: &PgPool,
    resource_id: Uuid,
    after: Option<Uuid>,
) -> Result<Option<OperationRow>> {
    Ok(sqlx::query_as!(
        OperationRow,
        r#"
        SELECT resource_id, request_id, action, status, problem, caller AS "caller: _"
        FROM gesang_sandbox_operations
        WHERE resource_id = $1 AND action = 'stop' AND status IN ('accepted', 'unknown')
          AND ($2::uuid IS NULL OR request_id > $2)
        ORDER BY request_id
        LIMIT 1
        "#,
        resource_id,
        after,
    )
    .fetch_optional(pool)
    .await?)
}

pub(super) async fn finish(
    transaction: &mut Transaction<'_, Postgres>,
    resource_id: Uuid,
    action: Action,
    status: OperationStatus,
    problem: Option<&str>,
    request_id: Option<Uuid>,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE gesang_sandbox_operations
        SET status = $3, problem = $4, updated_at = clock_timestamp()
        WHERE resource_id = $1 AND action = $2
          AND status IN ('accepted', 'unknown')
          AND ($5::uuid IS NULL OR request_id = $5)
        "#,
        resource_id,
        action.name(),
        status.name(),
        problem,
        request_id,
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(super) async fn pending(
    pool: &PgPool,
    endpoint: &ServiceEndpoint,
    after: Option<Uuid>,
    batch_size: NonZeroU32,
) -> Result<Vec<Uuid>> {
    Ok(sqlx::query_scalar!(
        r#"
        SELECT id FROM gesang_sandbox_instances
        WHERE endpoint = $1 AND ($2::uuid IS NULL OR id > $2)
          AND removal <> 'stopped'
          AND (stop_requested OR launch IN ('queued', 'issued', 'allocated', 'starting'))
        ORDER BY id
        LIMIT $3
        "#,
        Json(endpoint) as _,
        after,
        i64::from(batch_size.get()),
    )
    .fetch_all(pool)
    .await?)
}
