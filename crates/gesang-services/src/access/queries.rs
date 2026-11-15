use std::collections::BTreeSet;

use gesang_api::CapabilityScope;
use gesang_plugins::{BindingScope, PrincipalId};
use sqlx::{PgPool, Postgres, Transaction, types::Json};
use uuid::Uuid;

use super::{AuthorizationRef, CredentialGrant};

pub(super) struct CredentialRow {
    pub id: Uuid,
    pub binding: Json<BindingScope>,
    pub principal: Json<PrincipalId>,
    pub scopes: Json<BTreeSet<CapabilityScope>>,
    pub revision: i64,
}

pub(super) async fn insert(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
    fingerprint: &[u8],
    grant: &CredentialGrant,
) -> sqlx::Result<bool> {
    let result = sqlx::query!(
        r#"
        INSERT INTO gesang_service_credentials (
            id, token_hash, binding, principal, scopes, expires_at
        )
        SELECT $1, $2, $3, $4, $5, $6
        WHERE $6 > clock_timestamp()
        "#,
        id,
        fingerprint,
        Json(grant.binding) as _,
        Json(grant.principal) as _,
        Json(&grant.scopes) as _,
        grant.expires_at,
    )
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

pub(super) async fn authenticate(
    pool: &PgPool,
    fingerprint: &[u8],
) -> sqlx::Result<Option<CredentialRow>> {
    sqlx::query_as!(
        CredentialRow,
        r#"
        SELECT id,
               binding AS "binding: _",
               principal AS "principal: _",
               scopes AS "scopes: _",
               revision
        FROM gesang_service_credentials
        WHERE token_hash = $1
          AND revoked_at IS NULL
          AND expires_at > clock_timestamp()
        "#,
        fingerprint,
    )
    .fetch_optional(pool)
    .await
}

pub(super) async fn lock_grant(
    transaction: &mut Transaction<'_, Postgres>,
    reference: &AuthorizationRef,
) -> sqlx::Result<Option<CredentialRow>> {
    sqlx::query_as!(
        CredentialRow,
        r#"
        SELECT id,
               binding AS "binding: _",
               principal AS "principal: _",
               scopes AS "scopes: _",
               revision
        FROM gesang_service_credentials
        WHERE id = $1
          AND revision = $2
          AND revoked_at IS NULL
          AND expires_at > clock_timestamp()
        FOR SHARE
        "#,
        reference.id,
        reference.revision,
    )
    .fetch_optional(&mut **transaction)
    .await
}

pub(super) async fn replace(
    transaction: &mut Transaction<'_, Postgres>,
    reference: &AuthorizationRef,
    scopes: &BTreeSet<CapabilityScope>,
) -> sqlx::Result<Option<i64>> {
    sqlx::query_scalar!(
        r#"
        UPDATE gesang_service_credentials
        SET scopes = $1,
            revision = revision + 1
        WHERE id = $2
          AND revision = $3
          AND revoked_at IS NULL
          AND expires_at > clock_timestamp()
        RETURNING revision
        "#,
        Json(scopes) as _,
        reference.id,
        reference.revision,
    )
    .fetch_optional(&mut **transaction)
    .await
}

pub(super) async fn revoke(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
) -> sqlx::Result<bool> {
    let existing = sqlx::query_scalar!(
        r#"
        UPDATE gesang_service_credentials
        SET revoked_at = COALESCE(revoked_at, clock_timestamp())
        WHERE id = $1
        RETURNING id
        "#,
        id,
    )
    .fetch_optional(&mut **transaction)
    .await?;
    Ok(existing.is_some())
}
