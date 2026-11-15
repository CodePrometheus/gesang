use chrono::{DateTime, Utc};
use gesang_plugins::{
    Authority, BindingScope, InterfaceId, PackageVersion, PrincipalId, ServiceEndpoint,
};
use sqlx::{PgPool, Postgres, Transaction, types::Json};
use uuid::Uuid;

use super::ResourceReservation;

pub(super) struct BindingRow {
    pub id: Uuid,
    pub binding: Json<BindingScope>,
    pub principal: Json<PrincipalId>,
    pub interface_name: String,
    pub interface_version: i64,
    pub request_id: Uuid,
    pub request_digest: Vec<u8>,
    pub endpoint: Json<ServiceEndpoint>,
    pub package: Json<PackageVersion>,
    pub created_at: DateTime<Utc>,
}

pub(super) async fn read(
    pool: &PgPool,
    caller: &Authority,
    id: Uuid,
) -> sqlx::Result<Option<BindingRow>> {
    sqlx::query_as!(
        BindingRow,
        r#"
        SELECT id,
               binding AS "binding: _",
               principal AS "principal: _",
               interface_name, interface_version, request_id, request_digest,
               endpoint AS "endpoint: _",
               package AS "package: _",
               created_at
        FROM gesang_resource_bindings
        WHERE id = $1 AND binding = $2 AND principal = $3
        "#,
        id,
        Json(caller.binding()) as _,
        Json(caller.principal()) as _,
    )
    .fetch_optional(pool)
    .await
}

pub(super) async fn request(
    pool: &PgPool,
    caller: &Authority,
    interface: &InterfaceId,
    request_id: Uuid,
) -> sqlx::Result<Option<BindingRow>> {
    sqlx::query_as!(
        BindingRow,
        r#"
        SELECT id,
               binding AS "binding: _",
               principal AS "principal: _",
               interface_name, interface_version, request_id, request_digest,
               endpoint AS "endpoint: _",
               package AS "package: _",
               created_at
        FROM gesang_resource_bindings
        WHERE binding = $1
          AND principal = $2
          AND interface_name = $3
          AND interface_version = $4
          AND request_id = $5
        "#,
        Json(caller.binding()) as _,
        Json(caller.principal()) as _,
        &interface.name,
        i64::from(interface.version),
        request_id,
    )
    .fetch_optional(pool)
    .await
}

pub(super) async fn reserve(
    transaction: &mut Transaction<'_, Postgres>,
    id: Uuid,
    reservation: &ResourceReservation<'_>,
) -> sqlx::Result<BindingRow> {
    sqlx::query_file_as!(
        BindingRow,
        "queries/resources/reserve.sql",
        id,
        Json(reservation.caller.binding()) as _,
        Json(reservation.caller.principal()) as _,
        &reservation.interface.name,
        i64::from(reservation.interface.version),
        reservation.request_id,
        reservation.request_digest.as_slice(),
        Json(reservation.endpoint) as _,
        Json(reservation.package) as _,
    )
    .fetch_one(&mut **transaction)
    .await
}
