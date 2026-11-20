use gesang_plugins::{
    Authority, BindingScope, InterfaceId, PackageVersion, PrincipalId, ServiceEndpoint,
};
use jiff::Timestamp;
use sqlx::types::Json;
use uuid::Uuid;

use crate::store::database::{Executor, Row, query};

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
    pub created_at: Timestamp,
}

impl TryFrom<Row> for BindingRow {
    type Error = sqlx::Error;

    fn try_from(row: Row) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            binding: row.try_get("binding")?,
            principal: row.try_get("principal")?,
            interface_name: row.try_get("interface_name")?,
            interface_version: row.try_get("interface_version")?,
            request_id: row.try_get("request_id")?,
            request_digest: row.try_get("request_digest")?,
            endpoint: row.try_get("endpoint")?,
            package: row.try_get("package")?,
            created_at: row.timestamp("created_at")?,
        })
    }
}

pub(super) async fn read(
    executor: Executor<'_>,
    caller: &Authority,
    id: Uuid,
) -> sqlx::Result<Option<BindingRow>> {
    query(
        executor,
        "SELECT id, binding, principal, interface_name, interface_version, request_id,
                request_digest, endpoint, package, created_at
         FROM gesang_resource_bindings WHERE id=$1 AND binding=$2 AND principal=$3",
    )
    .bind(id)
    .bind(Json(caller.binding()))
    .bind(Json(caller.principal()))
    .fetch_optional()
    .await?
    .map(TryInto::try_into)
    .transpose()
}

pub(super) async fn request(
    executor: Executor<'_>,
    caller: &Authority,
    interface: &InterfaceId,
    request_id: Uuid,
) -> sqlx::Result<Option<BindingRow>> {
    query(
        executor,
        "SELECT id, binding, principal, interface_name, interface_version, request_id,
                request_digest, endpoint, package, created_at
         FROM gesang_resource_bindings
         WHERE binding=$1 AND principal=$2 AND interface_name=$3
           AND interface_version=$4 AND request_id=$5",
    )
    .bind(Json(caller.binding()))
    .bind(Json(caller.principal()))
    .bind(&interface.name)
    .bind(i64::from(interface.version))
    .bind(request_id)
    .fetch_optional()
    .await?
    .map(TryInto::try_into)
    .transpose()
}

pub(super) async fn reserve(
    executor: Executor<'_>,
    id: Uuid,
    reservation: &ResourceReservation<'_>,
) -> sqlx::Result<BindingRow> {
    query(
        executor,
        include_str!("../../queries/resources/reserve.sql"),
    )
    .bind(id)
    .bind(Json(reservation.caller.binding()))
    .bind(Json(reservation.caller.principal()))
    .bind(&reservation.interface.name)
    .bind(i64::from(reservation.interface.version))
    .bind(reservation.request_id)
    .bind(reservation.request_digest.as_slice())
    .bind(Json(reservation.endpoint))
    .bind(Json(reservation.package))
    .fetch_one()
    .await?
    .try_into()
}
