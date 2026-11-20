//! Immutable resource ownership and provider references, without a provider state machine.

mod queries;

use chrono::{DateTime, Utc};
use gesang_plugins::{
    Authority, BindingScope, InterfaceId, PackageVersion, PinSubject, PrincipalId, ServiceEndpoint,
    VersionPin,
};
use uuid::Uuid;

use crate::{Error, Result, Store, pins::pin_in};

/// The original resource route; its presence does not assert that provisioning has completed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceBinding {
    /// Stable logical resource identity allocated before provider effects.
    pub id: Uuid,
    /// Exact business boundary that owns this resource reference.
    pub binding: BindingScope,
    /// Original owner or service account, not an authentication claim.
    pub principal: PrincipalId,
    /// Service contract used when this resource was requested.
    pub interface: InterfaceId,
    /// Original caller-chosen request identity within its principal and interface.
    pub request_id: Uuid,
    /// Host-derived digest of immutable creation arguments, excluding current provider selection.
    pub request_digest: [u8; 32],
    /// Exact original installation, configuration, and export used for recovery and stop.
    pub endpoint: ServiceEndpoint,
    /// Installer-verified package retained atomically with the resource binding.
    pub package: PackageVersion,
    /// Database-observed reservation time; not a provisioning completion timestamp.
    pub created_at: DateTime<Utc>,
}

pub(crate) struct ResourceReservation<'a> {
    pub caller: &'a Authority,
    pub interface: &'a InterfaceId,
    pub request_id: Uuid,
    pub request_digest: [u8; 32],
    pub endpoint: &'a ServiceEndpoint,
    pub package: &'a PackageVersion,
}

impl Store {
    /// Reads the original provider route within the caller's business and principal scope.
    ///
    /// A binding records ownership and routing, not successful provisioning or
    /// permission to act. The service must check current action permissions before
    /// reading private state or admitting an external operation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] for both an unknown resource and a resource
    /// belonging to another principal or binding. Invalid stored metadata and
    /// database failures are preserved as errors.
    ///
    /// # Cancel safety
    ///
    /// The read does not modify the binding or start provider work.
    pub async fn resource_binding(&self, caller: &Authority, id: Uuid) -> Result<ResourceBinding> {
        queries::read(self.database.executor(), caller, id)
            .await?
            .ok_or(Error::NotFound)?
            .try_into()
    }

    /// Looks up the resource reserved by an original request in the caller's scope.
    ///
    /// This enables receipt recovery when the creation response, including its
    /// resource identifier, was lost. The service still authorizes access to the
    /// corresponding operation before returning any private result.
    ///
    /// # Errors
    ///
    /// Returns `None` when no matching reservation exists. Invalid metadata and
    /// storage failures remain errors rather than absence.
    pub async fn resource_request(
        &self,
        caller: &Authority,
        interface: &InterfaceId,
        request_id: Uuid,
    ) -> Result<Option<ResourceBinding>> {
        queries::request(self.database.executor(), caller, interface, request_id)
            .await?
            .map(TryInto::try_into)
            .transpose()
    }

    pub(crate) async fn reserve_resource(
        &self,
        reservation: ResourceReservation<'_>,
    ) -> Result<ResourceBinding> {
        let mut tx = self.database.begin().await?;
        let actual: ResourceBinding = queries::reserve(tx.executor(), Uuid::new_v4(), &reservation)
            .await?
            .try_into()?;
        if actual.request_digest != reservation.request_digest {
            return Err(Error::RequestConflict);
        }
        pin_in(
            &mut tx.coordination(),
            VersionPin {
                subject: PinSubject::Resource {
                    binding: actual.binding,
                    resource_id: actual.id,
                },
                reference_id: actual.id,
                package: actual.package.package.clone(),
                version: actual.package.version.clone(),
                digest: actual.package.digest.clone(),
            },
        )
        .await?;
        tx.commit().await?;
        Ok(actual)
    }
}

impl TryFrom<queries::BindingRow> for ResourceBinding {
    type Error = Error;

    fn try_from(row: queries::BindingRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            binding: row.binding.0,
            principal: row.principal.0,
            interface: InterfaceId {
                name: row.interface_name,
                version: u32::try_from(row.interface_version)
                    .map_err(|_| Error::InvalidMetadata)?,
            },
            request_id: row.request_id,
            request_digest: row
                .request_digest
                .try_into()
                .map_err(|_| Error::InvalidMetadata)?,
            endpoint: row.endpoint.0,
            package: row.package.0,
            created_at: row.created_at,
        })
    }
}
