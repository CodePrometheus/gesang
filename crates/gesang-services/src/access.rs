mod queries;

use std::collections::BTreeSet;

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use gesang_api::CapabilityScope;
use gesang_core::Store;
use gesang_plugins::{Authority, BindingScope, PrincipalId, ResourceScope};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{Error, Result};

const ISSUER: &str = "gesang.services.v1";

/// The authentication contract selected by a trusted application host.
#[async_trait::async_trait]
pub trait Authenticate: Send + Sync + std::fmt::Debug {
    /// Verifies bearer possession and returns authority for this invocation.
    ///
    /// The returned caller must come from the authority's current records. An
    /// untrusted request must never supply its own principal or grant reference.
    async fn authenticate(&self, bearer: &str) -> Result<Caller>;
}

/// The service contract used to resolve the host's configured authenticator.
#[must_use]
pub fn authentication_interface() -> gesang_plugins::InterfaceId {
    gesang_plugins::InterfaceId {
        name: "gesang.authentication".into(),
        version: 1,
    }
}

/// Secret bearer material, exposed only at authentication and credential-delivery boundaries.
pub struct Credential(Zeroizing<String>);

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Credential([redacted])")
    }
}

impl Credential {
    /// Accepts the canonical version-one 256-bit bearer encoding without logging its value.
    pub fn parse(value: String) -> Result<Self> {
        let value = Zeroizing::new(value);
        let encoded = value.strip_prefix("s1.").ok_or(Error::Unauthorized)?;
        let decoded = Zeroizing::new(
            BASE64_URL_SAFE_NO_PAD
                .decode(encoded)
                .map_err(|_| Error::Unauthorized)?,
        );
        let canonical = Zeroizing::new(BASE64_URL_SAFE_NO_PAD.encode(decoded.as_slice()));
        if decoded.len() != 32 || canonical.as_str() != encoded {
            return Err(Error::Unauthorized);
        }
        Ok(Self(value))
    }

    /// Exposes the bearer only to a protected transport or the authorized recipient.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    fn fingerprint(&self) -> Vec<u8> {
        let mut digest = Sha256::new();
        digest.update(b"gesang:service-credential:v1\0");
        digest.update(self.0.as_bytes());
        digest.finalize().to_vec()
    }
}

/// A durable authorization reference, which is not itself proof of possession or permission.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthorizationRef {
    /// Authority namespace whose current records must be checked at the service boundary.
    pub issuer: String,
    /// Original credential or delegated authorization identity.
    pub id: Uuid,
    /// Exact accepted revision; changed grants do not silently authorize old pending effects.
    pub revision: i64,
}

/// A host-authenticated invocation; request bodies cannot deserialize this type.
#[derive(Clone, Debug)]
pub struct Caller {
    authority: Authority,
    authorization: AuthorizationRef,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StoredCaller {
    pub binding: BindingScope,
    pub principal: PrincipalId,
    pub owner_scope: Option<ResourceScope>,
    pub authorization: AuthorizationRef,
    pub capabilities: BTreeSet<CapabilityScope>,
}

impl Caller {
    /// Constructs a call after trusted host authentication; providers must check current policy.
    #[must_use]
    pub fn verified(authority: Authority, authorization: AuthorizationRef) -> Self {
        Self {
            authority,
            authorization,
        }
    }

    /// The exact authenticated business boundary, principal, and capability ceiling.
    #[must_use]
    pub fn authority(&self) -> &Authority {
        &self.authority
    }

    /// Original authority record to recheck before admitting each external effect.
    #[must_use]
    pub fn authorization(&self) -> &AuthorizationRef {
        &self.authorization
    }

    pub(crate) fn stored(&self) -> StoredCaller {
        StoredCaller {
            binding: self.authority.binding(),
            principal: self.authority.principal(),
            owner_scope: self.authority.owner_scope().cloned(),
            authorization: self.authorization.clone(),
            capabilities: self.authority.capabilities().clone(),
        }
    }
}

/// Explicit host-approved scope and expiry for a new service credential.
#[derive(Clone, Debug)]
pub struct CredentialGrant {
    /// Exact deployment or business connection verified by the issuing host.
    pub binding: BindingScope,
    /// Verified stable owner or service account; issuance does not create an owner.
    pub principal: PrincipalId,
    /// Exact capability/resource pairs; no wildcard interpretation or implicit full access.
    pub scopes: BTreeSet<CapabilityScope>,
    /// Explicit expiry supplied by the host's credential policy.
    pub expires_at: Timestamp,
}

/// Concrete credential authentication and transactional authorization for default host services.
#[derive(Clone, Debug)]
pub struct ServiceAccess {
    store: Store,
}

impl ServiceAccess {
    /// Shares the platform transaction pool while owning separate service tables and migrations.
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    /// Applies numbered service migrations using a separate, stable component ledger.
    pub async fn migrate(&self) -> Result<()> {
        let mut migrations = sqlx::migrate!("./migrations");
        migrations.dangerous_set_table_name(include_str!("../migrations/table-name").trim());
        migrations.run(self.store.pool()).await?;
        Ok(())
    }

    /// Issues a credential with the host-approved scope and expiry in `grant`.
    ///
    /// Only a fingerprint is persisted. The returned bearer is the sole copy that
    /// can be delivered to its recipient; it cannot be recovered from the store.
    /// This method belongs behind the host's administrator authorization boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Invalid`] for empty scope identifiers or an expiry that is
    /// no longer in the future according to the database clock. Randomness and
    /// storage failures are propagated without issuing a usable result.
    ///
    /// # Cancel safety
    ///
    /// Cancellation before commit rolls the transaction back. Once commit begins,
    /// cancellation cannot establish whether the credential was stored. A stored
    /// credential whose bearer was not received cannot be recovered by repeating
    /// this call; repeating it issues a distinct credential.
    pub async fn issue(&self, grant: &CredentialGrant) -> Result<(AuthorizationRef, Credential)> {
        validate_scopes(&grant.scopes)?;
        let mut bytes = Zeroizing::new([0u8; 32]);
        getrandom::fill(bytes.as_mut_slice()).map_err(|_| Error::Randomness)?;
        let encoded = Zeroizing::new(BASE64_URL_SAFE_NO_PAD.encode(bytes.as_slice()));
        let mut text = Zeroizing::new(String::with_capacity(encoded.len() + 3));
        text.push_str("s1.");
        text.push_str(&encoded);
        let credential = Credential(text);
        let id = Uuid::new_v4();
        let mut tx = self.store.begin().await?;
        if !queries::insert(&mut tx, id, &credential.fingerprint(), grant).await? {
            return Err(Error::Invalid(
                "credential expiry must be in the future".into(),
            ));
        }
        tx.commit().await?;
        Ok((
            AuthorizationRef {
                issuer: ISSUER.into(),
                id,
                revision: 1,
            },
            credential,
        ))
    }

    /// Authenticates bearer possession against the current credential record.
    ///
    /// The returned [`Caller`] captures the exact grant revision and scope at this
    /// read. Services must call [`Self::authorize_in`] again when admitting an
    /// operation, because the credential can expire or be revoked afterward.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Unauthorized`] when the bearer is unknown, expired, or
    /// revoked. Storage failures remain errors; they are not converted into an
    /// authentication decision or answered from cached authority.
    ///
    /// # Cancel safety
    ///
    /// This method does not change the grant or start external work. Cancelling
    /// the read does not revoke the credential.
    pub async fn authenticate(&self, credential: &Credential) -> Result<Caller> {
        let row = queries::authenticate(self.store.pool(), &credential.fingerprint())
            .await?
            .ok_or(Error::Unauthorized)?;
        row.into_caller()
    }

    /// Authorizes one exact capability within the caller's current authority.
    ///
    /// The credential's shared row lock remains held by `transaction`. Persist
    /// the operation's admission in that same transaction, then commit before
    /// making an external call. Revocation and permission changes wait for this
    /// transaction to finish. All paths must lock credentials before operation
    /// records to preserve the lock order.
    ///
    /// The returned authority contains only `scope` and preserves the caller's
    /// narrower ownership boundary. A runtime-backed operation must additionally
    /// perform its kernel writer and delegation checks in this transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Forbidden`] for an issuer, principal, binding, revision,
    /// expiry, or capability mismatch. The transaction must use this service's
    /// authoritative database; storage errors are propagated.
    ///
    /// # Cancel safety
    ///
    /// Cancelling this future does not end the caller-owned transaction. The
    /// caller must still commit or roll it back, and must not hold its locks
    /// while waiting on a provider or a client connection.
    pub async fn authorize_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        caller: &Caller,
        scope: &CapabilityScope,
    ) -> Result<Authority> {
        if !caller.authority.capabilities().contains(scope) {
            return Err(Error::Forbidden);
        }
        self.authorize_record_in(
            transaction,
            caller.authority.binding(),
            caller.authority.principal(),
            &caller.authorization,
            scope,
        )
        .await?;
        caller
            .authority
            .restrict([scope.clone()])
            .map_err(|_| Error::Forbidden)
    }

    pub(crate) async fn resume_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        record: &StoredCaller,
        scope: &CapabilityScope,
    ) -> Result<Authority> {
        if !record.capabilities.contains(scope) {
            return Err(Error::Forbidden);
        }
        let authority = self
            .authorize_record_in(
                transaction,
                record.binding,
                record.principal,
                &record.authorization,
                scope,
            )
            .await?;
        match &record.owner_scope {
            Some(owner) => authority
                .narrow(owner.clone(), [scope.clone()])
                .map_err(|_| Error::Forbidden),
            None if matches!(record.principal, PrincipalId::Service { .. }) => Ok(authority),
            _ => Err(Error::Forbidden),
        }
    }

    pub(crate) async fn authorize_record_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        binding: BindingScope,
        principal: PrincipalId,
        reference: &AuthorizationRef,
        scope: &CapabilityScope,
    ) -> Result<Authority> {
        if reference.issuer != ISSUER {
            return Err(Error::Forbidden);
        }
        let current = queries::lock_grant(transaction, reference)
            .await?
            .ok_or(Error::Forbidden)?
            .into_caller()?;
        if current.authority.binding() != binding
            || current.authority.principal() != principal
            || !current.authority.capabilities().contains(scope)
        {
            return Err(Error::Forbidden);
        }
        current
            .authority
            .restrict([scope.clone()])
            .map_err(|_| Error::Forbidden)
    }

    /// Replaces permissions if `reference` still identifies the current grant.
    ///
    /// A successful replacement increments the revision. Pending operations that
    /// captured the previous revision must obtain fresh authorization; they do
    /// not silently inherit the new permission set.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Conflict`] when the revision is stale, expired, or revoked,
    /// and [`Error::Invalid`] for malformed scope identities. An unknown issuer
    /// is [`Error::Forbidden`].
    ///
    /// # Cancel safety
    ///
    /// Once commit begins, cancellation leaves its outcome uncertain. Read the
    /// credential's current revision before attempting another replacement.
    pub async fn replace(
        &self,
        reference: &AuthorizationRef,
        scopes: &BTreeSet<CapabilityScope>,
    ) -> Result<AuthorizationRef> {
        if reference.issuer != ISSUER {
            return Err(Error::Forbidden);
        }
        validate_scopes(scopes)?;
        let mut tx = self.store.begin().await?;
        let revision = queries::replace(&mut tx, reference, scopes)
            .await?
            .ok_or(Error::Conflict)?;
        tx.commit().await?;
        Ok(AuthorizationRef {
            issuer: ISSUER.into(),
            id: reference.id,
            revision,
        })
    }

    /// Revokes the credential identified by `id`, preserving its first revocation time.
    ///
    /// The update waits for transactions that already hold an admission lock.
    /// After commit, no new admission can use the credential. An external effect
    /// admitted earlier can still complete and must be reconciled independently.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if `id` does not exist. Repeating a successful
    /// revocation is allowed and does not change its recorded time.
    ///
    /// # Cancel safety
    ///
    /// A cancelled commit may already have succeeded. Retrying the same `id` is
    /// safe; it never selects another credential.
    pub async fn revoke(&self, id: Uuid) -> Result<()> {
        let mut tx = self.store.begin().await?;
        if !queries::revoke(&mut tx, id).await? {
            return Err(Error::NotFound);
        }
        tx.commit().await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Authenticate for ServiceAccess {
    async fn authenticate(&self, bearer: &str) -> Result<Caller> {
        let credential = Credential::parse(bearer.to_owned())?;
        ServiceAccess::authenticate(self, &credential).await
    }
}

fn validate_scopes(scopes: &BTreeSet<CapabilityScope>) -> Result<()> {
    if scopes
        .iter()
        .any(|scope| scope.capability.is_empty() || scope.resource.is_empty())
    {
        return Err(Error::Invalid(
            "capability and resource identities must be nonempty".into(),
        ));
    }
    Ok(())
}

impl queries::CredentialRow {
    fn into_caller(self) -> Result<Caller> {
        let authority = match self.principal.0 {
            PrincipalId::Owner { owner_id } => Authority::owner(
                self.binding.0,
                ResourceScope::Owner { owner_id },
                self.scopes.0,
            ),
            PrincipalId::Service { service_id } => {
                Authority::service(self.binding.0, service_id, self.scopes.0)
            }
            _ => return Err(Error::Forbidden),
        };
        Ok(Caller::verified(
            authority,
            AuthorizationRef {
                issuer: ISSUER.into(),
                id: self.id,
                revision: self.revision,
            },
        ))
    }
}
