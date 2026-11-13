use hkdf::Hkdf;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use crate::{Error, PayloadKey, Result};

/// A derived or independently generated service key; never a password or the client's master key.
pub struct ServiceKey(Zeroizing<[u8; 32]>);

impl ServiceKey {
    /// Takes possession of a client-derived key or independently generated first-store key.
    pub fn new(mut bytes: [u8; 32]) -> Self {
        let key = Self(Zeroizing::new(bytes));
        bytes.zeroize();
        key
    }

    /// Borrows secret material only for protected vault verification or unlock transport.
    pub fn expose(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for ServiceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ServiceKey([REDACTED])")
    }
}

/// A host-owned wrapping key with independent encryption and idempotency purposes.
pub struct CustodyKey {
    sealing: PayloadKey,
    fingerprint: Zeroizing<[u8; 32]>,
}

impl CustodyKey {
    /// Derives custody subkeys from random host key material retained outside user volumes.
    pub fn new(mut seed: [u8; 32]) -> Result<Self> {
        let derivation = Hkdf::<Sha256>::new(None, &seed);
        seed.zeroize();
        let mut sealing = Zeroizing::new([0; 32]);
        let mut fingerprint = Zeroizing::new([0; 32]);
        derivation
            .expand(b"gesang:custody:encryption:v1", sealing.as_mut())
            .map_err(|_| Error::InvalidCiphertext)?;
        derivation
            .expand(b"gesang:custody:idempotency:v1", fingerprint.as_mut())
            .map_err(|_| Error::InvalidCiphertext)?;
        Ok(Self {
            sealing: PayloadKey::new(*sealing),
            fingerprint,
        })
    }

    pub(super) fn digest(&self, parts: &[&[u8]]) -> Result<Vec<u8>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.fingerprint.as_ref())
            .map_err(|_| Error::InvalidCiphertext)?;
        for part in parts {
            mac.update(&(part.len() as u64).to_be_bytes());
            mac.update(part);
        }
        Ok(mac.finalize().into_bytes().to_vec())
    }

    pub(super) fn seal(&self, aad: &[u8], key: &ServiceKey) -> Result<Vec<u8>> {
        self.sealing.seal(aad, key.expose())
    }

    pub(super) fn open(&self, aad: &[u8], sealed: &[u8]) -> Result<ServiceKey> {
        let plaintext = Zeroizing::new(self.sealing.open(aad, sealed)?);
        let bytes = plaintext
            .as_slice()
            .try_into()
            .map_err(|_| Error::InvalidCiphertext)?;
        Ok(ServiceKey::new(bytes))
    }
}

impl std::fmt::Debug for CustodyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CustodyKey([REDACTED])")
    }
}
