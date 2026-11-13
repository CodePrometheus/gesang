use hkdf::Hkdf;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use crate::{Error, PayloadKey, Result};

/// Purpose-separated encryption and idempotency keys derived from host-owned random material.
pub struct ConfigurationKey {
    sealing: PayloadKey,
    fingerprint: Zeroizing<[u8; 32]>,
}

impl ConfigurationKey {
    /// Derives independent subkeys; the seed must be random key material, not a password.
    pub fn new(mut seed: [u8; 32]) -> Result<Self> {
        let derivation = Hkdf::<Sha256>::new(None, &seed);
        seed.zeroize();
        let mut sealing = Zeroizing::new([0; 32]);
        let mut fingerprint = Zeroizing::new([0; 32]);
        derivation
            .expand(b"gesang:configuration:encryption:v1", sealing.as_mut())
            .map_err(|_| Error::InvalidCiphertext)?;
        derivation
            .expand(b"gesang:configuration:idempotency:v1", fingerprint.as_mut())
            .map_err(|_| Error::InvalidCiphertext)?;
        Ok(Self {
            sealing: PayloadKey::new(*sealing),
            fingerprint,
        })
    }

    pub(super) fn mac(&self) -> Result<Hmac<Sha256>> {
        Hmac::<Sha256>::new_from_slice(self.fingerprint.as_ref())
            .map_err(|_| Error::InvalidCiphertext)
    }

    pub(super) fn seal(&self, aad: &[u8], value: &[u8]) -> Result<Vec<u8>> {
        self.sealing.seal(aad, value)
    }

    pub(super) fn open(&self, aad: &[u8], value: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
        self.sealing.open(aad, value).map(Zeroizing::new)
    }
}

impl std::fmt::Debug for ConfigurationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConfigurationKey([REDACTED])")
    }
}

pub(super) fn add_part(mac: &mut Hmac<Sha256>, value: &[u8]) {
    mac.update(&(value.len() as u64).to_be_bytes());
    mac.update(value);
}
