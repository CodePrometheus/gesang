use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::{Error, Result};

/// Zeroized key for authenticated platform delivery payloads, distinct from execution unlock keys.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct PayloadKey([u8; 32]);

impl std::fmt::Debug for PayloadKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PayloadKey([redacted])")
    }
}

impl PayloadKey {
    /// Takes custody of key material supplied by a trusted host key service.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0; 24];
        getrandom::fill(&mut nonce).map_err(|_| Error::Randomness)?;
        let cipher = XChaCha20Poly1305::new((&self.0).into());
        let nonce_value = XNonce::from(nonce);
        let ciphertext = cipher
            .encrypt(
                &nonce_value,
                Payload {
                    msg: plaintext,
                    aad,
                },
            )
            .map_err(|_| Error::InvalidCiphertext)?;
        let mut result = Vec::with_capacity(nonce.len() + ciphertext.len());
        result.extend_from_slice(&nonce);
        result.extend(ciphertext);
        Ok(result)
    }

    pub(crate) fn open(&self, aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>> {
        let (nonce, ciphertext) = sealed
            .split_at_checked(24)
            .ok_or(Error::InvalidCiphertext)?;
        let nonce_bytes: [u8; 24] = nonce.try_into().map_err(|_| Error::InvalidCiphertext)?;
        XChaCha20Poly1305::new((&self.0).into())
            .decrypt(
                &XNonce::from(nonce_bytes),
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| Error::InvalidCiphertext)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn ciphertext_is_bound_to_its_request_and_detects_mutation() {
        let key = PayloadKey::new([7; 32]);
        let sealed = key.seal(b"request-a", b"private objective").unwrap();
        assert_eq!(
            key.open(b"request-a", &sealed).unwrap(),
            b"private objective"
        );
        assert!(key.open(b"request-b", &sealed).is_err());
        assert!(
            PayloadKey::new([8; 32])
                .open(b"request-a", &sealed)
                .is_err()
        );
        let mut changed = sealed;
        *changed.last_mut().unwrap() ^= 1;
        assert!(key.open(b"request-a", &changed).is_err());
        assert!(key.open(b"request-a", b"short").is_err());
    }
}
