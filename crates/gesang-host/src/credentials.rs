use std::fmt;

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use gesang_core::{Error, Result, Runtime};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use zeroize::{Zeroize, ZeroizeOnDrop};

/// Deployment-owned secret used only by trusted hosts to issue runtime credentials.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct ControlKey([u8; 32]);

impl ControlKey {
    /// Loads an independently generated persistent control key from host secret storage.
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }

    /// Issues a credential for one exact admitted or host-planned execution instance.
    pub fn issue(&self, runtime: &Runtime) -> Result<ControlToken> {
        Ok(ControlToken(
            BASE64_URL_SAFE_NO_PAD.encode(self.mac(runtime)?.finalize().into_bytes()),
        ))
    }

    /// Verifies the exact credential binding; callers must independently verify current writer admission.
    pub fn verify(&self, runtime: &Runtime, token: &str) -> Result<()> {
        let mut bytes = [0; 32];
        if BASE64_URL_SAFE_NO_PAD
            .decode_slice(token, &mut bytes)
            .map_err(|_| Error::Forbidden)?
            != bytes.len()
        {
            return Err(Error::Forbidden);
        }
        self.mac(runtime)?
            .verify_slice(&bytes)
            .map_err(|_| Error::Forbidden)
    }

    fn mac(&self, runtime: &Runtime) -> Result<Hmac<Sha256>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| Error::Forbidden)?;
        mac.update(b"gesang:runtime-control:v1\0");
        mac.update(runtime.agent_id.as_bytes());
        mac.update(runtime.id.as_bytes());
        mac.update(&runtime.generation.to_be_bytes());
        mac.update(runtime.engine_owner.as_bytes());
        Ok(mac)
    }
}

impl fmt::Debug for ControlKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ControlKey([REDACTED])")
    }
}

/// Secret passed to the protected execution service, never to tools or public clients.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct ControlToken(String);

impl ControlToken {
    /// Exposes the credential only for protected process configuration or request headers.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ControlToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ControlToken([REDACTED])")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn credentials_bind_deployment_agent_instance_generation_and_execution_owner() {
        let key = ControlKey::new([7; 32]);
        let runtime = Runtime {
            id: Uuid::new_v4(),
            agent_id: Uuid::new_v4(),
            generation: 1,
            locator: "trusted-route".into(),
            engine_owner: "local".into(),
        };
        let token = key.issue(&runtime).unwrap();
        key.verify(&runtime, token.expose()).unwrap();
        assert!(
            ControlKey::new([8; 32])
                .verify(&runtime, token.expose())
                .is_err()
        );
        for changed in [
            Runtime {
                id: Uuid::new_v4(),
                ..runtime.clone()
            },
            Runtime {
                agent_id: Uuid::new_v4(),
                ..runtime.clone()
            },
            Runtime {
                generation: 2,
                ..runtime.clone()
            },
            Runtime {
                engine_owner: "another".into(),
                ..runtime.clone()
            },
        ] {
            assert!(key.verify(&changed, token.expose()).is_err());
        }
        for invalid in ["", "invalid", &format!("{}=", token.expose())] {
            assert!(key.verify(&runtime, invalid).is_err());
        }
        assert!(!format!("{token:?}").contains(token.expose()));
    }
}
