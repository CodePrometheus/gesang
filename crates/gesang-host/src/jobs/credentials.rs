//! Purpose-bound command credentials contain no workspace or command content.

use base64::{Engine as _, prelude::BASE64_URL_SAFE_NO_PAD};
use chopin_job_broker::{Caller, Credential, Error, JobId, Result, WorkerCaller};
use gesang_core::Runtime;
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use uuid::Uuid;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Persistent deployment secret dedicated to command control and worker callbacks.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct JobControlKey([u8; 32]);

impl std::fmt::Debug for JobControlKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JobControlKey([redacted])")
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Claims {
    version: u32,
    agent: Uuid,
    runtime: Uuid,
    generation: u64,
    owner: String,
    purpose: Purpose,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Purpose {
    Engine,
    Worker { operation: Uuid, worker: Uuid },
}

impl Claims {
    fn caller(&self) -> Result<Caller> {
        if self.version != 1
            || self.agent.is_nil()
            || self.runtime.is_nil()
            || self.generation == 0
            || i64::try_from(self.generation).is_err()
            || self.owner.is_empty()
        {
            return Err(Error::Unauthorized);
        }
        Ok(Caller {
            scope: self.agent,
            domain: self.runtime,
            runtime: self.runtime,
            generation: self.generation,
            owner: self.owner.clone(),
        })
    }
}

impl JobControlKey {
    /// Loads independently generated persistent key material from protected host storage.
    pub fn new(key: [u8; 32]) -> Self {
        Self(key)
    }

    /// Issues a protected engine credential for an exact host-planned allocation.
    ///
    /// Issuance does not admit execution. Every effect still requires current
    /// writer, purpose, readiness, delegation and resource-policy verification.
    pub fn issue(&self, runtime: &Runtime) -> Result<Credential> {
        self.sign(&Claims {
            version: 1,
            agent: runtime.agent_id,
            runtime: runtime.id,
            generation: u64::try_from(runtime.generation).map_err(|_| Error::Forbidden)?,
            owner: runtime.engine_owner.clone(),
            purpose: Purpose::Engine,
        })
    }

    pub(super) fn issue_worker(
        &self,
        caller: &Caller,
        id: JobId,
        worker: Uuid,
    ) -> Result<Credential> {
        if id.scope != caller.scope
            || caller.domain != caller.runtime
            || id.operation.is_nil()
            || worker.is_nil()
        {
            return Err(Error::Forbidden);
        }
        self.sign(&Claims {
            version: 1,
            agent: caller.scope,
            runtime: caller.runtime,
            generation: caller.generation,
            owner: caller.owner.clone(),
            purpose: Purpose::Worker {
                operation: id.operation,
                worker,
            },
        })
    }

    pub(super) fn engine(&self, credential: &str) -> Result<Caller> {
        let claims = self.verify(credential)?;
        if !matches!(claims.purpose, Purpose::Engine) {
            return Err(Error::Unauthorized);
        }
        claims.caller()
    }

    pub(super) fn worker(&self, credential: &str) -> Result<WorkerCaller> {
        let claims = self.verify(credential)?;
        let caller = claims.caller()?;
        match claims.purpose {
            Purpose::Worker { operation, worker } if !operation.is_nil() && !worker.is_nil() => {
                Ok(WorkerCaller {
                    caller,
                    operation,
                    worker,
                })
            }
            _ => Err(Error::Unauthorized),
        }
    }

    fn sign(&self, claims: &Claims) -> Result<Credential> {
        claims.caller()?;
        let bytes = Zeroizing::new(serde_json::to_vec(claims)?);
        let payload = BASE64_URL_SAFE_NO_PAD.encode(&bytes);
        let tag = self.mac(&payload)?.finalize().into_bytes();
        Credential::new(format!("{payload}.{}", BASE64_URL_SAFE_NO_PAD.encode(tag)))
    }

    fn verify(&self, credential: &str) -> Result<Claims> {
        let (payload, tag) = credential.split_once('.').ok_or(Error::Unauthorized)?;
        let mut signature = [0; 32];
        if BASE64_URL_SAFE_NO_PAD
            .decode_slice(tag, &mut signature)
            .map_err(|_| Error::Unauthorized)?
            != signature.len()
        {
            return Err(Error::Unauthorized);
        }
        self.mac(payload)?
            .verify_slice(&signature)
            .map_err(|_| Error::Unauthorized)?;
        let bytes = Zeroizing::new(
            BASE64_URL_SAFE_NO_PAD
                .decode(payload)
                .map_err(|_| Error::Unauthorized)?,
        );
        serde_json::from_slice(&bytes).map_err(|_| Error::Unauthorized)
    }

    fn mac(&self, payload: &str) -> Result<Hmac<Sha256>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).map_err(|_| Error::Unauthorized)?;
        mac.update(b"gesang:command-control:v1\0");
        mac.update(payload.as_bytes());
        Ok(mac)
    }
}
