//! One-time reverse delivery of a provider challenge for an exact host operation.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::http::Uri;
use gesang_core::{Error, Result, Runtime};
use tokio::time::Instant;
use uuid::Uuid;

/// A browser challenge returned only through the authenticated private callback.
pub struct SubscriptionChallenge {
    /// The first-party authorization URL.
    pub verification_url: String,
    /// The optional short code the owner enters on the provider's page.
    pub display_code: Option<String>,
}

impl fmt::Debug for SubscriptionChallenge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriptionChallenge")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default)]
pub(crate) struct SubscriptionChallenges(Arc<Mutex<HashMap<Uuid, Pending>>>);

struct Pending {
    runtime: Runtime,
    attempt: Uuid,
    deadline: Instant,
    challenge: Option<SubscriptionChallenge>,
}

pub(crate) struct ChallengeGuard {
    pub(crate) id: Uuid,
    challenges: SubscriptionChallenges,
}

impl Drop for ChallengeGuard {
    fn drop(&mut self) {
        self.challenges
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl ChallengeGuard {
    pub(crate) fn take(&self) -> Result<SubscriptionChallenge> {
        self.challenges
            .0
            .lock()
            .map_err(|_| unavailable())?
            .remove(&self.id)
            .filter(|entry| Instant::now() < entry.deadline)
            .and_then(|entry| entry.challenge)
            .ok_or(Error::NotFound)
    }
}

impl SubscriptionChallenges {
    pub(crate) fn reserve(
        &self,
        runtime: Runtime,
        attempt: Uuid,
        lifetime: Duration,
    ) -> Result<ChallengeGuard> {
        let id = Uuid::new_v4();
        let deadline = Instant::now()
            .checked_add(lifetime)
            .ok_or_else(unavailable)?;
        let mut pending = self.0.lock().map_err(|_| unavailable())?;
        let Entry::Vacant(entry) = pending.entry(id) else {
            return Err(unavailable());
        };
        entry.insert(Pending {
            runtime,
            attempt,
            deadline,
            challenge: None,
        });
        Ok(ChallengeGuard {
            id,
            challenges: self.clone(),
        })
    }

    pub(crate) fn deliver(
        &self,
        runtime: &Runtime,
        id: Uuid,
        attempt: Uuid,
        challenge: SubscriptionChallenge,
    ) -> Result<()> {
        validate(&challenge)?;
        let mut pending = self.0.lock().map_err(|_| unavailable())?;
        let entry = pending.get_mut(&id).ok_or(Error::NotFound)?;
        if entry.runtime != *runtime || entry.attempt != attempt {
            return Err(Error::Forbidden);
        }
        if Instant::now() >= entry.deadline {
            return Err(Error::NotFound);
        }
        if entry.challenge.is_some() {
            return Err(Error::RequestConflict);
        }
        entry.challenge = Some(challenge);
        Ok(())
    }
}

fn validate(challenge: &SubscriptionChallenge) -> Result<()> {
    let url = challenge
        .verification_url
        .parse::<Uri>()
        .map_err(|_| Error::Forbidden)?;
    if url.scheme_str() != Some("https") || challenge.verification_url.contains('#') {
        return Err(Error::Forbidden);
    }
    match (
        url.authority().map(axum::http::uri::Authority::as_str),
        url.path(),
    ) {
        (Some("auth.openai.com"), "/codex/device")
            if url.query().is_none()
                && challenge.display_code.as_ref().is_some_and(|code| {
                    !code.is_empty()
                        && code
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                }) =>
        {
            Ok(())
        }
        (Some("chat.qwen.ai"), "/authorize")
            if challenge.display_code.is_none()
                && url.query().is_some_and(|query| {
                    query.split('&').any(|field| {
                        field
                            .strip_prefix("user_code=")
                            .is_some_and(|code| !code.is_empty())
                    })
                }) =>
        {
            Ok(())
        }
        _ => Err(Error::Forbidden),
    }
}

fn unavailable() -> Error {
    Error::Uncertain("subscription challenge delivery is unavailable".into())
}
