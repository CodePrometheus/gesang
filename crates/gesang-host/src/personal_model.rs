//! Short-lived, single-claim personal model changes scoped to an exact execution writer.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use gesang_core::{Error, Result, Runtime};
use tokio::time::Instant;
use uuid::Uuid;
use zeroize::Zeroizing;

/// A credential mutation held only until a trusted execution process claims it.
pub enum PersonalCredentialChange {
    /// Install or replace the owner's provider credential.
    Set {
        /// Provider selected by the owner.
        provider: String,
        /// Secret held only in memory until delivery.
        key: Zeroizing<String>,
    },
    /// Delete the owner's credential for a provider.
    Remove {
        /// Provider selected by the owner.
        provider: String,
    },
}

impl fmt::Debug for PersonalCredentialChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersonalCredentialChange")
            .finish_non_exhaustive()
    }
}

/// In-memory handoff shared by the browser mutation adapter and the private claim router.
#[derive(Clone, Default)]
pub struct PersonalCredentialClaims {
    pending: Arc<Mutex<HashMap<Uuid, Pending>>>,
    pub(crate) challenges: crate::subscription::SubscriptionChallenges,
}

impl fmt::Debug for PersonalCredentialClaims {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersonalCredentialClaims")
            .finish_non_exhaustive()
    }
}

struct Pending {
    runtime: Runtime,
    deadline: Instant,
    change: PersonalCredentialChange,
}

pub(crate) struct ClaimGuard {
    pub(crate) id: Uuid,
    claims: PersonalCredentialClaims,
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        self.claims
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl PersonalCredentialClaims {
    pub(crate) fn stage(
        &self,
        runtime: Runtime,
        lifetime: Duration,
        change: PersonalCredentialChange,
    ) -> Result<ClaimGuard> {
        let id = Uuid::new_v4();
        let deadline = Instant::now()
            .checked_add(lifetime)
            .ok_or_else(unavailable)?;
        let mut pending = self.pending.lock().map_err(|_| unavailable())?;
        let Entry::Vacant(entry) = pending.entry(id) else {
            return Err(unavailable());
        };
        entry.insert(Pending {
            runtime,
            deadline,
            change,
        });
        Ok(ClaimGuard {
            id,
            claims: self.clone(),
        })
    }

    pub(crate) fn claim(&self, runtime: &Runtime, id: Uuid) -> Result<PersonalCredentialChange> {
        let mut pending = self.pending.lock().map_err(|_| unavailable())?;
        let entry = pending.get(&id).ok_or(Error::NotFound)?;
        if entry.runtime != *runtime {
            return Err(Error::Forbidden);
        }
        let live = Instant::now() < entry.deadline;
        pending
            .remove(&id)
            .filter(|_| live)
            .map(|entry| entry.change)
            .ok_or(Error::NotFound)
    }
}

fn unavailable() -> Error {
    Error::Uncertain("personal model change handoff is unavailable".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn runtime() -> Runtime {
        Runtime {
            id: Uuid::new_v4(),
            agent_id: Uuid::new_v4(),
            generation: 1,
            locator: "isolated".into(),
            engine_owner: "owner".into(),
        }
    }

    fn remove() -> PersonalCredentialChange {
        PersonalCredentialChange::Remove {
            provider: "example".into(),
        }
    }

    #[test]
    fn concurrent_claims_deliver_only_once_and_refuse_other_writers() {
        let claims = PersonalCredentialClaims::default();
        let writer = runtime();
        let guard = claims
            .stage(writer.clone(), Duration::from_secs(60), remove())
            .unwrap();
        let mut other = writer.clone();
        other.generation += 1;
        assert!(matches!(
            claims.claim(&other, guard.id),
            Err(Error::Forbidden)
        ));
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            let claim = || {
                start.wait();
                claims.claim(&writer, guard.id)
            };
            let one = scope.spawn(claim);
            let two = scope.spawn(claim);
            let (one, two) = (one.join().unwrap(), two.join().unwrap());
            assert!(matches!(
                (one, two),
                (Ok(_), Err(Error::NotFound)) | (Err(Error::NotFound), Ok(_)),
            ));
        });
    }

    #[test]
    fn cancellation_and_expiry_remove_unclaimed_changes() {
        let claims = PersonalCredentialClaims::default();
        let writer = runtime();
        let guard = claims
            .stage(writer.clone(), Duration::from_secs(60), remove())
            .unwrap();
        let canceled = guard.id;
        drop(guard);
        assert!(matches!(
            claims.claim(&writer, canceled),
            Err(Error::NotFound)
        ));

        let expired = claims
            .stage(writer.clone(), Duration::ZERO, remove())
            .unwrap();
        assert!(matches!(
            claims.claim(&writer, expired.id),
            Err(Error::NotFound)
        ));
        assert!(claims.pending.lock().unwrap().is_empty());
    }
}
