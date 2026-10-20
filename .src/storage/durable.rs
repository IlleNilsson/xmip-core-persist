//! When a write that was answered before its sync is durable.
//!
//! The embedded Storage node's one writer decides and writes a batch, then
//! syncs it (`super::commit`). A Publication is answered between the two:
//! its claims are decided and its records readable, and what it is worth
//! on disk is told here once the sync returns. So a send of the Journeys it
//! opened is prepared while the Ledger syncs, and lets its far end see the
//! Message only once [`Durable::wait`] says the Ledger holds it (ADR-0018,
//! amendment 2026-10-20). Every other write is answered once durable, as
//! before.

use std::sync::{Arc, Condvar, Mutex, PoisonError};

use crate::PersistError;

/// A write's durability: told once, waited on by anyone holding it.
#[derive(Clone, Debug)]
pub struct Durable(Arc<State>);

#[derive(Debug, Default)]
struct State {
    told: Mutex<Option<Result<(), String>>>,
    woken: Condvar,
}

impl Durable {
    /// Durable already: a write synced, or committed behind a server,
    /// before it was answered.
    #[must_use]
    pub fn kept() -> Self {
        let durable = Self::pending();
        durable.tell(Ok(()));
        durable
    }

    /// Not told yet.
    pub(crate) fn pending() -> Self {
        Self(Arc::default())
    }

    /// Told what the sync came to; told once, the first word stands.
    pub(crate) fn tell(&self, synced: Result<(), String>) {
        let mut told = self.0.told.lock().unwrap_or_else(PoisonError::into_inner);
        if told.is_none() {
            *told = Some(synced);
            self.0.woken.notify_all();
        }
    }

    /// Wait until the write is durable, or its sync failed.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] where the sync failed: what was written is
    /// worth what an operating system keeps of a process that died.
    pub fn wait(&self) -> Result<(), PersistError> {
        let told = self.0.told.lock().unwrap_or_else(PoisonError::into_inner);
        let told = self
            .0
            .woken
            .wait_while(told, |told| told.is_none())
            .unwrap_or_else(PoisonError::into_inner);
        match told.as_ref() {
            Some(Err(why)) => Err(PersistError::engine("xmip-storage", why.clone())),
            _ => Ok(()),
        }
    }

    /// Whether it was told, and durable: `None` while the sync runs.
    #[must_use]
    pub fn told(&self) -> Option<bool> {
        let told = self.0.told.lock().unwrap_or_else(PoisonError::into_inner);
        told.as_ref().map(Result::is_ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_wait_ends_once_told_and_says_what_the_sync_came_to() {
        assert!(Durable::kept().wait().is_ok());
        let durable = Durable::pending();
        assert_eq!(durable.told(), None);
        let waiting = durable.clone();
        let waiter = std::thread::spawn(move || waiting.wait());
        durable.tell(Err("the disk is full".to_string()));
        durable.tell(Ok(()));
        let failed = waiter.join().expect("the waiter ends");
        assert!(failed.is_err_and(|why| why.to_string().contains("the disk is full")));
        assert_eq!(durable.told(), Some(false), "the first word stands");
    }
}
