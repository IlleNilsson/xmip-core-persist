//! When a write that was answered before its sync is durable.
//!
//! The embedded Storage node's one writer decides and writes a batch, then
//! syncs it (`super::commit`). A Publication is answered between the two:
//! its claims are decided and its records readable, and what it is worth
//! on disk is told here once the sync returns. So a send of the Messages it
//! queued is prepared while the Ledger syncs, and lets its far end see the
//! Message only once [`Durable::wait`] says the Ledger holds it (ADR-0018,
//! amendment 2026-10-20). Every other write is answered once durable, as
//! before.
//!
//! **It says what the Storage node's writer worked for each write in it**
//! ([`Durable::worked`]): the processor time its batch took outside its
//! I/O, shared equally among the batch's writes — a Message's share of the
//! Ledger's work, which its own share counts (`xcore::spent`). **And what
//! the sync that made it durable waited for the disk** ([`Durable::synced`]):
//! the sync's wait shared equally among every write it made durable, a
//! Message's share of the disk's time, never its own share.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;
use xcore::lock::Locked;

use crate::PersistError;

/// A write's durability: told once, waited on by anyone holding it.
#[derive(Clone, Debug)]
pub struct Durable(Arc<State>);

#[derive(Debug, Default)]
struct State {
    told: Mutex<Option<Result<(), String>>>,
    woken: Condvar,
    /// What the writer worked for each write of its batch, in nanoseconds.
    worked: AtomicU64,
    /// How many writes its batch holds.
    writes: AtomicU32,
    /// What the sync waited for the disk for each write it made durable,
    /// in nanoseconds.
    synced: AtomicU64,
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
        let mut told = self.0.told.locked();
        if told.is_none() {
            *told = Some(synced);
            self.0.woken.notify_all();
        }
    }

    /// `worked`, what the writer worked for its batch of `writes` writes,
    /// shared equally among them.
    pub(crate) fn worked_by(&self, worked: Duration, writes: u32) {
        let writes = writes.max(1);
        self.0.writes.store(writes, Ordering::Relaxed);
        self.0
            .worked
            .store(nanos(worked / writes), Ordering::Relaxed);
    }

    /// How many writes its batch holds: what a sync's wait is shared by.
    pub(crate) fn writes(&self) -> u32 {
        self.0.writes.load(Ordering::Relaxed).max(1)
    }

    /// `synced`, what the sync waited for the disk for each write it made
    /// durable. Set before it is told.
    pub(crate) fn synced_each(&self, synced: Duration) {
        self.0.synced.store(nanos(synced), Ordering::Relaxed);
    }

    /// The processor time the Storage node's writer worked for each write
    /// of the batch this one was written in, outside its I/O; nothing for
    /// a write answered durable already. Set before the write is answered.
    #[must_use]
    pub fn worked(&self) -> Duration {
        Duration::from_nanos(self.0.worked.load(Ordering::Relaxed))
    }

    /// What the sync that made this write durable waited for the storage
    /// device, for each write it made durable: the disk's time, never
    /// Xmip's own share (`xcore::spent::on_sync`); nothing for a write
    /// answered durable already. Set before it is told ([`Durable::wait`]).
    #[must_use]
    pub fn synced(&self) -> Duration {
        Duration::from_nanos(self.0.synced.load(Ordering::Relaxed))
    }

    /// Wait until the write is durable, or its sync failed.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] where the sync failed: what was written is
    /// lost with the process, or with the machine.
    pub fn wait(&self) -> Result<(), PersistError> {
        let told = self.0.told.locked();
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
        let told = self.0.told.locked();
        told.as_ref().map(Result::is_ok)
    }
}

/// A duration in nanoseconds, the most a `u64` holds at most.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
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
