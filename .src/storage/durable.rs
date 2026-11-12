//! When a write that was answered before its sync is durable.
//!
//! The embedded Storage node's one writer decides and writes a batch, then
//! syncs it (`super::commit`). A Publication is answered between the two:
//! its claims are decided and its records readable, and what it is worth
//! on disk is told here once the sync returns. So a send of the Messages it
//! queued is prepared while the Ledger syncs, and lets its far end see the
//! Message only once [`Durable::wait`] says the Ledger holds it (ADR-0018,
//! amendment 2026-10-20). So is a claim a send is taken under, asked ahead
//! (`XmipStorage::claim_ahead`): its send is prepared while the claim
//! syncs, and abandoned where it never is (ADR-0018, amendment 2026-11-12).
//! Every other write is answered once durable, as before.
//!
//! **Told where it was decided, read once.** A Storage node reached over
//! the wire tells a claim's durability in a frame of its own after the
//! claim's answer ([`Durable::fetching`]): the first thread to wait reads
//! it, every other waits for that word, and one never waited on is never
//! read — the read, and the connection it is on, dropped with the last
//! clone, so no other request is ever answered by it.
//!
//! **It says what the Storage node's writer worked for each write in it**
//! ([`Durable::worked`]): the processor time its batch took outside its
//! I/O, shared equally among the batch's writes — a Message's share of the
//! Ledger's work, which its own share counts (`xcore::spent`). **And what
//! the sync that made it durable waited for the disk** ([`Durable::synced`]):
//! the sync's wait shared equally among every write it made durable, a
//! Message's share of the disk's time, never its own share.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::Duration;
use xcore::lock::Locked;

use crate::PersistError;

/// What reads a write's durability where it was decided: the Storage
/// node's second frame of a claim asked ahead (`super::client`).
type Fetch = Box<dyn FnOnce() -> Result<(), String> + Send>;

/// The read not made yet, where there is one.
#[derive(Default)]
struct Fetching(Option<Fetch>);

impl fmt::Debug for Fetching {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "to read" } else { "none" })
    }
}

/// A write's durability: told once, waited on by anyone holding it.
#[derive(Clone, Debug)]
pub struct Durable(Arc<State>);

#[derive(Debug, Default)]
struct State {
    told: Mutex<Option<Result<(), String>>>,
    woken: Condvar,
    /// What tells it, run by the first wait, where it is read from afar.
    fetch: Mutex<Fetching>,
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

    /// Told by `fetch`, run once by the first [`Durable::wait`] — every
    /// other waiter waits for its word — and never where none waits: it is
    /// dropped unrun with the last clone (ADR-0018, amendment 2026-11-12).
    pub(crate) fn fetching(fetch: impl FnOnce() -> Result<(), String> + Send + 'static) -> Self {
        let durable = Self::pending();
        durable.0.fetch.locked().0 = Some(Box::new(fetch));
        durable
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

    /// Wait until the write is durable, or its sync failed: where it is read
    /// from afar and not read yet, read here, once.
    ///
    /// # Errors
    ///
    /// [`PersistError::Engine`] where the sync failed, or no word of it came:
    /// what was written may be lost with the process, or with the machine.
    pub fn wait(&self) -> Result<(), PersistError> {
        let fetch = self.0.fetch.locked().0.take();
        if let Some(fetch) = fetch {
            // Told whatever the read comes to, so no other waiter waits on.
            let read = catch_unwind(AssertUnwindSafe(fetch));
            self.tell(read.unwrap_or_else(|_| Err("its read panicked".to_string())));
        }
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

    /// Whether it was told, and durable: `None` while the sync runs, and,
    /// where it is read from afar, until a wait has read it.
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

    /// Counts its drop: what a read held — a connection — let go of.
    struct Held(Arc<AtomicU32>);

    impl Drop for Held {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn a_read_from_afar_runs_once_for_every_waiter_and_never_where_none_waits() {
        let (runs, dropped) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
        let reading = |said: Result<(), String>| {
            let (runs, held) = (Arc::clone(&runs), Held(Arc::clone(&dropped)));
            Durable::fetching(move || {
                let _held = held;
                runs.fetch_add(1, Ordering::SeqCst);
                said
            })
        };
        let durable = reading(Ok(()));
        assert_eq!(durable.told(), None, "not read before a wait");
        let waiters: Vec<_> = (0..4)
            .map(|_| {
                let waiting = durable.clone();
                std::thread::spawn(move || waiting.wait())
            })
            .collect();
        for waiter in waiters {
            waiter.join().expect("the waiter ends").expect("durable");
        }
        assert!(durable.wait().is_ok(), "and again, read no more");
        assert_eq!(runs.load(Ordering::SeqCst), 1, "read once");
        assert_eq!(durable.told(), Some(true));

        let refused = reading(Err("the sync failed".to_string()));
        let failed = refused.wait();
        assert!(failed.is_err_and(|why| why.to_string().contains("the sync failed")));
        assert_eq!(runs.load(Ordering::SeqCst), 2);

        let unread = reading(Ok(()));
        let clone = unread.clone();
        drop(unread);
        assert_eq!(dropped.load(Ordering::SeqCst), 2, "held while a clone is");
        drop(clone);
        assert_eq!(runs.load(Ordering::SeqCst), 2, "never read");
        assert_eq!(dropped.load(Ordering::SeqCst), 3, "let go of with the last");
    }
}
