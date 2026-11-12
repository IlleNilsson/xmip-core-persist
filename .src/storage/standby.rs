//! A Storage node among several, each keeping the databases itself: one
//! is the primary, serving every operation, and the others are standbys,
//! each copying the primary's writes behind it, in order (ADR-0056,
//! amendments 2026-10-22, drafted, and 2026-11-08: the owner's design of
//! 2026-10-01 — *one primary Storage node takes every
//! write; standby Storage nodes copy asynchronously behind*, the last
//! writes not yet copied lost if the primary dies — built for embedded
//! Storage nodes, which round robin alone cannot share a Ledger between).
//!
//! **Which is which is the Storage nodes' own to settle**, from the list
//! each is configured with — every Storage node of the cluster, in the
//! same order everywhere, the list a node reaches them through. Each asks
//! every one listed what it is now ([`Standing`]); it finds itself among
//! them by the process it answers as, never by a name. A primary that
//! answers is followed. **One is promoted only where every one listed
//! answered and none of them is a primary**: the one holding most of the
//! latest epoch, the first listed of equals. A primary that is gone is not
//! replaced; the others wait for it to come back, since with no witness a
//! primary cut off from them would serve on beside the one promoted, and
//! automatic failover returns with a witness (the owner, 2026-11-08;
//! ADR-0056, amendment 2026-11-08). Each promotion raises the epoch, and a
//! primary that finds one of a later epoch steps down; a node keeps the
//! latest epoch it met, and a primary of an earlier one is not asked
//! (`client`).
//!
//! **A standby copies the engines' writes, not the operations**
//! ([`copies`]): each write as the primary's engine took it, a keyed hash
//! and a sealed record, numbered in the order written, so what a standby
//! holds is what its primary held, and claims are decided once, by the
//! primary. It keeps the primary's data keys, each handed inside the
//! mutual TLS between them and wrapped under its own key-encryption key.
//! One that cannot take the writes on from where it is — it follows
//! another history, or what it lacks is no longer kept — is copied whole
//! first. A standby serves no operation: a node asking one is told it is a
//! standby, and asks the next listed (`client`).
//!
//! **Asynchronous, so a standby can lack the last writes.** The primary
//! answers before its standbys have a write, and hands one on only once it
//! is synced ([`copies`]); a primary stopped in order hands on everything it
//! wrote first, and one killed loses what it had not handed on: it gave up
//! its positions when it was promoted, so once it is back and every one
//! listed answers, a standby holding more is promoted, and the former
//! primary follows it, copied whole. **A standby copied whole is copying,
//! then catching up, then ready** ([`bootstrap`]; ADR-0056, amendment
//! 2026-11-12): its primary's pages reflect the writes taken while it
//! scanned in part, so it holds one state of its primary's only once it has
//! replayed them as far as the primary said the copy reaches; until then it
//! says so in its standing and is never promoted, across a restart too.
//! Each epoch is kept, durable, before a role is taken in it; a promotion
//! whose epoch cannot be kept fails. What a failover loses and duplicates
//! is the README's *What a failover loses*.

mod bootstrap;
mod copies;
mod delegated;
mod finding;
mod follow;
mod position;
mod served;
mod ship;
mod shipped;

pub use bootstrap::Bootstrap;
pub use copies::{Behind, Copied, Copies, DATABASES};
pub use delegated::{Delegated, Lent, Now};
pub use shipped::{Follow, Shipped, Standing};

pub(crate) use position::POSITION;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use xcore::lock::{Locked, ReadWriteLocked};

use secret::{KekName, KeyStore};

use super::client::StorageClient;
use super::embedded::Embedded;
use super::server::ALPN;
use crate::{Engine, PersistError};

/// Where a Storage node keeps, in its runtime database, the latest epoch it
/// met.
const EPOCH: &[u8] = b"\0xmip-core-persist/epoch\0";

/// How often a primary says it is there to a standby it has nothing to
/// hand, where the node's `[tuning] storage_beat` does not say.
pub const BEAT: Duration = Duration::from_millis(100);

/// How long a Storage node waits to hear from another — the standing it
/// asks for, its primary's next write or beat — and how often a primary
/// asks the others what they are, where the node's `[tuning]
/// storage_silence` does not say.
pub const SILENCE: Duration = Duration::from_secs(1);

/// The most bytes of each database's writes a Storage node keeps for a
/// standby behind it, where the node's `[tuning] storage_kept` does not say.
pub const KEPT: usize = 64 * 1024 * 1024;

/// What bounds a Storage node's dealings with the others, and what it keeps
/// of their waits while it is the primary.
#[derive(Clone, Copy, Debug)]
pub struct Timings {
    /// How often a primary beats to an idle standby.
    pub beat: Duration,
    /// How long it waits to hear from another — a standing asked, its
    /// primary's next write or beat — and how often a primary asks the
    /// others what they are.
    pub silence: Duration,
    /// What bounds each connect and each read of another.
    pub timeout: Duration,
    /// How long one that did not answer is asked last, as a node asks.
    pub pass_over: Duration,
    /// The most bytes of each database's writes kept for a standby.
    pub kept: usize,
    /// The most of what no node waits on now its primary keeps where its
    /// writer left it (`super::watch`, [`super::UNWAITED`] by default).
    pub unwaited: usize,
}

/// One database as a Storage node keeps it: its engine, its writes kept.
pub type Database = Copied<Arc<dyn Engine>>;

/// A Storage node's databases while it is the primary.
pub type Primary = Embedded<Database, Database>;

/// What a Storage node is now.
enum Role {
    /// Asking the others, before it follows one or is promoted.
    Finding,
    /// The primary, and its databases.
    Primary(Arc<Primary>),
    /// A standby of the primary at this address.
    Standby(String),
}

struct Held {
    epoch: u64,
    role: Role,
}

/// A Storage node of several: primary or standby, as they settle it.
pub struct StorageNode {
    process: u128,
    listed: Vec<String>,
    engines: [Arc<dyn Engine>; DATABASES],
    keys: Arc<dyn KeyStore>,
    kek: KekName,
    copies: Arc<Copies>,
    /// How far its bootstrap by a whole copy is, as its runtime database
    /// keeps it ([`bootstrap`]).
    bootstrap: Mutex<Bootstrap>,
    held: RwLock<Held>,
    settled: Condvar,
    settling: Mutex<()>,
    config: Arc<tls::ClientConfig>,
    reaching: Arc<StorageClient>,
    timings: Timings,
    stopping: AtomicBool,
    /// Each standby following it, by where it connected from, and the next
    /// write of each database it is handed.
    followers: Mutex<BTreeMap<String, [u64; DATABASES]>>,
    finding: Mutex<Option<JoinHandle<()>>>,
    /// The last thing it had to say of its standing: why a promotion
    /// failed, that it stepped down.
    last: Mutex<Option<String>>,
    /// How many operations its databases are lent to now.
    lent: AtomicUsize,
    /// Set once a stop has waited for every operation lent: what its
    /// standbys are handed is then all it wrote.
    drained: AtomicBool,
}

impl StorageNode {
    /// A Storage node over `engines` — runtime, administration, audit —
    /// sealed under `kek` in `keys`, one of the Storage nodes `listed`,
    /// presenting `identity` to the others and to a primary it asks for
    /// its own work. It serves nothing and settles nothing until
    /// [`StorageNode::start`].
    ///
    /// # Errors
    ///
    /// Where an engine cannot be read, `listed` is empty or names an
    /// address with no port, or `identity` makes no client.
    pub fn new(
        engines: [Arc<dyn Engine>; DATABASES],
        (keys, kek): (Arc<dyn KeyStore>, KekName),
        listed: &[String],
        identity: &tls::Identity,
        timings: Timings,
    ) -> Result<Arc<Self>, PersistError> {
        let positions = engines
            .iter()
            .map(|engine| position::positioned(engine.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        let history = positions.first().copied().flatten().map(|(h, _)| h);
        let same = positions
            .iter()
            .all(|at| at.is_some_and(|(h, _)| Some(h) == history));
        let (history, next) = match (same, history) {
            (true, Some(history)) => (history, [0, 1, 2].map(|d| positions[d].map_or(1, |p| p.1))),
            _ => (0, [1; DATABASES]),
        };
        let epoch = engines[0]
            .read(EPOCH)?
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .map_or(0, u64::from_be_bytes);
        let bootstrap = bootstrap::kept(engines[0].as_ref())?;
        let reaching = StorageClient::new(listed, identity, timings.timeout, timings.pass_over)?;
        let config = identity
            .client()
            .map_err(|error| PersistError::Unreachable {
                reason: error.message,
            })?;
        Ok(Arc::new(Self {
            process: u128::from_be_bytes(codec::random::array()),
            listed: listed.to_vec(),
            engines,
            keys,
            kek,
            copies: Arc::new(Copies::new(history, next, timings.kept)),
            bootstrap: Mutex::new(bootstrap),
            held: RwLock::new(Held {
                epoch,
                role: Role::Finding,
            }),
            settled: Condvar::new(),
            settling: Mutex::new(()),
            config: Arc::new(tls::alpn::offering(config, &[ALPN])),
            reaching: Arc::new(reaching),
            timings,
            stopping: AtomicBool::new(false),
            followers: Mutex::default(),
            finding: Mutex::new(None),
            last: Mutex::new(None),
            lent: AtomicUsize::new(0),
            drained: AtomicBool::new(false),
        }))
    }

    /// Settle with the others what it is, and keep settling it, on a thread
    /// of its own; return once it is primary or standby, or `within` has
    /// passed. Its server must already be listening, so that it finds
    /// itself among those listed.
    ///
    /// # Errors
    ///
    /// Where the thread cannot be started.
    pub fn start(self: &Arc<Self>, within: Duration) -> Result<(), PersistError> {
        let node = Arc::clone(self);
        let thread = std::thread::Builder::new()
            .name("xmip-storage-standing".to_string())
            .spawn(move || node.find())
            .map_err(|error| PersistError::engine("xmip-storage", error))?;
        *self.finding.locked() = Some(thread);
        let deadline = Instant::now() + within;
        let mut settling = self.settling.locked();
        while self.is_finding() {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            settling = self
                .settled
                .wait_timeout(settling, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        Ok(())
    }

    /// Stop: serve no operation more, wait for each one under way, then let
    /// every standby it hands writes to have what it wrote — a primary
    /// stopped in order has handed on everything — and stop settling.
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        // No wait holds a loan: each one on its queues is ended now, so the
        // node waiting asks the next Storage node at once.
        if let Some(primary) = self.primary() {
            primary.refuse_waits("this Storage node is stopping");
        }
        let deadline = Instant::now() + self.timings.timeout;
        let mut settling = self.settling.locked();
        while self.lent.load(Ordering::SeqCst) > 0 {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            settling = self
                .settled
                .wait_timeout(settling, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        self.drained.store(true, Ordering::SeqCst);
        self.settled.notify_all();
        drop(settling);
        if let Some(thread) = self.finding.locked().take() {
            let _ = thread.join();
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn held(&self) -> std::sync::RwLockReadGuard<'_, Held> {
        self.held.read_locked()
    }

    /// Whether it is still finding its primary, or being promoted: neither
    /// primary nor following one.
    #[must_use]
    pub fn is_finding(&self) -> bool {
        matches!(self.held().role, Role::Finding)
    }

    fn primary(&self) -> Option<Arc<Primary>> {
        match &self.held().role {
            Role::Primary(primary) => Some(Arc::clone(primary)),
            _ => None,
        }
    }

    /// `epoch` written, durable on return, where it is later than the one
    /// held, and held from then: before any role is taken in it, so a node
    /// started again never holds an epoch before one it took a role in.
    ///
    /// # Errors
    ///
    /// Where it could not be written; the epoch held is as it was then.
    fn keep_epoch(&self, epoch: u64) -> Result<(), PersistError> {
        if epoch <= self.held().epoch {
            return Ok(());
        }
        self.engines[0].write(EPOCH, &epoch.to_be_bytes())?;
        let mut held = self.held.write_locked();
        held.epoch = held.epoch.max(epoch);
        Ok(())
    }

    /// `role` held from now, in `epoch`, the epoch kept first where it is
    /// later than the one kept ([`StorageNode::keep_epoch`]).
    ///
    /// # Errors
    ///
    /// Where the epoch could not be kept; no role is taken then.
    fn take_up(&self, role: Role, epoch: u64) -> Result<(), PersistError> {
        self.keep_epoch(epoch)?;
        self.stand(role);
        Ok(())
    }

    /// `role` held from now, in the epoch held.
    fn stand(&self, role: Role) {
        let was = std::mem::replace(&mut self.held.write_locked().role, role);
        // A primary no more: every wait on its queues refused, so the node
        // waiting moves to the new primary at once (`super::watch`).
        if let Role::Primary(primary) = was {
            primary.refuse_waits("this Storage node is the primary no more");
        }
        let _settling = self.settling.locked();
        self.settled.notify_all();
    }

    /// Whether it is the primary now.
    #[must_use]
    pub fn is_primary(&self) -> bool {
        self.primary().is_some()
    }
}

impl Drop for StorageNode {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests;
