//! A standby following its primary (`super`): it asks to be handed the
//! primary's writes from where it is, and takes them in order — first the
//! whole of each database where it cannot take them on from there, then
//! the writes taken meanwhile until it is caught up (`super::bootstrap`) —
//! until the primary goes silent, declines, stops in order, saying who
//! succeeds it (`super::succession`), or the node stops. What it took
//! is synced on each beat, and once [`SYNC`] has passed on the node's clock
//! since it last synced.
//!
//! **A stop ends it at once** (open problem 94): the stop shuts the socket
//! it follows on ([`Followed`]), so a read waiting for the primary's next
//! frame — a beat away, or its silence mid-copy — ends there, on Linux and
//! macOS; Windows leaves such a read waiting, and the read looks at the stop
//! each [`WATCH`] as the Storage server's connections do. A whole copy's
//! wipe of its own databases looks at the stop between its pages. Until
//! 2026-11-22 a stop waited out both, and a standby the service manager
//! stopped while it was copied whole was killed 90 s later.
use std::net::{Shutdown, TcpStream};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use net::watched::Watched;
use secret::DataKey;
use xcore::clock::between;
use xcore::lock::Locked;

use super::super::server::WATCH;
use super::super::wire::{self, Request};
use super::bootstrap::{BOOTSTRAP, Bootstrap};
use super::copies::DATABASES;
use super::position::position;
use super::succession::Ended;
use super::{EPOCH, Follow, Shipped, StorageNode};
use crate::encrypted_store::{DATA_KEY, OWN};
use crate::{Change, PersistError, borrowed};

/// How long a standby writes copies before it syncs them: a copy counts
/// on a standby once synced, as a write does on its primary.
pub(super) const SYNC: Duration = Duration::from_millis(10);

/// The most entries a page of a whole copy's wipe removes at once.
const PAGE: usize = 1024;

/// The engine key after every key an engine keeps.
pub(super) const LAST: [u8; 1024] = [0xff; 1024];

/// What a whole copy's wipe leaves in the runtime database: the node's own
/// standing — the latest epoch it met, kept before it took a role in it,
/// and that it is copying.
const STANDING: [&[u8]; 2] = [EPOCH, BOOTSTRAP];

/// A whole copy under way: the history it brings, the epoch of the primary
/// it comes from, and, once said for each database, where its copies follow
/// from and the write replay reaches before the copy is one state.
struct Whole {
    history: u128,
    epoch: u64,
    said: [Option<(u64, u64)>; DATABASES],
}

/// What a standby taking its primary's frames holds between them.
pub(super) struct Taking {
    /// Each database written since its last sync.
    pub(super) dirty: [bool; DATABASES],
    /// When it last synced, on the node's monotonic reading.
    synced: i128,
    whole: Option<Whole>,
}

impl Taking {
    /// Before the first frame, at `now`: nothing written, nothing copied.
    pub(super) fn new(now: i128) -> Self {
        Self {
            dirty: [false; DATABASES],
            synced: now,
            whole: None,
        }
    }
}

/// The socket a standby follows its primary on, while it does: what its
/// stop shuts, which ends a read blocked on it at once; and whether its
/// primary has handed it anything on it yet.
#[derive(Default)]
pub(super) struct Followed {
    socket: Mutex<Option<TcpStream>>,
    /// Set by the first frame its primary hands it on the socket held, so
    /// it is following only once its primary hands it writes: a stop of
    /// that primary hands over to it (`super::succession`).
    heard: AtomicBool,
}

impl Followed {
    /// `socket` the one followed on from now, or none: nothing heard on it
    /// yet.
    fn hold(&self, socket: Option<TcpStream>) {
        self.heard.store(false, Ordering::SeqCst);
        *self.socket.locked() = socket;
    }

    /// The socket followed on shut, both ways, where there is one.
    pub(super) fn shut(&self) {
        if let Some(socket) = self.socket.locked().take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    /// Whether its primary has handed it a frame on the socket held.
    pub(super) fn heard(&self) -> bool {
        self.heard.load(Ordering::SeqCst)
    }
}

impl StorageNode {
    /// Follow the primary at `address` until it is lost or stops in order:
    /// how it ended, each database written since its last sync synced. A
    /// successor synced them already, so it syncs none again before it
    /// serves (open problem 141; until 2026-11-24 every database was synced
    /// again here).
    pub(super) fn follow(&self, address: &str) -> Ended {
        let mut taking = Taking::new(self.clock.monotonic_nanos());
        let followed = self.following(address, &mut taking);
        self.followed.hold(None);
        let _ = self.synced(&mut taking.dirty);
        followed.unwrap_or_else(|problem| {
            if !self.stopping() {
                self.told(&format!("stopped following {address}: {problem}"));
            }
            Ended::Lost
        })
    }

    /// Its primary's frames taken until the primary falls silent for the
    /// silence, on the node's clock, stops in order, or the node stops: read
    /// through the estate's watched connection (`net::watched`), which looks
    /// at the stop each [`WATCH`] a read waits — Windows wakes a read blocked
    /// on a socket shut from another thread only when its peer closes — and
    /// the socket held for the stop to shut, which ends the read at once
    /// where the platform wakes it, Linux and macOS.
    fn following(&self, address: &str, taking: &mut Taking) -> Result<Ended, String> {
        let mut connection = self.connect(address, self.timings.silence)?;
        // Held before the stop is looked at, so a stop that shuts nothing
        // yet is seen before the first read.
        self.followed.hold(connection.sock.try_clone().ok());
        let asked = self.asked();
        wire::send(&mut connection, &Request::Follow(asked)).map_err(|e| e.to_string())?;
        connection
            .sock
            .set_read_timeout(Some(WATCH))
            .map_err(|e| e.to_string())?;
        let mut watched = Watched {
            silence: Some(self.timings.silence),
            ..Watched::new(
                &mut connection,
                || self.stopping(),
                || self.clock.monotonic_nanos(),
            )
        };
        while !self.stopping() {
            let frame = wire::receive::<Shipped>(&mut watched)
                .map_err(|e| format!("heard nothing for {:?}: {e}", self.timings.silence))?
                .ok_or("the primary ended the connection")?;
            // Handed its first frame, it is following: told, so its node
            // publishes it at once.
            if !self.followed.heard.swap(true, Ordering::SeqCst) {
                self.tell();
            }
            if let Shipped::Stopped(epoch, next, successor) = frame {
                return Ok(self.succeeding(&mut watched, (epoch, next, successor), taking));
            }
            self.take(frame, taking)?;
        }
        Ok(Ended::Lost)
    }

    /// Where it asks to be followed from: where it is; or nowhere, so it is
    /// copied whole, while it is copying, or while it catches up on a copy
    /// from the primary of another epoch than the one it follows now, whose
    /// writes from where it is need not be those its pages reflect. Its
    /// place among the Storage nodes listed is its primary's to work out,
    /// from the certificate it proves (`super::proven`).
    pub(super) fn asked(&self) -> Follow {
        let epoch = self.held().epoch;
        let goes_on = match self.bootstrap() {
            Bootstrap::Ready => true,
            Bootstrap::Copying => false,
            Bootstrap::CatchingUp { epoch: copied, .. } => copied == epoch,
        };
        Follow {
            epoch,
            history: if goes_on { self.copies.history() } else { 0 },
            next: self.copies.next(),
        }
    }

    /// One frame of its primary's taken, in the order sent.
    ///
    /// # Errors
    ///
    /// Where the primary declines, a frame comes out of order or a write
    /// fails: it stops following then.
    pub(super) fn take(&self, frame: Shipped, taking: &mut Taking) -> Result<(), String> {
        let failed = |e: PersistError| e.to_string();
        match frame {
            Shipped::Beat(epoch) => {
                self.keep_epoch(epoch).map_err(failed)?;
                self.synced(&mut taking.dirty).map_err(failed)?;
                taking.synced = self.clock.monotonic_nanos();
            }
            Shipped::Copy(database, number, changes) => {
                let at = usize::from(database);
                self.copies
                    .copied(self.engines[at].as_ref(), (at, number), changes.into())
                    .map_err(failed)?;
                taking.dirty[at] = true;
            }
            Shipped::Whole(history, epoch, keys) => {
                // Nothing held is any history's until the copy is whole, and
                // the node is not promoted until it is ready (`Bootstrap`).
                self.bootstrapped(Bootstrap::Copying, &[]).map_err(failed)?;
                self.copies.restart(0, [1; DATABASES]);
                self.wiped(&keys).map_err(failed)?;
                self.keep_epoch(epoch).map_err(failed)?;
                taking.whole = Some(Whole {
                    history,
                    epoch,
                    said: [None; DATABASES],
                });
            }
            Shipped::Entries(database, entries) => {
                let changes: Vec<Change> = entries
                    .into_iter()
                    .filter(|(key, _)| !key.starts_with(OWN))
                    .map(|(key, value)| (key, Some(value)))
                    .collect();
                let at = usize::from(database);
                let changes = borrowed(&changes);
                self.engines[at].apply_deferred(&changes).map_err(failed)?;
                taking.dirty[at] = true;
            }
            Shipped::Copied(database, from, through) => {
                let copying = taking
                    .whole
                    .as_mut()
                    .ok_or("a whole copy's end with no start")?;
                copying.said[usize::from(database)] = Some((from, through));
                if let Some(whole) = taking.whole.take_if(|w| w.said.iter().all(Option::is_some)) {
                    self.copied_whole(&whole).map_err(failed)?;
                    taking.dirty = [false; DATABASES];
                }
            }
            Shipped::Declined(reason) => return Err(format!("declined: {reason}")),
            // Answered on the connection before it is taken: following ends
            // there (`super::succession`), the acknowledgment read there too.
            Shipped::Stopped(..) => return Err("the primary stopped in order".to_string()),
            Shipped::Acknowledged(_) => {
                return Err("an acknowledgment of an epoch never kept".to_string());
            }
        }
        let now = self.clock.monotonic_nanos();
        if between(taking.synced, now) >= SYNC {
            self.synced(&mut taking.dirty).map_err(failed)?;
            taking.synced = now;
        }
        Ok(())
    }

    /// Each database written since its last sync synced, its copies so far
    /// on disk (`Copies::sync`); then ready, where that caught it up
    /// (`StorageNode::caught_up`).
    pub(super) fn synced(&self, dirty: &mut [bool; DATABASES]) -> Result<(), PersistError> {
        let databases = self.engines.iter().zip(dirty.iter_mut()).enumerate();
        for (at, (engine, dirty)) in databases {
            if *dirty {
                self.copies.sync(at, engine.as_ref())?;
                *dirty = false;
            }
        }
        self.caught_up()
    }

    /// Every database emptied, but for the node's own standing ([`STANDING`]),
    /// and given its primary's data key, wrapped under this node's own
    /// key-encryption key, before a whole copy: page by page, each scan on
    /// from the last key the one before removed, the stop looked at before
    /// each. Until 2026-11-22 each page was scanned from the first key, so
    /// a log-structured engine walked every key removed before it again, a
    /// tombstone each until compacted.
    ///
    /// # Errors
    ///
    /// Where an engine fails, or the node stops: it is copying still, and
    /// copied whole again at its next start (`Bootstrap::Copying`).
    fn wiped(&self, keys: &[Vec<u8>]) -> Result<(), PersistError> {
        if keys.len() != DATABASES {
            return Err(PersistError::Failed {
                reason: format!("a whole copy handed {} data keys", keys.len()),
            });
        }
        for (engine, key) in self.engines.iter().zip(keys) {
            let mut first = Vec::new();
            loop {
                if self.stopping() {
                    return Err(PersistError::Failed {
                        reason: "the wipe stopped: this Storage node is stopping".to_string(),
                    });
                }
                let page = engine.scan(&first, &LAST, PAGE, false)?;
                let Some((last, _)) = page.last() else {
                    break;
                };
                first = [last.as_slice(), &[0]].concat();
                let gone: Vec<Change> = page
                    .into_iter()
                    .filter(|(key, _)| !STANDING.contains(&key.as_slice()))
                    .map(|(key, _)| (key, None))
                    .collect();
                if !gone.is_empty() {
                    engine.apply_deferred(&borrowed(&gone))?;
                }
            }
            let wrapped = self.keys.wrap(&self.kek, &DataKey::taken(key)?)?;
            engine.write(DATA_KEY, &wrapped)?;
        }
        Ok(())
    }

    /// Every page of a whole copy in: each database's position written,
    /// durable, the runtime database's in one write with catching up as
    /// far as the primary said the copy reaches, and its writes numbered on
    /// from where its copies follow — ready at once where the primary took
    /// no write while it scanned, each database synced by its position.
    fn copied_whole(&self, whole: &Whole) -> Result<(), PersistError> {
        let from = whole.said.map(|said| said.map_or(1, |(from, _)| from));
        let through = whole
            .said
            .map(|said| said.map_or(1, |(_, through)| through));
        for (engine, from) in self.engines.iter().zip(from).skip(1) {
            engine.apply(&borrowed(&[position(whole.history, from)]))?;
        }
        let catching_up = Bootstrap::CatchingUp {
            epoch: whole.epoch,
            through,
        };
        self.bootstrapped(catching_up, &[position(whole.history, from[0])])?;
        self.copies.restart(whole.history, from);
        self.caught_up()
    }
}
