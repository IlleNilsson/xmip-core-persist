//! A standby following its primary (`super`): it asks to be handed the
//! primary's writes from where it is, and takes them in order — first the
//! whole of each database where it cannot take them on from there, then
//! the writes taken meanwhile until it is caught up (`super::bootstrap`) —
//! until the primary goes silent, declines or the node stops.
use std::time::{Duration, Instant};

use secret::DataKey;

use super::super::wire::{self, Request};
use super::bootstrap::{BOOTSTRAP, Bootstrap};
use super::copies::DATABASES;
use super::position::position;
use super::{EPOCH, Follow, Shipped, StorageNode};
use crate::encrypted_store::{DATA_KEY, OWN};
use crate::{Change, PersistError};

/// How long a standby writes copies before it syncs them: a copy counts
/// on a standby once synced, as a write does on its primary.
const SYNC: Duration = Duration::from_millis(10);

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
    dirty: [bool; DATABASES],
    synced: Instant,
    whole: Option<Whole>,
}

impl Taking {
    /// Before the first frame: nothing written, nothing copied.
    pub(super) fn new() -> Self {
        Self {
            dirty: [false; DATABASES],
            synced: Instant::now(),
            whole: None,
        }
    }
}

impl StorageNode {
    /// Follow the primary at `address` until it is lost.
    pub(super) fn follow(&self, address: &str) {
        let followed = self.following(address);
        if let Err(problem) = followed
            && !self.stopping()
        {
            self.told(&format!("stopped following {address}: {problem}"));
        }
        let _ = self.synced(&mut [true; DATABASES]);
    }

    fn following(&self, address: &str) -> Result<(), String> {
        let mut connection = self.connect(address, self.timings.silence)?;
        let asked = self.asked();
        wire::send(&mut connection, &Request::Follow(asked)).map_err(|e| e.to_string())?;
        let mut taking = Taking::new();
        while !self.stopping() {
            let frame = wire::receive::<Shipped>(&mut connection)
                .map_err(|e| format!("heard nothing for {:?}: {e}", self.timings.silence))?
                .ok_or("the primary ended the connection")?;
            self.take(frame, &mut taking)?;
        }
        Ok(())
    }

    /// Where it asks to be followed from: where it is; or nowhere, so it is
    /// copied whole, while it is copying, or while it catches up on a copy
    /// from the primary of another epoch than the one it follows now, whose
    /// writes from where it is need not be those its pages reflect.
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
                taking.synced = Instant::now();
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
        }
        if taking.synced.elapsed() >= SYNC {
            self.synced(&mut taking.dirty).map_err(failed)?;
            taking.synced = Instant::now();
        }
        Ok(())
    }

    /// Each database written since its last sync synced, its copies so far
    /// on disk (`Copies::sync`); then ready, where that caught it up
    /// (`StorageNode::caught_up`).
    fn synced(&self, dirty: &mut [bool; DATABASES]) -> Result<(), PersistError> {
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
    /// key-encryption key, before a whole copy.
    fn wiped(&self, keys: &[Vec<u8>]) -> Result<(), PersistError> {
        if keys.len() != DATABASES {
            return Err(PersistError::Failed {
                reason: format!("a whole copy handed {} data keys", keys.len()),
            });
        }
        for (engine, key) in self.engines.iter().zip(keys) {
            loop {
                let page = engine.scan(&[], &LAST, PAGE, false)?;
                if page.is_empty() {
                    break;
                }
                let gone: Vec<Change> = page
                    .into_iter()
                    .filter(|(key, _)| !STANDING.contains(&key.as_slice()))
                    .map(|(key, _)| (key, None))
                    .collect();
                if gone.is_empty() {
                    break;
                }
                engine.apply_deferred(&gone)?;
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
            engine.apply(&[position(whole.history, from)])?;
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
