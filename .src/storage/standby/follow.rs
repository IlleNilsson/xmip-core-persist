//! A standby following its primary (`super`): it asks to be handed the
//! primary's writes from where it is, and takes them in order — first the
//! whole of each database where it cannot take them on from there — until
//! the primary goes silent, declines or the node stops.

use std::sync::PoisonError;
use std::time::{Duration, Instant};

use secret::DataKey;

use super::super::wire::{self, Request};
use super::copies::{DATABASES, position};
use super::{Follow, Shipped, StorageNode};
use crate::encrypted_store::{DATA_KEY, OWN};
use crate::{Change, PersistError};

/// How long a standby writes copies before it syncs them: a copy counts
/// on a standby once synced, as a write does on its primary.
const SYNC: Duration = Duration::from_millis(10);

/// The most entries a page of a whole copy's wipe removes at once.
const PAGE: usize = 1024;

/// The engine key after every key an engine keeps.
pub(super) const LAST: [u8; 1024] = [0xff; 1024];

/// A whole copy under way: the history it brings, and where each
/// database's copies follow from, once said.
struct Whole {
    history: u128,
    next: [Option<u64>; DATABASES],
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
        let asked = Follow {
            epoch: self.held().epoch,
            history: self.copies.history(),
            next: self.copies.next(),
        };
        wire::send(&mut connection, &Request::Follow(asked)).map_err(|e| e.to_string())?;
        let mut dirty = [false; DATABASES];
        let mut synced = Instant::now();
        let mut whole: Option<Whole> = None;
        while !self.stopping() {
            let frame = wire::receive::<Shipped>(&mut connection)
                .map_err(|e| format!("heard nothing for {:?}: {e}", self.timings.silence))?
                .ok_or("the primary ended the connection")?;
            match frame {
                Shipped::Beat(epoch) => {
                    self.heard(epoch);
                    self.synced(&mut dirty).map_err(|e| e.to_string())?;
                    synced = Instant::now();
                }
                Shipped::Copy(database, number, changes) => {
                    let at = usize::from(database);
                    self.copies
                        .copied(self.engines[at].as_ref(), (at, number), changes.into())
                        .map_err(|e| e.to_string())?;
                    dirty[at] = true;
                    self.heard(0);
                }
                Shipped::Whole(history, epoch, keys) => {
                    // Nothing held is any history's until the copy is whole.
                    self.copies.restart(0, [1; DATABASES]);
                    self.wiped(&keys).map_err(|e| e.to_string())?;
                    self.heard(epoch);
                    whole = Some(Whole {
                        history,
                        next: [None; DATABASES],
                    });
                }
                Shipped::Entries(database, entries) => {
                    let changes: Vec<Change> = entries
                        .into_iter()
                        .filter(|(key, _)| !key.starts_with(OWN))
                        .map(|(key, value)| (key, Some(value)))
                        .collect();
                    let at = usize::from(database);
                    self.engines[at]
                        .apply_deferred(&changes)
                        .map_err(|e| e.to_string())?;
                    dirty[at] = true;
                }
                Shipped::Copied(database, next) => {
                    let taken = whole.as_mut().ok_or("a whole copy's end with no start")?;
                    taken.next[usize::from(database)] = Some(next);
                    if let Some(whole) = whole.take_if(|w| w.next.iter().all(Option::is_some)) {
                        self.copied_whole(&whole).map_err(|e| e.to_string())?;
                    }
                }
                Shipped::Declined(reason) => return Err(format!("declined: {reason}")),
            }
            if synced.elapsed() >= SYNC {
                self.synced(&mut dirty).map_err(|e| e.to_string())?;
                synced = Instant::now();
            }
        }
        Ok(())
    }

    /// The primary heard, in `epoch` where it says one.
    fn heard(&self, epoch: u64) {
        let later = {
            let mut held = self.held.write().unwrap_or_else(PoisonError::into_inner);
            held.heard = Instant::now();
            let later = epoch > held.epoch;
            held.epoch = held.epoch.max(epoch);
            later.then_some(held.epoch)
        };
        if let Some(epoch) = later {
            let _ = self.engines[0].write(super::EPOCH, &epoch.to_be_bytes());
        }
    }

    /// Each database written since its last sync synced.
    fn synced(&self, dirty: &mut [bool; DATABASES]) -> Result<(), PersistError> {
        for (engine, dirty) in self.engines.iter().zip(dirty.iter_mut()) {
            if *dirty {
                engine.sync()?;
                *dirty = false;
            }
        }
        Ok(())
    }

    /// Every database emptied and given its primary's data key, wrapped
    /// under this node's own key-encryption key, before a whole copy.
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
                let gone: Vec<Change> = page.into_iter().map(|(key, _)| (key, None)).collect();
                engine.apply_deferred(&gone)?;
            }
            let wrapped = self.keys.wrap(&self.kek, &DataKey::taken(key)?)?;
            engine.write(DATA_KEY, &wrapped)?;
        }
        Ok(())
    }

    /// A whole copy taken: each database's position written and synced,
    /// and its writes numbered on from where its copies follow.
    fn copied_whole(&self, whole: &Whole) -> Result<(), PersistError> {
        let next = whole.next.map(|next| next.unwrap_or(1));
        for (engine, next) in self.engines.iter().zip(next) {
            engine.apply(&[position(whole.history, next)])?;
        }
        self.copies.restart(whole.history, next);
        Ok(())
    }
}
