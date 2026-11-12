//! What a batch moved, for its writer to tell (`super`): each queue it put a
//! Message in, where the batch leaves it, and the holder whose claims taken
//! in the same batch hold every Message it put there, where one does — a
//! node's Publication into a queue it sends, its Journeys claimed in it.
//! That holder's own waiter is told quietly, never woken: until 2026-11-12
//! each such Publication woke the node's own watch, and its send step read
//! the queue over the wire only to find its own claims (open problem 46).

use std::collections::HashMap;

use codec::form::Form;

use super::super::claim::{Standing, Stored};
use super::super::commit::{Batch, CLAIM, HELD, PLACES};
use super::super::hold::Held;
use super::super::queue::Places;
use crate::{EncryptedStore, Engine};

/// A queue a batch moved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Moved {
    pub(crate) queue: u128,
    /// Where the batch leaves it; `None` where its places cannot be read,
    /// read again by the next wait on it.
    pub(crate) next: Option<u64>,
    /// The holder whose claims in the batch hold every Message the batch
    /// put in it, where one does.
    pub(crate) own: Option<String>,
}

/// Every queue `batch` puts a Message in, where the batch leaves it: what its
/// writer tells the waiters once the batch is readable. Read before the batch
/// is in the engine, as it found each queue.
pub(crate) fn moved<R: Engine>(store: &EncryptedStore<R>, batch: &Batch<'_>) -> Vec<Moved> {
    let next = |bytes: Option<&[u8]>| {
        bytes.map_or(Some(0), |bytes| {
            Places::from_bytes(bytes).ok().map(|places| places.next)
        })
    };
    let holders = holders(batch);
    batch
        .written_of(PLACES)
        .filter_map(|(key, after)| {
            let queue = u128::from_be_bytes(key.try_into().ok()?);
            let before = batch.before(store, PLACES, key).ok();
            let (before, after) = (before.and_then(|b| next(b.as_deref())), next(after));
            let own = before.and_then(|from| own(batch, &holders, (queue, from)));
            match (before, after) {
                (Some(before), Some(after)) => (after != before).then_some(Moved {
                    queue,
                    next: Some(after),
                    own,
                }),
                _ => Some(Moved {
                    queue,
                    next: None,
                    own: None,
                }),
            }
        })
        .collect()
}

/// By Journey, the holder of each claim `batch` takes or keeps held.
fn holders(batch: &Batch<'_>) -> HashMap<u128, String> {
    batch
        .written_of(CLAIM)
        .filter_map(|(key, value)| {
            let journey = u128::from_be_bytes(key.try_into().ok()?);
            let stored = Stored::from_bytes(value?).ok()?;
            (stored.standing == Standing::Held).then_some((journey, stored.claim.holder))
        })
        .collect()
}

/// The one holder of `holders` whose claims hold every Message `batch` put
/// in `queue` at or after the place `from`, where one does.
fn own(
    batch: &Batch<'_>,
    holders: &HashMap<u128, String>,
    (queue, from): (u128, u64),
) -> Option<String> {
    let mut own: Option<&String> = None;
    for (key, value) in batch.written_of(HELD) {
        let (Some(at), Some(place)) = (key.get(..16), key.get(16..24)) else {
            continue;
        };
        let at = u128::from_be_bytes(at.try_into().ok()?);
        let place = u64::from_be_bytes(place.try_into().ok()?);
        if at != queue || place < from {
            continue;
        }
        let held = Held::from_bytes(value?).ok()?;
        let holder = holders.get(&held.hold.journey.value())?;
        if own.is_some_and(|own| own != holder) {
            return None;
        }
        own = Some(holder);
    }
    own.cloned()
}
