//! What a batch moved, for its writer to tell (`super`): each queue it put a
//! Message in, where the batch leaves its tail, and the holder whose claims
//! taken in the same batch hold every Message it put there, where one does
//! — a node's Publication into a queue it sends, its Journeys claimed in
//! it; each queue it took an entry out of, where it leaves its head; each
//! Instance a turn moved; and each key of the Instance Key table it gave to
//! an Instance or let go. That holder's own waiter is told quietly, never
//! woken: until 2026-11-12 each such Publication woke the node's own watch,
//! and its send step read the queue over the wire only to find its own
//! claims (open problem 46).

use std::collections::HashMap;

use codec::form::Form;

use super::super::claim::{Standing, Stored};
use super::super::commit::{Batch, CLAIM, HELD, PLACES};
use super::super::hold::Held;
use super::super::instance::{INSTANCE, INSTANCE_KEY, turns_of};
use super::super::queue::Places;
use super::Watched;
use crate::{EncryptedStore, Engine};

/// What a batch moved of one thing a wait watches.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Change {
    pub(crate) watched: Watched,
    /// Where the batch leaves it; `None` where it cannot be read, read
    /// again by the next wait on it.
    pub(crate) at: Option<u64>,
    /// For a queue's tail, the holder whose claims in the batch hold every
    /// Message the batch put in it, where one does.
    pub(crate) own: Option<String>,
}

/// Everything `batch` moves that a wait watches, where the batch leaves it:
/// what its writer tells the waiters once the batch is readable. Read
/// before the batch is in the engine, as it found each.
pub(crate) fn moved<R: Engine>(store: &EncryptedStore<R>, batch: &Batch<'_>) -> Vec<Change> {
    let mut changes = queues(store, batch);
    changes.extend(counted(
        store,
        batch,
        (INSTANCE, Watched::Instance),
        turns_of,
    ));
    changes.extend(counted(store, batch, (INSTANCE_KEY, Watched::Key), |_| {
        Some(1)
    }));
    changes
}

/// Each queue whose places `batch` writes: its tail where an entry was put
/// in it, its head where one was taken out.
fn queues<R: Engine>(store: &EncryptedStore<R>, batch: &Batch<'_>) -> Vec<Change> {
    let places = |bytes: Option<&[u8]>| bytes.map_or(Some(Places::default()), read_places);
    let holders = holders(batch);
    let mut changes = Vec::new();
    for (key, after) in batch.written_of(PLACES) {
        let Some(queue) = key.try_into().ok().map(u128::from_be_bytes) else {
            continue;
        };
        let before = batch.before(store, PLACES, key).ok();
        let (before, after) = (before.and_then(|b| places(b.as_deref())), places(after));
        let (Some(before), Some(after)) = (before, after) else {
            for watched in [Watched::Queue(queue), Watched::Head(queue)] {
                changes.push(unread(watched));
            }
            continue;
        };
        if after.next != before.next {
            changes.push(Change {
                watched: Watched::Queue(queue),
                at: Some(after.next),
                own: own(batch, &holders, (queue, before.next)),
            });
        }
        if after.taken() != before.taken() {
            changes.push(Change {
                watched: Watched::Head(queue),
                at: Some(after.taken()),
                own: None,
            });
        }
    }
    changes
}

fn read_places(bytes: &[u8]) -> Option<Places> {
    Places::from_bytes(bytes).ok()
}

/// One that cannot be read: read again by the next wait on it.
const fn unread(watched: Watched) -> Change {
    Change {
        watched,
        at: None,
        own: None,
    }
}

/// Each record of `kind` — keyed by its identifier, watched as `watched`
/// says — that `batch` writes and whose count, as `count` reads a kept
/// record, it moves; none where it holds none.
fn counted<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    (kind, watched): (&'static str, fn(u128) -> Watched),
    count: impl Fn(&[u8]) -> Option<u64>,
) -> Vec<Change> {
    let counted = |bytes: Option<&[u8]>| bytes.map_or(Some(0), &count);
    batch
        .written_of(kind)
        .filter_map(|(key, after)| {
            let id = u128::from_be_bytes(key.try_into().ok()?);
            let before = batch.before(store, kind, key).ok();
            match (before.and_then(|b| counted(b.as_deref())), counted(after)) {
                (Some(before), Some(after)) => (after != before).then_some(Change {
                    watched: watched(id),
                    at: Some(after),
                    own: None,
                }),
                _ => Some(unread(watched(id))),
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
