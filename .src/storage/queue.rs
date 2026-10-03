//! A queue in the runtime database, as Xmip Storage keeps each of them:
//! what a paused Subscription holds ([`super::hold`]) and a node's Dead
//! Message Queue ([`super::dead`]).
//!
//! **Numbered as it is written.** Xmip Storage gives each entry the queue's
//! next place, from zero, so the order of a queue is the Ledger's, kept
//! across a restart and the same to every node (`runtime-model.md` section
//! 3, *Sequential enforcement is state-based and durable*). A queue keeps
//! its places — its first, its next and how many it holds — beside its
//! entries, and an index from what each entry is about to its place, so a
//! write asked again after a lost answer finds it placed already and takes
//! no second place.
//!
//! **Taken out in any order.** An entry removed out of turn leaves a gap
//! that a read passes over; the queue's first moves past every gap before
//! it, so the oldest it still holds is always where a read starts.

use codec::cursor::Cursor;

use super::commit::Batch;
use super::record::{Form, read_u64, write_u64};
use crate::{EncryptedStore, Engine, PersistError};

/// Where one kind of queue keeps what it keeps: its entries by queue and
/// place, its index by queue and what an entry is about, and its places by
/// queue.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Kinds {
    pub(crate) entry: &'static str,
    pub(crate) by: &'static str,
    pub(crate) places: &'static str,
}

/// A queue's places, as the runtime database keeps them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Places {
    /// The place of the oldest it still holds; `next` where it holds none.
    pub(crate) first: u64,
    /// The place the next entry takes.
    pub(crate) next: u64,
    /// How many it holds.
    pub(crate) count: u64,
}

/// Where an entry is kept: its queue and its place.
pub(crate) fn entry_key(queue: u128, sequence: u64) -> Vec<u8> {
    [
        queue.to_be_bytes().as_slice(),
        sequence.to_be_bytes().as_slice(),
    ]
    .concat()
}

/// Where a queue's places are kept: under the queue.
pub(crate) fn places_key(queue: u128) -> Vec<u8> {
    queue.to_be_bytes().to_vec()
}

/// Where the index keeps an entry's place: its queue and what it is about.
pub(crate) fn by_key(queue: u128, about: u128) -> Vec<u8> {
    [queue.to_be_bytes().as_slice(), about.to_be_bytes().as_slice()].concat()
}

/// A queue's places in `store`, as `batch` has left them.
pub(crate) fn places<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    kinds: Kinds,
    queue: u128,
) -> Result<Places, PersistError> {
    batch
        .read(store, kinds.places, &places_key(queue))?
        .map_or(Ok(Places::default()), |bytes| Places::from_bytes(&bytes))
}

/// An entry about `about` placed at the end of `queue`, in `batch`: the
/// place it takes is the queue's next, and `entry` makes what is kept there
/// from it. `false`, and nothing written, where the index has `about` in
/// the queue already — placed before, or taken out and remembered so.
pub(crate) fn place<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    kinds: Kinds,
    (queue, about): (u128, u128),
    entry: impl FnOnce(u64) -> Vec<u8>,
) -> Result<bool, PersistError> {
    let by = by_key(queue, about);
    if batch.read(store, kinds.by, &by)?.is_some() {
        return Ok(false);
    }
    let mut places = places(store, batch, kinds, queue)?;
    let sequence = places.next;
    batch.put(kinds.entry, entry_key(queue, sequence), Some(entry(sequence)));
    batch.put(kinds.by, by, Some(sequence.to_be_bytes().to_vec()));
    places.next += 1;
    places.count += 1;
    batch.put(kinds.places, places_key(queue), Some(places.bytes()));
    Ok(true)
}

/// What the index keeps for something in a queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Placed {
    /// Never placed there.
    Never,
    /// Placed, and taken out and remembered so ([`remember`]).
    TakenOut,
    /// At this place.
    At(u64),
}

/// What the index keeps for `about` in `queue`, as `batch` has left it.
pub(crate) fn placed<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &Batch<'_>,
    kinds: Kinds,
    (queue, about): (u128, u128),
) -> Result<Placed, PersistError> {
    let Some(bytes) = batch.read(store, kinds.by, &by_key(queue, about))? else {
        return Ok(Placed::Never);
    };
    if bytes.is_empty() {
        return Ok(Placed::TakenOut);
    }
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| super::record::malformed("a place that is not eight bytes"))?;
    Ok(Placed::At(u64::from_be_bytes(array)))
}

/// The entry at `sequence` in `queue` taken out, in `batch`: what was kept
/// there, or `None` where nothing is — taken out already, so a removal asked
/// again does nothing twice. Its index is the caller's: forgotten, or
/// remembered as taken out ([`forget`], [`remember`]).
pub(crate) fn take_out<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    kinds: Kinds,
    queue: u128,
    sequence: u64,
) -> Result<Option<Vec<u8>>, PersistError> {
    let Some(bytes) = batch.read(store, kinds.entry, &entry_key(queue, sequence))? else {
        return Ok(None);
    };
    batch.put(kinds.entry, entry_key(queue, sequence), None);
    let mut places = places(store, batch, kinds, queue)?;
    places.count = places.count.saturating_sub(1);
    while places.first < places.next
        && batch
            .read(store, kinds.entry, &entry_key(queue, places.first))?
            .is_none()
    {
        places.first += 1;
    }
    batch.put(kinds.places, places_key(queue), Some(places.bytes()));
    Ok(Some(bytes))
}

/// The index of `about` in `queue` forgotten: placed again, it takes a new
/// place.
pub(crate) fn forget(batch: &mut Batch<'_>, kinds: Kinds, (queue, about): (u128, u128)) {
    batch.put(kinds.by, by_key(queue, about), None);
}

/// The index of `about` in `queue` kept as taken out: placed again, it is
/// not ([`place`]), and [`placed`] says so.
pub(crate) fn remember(batch: &mut Batch<'_>, kinds: Kinds, (queue, about): (u128, u128)) {
    batch.put(kinds.by, by_key(queue, about), Some(Vec::new()));
}

/// `queue` as `store` keeps it: its places, and up to `most` of its entries
/// from the place `from` on, oldest first. A place taken out of turn is
/// passed over.
pub(crate) fn page<R: Engine>(
    store: &EncryptedStore<R>,
    kinds: Kinds,
    queue: u128,
    from: u64,
    most: u32,
) -> Result<(Places, Vec<Vec<u8>>), PersistError> {
    let places = store
        .get(kinds.places, &places_key(queue))?
        .map_or(Ok(Places::default()), |bytes| Places::from_bytes(&bytes))?;
    let mut entries = Vec::new();
    let mut at = from.max(places.first);
    while at < places.next && entries.len() < most as usize {
        if let Some(bytes) = store.get(kinds.entry, &entry_key(queue, at))? {
            entries.push(bytes);
        }
        at += 1;
    }
    Ok((places, entries))
}

impl Form for Places {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.first);
        write_u64(out, self.next);
        write_u64(out, self.count);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            first: read_u64(cursor)?,
            next: read_u64(cursor)?,
            count: read_u64(cursor)?,
        })
    }
}
