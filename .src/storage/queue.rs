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
//!
//! **What a gap costs.** An entry is kept under a keyed hash
//! ([`crate::EncryptedStore`]), which sorts nowhere, so the next entry is
//! found by asking each place in turn. A read stops once it has found as
//! many as the queue holds from where it starts, and a queue emptied moves
//! its first to its next at once; a gap with an entry held on each side of
//! it is still read place by place, by a page and by the move of the first
//! past it.

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
    [
        queue.to_be_bytes().as_slice(),
        about.to_be_bytes().as_slice(),
    ]
    .concat()
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
    batch.put(
        kinds.entry,
        entry_key(queue, sequence),
        Some(entry(sequence)),
    );
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
    if places.count == 0 {
        // Empty: its first is its next, with no gap walked to find it.
        places.first = places.next;
    }
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
/// passed over, and none is read once every entry the queue can hold from
/// `from` on has been found.
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
    let mut at = from.max(places.first);
    // The most it can hold from `at` on: all it holds, or, past its first
    // — which it holds — one fewer. Once found, no later place is read.
    let left = if at > places.first {
        places.count.saturating_sub(1)
    } else {
        places.count
    };
    let most = usize::try_from(left.min(u64::from(most))).unwrap_or(usize::MAX);
    let mut entries = Vec::new();
    while at < places.next && entries.len() < most {
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

#[cfg(test)]
mod tests {
    use super::super::hold::KINDS;
    use super::*;
    use crate::fixture::Memory;
    use crate::{Change, Entry};
    use secret::{Held, KekName};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// An engine in memory that counts the keys read of it.
    #[derive(Default)]
    struct Counted {
        memory: Memory,
        reads: AtomicUsize,
    }

    impl Counted {
        /// The keys read since it was last asked.
        fn reads(&self) -> usize {
            self.reads.swap(0, Ordering::SeqCst)
        }
    }

    impl Engine for Counted {
        fn engine(&self) -> &'static str {
            "counted"
        }

        fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, PersistError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.memory.read(key)
        }

        fn write(&self, key: &[u8], value: &[u8]) -> Result<(), PersistError> {
            self.memory.write(key, value)
        }

        fn write_new(&self, key: &[u8], value: &[u8]) -> Result<bool, PersistError> {
            self.memory.write_new(key, value)
        }

        fn remove(&self, key: &[u8]) -> Result<(), PersistError> {
            self.memory.remove(key)
        }

        fn apply(&self, batch: &[Change]) -> Result<(), PersistError> {
            self.memory.apply(batch)
        }

        fn scan(
            &self,
            first: &[u8],
            last: &[u8],
            most: usize,
            reverse: bool,
        ) -> Result<Vec<Entry>, PersistError> {
            self.reads.fetch_add(most, Ordering::SeqCst);
            self.memory.scan(first, last, most, reverse)
        }
    }

    const QUEUE: u128 = 7;

    /// A queue whose entries at `held` it still holds, the rest of the
    /// places before `next` taken out.
    fn queue<'c>(counted: &'c Counted, held: &[u64], next: u64) -> EncryptedStore<&'c Counted> {
        let keys = Held::new(secret::fixture::Memory::default());
        let kek = KekName::new("runtime").expect("a name");
        let store = EncryptedStore::open(counted, &keys, &kek).expect("open");
        for &sequence in held {
            let entry = sequence.to_be_bytes();
            let key = entry_key(QUEUE, sequence);
            store.put(KINDS.entry, &key, &entry).expect("an entry");
        }
        let places = Places {
            first: held.first().copied().unwrap_or(next),
            next,
            count: held.len() as u64,
        };
        let key = places_key(QUEUE);
        store
            .put(KINDS.places, &key, &places.bytes())
            .expect("places");
        counted.reads();
        store
    }

    fn sequences(entries: &[Vec<u8>]) -> Vec<u64> {
        let place = |bytes: &Vec<u8>| u64::from_be_bytes(bytes.as_slice().try_into().expect("8"));
        entries.iter().map(place).collect()
    }

    #[test]
    fn a_page_of_a_queue_holding_its_oldest_alone_reads_what_it_returns() {
        let counted = Counted::default();
        let store = queue(&counted, &[0], 1_000_001);
        let (_, entries) = page(&store, KINDS, QUEUE, 0, 10).expect("a page");
        assert_eq!(sequences(&entries), [0]);
        assert_eq!(counted.reads(), 2, "its places and its one entry");
        let (_, entries) = page(&store, KINDS, QUEUE, 1, 10).expect("the next");
        assert!(entries.is_empty());
        assert_eq!(counted.reads(), 1, "its places alone");
    }

    #[test]
    fn its_oldest_taken_out_empties_a_queue_without_reading_its_gap() {
        let counted = Counted::default();
        let store = queue(&counted, &[0], 1_000_001);
        let mut batch = Batch::empty();
        let taken = take_out(&store, &mut batch, KINDS, QUEUE, 0).expect("taken");
        assert_eq!(taken.as_deref(), Some(0u64.to_be_bytes().as_slice()));
        assert_eq!(counted.reads(), 2, "the entry and its places");
        let left = places(&store, &batch, KINDS, QUEUE).expect("places");
        let empty = Places {
            first: 1_000_001,
            next: 1_000_001,
            count: 0,
        };
        assert_eq!(left, empty);
    }

    #[test]
    fn a_gap_between_two_held_entries_is_passed_over() {
        let counted = Counted::default();
        let store = queue(&counted, &[0, 500], 501);
        let (_, entries) = page(&store, KINDS, QUEUE, 0, 10).expect("a page");
        assert_eq!(sequences(&entries), [0, 500]);
        let (_, entries) = page(&store, KINDS, QUEUE, 1, 10).expect("the next");
        assert_eq!(sequences(&entries), [500]);
        let (_, entries) = page(&store, KINDS, QUEUE, 501, 10).expect("past it");
        assert!(entries.is_empty());
        let mut batch = Batch::empty();
        take_out(&store, &mut batch, KINDS, QUEUE, 0).expect("taken");
        let left = places(&store, &batch, KINDS, QUEUE).expect("places");
        assert_eq!((left.first, left.count), (500, 1), "moved past the gap");
    }
}
