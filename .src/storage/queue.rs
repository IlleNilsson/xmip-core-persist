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
//! that a read passes over.
//!
//! **Read in its order by an index** (open problem 40; the owner,
//! 2026-10-19). An entry is kept under a keyed hash
//! ([`crate::EncryptedStore`]), which sorts nowhere, so each kind of queue
//! is indexed on its queue and place (`held_place`, `dead_message_place`,
//! `failed_message_place`), written with the entry like every other index
//! (`super::columns`) and taken out with it. A page is one ordered range of
//! that index from its place, and then a read of each entry it finds; a
//! gap is never read. The queue's first place is found when a read asks
//! for it, by the same range; the one writer never walks a gap to move it:
//! what it keeps as the first is a place no entry is held before, moved to
//! the next once the queue is emptied. An embedded store made before the
//! index lacks it, and before the first release nothing migrates it.

use codec::cursor::Cursor;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::columns::Columns;
use super::commit::Batch;
use super::row::Value;
use super::schema::Database;
use crate::{EncryptedStore, Engine, IndexScan, PersistError};

/// A queue with a gap of a million places, and what it costs: for tests.
#[cfg(any(test, feature = "test-support"))]
pub mod gap;

/// Where one kind of queue keeps what it keeps: its entries by queue and
/// place, its index by queue and what an entry is about, its places by
/// queue, and the table and index its entries are found in order by.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Kinds {
    pub(crate) entry: &'static str,
    pub(crate) by: &'static str,
    pub(crate) places: &'static str,
    pub(crate) table: &'static str,
    pub(crate) index: &'static str,
}

/// A queue's places, as the runtime database keeps them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Places {
    /// No entry is held before it: the place of the oldest it still holds
    /// as a read finds it ([`page`]); `next` where it holds none.
    pub(crate) first: u64,
    /// The place the next entry takes.
    pub(crate) next: u64,
    /// How many it holds.
    pub(crate) count: u64,
}

impl Places {
    /// How many entries were ever taken out of it: every one placed takes
    /// the next place and counts one, and every one taken out counts one
    /// less, so this only grows, and only by a take-out — its head moving
    /// on, where it is a sequence (`super::watch`).
    pub(crate) const fn taken(&self) -> u64 {
        self.next.saturating_sub(self.count)
    }
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
        .map_or(Ok(Places::default()), |bytes| {
            Ok(Places::from_bytes(&bytes)?)
        })
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
        // Empty: its first is its next. Otherwise it stays where it is, a
        // place none is held before, and a read finds the first.
        places.first = places.next;
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

/// `queue` as `store` keeps it: its places, its first as this read finds
/// it, and up to `most` of its entries from the place `from` on, oldest
/// first — one ordered range of its index from that place, and a read of
/// each entry the range finds. A place taken out is in no index, so a gap
/// is never read. The first is the first the range finds where the read
/// starts at the kept first; past it, a range of one from there finds it.
pub(crate) fn page<R: Engine>(
    store: &EncryptedStore<R>,
    kinds: Kinds,
    queue: u128,
    from: u64,
    most: u32,
) -> Result<(Places, Vec<Vec<u8>>), PersistError> {
    // Read in one consistent view: no write lands between the index and
    // the entries it finds, so the page never comes back short of what is
    // held (corrected 2026-10-27; a sequence's head read empty under load).
    let view = store.viewed()?;
    let mut places = view
        .get(kinds.places, &places_key(queue))?
        .map_or(Ok(Places::default()), |bytes| Places::from_bytes(&bytes))?;
    if places.count == 0 {
        return Ok((places, Vec::new()));
    }
    let at = from.max(places.first);
    let most = usize::try_from(most).unwrap_or(usize::MAX);
    let found = if most == 0 {
        Vec::new()
    } else {
        placed_from(&view, kinds, queue, at, most)?
    };
    let first = match found.first() {
        Some(&(sequence, _)) if at == places.first => Some(sequence),
        _ => placed_from(&view, kinds, queue, places.first, 1)?
            .first()
            .map(|&(sequence, _)| sequence),
    };
    places.first = first.unwrap_or(places.next);
    let mut entries = Vec::with_capacity(found.len());
    for (sequence, _) in found {
        // Written and taken out with its entry, an index entry finds one;
        // one written before a read of it ended is read as it is now.
        if let Some(bytes) = view.get(kinds.entry, &entry_key(queue, sequence))? {
            entries.push(bytes);
        }
    }
    Ok((places, entries))
}

/// The places held in `queue` from `from` on, up to `most`, in order: one
/// range of its index.
fn placed_from<S: IndexScan + ?Sized>(
    store: &S,
    kinds: Kinds,
    queue: u128,
    from: u64,
    most: usize,
) -> Result<Vec<(u64, u128)>, PersistError> {
    let at = (kinds.table, kinds.index);
    Columns::of(Database::Runtime).find_from(store, at, &[Value::Id(queue)], from, most)
}

impl Form for Places {
    fn write(&self, out: &mut Vec<u8>) {
        out.u64_be(self.first);
        out.u64_be(self.next);
        out.u64_be(self.count);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            first: cursor.u64_be()?,
            next: cursor.u64_be()?,
            count: cursor.u64_be()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::hold::{self, KINDS};
    use super::gap::{GAP, QUEUE, queue, taken_out};
    use super::*;
    use crate::fixture::Counted;

    fn sequences(entries: &[Vec<u8>]) -> Vec<u64> {
        let held = |bytes: &Vec<u8>| hold::Held::from_bytes(bytes).expect("held").sequence;
        entries.iter().map(held).collect()
    }

    #[test]
    fn a_page_of_a_queue_holding_its_oldest_alone_reads_what_it_returns() {
        let counted = Counted::default();
        let store = queue(&counted, &[0], GAP + 1);
        let (places, entries) = page(&store, KINDS, QUEUE, 0, 10).expect("a page");
        assert_eq!((places.first, sequences(&entries)), (0, vec![0]));
        assert_eq!(
            counted.read(),
            (2, 1),
            "its places, its one entry; one range"
        );
        let (places, entries) = page(&store, KINDS, QUEUE, 1, 10).expect("the next");
        assert_eq!((places.first, entries.len()), (0, 0));
        assert_eq!(
            counted.read(),
            (1, 2),
            "its places; a range, and one for its first"
        );
    }

    #[test]
    fn its_oldest_taken_out_empties_a_queue_without_reading_its_gap() {
        let counted = Counted::default();
        let store = queue(&counted, &[0], GAP + 1);
        let mut batch = Batch::empty();
        let taken = take_out(&store, &mut batch, KINDS, QUEUE, 0).expect("taken");
        assert_eq!(taken.map(|bytes| sequences(&[bytes])), Some(vec![0]));
        assert_eq!(counted.read(), (2, 0), "the entry and its places");
        let left = places(&store, &batch, KINDS, QUEUE).expect("places");
        let empty = Places {
            first: GAP + 1,
            next: GAP + 1,
            count: 0,
        };
        assert_eq!(left, empty);
    }

    #[test]
    fn a_page_across_a_gap_between_two_held_entries_is_one_ordered_range() {
        let counted = Counted::default();
        let store = queue(&counted, &[0, GAP], GAP + 1);
        let (places, entries) = page(&store, KINDS, QUEUE, 0, 10).expect("a page");
        assert_eq!((places.first, sequences(&entries)), (0, vec![0, GAP]));
        assert_eq!(
            counted.read(),
            (3, 1),
            "its places and two entries; one range"
        );
        let (_, entries) = page(&store, KINDS, QUEUE, 1, 10).expect("the next");
        assert_eq!(sequences(&entries), [GAP]);
        assert_eq!(
            counted.read(),
            (2, 2),
            "one entry; a range, and one for its first"
        );
        let (_, entries) = page(&store, KINDS, QUEUE, GAP + 1, 10).expect("past it");
        assert_eq!(entries.len(), 0);
        counted.read();
    }

    #[test]
    fn the_writer_walks_no_gap_and_a_read_finds_the_first_past_it() {
        let counted = Counted::default();
        let store = queue(&counted, &[0, GAP], GAP + 1);
        let (changes, entries) = taken_out(&store, 0);
        assert_eq!(
            counted.read(),
            (3, 0),
            "the entry, its places, and the entry again for its index entries; no range"
        );
        store.apply_indexed(&changes, &entries).expect("written");
        let left = places(&store, &Batch::empty(), KINDS, QUEUE).expect("places");
        assert_eq!((left.first, left.count), (0, 1), "kept where it was");
        counted.read();
        let (places, entries) = page(&store, KINDS, QUEUE, 0, 10).expect("a page");
        assert_eq!((places.first, sequences(&entries)), (GAP, vec![GAP]));
        assert_eq!(
            counted.read(),
            (2, 1),
            "its places and one entry; one range"
        );
        let (changes, entries) = taken_out(&store, GAP);
        store.apply_indexed(&changes, &entries).expect("written");
        let (places, _) = page(&store, KINDS, QUEUE, 0, 10).expect("emptied");
        assert_eq!((places.first, places.count), (GAP + 1, 0));
    }
}
