//! A queue whose entries lie a million places apart, written as the one
//! writer writes them, and its page and the writer's removal before it:
//! what `queue`'s tests read, and what the module's timing program
//! (`tests/measured`) times and holds to the millisecond rule. A test's,
//! never a node's.

use codec::form::Form;
use secret::KekName;
use xcore::JourneyId;

use super::super::columns::Columns;
use super::super::commit::Batch;
use super::super::hold::{self, Hold, KINDS};
use super::super::schema::Database;
use super::{Places, entry_key, page, places, places_key, take_out};
use crate::fixture::Counted;
use crate::{EncryptedStore, IndexEntry, RecordChange};

/// The queue every entry here is held in.
pub const QUEUE: u128 = 7;
/// A gap of a million places.
pub const GAP: u64 = 1_000_000;

/// What the writer applies: the changes and their index entries.
pub type Laid = (Vec<RecordChange<'static>>, Vec<IndexEntry>);

/// `changes` written as the one writer writes them: each entry with its
/// index entries.
fn written(store: &EncryptedStore<&Counted>, changes: Vec<RecordChange<'_>>) {
    let columns = Columns::of(Database::Runtime);
    let (changes, entries) = columns.written(store, changes, 0).expect("laid out");
    store.apply_indexed(&changes, &entries).expect("written");
}

/// The Message held at `sequence`, its Journey numbered one past it.
pub fn held(sequence: u64) -> Vec<u8> {
    hold::Held {
        sequence,
        hold: Hold {
            queue: QUEUE,
            journey: JourneyId::new(u128::from(sequence) + 1),
            body: Vec::new(),
        },
        held_unix_nanos: 0,
        failed: false,
        due_unix_nanos: None,
    }
    .bytes()
}

/// A queue whose entries at `kept` it still holds, the rest of the places
/// before `next` taken out.
pub fn queue<'c>(counted: &'c Counted, kept: &[u64], next: u64) -> EncryptedStore<&'c Counted> {
    let keys = secret::Held::new(secret::fixture::Memory::default());
    let kek = KekName::new("runtime").expect("a name");
    let store = EncryptedStore::open(counted, &keys, &kek).expect("open");
    let places = Places {
        first: kept.first().copied().unwrap_or(next),
        next,
        count: kept.len() as u64,
    };
    let entries = kept
        .iter()
        .map(|&at| (KINDS.entry, entry_key(QUEUE, at).to_vec(), Some(held(at))));
    let places = (
        KINDS.places,
        places_key(QUEUE).to_vec(),
        Some(places.bytes()),
    );
    written(&store, entries.chain([places]).collect());
    counted.read();
    store
}

/// The entry at `sequence` taken out as the one writer decides it, and
/// what its batch writes laid out as the writer lays it out: the writer's
/// whole part of it.
pub fn taken_out(store: &EncryptedStore<&Counted>, sequence: u64) -> Laid {
    let mut batch = Batch::empty(0);
    take_out(store, &mut batch, KINDS, QUEUE, sequence).expect("taken");
    let left = places(store, &batch, KINDS, QUEUE).expect("places");
    let changes = vec![
        (KINDS.entry, entry_key(QUEUE, sequence).to_vec(), None),
        (KINDS.places, places_key(QUEUE).to_vec(), Some(left.bytes())),
    ];
    let columns = Columns::of(Database::Runtime);
    let laid = columns.written(store, changes, 0).expect("laid out");
    assert_eq!(laid.1.len(), 3, "its three index entries taken out with it");
    laid
}

/// A queue whose two entries lie a million places apart, handed to
/// `measure` as two calls it may make as often as it likes: a page across
/// the gap, and the writer's removal of the entry before it decided. What
/// it answers; the timing program times each (`tests/measured`), so how a
/// time is held is written there once.
///
/// # Panics
///
/// Where the page or the removal fails.
pub fn across_a_gap<T>(measure: impl FnOnce(&mut dyn FnMut(), &mut dyn FnMut()) -> T) -> T {
    let counted = Counted::default();
    let store = queue(&counted, &[0, GAP], GAP + 1);
    let mut paged = || {
        let (_, entries) = page(&store, KINDS, QUEUE, 0, 64).expect("a page");
        assert_eq!(entries.len(), 2);
    };
    let mut taken = || drop(taken_out(&store, 0));
    measure(&mut paged, &mut taken)
}
