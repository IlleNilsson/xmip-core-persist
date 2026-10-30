//! A Send Port's queue read by when each Message in it is due (the owner,
//! 2026-10-19: *a send scan reads only what is due*).
//!
//! **One index, three parts.** `held_due` keeps each entry by its queue,
//! whether it failed, when it is due and its place
//! (`super::schema::searchable`): first the Messages due since they were
//! held — due at the epoch — in their order, then each retry by its due
//! time. What is due now is a range of it up to Xmip Storage's now, what
//! waits for a retry the rest, and what failed a part of its own; a read of
//! one part never reads another, so a scan's cost follows what is due, not
//! what is held.
//!
//! **Read from where the first is.** An entry taken out of an embedded
//! engine's index leaves a mark the engine steps over until it compacts it
//! away — on `RocksDB` about 85 ns each, measured 2026-10-21 — and a queue
//! being sent takes out each entry it sends, ahead of what it holds. So
//! each queue keeps where its first entry due since it was held is, and
//! where its first retry is ([`DuePlaces`]), and a read starts there. The
//! one writer keeps both where the first is ([`settle`]): once a batch is
//! decided, each queue it wrote an entry of finds its first again with one
//! range from where it was, which steps over each mark once.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use codec::cursor::Cursor;

use super::columns::Columns;
use super::commit::{Batch, DUE_PLACES, HELD};
use super::hold::{Held, KINDS};
use super::queue::{self, entry_key, places_key};
use super::record::{
    Form, malformed, read_byte, read_i128, read_u64, write_byte, write_i128, write_u64,
};
use super::row::{Value, nanos};
use super::schema::Database;
use crate::{EncryptedStore, Engine, PersistError};

/// The index a queue is read by when each is due.
const DUE: (&str, &str) = ("held", "held_due");

/// No retry is held: where a queue's first retry is then.
const NO_RETRY: (u64, u64) = (u64::MAX, u64::MAX);

/// Which part of a queue a read by when each is due finds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DuePart {
    /// What is due now: each Message due since it was held, in its order,
    /// then each retry whose due time has come, by its due time.
    Due,
    /// Each retry not yet due, by its due time.
    Waiting,
    /// What failed and waits for an operator, in its order.
    Failed,
}

/// A part of a queue as read by when each is due: what was read, how many
/// keep a retry's due time, the earliest of those not yet due, and Xmip
/// Storage's clock as it read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DueQueue {
    /// In the order the index keeps them.
    pub held: Vec<Held>,
    /// How many Messages of the queue keep a retry's due time, due or not
    /// yet; what failed aside.
    pub retrying: u64,
    /// The earliest due time of a retry not yet due, on Xmip Storage's
    /// clock.
    pub next_due_unix_nanos: Option<i128>,
    /// Xmip Storage's clock as it read, in nanoseconds since the Unix epoch.
    pub read_unix_nanos: i128,
}

impl DueQueue {
    /// How long `held`, one of those read, still waits for its due time as
    /// this read found it, or `None` where it is due ([`Held::due_in`]).
    #[must_use]
    pub fn due_in(&self, held: &Held) -> Option<Duration> {
        held.due_in(self.read_unix_nanos)
    }

    /// How long the earliest retry not yet due still waits as this read
    /// found it, or `None` where none waits.
    #[must_use]
    pub fn next_due_in(&self) -> Option<Duration> {
        let left = self.next_due_unix_nanos? - self.read_unix_nanos;
        Some(Duration::from_nanos(u64::try_from(left).unwrap_or(0)))
    }

    /// Where a read of the same part goes on after `held`, one of those
    /// read: its due time as the index keeps it, and the place after its.
    #[must_use]
    pub fn after(held: &Held) -> (u64, u64) {
        (held.due_at(), held.sequence.saturating_add(1))
    }
}

/// Where a queue's first entries by when they are due are, as the one
/// writer keeps them: no entry due since it was held is before `fresh`, and
/// no retry before `retry`; and how many keep a retry's due time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DuePlaces {
    fresh: u64,
    retry: (u64, u64),
    retrying: u64,
}

impl Default for DuePlaces {
    fn default() -> Self {
        Self {
            fresh: 0,
            retry: (1, 0),
            retrying: 0,
        }
    }
}

/// `part` of `queue` in `store` read at `now`, Xmip Storage's clock: up to
/// `most` from the due time and place `from` on, in the index's order —
/// a range of the index from where the part's first is and a read of each
/// entry it finds — and the earliest retry not yet due, one range of one.
pub(crate) fn read<R: Engine>(
    store: &EncryptedStore<R>,
    (queue, part): (u128, DuePart),
    (from, most): ((u64, u64), u32),
    now: i128,
) -> Result<DueQueue, PersistError> {
    let places = kept(store, None, queue)?;
    let at = nanos(now);
    let most = usize::try_from(most).unwrap_or(usize::MAX);
    let later = (at.saturating_add(1), 0).max(places.retry);
    let found = match part {
        DuePart::Due => {
            let mut found = if from.0 == 0 {
                let start = (0, from.1.max(places.fresh));
                region(store, (queue, false), start, 0, most)?
            } else {
                Vec::new()
            };
            let start = from.max(places.retry);
            if found.len() < most && start.0 <= at {
                let rest = most - found.len();
                found.extend(region(store, (queue, false), start, at, rest)?);
            }
            found
        }
        DuePart::Waiting => region(store, (queue, false), from.max(later), u64::MAX, most)?,
        DuePart::Failed => region(store, (queue, true), from, u64::MAX, most)?,
    };
    let next = region(store, (queue, false), later, u64::MAX, 1)?;
    let mut held = Vec::with_capacity(found.len());
    for (_, place) in found {
        // Written and taken out with its entry, an index entry finds one;
        // one written since is read as it is now.
        if let Some(bytes) = store.get(HELD, &entry_key(queue, place))? {
            held.push(Held::from_bytes(&bytes)?);
        }
    }
    Ok(DueQueue {
        held,
        retrying: places.retrying,
        next_due_unix_nanos: next.first().map(|&(due, _)| i128::from(due)),
        read_unix_nanos: now,
    })
}

/// Every queue `batch` writes a held Message of, its [`DuePlaces`] kept as
/// the batch leaves it, in the batch: how many keep a retry's due time
/// counted on, and the first of each part found again from where it was —
/// the first the batch did not write of what the engine holds, or one the
/// batch wrote, whichever is first. Called once a batch is decided, before
/// its index entries are laid out.
pub(crate) fn settle<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
) -> Result<(), PersistError> {
    let mut queues: BTreeMap<u128, Vec<Written>> = BTreeMap::new();
    for (key, value) in batch.written_of(HELD) {
        let (Some(queue), Some(place)) = (key.get(..16), key.get(16..24)) else {
            continue;
        };
        let queue = u128::from_be_bytes(queue.try_into().unwrap_or_default());
        let place = u64::from_be_bytes(place.try_into().unwrap_or_default());
        let value = value.map(<[u8]>::to_vec);
        queues.entry(queue).or_default().push((place, value));
    }
    for (queue, written) in queues {
        let before = kept(store, Some(batch), queue)?;
        let mut places = before;
        let mut touched = HashSet::new();
        let (mut fresh, mut retry) = (None, None);
        for (place, value) in written {
            let old = store.get(HELD, &entry_key(queue, place))?;
            let old = old.map(|bytes| Held::from_bytes(&bytes)).transpose()?;
            let new = value.map(|bytes| Held::from_bytes(&bytes)).transpose()?;
            places.retrying =
                (places.retrying + retries(new.as_ref())).saturating_sub(retries(old.as_ref()));
            touched.insert(place);
            match new.filter(|new| !new.failed).map(|new| new.due_at()) {
                Some(0) => fresh = Some(fresh.map_or(place, |first: u64| first.min(place))),
                Some(due) => {
                    let at = (due, place);
                    retry = Some(retry.map_or(at, |first: (u64, u64)| first.min(at)));
                }
                None => {}
            }
        }
        let most = touched.len() + 1;
        let kept_fresh = untouched(store, queue, ((0, places.fresh), 0), most, &touched)?;
        places.fresh = match kept_fresh
            .map(|(_, place)| place)
            .into_iter()
            .chain(fresh)
            .min()
        {
            Some(first) => first,
            None => queue::places(store, batch, KINDS, queue)?.next,
        };
        let kept_retry = untouched(store, queue, (places.retry, u64::MAX), most, &touched)?;
        places.retry = kept_retry
            .into_iter()
            .chain(retry)
            .min()
            .unwrap_or(NO_RETRY);
        if places != before {
            batch.put(DUE_PLACES, places_key(queue), Some(places.bytes()));
        }
    }
    Ok(())
}

/// A held Message's place a batch wrote, and what it left there.
type Written = (u64, Option<Vec<u8>>);

/// One where it keeps a retry's due time and did not fail.
fn retries(held: Option<&Held>) -> u64 {
    u64::from(held.is_some_and(|held| held.due_unix_nanos.is_some() && !held.failed))
}

/// The first entry of `queue` not failed, from the due time and place
/// `from` on and due at `to` at the latest, that the engine holds and is
/// not `touched`: one range of up to `most`.
fn untouched<R: Engine>(
    store: &EncryptedStore<R>,
    queue: u128,
    (from, to): ((u64, u64), u64),
    most: usize,
    touched: &HashSet<u64>,
) -> Result<Option<(u64, u64)>, PersistError> {
    let found = region(store, (queue, false), from, to, most)?;
    Ok(found
        .into_iter()
        .find(|(_, place)| !touched.contains(place)))
}

/// The due times and places of up to `most` entries of `queue`, failed or
/// not, from the due time and place `from` on, due at `to` at the latest:
/// one range of the index.
fn region<R: Engine>(
    store: &EncryptedStore<R>,
    (queue, failed): (u128, bool),
    from: (u64, u64),
    to: u64,
    most: usize,
) -> Result<Vec<(u64, u64)>, PersistError> {
    let equal = [Value::Id(queue), Value::Flag(failed)];
    let columns = Columns::of(Database::Runtime);
    let found = columns.find_ordered(store, DUE, &equal, (&[from.0, from.1], to), most)?;
    found
        .into_iter()
        .map(|(values, _)| match values[..] {
            [due, place] => Ok((due, place)),
            _ => Err(malformed(
                "a held_due entry that is not a due time and a place",
            )),
        })
        .collect()
}

/// `queue`'s [`DuePlaces`] as `batch` leaves them, or as `store` keeps them.
fn kept<R: Engine>(
    store: &EncryptedStore<R>,
    batch: Option<&Batch<'_>>,
    queue: u128,
) -> Result<DuePlaces, PersistError> {
    let key = places_key(queue);
    let bytes = match batch {
        Some(batch) => batch.read(store, DUE_PLACES, &key)?,
        None => store.get(DUE_PLACES, &key)?,
    };
    bytes.map_or(Ok(DuePlaces::default()), |bytes| {
        DuePlaces::from_bytes(&bytes)
    })
}

impl Form for DuePlaces {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.fresh);
        write_u64(out, self.retry.0);
        write_u64(out, self.retry.1);
        write_u64(out, self.retrying);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            fresh: read_u64(cursor)?,
            retry: (read_u64(cursor)?, read_u64(cursor)?),
            retrying: read_u64(cursor)?,
        })
    }
}

impl Form for DuePart {
    fn write(&self, out: &mut Vec<u8>) {
        write_byte(
            out,
            match self {
                Self::Due => 0,
                Self::Waiting => 1,
                Self::Failed => 2,
            },
        );
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        match read_byte(cursor)? {
            0 => Ok(Self::Due),
            1 => Ok(Self::Waiting),
            2 => Ok(Self::Failed),
            other => Err(malformed(format!("no part of a queue is numbered {other}"))),
        }
    }
}

impl Form for DueQueue {
    fn write(&self, out: &mut Vec<u8>) {
        self.held.write(out);
        write_u64(out, self.retrying);
        write_byte(out, u8::from(self.next_due_unix_nanos.is_some()));
        write_i128(out, self.next_due_unix_nanos.unwrap_or_default());
        write_i128(out, self.read_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            held: Vec::read(cursor)?,
            retrying: read_u64(cursor)?,
            next_due_unix_nanos: {
                let waits = read_byte(cursor)? != 0;
                let due = read_i128(cursor)?;
                waits.then_some(due)
            },
            read_unix_nanos: read_i128(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{Counted, Memory};
    use crate::storage::{
        AuditEntry, AuditFacts, Embedded, Failure, HandOn, Hold, JourneyFacts, JourneyRecord,
        MessageFacts, MessageRecord, Publication, Wait, XmipStorage,
    };
    use std::sync::{Arc, Mutex};
    use std::time::Instant;
    use xcore::{AuditId, Clock, JourneyId, MessageId};

    const QUEUE: u128 = 7;
    const LEASE: Duration = Duration::from_secs(30);
    /// An hour, in nanoseconds: a retry not due while a test runs.
    const HOUR: u64 = 3_600_000_000_000;

    /// A clock a test moves by hand.
    #[derive(Default)]
    struct Pinned(Mutex<i128>);

    impl Pinned {
        fn pass(&self, nanos: i128) {
            *self.0.lock().expect("clock") += nanos;
        }
    }

    impl Clock for Pinned {
        fn unix_timestamp_nanos(&self) -> i128 {
            *self.0.lock().expect("clock")
        }
    }

    type Node = Embedded<Box<Counted>, Memory>;

    fn node() -> (Node, Arc<Pinned>) {
        let keys = secret::Held::new(secret::fixture::Memory::default());
        let kek = secret::KekName::new("storage").expect("name");
        let clock = Arc::new(Pinned::default());
        clock.pass(1_000);
        let node = Embedded::over(
            EncryptedStore::open(Box::default(), &keys, &kek).expect("runtime"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("administration"),
            EncryptedStore::open(Memory::default(), &keys, &kek).expect("audit"),
            Arc::clone(&clock) as Arc<dyn Clock>,
        )
        .expect("node");
        (node, clock)
    }

    /// The Messages of the Journeys `ids` published in one write, each held
    /// in [`QUEUE`].
    fn published(node: &Node, ids: std::ops::Range<u128>) {
        let journey = |id| JourneyRecord {
            journey: JourneyId::new(id),
            body: b"to send".to_vec(),
            facts: JourneyFacts::default(),
        };
        let hold = |id| Hold {
            queue: QUEUE,
            journey: JourneyId::new(id),
            body: Vec::new(),
        };
        node.publish(&Publication {
            message: MessageRecord {
                message: MessageId::new(ids.start),
                body: Vec::new(),
                facts: MessageFacts::default(),
            },
            journeys: ids.clone().map(journey).collect(),
            held: ids.clone().map(hold).collect(),
            dead: None,
            audit: AuditEntry {
                id: AuditId::new(ids.start),
                body: Vec::new(),
                audited: None,
                facts: AuditFacts::default(),
            },
            claims: Vec::new(),
            lease_nanos: 0,
        })
        .expect("published");
    }

    /// The Message of the Journey `id` claimed and handed on as `how` leaves
    /// it.
    fn handed(node: &Node, id: u128, how: impl FnOnce(&mut HandOn)) {
        let holder = configure::fixture::test_cluster().node_scope(0);
        let claim = node.claim(JourneyId::new(id), &holder, id, LEASE);
        let mut hand_on = HandOn {
            claim: claim.expect("asked").expect("claimed"),
            result: JourneyRecord {
                journey: JourneyId::new(id),
                body: b"tried".to_vec(),
                facts: JourneyFacts::default(),
            },
            messages: Vec::new(),
            next: Vec::new(),
            leaves: Vec::new(),
            queued: Vec::new(),
            requeued: Vec::new(),
            failed: None,
            cleared: Vec::new(),
            waits: None,
            kept_for_nanos: None,
        };
        how(&mut hand_on);
        assert!(node.hand_on(&hand_on).expect("handed on"));
    }

    fn waits(after_nanos: u64) -> impl FnOnce(&mut HandOn) {
        move |hand_on| {
            hand_on.waits = Some(Wait {
                queue: QUEUE,
                after_nanos,
            });
        }
    }

    fn fails(hand_on: &mut HandOn) {
        hand_on.failed = Some(Failure {
            queue: QUEUE,
            reason: "refused".to_string(),
        });
    }

    fn sent(hand_on: &mut HandOn) {
        hand_on.leaves = vec![QUEUE];
    }

    /// The Journeys of the Messages `part` of [`QUEUE`] holds, read whole two
    /// at a time, in the order read.
    fn messages(node: &Node, part: DuePart) -> Vec<u128> {
        let (mut from, mut found) = ((0, 0), Vec::new());
        loop {
            let page = node.read_due(QUEUE, part, from, 2).expect("read");
            found.extend(page.held.iter().map(|held| held.hold.journey.value()));
            match page.held.last() {
                Some(last) if page.held.len() == 2 => from = DueQueue::after(last),
                _ => return found,
            }
        }
    }

    fn places(node: &Node) -> DuePlaces {
        kept(node.runtime(), None, QUEUE).expect("its places")
    }

    #[test]
    fn each_part_holds_what_it_says_in_its_order_and_a_retry_falls_due_on_its_time() {
        let (node, clock) = node();
        published(&node, 1..7);
        handed(&node, 2, waits(HOUR));
        handed(&node, 3, waits(10));
        handed(&node, 4, fails);
        handed(&node, 1, sent);
        assert_eq!(messages(&node, DuePart::Due), [5, 6], "due since held");
        assert_eq!(messages(&node, DuePart::Waiting), [3, 2], "by due time");
        assert_eq!(messages(&node, DuePart::Failed), [4]);
        let read = node
            .read_due(QUEUE, DuePart::Due, (0, 0), 64)
            .expect("read");
        assert_eq!(read.retrying, 2);
        assert_eq!(read.next_due_in(), Some(Duration::from_nanos(10)));
        clock.pass(10);
        assert_eq!(messages(&node, DuePart::Due), [5, 6, 3], "then the retry");
        let read = node
            .read_due(QUEUE, DuePart::Due, (0, 0), 64)
            .expect("read");
        assert_eq!(read.due_in(&read.held[2]), None, "due");
        assert_eq!(read.next_due_in(), Some(Duration::from_nanos(HOUR - 10)));
        handed(&node, 3, sent);
        handed(&node, 2, fails);
        let read = node
            .read_due(QUEUE, DuePart::Waiting, (0, 0), 64)
            .expect("read");
        assert_eq!(
            (read.held.len(), read.retrying),
            (0, 0),
            "failed waits for none"
        );
        assert_eq!(read.next_due_unix_nanos, None);
        assert_eq!(messages(&node, DuePart::Failed), [2, 4], "in their order");
    }

    #[test]
    fn the_writer_keeps_where_each_part_s_first_is() {
        let (node, _) = node();
        published(&node, 1..9);
        assert_eq!(places(&node).fresh, 0);
        handed(&node, 1, sent);
        handed(&node, 2, waits(HOUR));
        assert_eq!(
            places(&node).fresh,
            2,
            "the place of the Message of the Journey 3"
        );
        handed(&node, 5, waits(HOUR / 2));
        assert_eq!(places(&node).retry.1, 4, "the earlier retry first");
        handed(&node, 4, sent);
        handed(&node, 3, sent);
        assert_eq!(
            places(&node).fresh,
            5,
            "past the one waiting, to the Message of the Journey 6"
        );
        handed(&node, 5, sent);
        assert_eq!(
            places(&node).retry.1,
            1,
            "the Message of the Journey 2 next"
        );
        for id in [2, 6, 7, 8] {
            handed(&node, id, sent);
        }
        let emptied = places(&node);
        assert_eq!((emptied.fresh, emptied.retry), (8, NO_RETRY), "none held");
        published(&node, 9..10);
        assert_eq!(places(&node).fresh, 8, "the next place taken");
        assert_eq!(messages(&node, DuePart::Due), [9]);
    }

    /// Many held, few due: [`QUEUE`] holds `waiting` retries not yet due,
    /// `failed` failed, and three due.
    fn crowded(waiting: u128, failed: u128) -> Node {
        let (node, _) = node();
        published(&node, 1..waiting + failed + 4);
        for id in 1..=waiting {
            handed(&node, id, waits(HOUR));
        }
        for id in waiting + 1..=waiting + failed {
            handed(&node, id, fails);
        }
        node
    }

    #[test]
    fn a_read_of_what_is_due_touches_only_what_is_due_however_many_are_held() {
        let node = crowded(10_000, 1_000);
        let counted = node.runtime().engine();
        counted.touched();
        let read = node
            .read_due(QUEUE, DuePart::Due, (0, 0), 64)
            .expect("read");
        assert_eq!(read.held.len(), 3);
        assert_eq!(read.retrying, 10_000);
        assert_eq!(
            counted.touched(),
            (4, 2, 4),
            "its places and the three due; a range of those due since held and one for \
             the next due, finding the three and the next; no retry due, so none read"
        );
        let (mut from, mut reads) = (0, 0);
        loop {
            let page = node.read_held(QUEUE, from, 64).expect("a page");
            reads += counted.touched().0;
            match page.held.last() {
                Some(last) => from = last.sequence + 1,
                None => break,
            }
        }
        eprintln!("what is due: 4 keys read; the whole queue, as a scan read it: {reads}");
        assert!(reads > 11_000, "{reads}");
    }

    #[test]
    fn a_read_of_what_is_due_among_many_held_takes_under_a_millisecond() {
        let node = crowded(10_000, 1_000);
        let fastest = (0..5)
            .map(|_| {
                let started = Instant::now();
                let read = node
                    .read_due(QUEUE, DuePart::Due, (0, 0), 64)
                    .expect("read");
                assert_eq!(read.held.len(), 3);
                started.elapsed()
            })
            .min()
            .unwrap_or_default();
        eprintln!("three due of 11,003 held: read in {fastest:?}");
        assert!(fastest < Duration::from_millis(1), "{fastest:?}");
    }
}
