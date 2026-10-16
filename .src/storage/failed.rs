//! The Journeys that failed waiting in a Send Port's queue, indexed in the
//! Ledger beside it (`runtime-model.md` section 10; open problem 34: *the
//! count, and an index of the failed Journeys of each queue, kept by Xmip
//! Storage in the Ledger and read from it*).
//!
//! **Written with the failure.** A hand-on that writes its Journey Failed
//! and keeps it in its queue names that queue and why
//! ([`super::HandOn::failed`]): Xmip Storage enters the Journey at the end
//! of the queue's index and marks its place in the queue failed
//! ([`super::Held::failed`]), in that one write. An operator's Retry or
//! Dismiss names the queues whose index it leaves
//! ([`super::HandOn::cleared`]), in its own one write. So the count and the
//! index never disagree with the Journeys, whichever node failed them and
//! however often a node restarts.
//!
//! **Numbered in the order they failed**, from zero, as every queue is
//! ([`super::queue`]): how many wait is one read of the index's places, and
//! the oldest are its first entries, read a page at a time — by a node
//! keeping a bounded window of them, by a surface paging through them —
//! never by reading the Send Port's whole queue.

use codec::cursor::Cursor;
use xcore::JourneyId;

use super::commit::Batch;
use super::hold;
use super::queue::{self, Kinds, Placed};
use super::record::{
    Form, read_i128, read_text, read_u64, read_u128, write_i128, write_text, write_u64, write_u128,
};
use crate::{EncryptedStore, Engine, PersistError};

/// Where a hand-on leaves its Journey Failed: the queue it waits in for an
/// operator, and why it failed, in words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub queue: u128,
    pub reason: String,
}

/// A Journey that failed, at its place in its queue's index of them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failed {
    /// Its place in the index, given as it failed: oldest lowest.
    pub sequence: u64,
    pub journey: JourneyId,
    /// Its place in its Send Port's queue.
    pub place: u64,
    /// Why it failed, in words.
    pub reason: String,
    /// When Xmip Storage wrote it Failed, in nanoseconds since the Unix
    /// epoch: Xmip Storage's to set.
    pub failed_unix_nanos: i128,
}

/// A queue's index of failed Journeys as read: where it starts and ends,
/// how many it holds, and the ones asked for, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FailedQueue {
    /// The place of the oldest it still holds; `next` where it holds none.
    pub first: u64,
    /// The place the next that fails takes.
    pub next: u64,
    /// How many failed Journeys wait in the queue.
    pub count: u64,
    /// The ones read, oldest first.
    pub failed: Vec<Failed>,
}

/// Where a queue's index of failed Journeys keeps what it keeps: its
/// entries, each one's place by its Journey, and its places.
pub(crate) const KINDS: Kinds = Kinds {
    entry: "failed-journey",
    by: "failed-journey-by-journey",
    places: "failed-journey-places",
};

/// `journey`, left Failed by its hand-on at `now` as `failure` says,
/// entered at the end of its queue's index and its place there marked
/// failed, in `batch`. Nothing where it holds no place in that queue; once
/// where it is entered already.
pub(crate) fn enter<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    journey: JourneyId,
    failure: &Failure,
    now: i128,
) -> Result<(), PersistError> {
    let about = (failure.queue, journey.value());
    let Placed::At(place) = queue::placed(store, batch, hold::KINDS, about)? else {
        return Ok(());
    };
    hold::mark(store, batch, failure.queue, place, true)?;
    queue::place(store, batch, KINDS, about, |sequence| {
        Failed {
            sequence,
            journey,
            place,
            reason: failure.reason.clone(),
            failed_unix_nanos: now,
        }
        .bytes()
    })
    .map(drop)
}

/// `journey` out of `queue`'s index of failed Journeys, and its place in
/// the queue, where it still holds one, no longer marked failed, in
/// `batch`: an operator acted on it. Nothing where it is not in the index,
/// so an act asked again does nothing twice.
pub(crate) fn clear<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    queue: u128,
    journey: JourneyId,
) -> Result<(), PersistError> {
    let about = (queue, journey.value());
    if let Placed::At(sequence) = queue::placed(store, batch, KINDS, about)? {
        queue::take_out(store, batch, KINDS, queue, sequence)?;
        queue::forget(batch, KINDS, about);
    }
    if let Placed::At(place) = queue::placed(store, batch, hold::KINDS, about)? {
        hold::mark(store, batch, queue, place, false)?;
    }
    Ok(())
}

/// `queue`'s index of failed Journeys as `store` keeps it: its places, and
/// up to `most` of them from the place `from` on, oldest first.
pub(crate) fn read<R: Engine>(
    store: &EncryptedStore<R>,
    queue: u128,
    from: u64,
    most: u32,
) -> Result<FailedQueue, PersistError> {
    let (places, entries) = queue::page(store, KINDS, queue, from, most)?;
    Ok(FailedQueue {
        first: places.first,
        next: places.next,
        count: places.count,
        failed: entries
            .iter()
            .map(|bytes| Failed::from_bytes(bytes))
            .collect::<Result<_, _>>()?,
    })
}

impl Form for Failure {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.queue);
        write_text(out, &self.reason);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            queue: read_u128(cursor)?,
            reason: read_text(cursor)?,
        })
    }
}

impl Form for Failed {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.sequence);
        write_u128(out, self.journey.value());
        write_u64(out, self.place);
        write_text(out, &self.reason);
        write_i128(out, self.failed_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            sequence: read_u64(cursor)?,
            journey: JourneyId::new(read_u128(cursor)?),
            place: read_u64(cursor)?,
            reason: read_text(cursor)?,
            failed_unix_nanos: read_i128(cursor)?,
        })
    }
}

impl Form for FailedQueue {
    fn write(&self, out: &mut Vec<u8>) {
        write_u64(out, self.first);
        write_u64(out, self.next);
        write_u64(out, self.count);
        self.failed.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            first: read_u64(cursor)?,
            next: read_u64(cursor)?,
            count: read_u64(cursor)?,
            failed: Vec::read(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::Memory;
    use crate::storage::{
        AuditEntry, AuditFacts, Claim, Embedded, HandOn, Hold, JourneyFacts, JourneyRecord,
        MessageFacts, MessageRecord, Publication, XmipStorage,
    };
    use std::time::Duration;
    use xcore::{AuditId, MessageId};

    const QUEUE: u128 = 7;

    /// Engines and the key store their data keys are wrapped in, both
    /// outliving a node, so a node opened again over them reads what the
    /// first wrote.
    type Kept = ([Memory; 3], secret::Held<secret::fixture::Memory>);

    fn kept() -> &'static Kept {
        let keys = secret::Held::new(secret::fixture::Memory::default());
        Box::leak(Box::new((Default::default(), keys)))
    }

    fn opened((engines, keys): &'static Kept) -> Embedded<&'static Memory, &'static Memory> {
        let kek = secret::KekName::new("storage").expect("a name");
        let [runtime, administration, audit] = engines;
        Embedded::open(runtime, administration, audit, keys, &kek).expect("open")
    }

    /// `count` Journeys published into [`QUEUE`], numbered from one.
    fn published(node: &dyn XmipStorage, count: u128) {
        for id in 1..=count {
            let journey = JourneyId::new(id);
            node.publish(&Publication {
                message: MessageRecord {
                    message: MessageId::new(id),
                    body: Vec::new(),
                    facts: MessageFacts::default(),
                },
                journeys: vec![record(id, b"waiting")],
                held: vec![Hold {
                    queue: QUEUE,
                    journey,
                    body: Vec::new(),
                }],
                dead: None,
                audit: AuditEntry {
                    id: AuditId::new(id),
                    body: Vec::new(),
                    audited: None,
                    facts: AuditFacts::default(),
                },
                claims: Vec::new(),
                lease_nanos: 0,
            })
            .expect("published");
        }
    }

    fn record(id: u128, body: &[u8]) -> JourneyRecord {
        JourneyRecord {
            journey: JourneyId::new(id),
            body: body.to_vec(),
            facts: JourneyFacts::default(),
        }
    }

    /// The Journey `id` claimed and handed on as `step` makes the hand-on.
    fn handed(node: &dyn XmipStorage, id: u128, step: impl FnOnce(Claim) -> HandOn) {
        let holder = configure::fixture::test_cluster().node_scope(0);
        let lease = Duration::from_secs(30);
        let claim = node.claim(JourneyId::new(id), &holder, id ^ 0x5eed, lease);
        let claim = claim.expect("asked").expect("claimed");
        assert!(node.hand_on(&step(claim)).expect("handed on"));
    }

    fn hand_on(claim: Claim, body: &[u8]) -> HandOn {
        HandOn {
            result: record(claim.journey.value(), body),
            claim,
            messages: Vec::new(),
            next: Vec::new(),
            leaves: Vec::new(),
            queued: Vec::new(),
            requeued: Vec::new(),
            failed: None,
            cleared: Vec::new(),
            waits: None,
            kept_for_nanos: None,
        }
    }

    fn failed(node: &dyn XmipStorage, id: u128) {
        handed(node, id, |claim| HandOn {
            failed: Some(Failure {
                queue: QUEUE,
                reason: format!("the far end refused {id}"),
            }),
            ..hand_on(claim, b"failed")
        });
    }

    fn journeys(read: &FailedQueue) -> Vec<u128> {
        read.failed.iter().map(|f| f.journey.value()).collect()
    }

    fn marked(node: &dyn XmipStorage) -> Vec<u128> {
        let held = node.read_held(QUEUE, 0, 64).expect("read").held;
        let failed = held.iter().filter(|held| held.failed);
        failed.map(|held| held.hold.journey.value()).collect()
    }

    #[test]
    fn a_failure_is_indexed_and_counted_in_its_hand_on_and_an_act_takes_it_out() {
        let engines = kept();
        let node = opened(engines);
        published(&node, 4);
        for id in [3, 1, 4] {
            failed(&node, id);
        }
        let read = node.read_failed(QUEUE, 0, 64).expect("read");
        assert_eq!(
            (read.count, journeys(&read)),
            (3, vec![3, 1, 4]),
            "as they failed"
        );
        assert_eq!(
            read.failed[1].place, 0,
            "its place in the Send Port's queue"
        );
        assert!(read.failed[1].reason.contains("refused 1"));
        assert_eq!(marked(&node), [1, 3, 4], "their places marked failed");
        // A Retry moves it to the end of the queue, a blocking one keeps its
        // place, a Dismiss lets go of it: each leaves the index.
        handed(&node, 3, |claim| HandOn {
            requeued: vec![QUEUE],
            cleared: vec![QUEUE],
            waits: None,
            ..hand_on(claim, b"retried")
        });
        handed(&node, 4, |claim| HandOn {
            cleared: vec![QUEUE],
            waits: None,
            ..hand_on(claim, b"retried where it blocks")
        });
        let read = node.read_failed(QUEUE, 0, 64).expect("read");
        assert_eq!((read.count, journeys(&read)), (1, vec![1]));
        assert_eq!(marked(&node), [1], "retried, no longer marked");
        handed(&node, 1, |claim| HandOn {
            leaves: vec![QUEUE],
            cleared: vec![QUEUE],
            waits: None,
            ..hand_on(claim, b"dismissed")
        });
        let read = node.read_failed(QUEUE, 0, 64).expect("read");
        assert_eq!((read.count, read.failed.len()), (0, 0), "none waits");
        assert_eq!(node.read_held(QUEUE, 0, 64).expect("read").count, 3);
        // Failed again after its Retry, it is entered again, at the end.
        failed(&node, 3);
        drop(node);
        let again = opened(engines);
        let read = again.read_failed(QUEUE, 0, 64).expect("read");
        assert_eq!(
            (read.count, journeys(&read)),
            (1, vec![3]),
            "kept across a restart"
        );
        assert_eq!(
            read.failed[0].sequence, 3,
            "its own place, never one before"
        );
    }

    #[test]
    fn many_failures_are_counted_at_once_and_read_a_page_at_a_time() {
        let node = opened(kept());
        published(&node, 300);
        for id in 1..=300 {
            failed(&node, id);
        }
        let places = node.read_failed(QUEUE, 0, 0).expect("its places");
        assert_eq!(
            (places.count, places.failed.len()),
            (300, 0),
            "a count, nothing read"
        );
        let mut seen = Vec::new();
        let mut from = 0;
        loop {
            let page = node.read_failed(QUEUE, from, 64).expect("a page");
            let Some(last) = page.failed.last() else {
                break;
            };
            from = last.sequence + 1;
            seen.extend(journeys(&page));
        }
        assert_eq!(
            seen,
            (1..=300).collect::<Vec<_>>(),
            "oldest first, each once"
        );
    }

    #[test]
    fn a_journey_left_waiting_keeps_its_due_time_at_its_place_and_no_claim() {
        let engines = kept();
        let node = opened(engines);
        published(&node, 2);
        let hour = 3_600_000_000_000;
        handed(&node, 1, |claim| HandOn {
            waits: Some(crate::storage::Wait {
                queue: QUEUE,
                after_nanos: hour,
            }),
            ..hand_on(claim, b"recovering")
        });
        drop(node);
        let again = opened(engines);
        let read = again.read_held(QUEUE, 0, 64).expect("read");
        let left = read.due_in(&read.held[0]).expect("waits, across a restart");
        assert!(left.as_nanos() > u128::from(hour) / 2, "{left:?}");
        assert_eq!(
            read.due_in(&read.held[1]),
            None,
            "the other waits for nothing"
        );
        let other = configure::fixture::test_cluster().node_scope(1);
        let lease = Duration::from_secs(30);
        let free = again.claim(JourneyId::new(1), &other, 3, lease);
        assert!(free.expect("asked").is_some(), "its claim released");
    }

    #[test]
    fn a_failed_queue_comes_back_from_its_bytes_as_it_was() {
        let queue = FailedQueue {
            first: 2,
            next: 5,
            count: 1,
            failed: vec![Failed {
                sequence: 3,
                journey: JourneyId::new(4),
                place: 9,
                reason: "refused".to_string(),
                failed_unix_nanos: -6,
            }],
        };
        assert_eq!(
            FailedQueue::from_bytes(&queue.bytes()).expect("read"),
            queue
        );
    }
}
