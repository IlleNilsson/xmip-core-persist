//! A Storage node in memory crowded with what a queue holds: Messages
//! published, claimed and handed on as waiting a retry or failed. What
//! `storage::due`'s tests and the module's timing program (`tests/measured`)
//! both read what is due from. Through the storage's public surface alone.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use xcore::{AuditId, Clock, JourneyId, MessageId};

use super::{Counted, Memory};
use crate::EncryptedStore;
use crate::storage::{
    AuditEntry, AuditFacts, Embedded, Failure, HandOn, Hold, JourneyFacts, JourneyRecord,
    MessageFacts, MessageRecord, Publication, Wait, XmipStorage,
};

/// The queue every Message here is held in.
pub const QUEUE: u128 = 7;
/// How long a claim holds.
pub const LEASE: Duration = Duration::from_secs(30);
/// An hour, in nanoseconds: a retry not due while a test runs.
pub const HOUR: u64 = 3_600_000_000_000;

/// A clock a test moves by hand.
#[derive(Default)]
pub struct Pinned(Mutex<i128>);

impl Pinned {
    /// The clock moved on by `nanos`.
    ///
    /// # Panics
    ///
    /// Where another thread panicked holding it.
    pub fn pass(&self, nanos: i128) {
        *self.0.lock().expect("clock") += nanos;
    }
}

impl Clock for Pinned {
    fn unix_timestamp_nanos(&self) -> i128 {
        *self.0.lock().expect("clock")
    }
}

/// A Storage node in memory, its runtime database counting what it reads.
pub type Node = Embedded<Box<Counted>, Memory>;

/// An empty [`Node`] and its clock, a microsecond past the epoch.
///
/// # Panics
///
/// Where the node does not open.
#[must_use]
pub fn node() -> (Node, Arc<Pinned>) {
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

/// The Messages of the Journeys `ids` published in one write, each held in
/// [`QUEUE`].
///
/// # Panics
///
/// Where the publication is refused.
pub fn published(node: &Node, ids: std::ops::Range<u128>) {
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
    node.publish(Publication {
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

/// The Message of the Journey `id` claimed by `holder` and handed on as
/// `how` leaves it.
///
/// # Panics
///
/// Where the claim or the hand-on is refused.
pub fn handed_by(node: &Node, holder: &str, id: u128, how: impl FnOnce(&mut HandOn)) {
    let claim = node.claim(JourneyId::new(id), holder, id, LEASE);
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

/// A hand-on that waits `after_nanos` to be retried.
pub fn waits(after_nanos: u64) -> impl FnOnce(&mut HandOn) {
    move |hand_on| {
        hand_on.waits = Some(Wait {
            queue: QUEUE,
            after_nanos,
        });
    }
}

/// A hand-on that failed.
pub fn fails(hand_on: &mut HandOn) {
    hand_on.failed = Some(Failure {
        queue: QUEUE,
        reason: "refused".to_string(),
    });
}

/// A node holding three Messages due, `waiting` waiting an hour to be
/// retried and `failed` failed, each claimed by `holder`.
#[must_use]
pub fn crowded(holder: &str, waiting: u128, failed: u128) -> Node {
    let (node, _) = node();
    published(&node, 1..waiting + failed + 4);
    for id in 1..=waiting {
        handed_by(&node, holder, id, waits(HOUR));
    }
    for id in waiting + 1..=waiting + failed {
        handed_by(&node, holder, id, fails);
    }
    node
}
