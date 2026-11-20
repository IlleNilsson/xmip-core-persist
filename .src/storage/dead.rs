//! A node's Dead Message Queue, in the Ledger (`runtime-model.md` section
//! 9: *The Dead Message Queue is Ledger state, not a place beside it*;
//! ADR-0013, amendments 2026-10-01 and 2026-10-03).
//!
//! **Kept with its Publication.** A Publication that matched nothing
//! carries its entry, [`DeadMessage`] — the Message's identity, its receive
//! context, what its gates concluded, its promoted properties and every
//! Subscription's reason for declining — and Xmip Storage writes it in the
//! Publication's one write: the Message and its entry together, or neither,
//! so a Message nothing matched is never kept without the answer to *why*.
//!
//! **One queue per node**, found by its identifier
//! ([`dead_message_queue`]), numbered as it is written and read
//! oldest first, as every queue is ([`super::queue`]).
//!
//! **Replay is one write, once** ([`Replay`]). The Operator's Replay writes
//! the Journeys a routing against the Subscriptions of now opened, the ones
//! a paused Subscription holds, and its audit record, and takes the entry
//! out, together. The entry is remembered as replayed by its Message, so a
//! Replay asked again after a lost answer writes nothing twice
//! ([`Replayed::Before`]), and so does the Publication's write asked again.

use codec::form::Form;
use xcore::{MessageId, StreamId};

use super::audit_entry::AuditEntry;
use super::commit::{Batch, DEAD, DEAD_MESSAGE, DEAD_PLACES, JOURNEY};
use super::hold::{self, Hold};
use super::queue::{self, Kinds, Placed};
use super::record::JourneyRecord;
use crate::{EncryptedStore, Engine, PersistError};

mod form;

/// Where a node's Dead Message Queue keeps its entries.
pub(crate) const KINDS: Kinds = Kinds {
    entry: DEAD,
    by: DEAD_MESSAGE,
    places: DEAD_PLACES,
    table: "dead_message",
    index: "dead_message_place",
};

/// The identifier of the Dead Message Queue of the node at `node`
/// (`xmip:///<cluster>/node/<name>`): the same on every node.
#[must_use]
pub fn dead_message_queue(node: &str) -> u128 {
    super::named(&format!("{node}/dead-message-queue"))
}

/// A name and what it said: a promoted property and its value, a gate and
/// what it concluded, a Subscription and why it declined.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Named {
    pub name: String,
    pub value: String,
}

impl Named {
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// A Message nothing matched, as its node's Dead Message Queue keeps it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadMessage {
    /// The queue: the node's Dead Message Queue's identifier.
    pub queue: u128,
    pub message: MessageId,
    /// The Stream the Message was created over.
    pub stream: StreamId,
    /// The node that received it: `xmip:///<cluster>/node/<name>`.
    pub node: String,
    /// The Receive Location it arrived at.
    pub location: String,
    /// When its Publication was written, in nanoseconds since the Unix epoch.
    pub received_unix_nanos: i128,
    /// What each gate concluded of it, in the order they ran.
    pub validation: Vec<Named>,
    /// The properties routing read, by name.
    pub promoted: Vec<Named>,
    /// Every Subscription asked, and why it declined, in the order asked.
    pub declines: Vec<Named>,
    /// What a Replay holds beside each Journey it opens, in the holder's own
    /// form ([`Hold::body`]).
    pub body: Vec<u8>,
}

impl Default for DeadMessage {
    /// An entry about nothing yet: what a test fills in.
    fn default() -> Self {
        Self {
            queue: 0,
            message: MessageId::new(0),
            stream: StreamId::new(0),
            node: String::new(),
            location: String::new(),
            received_unix_nanos: 0,
            validation: Vec::new(),
            promoted: Vec::new(),
            declines: Vec::new(),
            body: Vec::new(),
        }
    }
}

/// An entry at its place in its queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dead {
    /// Its place, given by Xmip Storage as it was kept: oldest lowest.
    pub sequence: u64,
    pub message: DeadMessage,
}

/// A Dead Message Queue as read: its places, how many it holds, and the
/// entries asked for, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeadQueue {
    /// The place of the oldest it still holds; `next` where it holds none.
    pub first: u64,
    /// The place the next entry takes.
    pub next: u64,
    /// How many it holds.
    pub count: u64,
    /// The entries read, oldest first.
    pub dead: Vec<Dead>,
}

/// An Operator's Replay of one entry: the Journeys a routing of its Message
/// against the Subscriptions of now opened, those of them a Subscription
/// holds, and the audit record of the act, written with the entry taken out
/// as one write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replay {
    /// The node's Dead Message Queue.
    pub queue: u128,
    /// The Message the entry is for.
    pub message: MessageId,
    pub journeys: Vec<JourneyRecord>,
    pub held: Vec<Hold>,
    pub audit: AuditEntry,
}

/// What a Dead Message Queue keeps for one Message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeadEntry {
    /// Its entry, at its place.
    Kept(Box<Dead>),
    /// Replayed: the entry was taken out by a Replay.
    Replayed,
    /// The queue never kept one for it.
    Never,
}

/// What came of a Replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Replayed {
    /// Replayed now: its Journeys written and the entry taken out.
    Now,
    /// Replayed before, by this ask whose answer was lost or by another:
    /// nothing written.
    Before,
    /// The queue holds no entry for the Message, and never did: nothing
    /// written.
    Absent,
}

/// `dead` kept at the end of its node's queue, in `batch`. An entry its
/// queue keeps already, or replayed already, is not kept again, so a
/// Publication asked again keeps it once.
pub(crate) fn keep<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    dead: &DeadMessage,
) -> Result<(), PersistError> {
    let about = (dead.queue, dead.message.value());
    queue::place(store, batch, KINDS, about, |sequence| Dead {
        sequence,
        message: dead.clone(),
    })
    .map(drop)
}

/// `replay` decided in `batch`: its Journeys written, those held kept at the
/// end of their queues, the entry taken out and remembered as replayed —
/// where the queue holds it. Its audit record is the writer's to number.
pub(crate) fn replay<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    replay: &Replay,
) -> Result<Replayed, PersistError> {
    let about = (replay.queue, replay.message.value());
    let sequence = match queue::placed(store, batch, KINDS, about)? {
        Placed::Never => return Ok(Replayed::Absent),
        Placed::TakenOut => return Ok(Replayed::Before),
        Placed::At(sequence) => sequence,
    };
    queue::take_out(store, batch, KINDS, replay.queue, sequence)?;
    queue::remember(batch, KINDS, about);
    for journey in &replay.journeys {
        super::origin::counted(store, batch, journey, &replay.held)?;
        batch.record(JOURNEY, journey.journey.value(), journey);
    }
    for held in &replay.held {
        hold::keep(store, batch, held.clone())?;
    }
    Ok(Replayed::Now)
}

/// `queue` as `store` keeps it: its places, and up to `most` entries from
/// the place `from` on, oldest first.
pub(crate) fn read<R: Engine>(
    store: &EncryptedStore<R>,
    queue: u128,
    from: u64,
    most: u32,
) -> Result<DeadQueue, PersistError> {
    let (places, entries) = queue::page(store, KINDS, queue, from, most)?;
    Ok(DeadQueue {
        first: places.first,
        next: places.next,
        count: places.count,
        dead: entries
            .iter()
            .map(|bytes| Dead::from_bytes(bytes))
            .collect::<Result<_, _>>()?,
    })
}

/// What `queue` keeps for `message`: its entry, or that it was replayed, or
/// that it never kept one.
pub(crate) fn read_one<R: Engine>(
    store: &EncryptedStore<R>,
    queue: u128,
    message: MessageId,
) -> Result<DeadEntry, PersistError> {
    let Some(place) = store.get(DEAD_MESSAGE, &queue::by_key(queue, message.value()))? else {
        return Ok(DeadEntry::Never);
    };
    let Ok(place) = <[u8; 8]>::try_from(place.as_slice()) else {
        return Ok(DeadEntry::Replayed);
    };
    let at = queue::entry_key(queue, u64::from_be_bytes(place));
    Ok(match store.get(DEAD, &at)? {
        Some(bytes) => DeadEntry::Kept(Box::new(Dead::from_bytes(&bytes)?)),
        None => DeadEntry::Never,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AuditFacts, MessageFacts, MessageRecord, Publication};
    use codec::cursor::Cursor;
    use xcore::AuditId;

    #[test]
    fn an_entry_and_a_replay_come_back_from_their_bytes_as_they_were() {
        let dead = Dead {
            sequence: 4,
            message: DeadMessage {
                queue: 9,
                message: MessageId::new(1),
                stream: StreamId::new(2),
                node: "a node".to_string(),
                location: "a Location".to_string(),
                received_unix_nanos: -7,
                validation: vec![Named::new("transport", "proven")],
                promoted: vec![Named::new("MessageType", "Invoice")],
                declines: vec![Named::new("orders", "MessageType is Invoice")],
                body: b"facts".to_vec(),
            },
        };
        assert_eq!(Dead::from_bytes(&dead.bytes()).expect("read"), dead);
        for entry in [
            DeadEntry::Kept(Box::new(dead.clone())),
            DeadEntry::Replayed,
            DeadEntry::Never,
        ] {
            assert_eq!(DeadEntry::from_bytes(&entry.bytes()).expect("read"), entry);
        }
        let queue = DeadQueue {
            first: 4,
            next: 5,
            count: 1,
            dead: vec![dead],
        };
        assert_eq!(DeadQueue::from_bytes(&queue.bytes()).expect("read"), queue);
        for replayed in [Replayed::Now, Replayed::Before, Replayed::Absent] {
            assert_eq!(
                Replayed::from_bytes(&replayed.bytes()).expect("read"),
                replayed
            );
        }
        assert!(Replayed::from_bytes(&[3]).is_err());
        let refused = DeadMessage::read_numbered(&mut Cursor::new(&[2])).expect_err("another form");
        assert_eq!(
            refused.message,
            "a Dead Message Queue entry of form 2, and this reads form 1"
        );
    }

    /// An entry and the bytes it is written in: its form's number, then its
    /// fields, each laid out by hand here, so a change of the form shows.
    fn golden() -> (DeadMessage, Vec<u8>) {
        let dead = DeadMessage {
            queue: 9,
            message: MessageId::new(1),
            stream: StreamId::new(2),
            node: "n".to_string(),
            location: "l".to_string(),
            received_unix_nanos: -7,
            validation: vec![Named::new("a", "b")],
            promoted: Vec::new(),
            declines: vec![Named::new("c", "d")],
            body: b"x".to_vec(),
        };
        let bytes = [
            &[1][..],
            &9u128.to_be_bytes(),
            &1u128.to_be_bytes(),
            &2u128.to_be_bytes(),
            &[1, b'n', 1, b'l'],
            &(-7i128).to_be_bytes(),
            &[0, 0, 0, 1, 1, b'a', 1, b'b'],
            &[0, 0, 0, 0],
            &[0, 0, 0, 1, 1, b'c', 1, b'd'],
            &[1, b'x'],
        ]
        .concat();
        (dead, bytes)
    }

    #[test]
    fn an_entry_is_written_kept_and_handed_on_in_the_bytes_of_its_form() {
        let (message, entry) = golden();
        let mut written = Vec::new();
        message.write_numbered(&mut written);
        assert_eq!(written, entry, "numbered");
        let mut cursor = Cursor::new(&entry);
        assert_eq!(DeadMessage::read_numbered(&mut cursor), Ok(message.clone()));
        assert!(cursor.is_empty(), "read whole");
        let dead = Dead {
            sequence: 4,
            message: message.clone(),
        };
        let kept = [&4u64.to_be_bytes()[..], &entry].concat();
        assert_eq!(dead.bytes(), kept, "kept in its queue");
        assert_eq!(Dead::from_bytes(&kept).expect("kept"), dead);
        let standing = DeadEntry::Kept(Box::new(dead));
        assert_eq!(standing.bytes(), [&[0][..], &kept].concat(), "as read");
        let audit = AuditEntry {
            id: AuditId::new(5),
            body: Vec::new(),
            audited: None,
            facts: AuditFacts::default(),
        };
        let published = Publication {
            message: MessageRecord {
                message: MessageId::new(1),
                body: Vec::new(),
                facts: MessageFacts::default(),
            },
            journeys: Vec::new(),
            held: Vec::new(),
            dead: Some(message),
            audit: audit.clone(),
            claims: Vec::new(),
            lease_nanos: 3,
        };
        let mut without = published.clone();
        without.dead = None;
        let before = without.message.bytes().len() + 8;
        let handed = without.bytes();
        let expected = [
            &handed[..before],
            &[1],
            &entry,
            &audit.bytes(),
            &[0, 0, 0, 0],
            &3u64.to_be_bytes(),
        ]
        .concat();
        assert_eq!(published.bytes(), expected, "handed on in its Publication");
    }
}
