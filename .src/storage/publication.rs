//! A Publication as Xmip Storage keeps it: the Message a receive made, the
//! Journeys it opened — one per matched Subscription — the ones a paused
//! Subscription holds ([`super::Hold`]), its Dead Message Queue entry where
//! nothing matched ([`super::DeadMessage`]), and the audit record that says
//! so, written as one (`runtime-model.md` section 5: *Publication — the
//! Message record in the Ledger, audited*).
//!
//! **One write, all or nothing.** A receive killed in the middle of its
//! Publication leaves its Message with every Journey, or with its Dead
//! Message Queue entry, or nothing, so no Message goes on one way whose
//! other Journeys were never written, no hold is lost to a write that was
//! refused, no Message nothing matched is kept without why, and the sender
//! — not acknowledged — sends again. One write is also one sync: a
//! Publication costs what one record costs.
//!
//! **Written once, by its Message.** A Publication is known by its
//! Message's identifier, and its write keeps, in the same batch, that it
//! was written and the digest of what was asked: of its identity
//! ([`identity`]), every identifier it carries. A Publication asked again
//! — a node's request repeated after its answer was lost — writes nothing
//! and is answered as it stands: the claims its node still holds of those
//! it took. So a Message another node has since sent, retried or dismissed
//! is never written back to where the Publication left it, nor queued
//! again, nor claimed again by the node that published it (review of
//! 2026-10-06: a repeated Publication reset a Message another node had
//! completed and queued it for a second delivery). Another Publication of
//! the same Message is refused, and writes nothing: a business Message
//! published again is another Message, with an identifier of its own
//! (`runtime-model.md` section 9, *Duplicates are a business decision*).
//!
//! **What it makes is new** (2026-11-14). Its Message and its Journeys are
//! drawn fresh (`UUIDv7`), so no record is at them, nor at their claims and
//! their places in a queue, before it: the same premise its own record,
//! by its Message, rests on. The writer reads none of them ([`made_new`]).

use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::audit_entry::AuditEntry;
use super::claim;
use super::commit::{Batch, CLAIM, DEAD_MESSAGE, HELD_JOURNEY, JOURNEY, MESSAGE, PUBLICATION};
use super::dead::{self, DeadMessage};
use super::hold::{self, Hold};
use super::origin::ORIGIN;
use super::queue::by_key;
use super::record::{Claim, JourneyRecord, MessageRecord};
use crate::{EncryptedStore, Engine, PersistError};

/// A Message, the Journeys it opened, the ones held, its Dead Message Queue
/// entry, and the audit record of its Publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publication {
    pub message: MessageRecord,
    /// One per matched Subscription; none where nothing matched.
    pub journeys: Vec<JourneyRecord>,
    /// Those of `journeys` a paused Subscription holds, each kept at the
    /// end of its queue.
    pub held: Vec<Hold>,
    /// Where nothing matched: the entry its node's Dead Message Queue keeps.
    pub dead: Option<DeadMessage>,
    pub audit: AuditEntry,
    /// Those of `journeys` the publishing node claims in the same write, to
    /// carry on itself on its own Send pool, each for `lease_nanos` from
    /// the Storage node's now: the send costs no sync of its own before it
    /// starts (`runtime-model.md` section 10). A claim another holds, and
    /// has not let lapse, is not taken.
    pub claims: Vec<Claim>,
    pub lease_nanos: u64,
}

/// What a Publication's write decided: written now, its audit record still
/// to be numbered, or written before — and either way the claims its node
/// holds of those it asked for.
pub(crate) enum Decided {
    Now(Vec<Claim>, Box<AuditEntry>),
    Before(Vec<Claim>),
}

/// `publication` decided in `batch` at `now`: its Message, its Journeys,
/// the ones held, its Dead Message Queue entry and its claims written, and
/// that it was written kept by its Message — or, where it was written
/// before, nothing, and the claims of it still held under their tokens.
/// Its Message and its Journeys are records it makes new, each laid out as
/// it is made (`Batch::created`).
///
/// # Errors
///
/// Where a record it reads cannot be read, or the Message was published
/// before as another Publication; nothing of it is written then.
pub(crate) fn decide<R: Engine>(
    store: &EncryptedStore<R>,
    batch: &mut Batch<'_>,
    publication: Publication,
    (now, epoch): (i128, u64),
) -> Result<Decided, PersistError> {
    let key = publication.message.message.value().to_be_bytes();
    let digest = identity(&publication);
    if let Some(written) = batch.read(store, PUBLICATION, &key)? {
        if written != digest {
            return Err(PersistError::Failed {
                reason: format!(
                    "the Message {} was published before as another Publication; \
                     nothing of this one is written",
                    publication.message.message
                ),
            });
        }
        let mut held = Vec::new();
        for asked in &publication.claims {
            if let Some(stored) = claim::stored(store, batch, asked)?
                && stored.asked_by(asked)
            {
                held.push(stored.claim);
            }
        }
        return Ok(Decided::Before(held));
    }
    made_new(batch, &publication);
    let Publication {
        message,
        journeys,
        held: holds,
        dead,
        audit,
        claims,
        lease_nanos,
    } = publication;
    batch.created(MESSAGE, key.to_vec(), message)?;
    for journey in journeys {
        super::origin::counted(store, batch, &journey, &holds)?;
        let at = journey.journey.value().to_be_bytes().to_vec();
        batch.created(JOURNEY, at, journey)?;
    }
    for kept in &holds {
        hold::keep(store, batch, kept)?;
    }
    if let Some(entry) = &dead {
        dead::keep(store, batch, entry)?;
    }
    let until = now.saturating_add(i128::from(lease_nanos));
    let mut held = Vec::new();
    for asked in claims {
        held.extend(claim::take(store, batch, asked, (now, until, epoch))?);
    }
    batch.put(PUBLICATION, key.to_vec(), Some(digest));
    Ok(Decided::Now(held, Box::new(audit)))
}

/// What `publication` is known by, asked again: the SHA-1 of every
/// identifier it carries — its Message, each Journey, each place it holds
/// and its queue, its Dead Message Queue entry's queue, its audit record,
/// each claim it asks for by its Journey, its holder and its token — and
/// its lease. A Publication asked again carries the same; one built again
/// draws a new audit record and new claim tokens. Until 2026-11-14 the whole
/// Publication was written out again, its bodies with it, and hashed.
fn identity(publication: &Publication) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    out.u128_be(publication.message.message.value());
    out.u32_be(count(publication.journeys.len()));
    for journey in &publication.journeys {
        out.u128_be(journey.journey.value());
    }
    out.u32_be(count(publication.held.len()));
    for kept in &publication.held {
        out.u128_be(kept.queue);
        out.u128_be(kept.journey.value());
    }
    out.byte(u8::from(publication.dead.is_some()));
    if let Some(dead) = &publication.dead {
        out.u128_be(dead.queue);
    }
    out.u128_be(publication.audit.id.value());
    out.u32_be(count(publication.claims.len()));
    for asked in &publication.claims {
        out.u128_be(asked.journey.value());
        field::counted(&mut out, asked.holder.as_bytes());
        out.u128_be(asked.token);
    }
    out.u64_be(publication.lease_nanos);
    codec::sha1::digest(&out).to_vec()
}

/// A length as a count of four bytes, at most what it holds.
fn count(length: usize) -> u32 {
    u32::try_from(length).unwrap_or(u32::MAX)
}

/// The places `publication` makes new, kept as found empty in `batch`
/// (`Batch::absent`): its Message's record and the holder record of the
/// Journeys it is the origin of; each of its Journeys' record and claim;
/// each place it holds of one of them, or of its Message, in its queue; and
/// its Dead Message Queue entry's. Its identifiers drawn fresh, none is
/// written before it, so none is read.
fn made_new(batch: &Batch<'_>, publication: &Publication) {
    let message = publication.message.message.value();
    let journeys = || publication.journeys.iter().map(|j| j.journey.value());
    batch.absent(MESSAGE, &message.to_be_bytes());
    batch.absent(ORIGIN, &message.to_be_bytes());
    for journey in journeys() {
        batch.absent(JOURNEY, &journey.to_be_bytes());
        batch.absent(CLAIM, &journey.to_be_bytes());
    }
    for kept in &publication.held {
        let about = kept.journey.value();
        if about == message || journeys().any(|journey| journey == about) {
            batch.absent(HELD_JOURNEY, &by_key(kept.queue, about));
        }
    }
    if let Some(dead) = &publication.dead
        && dead.message.value() == message
    {
        batch.absent(DEAD_MESSAGE, &by_key(dead.queue, message));
    }
}

impl Form for Publication {
    fn write(&self, out: &mut Vec<u8>) {
        self.message.write(out);
        self.journeys.write(out);
        self.held.write(out);
        out.byte(u8::from(self.dead.is_some()));
        if let Some(dead) = &self.dead {
            dead.write_numbered(out);
        }
        self.audit.write(out);
        self.claims.write(out);
        out.u64_be(self.lease_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            message: MessageRecord::read(cursor)?,
            journeys: Vec::read(cursor)?,
            held: Vec::read(cursor)?,
            dead: match cursor.byte()? {
                0 => None,
                _ => Some(DeadMessage::read_numbered(cursor)?),
            },
            audit: AuditEntry::read(cursor)?,
            claims: Vec::read(cursor)?,
            lease_nanos: cursor.u64_be()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AuditFacts, JourneyFacts, MessageFacts};
    use xcore::{AuditId, JourneyId, MessageId};

    #[test]
    fn a_publication_comes_back_from_its_bytes_as_it_was() {
        let publication = Publication {
            message: MessageRecord {
                message: MessageId::new(1),
                body: b"message".to_vec(),
                facts: MessageFacts::default(),
            },
            journeys: vec![
                JourneyRecord {
                    journey: JourneyId::new(2),
                    body: b"to billing".to_vec(),
                    facts: JourneyFacts::default(),
                },
                JourneyRecord {
                    journey: JourneyId::new(3),
                    body: b"to archive".to_vec(),
                    facts: JourneyFacts::default(),
                },
            ],
            held: vec![Hold {
                queue: 5,
                journey: JourneyId::new(3),
                body: b"held".to_vec(),
            }],
            dead: Some(super::super::DeadMessage {
                queue: 6,
                message: MessageId::new(1),
                node: "a node".to_string(),
                ..super::super::DeadMessage::default()
            }),
            audit: AuditEntry {
                id: AuditId::new(4),
                body: b"published".to_vec(),
                audited: None,
                facts: AuditFacts::default(),
            },
            claims: vec![Claim {
                journey: JourneyId::new(2),
                holder: configure::fixture::test_cluster().node_scope(0),
                token: 11,
                until_unix_nanos: 0,
                fence: 0,
            }],
            lease_nanos: 30_000_000_000,
        };
        assert_eq!(
            Publication::from_bytes(&publication.bytes()).expect("read"),
            publication
        );
    }

    /// The Publication of the Message of the Journeys 1 and 2, each held in
    /// the crowd's queue, the first claimed by the test cluster's first node
    /// under the token 11.
    fn claiming() -> Publication {
        Publication {
            claims: vec![Claim {
                journey: JourneyId::new(1),
                holder: configure::fixture::test_cluster().node_scope(0),
                token: 11,
                until_unix_nanos: 0,
                fence: 0,
            }],
            lease_nanos: 30_000_000_000,
            ..crate::fixture::crowd::publication(1..3)
        }
    }

    #[test]
    fn a_publication_asked_again_is_known_by_its_identifiers_and_one_built_again_is_refused() {
        use crate::fixture::crowd::{QUEUE, node};
        use crate::storage::XmipStorage;
        let (node, _) = node();
        let (first, _) = node.publish(claiming()).expect("published");
        let (again, _) = node.publish(claiming()).expect("asked again");
        assert_eq!(again, first, "the claims it holds, nothing written twice");
        assert_eq!(node.read_held(QUEUE, 0, 10).expect("read").count, 2);
        let built_again = |change: fn(&mut Publication)| {
            let mut other = claiming();
            change(&mut other);
            let refused = node.publish(other);
            assert!(
                matches!(&refused, Err(PersistError::Failed { reason }) if reason.contains("before")),
                "{refused:?}"
            );
        };
        // The same Message, Journeys and claims, each with something of its
        // own: a Publication built again is refused, and writes nothing.
        built_again(|other| other.audit.id = AuditId::new(99));
        built_again(|other| other.claims[0].token = 12);
        built_again(|other| other.held[1].queue = 8);
        built_again(|other| other.lease_nanos = 1);
        built_again(|other| other.journeys.truncate(1));
        assert_eq!(node.read_held(8, 0, 10).expect("read").count, 0);
        assert_eq!(node.read_held(QUEUE, 0, 10).expect("read").count, 2);
    }

    #[test]
    fn a_publication_reads_only_whether_it_was_written_and_where_its_queue_ends() {
        use crate::fixture::crowd::{node, published};
        let (node, _) = node();
        published(&node, 1..2);
        let counted = node.runtime().engine();
        counted.touched();
        published(&node, 2..3);
        // Its Message, Journey, claim, place and holder record are new, and
        // its queue's places, and by when each is due, kept by the writer as
        // the Publication before left them: read is its own record, by its
        // Message; and one range, for the first due since held, which finds
        // the Message before it.
        assert_eq!(counted.touched(), (1, 1, 1));
    }
}
