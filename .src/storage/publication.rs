//! A Publication as Xmip Storage keeps it: the Message a receive made, the
//! Journeys it opened — one per matched Subscription — the ones a paused
//! Subscription holds ([`super::Hold`]), its Dead Message Queue entry where
//! nothing matched ([`super::DeadMessage`]), and the audit record that says
//! so, written as one (`runtime-model.md` section 5: *Publication — the
//! Message record in the Ledger, audited*).
//!
//! **One write, all or nothing.** A receive killed in the middle of its
//! Publication leaves its Message with every Journey, or with its Dead
//! Message Queue entry, or nothing, so no Journey goes on from a Message
//! whose others were never written, no hold is lost to a write that was
//! refused, no Message nothing matched is kept without why, and the sender
//! — not acknowledged — sends again. One write is also one sync: a
//! Publication costs what one record costs.

use codec::cursor::Cursor;

use super::dead::DeadMessage;
use super::hold::Hold;
use super::record::{
    AuditEntry, Claim, Form, JourneyRecord, MessageRecord, read_byte, read_u64, write_byte,
    write_u64,
};
use crate::PersistError;

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

impl Form for Publication {
    fn write(&self, out: &mut Vec<u8>) {
        self.message.write(out);
        self.journeys.write(out);
        self.held.write(out);
        write_byte(out, u8::from(self.dead.is_some()));
        if let Some(dead) = &self.dead {
            dead.write(out);
        }
        self.audit.write(out);
        self.claims.write(out);
        write_u64(out, self.lease_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            message: MessageRecord::read(cursor)?,
            journeys: Vec::read(cursor)?,
            held: Vec::read(cursor)?,
            dead: match read_byte(cursor)? {
                0 => None,
                _ => Some(DeadMessage::read(cursor)?),
            },
            audit: AuditEntry::read(cursor)?,
            claims: Vec::read(cursor)?,
            lease_nanos: read_u64(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xcore::{AuditId, JourneyId, MessageId};

    #[test]
    fn a_publication_comes_back_from_its_bytes_as_it_was() {
        let publication = Publication {
            message: MessageRecord {
                message: MessageId::new(1),
                body: b"message".to_vec(),
            },
            journeys: vec![
                JourneyRecord {
                    journey: JourneyId::new(2),
                    body: b"to billing".to_vec(),
                },
                JourneyRecord {
                    journey: JourneyId::new(3),
                    body: b"to archive".to_vec(),
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
            },
            claims: vec![Claim {
                journey: JourneyId::new(2),
                holder: configure::fixture::test_cluster().node_scope(0),
                token: 11,
                until_unix_nanos: 0,
            }],
            lease_nanos: 30_000_000_000,
        };
        assert_eq!(
            Publication::from_bytes(&publication.bytes()).expect("read"),
            publication
        );
    }
}
