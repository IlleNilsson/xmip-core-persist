//! Xmip Storage on the wire: one request, one answer, over a mutual TLS
//! connection between a node and a Storage node (ADR-0063, amendment
//! 2026-10-01: *Xmip encrypts to Xmip Node with Role/Type Storage*).
//!
//! A frame is its length, four bytes big-endian, and that many bytes: a
//! request is an operation's number and its fields, an answer a verdict and
//! what the operation returned or why it failed. Each record is written in
//! its one binary form ([`Form`]), the form either database seals it in. A
//! connection carries one request at a time and answers it before the next,
//! synchronously, with no async runtime; a node keeps a connection per
//! Storage node per caller in flight, so many requests are in flight at
//! once and the Storage node's writer shares their sync.

use std::io::{self, Read, Write};
use std::time::Duration;

use codec::CodecError;
use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::{AuditId, JourneyId, MessageId, SecretName, StreamId};

use super::audit_entry::AuditEntry;
use super::dead::Replay;
use super::due::DuePart;
use super::hand_on::HandOn;
use super::instance::Turn;
use super::publication::Publication;
use super::query::Query;
use super::record::{
    AdministrationKind, AdministrationRecord, Claim, JourneyRecord, MessageRecord, StreamChunk,
};
use super::secret::SecretRecord;
use super::standby::Follow;
use super::stream::StreamRecord;

mod answer;
mod operation;

pub(crate) use answer::Answer;

/// One operation asked of Xmip Storage, with its fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Request {
    WriteChunk(StreamChunk),
    ReadChunk(StreamId, u32),
    WriteMessage(MessageRecord),
    ReadMessage(MessageId),
    WriteJourney(JourneyRecord),
    ReadJourney(JourneyId),
    Claim(Claim, u64),
    Renew(Vec<Claim>, u64),
    Release(Claim),
    HandOn(HandOn),
    WriteAudit(AuditEntry),
    KeepAudit(u32, u64),
    ReadKeptAudit(AuditId),
    WriteAdministration(AdministrationRecord),
    ReadAdministration(AdministrationKind, u128),
    RemoveAdministration(AdministrationKind, u128),
    Publish(Box<Publication>),
    ReadHeld(u128, u64, u32),
    ReadDead(u128, u64, u32),
    ReadDeadMessage(u128, MessageId),
    Replay(Box<Replay>),
    Query(Query),
    WriteStream(StreamChunk, StreamRecord),
    ReadStream(StreamId),
    ReadKeptAuditChunk(AuditId, Option<StreamId>, u32),
    ReadKeptAuditStream(AuditId, StreamId),
    ReadFailed(u128, u64, u32),
    ReadDue(u128, DuePart, (u64, u64), u32),
    /// What the Storage node is now: primary or standby, in which epoch
    /// (`super::standby`).
    Standing,
    /// A standby asking to be handed the primary's writes from where it is;
    /// the connection carries them from then on, frame after frame.
    Follow(Follow),
    /// A Work Process's turn handed on (`super::instance`).
    Turn(Box<Turn>),
    ReadInstance(u128),
    ReadInstanceKey(u128),
    /// A secret set (`super::secret`).
    WriteSecret(Box<SecretRecord>),
    ReadSecret(SecretName),
    ListSecrets,
}

fn nanos(lease: Duration) -> u64 {
    u64::try_from(lease.as_nanos()).unwrap_or(u64::MAX)
}

impl Request {
    /// The operations numbered from 22 on, written as `write` writes the rest.
    fn write_late(&self, out: &mut Vec<u8>) {
        match self {
            Self::Query(query) => {
                out.byte(22);
                query.write(out);
            }
            Self::WriteStream(last, stream) => {
                out.byte(23);
                last.write(out);
                stream.write(out);
            }
            Self::ReadStream(id) => {
                out.byte(24);
                out.u128_be(id.value());
            }
            Self::ReadKeptAuditChunk(id, stream, index) => {
                out.byte(25);
                out.u128_be(id.value());
                out.byte(u8::from(stream.is_some()));
                out.u128_be(stream.map_or(0, StreamId::value));
                out.u32_be(*index);
            }
            Self::ReadKeptAuditStream(id, stream) => {
                out.byte(26);
                out.u128_be(id.value());
                out.u128_be(stream.value());
            }
            Self::ReadFailed(queue, from, most) => {
                out.byte(27);
                out.u128_be(*queue);
                out.u64_be(*from);
                out.u32_be(*most);
            }
            Self::ReadDue(queue, part, (due, place), most) => {
                out.byte(28);
                out.u128_be(*queue);
                part.write(out);
                out.u64_be(*due);
                out.u64_be(*place);
                out.u32_be(*most);
            }
            Self::Standing => {
                out.byte(29);
            }
            Self::Follow(follow) => {
                out.byte(30);
                follow.write(out);
            }
            Self::Turn(turn) => {
                out.byte(31);
                turn.write(out);
            }
            Self::ReadInstance(id) => {
                out.byte(32);
                out.u128_be(*id);
            }
            Self::ReadInstanceKey(id) => {
                out.byte(33);
                out.u128_be(*id);
            }
            Self::WriteSecret(secret) => {
                out.byte(34);
                secret.write(out);
            }
            Self::ReadSecret(name) => {
                out.byte(35);
                field::text(out, name.as_str());
            }
            Self::ListSecrets => {
                out.byte(36);
            }
            _ => {}
        }
    }

    pub(crate) fn claim(claim: Claim, lease: Duration) -> Self {
        Self::Claim(claim, nanos(lease))
    }

    pub(crate) fn renew(claims: Vec<Claim>, lease: Duration) -> Self {
        Self::Renew(claims, nanos(lease))
    }
}

impl Form for Request {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::WriteChunk(chunk) => {
                out.byte(1);
                chunk.write(out);
            }
            Self::ReadChunk(stream, index) => {
                out.byte(2);
                out.u128_be(stream.value());
                out.u32_be(*index);
            }
            Self::WriteMessage(message) => {
                out.byte(3);
                message.write(out);
            }
            Self::ReadMessage(id) => {
                out.byte(4);
                out.u128_be(id.value());
            }
            Self::WriteJourney(journey) => {
                out.byte(5);
                journey.write(out);
            }
            Self::ReadJourney(id) => {
                out.byte(6);
                out.u128_be(id.value());
            }
            Self::Claim(claim, lease) => {
                out.byte(7);
                claim.write(out);
                out.u64_be(*lease);
            }
            Self::Renew(claims, lease) => {
                out.byte(8);
                claims.write(out);
                out.u64_be(*lease);
            }
            Self::Release(claim) => {
                out.byte(9);
                claim.write(out);
            }
            Self::HandOn(hand_on) => {
                out.byte(10);
                hand_on.write(out);
            }
            Self::WriteAudit(entry) => {
                out.byte(11);
                entry.write(out);
            }
            Self::KeepAudit(most, chunk) => {
                out.byte(12);
                out.u32_be(*most);
                out.u64_be(*chunk);
            }
            Self::ReadKeptAudit(id) => {
                out.byte(13);
                out.u128_be(id.value());
            }
            Self::WriteAdministration(record) => {
                out.byte(14);
                record.write(out);
            }
            Self::ReadAdministration(kind, id) | Self::RemoveAdministration(kind, id) => {
                let read = matches!(self, Self::ReadAdministration(..));
                out.byte(if read { 15 } else { 16 });
                kind.write(out);
                out.u128_be(*id);
            }
            Self::Publish(publication) => {
                out.byte(17);
                publication.write(out);
            }
            Self::ReadHeld(queue, from, most) => {
                out.byte(18);
                out.u128_be(*queue);
                out.u64_be(*from);
                out.u32_be(*most);
            }
            Self::ReadDead(queue, from, most) => {
                out.byte(19);
                out.u128_be(*queue);
                out.u64_be(*from);
                out.u32_be(*most);
            }
            Self::ReadDeadMessage(queue, message) => {
                out.byte(20);
                out.u128_be(*queue);
                out.u128_be(message.value());
            }
            Self::Replay(replay) => {
                out.byte(21);
                replay.write(out);
            }
            // Numbered from 22 on.
            _ => self.write_late(out),
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(match cursor.byte()? {
            1 => Self::WriteChunk(StreamChunk::read(cursor)?),
            2 => Self::ReadChunk(StreamId::new(cursor.u128_be()?), cursor.u32_be()?),
            3 => Self::WriteMessage(MessageRecord::read(cursor)?),
            4 => Self::ReadMessage(MessageId::new(cursor.u128_be()?)),
            5 => Self::WriteJourney(JourneyRecord::read(cursor)?),
            6 => Self::ReadJourney(JourneyId::new(cursor.u128_be()?)),
            7 => Self::Claim(Claim::read(cursor)?, cursor.u64_be()?),
            8 => Self::Renew(Vec::read(cursor)?, cursor.u64_be()?),
            9 => Self::Release(Claim::read(cursor)?),
            10 => Self::HandOn(HandOn::read(cursor)?),
            11 => Self::WriteAudit(AuditEntry::read(cursor)?),
            12 => Self::KeepAudit(cursor.u32_be()?, cursor.u64_be()?),
            13 => Self::ReadKeptAudit(AuditId::new(cursor.u128_be()?)),
            14 => Self::WriteAdministration(AdministrationRecord::read(cursor)?),
            15 => Self::ReadAdministration(AdministrationKind::read(cursor)?, cursor.u128_be()?),
            16 => Self::RemoveAdministration(AdministrationKind::read(cursor)?, cursor.u128_be()?),
            17 => Self::Publish(Box::new(Publication::read(cursor)?)),
            18 => Self::ReadHeld(cursor.u128_be()?, cursor.u64_be()?, cursor.u32_be()?),
            19 => Self::ReadDead(cursor.u128_be()?, cursor.u64_be()?, cursor.u32_be()?),
            20 => Self::ReadDeadMessage(cursor.u128_be()?, MessageId::new(cursor.u128_be()?)),
            21 => Self::Replay(Box::new(Replay::read(cursor)?)),
            22 => Self::Query(Query::read(cursor)?),
            23 => Self::WriteStream(StreamChunk::read(cursor)?, StreamRecord::read(cursor)?),
            24 => Self::ReadStream(StreamId::new(cursor.u128_be()?)),
            25 => {
                let id = AuditId::new(cursor.u128_be()?);
                let carried = cursor.byte()? != 0;
                let stream = StreamId::new(cursor.u128_be()?);
                Self::ReadKeptAuditChunk(id, carried.then_some(stream), cursor.u32_be()?)
            }
            26 => {
                let id = AuditId::new(cursor.u128_be()?);
                Self::ReadKeptAuditStream(id, StreamId::new(cursor.u128_be()?))
            }
            27 => Self::ReadFailed(cursor.u128_be()?, cursor.u64_be()?, cursor.u32_be()?),
            28 => Self::ReadDue(
                cursor.u128_be()?,
                DuePart::read(cursor)?,
                (cursor.u64_be()?, cursor.u64_be()?),
                cursor.u32_be()?,
            ),
            29 => Self::Standing,
            30 => Self::Follow(Follow::read(cursor)?),
            31 => Self::Turn(Box::new(Turn::read(cursor)?)),
            32 => Self::ReadInstance(cursor.u128_be()?),
            33 => Self::ReadInstanceKey(cursor.u128_be()?),
            34 => Self::WriteSecret(Box::new(SecretRecord::read(cursor)?)),
            35 => Self::ReadSecret(
                SecretName::new(field::read_text(cursor)?).map_err(CodecError::new)?,
            ),
            36 => Self::ListSecrets,
            other => return Err(CodecError::new(format!("no operation is numbered {other}"))),
        })
    }
}

/// Write `record` as one frame (`net::frame`).
///
/// # Errors
///
/// Where the connection fails, or the record is larger than a frame holds.
pub(crate) fn send(connection: &mut impl Write, record: &impl Form) -> io::Result<()> {
    net::frame::write(connection, &record.bytes()).map_err(failed)
}

/// Read one frame, and the record in it. `None` where the connection ended
/// cleanly before a frame began.
///
/// # Errors
///
/// Where the connection fails or ends inside a frame, or the frame is not
/// a record.
pub(crate) fn receive<T: Form>(connection: &mut impl Read) -> io::Result<Option<T>> {
    let Some(bytes) = net::frame::read(connection).map_err(failed)? else {
        return Ok(None);
    };
    T::from_bytes(&bytes)
        .map(Some)
        .map_err(|error| io::Error::other(format!("a frame that is not a record: {error}")))
}

/// A frame's failure as the connection's error, its kind kept where it
/// was the connection's.
fn failed(error: net::NetError) -> io::Error {
    io::Error::new(error.io.unwrap_or(io::ErrorKind::Other), error.message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AuditFacts, JourneyFacts, MessageFacts};
    use crate::storage::{DeadEntry, DeadQueue, FailedQueue, HeldQueue, Replayed};

    /// The hand-on of a send of the Message `claim` names, delivered.
    fn delivered(claim: &Claim) -> HandOn {
        HandOn {
            claim: claim.clone(),
            result: JourneyRecord {
                journey: claim.journey,
                body: b"delivered".to_vec(),
                facts: JourneyFacts::default(),
            },
            messages: Vec::new(),
            next: Vec::new(),
            leaves: vec![7],
            queued: Vec::new(),
            requeued: Vec::new(),
            failed: None,
            cleared: vec![7],
            waits: Some(super::super::Wait {
                queue: 7,
                after_nanos: 1,
            }),
            kept_for_nanos: None,
        }
    }

    #[test]
    fn a_request_and_an_answer_cross_a_frame_as_they_were() {
        let claim = Claim {
            journey: JourneyId::new(5),
            holder: configure::fixture::test_cluster().node_scope(0),
            token: 9,
            until_unix_nanos: 0,
            fence: 0,
        };
        let requests = [
            Request::claim(claim.clone(), Duration::from_secs(30)),
            Request::renew(vec![claim.clone(), claim.clone()], Duration::from_secs(30)),
            Request::Release(claim.clone()),
            Request::ReadChunk(StreamId::new(1), 2),
            Request::KeepAudit(64, 4096),
            Request::ReadHeld(7, 2, 64),
            Request::ReadFailed(7, 2, 64),
            Request::ReadDue(7, crate::storage::DuePart::Waiting, (3, 2), 64),
            Request::HandOn(delivered(&claim)),
            Request::RemoveAdministration(AdministrationKind::Operator, 3),
            Request::Publish(Box::new(Publication {
                message: MessageRecord {
                    message: MessageId::new(1),
                    body: b"order".to_vec(),
                    facts: MessageFacts::default(),
                },
                journeys: Vec::new(),
                held: Vec::new(),
                dead: None,
                audit: AuditEntry {
                    id: AuditId::new(2),
                    body: b"published".to_vec(),
                    audited: None,
                    facts: AuditFacts::default(),
                },
                claims: vec![claim.clone()],
                lease_nanos: 30_000_000_000,
            })),
            Request::ReadDead(7, 2, 64),
            Request::ReadDeadMessage(7, MessageId::new(1)),
            Request::Replay(Box::new(Replay {
                queue: 7,
                message: MessageId::new(1),
                journeys: Vec::new(),
                held: Vec::new(),
                audit: AuditEntry {
                    id: AuditId::new(3),
                    body: b"replayed".to_vec(),
                    audited: None,
                    facts: AuditFacts::default(),
                },
            })),
            Request::Query(super::super::Query {
                ask: super::super::Ask::MessagesFromParty {
                    party: "partner-x".to_string(),
                    created: super::super::Span::ALL,
                },
                most: 64,
                newest_first: true,
            }),
        ];
        let mut wire = Vec::new();
        for request in &requests {
            send(&mut wire, request).expect("sent");
        }
        let mut reading = wire.as_slice();
        for request in requests {
            let back: Request = receive(&mut reading).expect("read").expect("a frame");
            assert_eq!(back, request);
        }
        assert_eq!(receive::<Request>(&mut reading).expect("clean"), None);
        let answers = [
            Answer::Claim(Some(claim.clone())),
            Answer::Claims(vec![claim]),
            Answer::Refused("rocksdb record".to_string(), "its tag".to_string()),
            Answer::Count(3),
            Answer::Held(HeldQueue {
                first: 1,
                next: 2,
                count: 1,
                held: Vec::new(),
                read_unix_nanos: 3,
            }),
            Answer::DeadQueue(DeadQueue::default()),
            Answer::FailedQueue(FailedQueue::default()),
            Answer::DueQueue(crate::storage::DueQueue {
                retrying: 4,
                next_due_unix_nanos: Some(9),
                read_unix_nanos: 5,
                ..crate::storage::DueQueue::default()
            }),
            Answer::Dead(DeadEntry::Replayed),
            Answer::Replayed(Replayed::Before),
            Answer::Records(vec![1, 2, 3]),
        ];
        for answer in answers {
            assert_eq!(Answer::from_bytes(&answer.bytes()).expect("answer"), answer);
        }
    }

    #[test]
    fn a_stream_s_record_and_its_last_chunk_cross_a_frame_as_they_were() {
        let record = super::super::StreamRecord {
            stream: StreamId::new(1),
            length: 5,
            chunks: 1,
            digest: [3; super::super::DIGEST],
            written_unix_nanos: 2,
        };
        let last = StreamChunk {
            stream: StreamId::new(1),
            index: 0,
            bytes: b"order".to_vec(),
        };
        for request in [
            Request::WriteStream(last, record),
            Request::ReadStream(StreamId::new(1)),
            Request::ReadKeptAuditChunk(AuditId::new(2), Some(StreamId::new(1)), 3),
            Request::ReadKeptAuditChunk(AuditId::new(2), None, 3),
            Request::ReadKeptAuditStream(AuditId::new(2), StreamId::new(1)),
            Request::WriteAudit(AuditEntry {
                id: AuditId::new(2),
                body: b"published".to_vec(),
                audited: Some(super::super::Audited {
                    message: b"order".to_vec(),
                    streams: vec![StreamId::new(1)],
                }),
                facts: AuditFacts::default(),
            }),
        ] {
            assert_eq!(
                Request::from_bytes(&request.bytes()).expect("read"),
                request
            );
        }
        for answer in [Answer::Stream(Some(record)), Answer::Stream(None)] {
            assert_eq!(Answer::from_bytes(&answer.bytes()).expect("read"), answer);
        }
    }

    #[test]
    fn a_frame_cut_short_or_numbered_for_nothing_is_refused() {
        let mut wire = Vec::new();
        send(&mut wire, &Request::KeepAudit(1, 4096)).expect("sent");
        let cut = &wire[..wire.len() - 1];
        assert!(receive::<Request>(&mut &cut[..]).is_err());
        let nothing = [0, 0, 0, 1, 99];
        assert!(receive::<Request>(&mut &nothing[..]).is_err());
        let huge = (u32::MAX).to_be_bytes();
        assert!(receive::<Request>(&mut &huge[..]).is_err());
    }
}
