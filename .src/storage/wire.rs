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

use std::time::Duration;

use codec::cursor::Cursor;
use xcore::{AuditId, JourneyId, MessageId, StreamId};

use super::audit_entry::AuditEntry;
use super::dead::Replay;
use super::hand_on::HandOn;
use super::publication::Publication;
use super::query::Query;
use super::record::{
    AdministrationKind, AdministrationRecord, Claim, Form, JourneyRecord, MessageRecord,
    StreamChunk, malformed, read_byte, read_u32, read_u64, read_u128, write_byte, write_u32,
    write_u64, write_u128,
};
use super::stream::StreamRecord;
use crate::PersistError;

mod answer;
mod frame;

pub(crate) use answer::Answer;
pub(crate) use frame::{receive, send};

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
}

fn nanos(lease: Duration) -> u64 {
    u64::try_from(lease.as_nanos()).unwrap_or(u64::MAX)
}

impl Request {
    /// The operations numbered from 22 on, written as `write` writes the rest.
    fn write_late(&self, out: &mut Vec<u8>) {
        match self {
            Self::Query(query) => {
                write_byte(out, 22);
                query.write(out);
            }
            Self::WriteStream(last, stream) => {
                write_byte(out, 23);
                last.write(out);
                stream.write(out);
            }
            Self::ReadStream(id) => {
                write_byte(out, 24);
                write_u128(out, id.value());
            }
            Self::ReadKeptAuditChunk(id, stream, index) => {
                write_byte(out, 25);
                write_u128(out, id.value());
                write_byte(out, u8::from(stream.is_some()));
                write_u128(out, stream.map_or(0, StreamId::value));
                write_u32(out, *index);
            }
            Self::ReadKeptAuditStream(id, stream) => {
                write_byte(out, 26);
                write_u128(out, id.value());
                write_u128(out, stream.value());
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
                write_byte(out, 1);
                chunk.write(out);
            }
            Self::ReadChunk(stream, index) => {
                write_byte(out, 2);
                write_u128(out, stream.value());
                write_u32(out, *index);
            }
            Self::WriteMessage(message) => {
                write_byte(out, 3);
                message.write(out);
            }
            Self::ReadMessage(id) => {
                write_byte(out, 4);
                write_u128(out, id.value());
            }
            Self::WriteJourney(journey) => {
                write_byte(out, 5);
                journey.write(out);
            }
            Self::ReadJourney(id) => {
                write_byte(out, 6);
                write_u128(out, id.value());
            }
            Self::Claim(claim, lease) => {
                write_byte(out, 7);
                claim.write(out);
                write_u64(out, *lease);
            }
            Self::Renew(claims, lease) => {
                write_byte(out, 8);
                claims.write(out);
                write_u64(out, *lease);
            }
            Self::Release(claim) => {
                write_byte(out, 9);
                claim.write(out);
            }
            Self::HandOn(hand_on) => {
                write_byte(out, 10);
                hand_on.write(out);
            }
            Self::WriteAudit(entry) => {
                write_byte(out, 11);
                entry.write(out);
            }
            Self::KeepAudit(most, chunk) => {
                write_byte(out, 12);
                write_u32(out, *most);
                write_u64(out, *chunk);
            }
            Self::ReadKeptAudit(id) => {
                write_byte(out, 13);
                write_u128(out, id.value());
            }
            Self::WriteAdministration(record) => {
                write_byte(out, 14);
                record.write(out);
            }
            Self::ReadAdministration(kind, id) | Self::RemoveAdministration(kind, id) => {
                let read = matches!(self, Self::ReadAdministration(..));
                write_byte(out, if read { 15 } else { 16 });
                kind.write(out);
                write_u128(out, *id);
            }
            Self::Publish(publication) => {
                write_byte(out, 17);
                publication.write(out);
            }
            Self::ReadHeld(queue, from, most) => {
                write_byte(out, 18);
                write_u128(out, *queue);
                write_u64(out, *from);
                write_u32(out, *most);
            }
            Self::ReadDead(queue, from, most) => {
                write_byte(out, 19);
                write_u128(out, *queue);
                write_u64(out, *from);
                write_u32(out, *most);
            }
            Self::ReadDeadMessage(queue, message) => {
                write_byte(out, 20);
                write_u128(out, *queue);
                write_u128(out, message.value());
            }
            Self::Replay(replay) => {
                write_byte(out, 21);
                replay.write(out);
            }
            Self::Query(_)
            | Self::WriteStream(..)
            | Self::ReadStream(_)
            | Self::ReadKeptAuditChunk(..)
            | Self::ReadKeptAuditStream(..) => self.write_late(out),
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(match read_byte(cursor)? {
            1 => Self::WriteChunk(StreamChunk::read(cursor)?),
            2 => Self::ReadChunk(StreamId::new(read_u128(cursor)?), read_u32(cursor)?),
            3 => Self::WriteMessage(MessageRecord::read(cursor)?),
            4 => Self::ReadMessage(MessageId::new(read_u128(cursor)?)),
            5 => Self::WriteJourney(JourneyRecord::read(cursor)?),
            6 => Self::ReadJourney(JourneyId::new(read_u128(cursor)?)),
            7 => Self::Claim(Claim::read(cursor)?, read_u64(cursor)?),
            8 => Self::Renew(Vec::read(cursor)?, read_u64(cursor)?),
            9 => Self::Release(Claim::read(cursor)?),
            10 => Self::HandOn(HandOn::read(cursor)?),
            11 => Self::WriteAudit(AuditEntry::read(cursor)?),
            12 => Self::KeepAudit(read_u32(cursor)?, read_u64(cursor)?),
            13 => Self::ReadKeptAudit(AuditId::new(read_u128(cursor)?)),
            14 => Self::WriteAdministration(AdministrationRecord::read(cursor)?),
            15 => Self::ReadAdministration(AdministrationKind::read(cursor)?, read_u128(cursor)?),
            16 => Self::RemoveAdministration(AdministrationKind::read(cursor)?, read_u128(cursor)?),
            17 => Self::Publish(Box::new(Publication::read(cursor)?)),
            18 => Self::ReadHeld(read_u128(cursor)?, read_u64(cursor)?, read_u32(cursor)?),
            19 => Self::ReadDead(read_u128(cursor)?, read_u64(cursor)?, read_u32(cursor)?),
            20 => Self::ReadDeadMessage(read_u128(cursor)?, MessageId::new(read_u128(cursor)?)),
            21 => Self::Replay(Box::new(Replay::read(cursor)?)),
            22 => Self::Query(Query::read(cursor)?),
            23 => Self::WriteStream(StreamChunk::read(cursor)?, StreamRecord::read(cursor)?),
            24 => Self::ReadStream(StreamId::new(read_u128(cursor)?)),
            25 => {
                let id = AuditId::new(read_u128(cursor)?);
                let carried = read_byte(cursor)? != 0;
                let stream = StreamId::new(read_u128(cursor)?);
                Self::ReadKeptAuditChunk(id, carried.then_some(stream), read_u32(cursor)?)
            }
            26 => {
                let id = AuditId::new(read_u128(cursor)?);
                Self::ReadKeptAuditStream(id, StreamId::new(read_u128(cursor)?))
            }
            other => return Err(malformed(format!("no operation is numbered {other}"))),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{AuditFacts, JourneyFacts, MessageFacts};
    use crate::storage::{DeadEntry, DeadQueue, HeldQueue, Replayed};

    #[test]
    fn a_request_and_an_answer_cross_a_frame_as_they_were() {
        let claim = Claim {
            journey: JourneyId::new(5),
            holder: configure::fixture::test_cluster().node_scope(0),
            token: 9,
            until_unix_nanos: 0,
        };
        let requests = [
            Request::claim(claim.clone(), Duration::from_secs(30)),
            Request::renew(vec![claim.clone(), claim.clone()], Duration::from_secs(30)),
            Request::Release(claim.clone()),
            Request::ReadChunk(StreamId::new(1), 2),
            Request::KeepAudit(64, 4096),
            Request::ReadHeld(7, 2, 64),
            Request::HandOn(HandOn {
                claim: claim.clone(),
                result: JourneyRecord {
                    journey: JourneyId::new(5),
                    body: b"delivered".to_vec(),
                    facts: JourneyFacts::default(),
                },
                messages: Vec::new(),
                next: Vec::new(),
                leaves: vec![7],
                queued: Vec::new(),
                requeued: Vec::new(),
                kept_for_nanos: None,
            }),
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
            }),
            Answer::DeadQueue(DeadQueue::default()),
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
