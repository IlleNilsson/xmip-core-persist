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

use std::io::{Read, Write};
use std::time::Duration;

use codec::cursor::Cursor;
use xcore::{AuditId, JourneyId, MessageId, StreamId};

use super::dead::{DeadEntry, DeadQueue, Replay, Replayed};
use super::hand_on::HandOn;
use super::hold::HeldQueue;
use super::publication::Publication;
use super::record::{
    AdministrationKind, AdministrationRecord, AuditEntry, Claim, Form, JourneyRecord,
    MessageRecord, StreamChunk, malformed, read_byte, read_text, read_u32, read_u64, read_u128,
    write_byte, write_text, write_u32, write_u64, write_u128,
};
use crate::PersistError;

/// The most one frame holds: the ceiling every connection in the estate is
/// read for (`net::MAX_BODY`).
const MOST: usize = net::MAX_BODY;

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
    Renew(Claim, u64),
    Release(Claim),
    HandOn(HandOn),
    WriteAudit(AuditEntry),
    KeepAudit(u32),
    ReadKeptAudit(AuditId),
    WriteAdministration(AdministrationRecord),
    ReadAdministration(AdministrationKind, u128),
    RemoveAdministration(AdministrationKind, u128),
    Publish(Publication),
    ReadHeld(u128, u64, u32),
    ReadDead(u128, u64, u32),
    ReadDeadMessage(u128, MessageId),
    Replay(Replay),
}

/// What an operation answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    Done,
    Chunk(Option<StreamChunk>),
    Message(Option<MessageRecord>),
    Journey(Option<JourneyRecord>),
    Claim(Option<Claim>),
    Yes(bool),
    Count(u32),
    Audit(Option<AuditEntry>),
    Administration(Option<AdministrationRecord>),
    Held(HeldQueue),
    DeadQueue(DeadQueue),
    Dead(DeadEntry),
    Replayed(Replayed),
    /// A record failed its authentication tag: the scope and the reason,
    /// for the caller to audit (ADR-0063, Consequences).
    Refused(String, String),
    /// The operation failed, in words.
    Failed(String),
}

fn write_optional<T: Form>(out: &mut Vec<u8>, value: Option<&T>) {
    write_byte(out, u8::from(value.is_some()));
    if let Some(value) = value {
        value.write(out);
    }
}

fn read_optional<T: Form>(cursor: &mut Cursor<'_>) -> Result<Option<T>, PersistError> {
    match read_byte(cursor)? {
        0 => Ok(None),
        _ => T::read(cursor).map(Some),
    }
}

fn nanos(lease: Duration) -> u64 {
    u64::try_from(lease.as_nanos()).unwrap_or(u64::MAX)
}

impl Request {
    pub(crate) fn claim(claim: Claim, lease: Duration) -> Self {
        Self::Claim(claim, nanos(lease))
    }

    pub(crate) fn renew(claim: Claim, lease: Duration) -> Self {
        Self::Renew(claim, nanos(lease))
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
            Self::Claim(claim, lease) | Self::Renew(claim, lease) => {
                write_byte(
                    out,
                    if matches!(self, Self::Claim(..)) {
                        7
                    } else {
                        8
                    },
                );
                claim.write(out);
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
            Self::KeepAudit(most) => {
                write_byte(out, 12);
                write_u32(out, *most);
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
            8 => Self::Renew(Claim::read(cursor)?, read_u64(cursor)?),
            9 => Self::Release(Claim::read(cursor)?),
            10 => Self::HandOn(HandOn::read(cursor)?),
            11 => Self::WriteAudit(AuditEntry::read(cursor)?),
            12 => Self::KeepAudit(read_u32(cursor)?),
            13 => Self::ReadKeptAudit(AuditId::new(read_u128(cursor)?)),
            14 => Self::WriteAdministration(AdministrationRecord::read(cursor)?),
            15 => Self::ReadAdministration(AdministrationKind::read(cursor)?, read_u128(cursor)?),
            16 => Self::RemoveAdministration(AdministrationKind::read(cursor)?, read_u128(cursor)?),
            17 => Self::Publish(Publication::read(cursor)?),
            18 => Self::ReadHeld(read_u128(cursor)?, read_u64(cursor)?, read_u32(cursor)?),
            19 => Self::ReadDead(read_u128(cursor)?, read_u64(cursor)?, read_u32(cursor)?),
            20 => Self::ReadDeadMessage(read_u128(cursor)?, MessageId::new(read_u128(cursor)?)),
            21 => Self::Replay(Replay::read(cursor)?),
            other => return Err(malformed(format!("no operation is numbered {other}"))),
        })
    }
}

impl Form for Answer {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Done => write_byte(out, 0),
            Self::Chunk(chunk) => {
                write_byte(out, 1);
                write_optional(out, chunk.as_ref());
            }
            Self::Message(record) => {
                write_byte(out, 2);
                write_optional(out, record.as_ref());
            }
            Self::Journey(record) => {
                write_byte(out, 3);
                write_optional(out, record.as_ref());
            }
            Self::Claim(claim) => {
                write_byte(out, 4);
                write_optional(out, claim.as_ref());
            }
            Self::Yes(yes) => {
                write_byte(out, 5);
                write_byte(out, u8::from(*yes));
            }
            Self::Count(count) => {
                write_byte(out, 6);
                write_u32(out, *count);
            }
            Self::Audit(entry) => {
                write_byte(out, 7);
                write_optional(out, entry.as_ref());
            }
            Self::Administration(record) => {
                write_byte(out, 8);
                write_optional(out, record.as_ref());
            }
            Self::Refused(scope, reason) => {
                write_byte(out, 9);
                write_text(out, scope);
                write_text(out, reason);
            }
            Self::Failed(reason) => {
                write_byte(out, 10);
                write_text(out, reason);
            }
            Self::Held(queue) => {
                write_byte(out, 11);
                queue.write(out);
            }
            Self::DeadQueue(queue) => {
                write_byte(out, 12);
                queue.write(out);
            }
            Self::Dead(entry) => {
                write_byte(out, 13);
                entry.write(out);
            }
            Self::Replayed(replayed) => {
                write_byte(out, 14);
                replayed.write(out);
            }
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(match read_byte(cursor)? {
            0 => Self::Done,
            1 => Self::Chunk(read_optional(cursor)?),
            2 => Self::Message(read_optional(cursor)?),
            3 => Self::Journey(read_optional(cursor)?),
            4 => Self::Claim(read_optional(cursor)?),
            5 => Self::Yes(read_byte(cursor)? != 0),
            6 => Self::Count(read_u32(cursor)?),
            7 => Self::Audit(read_optional(cursor)?),
            8 => Self::Administration(read_optional(cursor)?),
            9 => Self::Refused(read_text(cursor)?, read_text(cursor)?),
            10 => Self::Failed(read_text(cursor)?),
            11 => Self::Held(HeldQueue::read(cursor)?),
            12 => Self::DeadQueue(DeadQueue::read(cursor)?),
            13 => Self::Dead(DeadEntry::read(cursor)?),
            14 => Self::Replayed(Replayed::read(cursor)?),
            other => return Err(malformed(format!("no answer is numbered {other}"))),
        })
    }
}

/// Write `record` as one frame.
///
/// # Errors
///
/// Where the connection fails, or the record is larger than a frame holds.
pub(crate) fn send(connection: &mut impl Write, record: &impl Form) -> std::io::Result<()> {
    let bytes = record.bytes();
    let length = u32::try_from(bytes.len())
        .ok()
        .filter(|length| *length as usize <= MOST)
        .ok_or_else(|| std::io::Error::other("a record larger than a frame holds"))?;
    let mut frame = Vec::with_capacity(4 + bytes.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&bytes);
    connection.write_all(&frame)?;
    connection.flush()
}

/// Read one frame, and the record in it. `None` where the connection ended
/// cleanly before a frame began.
///
/// # Errors
///
/// Where the connection fails or ends inside a frame, or the frame is not
/// a record.
pub(crate) fn receive<T: Form>(connection: &mut impl Read) -> std::io::Result<Option<T>> {
    let mut length = [0u8; 4];
    match connection.read_exact(&mut length) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > MOST {
        return Err(std::io::Error::other("a frame larger than a frame holds"));
    }
    let mut bytes = vec![0u8; length];
    connection.read_exact(&mut bytes)?;
    T::from_bytes(&bytes)
        .map(Some)
        .map_err(|error| std::io::Error::other(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            Request::Release(claim.clone()),
            Request::ReadChunk(StreamId::new(1), 2),
            Request::KeepAudit(64),
            Request::ReadHeld(7, 2, 64),
            Request::HandOn(HandOn {
                claim: claim.clone(),
                result: JourneyRecord {
                    journey: JourneyId::new(5),
                    body: b"delivered".to_vec(),
                },
                messages: Vec::new(),
                next: Vec::new(),
                leaves: vec![7],
                queued: Vec::new(),
                requeued: Vec::new(),
                kept_for_nanos: None,
            }),
            Request::RemoveAdministration(AdministrationKind::Operator, 3),
            Request::Publish(Publication {
                message: MessageRecord {
                    message: MessageId::new(1),
                    body: b"order".to_vec(),
                },
                journeys: Vec::new(),
                held: Vec::new(),
                dead: None,
                audit: AuditEntry {
                    id: AuditId::new(2),
                    body: b"published".to_vec(),
                },
                claims: vec![claim.clone()],
                lease_nanos: 30_000_000_000,
            }),
            Request::ReadDead(7, 2, 64),
            Request::ReadDeadMessage(7, MessageId::new(1)),
            Request::Replay(Replay {
                queue: 7,
                message: MessageId::new(1),
                journeys: Vec::new(),
                held: Vec::new(),
                audit: AuditEntry {
                    id: AuditId::new(3),
                    body: b"replayed".to_vec(),
                },
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
            Answer::Claim(Some(claim)),
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
        ];
        for answer in answers {
            assert_eq!(Answer::from_bytes(&answer.bytes()).expect("answer"), answer);
        }
    }

    #[test]
    fn a_frame_cut_short_or_numbered_for_nothing_is_refused() {
        let mut wire = Vec::new();
        send(&mut wire, &Request::KeepAudit(1)).expect("sent");
        let cut = &wire[..wire.len() - 1];
        assert!(receive::<Request>(&mut &cut[..]).is_err());
        let nothing = [0, 0, 0, 1, 99];
        assert!(receive::<Request>(&mut &nothing[..]).is_err());
        let huge = (u32::MAX).to_be_bytes();
        assert!(receive::<Request>(&mut &huge[..]).is_err());
    }
}
