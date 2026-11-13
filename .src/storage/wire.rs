//! Xmip Storage on the wire: one request, one answer, over a mutual TLS
//! connection between a node and a Storage node (ADR-0063, amendment
//! 2026-10-01: *Xmip encrypts to Xmip Node with Role/Type Storage*).
//!
//! A frame is its length, four bytes big-endian, and that many bytes: a
//! request is an operation's number and its fields, an answer a verdict and
//! what the operation returned or why it failed. Each record is written in
//! its one binary form ([`Form`]), the form either database seals it in. A
//! connection carries one request at a time and answers it before the next,
//! synchronously, with no async runtime — a claim asked ahead of its sync
//! with two frames, the claim and then when it is durable, and nothing else
//! is asked on its connection before the second is read (ADR-0018,
//! amendment 2026-11-12); a node keeps a connection per
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
use super::watch::Position;

mod answer;
mod late;
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
    /// A wait by the token its node ends it with, for the holder whose
    /// claims are its node's (empty for none), each it watches with where it
    /// was last seen, for at most so many nanoseconds, answered with up to
    /// so many of what is due in each queue that moved (`super::watch`).
    Wait(u128, String, Vec<Position>, u64, u32),
    /// The wait asked with this token ended, its waiter stopped.
    EndWait(u128),
    /// What a Message is acted on under its claim: its place in a queue,
    /// its Journey, its Message and its Stream's record (`super::claimed`).
    ReadClaimed(u128, u64, JourneyId),
    /// A claim for so many nanoseconds, asked ahead of its sync: answered
    /// twice, the claim once readable and, where it was taken, a second
    /// frame once it is durable or why it is not (`Answer::Kept`).
    ClaimAhead(Claim, u64),
}

fn nanos(lease: Duration) -> u64 {
    u64::try_from(lease.as_nanos()).unwrap_or(u64::MAX)
}

impl Request {
    pub(crate) fn claim(claim: Claim, lease: Duration) -> Self {
        Self::Claim(claim, nanos(lease))
    }

    pub(crate) fn claim_ahead(claim: Claim, lease: Duration) -> Self {
        Self::ClaimAhead(claim, nanos(lease))
    }

    pub(crate) fn wait(
        (token, holder): (u128, Option<&str>),
        seen: &[Position],
        (bound, due): (Duration, u32),
    ) -> Self {
        let holder = holder.unwrap_or_default().to_string();
        Self::Wait(token, holder, seen.to_vec(), nanos(bound), due)
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
            37 => {
                let (token, holder) = (cursor.u128_be()?, field::read_text(cursor)?);
                let (seen, bound) = (Vec::read(cursor)?, cursor.u64_be()?);
                Self::Wait(token, holder, seen, bound, cursor.u32_be()?)
            }
            38 => Self::EndWait(cursor.u128_be()?),
            39 => {
                let (queue, at) = (cursor.u128_be()?, cursor.u64_be()?);
                Self::ReadClaimed(queue, at, JourneyId::new(cursor.u128_be()?))
            }
            40 => Self::ClaimAhead(Claim::read(cursor)?, cursor.u64_be()?),
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
    sent(connection, record).map(|_| ())
}

/// Write `record` as one frame, as [`send`] does, and say how many bytes
/// the record took: what a primary counts it handed a standby
/// (`standby::followers`).
///
/// # Errors
///
/// As [`send`].
pub(crate) fn sent(connection: &mut impl Write, record: &impl Form) -> io::Result<usize> {
    let bytes = record.bytes();
    net::frame::write(connection, &bytes).map_err(failed)?;
    Ok(bytes.len())
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
    fn a_wait_on_queues_and_its_answer_cross_a_frame_as_they_were() {
        use super::super::watch::{Moved, Watched};
        let seen = [
            Position::queue(7, 3),
            Position::queue(9, 0),
            Position {
                watched: Watched::Head(5),
                at: 2,
            },
            Position {
                watched: Watched::Instance(u128::MAX),
                at: 4,
            },
            Position {
                watched: Watched::Key(6),
                at: 0,
            },
        ];
        let holder = configure::fixture::test_cluster().node_scope(0);
        let request = Request::wait(
            (u128::MAX - 5, Some(holder.as_str())),
            &seen,
            (Duration::from_secs(1), 64),
        );
        assert_eq!(request.operation(), ("read", "wait_queues"));
        let ended = Request::EndWait(u128::MAX - 5);
        assert_eq!(ended.operation(), ("read", "end_wait"));
        for request in [request, ended] {
            assert_eq!(
                Request::from_bytes(&request.bytes()).expect("read"),
                request
            );
        }
        let due = crate::storage::DueQueue {
            retrying: 1,
            read_unix_nanos: 5,
            ..crate::storage::DueQueue::default()
        };
        for answer in [
            Answer::Moved(vec![
                Moved {
                    position: Position::queue(7, 4),
                    due: Some(due),
                },
                Moved {
                    position: seen[2],
                    due: None,
                },
            ]),
            Answer::Moved(Vec::new()),
        ] {
            assert_eq!(Answer::from_bytes(&answer.bytes()).expect("read"), answer);
        }
    }

    #[test]
    fn a_read_under_a_claim_and_its_answer_cross_a_frame_as_they_were() {
        let request = Request::ReadClaimed(7, 3, JourneyId::new(u128::MAX - 1));
        assert_eq!(request.operation(), ("read", "read_claimed"));
        assert_eq!(
            Request::from_bytes(&request.bytes()).expect("read"),
            request
        );
        let claimed = super::super::Claimed {
            journey: Some(JourneyRecord {
                journey: JourneyId::new(4),
                body: b"to send".to_vec(),
                facts: JourneyFacts::default(),
            }),
            ..super::super::Claimed::default()
        };
        let answer = Answer::Claimed(Box::new(claimed));
        assert_eq!(Answer::from_bytes(&answer.bytes()).expect("read"), answer);
    }

    #[test]
    fn a_claim_asked_ahead_and_its_second_frame_cross_a_frame_as_they_were() {
        let claim = Claim::asked(JourneyId::new(5), "edge-01", 9);
        let request = Request::claim_ahead(claim, Duration::from_secs(30));
        assert_eq!(
            request.operation(),
            ("write", "claim"),
            "admitted as a claim"
        );
        assert_eq!(
            Request::from_bytes(&request.bytes()).expect("read"),
            request
        );
        for answer in [Answer::Kept(None), Answer::Kept(Some("the disk".into()))] {
            assert_eq!(Answer::from_bytes(&answer.bytes()).expect("read"), answer);
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
