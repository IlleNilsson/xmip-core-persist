//! What Xmip Storage answers on the wire: a verdict, and what the
//! operation returned or why it failed (`super`).

use codec::cursor::Cursor;

use super::super::dead::{DeadEntry, DeadQueue, Replayed};
use super::super::hold::HeldQueue;
use super::super::kept_audit::KeptAudit;
use super::super::record::{
    AdministrationRecord, Claim, Form, JourneyRecord, MessageRecord, StreamChunk, malformed,
    read_byte, read_bytes, read_text, read_u32, read_u128, write_byte, write_bytes, write_text,
    write_u32, write_u128,
};
use super::super::stream::StreamRecord;
use crate::PersistError;

/// What an operation answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    Done,
    Chunk(Option<StreamChunk>),
    Message(Option<Box<MessageRecord>>),
    Journey(Option<Box<JourneyRecord>>),
    Claim(Option<Claim>),
    /// The claims a Publication's node holds.
    Claims(Vec<Claim>),
    Yes(bool),
    Count(u32),
    Audit(Option<Box<KeptAudit>>),
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
    /// The identifiers of the records a query found.
    Records(Vec<u128>),
    /// A Stream's own record, or none.
    Stream(Option<StreamRecord>),
    /// A chunk of a kept audit record's body, or none.
    Bytes(Option<Vec<u8>>),
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
            Self::Claims(held) => {
                write_byte(out, 15);
                held.write(out);
            }
            Self::Records(records) => {
                write_byte(out, 16);
                write_u32(out, u32::try_from(records.len()).unwrap_or(u32::MAX));
                for record in records {
                    write_u128(out, *record);
                }
            }
            Self::Stream(stream) => {
                write_byte(out, 17);
                write_optional(out, stream.as_ref());
            }
            Self::Bytes(bytes) => {
                write_byte(out, 18);
                write_byte(out, u8::from(bytes.is_some()));
                if let Some(bytes) = bytes {
                    write_bytes(out, bytes);
                }
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
            15 => Self::Claims(Vec::read(cursor)?),
            16 => Self::Records(
                (0..read_u32(cursor)?)
                    .map(|_| read_u128(cursor))
                    .collect::<Result<_, _>>()?,
            ),
            17 => Self::Stream(read_optional(cursor)?),
            18 => Self::Bytes(match read_byte(cursor)? {
                0 => None,
                _ => Some(read_bytes(cursor)?),
            }),
            other => return Err(malformed(format!("no answer is numbered {other}"))),
        })
    }
}
