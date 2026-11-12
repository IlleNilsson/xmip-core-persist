//! What Xmip Storage answers on the wire: a verdict, and what the
//! operation returned or why it failed (`super`).

use codec::CodecError;
use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;

use super::super::claimed::Claimed;
use super::super::dead::{DeadEntry, DeadQueue, Replayed};
use super::super::due::DueQueue;
use super::super::failed::FailedQueue;
use super::super::hold::HeldQueue;
use super::super::instance::InstanceRecord;
use super::super::kept_audit::KeptAudit;
use super::super::record::{
    AdministrationRecord, Claim, JourneyRecord, MessageRecord, StreamChunk,
};
use super::super::secret::{SecretEntry, SecretRecord};
use super::super::standby::Standing;
use super::super::stream::StreamRecord;
use super::super::watch::Moved;

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
    /// A queue's index of failed Messages.
    FailedQueue(FailedQueue),
    /// A part of a queue read by when each is due.
    DueQueue(DueQueue),
    /// What the Storage node is now (`super::super::standby`).
    Standing(Standing),
    /// The Storage node is a standby and serves no operation, in words.
    Standby(String),
    /// A Work Process's Instance, or none (`super::super::instance`).
    Instance(Option<Box<InstanceRecord>>),
    /// A secret, with its value, for the node that presents it.
    Secret(Option<Box<SecretRecord>>),
    /// The secrets kept, without their values.
    Secrets(Vec<SecretEntry>),
    /// What a wait found standing elsewhere than seen, as it stands, each
    /// queue's tail with what is due in it (`super::super::watch`).
    Moved(Vec<Moved>),
    /// What a Message is acted on under its claim
    /// (`super::super::claimed`).
    Claimed(Box<Claimed>),
    /// A claim asked ahead's second frame: durable on the Storage node, or
    /// why it is not.
    Kept(Option<String>),
}

impl Answer {
    /// The answers numbered from 21 on, written as `write` writes the rest.
    fn write_late(&self, out: &mut Vec<u8>) {
        match self {
            Self::Standing(standing) => {
                out.byte(21);
                standing.write(out);
            }
            Self::Standby(reason) => {
                out.byte(22);
                field::text(out, reason);
            }
            Self::Instance(record) => {
                out.byte(23);
                write_optional(out, record.as_ref());
            }
            Self::Secret(record) => {
                out.byte(24);
                write_optional(out, record.as_ref());
            }
            Self::Secrets(entries) => {
                out.byte(25);
                entries.write(out);
            }
            Self::Moved(moved) => {
                out.byte(26);
                moved.write(out);
            }
            Self::Claimed(claimed) => {
                out.byte(27);
                claimed.write(out);
            }
            Self::Kept(why) => {
                out.byte(28);
                out.byte(u8::from(why.is_some()));
                field::text(out, why.as_deref().unwrap_or_default());
            }
            _ => {}
        }
    }
}

fn write_optional<T: Form>(out: &mut Vec<u8>, value: Option<&T>) {
    out.byte(u8::from(value.is_some()));
    if let Some(value) = value {
        value.write(out);
    }
}

fn read_optional<T: Form>(cursor: &mut Cursor<'_>) -> codec::Result<Option<T>> {
    match cursor.byte()? {
        0 => Ok(None),
        _ => T::read(cursor).map(Some),
    }
}

impl Form for Answer {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::Done => {
                out.byte(0);
            }
            Self::Chunk(chunk) => {
                out.byte(1);
                write_optional(out, chunk.as_ref());
            }
            Self::Message(record) => {
                out.byte(2);
                write_optional(out, record.as_ref());
            }
            Self::Journey(record) => {
                out.byte(3);
                write_optional(out, record.as_ref());
            }
            Self::Claim(claim) => {
                out.byte(4);
                write_optional(out, claim.as_ref());
            }
            Self::Yes(yes) => {
                out.byte(5);
                out.byte(u8::from(*yes));
            }
            Self::Count(count) => {
                out.byte(6);
                out.u32_be(*count);
            }
            Self::Audit(entry) => {
                out.byte(7);
                write_optional(out, entry.as_ref());
            }
            Self::Administration(record) => {
                out.byte(8);
                write_optional(out, record.as_ref());
            }
            Self::Refused(scope, reason) => {
                out.byte(9);
                field::text(out, scope);
                field::text(out, reason);
            }
            Self::Failed(reason) => {
                out.byte(10);
                field::text(out, reason);
            }
            Self::Held(queue) => {
                out.byte(11);
                queue.write(out);
            }
            Self::DeadQueue(queue) => {
                out.byte(12);
                queue.write(out);
            }
            Self::Dead(entry) => {
                out.byte(13);
                entry.write(out);
            }
            Self::Replayed(replayed) => {
                out.byte(14);
                replayed.write(out);
            }
            Self::Claims(held) => {
                out.byte(15);
                held.write(out);
            }
            Self::Records(records) => {
                out.byte(16);
                out.u32_be(u32::try_from(records.len()).unwrap_or(u32::MAX));
                for record in records {
                    out.u128_be(*record);
                }
            }
            Self::Stream(stream) => {
                out.byte(17);
                write_optional(out, stream.as_ref());
            }
            Self::Bytes(bytes) => {
                out.byte(18);
                out.byte(u8::from(bytes.is_some()));
                if let Some(bytes) = bytes {
                    field::counted(out, bytes);
                }
            }
            Self::FailedQueue(queue) => {
                out.byte(19);
                queue.write(out);
            }
            Self::DueQueue(queue) => {
                out.byte(20);
                queue.write(out);
            }
            // Numbered from 21 on.
            _ => self.write_late(out),
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(match cursor.byte()? {
            0 => Self::Done,
            1 => Self::Chunk(read_optional(cursor)?),
            2 => Self::Message(read_optional(cursor)?),
            3 => Self::Journey(read_optional(cursor)?),
            4 => Self::Claim(read_optional(cursor)?),
            5 => Self::Yes(cursor.byte()? != 0),
            6 => Self::Count(cursor.u32_be()?),
            7 => Self::Audit(read_optional(cursor)?),
            8 => Self::Administration(read_optional(cursor)?),
            9 => Self::Refused(field::read_text(cursor)?, field::read_text(cursor)?),
            10 => Self::Failed(field::read_text(cursor)?),
            11 => Self::Held(HeldQueue::read(cursor)?),
            12 => Self::DeadQueue(DeadQueue::read(cursor)?),
            13 => Self::Dead(DeadEntry::read(cursor)?),
            14 => Self::Replayed(Replayed::read(cursor)?),
            15 => Self::Claims(Vec::read(cursor)?),
            16 => Self::Records(
                (0..cursor.u32_be()?)
                    .map(|_| cursor.u128_be())
                    .collect::<Result<_, _>>()?,
            ),
            17 => Self::Stream(read_optional(cursor)?),
            18 => Self::Bytes(match cursor.byte()? {
                0 => None,
                _ => Some(field::read_counted(cursor)?.to_vec()),
            }),
            19 => Self::FailedQueue(FailedQueue::read(cursor)?),
            20 => Self::DueQueue(DueQueue::read(cursor)?),
            21 => Self::Standing(Standing::read(cursor)?),
            22 => Self::Standby(field::read_text(cursor)?),
            23 => Self::Instance(read_optional(cursor)?),
            24 => Self::Secret(read_optional(cursor)?),
            25 => Self::Secrets(Vec::read(cursor)?),
            26 => Self::Moved(Vec::read(cursor)?),
            27 => Self::Claimed(Box::new(Claimed::read(cursor)?)),
            28 => {
                let said = cursor.byte()? != 0;
                let why = field::read_text(cursor)?;
                Self::Kept(said.then_some(why))
            }
            other => return Err(CodecError::new(format!("no answer is numbered {other}"))),
        })
    }
}
