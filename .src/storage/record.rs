//! What Xmip Storage keeps: the records its operations write and read, and
//! the one binary form each is written in — on the wire between a node and
//! a Storage node, and sealed in either database.
//!
//! Every record is keyed by a `UUIDv7` (`deployment-model.md` section 7,
//! *Record identifiers are `UUIDv7`*): a Stream chunk by its Stream and its
//! number in it, a Message, a Journey, an audit record and an
//! administration record by their own identifiers, a claim by the Journey
//! it is on. What a Message's or a Journey's body holds is the step's
//! serialization, which the Ledger path decides; Xmip Storage keeps it as
//! it is given, byte for byte.

use codec::cursor::Cursor;
use codec::field;
use codec::writer::ByteWriter;
use xcore::{AuditId, JourneyId, MessageId, StreamId};

use crate::PersistError;

/// A piece of a Stream: a Stream is written in chunks, never whole in
/// memory (`runtime-model.md` section 3, *Threads, pools and chunks*).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamChunk {
    pub stream: StreamId,
    /// Its number in the Stream, from zero.
    pub index: u32,
    /// Whether it is the Stream's last.
    pub last: bool,
    pub bytes: Vec<u8>,
}

/// A Message as a step wrote it to the Ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageRecord {
    pub message: MessageId,
    pub body: Vec<u8>,
}

/// A Journey as a step wrote it to the Ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JourneyRecord {
    pub journey: JourneyId,
    pub body: Vec<u8>,
}

/// A claim on a Journey: who holds it, by which token, until when
/// (`runtime-model.md` section 3, *Work moves by claim*). The time is the
/// Storage node's, so no two claimants' clocks are compared.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub journey: JourneyId,
    /// The node holding it: `xmip:///<cluster>/node/<name>`.
    pub holder: String,
    /// The claimant's own `UUIDv7` for this claim: a renewal, a release or a
    /// hand-on is the holder's only with it.
    pub token: u128,
    /// When it lapses, in nanoseconds since the Unix epoch.
    pub until_unix_nanos: i128,
}

/// One step handed on: its result, what it made and the Journeys that go
/// on from it, written with the claim released, as one write
/// (`runtime-model.md` section 3: *Every hand-on is one atomic write — the
/// step's result, the next Journey and the claim released together*).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HandOn {
    pub claim: Claim,
    /// The claimed Journey as the step left it.
    pub result: JourneyRecord,
    /// The Messages the step made.
    pub messages: Vec<MessageRecord>,
    /// The Journeys that go on from it.
    pub next: Vec<JourneyRecord>,
}

/// An audit record as its writer said it: written to the runtime database
/// first and moved to the administration database by the audit keeper
/// (ADR-0062, amendment 2026-10-01). The body is the audit capability's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    pub id: AuditId,
    pub body: Vec<u8>,
}

/// What the administration database keeps — what must be shared and kept
/// over time, and never configuration (`deployment-model.md` section 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AdministrationKind {
    /// A node's registration.
    Registration,
    /// Cluster membership.
    Membership,
    /// An installed Module.
    Module,
    /// An available Handler or Extension.
    Handler,
    /// A configuration version and deployment state.
    Deployment,
    /// Operator state: what is paused, by whom and when.
    Operator,
}

impl AdministrationKind {
    /// Every kind, in the order the record lists them.
    pub const ALL: [Self; 6] = [
        Self::Registration,
        Self::Membership,
        Self::Module,
        Self::Handler,
        Self::Deployment,
        Self::Operator,
    ];

    /// Its word, which names its records' kind in the database.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Registration => "registration",
            Self::Membership => "membership",
            Self::Module => "module",
            Self::Handler => "handler",
            Self::Deployment => "deployment",
            Self::Operator => "operator",
        }
    }

    fn number(self) -> u8 {
        match self {
            Self::Registration => 0,
            Self::Membership => 1,
            Self::Module => 2,
            Self::Handler => 3,
            Self::Deployment => 4,
            Self::Operator => 5,
        }
    }

    fn numbered(number: u8) -> Result<Self, PersistError> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.number() == number)
            .ok_or_else(|| malformed(format!("no administration kind is numbered {number}")))
    }
}

/// One administration record, keyed by its `UUIDv7`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdministrationRecord {
    pub kind: AdministrationKind,
    pub id: u128,
    pub body: Vec<u8>,
}

/// A record's one binary form.
pub trait Form: Sized {
    /// Written after what `out` holds.
    fn write(&self, out: &mut Vec<u8>);

    /// Read from where `cursor` is.
    ///
    /// # Errors
    ///
    /// [`PersistError::Record`] where the bytes are not one.
    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError>;

    /// The record alone, as bytes.
    fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.write(&mut out);
        out
    }

    /// The record `bytes` hold, and nothing after it.
    ///
    /// # Errors
    ///
    /// [`PersistError::Record`] where they are not one, or hold more.
    fn from_bytes(bytes: &[u8]) -> Result<Self, PersistError> {
        let mut cursor = Cursor::new(bytes);
        let record = Self::read(&mut cursor)?;
        if cursor.is_empty() {
            Ok(record)
        } else {
            Err(malformed("bytes after the record"))
        }
    }
}

pub(crate) fn malformed(reason: impl Into<String>) -> PersistError {
    PersistError::Record {
        reason: reason.into(),
    }
}

fn read<T>(result: codec::Result<T>) -> Result<T, PersistError> {
    result.map_err(|error| malformed(error.to_string()))
}

pub(crate) fn write_u128(out: &mut Vec<u8>, value: u128) {
    out.u128_be(value);
}

pub(crate) fn read_u128(cursor: &mut Cursor<'_>) -> Result<u128, PersistError> {
    read(cursor.u128_be())
}

pub(crate) fn write_i128(out: &mut Vec<u8>, value: i128) {
    out.i128_be(value);
}

pub(crate) fn read_i128(cursor: &mut Cursor<'_>) -> Result<i128, PersistError> {
    read(cursor.i128_be())
}

pub(crate) fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.u32_be(value);
}

pub(crate) fn read_u32(cursor: &mut Cursor<'_>) -> Result<u32, PersistError> {
    read(cursor.u32_be())
}

pub(crate) fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.u64_be(value);
}

pub(crate) fn read_u64(cursor: &mut Cursor<'_>) -> Result<u64, PersistError> {
    read(cursor.u64_be())
}

pub(crate) fn write_byte(out: &mut Vec<u8>, value: u8) {
    out.byte(value);
}

pub(crate) fn read_byte(cursor: &mut Cursor<'_>) -> Result<u8, PersistError> {
    read(cursor.byte())
}

/// Bytes, counted ([`codec::field::counted`]).
pub(crate) fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    field::counted(out, bytes);
}

pub(crate) fn read_bytes(cursor: &mut Cursor<'_>) -> Result<Vec<u8>, PersistError> {
    Ok(read(field::read_counted(cursor))?.to_vec())
}

pub(crate) fn write_text(out: &mut Vec<u8>, text: &str) {
    field::text(out, text);
}

pub(crate) fn read_text(cursor: &mut Cursor<'_>) -> Result<String, PersistError> {
    read(field::read_text(cursor))
}

impl Form for StreamChunk {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.stream.value());
        write_u32(out, self.index);
        write_byte(out, u8::from(self.last));
        write_bytes(out, &self.bytes);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            stream: StreamId::new(read_u128(cursor)?),
            index: read_u32(cursor)?,
            last: read_byte(cursor)? != 0,
            bytes: read_bytes(cursor)?,
        })
    }
}

impl Form for MessageRecord {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.message.value());
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            message: MessageId::new(read_u128(cursor)?),
            body: read_bytes(cursor)?,
        })
    }
}

impl Form for JourneyRecord {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.journey.value());
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            journey: JourneyId::new(read_u128(cursor)?),
            body: read_bytes(cursor)?,
        })
    }
}

impl Form for Claim {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.journey.value());
        write_text(out, &self.holder);
        write_u128(out, self.token);
        write_i128(out, self.until_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            journey: JourneyId::new(read_u128(cursor)?),
            holder: read_text(cursor)?,
            token: read_u128(cursor)?,
            until_unix_nanos: read_i128(cursor)?,
        })
    }
}

impl<T: Form> Form for Vec<T> {
    fn write(&self, out: &mut Vec<u8>) {
        write_u32(out, u32::try_from(self.len()).unwrap_or(u32::MAX));
        for item in self {
            item.write(out);
        }
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let count = read_u32(cursor)?;
        (0..count).map(|_| T::read(cursor)).collect()
    }
}

impl Form for HandOn {
    fn write(&self, out: &mut Vec<u8>) {
        self.claim.write(out);
        self.result.write(out);
        self.messages.write(out);
        self.next.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            claim: Claim::read(cursor)?,
            result: JourneyRecord::read(cursor)?,
            messages: Vec::read(cursor)?,
            next: Vec::read(cursor)?,
        })
    }
}

impl Form for AuditEntry {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.id.value());
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            id: AuditId::new(read_u128(cursor)?),
            body: read_bytes(cursor)?,
        })
    }
}

impl Form for AdministrationKind {
    fn write(&self, out: &mut Vec<u8>) {
        write_byte(out, self.number());
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Self::numbered(read_byte(cursor)?)
    }
}

impl Form for AdministrationRecord {
    fn write(&self, out: &mut Vec<u8>) {
        self.kind.write(out);
        write_u128(out, self.id);
        write_bytes(out, &self.body);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            kind: AdministrationKind::read(cursor)?,
            id: read_u128(cursor)?,
            body: read_bytes(cursor)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim() -> Claim {
        Claim {
            journey: JourneyId::new(0x0199_0000_0000_7000_8000_0000_0000_0007),
            holder: configure::fixture::test_cluster().node_scope(0),
            token: 42,
            until_unix_nanos: -5,
        }
    }

    #[test]
    fn every_record_comes_back_from_its_bytes_as_it_was() {
        let chunk = StreamChunk {
            stream: StreamId::new(9),
            index: 3,
            last: true,
            bytes: b"<Order/>".to_vec(),
        };
        assert_eq!(
            StreamChunk::from_bytes(&chunk.bytes()).expect("chunk"),
            chunk
        );
        let hand_on = HandOn {
            claim: claim(),
            result: JourneyRecord {
                journey: claim().journey,
                body: b"done".to_vec(),
            },
            messages: vec![MessageRecord {
                message: MessageId::new(8),
                body: vec![0, 1, 2],
            }],
            next: Vec::new(),
        };
        assert_eq!(
            HandOn::from_bytes(&hand_on.bytes()).expect("hand-on"),
            hand_on
        );
        for kind in AdministrationKind::ALL {
            let record = AdministrationRecord {
                kind,
                id: 7,
                body: kind.word().as_bytes().to_vec(),
            };
            assert_eq!(
                AdministrationRecord::from_bytes(&record.bytes()).expect("record"),
                record
            );
        }
    }

    #[test]
    fn bytes_that_are_short_or_long_are_refused() {
        let bytes = claim().bytes();
        assert!(Claim::from_bytes(&bytes[..bytes.len() - 1]).is_err());
        let longer = [bytes.as_slice(), &[0]].concat();
        assert!(Claim::from_bytes(&longer).is_err());
        assert!(AdministrationKind::from_bytes(&[9]).is_err());
    }
}
