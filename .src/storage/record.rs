//! What Xmip Storage keeps: the records its operations write and read, and
//! the one binary form each is written in — on the wire between a node and
//! a Storage node, and sealed in either database.
//!
//! Every record is keyed by a `UUIDv7` (`deployment-model.md` section 7,
//! *Record identifiers are `UUIDv7`*): a Stream chunk by its Stream and its
//! number in it, a Message, a Journey, an audit record and an
//! administration record by their own identifiers, a claim by the Journey
//! of the Message it is on. What a Message's or a Journey's body holds is the step's
//! serialization, which the Ledger path decides; Xmip Storage keeps it as
//! it is given, byte for byte.

use codec::CodecError;
use codec::cursor::Cursor;
use codec::field;
use codec::form::Form;
use codec::writer::ByteWriter;
use xcore::{JourneyId, MessageId, StreamId};

use super::facts::{JourneyFacts, MessageFacts};
use crate::PersistError;

/// A piece of a Stream: a Stream is written in chunks, never whole in
/// memory (`runtime-model.md` section 3, *Threads, pools and chunks*). A
/// Stream ends where it has no further chunk; its Message keeps its length,
/// which a reader holds what it read to (the owner, 2026-10-09).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamChunk {
    pub stream: StreamId,
    /// Its number in the Stream, from zero.
    pub index: u32,
    pub bytes: Vec<u8>,
}

/// A Message as a step wrote it to the Ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageRecord {
    pub message: MessageId,
    pub body: Vec<u8>,
    /// What its table keeps of it in columns of their own (`super::facts`).
    pub facts: MessageFacts,
}

/// A Journey as a step wrote it to the Ledger.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JourneyRecord {
    pub journey: JourneyId,
    pub body: Vec<u8>,
    /// What its table keeps of it in columns of their own (`super::facts`).
    pub facts: JourneyFacts,
}

/// A claim on a Message, by its Journey: who holds it, by which token, until when
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
    /// Its fencing token, Xmip Storage's to set as it takes the claim: the
    /// Storage epoch in the high 32 bits and how many holders the Message
    /// has had in the low 32, so it grows with every holder and every
    /// failover, and a write by a holder the claim has passed from is
    /// refused (the owner, 2026-10-27). Asked for, it is zero.
    pub fence: u64,
}

impl Claim {
    /// The claim on `journey` that `holder` asks for under `token`: its
    /// lapse and its fence are Xmip Storage's to set as it takes it.
    pub(crate) fn asked(journey: JourneyId, holder: &str, token: u128) -> Self {
        Self {
            journey,
            holder: holder.to_string(),
            token,
            until_unix_nanos: 0,
            fence: 0,
        }
    }
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

    fn numbered(number: u8) -> codec::Result<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.number() == number)
            .ok_or_else(|| CodecError::new(format!("no administration kind is numbered {number}")))
    }
}

/// One administration record, keyed by its `UUIDv7`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdministrationRecord {
    pub kind: AdministrationKind,
    pub id: u128,
    pub body: Vec<u8>,
    /// When Xmip Storage last wrote it, in nanoseconds since the Unix
    /// epoch: Xmip Storage's to set.
    pub updated_unix_nanos: u64,
}

/// A record that would not encode or decode, in words: what is not a
/// [`Form`]'s refusal. A refusal of one becomes a [`PersistError::Record`]
/// through `From`, once, where the bytes came in.
pub(crate) fn malformed(reason: impl Into<String>) -> PersistError {
    PersistError::Record {
        reason: reason.into(),
    }
}

impl Form for StreamChunk {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.stream.value()).u32_be(self.index);
        field::counted(out, &self.bytes);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            stream: StreamId::new(cursor.u128_be()?),
            index: cursor.u32_be()?,
            bytes: field::read_counted(cursor)?.to_vec(),
        })
    }
}

impl Form for MessageRecord {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.message.value());
        field::counted(out, &self.body);
        self.facts.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            message: MessageId::new(cursor.u128_be()?),
            body: field::read_counted(cursor)?.to_vec(),
            facts: MessageFacts::read(cursor)?,
        })
    }
}

impl Form for JourneyRecord {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.journey.value());
        field::counted(out, &self.body);
        self.facts.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            journey: JourneyId::new(cursor.u128_be()?),
            body: field::read_counted(cursor)?.to_vec(),
            facts: JourneyFacts::read(cursor)?,
        })
    }
}

impl Form for Claim {
    fn write(&self, out: &mut Vec<u8>) {
        out.u128_be(self.journey.value());
        field::text(out, &self.holder);
        out.u128_be(self.token)
            .i128_be(self.until_unix_nanos)
            .u64_be(self.fence);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            journey: JourneyId::new(cursor.u128_be()?),
            holder: field::read_text(cursor)?,
            token: cursor.u128_be()?,
            until_unix_nanos: cursor.i128_be()?,
            // A claim kept before fences were written ends here: it was
            // taken with none (`versioned`: a value added is read as
            // nothing where a record written before lacks it).
            fence: if cursor.is_empty() {
                0
            } else {
                cursor.u64_be()?
            },
        })
    }
}

impl Form for AdministrationKind {
    fn write(&self, out: &mut Vec<u8>) {
        out.byte(self.number());
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Self::numbered(cursor.byte()?)
    }
}

impl Form for AdministrationRecord {
    fn write(&self, out: &mut Vec<u8>) {
        self.kind.write(out);
        out.u128_be(self.id);
        field::counted(out, &self.body);
        out.u64_be(self.updated_unix_nanos);
    }

    fn read(cursor: &mut Cursor<'_>) -> codec::Result<Self> {
        Ok(Self {
            kind: AdministrationKind::read(cursor)?,
            id: cursor.u128_be()?,
            body: field::read_counted(cursor)?.to_vec(),
            updated_unix_nanos: cursor.u64_be()?,
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
            fence: 7,
        }
    }

    #[test]
    fn every_record_comes_back_from_its_bytes_as_it_was() {
        let chunk = StreamChunk {
            stream: StreamId::new(9),
            index: 3,
            bytes: b"<Order/>".to_vec(),
        };
        assert_eq!(
            StreamChunk::from_bytes(&chunk.bytes()).expect("chunk"),
            chunk
        );
        for kind in AdministrationKind::ALL {
            let record = AdministrationRecord {
                kind,
                id: 7,
                body: kind.word().as_bytes().to_vec(),
                updated_unix_nanos: 0,
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
