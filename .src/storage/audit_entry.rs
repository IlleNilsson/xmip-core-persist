//! An audit record as Xmip Storage keeps it: its body, the Message and
//! Streams it carries, and its fields laid out (ADR-0062, amendment
//! 2026-10-01; ADR-0070).

use codec::cursor::Cursor;
use xcore::AuditId;

use super::audited::Audited;
use super::facts::AuditFacts;
use super::record::{Form, read_byte, read_bytes, read_u128, write_byte, write_bytes, write_u128};
use crate::PersistError;

/// An audit record as its writer said it: written to the runtime database
/// first and moved to the audit database by the audit keeper
/// (ADR-0062, amendment 2026-10-01). The body is the audit capability's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    pub id: AuditId,
    pub body: Vec<u8>,
    /// The Message an audited act was on, in full, and its Stream, whose
    /// bytes the keeper keeps beside the record (ADR-0070,
    /// `super::audited`); `None` for an act on none.
    pub audited: Option<Audited>,
    /// What the audit database keeps of it in columns of their
    /// own once the keeper moved it there (`super::facts`).
    pub facts: AuditFacts,
}

impl AuditEntry {
    /// Its body as the keeper keeps it in chunks: its record and what it
    /// carries of the Message ([`AuditBody`]'s form).
    #[must_use]
    pub fn body_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_body(&mut out, &self.body, self.audited.as_ref());
        out
    }
}

impl Form for AuditEntry {
    fn write(&self, out: &mut Vec<u8>) {
        write_u128(out, self.id.value());
        write_body(out, &self.body, self.audited.as_ref());
        self.facts.write(out);
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        let id = AuditId::new(read_u128(cursor)?);
        let AuditBody { body, audited } = AuditBody::read(cursor)?;
        Ok(Self {
            id,
            body,
            audited,
            facts: AuditFacts::read(cursor)?,
        })
    }
}

/// A kept audit record's body, read back from its chunks
/// (`super::ChunkReader::audit_body`): its record, and the Message it
/// carries with the Streams it is over (ADR-0070, amendment 2026-10-10:
/// *The audit body has to be like the stream, in chunks*).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditBody {
    pub body: Vec<u8>,
    pub audited: Option<Audited>,
}

/// The one writing of a body, in a record and in its chunks.
fn write_body(out: &mut Vec<u8>, body: &[u8], audited: Option<&Audited>) {
    write_bytes(out, body);
    write_byte(out, u8::from(audited.is_some()));
    if let Some(audited) = audited {
        audited.write(out);
    }
}

impl Form for AuditBody {
    fn write(&self, out: &mut Vec<u8>) {
        write_body(out, &self.body, self.audited.as_ref());
    }

    fn read(cursor: &mut Cursor<'_>) -> Result<Self, PersistError> {
        Ok(Self {
            body: read_bytes(cursor)?,
            audited: match read_byte(cursor)? {
                0 => None,
                _ => Some(Audited::read(cursor)?),
            },
        })
    }
}
